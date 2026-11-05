use anyhow::{Context, Result, bail};
use chrono::Utc;
use notify::{Config as NotifyConfig, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::BTreeSet;
#[cfg(target_os = "linux")]
use std::ffi::CString;
use std::ffi::OsString;
use std::fs;
#[cfg(target_os = "linux")]
use std::mem;
#[cfg(target_os = "linux")]
use std::os::fd::RawFd;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, TryLockError, mpsc};
use std::time::{Duration, Instant, SystemTime};
use walkdir::WalkDir;

use crate::cli::{ResolvedWatchArgs, SnapshotArgs, WatchArgs};
use crate::cluster_runtime::{
    cluster_heartbeat_interval, cluster_master_mode_enabled, cluster_slave_heartbeat,
    serve_master_from_env,
};
use crate::config::{
    Paths, RootConfig, WatchConfig, maintenance_lock_path, read_config, validate_watch_mode,
};
use crate::daemon_runtime::{
    ForegroundDaemonRuntime, WatchDaemonLaunchConfig, acquire_daemon_lock, child_process_exe,
    ensure_daemon_ipc, start_daemon_ipc, start_watch_daemon,
};
use crate::history_runtime::refresh_runtime_health;
use crate::operation_log::OperationOriginOverride;
#[cfg(target_os = "linux")]
use crate::operation_log::origin_override_from_pid;
use crate::platform_runtime::configure_background_command;
use crate::process_runtime::{
    ProcessIdentity, acquire_process_lock, pid_alive, process_identity, process_identity_matches,
    process_lock_owner,
};
use crate::queue_runtime::{
    UploadQueueStats, event_journal_records, record_event, record_file_event,
    remote_event_journal_stats, upload_queue_stats,
};
use crate::root_state::roots;
use crate::snapshot_rules::{
    build_ignore, explicitly_untracked, is_ignored, root_dir_allows_descend,
    volatile_allows_watch_snapshot,
};
use crate::snapshot_state::current_snapshot;
use crate::sync_runtime::{AutoSyncResult, sync_current_if_remote};
use crate::{ensure_ready, open_db, snapshot};

// watchと再送schedulerが、子プロセスのsync lock取得前に競合するのを防ぐ。
// clusterでは単発syncの終了だけを待ち、全journalのACK待機は追加しない。
static WATCH_SYNC_SLOT: Mutex<()> = Mutex::new(());

fn watch_sync_slot() -> Result<MutexGuard<'static, ()>> {
    WATCH_SYNC_SLOT
        .lock()
        .map_err(|_| anyhow::anyhow!("watch同期の排他状態が破損しています"))
}

pub fn normalize_watch_backend(backend: &str) -> Result<&'static str> {
    crate::majutsu_watch::WatchBackend::normalize(backend)
        .map(|backend| backend.as_cli())
        .map_err(anyhow::Error::msg)
}

pub fn default_daemon_backend() -> &'static str {
    crate::majutsu_watch::default_backend()
}

pub fn default_watch_backend() -> String {
    default_daemon_backend().into()
}

fn write_active_watch_backend(paths: &Paths, backend: &str) -> Result<()> {
    fs::create_dir_all(&paths.runtime)?;
    crate::atomic_io::write_atomic(&paths.runtime.join("watch-backend"), backend.as_bytes())
}

pub(crate) fn default_watch_max_rss_mib() -> u64 {
    std::env::var("MAJUTSU_WATCH_MAX_RSS_MIB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(2048)
}

pub(crate) fn watch_cmd(paths: &Paths, args: WatchArgs) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let args = resolve_watch_args(args, &config.watch)?;
    let backend = normalize_watch_backend(&args.backend)?;
    if !args.foreground {
        let pid = start_watch_daemon(
            paths,
            WatchDaemonLaunchConfig {
                backend,
                mode: args.mode.clone(),
                interval_secs: args.interval_secs,
                debounce_ms: args.debounce_ms,
                settle_ms: args.settle_ms,
                buffer_max_ms: args.buffer_max_ms,
                buffer_max_events: args.buffer_max_events,
                periodic_rescan_secs: args.periodic_rescan_secs,
                max_rss_mib: args.max_rss_mib,
            },
        )?;
        println!("started daemon pid {pid}");
        return Ok(());
    }
    let _lock = acquire_daemon_lock(paths)?;
    let _runtime = ForegroundDaemonRuntime::register(paths)?;
    start_daemon_ipc(paths)?;
    if cluster_master_mode_enabled() {
        write_active_watch_backend(paths, "cluster-master")?;
        return serve_master_from_env(paths);
    }
    if !args.once {
        if configured_remote_is_cluster(paths) {
            start_cluster_heartbeat_scheduler(paths)?;
        }
        start_upload_retry_scheduler(paths)?;
    }
    match backend {
        "fanotify" => match watch_fanotify(paths, args.clone(), &config) {
            Ok(()) => Ok(()),
            Err(err) => {
                record_event(
                    paths,
                    "watch-backend-error",
                    &format!("backend=fanotify error={err:#}"),
                )?;
                Err(err)
            }
        },
        "notify" => watch_notify(paths, args, "notify"),
        "inotify" => watch_notify(paths, args, "inotify"),
        "poll" => watch_poll(paths, &args),
        other => bail!("unsupported watch backend: {other}"),
    }
}

#[cfg(not(target_os = "linux"))]
fn watch_fanotify(
    _paths: &Paths,
    _args: ResolvedWatchArgs,
    _config: &crate::config::Config,
) -> Result<()> {
    bail!("fanotify backend is only available on Linux")
}

#[cfg(target_os = "linux")]
fn watch_fanotify(
    paths: &Paths,
    args: ResolvedWatchArgs,
    _config: &crate::config::Config,
) -> Result<()> {
    if unsafe { libc::geteuid() } != 0 {
        bail!("fanotify backend requires root privileges")
    }
    let conn = open_db(paths)?;
    let active_roots = roots(&conn)?
        .into_iter()
        .filter(|root| root.status == "active" && root.path.exists())
        .collect::<Vec<_>>();
    if active_roots.is_empty() {
        bail!("no active roots are available to watch");
    }
    let fanotify = FanotifyFd::new()?;
    let mut watched = 0usize;
    for root in &active_roots {
        let watch_dirs = match watchable_directories(root) {
            Ok(watch_dirs) => watch_dirs,
            Err(err) => {
                record_event(
                    paths,
                    "watch-root-error",
                    &format!("{} {}: {err:#}", root.id, root.path.display()),
                )?;
                continue;
            }
        };
        for dir in &watch_dirs {
            match fanotify.mark_dir(dir) {
                Ok(()) => watched += 1,
                Err(err) => record_event(
                    paths,
                    "watch-dir-error",
                    &format!("{} {}: fanotify {err:#}", root.id, dir.display()),
                )?,
            }
        }
        record_event(
            paths,
            "watch-root",
            &format!("{} {} backend=fanotify", root.id, root.path.display()),
        )?;
    }
    if watched == 0 {
        bail!("no active roots could be watched by fanotify");
    }
    write_active_watch_backend(paths, "fanotify")?;
    record_event(
        paths,
        "watch-start",
        &format!(
            "backend=fanotify mode={} debounce_ms={} settle_ms={} buffer_max_ms={} buffer_max_events={} periodic_rescan_secs={} max_rss_mib={}",
            args.mode,
            args.debounce_ms,
            args.settle_ms,
            args.buffer_max_ms,
            args.buffer_max_events,
            args.periodic_rescan_secs,
            args.max_rss_mib
        ),
    )?;
    record_health(paths);
    enforce_watch_memory_guard(paths, args.max_rss_mib)?;
    let replayed_journal = crate::watch_recovery::replay(paths, args.once)?;
    sync_current_external(paths, true, &[])?;
    if replayed_journal {
        record_health(paths);
        enforce_watch_memory_guard(paths, args.max_rss_mib)?;
        if args.once {
            record_event(
                paths,
                "watch-stop",
                "foreground fanotify watch stopped after journal replay",
            )?;
            return Ok(());
        }
    }
    loop {
        enforce_watch_memory_guard(paths, args.max_rss_mib)?;
        let Some(event) = fanotify.recv(crate::watch_recovery::wait_secs(
            paths,
            args.periodic_rescan_secs,
        )?)?
        else {
            notify_stalled_pending_journal(paths)?;
            record_event(
                paths,
                "periodic-rescan",
                &format!("interval_secs={}", args.periodic_rescan_secs),
            )?;
            snapshot_and_maybe_sync(
                paths,
                SnapshotArgs {
                    message: Some("watch periodic rescan".into()),
                    changed_roots: Vec::new(),
                    origin: fallback_watch_origin("watch:fanotify", Some("fanotify")),
                },
                args.once,
            )?;
            record_health(paths);
            enforce_watch_memory_guard(paths, args.max_rss_mib)?;
            if args.once {
                break;
            }
            continue;
        };
        if !fanotify_event_relevant_for_roots(&active_roots, &event) {
            continue;
        }
        let changed_roots = fanotify_event_changed_roots(&active_roots, &event);
        record_fanotify_event(paths, &active_roots, &event)?;
        let origin = event.origin();
        if args.mode == "strict" {
            snapshot_and_maybe_sync(
                paths,
                SnapshotArgs {
                    message: Some("watch strict event snapshot".into()),
                    changed_roots: changed_roots_vec(changed_roots),
                    origin,
                },
                args.once,
            )?;
            if args.once {
                break;
            }
            continue;
        }
        let buffer = FanotifyBufferConfig {
            quiet: Duration::from_millis(args.debounce_ms.saturating_add(args.settle_ms).max(1)),
            max_latency: Duration::from_millis(args.buffer_max_ms.max(1)),
            max_events: args.buffer_max_events.max(1),
        };
        let outcome = fanotify.drain_buffer(paths, buffer, origin, &active_roots, changed_roots)?;
        record_event(
            paths,
            "watch-buffer-flush",
            &format!(
                "reason={} events={} elapsed_ms={} origin_pid={}",
                outcome.reason,
                outcome.events,
                outcome.elapsed_ms,
                outcome
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.process_id)
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "(none)".into())
            ),
        )?;
        snapshot_and_maybe_sync(
            paths,
            SnapshotArgs {
                message: Some("watch event snapshot".into()),
                changed_roots: changed_roots_vec(outcome.changed_roots),
                origin: outcome.origin,
            },
            args.once,
        )?;
        record_health(paths);
        enforce_watch_memory_guard(paths, args.max_rss_mib)?;
        if args.once {
            break;
        }
    }
    record_event(paths, "watch-stop", "foreground fanotify watch stopped")?;
    Ok(())
}

fn resolve_watch_args(args: WatchArgs, config: &WatchConfig) -> Result<ResolvedWatchArgs> {
    let mode = args.mode.unwrap_or_else(|| config.mode.clone());
    validate_watch_mode(&mode)?;
    Ok(ResolvedWatchArgs {
        foreground: args.foreground,
        mode,
        interval_secs: args.interval_secs.unwrap_or(config.interval),
        debounce_ms: args.debounce_ms.unwrap_or(config.debounce),
        settle_ms: args.settle_ms.unwrap_or(config.settle),
        buffer_max_ms: args.buffer_max_ms.unwrap_or(config.buffer_max),
        buffer_max_events: args.buffer_max_events.unwrap_or(config.buffer_max_events),
        periodic_rescan_secs: args.periodic_rescan_secs.unwrap_or(config.periodic_rescan),
        max_rss_mib: args.max_rss_mib.unwrap_or_else(default_watch_max_rss_mib),
        backend: args.backend.unwrap_or_else(|| config.backend.clone()),
        once: args.once,
    })
}

fn watch_poll(paths: &Paths, args: &ResolvedWatchArgs) -> Result<()> {
    write_active_watch_backend(paths, "poll")?;
    record_event(
        paths,
        "watch-start",
        &format!(
            "backend=poll mode={} interval_secs={} max_rss_mib={}",
            args.mode, args.interval_secs, args.max_rss_mib
        ),
    )?;
    loop {
        enforce_watch_memory_guard(paths, args.max_rss_mib)?;
        snapshot_and_maybe_sync(
            paths,
            SnapshotArgs {
                message: Some("watch snapshot".into()),
                changed_roots: Vec::new(),
                origin: None,
            },
            args.once,
        )?;
        record_health(paths);
        enforce_watch_memory_guard(paths, args.max_rss_mib)?;
        if args.once {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(
            crate::watch_recovery::wait_secs(paths, args.interval_secs.max(1))?,
        ));
    }
    record_event(paths, "watch-stop", "foreground watch stopped")?;
    Ok(())
}

fn watch_notify(paths: &Paths, args: ResolvedWatchArgs, backend_label: &str) -> Result<()> {
    let conn = open_db(paths)?;
    let active_roots = roots(&conn)?
        .into_iter()
        .filter(|root| root.status == "active" && root.path.exists())
        .collect::<Vec<_>>();
    if active_roots.is_empty() {
        bail!("no active roots are available to watch");
    }
    let (tx, rx) = mpsc::channel();
    #[cfg(target_os = "linux")]
    if backend_label == "inotify" {
        let watcher = notify::INotifyWatcher::new(
            move |res| {
                let _ = tx.send(res);
            },
            NotifyConfig::default(),
        )?;
        write_active_watch_backend(paths, backend_label)?;
        record_watch_start(paths, &args, backend_label)?;
        return watch_notify_loop(paths, args, backend_label, active_roots, watcher, rx);
    }
    let watcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        NotifyConfig::default(),
    )?;
    write_active_watch_backend(paths, backend_label)?;
    record_watch_start(paths, &args, backend_label)?;
    watch_notify_loop(paths, args, backend_label, active_roots, watcher, rx)
}

fn record_watch_start(paths: &Paths, args: &ResolvedWatchArgs, backend_label: &str) -> Result<()> {
    record_event(
        paths,
        "watch-start",
        &format!(
            "backend={} mode={} debounce_ms={} settle_ms={} buffer_max_ms={} buffer_max_events={} periodic_rescan_secs={} max_rss_mib={}",
            backend_label,
            args.mode,
            args.debounce_ms,
            args.settle_ms,
            args.buffer_max_ms,
            args.buffer_max_events,
            args.periodic_rescan_secs,
            args.max_rss_mib
        ),
    )
}

#[cfg(target_os = "linux")]
const FAN_CLOSE_WRITE_MASK: u64 = 0x00000008;
#[cfg(target_os = "linux")]
const FAN_MODIFY_MASK: u64 = 0x00000002;
#[cfg(target_os = "linux")]
const FAN_EVENT_ON_CHILD_MASK: u64 = 0x08000000;

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
struct FanotifyEventMetadata {
    event_len: u32,
    vers: u8,
    reserved: u8,
    metadata_len: u16,
    mask: u64,
    fd: i32,
    pid: i32,
}

#[cfg(target_os = "linux")]
struct FanotifyFd {
    fd: RawFd,
}

#[cfg(target_os = "linux")]
impl FanotifyFd {
    fn new() -> Result<Self> {
        let fd = unsafe {
            libc::fanotify_init(
                libc::FAN_CLASS_NOTIF | libc::FAN_CLOEXEC | libc::FAN_NONBLOCK,
                (libc::O_RDONLY | libc::O_CLOEXEC) as u32,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).context("fanotify_init");
        }
        Ok(Self { fd })
    }

    fn mark_dir(&self, path: &Path) -> Result<()> {
        let path = CString::new(path.as_os_str().as_bytes())
            .with_context(|| format!("fanotify path contains NUL: {}", path.display()))?;
        let mask = FAN_CLOSE_WRITE_MASK | FAN_MODIFY_MASK | FAN_EVENT_ON_CHILD_MASK;
        let rc = unsafe {
            libc::fanotify_mark(
                self.fd,
                libc::FAN_MARK_ADD | libc::FAN_MARK_ONLYDIR | libc::FAN_MARK_DONT_FOLLOW,
                mask,
                libc::AT_FDCWD,
                path.as_ptr(),
            )
        };
        if rc < 0 {
            return Err(std::io::Error::last_os_error()).context("fanotify_mark");
        }
        Ok(())
    }

    fn recv(&self, periodic_rescan_secs: u64) -> Result<Option<FanotifyObservedEvent>> {
        let timeout_ms = if periodic_rescan_secs == 0 {
            -1
        } else {
            periodic_rescan_secs
                .saturating_mul(1000)
                .min(i32::MAX as u64) as i32
        };
        let mut pollfd = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        if rc < 0 {
            return Err(std::io::Error::last_os_error()).context("poll fanotify fd");
        }
        if rc == 0 {
            return Ok(None);
        }
        self.read_one()
    }

    fn read_one(&self) -> Result<Option<FanotifyObservedEvent>> {
        let mut buffer = [0u8; 8192];
        let n = unsafe {
            libc::read(
                self.fd,
                buffer.as_mut_ptr().cast::<libc::c_void>(),
                buffer.len(),
            )
        };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(err).context("read fanotify event");
        }
        let mut offset = 0usize;
        let n = n as usize;
        let mut observed = None;
        while offset + mem::size_of::<FanotifyEventMetadata>() <= n {
            let metadata = unsafe {
                buffer
                    .as_ptr()
                    .add(offset)
                    .cast::<FanotifyEventMetadata>()
                    .read_unaligned()
            };
            if metadata.event_len == 0 {
                break;
            }
            let event_path = (metadata.fd >= 0)
                .then(|| fanotify_path_from_fd(metadata.fd))
                .flatten();
            if metadata.fd >= 0 {
                unsafe {
                    libc::close(metadata.fd);
                }
            }
            offset = offset.saturating_add(metadata.event_len as usize);
            if metadata.pid > 0 && (metadata.mask & (FAN_CLOSE_WRITE_MASK | FAN_MODIFY_MASK)) != 0 {
                observed = Some(FanotifyObservedEvent {
                    pid: metadata.pid as u32,
                    mask: metadata.mask,
                    path: event_path,
                });
            }
        }
        Ok(observed)
    }

    fn drain_buffer(
        &self,
        paths: &Paths,
        config: FanotifyBufferConfig,
        mut origin: Option<OperationOriginOverride>,
        active_roots: &[RootConfig],
        mut changed_roots: Option<BTreeSet<String>>,
    ) -> Result<FanotifyBufferOutcome> {
        let started = Instant::now();
        let mut last_event = started;
        let mut events = 1usize;
        loop {
            if events >= config.max_events {
                return Ok(FanotifyBufferOutcome {
                    reason: "max-events",
                    events,
                    elapsed_ms: started.elapsed().as_millis(),
                    origin,
                    changed_roots,
                });
            }
            let elapsed = started.elapsed();
            if elapsed >= config.max_latency {
                return Ok(FanotifyBufferOutcome {
                    reason: "max-latency",
                    events,
                    elapsed_ms: elapsed.as_millis(),
                    origin,
                    changed_roots,
                });
            }
            let quiet_remaining = config
                .quiet
                .checked_sub(last_event.elapsed())
                .unwrap_or(Duration::ZERO);
            if quiet_remaining.is_zero() {
                return Ok(FanotifyBufferOutcome {
                    reason: "quiet",
                    events,
                    elapsed_ms: started.elapsed().as_millis(),
                    origin,
                    changed_roots,
                });
            }
            let timeout = quiet_remaining
                .min(config.max_latency.saturating_sub(elapsed))
                .max(Duration::from_millis(1));
            let mut pollfd = libc::pollfd {
                fd: self.fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let rc = unsafe {
                libc::poll(
                    &mut pollfd,
                    1,
                    timeout.as_millis().min(i32::MAX as u128) as i32,
                )
            };
            if rc < 0 {
                return Err(std::io::Error::last_os_error()).context("poll fanotify buffer");
            }
            if rc == 0 {
                continue;
            }
            if let Some(event) = self.read_one()? {
                if !fanotify_event_relevant_for_roots(active_roots, &event) {
                    continue;
                }
                record_fanotify_event(paths, active_roots, &event)?;
                merge_changed_roots(
                    &mut changed_roots,
                    fanotify_event_changed_roots(active_roots, &event),
                );
                origin = event.origin();
                events += 1;
                last_event = Instant::now();
            }
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for FanotifyFd {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

#[cfg(target_os = "linux")]
struct FanotifyObservedEvent {
    pid: u32,
    mask: u64,
    path: Option<PathBuf>,
}

#[cfg(target_os = "linux")]
impl FanotifyObservedEvent {
    fn origin(&self) -> Option<OperationOriginOverride> {
        Some(origin_override_from_pid(
            self.pid,
            Some("fanotify".into()),
            Some(format!("pid-{}", self.pid)),
            "fanotify",
        ))
    }
}

#[cfg(target_os = "linux")]
struct FanotifyBufferConfig {
    quiet: Duration,
    max_latency: Duration,
    max_events: usize,
}

#[cfg(target_os = "linux")]
struct FanotifyBufferOutcome {
    reason: &'static str,
    events: usize,
    elapsed_ms: u128,
    origin: Option<OperationOriginOverride>,
    changed_roots: Option<BTreeSet<String>>,
}

#[cfg(target_os = "linux")]
fn record_fanotify_event(
    paths: &Paths,
    active_roots: &[RootConfig],
    event: &FanotifyObservedEvent,
) -> Result<()> {
    let detail = format!(
        "fanotify mask=0x{:x} origin_pid={} origin_confidence=fanotify path={}",
        event.mask,
        event.pid,
        event
            .path
            .as_deref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "(unknown)".into())
    );
    if let Some(path) = event.path.as_deref()
        && let Some((root_id, relative_path)) = event_path_for_roots(active_roots, path)
    {
        return record_file_event(
            paths,
            &root_id,
            &relative_path,
            "modify",
            "fanotify",
            &detail,
        );
    }
    record_event(paths, "fs-event", &detail)
}

#[cfg(target_os = "linux")]
fn fanotify_path_from_fd(fd: RawFd) -> Option<PathBuf> {
    fs::read_link(format!("/proc/self/fd/{fd}")).ok()
}

#[cfg(target_os = "linux")]
fn fanotify_event_relevant_for_roots(
    active_roots: &[RootConfig],
    event: &FanotifyObservedEvent,
) -> bool {
    event
        .path
        .as_deref()
        .is_none_or(|path| event_path_for_roots(active_roots, path).is_some())
}

#[cfg(target_os = "linux")]
fn fanotify_event_changed_roots(
    active_roots: &[RootConfig],
    event: &FanotifyObservedEvent,
) -> Option<BTreeSet<String>> {
    let path = event.path.as_deref()?;
    changed_roots_for_paths(active_roots, std::iter::once(path))
}

fn watch_notify_loop<W: Watcher>(
    paths: &Paths,
    args: ResolvedWatchArgs,
    backend_label: &str,
    active_roots: Vec<RootConfig>,
    mut watcher: W,
    rx: mpsc::Receiver<notify::Result<notify::Event>>,
) -> Result<()> {
    let mut watched_roots = Vec::new();
    for root in &active_roots {
        let watch_dirs = match watchable_directories(root) {
            Ok(watch_dirs) => watch_dirs,
            Err(err) => {
                record_event(
                    paths,
                    "watch-root-error",
                    &format!("{} {}: {err:#}", root.id, root.path.display()),
                )?;
                continue;
            }
        };
        let mut watched = 0usize;
        for dir in &watch_dirs {
            match watcher.watch(dir, RecursiveMode::NonRecursive) {
                Ok(()) => watched += 1,
                Err(err) => record_event(
                    paths,
                    "watch-dir-error",
                    &format!("{} {}: {err}", root.id, dir.display()),
                )?,
            }
        }
        if watched > 0 {
            record_event(
                paths,
                "watch-root",
                &format!("{} {} dirs={}", root.id, root.path.display(), watched),
            )?;
            watched_roots.push(root.clone());
        }
    }
    if watched_roots.is_empty() {
        bail!("no active roots could be watched");
    }
    record_health(paths);
    enforce_watch_memory_guard(paths, args.max_rss_mib)?;
    let replayed_journal = crate::watch_recovery::replay(paths, args.once)?;
    sync_current_external(paths, true, &[])?;
    if replayed_journal {
        record_health(paths);
        enforce_watch_memory_guard(paths, args.max_rss_mib)?;
        if args.once {
            record_event(
                paths,
                "watch-stop",
                &format!("foreground {backend_label} watch stopped after journal replay"),
            )?;
            return Ok(());
        }
    }
    loop {
        enforce_watch_memory_guard(paths, args.max_rss_mib)?;
        let event = match recv_watch_event(
            &rx,
            crate::watch_recovery::wait_secs(paths, args.periodic_rescan_secs)?,
        ) {
            Ok(Some(event)) => event,
            Ok(None) => {
                notify_stalled_pending_journal(paths)?;
                record_event(
                    paths,
                    "periodic-rescan",
                    &format!("interval_secs={}", args.periodic_rescan_secs),
                )?;
                snapshot_and_maybe_sync(
                    paths,
                    SnapshotArgs {
                        message: Some("watch periodic rescan".into()),
                        changed_roots: Vec::new(),
                        origin: fallback_watch_origin(
                            &format!("watch:{backend_label}"),
                            Some(backend_label),
                        ),
                    },
                    args.once,
                )?;
                record_health(paths);
                enforce_watch_memory_guard(paths, args.max_rss_mib)?;
                if args.once {
                    break;
                }
                continue;
            }
            Err(err) => {
                if is_watch_channel_disconnected(&err) {
                    return Err(err);
                }
                record_watch_error(paths, backend_label, &err)?;
                continue;
            }
        };
        if !snapshot_relevant_event(&event) || !event_relevant_for_roots(&watched_roots, &event) {
            continue;
        }
        let changed_roots = notify_event_changed_roots(&watched_roots, &event);
        record_notify_event(paths, backend_label, &watched_roots, &event)?;
        if args.mode == "strict" {
            snapshot_and_maybe_sync(
                paths,
                SnapshotArgs {
                    message: Some("watch strict event snapshot".into()),
                    changed_roots: changed_roots_vec(changed_roots),
                    origin: fallback_watch_origin(
                        &format!("watch:{backend_label}"),
                        Some(backend_label),
                    ),
                },
                args.once,
            )?;
            if args.once {
                break;
            }
            continue;
        }
        let buffer = WatchEventBufferConfig {
            quiet: Duration::from_millis(args.debounce_ms.saturating_add(args.settle_ms).max(1)),
            settle: Duration::ZERO,
            max_latency: Duration::from_millis(args.buffer_max_ms.max(1)),
            max_events: args.buffer_max_events.max(1),
            max_rss_mib: args.max_rss_mib,
        };
        let outcome = drain_watch_event_buffer(
            paths,
            &rx,
            buffer,
            backend_label,
            &watched_roots,
            changed_roots,
        )?;
        record_event(
            paths,
            "watch-buffer-flush",
            &format!(
                "reason={} events={} elapsed_ms={}",
                outcome.reason, outcome.events, outcome.elapsed_ms
            ),
        )?;
        snapshot_and_maybe_sync(
            paths,
            SnapshotArgs {
                message: Some("watch event snapshot".into()),
                changed_roots: changed_roots_vec(outcome.changed_roots),
                origin: fallback_watch_origin(
                    &format!("watch:{backend_label}"),
                    Some(backend_label),
                ),
            },
            args.once,
        )?;
        record_health(paths);
        enforce_watch_memory_guard(paths, args.max_rss_mib)?;
        if args.once {
            break;
        }
    }
    record_event(
        paths,
        "watch-stop",
        &format!("foreground {backend_label} watch stopped"),
    )?;
    Ok(())
}

fn record_health(paths: &Paths) {
    if let Err(err) = ensure_daemon_ipc(paths) {
        let _ = record_event(paths, "daemon-ipc-recover-error", &format!("{err:#}"));
    }
    if let Err(err) = refresh_runtime_health(paths) {
        let _ = record_event(paths, "health-record-error", &format!("{err:#}"));
    }
}

fn notify_stalled_pending_journal(paths: &Paths) -> Result<()> {
    let Some(command) = std::env::var("MAJUTSU_STALLED_NOTICE_CMD")
        .ok()
        .filter(|command| !command.trim().is_empty())
    else {
        return Ok(());
    };
    let threshold_secs = env_u64("MAJUTSU_STALLED_NOTICE_AFTER_SECS", 300);
    let rate_limit_secs = env_u64("MAJUTSU_STALLED_NOTICE_RATE_LIMIT_SECS", 3600);
    let Some((pending, oldest_age_secs)) = pending_journal_summary(paths)? else {
        return Ok(());
    };
    if oldest_age_secs < threshold_secs || notice_recently_sent(paths, rate_limit_secs) {
        return Ok(());
    }
    let status = crate::platform_runtime::shell_command(&command)
        .env("MAJUTSU_HOME", &paths.home)
        .env("MAJUTSU_PENDING_JOURNAL_COUNT", pending.to_string())
        .env(
            "MAJUTSU_PENDING_OLDEST_AGE_SECS",
            oldest_age_secs.to_string(),
        )
        .status();
    match status {
        Ok(status) if status.success() => {
            mark_notice_sent(paths)?;
            record_event(
                paths,
                "watch-stalled-notice",
                &format!("pending={pending} oldest_age_secs={oldest_age_secs}"),
            )?;
        }
        Ok(status) => record_event(
            paths,
            "watch-stalled-notice-error",
            &format!("notice command exited with status {status}"),
        )?,
        Err(err) => record_event(
            paths,
            "watch-stalled-notice-error",
            &format!("notice command failed: {err}"),
        )?,
    }
    Ok(())
}

fn pending_journal_summary(paths: &Paths) -> Result<Option<(usize, u64)>> {
    let records = event_journal_records(paths)?;
    let last_snapshot_finish = records
        .iter()
        .filter(|event| event.is_snapshot_finish())
        .map(|event| event.observed_at)
        .max();
    let pending = records
        .iter()
        .filter(|event| {
            event.is_pending_trigger()
                && event.remote_journal_synced_at.is_none()
                && last_snapshot_finish
                    .map(|finished_at| event.observed_at > finished_at)
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return Ok(None);
    }
    let oldest_age_secs = pending
        .iter()
        .map(|event| Utc::now().signed_duration_since(event.observed_at))
        .filter_map(|duration| u64::try_from(duration.num_seconds().max(0)).ok())
        .max()
        .unwrap_or(0);
    Ok(Some((pending.len(), oldest_age_secs)))
}

fn notice_recently_sent(paths: &Paths, rate_limit_secs: u64) -> bool {
    let Ok(metadata) = notice_marker_path(paths).metadata() else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified)
        .map(|age| age.as_secs() < rate_limit_secs)
        .unwrap_or(false)
}

fn mark_notice_sent(paths: &Paths) -> Result<()> {
    let marker = notice_marker_path(paths);
    if let Some(parent) = marker.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(marker, Utc::now().to_rfc3339())?;
    Ok(())
}

fn notice_marker_path(paths: &Paths) -> PathBuf {
    paths.runtime.join("stalled-notice.sent")
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn watchable_directories(root: &RootConfig) -> Result<Vec<PathBuf>> {
    let ignore = build_ignore(root)?;
    let walker = WalkDir::new(&root.path)
        .follow_links(false)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| {
            if entry.path() == root.path {
                return true;
            }
            let Ok(rel) = entry.path().strip_prefix(&root.path) else {
                return true;
            };
            !entry.file_type().is_dir() || root_dir_allows_descend(root, &ignore, rel)
        });
    let mut dirs = Vec::new();
    for entry in walker {
        let entry = entry.with_context(|| format!("walk watch root {}", root.path.display()))?;
        if entry.file_type().is_dir() {
            dirs.push(entry.path().to_path_buf());
        }
    }
    Ok(dirs)
}

fn snapshot_and_maybe_sync(paths: &Paths, mut args: SnapshotArgs, once: bool) -> Result<()> {
    crate::watch_recovery::run(paths, once, |full_rescan| {
        if full_rescan {
            args.changed_roots.clear();
        }
        capture_snapshot_and_sync(paths, args)
    })
}

fn capture_snapshot_and_sync(paths: &Paths, args: SnapshotArgs) -> Result<bool> {
    let changed_roots = args.changed_roots.clone();
    sync_current_external(paths, false, &changed_roots)?;
    let before_snapshot = local_current_snapshot(paths)?;
    if std::env::var("MAJUTSU_WATCH_INLINE_SNAPSHOT").as_deref() == Ok("1") {
        snapshot(paths, args)?;
        let after_snapshot = local_current_snapshot(paths)?;
        if before_snapshot == after_snapshot {
            record_watch_sync_skipped(paths, after_snapshot.as_deref())?;
            return Ok(true);
        }
        let sync_result = {
            let _slot = watch_sync_slot()?;
            sync_current_if_remote(paths)
        };
        match sync_result {
            Ok(AutoSyncResult::Synced) => {
                clear_cluster_failure(paths, false);
                record_event(paths, "watch-sync", "synced current snapshot to remote")?;
                auto_prune_after_sync(paths)?;
            }
            Ok(AutoSyncResult::Deferred {
                delayed,
                next_retry_after,
            }) => record_event(
                paths,
                "watch-sync-deferred",
                &format!(
                    "delayed_uploads={} next_retry_after={}",
                    delayed,
                    next_retry_after.unwrap_or_else(|| "(unknown)".into())
                ),
            )?,
            Ok(AutoSyncResult::NoRemote) => {}
            Err(err) => record_event(paths, "watch-sync-error", &format!("{err:#}"))?,
        }
        return Ok(true);
    }

    let origin = args
        .origin
        .or_else(|| fallback_watch_origin("watch", Some("watch")));
    let message = args.message.unwrap_or_else(|| "watch snapshot".into());
    let exe = child_process_exe()?;
    let mut command = std::process::Command::new(&exe);
    command
        .arg("--home")
        .arg(&paths.home)
        .arg("snapshot")
        .arg("--message")
        .arg(&message);
    for root_id in &changed_roots {
        command.arg("--changed-root").arg(root_id);
    }
    if let Some(origin) = &origin {
        if let Some(label) = &origin.label {
            command.env("MAJUTSU_ORIGIN_LABEL", label);
        }
        if let Some(session_id) = &origin.session_id {
            command.env("MAJUTSU_ORIGIN_SESSION_ID", session_id);
        }
        if let Some(pid) = origin.process_id {
            command.env("MAJUTSU_ORIGIN_PID", pid.to_string());
        }
        if let Some(exe) = &origin.exe {
            command.env("MAJUTSU_ORIGIN_EXE", exe);
        }
        if let Some(confidence) = &origin.confidence {
            command.env("MAJUTSU_ORIGIN_CONFIDENCE", confidence);
        }
    }
    if matches!(
        run_snapshot_child_with_retry(paths, &mut command)?,
        SnapshotChildOutcome::Deferred
    ) {
        return Ok(false);
    }
    record_event(paths, "watch-snapshot-child", &message)?;

    let after_snapshot = local_current_snapshot(paths)?;
    if before_snapshot == after_snapshot {
        record_watch_sync_skipped(paths, after_snapshot.as_deref())?;
        return Ok(true);
    }
    sync_current_external(paths, true, &changed_roots)?;
    Ok(true)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotChildOutcome {
    Succeeded,
    Deferred,
}

const SNAPSHOT_DB_RETRY_DELAYS_MS: &[u64] = &[100, 250, 500, 1_000, 2_000, 4_000];

fn run_snapshot_child_with_retry(
    paths: &Paths,
    command: &mut std::process::Command,
) -> Result<SnapshotChildOutcome> {
    for (attempt, delay_ms) in SNAPSHOT_DB_RETRY_DELAYS_MS
        .iter()
        .copied()
        .map(Some)
        .chain(std::iter::once(None))
        .enumerate()
    {
        let output = command.output()?;
        if output.status.success() {
            return Ok(SnapshotChildOutcome::Succeeded);
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stderr.trim().replace('\n', " ");
        if detail.contains("snapshot already running with pid")
            || detail.contains("maintenance already running with pid")
        {
            record_event(paths, "watch-snapshot-deferred", &detail)?;
            return Ok(SnapshotChildOutcome::Deferred);
        }
        if !is_database_busy_error(&detail) {
            bail!(
                "watch snapshot child process failed with status {}: {}",
                output.status,
                detail
            );
        }
        let Some(delay_ms) = delay_ms else {
            record_event(
                paths,
                "watch-snapshot-deferred",
                &format!(
                    "reason=database-busy attempts={} error={detail}",
                    attempt + 1
                ),
            )?;
            return Ok(SnapshotChildOutcome::Deferred);
        };
        record_event(
            paths,
            "watch-snapshot-retry",
            &format!(
                "reason=database-busy attempt={} delay_ms={} error={detail}",
                attempt + 1,
                delay_ms
            ),
        )?;
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
    }
    unreachable!("snapshot retry loop must return after the final attempt")
}

fn is_database_busy_error(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("database is locked")
        || detail.contains("database is busy")
        || detail.contains("database busy")
}

fn local_current_snapshot(paths: &Paths) -> Result<Option<String>> {
    let conn = open_db(paths)?;
    current_snapshot(&conn)
}

fn record_watch_sync_skipped(paths: &Paths, current: Option<&str>) -> Result<()> {
    record_event(
        paths,
        "watch-sync-skipped",
        &format!("snapshot unchanged current={}", current.unwrap_or("(none)")),
    )
}

fn sync_current_external(paths: &Paths, prune_after: bool, changed_roots: &[String]) -> Result<()> {
    if read_config(paths)?.remote.is_none() {
        return Ok(());
    }
    let slot = watch_sync_slot()?;
    let upload_stats = upload_queue_stats(paths)?;
    if upload_stats.delayed > 0 {
        record_event(
            paths,
            "watch-sync-deferred",
            &format!(
                "delayed_uploads={} next_retry_after={}",
                upload_stats.delayed,
                upload_stats
                    .next_retry_after
                    .map(|retry_after| retry_after.to_rfc3339())
                    .unwrap_or_else(|| "(unknown)".into())
            ),
        )?;
        return Ok(());
    }
    let exe = child_process_exe()?;
    let mut command = std::process::Command::new(&exe);
    command.arg("--home").arg(&paths.home).arg("sync");
    configure_watch_sync_wait(&mut command, configured_remote_is_cluster(paths));
    for root_id in changed_roots {
        command.arg("--changed-root").arg(root_id);
    }
    let status = command.status()?;
    drop(slot);
    if status.success() {
        clear_cluster_failure(paths, false);
        record_event(paths, "watch-sync", "external sync completed")?;
        if prune_after {
            auto_prune_after_sync(paths)?;
        }
    } else {
        record_event(
            paths,
            "watch-sync-error",
            &format!("external sync exited with status {status}"),
        )?;
    }
    Ok(())
}

fn configure_watch_sync_wait(command: &mut std::process::Command, cluster_remote: bool) {
    if !cluster_remote {
        command.arg("--wait").arg("--timeout-secs").arg("300");
    }
}

fn start_cluster_heartbeat_scheduler(paths: &Paths) -> Result<()> {
    let paths = paths.clone();
    std::thread::Builder::new()
        .name("mj-cluster-heartbeat".into())
        .spawn(move || cluster_heartbeat_scheduler_loop(paths))
        .context("start cluster heartbeat scheduler")?;
    Ok(())
}

fn cluster_heartbeat_scheduler_loop(paths: Paths) {
    let heartbeat_interval = cluster_heartbeat_interval();
    loop {
        match cluster_slave_heartbeat(&paths) {
            Ok(_) => {
                clear_cluster_master_unavailable(&paths);
            }
            Err(err) => {
                eprintln!("cluster master heartbeat error: {err:#}");
                let _ = record_cluster_master_unavailable(&paths, &err);
            }
        }
        std::thread::sleep(heartbeat_interval);
    }
}

fn start_upload_retry_scheduler(paths: &Paths) -> Result<()> {
    let paths = paths.clone();
    std::thread::Builder::new()
        .name("mj-upload-retry".into())
        .spawn(move || upload_retry_scheduler_loop(paths))
        .context("start upload retry scheduler")?;
    Ok(())
}

fn upload_retry_scheduler_loop(paths: Paths) {
    loop {
        let work_sleep = match upload_retry_scheduler_work(&paths) {
            Ok(work) if work.is_empty() => Duration::from_secs(30),
            Ok(_)
                if process_lock_owner(&paths.sync_lock)
                    .ok()
                    .flatten()
                    .is_some() =>
            {
                Duration::from_secs(5)
            }
            Ok(work) if work.only_delayed_uploads() => work
                .uploads
                .next_retry_after
                .and_then(|retry_after| {
                    (retry_after - Utc::now())
                        .to_std()
                        .ok()
                        .map(|duration| duration.max(Duration::from_secs(1)))
                })
                .unwrap_or_else(|| Duration::from_secs(5))
                .min(Duration::from_secs(30)),
            Ok(work) => {
                let pending_before = work.pending_items();
                match retry_queued_uploads_external(&paths) {
                    Err(err) => {
                        eprintln!("upload retry scheduler error: {err:#}");
                        let _ = record_retry_sync_failure(&paths, &err);
                        Duration::from_secs(30)
                    }
                    Ok(RetrySyncOutcome::Deferred) => Duration::from_secs(5),
                    Ok(RetrySyncOutcome::Completed) => {
                        clear_cluster_master_unavailable(&paths);
                        clear_cluster_failure(&paths, false);
                        let pending_after = upload_retry_scheduler_work(&paths)
                            .map(|work| work.pending_items())
                            .unwrap_or(pending_before);
                        retry_scheduler_success_delay(pending_before, pending_after)
                    }
                }
            }
            Err(err) => {
                eprintln!("upload retry scheduler queue inspection failed: {err:#}");
                Duration::from_secs(30)
            }
        };
        std::thread::sleep(work_sleep);
    }
}

#[derive(Debug)]
struct UploadRetrySchedulerWork {
    uploads: UploadQueueStats,
    durable_journal_pending: usize,
}

impl UploadRetrySchedulerWork {
    fn is_empty(&self) -> bool {
        self.uploads.total == 0 && self.durable_journal_pending == 0
    }

    fn only_delayed_uploads(&self) -> bool {
        self.durable_journal_pending == 0
            && self.uploads.total > 0
            && self.uploads.delayed >= self.uploads.total
    }

    fn pending_items(&self) -> usize {
        self.uploads
            .total
            .saturating_add(self.durable_journal_pending)
    }
}

fn upload_retry_scheduler_work(paths: &Paths) -> Result<UploadRetrySchedulerWork> {
    Ok(UploadRetrySchedulerWork {
        uploads: upload_queue_stats(paths)?,
        durable_journal_pending: remote_event_journal_stats(paths)?.pending,
    })
}

fn retry_scheduler_success_delay(pending_before: usize, pending_after: usize) -> Duration {
    if pending_after < pending_before {
        Duration::from_secs(1)
    } else {
        Duration::from_secs(30)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetrySyncOutcome {
    Completed,
    Deferred,
}

fn retry_sync_if_needed(
    paths: &Paths,
    slot: &Mutex<()>,
    sync: impl FnOnce() -> Result<RetrySyncOutcome>,
) -> Result<RetrySyncOutcome> {
    let _slot = match slot.try_lock() {
        Ok(slot) => slot,
        Err(TryLockError::WouldBlock) => return Ok(RetrySyncOutcome::Deferred),
        Err(TryLockError::Poisoned(_)) => bail!("watch同期の排他状態が破損しています"),
    };
    // schedulerの最初の検査以降に、watchや手動syncが作業を引き受ける場合がある。
    if process_lock_owner(&paths.sync_lock)?.is_some()
        || process_lock_owner(&maintenance_lock_path(paths))?.is_some()
    {
        return Ok(RetrySyncOutcome::Deferred);
    }
    let work = upload_retry_scheduler_work(paths)?;
    if work.is_empty() || work.only_delayed_uploads() {
        return Ok(RetrySyncOutcome::Deferred);
    }
    sync()
}

fn retry_queued_uploads_external(paths: &Paths) -> Result<RetrySyncOutcome> {
    retry_sync_if_needed(paths, &WATCH_SYNC_SLOT, || {
        let exe = child_process_exe()?;
        let mut command = std::process::Command::new(exe);
        command
            .arg("--home")
            .arg(&paths.home)
            .arg("sync")
            .arg("--retry-queued-only");
        run_retry_sync_child(paths, &mut command)
    })
}

fn run_retry_sync_child(
    paths: &Paths,
    command: &mut std::process::Command,
) -> Result<RetrySyncOutcome> {
    let output = command.stdout(std::process::Stdio::inherit()).output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if output.status.success() {
        eprint!("{stderr}");
        return Ok(RetrySyncOutcome::Completed);
    }
    // 事前確認後に別プロセスがlockを取得する競合も、通信障害と区別する。
    let detail = stderr.trim();
    let lock_error = detail.strip_prefix("Error: ").unwrap_or(detail);
    if lock_error.starts_with("maintenance already running with pid ")
        || lock_error.starts_with("sync already running with pid ")
    {
        record_event(paths, "watch-retry-sync-deferred", detail)?;
        return Ok(RetrySyncOutcome::Deferred);
    }
    bail!(
        "retry sync exited with status {}: {}",
        output.status,
        detail
    )
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ClusterUnavailableState {
    first_failure_at: chrono::DateTime<Utc>,
    last_failure_at: chrono::DateTime<Utc>,
    failures: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_notice_at: Option<chrono::DateTime<Utc>>,
}

fn configured_remote_is_cluster(paths: &Paths) -> bool {
    read_config(paths)
        .ok()
        .and_then(|config| config.remote)
        .and_then(|remote| remote.url().ok())
        .is_some_and(|url| url.starts_with("cluster://"))
}

fn cluster_unavailable_state_path(paths: &Paths) -> PathBuf {
    paths.runtime.join("cluster-master-unavailable.json")
}

static CLUSTER_NOTICE_STATE_LOCK: Mutex<()> = Mutex::new(());

fn cluster_failure_path(paths: &Paths, master: bool) -> PathBuf {
    if master {
        cluster_unavailable_state_path(paths)
    } else {
        paths.runtime.join("cluster-sync-failure.json")
    }
}
fn record_cluster_master_unavailable(paths: &Paths, error: &anyhow::Error) -> Result<()> {
    record_cluster_failure(paths, error, true)
}
fn record_retry_sync_failure(paths: &Paths, error: &anyhow::Error) -> Result<()> {
    if !configured_remote_is_cluster(paths) {
        return Ok(());
    }
    // 同期失敗の内容だけからmaster停止と判断せず、独立した到達確認を行う。
    match cluster_slave_heartbeat(paths) {
        Ok(_) => {
            clear_cluster_master_unavailable(paths);
            record_cluster_failure(paths, error, false)
        }
        Err(unreachable) => record_cluster_master_unavailable(paths, &unreachable),
    }
}
fn append_notice_audit(paths: &Paths, issue: &str, result: &str) -> Result<()> {
    let path = paths.runtime.join("cluster-notice-history.json");
    let mut history: Vec<serde_json::Value> = match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("通知履歴の形式が不正です"))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e.into()),
    };
    history
        .push(serde_json::json!({"observed_at":Utc::now(), "issue_code":issue, "result":result}));
    if history.len() > 64 {
        history.drain(..history.len() - 64);
    }
    fs::create_dir_all(&paths.runtime)?;
    crate::atomic_io::write_atomic(&path, &serde_json::to_vec(&history)?)
}
fn clear_cluster_failure(paths: &Paths, master: bool) {
    let Ok(_guard) = CLUSTER_NOTICE_STATE_LOCK.lock() else {
        return;
    };
    let path = cluster_failure_path(paths, master);
    if path.exists() {
        if append_notice_audit(
            paths,
            if master {
                "master-unavailable"
            } else {
                "sync-failed"
            },
            "recovered",
        )
        .is_err()
        {
            eprintln!("cluster notice recovery audit failed");
        }
        let _ = fs::remove_file(path);
    }
}
fn clear_cluster_master_unavailable(paths: &Paths) {
    clear_cluster_failure(paths, true);
}
fn cluster_failure_issue(error: &anyhow::Error, master: bool) -> String {
    if master {
        "master-unavailable".into()
    } else {
        crate::hydration_error::code(&format!("{error:#}")).map_or_else(
            || "sync-failed".into(),
            |code| format!("sync-object-fetch-{code}"),
        )
    }
}

fn record_cluster_failure(paths: &Paths, error: &anyhow::Error, master: bool) -> Result<()> {
    let _guard = CLUSTER_NOTICE_STATE_LOCK
        .lock()
        .map_err(|_| anyhow::anyhow!("通知状態の排他が破損しています"))?;
    if !configured_remote_is_cluster(paths) {
        return Ok(());
    }
    let path = cluster_failure_path(paths, master);
    let now = Utc::now();
    let mut state = fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<ClusterUnavailableState>(&bytes).ok())
        .unwrap_or(ClusterUnavailableState {
            first_failure_at: now,
            last_failure_at: now,
            failures: 0,
            last_notice_at: None,
        });
    state.last_failure_at = now;
    state.failures = state.failures.saturating_add(1);
    let minimum_failures = env_u64("MAJUTSU_CLUSTER_UNAVAILABLE_NOTICE_FAILURES", 3);
    let minimum_age = env_u64("MAJUTSU_CLUSTER_UNAVAILABLE_NOTICE_AFTER_SECS", 300) as i64;
    let outage_age = now
        .signed_duration_since(state.first_failure_at)
        .num_seconds()
        .max(0);
    let rate_limit = env_u64("MAJUTSU_CLUSTER_NOTICE_RATE_LIMIT_SECS", 3600) as i64;
    let rate_limited = state
        .last_notice_at
        .is_some_and(|last| now.signed_duration_since(last).num_seconds() < rate_limit);
    let should_notice =
        state.failures >= minimum_failures && outage_age >= minimum_age && !rate_limited;
    let issue = cluster_failure_issue(error, master);
    if should_notice
        && let Some(command) = std::env::var("MAJUTSU_CLUSTER_NOTICE_CMD")
            .ok()
            .filter(|command| !command.trim().is_empty())
    {
        let mut notice = crate::platform_runtime::shell_command(&command);
        notice
            .env("MAJUTSU_HOME", &paths.home)
            .env("MAJUTSU_CLUSTER_NOTICE_ISSUES", &issue)
            .env(
                "MAJUTSU_CLUSTER_UNAVAILABLE_FAILURES",
                state.failures.to_string(),
            )
            .env(
                "MAJUTSU_CLUSTER_UNAVAILABLE_AGE_SECS",
                outage_age.to_string(),
            )
            .env("MAJUTSU_CLUSTER_LAST_ERROR", &issue);
        let status = crate::platform_runtime::command_status_with_timeout(
            &mut notice,
            Duration::from_secs(10),
        );
        // 通知受理後の監査書込み失敗で再送を繰り返さない。
        if status.as_ref().is_ok_and(|s| s.success()) {
            state.last_notice_at = Some(now);
            crate::atomic_io::write_atomic(&path, &serde_json::to_vec_pretty(&state)?)?;
        }
        if append_notice_audit(
            paths,
            &issue,
            if status.as_ref().is_ok_and(|s| s.success()) {
                "delivered"
            } else {
                "failed"
            },
        )
        .is_err()
        {
            eprintln!("cluster notice audit failed");
        }
        match status {
            Ok(status) if status.success() => {
                state.last_notice_at = Some(now);
                record_event(
                    paths,
                    "cluster-failure-notice",
                    &format!(
                        "issue={issue} failures={} age_secs={outage_age}",
                        state.failures
                    ),
                )?;
            }
            Ok(status) => record_event(
                paths,
                "cluster-failure-notice-error",
                &format!("notice command exited with status {status}"),
            )?,
            Err(err) => record_event(
                paths,
                "cluster-failure-notice-error",
                &format!("notice command failed: {err}"),
            )?,
        }
    }
    crate::atomic_io::write_atomic(&path, &serde_json::to_vec_pretty(&state)?)
}

fn auto_prune_after_sync(paths: &Paths) -> Result<()> {
    if std::env::var("MAJUTSU_WATCH_AUTO_PRUNE").as_deref() == Ok("0") {
        return Ok(());
    }
    let prune_interval = watch_prune_interval();
    if let Some(interval) = prune_interval
        && !watch_prune_due(paths, interval)?
    {
        record_event(
            paths,
            "watch-prune-deferred",
            &format!("interval_secs={} reason=rate-limit", interval.as_secs()),
        )?;
        return Ok(());
    }
    let keep_daily = std::env::var("MAJUTSU_WATCH_PRUNE_KEEP_DAILY").unwrap_or_else(|_| "0".into());
    let keep_monthly =
        std::env::var("MAJUTSU_WATCH_PRUNE_KEEP_MONTHLY").unwrap_or_else(|_| "0".into());
    let exe = child_process_exe()?;
    let prune_args = auto_prune_args(paths, &keep_daily, &keep_monthly);

    // テストや明示的な互換運用では従来どおり完了を待てるようにする。
    // 通常のdaemonでは保守処理をwatchの同期hot pathから切り離す。
    if std::env::var("MAJUTSU_WATCH_PRUNE_ASYNC").as_deref() == Ok("0") {
        let status = std::process::Command::new(&exe).args(prune_args).status()?;
        if status.success() {
            record_watch_prune_completed(paths, &keep_daily, &keep_monthly, prune_interval)?;
        } else {
            record_event(
                paths,
                "watch-prune-error",
                &format!("auto prune exited with status {status}"),
            )?;
        }
        return Ok(());
    }

    let _schedule_lock = match acquire_process_lock(
        &paths.home.join("locks/watch-prune-schedule.lock"),
        "watch prune scheduling",
    ) {
        Ok(lock) => lock,
        Err(error) if error.to_string().contains("already running") => {
            record_event(paths, "watch-prune-deferred", "reason=schedule-lock-busy")?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    if watch_prune_job_running(paths)? {
        record_event(paths, "watch-prune-deferred", "reason=job-running")?;
        return Ok(());
    }

    let mut command = std::process::Command::new(&exe);
    command.args(prune_args);
    configure_background_command(&mut command);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            record_event(
                paths,
                "watch-prune-deferred",
                &format!("reason=spawn-failed error={error}"),
            )?;
            return Ok(());
        }
    };
    let job_path = watch_prune_job_path(paths);
    let job = WatchPruneJob {
        version: 1,
        identity: process_identity(child.id()),
        started_at_unix_secs: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };
    if let Err(error) = write_watch_prune_job(paths, &job) {
        let _ = child.kill();
        return Err(error);
    }

    let worker_paths = paths.clone();
    let worker_job_path = job_path.clone();
    let worker_keep_daily = keep_daily.clone();
    let worker_keep_monthly = keep_monthly.clone();
    let worker_prune_interval = prune_interval;
    std::thread::spawn(move || {
        let result = child.wait();
        let _ = fs::remove_file(&worker_job_path);
        match result {
            Ok(status) if status.success() => {
                let _ = record_watch_prune_completed(
                    &worker_paths,
                    &worker_keep_daily,
                    &worker_keep_monthly,
                    worker_prune_interval,
                );
            }
            Ok(status) => {
                let _ = record_event(
                    &worker_paths,
                    "watch-prune-error",
                    &format!("auto prune exited with status {status}"),
                );
            }
            Err(error) => {
                let _ = record_event(
                    &worker_paths,
                    "watch-prune-error",
                    &format!("wait for auto prune failed: {error}"),
                );
            }
        }
    });
    record_event(
        paths,
        "watch-prune-started",
        &format!(
            "auto prune started keep_daily={keep_daily} keep_monthly={keep_monthly} drop_missing_remote_history=false interval_secs={}",
            prune_interval.map_or(0, |interval| interval.as_secs())
        ),
    )?;
    Ok(())
}

fn record_watch_prune_completed(
    paths: &Paths,
    keep_daily: &str,
    keep_monthly: &str,
    prune_interval: Option<Duration>,
) -> Result<()> {
    record_event(
        paths,
        "watch-prune",
        &format!(
            "auto prune completed keep_daily={keep_daily} keep_monthly={keep_monthly} drop_missing_remote_history=false interval_secs={}",
            prune_interval.map_or(0, |interval| interval.as_secs())
        ),
    )?;
    if prune_interval.is_some() {
        write_watch_prune_state(paths)?;
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WatchPruneState {
    version: u32,
    pruned_at_unix_secs: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WatchPruneJob {
    version: u32,
    identity: ProcessIdentity,
    started_at_unix_secs: u64,
}

fn watch_prune_interval() -> Option<Duration> {
    std::env::var("MAJUTSU_WATCH_PRUNE_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
}

fn watch_prune_state_path(paths: &Paths) -> PathBuf {
    paths.home.join("cache/watch-prune-state.json")
}

fn watch_prune_job_path(paths: &Paths) -> PathBuf {
    paths.runtime.join("watch-prune-job.json")
}

fn watch_prune_job_running(paths: &Paths) -> Result<bool> {
    let path = watch_prune_job_path(paths);
    let Some(job) = fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<WatchPruneJob>(&bytes).ok())
    else {
        // 旧形式、破損、または完了後に残ったmarkerは次の起動で回収する。
        let _ = fs::remove_file(path);
        return Ok(false);
    };
    if job.version != 1 {
        let _ = fs::remove_file(path);
        return Ok(false);
    }
    let actual = process_identity(job.identity.pid);
    let matches = process_identity_matches(&job.identity, &actual)
        || (job.identity.start_token.is_none()
            && job.identity.executable.is_none()
            && pid_alive(job.identity.pid));
    if matches {
        return Ok(true);
    }
    let _ = fs::remove_file(path);
    Ok(false)
}

fn write_watch_prune_job(paths: &Paths, job: &WatchPruneJob) -> Result<()> {
    if let Some(parent) = watch_prune_job_path(paths).parent() {
        fs::create_dir_all(parent)?;
    }
    crate::atomic_io::write_atomic(&watch_prune_job_path(paths), &serde_json::to_vec(job)?)
}

fn watch_prune_due(paths: &Paths, interval: Duration) -> Result<bool> {
    let path = watch_prune_state_path(paths);
    if !path.exists() {
        return Ok(true);
    }
    let state: WatchPruneState = match fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    {
        Some(state) => state,
        None => return Ok(true),
    };
    if state.version != 1 {
        return Ok(true);
    }
    let now = SystemTime::now();
    let last_pruned = SystemTime::UNIX_EPOCH + Duration::from_secs(state.pruned_at_unix_secs);
    Ok(now.duration_since(last_pruned).unwrap_or(Duration::MAX) >= interval)
}

fn write_watch_prune_state(paths: &Paths) -> Result<()> {
    let path = watch_prune_state_path(paths);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let state = WatchPruneState {
        version: 1,
        pruned_at_unix_secs: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };
    crate::atomic_io::write_atomic(&path, &serde_json::to_vec(&state)?)
}

fn auto_prune_args(paths: &Paths, keep_daily: &str, keep_monthly: &str) -> Vec<OsString> {
    vec![
        "--home".into(),
        paths.home.as_os_str().to_owned(),
        "prune".into(),
        "--dry-run=false".into(),
        "--keep-daily".into(),
        keep_daily.into(),
        "--keep-monthly".into(),
        keep_monthly.into(),
    ]
}

fn fallback_watch_origin(label: &str, confidence: Option<&str>) -> Option<OperationOriginOverride> {
    Some(OperationOriginOverride {
        label: Some(label.into()),
        session_id: Some(format!("daemon-pid-{}", std::process::id())),
        process_id: None,
        process_path: None,
        exe: None,
        confidence: confidence.map(str::to_string),
    })
}

pub fn recv_watch_event(
    rx: &mpsc::Receiver<notify::Result<notify::Event>>,
    periodic_rescan_secs: u64,
) -> Result<Option<notify::Event>> {
    if periodic_rescan_secs == 0 {
        return rx.recv()?.map(Some).map_err(Into::into);
    }
    match rx.recv_timeout(Duration::from_secs(periodic_rescan_secs)) {
        Ok(event) => event.map(Some).map_err(Into::into),
        Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
        Err(mpsc::RecvTimeoutError::Disconnected) => bail!("watch channel disconnected"),
    }
}

pub fn format_notify_event(event: &notify::Event) -> String {
    format!("{} {}", notify_event_kind(event), notify_event_paths(event))
}

fn notify_event_kind(event: &notify::Event) -> &'static str {
    match &event.kind {
        EventKind::Create(_) => "create",
        EventKind::Modify(_) => "modify",
        EventKind::Remove(_) => "remove",
        EventKind::Access(_) => "access",
        EventKind::Other => "other",
        _ => "unknown",
    }
}

fn snapshot_relevant_event(event: &notify::Event) -> bool {
    !matches!(event.kind, EventKind::Access(_))
        && event
            .paths
            .iter()
            .any(|path| !is_transient_watch_path(path))
}

fn is_transient_watch_path(path: &Path) -> bool {
    path.components()
        .collect::<Vec<_>>()
        .windows(2)
        .any(|window| {
            window[0].as_os_str() == ".git"
                && window[1].as_os_str().to_string_lossy().ends_with(".lock")
        })
}

fn notify_event_paths(event: &notify::Event) -> String {
    event
        .paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn record_notify_event(
    paths: &Paths,
    backend_label: &str,
    active_roots: &[RootConfig],
    event: &notify::Event,
) -> Result<()> {
    if !snapshot_relevant_event(event) {
        return Ok(());
    }
    let detail = format_notify_event(event);
    let event_kind = notify_event_kind(event);
    let mut recorded = false;
    for path in &event.paths {
        if let Some((root_id, relative_path)) = event_path_for_roots(active_roots, path) {
            record_file_event(
                paths,
                &root_id,
                &relative_path,
                event_kind,
                backend_label,
                &detail,
            )?;
            recorded = true;
        }
    }
    if !recorded {
        record_event(paths, "fs-event", &detail)?;
    }
    Ok(())
}

fn event_relevant_for_roots(active_roots: &[RootConfig], event: &notify::Event) -> bool {
    if active_roots.is_empty() {
        return true;
    }
    event
        .paths
        .iter()
        .any(|path| event_path_for_roots(active_roots, path).is_some())
}

fn notify_event_changed_roots(
    active_roots: &[RootConfig],
    event: &notify::Event,
) -> Option<BTreeSet<String>> {
    changed_roots_for_paths(active_roots, event.paths.iter().map(PathBuf::as_path))
}

fn changed_roots_for_paths<'a>(
    active_roots: &[RootConfig],
    paths: impl IntoIterator<Item = &'a Path>,
) -> Option<BTreeSet<String>> {
    if active_roots.is_empty() {
        return None;
    }
    let mut changed = BTreeSet::new();
    for path in paths {
        let matching_roots = event_root_ids_for_path(active_roots, path);
        if matching_roots.is_empty() {
            // pathをrootへ確実に対応付けられない場合は安全側で全rootを走査する。
            return None;
        }
        changed.extend(matching_roots);
    }
    (!changed.is_empty()).then_some(changed)
}

fn event_root_ids_for_path(active_roots: &[RootConfig], path: &Path) -> BTreeSet<String> {
    active_roots
        .iter()
        .filter_map(|root| {
            let relative = path.strip_prefix(&root.path).ok()?;
            if !relative.as_os_str().is_empty()
                && root_ignores_relative_path(root, relative, path.is_dir())
            {
                return None;
            }
            Some(root.id.clone())
        })
        .collect()
}

fn merge_changed_roots(changed: &mut Option<BTreeSet<String>>, next: Option<BTreeSet<String>>) {
    let (Some(changed), Some(next)) = (changed.as_mut(), next) else {
        *changed = None;
        return;
    };
    changed.extend(next);
}

fn changed_roots_vec(changed: Option<BTreeSet<String>>) -> Vec<String> {
    changed.map_or_else(Vec::new, |roots| roots.into_iter().collect())
}

fn event_path_for_roots(active_roots: &[RootConfig], path: &Path) -> Option<(String, String)> {
    active_roots
        .iter()
        .filter_map(|root| {
            path.strip_prefix(&root.path)
                .ok()
                .map(|relative| (root, relative))
        })
        .filter_map(|(root, relative)| {
            if relative.as_os_str().is_empty() {
                return Some((root, relative));
            }
            if root_ignores_relative_path(root, relative, path.is_dir()) {
                return None;
            }
            Some((root, relative))
        })
        .max_by_key(|(root, _)| root.path.components().count())
        .map(|(root, relative)| {
            let rel = if relative.as_os_str().is_empty() {
                ".".into()
            } else {
                slash_path(relative)
            };
            (root.id.clone(), rel)
        })
}

fn root_ignores_relative_path(root: &RootConfig, relative: &Path, is_dir: bool) -> bool {
    if explicitly_untracked(root, relative) {
        return true;
    }
    if !volatile_allows_watch_snapshot(root, relative) {
        return true;
    }
    build_ignore(root)
        .map(|ignore| is_ignored(&ignore, relative, is_dir))
        .unwrap_or(false)
}

fn slash_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[derive(Debug, Clone, Copy)]
struct WatchEventBufferConfig {
    quiet: Duration,
    settle: Duration,
    max_latency: Duration,
    max_events: usize,
    max_rss_mib: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WatchEventBufferOutcome {
    reason: &'static str,
    events: usize,
    elapsed_ms: u128,
    changed_roots: Option<BTreeSet<String>>,
}

fn drain_watch_event_buffer(
    paths: &Paths,
    rx: &mpsc::Receiver<notify::Result<notify::Event>>,
    config: WatchEventBufferConfig,
    backend_label: &str,
    active_roots: &[RootConfig],
    mut changed_roots: Option<BTreeSet<String>>,
) -> Result<WatchEventBufferOutcome> {
    let started = Instant::now();
    let mut last_event = started;
    let mut events = 1usize;
    let context = WatchEventBufferContext {
        paths,
        rx,
        config,
        backend_label,
        active_roots,
    };
    loop {
        enforce_watch_memory_guard(paths, config.max_rss_mib)?;
        if events >= config.max_events {
            return settle_before_flush(&context, started, events, "max-events", changed_roots);
        }
        let elapsed = started.elapsed();
        if elapsed >= config.max_latency {
            return settle_before_flush(&context, started, events, "max-latency", changed_roots);
        }
        let quiet_remaining = config
            .quiet
            .checked_sub(last_event.elapsed())
            .unwrap_or(Duration::ZERO);
        if quiet_remaining.is_zero() {
            return settle_before_flush(&context, started, events, "quiet", changed_roots);
        }
        let max_remaining = config.max_latency.saturating_sub(elapsed);
        let timeout = quiet_remaining
            .min(max_remaining)
            .max(Duration::from_millis(1));
        match rx.recv_timeout(timeout) {
            Ok(Ok(next)) => {
                if !snapshot_relevant_event(&next) || !event_relevant_for_roots(active_roots, &next)
                {
                    continue;
                }
                record_notify_event(paths, backend_label, active_roots, &next)?;
                merge_changed_roots(
                    &mut changed_roots,
                    notify_event_changed_roots(active_roots, &next),
                );
                events += 1;
                last_event = Instant::now();
            }
            Ok(Err(err)) => {
                record_watch_error(paths, backend_label, &err)?;
                continue;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let reason = if started.elapsed() >= config.max_latency {
                    "max-latency"
                } else {
                    "quiet"
                };
                return settle_before_flush(&context, started, events, reason, changed_roots);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => bail!("watch channel disconnected"),
        }
    }
}

struct WatchEventBufferContext<'a> {
    paths: &'a Paths,
    rx: &'a mpsc::Receiver<notify::Result<notify::Event>>,
    config: WatchEventBufferConfig,
    backend_label: &'a str,
    active_roots: &'a [RootConfig],
}

fn settle_before_flush(
    context: &WatchEventBufferContext<'_>,
    started: Instant,
    mut events: usize,
    reason: &'static str,
    mut changed_roots: Option<BTreeSet<String>>,
) -> Result<WatchEventBufferOutcome> {
    let paths = context.paths;
    let rx = context.rx;
    let config = context.config;
    let backend_label = context.backend_label;
    let active_roots = context.active_roots;
    if !config.settle.is_zero() && reason == "quiet" {
        record_event(
            paths,
            "watch-settle",
            &format!("settle_ms={}", config.settle.as_millis()),
        )?;
        loop {
            enforce_watch_memory_guard(paths, config.max_rss_mib)?;
            if events >= config.max_events {
                return Ok(buffer_outcome(started, events, "max-events", changed_roots));
            }
            if started.elapsed() >= config.max_latency {
                return Ok(buffer_outcome(
                    started,
                    events,
                    "max-latency",
                    changed_roots,
                ));
            }
            let timeout = config
                .settle
                .min(config.max_latency.saturating_sub(started.elapsed()))
                .max(Duration::from_millis(1));
            match rx.recv_timeout(timeout) {
                Ok(Ok(next)) => {
                    if !snapshot_relevant_event(&next)
                        || !event_relevant_for_roots(active_roots, &next)
                    {
                        continue;
                    }
                    record_notify_event(paths, backend_label, active_roots, &next)?;
                    merge_changed_roots(
                        &mut changed_roots,
                        notify_event_changed_roots(active_roots, &next),
                    );
                    events += 1;
                }
                Ok(Err(err)) => {
                    record_watch_error(paths, backend_label, &err)?;
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => bail!("watch channel disconnected"),
            }
        }
    }
    Ok(buffer_outcome(started, events, reason, changed_roots))
}

fn buffer_outcome(
    started: Instant,
    events: usize,
    reason: &'static str,
    changed_roots: Option<BTreeSet<String>>,
) -> WatchEventBufferOutcome {
    WatchEventBufferOutcome {
        reason,
        events,
        elapsed_ms: started.elapsed().as_millis(),
        changed_roots,
    }
}

fn enforce_watch_memory_guard(paths: &Paths, max_rss_mib: u64) -> Result<()> {
    if max_rss_mib == 0 {
        return Ok(());
    }
    let rss_kib = self_process_rss_kib().unwrap_or(0);
    if rss_kib == 0 {
        return Ok(());
    }
    let limit_kib = max_rss_mib.saturating_mul(1024);
    if rss_kib <= limit_kib {
        return Ok(());
    }
    record_event(
        paths,
        "watch-memory-limit",
        &format!("rss_kib={rss_kib} max_rss_mib={max_rss_mib}"),
    )?;
    bail!("watch daemon RSS exceeded limit: {rss_kib} KiB > {limit_kib} KiB");
}

#[cfg(target_os = "linux")]
fn self_process_rss_kib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS")?.trim_start();
        let rest = rest.strip_prefix(':')?.trim_start();
        rest.split_whitespace().next()?.parse().ok()
    })
}

#[cfg(target_os = "macos")]
fn self_process_rss_kib() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // macOSのru_maxrssはbyte単位。現在値が取れない場合も上限超過を安全側で検出する。
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return None;
    }
    let bytes = unsafe { usage.assume_init() }.ru_maxrss;
    u64::try_from(bytes).ok().map(|value| value / 1024)
}

#[cfg(windows)]
fn self_process_rss_kib() -> Option<u64> {
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let size = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
    let mut counters = PROCESS_MEMORY_COUNTERS {
        cb: size,
        ..Default::default()
    };
    if unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, size) } == 0 {
        return None;
    }
    u64::try_from(counters.WorkingSetSize / 1024).ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn self_process_rss_kib() -> Option<u64> {
    None
}

fn record_watch_error(
    paths: &Paths,
    backend_label: &str,
    err: &dyn std::fmt::Display,
) -> Result<()> {
    record_event(
        paths,
        "watch-error",
        &format!("backend={backend_label}: {err}"),
    )
}

fn is_watch_channel_disconnected(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    message.contains("watch channel disconnected")
        || message.contains("receiving on a closed channel")
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::{Event, EventKind};
    use std::path::PathBuf;
    use std::thread;

    #[test]
    fn notice_classifies_fetch_failure_and_bounds_history() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = test_paths(tmp.path().join("home"));
        let error = crate::hydration_error::failure("objects/trees/aa/bb", "missing");
        assert_eq!(
            cluster_failure_issue(&error, false),
            "sync-object-fetch-missing"
        );
        assert_eq!(cluster_failure_issue(&error, true), "master-unavailable");
        for _ in 0..70 {
            append_notice_audit(&paths, "sync-object-fetch-missing", "failed").unwrap();
        }
        let path = paths.runtime.join("cluster-notice-history.json");
        let entries: Vec<serde_json::Value> =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(entries.len(), 64);
        fs::write(cluster_failure_path(&paths, false), b"{}").unwrap();
        clear_cluster_failure(&paths, false);
        assert!(!cluster_failure_path(&paths, false).exists());
        let entries: Vec<serde_json::Value> =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(entries.len(), 64);
        assert_eq!(entries.last().unwrap()["result"], "recovered");
        assert!(entries.iter().all(|v| v.as_object().unwrap().len() == 3));
    }

    #[test]
    fn formats_notify_event_kind_and_paths() {
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
            paths: vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")],
            attrs: Default::default(),
        };

        assert_eq!(format_notify_event(&event), "modify /tmp/a,/tmp/b");
    }

    #[test]
    fn normalizes_watch_backend_for_cli() {
        assert_eq!(normalize_watch_backend("notify").unwrap(), "notify");
        assert_eq!(normalize_watch_backend("poll").unwrap(), "poll");
    }

    #[test]
    fn ignores_git_transient_lock_events() {
        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![PathBuf::from("/repo/.git/index.lock")],
            attrs: Default::default(),
        };

        assert!(!snapshot_relevant_event(&event));
    }

    #[test]
    fn keeps_git_index_events_relevant() {
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Any,
            )),
            paths: vec![PathBuf::from("/repo/.git/index")],
            attrs: Default::default(),
        };

        assert!(snapshot_relevant_event(&event));
    }

    #[test]
    fn watchable_directories_skip_excluded_subtrees() {
        let temp = tempfile::tempdir().unwrap();
        let root_path = temp.path().join("root");
        std::fs::create_dir_all(root_path.join(".devdata/postgres")).unwrap();
        std::fs::create_dir_all(root_path.join("src")).unwrap();
        let root = test_root(root_path.clone(), vec![".devdata/**".into()]);

        let dirs = watchable_directories(&root).unwrap();

        assert!(dirs.contains(&root_path));
        assert!(dirs.contains(&root_path.join("src")));
        assert!(!dirs.contains(&root_path.join(".devdata")));
        assert!(!dirs.contains(&root_path.join(".devdata/postgres")));
    }

    #[test]
    fn excluded_events_do_not_trigger_snapshots() {
        let temp = tempfile::tempdir().unwrap();
        let root_path = temp.path().join("root");
        std::fs::create_dir_all(root_path.join(".devdata/postgres")).unwrap();
        std::fs::create_dir_all(root_path.join("src")).unwrap();
        let root = test_root(root_path.clone(), vec![".devdata/**".into()]);

        assert!(!event_relevant_for_roots(
            std::slice::from_ref(&root),
            &test_event_abs(root_path.join(".devdata/postgres/base"))
        ));
        assert!(event_relevant_for_roots(
            &[root],
            &test_event_abs(root_path.join("src/main.rs"))
        ));
    }

    #[test]
    fn notify_events_identify_only_the_changed_roots() {
        let temp = tempfile::tempdir().unwrap();
        let root_a_path = temp.path().join("a");
        let root_b_path = temp.path().join("b");
        std::fs::create_dir_all(&root_a_path).unwrap();
        std::fs::create_dir_all(&root_b_path).unwrap();
        let root_a = test_root(root_a_path.clone(), Vec::new());
        let mut root_b = test_root(root_b_path.clone(), Vec::new());
        root_b.id = "root-b".into();
        let event = test_event_abs(root_b_path.join("note.txt"));

        let changed = notify_event_changed_roots(&[root_a, root_b], &event).unwrap();

        assert_eq!(changed, BTreeSet::from(["root-b".to_string()]));
    }

    #[test]
    fn notify_events_update_every_overlapping_root() {
        let temp = tempfile::tempdir().unwrap();
        let parent_path = temp.path().join("parent");
        let child_path = parent_path.join("child");
        std::fs::create_dir_all(&child_path).unwrap();
        let mut parent = test_root(parent_path, Vec::new());
        parent.id = "parent".into();
        let mut child = test_root(child_path.clone(), Vec::new());
        child.id = "child".into();
        let event = test_event_abs(child_path.join("note.txt"));

        let changed = notify_event_changed_roots(&[parent, child], &event).unwrap();

        assert_eq!(
            changed,
            BTreeSet::from(["child".to_string(), "parent".to_string()])
        );
    }

    #[test]
    fn volatile_checkpoint_events_do_not_trigger_snapshots() {
        let temp = tempfile::tempdir().unwrap();
        let root_path = temp.path().join("root");
        std::fs::create_dir_all(root_path.join("data")).unwrap();
        let mut root = test_root(root_path.clone(), Vec::new());
        root.volatile = Some(crate::config::RootVolatileConfig {
            patterns: vec!["**/*.sqlite-wal".into()],
            mode: "checkpoint".into(),
        });

        assert!(!event_relevant_for_roots(
            std::slice::from_ref(&root),
            &test_event_abs(root_path.join("data/app.sqlite-wal"))
        ));
        assert!(event_relevant_for_roots(
            &[root],
            &test_event_abs(root_path.join("data/app.sqlite"))
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fanotify_uses_event_path_to_skip_excluded_files() {
        use std::os::fd::AsRawFd;

        let temp = tempfile::tempdir().unwrap();
        let root_path = temp.path().join("root");
        std::fs::create_dir_all(&root_path).unwrap();
        let excluded = root_path.join("runtime.log");
        std::fs::write(&excluded, b"ignored").unwrap();
        let file = std::fs::File::open(&excluded).unwrap();
        let event = FanotifyObservedEvent {
            pid: 1,
            mask: FAN_CLOSE_WRITE_MASK,
            path: fanotify_path_from_fd(file.as_raw_fd()),
        };
        let root = test_root(root_path.clone(), vec!["*.log".into()]);

        assert!(!fanotify_event_relevant_for_roots(&[root], &event));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fanotify_unknown_path_is_fail_open() {
        let temp = tempfile::tempdir().unwrap();
        let root_path = temp.path().join("root");
        std::fs::create_dir_all(&root_path).unwrap();
        let root = test_root(root_path, vec!["*.log".into()]);
        let event = FanotifyObservedEvent {
            pid: 1,
            mask: FAN_CLOSE_WRITE_MASK,
            path: None,
        };

        assert!(fanotify_event_relevant_for_roots(&[root], &event));
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_child_retries_database_lock_before_succeeding() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        let attempts = temp.path().join("attempts");
        let mut command = std::process::Command::new("sh");
        command
            .arg("-c")
            .arg(
                r#"count=$(cat "$1" 2>/dev/null || echo 0); count=$((count + 1)); printf '%s' "$count" > "$1"; if [ "$count" -lt 3 ]; then echo 'database is locked' >&2; exit 1; fi"#,
            )
            .arg("mj-test")
            .arg(&attempts);

        let outcome = run_snapshot_child_with_retry(&paths, &mut command).unwrap();

        assert_eq!(outcome, SnapshotChildOutcome::Succeeded);
        assert_eq!(fs::read_to_string(attempts).unwrap(), "3");
        let events = fs::read_dir(paths.event_queue)
            .unwrap()
            .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(events.contains("watch-snapshot-retry"), "{events}");
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_child_defers_when_maintenance_lock_is_held() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        let mut command = std::process::Command::new("sh");
        command
            .arg("-c")
            .arg("echo 'maintenance already running with pid 42' >&2; exit 1")
            .arg("mj-test");

        let outcome = run_snapshot_child_with_retry(&paths, &mut command).unwrap();

        assert_eq!(outcome, SnapshotChildOutcome::Deferred);
        let events = fs::read_dir(paths.event_queue)
            .unwrap()
            .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(events.contains("watch-snapshot-deferred"), "{events}");
    }

    #[test]
    fn volatile_exclude_subtrees_are_not_watched() {
        let temp = tempfile::tempdir().unwrap();
        let root_path = temp.path().join("root");
        std::fs::create_dir_all(root_path.join("runtime/cache")).unwrap();
        std::fs::create_dir_all(root_path.join("src")).unwrap();
        let mut root = test_root(root_path.clone(), Vec::new());
        root.volatile = Some(crate::config::RootVolatileConfig {
            patterns: vec!["runtime/**".into()],
            mode: "exclude".into(),
        });

        let dirs = watchable_directories(&root).unwrap();

        assert!(dirs.contains(&root_path));
        assert!(dirs.contains(&root_path.join("src")));
        assert!(!dirs.contains(&root_path.join("runtime")));
        assert!(!dirs.contains(&root_path.join("runtime/cache")));
    }

    #[test]
    fn event_buffer_flushes_after_sliding_quiet_window() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        let (tx, rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        tx.send(Ok(test_event("a.txt"))).unwrap();
        let sender = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            tx.send(Ok(test_event("b.txt"))).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
        });

        let outcome = drain_watch_event_buffer(
            &paths,
            &rx,
            WatchEventBufferConfig {
                quiet: Duration::from_millis(250),
                settle: Duration::ZERO,
                max_latency: Duration::from_millis(1_000),
                max_events: 100,
                max_rss_mib: 0,
            },
            "test",
            &[],
            None,
        )
        .unwrap();
        release_tx.send(()).unwrap();
        sender.join().unwrap();

        assert_eq!(outcome.reason, "quiet");
        assert_eq!(outcome.events, 3);
        assert!(
            outcome.elapsed_ms >= 250,
            "quiet window should slide after later events: {outcome:?}"
        );
    }

    #[test]
    fn event_buffer_flushes_at_max_events() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        let (tx, rx) = mpsc::channel();
        tx.send(Ok(test_event("a.txt"))).unwrap();

        let outcome = drain_watch_event_buffer(
            &paths,
            &rx,
            WatchEventBufferConfig {
                quiet: Duration::from_secs(1),
                settle: Duration::ZERO,
                max_latency: Duration::from_secs(5),
                max_events: 2,
                max_rss_mib: 0,
            },
            "test",
            &[],
            None,
        )
        .unwrap();

        assert_eq!(outcome.reason, "max-events");
        assert_eq!(outcome.events, 2);
    }

    #[test]
    fn event_buffer_flushes_at_max_latency() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        let (tx, rx) = mpsc::channel();
        let sender = thread::spawn(move || {
            for index in 0..10 {
                thread::sleep(Duration::from_millis(15));
                if tx
                    .send(Ok(test_event(&format!("file-{index}.txt"))))
                    .is_err()
                {
                    break;
                }
            }
        });

        let outcome = drain_watch_event_buffer(
            &paths,
            &rx,
            WatchEventBufferConfig {
                quiet: Duration::from_millis(50),
                settle: Duration::ZERO,
                max_latency: Duration::from_millis(70),
                max_events: 100,
                max_rss_mib: 0,
            },
            "test",
            &[],
            None,
        )
        .unwrap();

        assert_eq!(outcome.reason, "max-latency");
        assert!(outcome.events > 1, "{outcome:?}");
        sender.join().unwrap();
    }

    #[test]
    fn pending_journal_summary_counts_events_after_last_snapshot_finish() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        record_event(&paths, "fs-event", "before finish").unwrap();
        record_event(&paths, "snapshot-finish", "finished").unwrap();
        record_event(&paths, "fs-event", "after finish").unwrap();

        let (pending, oldest_age_secs) = pending_journal_summary(&paths).unwrap().unwrap();

        assert_eq!(pending, 1);
        assert!(oldest_age_secs < 10);
    }

    #[test]
    fn pending_journal_summary_ignores_durable_remote_synced_events() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        record_event(&paths, "snapshot-finish", "finished").unwrap();
        let mut event = crate::majutsu_db::EventJournalRecord::new_file_event(
            "event-synced".into(),
            Utc::now(),
            "synced event".into(),
            "sample".into(),
            "db.sqlite-wal".into(),
            "modify".into(),
            "inotify".into(),
        );
        event.remote_journal_key = Some("host/journal/event-synced.json".into());
        event.remote_journal_synced_at = Some(Utc::now());
        std::fs::create_dir_all(&paths.event_queue).unwrap();
        std::fs::write(
            paths.event_queue.join("event-synced.json"),
            serde_json::to_vec_pretty(&event).unwrap(),
        )
        .unwrap();

        assert!(pending_journal_summary(&paths).unwrap().is_none());
    }

    #[test]
    fn upload_retry_scheduler_treats_remote_journal_as_work_without_uploads() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        record_file_event(
            &paths,
            "sample",
            "pending.txt",
            "modify",
            "test",
            "remote journal retry",
        )
        .unwrap();

        let work = upload_retry_scheduler_work(&paths).unwrap();

        assert_eq!(work.uploads.total, 0);
        assert_eq!(work.durable_journal_pending, 1);
        assert!(!work.is_empty());
        assert!(!work.only_delayed_uploads());
    }

    #[test]
    fn upload_retry_scheduler_backs_off_when_retry_makes_no_progress() {
        assert_eq!(retry_scheduler_success_delay(7, 7), Duration::from_secs(30));
        assert_eq!(retry_scheduler_success_delay(8, 7), Duration::from_secs(1));
    }

    #[test]
    fn retry_sync_defers_while_watch_owns_slot_without_losing_pending_work() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        record_file_event(&paths, "sample", "pending.txt", "modify", "test", "未送信").unwrap();
        let slot = Mutex::new(());
        let watch = slot.lock().unwrap();
        let deferred = thread::scope(|scope| {
            scope
                .spawn(|| retry_sync_if_needed(&paths, &slot, || panic!("競合中に再送を起動した")))
                .join()
                .unwrap()
                .unwrap()
        });
        assert_eq!(deferred, RetrySyncOutcome::Deferred);
        assert_eq!(
            upload_retry_scheduler_work(&paths).unwrap().pending_items(),
            1
        );
        drop(watch);
        let mut attempts = 0;
        assert_eq!(
            retry_sync_if_needed(&paths, &slot, || {
                attempts += 1;
                Ok(RetrySyncOutcome::Completed)
            })
            .unwrap(),
            RetrySyncOutcome::Completed
        );
        assert_eq!(attempts, 1);
    }

    #[test]
    fn retry_sync_rechecks_work_drained_by_watch_and_keeps_outage_marker() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        record_file_event(&paths, "sample", "pending.txt", "modify", "test", "未送信").unwrap();
        assert_eq!(
            upload_retry_scheduler_work(&paths).unwrap().pending_items(),
            1
        );
        let mut event = event_journal_records(&paths).unwrap().remove(0);
        event.remote_journal_key = Some("host/journal/pending.json".into());
        event.remote_journal_synced_at = Some(Utc::now());
        fs::write(
            paths.event_queue.join(format!("{}.json", event.event_id)),
            serde_json::to_vec(&event).unwrap(),
        )
        .unwrap();
        fs::create_dir_all(&paths.runtime).unwrap();
        let outage = cluster_unavailable_state_path(&paths);
        fs::write(&outage, b"outage marker").unwrap();
        assert_eq!(
            retry_sync_if_needed(&paths, &Mutex::new(()), || panic!(
                "処理済みの再送を起動した"
            ))
            .unwrap(),
            RetrySyncOutcome::Deferred
        );
        assert_eq!(fs::read(&outage).unwrap(), b"outage marker");
    }

    #[test]
    fn retry_sync_rechecks_process_lock_and_preserves_real_failure() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        record_file_event(&paths, "sample", "pending.txt", "modify", "test", "未送信").unwrap();
        let slot = Mutex::new(());
        let other_sync = acquire_process_lock(&paths.sync_lock, "sync").unwrap();
        assert_eq!(
            retry_sync_if_needed(&paths, &slot, || panic!("既存syncと競合した")).unwrap(),
            RetrySyncOutcome::Deferred
        );
        drop(other_sync);
        let error = retry_sync_if_needed(&paths, &slot, || bail!("通信失敗"));
        assert_eq!(error.unwrap_err().to_string(), "通信失敗");
        assert!(slot.try_lock().is_ok());
        assert_eq!(
            upload_retry_scheduler_work(&paths).unwrap().pending_items(),
            1
        );
    }

    #[test]
    fn retry_sync_defers_maintenance_without_losing_pending_work() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        record_file_event(&paths, "sample", "pending.txt", "modify", "test", "未送信").unwrap();
        let slot = Mutex::new(());
        let maintenance =
            acquire_process_lock(&maintenance_lock_path(&paths), "maintenance").unwrap();
        assert_eq!(
            retry_sync_if_needed(&paths, &slot, || panic!("保守中に再送を起動した")).unwrap(),
            RetrySyncOutcome::Deferred
        );
        assert_eq!(
            upload_retry_scheduler_work(&paths).unwrap().pending_items(),
            1
        );
        drop(maintenance);
        assert_eq!(
            retry_sync_if_needed(&paths, &slot, || Ok(RetrySyncOutcome::Completed)).unwrap(),
            RetrySyncOutcome::Completed
        );
    }

    #[cfg(unix)]
    #[test]
    fn retry_sync_child_defers_lock_race_and_preserves_network_failure() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        record_file_event(&paths, "sample", "pending.txt", "modify", "test", "未送信").unwrap();
        fs::create_dir_all(&paths.runtime).unwrap();
        let outage = cluster_unavailable_state_path(&paths);
        fs::write(&outage, b"outage marker").unwrap();
        for lock in ["maintenance", "sync"] {
            let mut command = std::process::Command::new("sh");
            command.arg("-c").arg(format!(
                "echo 'Error: {lock} already running with pid 42' >&2; exit 1"
            ));
            assert_eq!(
                run_retry_sync_child(&paths, &mut command).unwrap(),
                RetrySyncOutcome::Deferred
            );
        }
        assert_eq!(fs::read(&outage).unwrap(), b"outage marker");
        assert_eq!(
            upload_retry_scheduler_work(&paths).unwrap().pending_items(),
            1
        );
        let mut failure = std::process::Command::new("sh");
        failure.arg("-c").arg(
            "echo 'Error: connection failed: maintenance already running with pid 42' >&2; exit 1",
        );
        assert!(
            run_retry_sync_child(&paths, &mut failure)
                .unwrap_err()
                .to_string()
                .contains("connection failed")
        );
        let mut success = std::process::Command::new("sh");
        success.arg("-c").arg("exit 0");
        assert_eq!(
            run_retry_sync_child(&paths, &mut success).unwrap(),
            RetrySyncOutcome::Completed
        );
    }

    #[test]
    fn retry_sync_rechecks_backoff_before_spawning() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        crate::queue_runtime::enqueue_inline_upload(&paths, "objects/blobs/test", vec![1]).unwrap();
        let (item_path, mut item) = crate::queue_runtime::upload_queue_items(&paths)
            .unwrap()
            .remove(0);
        item.record_attempt(Some(Utc::now() + chrono::Duration::minutes(5)));
        fs::write(item_path, serde_json::to_vec(&item).unwrap()).unwrap();
        assert_eq!(
            retry_sync_if_needed(&paths, &Mutex::new(()), || panic!("backoff中に再送した"))
                .unwrap(),
            RetrySyncOutcome::Deferred
        );
        // 未ACKのjournalが別にあれば、遅延uploadだけを理由に全体を止めない。
        record_file_event(&paths, "sample", "pending.txt", "modify", "test", "未送信").unwrap();
        assert_eq!(
            retry_sync_if_needed(&paths, &Mutex::new(()), || Ok(RetrySyncOutcome::Completed))
                .unwrap(),
            RetrySyncOutcome::Completed
        );
    }

    #[test]
    fn cluster_watch_sync_does_not_wait_for_permanently_unreadable_journal() {
        let mut command = std::process::Command::new("mj");
        configure_watch_sync_wait(&mut command, true);

        assert_eq!(command.get_args().count(), 0);
    }

    #[test]
    fn standalone_watch_sync_keeps_legacy_wait_behavior() {
        let mut command = std::process::Command::new("mj");
        configure_watch_sync_wait(&mut command, false);
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(args, ["--wait", "--timeout-secs", "300"]);
    }

    #[test]
    fn active_watch_backend_state_survives_event_journal_compaction() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));

        write_active_watch_backend(&paths, "fanotify").unwrap();

        assert_eq!(
            std::fs::read_to_string(paths.runtime.join("watch-backend")).unwrap(),
            "fanotify"
        );
    }

    #[test]
    fn auto_prune_preserves_history_when_remote_catalog_is_incomplete() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        let args = auto_prune_args(&paths, "7", "3");
        let args = args
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>();

        assert!(
            !args
                .iter()
                .any(|arg| arg == "--drop-missing-remote-history")
        );
        assert!(args.windows(2).any(|pair| pair == ["--keep-daily", "7"]));
        assert!(args.windows(2).any(|pair| pair == ["--keep-monthly", "3"]));
    }

    #[test]
    fn watch_prune_state_defers_until_interval_has_elapsed() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        let state_path = watch_prune_state_path(&paths);
        std::fs::create_dir_all(state_path.parent().unwrap()).unwrap();
        write_watch_prune_state(&paths).unwrap();

        assert!(!watch_prune_due(&paths, Duration::from_secs(3600)).unwrap());
        assert!(watch_prune_due(&paths, Duration::from_secs(0)).unwrap());
    }

    #[test]
    fn watch_prune_job_marker_distinguishes_running_and_stale_jobs() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        let running = WatchPruneJob {
            version: 1,
            identity: process_identity(std::process::id()),
            started_at_unix_secs: 0,
        };
        write_watch_prune_job(&paths, &running).unwrap();
        assert!(watch_prune_job_running(&paths).unwrap());

        let stale = WatchPruneJob {
            version: 1,
            identity: ProcessIdentity {
                pid: u32::MAX,
                start_token: Some("stale".into()),
                executable: Some("stale".into()),
            },
            started_at_unix_secs: 0,
        };
        write_watch_prune_job(&paths, &stale).unwrap();
        assert!(!watch_prune_job_running(&paths).unwrap());
        assert!(!watch_prune_job_path(&paths).exists());
    }

    fn test_event(path: &str) -> Event {
        Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
            paths: vec![PathBuf::from(path)],
            attrs: Default::default(),
        }
    }

    fn test_event_abs(path: PathBuf) -> Event {
        Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
            paths: vec![path],
            attrs: Default::default(),
        }
    }

    fn test_root(path: PathBuf, exclude: Vec<String>) -> RootConfig {
        RootConfig {
            id: "sample".into(),
            name: "sample".into(),
            path,
            include: vec!["**".into()],
            exclude,
            explicit_track: Vec::new(),
            explicit_untrack: Vec::new(),
            follow_symlinks: false,
            require_mount: false,
            status: "active".into(),
            degraded: None,
            snapshot_mode: "default".into(),
            pre_snapshot: None,
            post_snapshot: None,
            snapshot_source: None,
            application_plugin: None,
            large: None,
            volatile: None,
        }
    }

    fn test_paths(home: PathBuf) -> Paths {
        Paths {
            db: home.join("db/majutsu.sqlite"),
            config: home.join("config.toml"),
            host: home.join("host"),
            objects: home.join("objects"),
            trees: home.join("trees"),
            large_chunks: home.join("large/chunks"),
            large_manifests: home.join("large/manifests"),
            packs: home.join("packs"),
            pack_indexes: home.join("pack-indexes"),
            logs: home.join("logs"),
            runtime: home.join("runtime"),
            daemon_pid: home.join("runtime/daemon.pid"),
            daemon_lock: home.join("runtime/daemon.lock"),
            snapshot_lock: home.join("runtime/snapshot.lock"),
            sync_lock: home.join("runtime/sync.lock"),
            upload_queue: home.join("queue/uploads"),
            event_queue: home.join("queue/events"),
            master_key: home.join("keys/master"),
            home,
        }
    }
}
