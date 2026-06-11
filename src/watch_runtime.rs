use anyhow::{Result, bail};
use notify::{Config as NotifyConfig, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::cli::{ResolvedWatchArgs, SnapshotArgs, WatchArgs};
use crate::config::{Paths, RootConfig, WatchConfig, read_config, validate_watch_mode};
use crate::daemon_runtime::{start_daemon_ipc, start_watch_daemon};
use crate::process_runtime::acquire_process_lock;
use crate::queue_runtime::{record_event, record_file_event, upload_queue_stats};
use crate::root_state::roots;
use crate::sync_runtime::{AutoSyncResult, sync_current_if_remote};
use crate::{ensure_ready, open_db, replay_pending_journal_events, snapshot};

pub fn normalize_watch_backend(backend: &str) -> Result<&'static str> {
    majutsu_watch::WatchBackend::normalize(backend)
        .map(|backend| backend.as_cli())
        .map_err(anyhow::Error::msg)
}

pub fn default_daemon_backend() -> &'static str {
    majutsu_watch::default_backend()
}

pub fn default_watch_backend() -> String {
    default_daemon_backend().into()
}

pub(crate) fn watch_cmd(paths: &Paths, args: WatchArgs) -> Result<()> {
    ensure_ready(paths)?;
    let config = read_config(paths)?;
    let args = resolve_watch_args(args, &config.watch)?;
    let backend = normalize_watch_backend(&args.backend)?;
    if !args.foreground {
        let pid = start_watch_daemon(
            paths,
            backend,
            &args.mode,
            args.interval_secs,
            args.debounce_ms,
            args.settle_ms,
            args.buffer_max_ms,
            args.buffer_max_events,
            args.periodic_rescan_secs,
        )?;
        println!("started daemon pid {pid}");
        return Ok(());
    }
    let _lock = acquire_process_lock(&paths.daemon_lock, "daemon")?;
    start_daemon_ipc(paths)?;
    match backend {
        "notify" => watch_notify(paths, args, "notify"),
        "inotify" => watch_notify(paths, args, "inotify"),
        "poll" => watch_poll(paths, &args),
        other => bail!("unsupported watch backend: {other}"),
    }
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
        backend: args.backend.unwrap_or_else(|| config.backend.clone()),
        once: args.once,
    })
}

fn watch_poll(paths: &Paths, args: &ResolvedWatchArgs) -> Result<()> {
    record_event(
        paths,
        "watch-start",
        &format!(
            "backend=poll mode={} interval_secs={}",
            args.mode, args.interval_secs
        ),
    )?;
    loop {
        snapshot_and_maybe_sync(
            paths,
            SnapshotArgs {
                message: Some("watch snapshot".into()),
            },
        )?;
        if args.once {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(args.interval_secs.max(1)));
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
    record_event(
        paths,
        "watch-start",
        &format!(
            "backend={} mode={} debounce_ms={} settle_ms={} buffer_max_ms={} buffer_max_events={} periodic_rescan_secs={}",
            backend_label,
            args.mode,
            args.debounce_ms,
            args.settle_ms,
            args.buffer_max_ms,
            args.buffer_max_events,
            args.periodic_rescan_secs
        ),
    )?;
    let (tx, rx) = mpsc::channel();
    #[cfg(target_os = "linux")]
    if backend_label == "inotify" {
        let watcher = notify::INotifyWatcher::new(
            move |res| {
                let _ = tx.send(res);
            },
            NotifyConfig::default(),
        )?;
        return watch_notify_loop(paths, args, backend_label, active_roots, watcher, rx);
    }
    let watcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        NotifyConfig::default(),
    )?;
    watch_notify_loop(paths, args, backend_label, active_roots, watcher, rx)
}

fn watch_notify_loop<W: Watcher>(
    paths: &Paths,
    args: ResolvedWatchArgs,
    backend_label: &str,
    active_roots: Vec<RootConfig>,
    mut watcher: W,
    rx: mpsc::Receiver<notify::Result<notify::Event>>,
) -> Result<()> {
    for root in &active_roots {
        watcher.watch(&root.path, RecursiveMode::Recursive)?;
        record_event(
            paths,
            "watch-root",
            &format!("{} {}", root.id, root.path.display()),
        )?;
    }
    if replay_pending_journal_events(paths)? && args.once {
        record_event(
            paths,
            "watch-stop",
            &format!("foreground {backend_label} watch stopped after journal replay"),
        )?;
        return Ok(());
    }
    loop {
        let event = match recv_watch_event(&rx, args.periodic_rescan_secs)? {
            Some(event) => event,
            None => {
                record_event(
                    paths,
                    "periodic-rescan",
                    &format!("interval_secs={}", args.periodic_rescan_secs),
                )?;
                snapshot_and_maybe_sync(
                    paths,
                    SnapshotArgs {
                        message: Some("watch periodic rescan".into()),
                    },
                )?;
                if args.once {
                    break;
                }
                continue;
            }
        };
        if !snapshot_relevant_event(&event) {
            continue;
        }
        record_notify_event(paths, backend_label, &active_roots, &event)?;
        if args.mode == "strict" {
            snapshot_and_maybe_sync(
                paths,
                SnapshotArgs {
                    message: Some("watch strict event snapshot".into()),
                },
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
        };
        let outcome = drain_watch_event_buffer(paths, &rx, buffer, backend_label, &active_roots)?;
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
            },
        )?;
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

fn snapshot_and_maybe_sync(paths: &Paths, args: SnapshotArgs) -> Result<()> {
    if std::env::var("MAJUTSU_WATCH_INLINE_SNAPSHOT").as_deref() == Ok("1") {
        snapshot(paths, args)?;
        match sync_current_if_remote(paths) {
            Ok(AutoSyncResult::Synced) => {
                record_event(paths, "watch-sync", "synced current snapshot to remote")?
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
        return Ok(());
    }

    let message = args.message.unwrap_or_else(|| "watch snapshot".into());
    let exe = std::env::current_exe()?;
    let status = std::process::Command::new(&exe)
        .arg("--home")
        .arg(&paths.home)
        .arg("snapshot")
        .arg("--message")
        .arg(&message)
        .status()?;
    if !status.success() {
        bail!("watch snapshot child process failed with status {status}");
    }
    record_event(paths, "watch-snapshot-child", &message)?;

    if read_config(paths)?.remote.is_some() {
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
        let status = std::process::Command::new(&exe)
            .arg("--home")
            .arg(&paths.home)
            .arg("sync")
            .arg("--wait")
            .arg("--timeout-secs")
            .arg("300")
            .status()?;
        if status.success() {
            record_event(paths, "watch-sync", "external sync completed")?;
        } else {
            record_event(
                paths,
                "watch-sync-error",
                &format!("external sync exited with status {status}"),
            )?;
        }
    }
    Ok(())
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
    let kind = match &event.kind {
        EventKind::Create(_) => "create",
        EventKind::Modify(_) => "modify",
        EventKind::Remove(_) => "remove",
        EventKind::Access(_) => "access",
        EventKind::Other => "other",
        _ => "unknown",
    };
    kind
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

fn event_path_for_roots(active_roots: &[RootConfig], path: &Path) -> Option<(String, String)> {
    active_roots.iter().find_map(|root| {
        path.strip_prefix(&root.path).ok().map(|relative| {
            let relative = if relative.as_os_str().is_empty() {
                ".".into()
            } else {
                slash_path(relative)
            };
            (root.id.clone(), relative)
        })
    })
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WatchEventBufferOutcome {
    reason: &'static str,
    events: usize,
    elapsed_ms: u128,
}

fn drain_watch_event_buffer(
    paths: &Paths,
    rx: &mpsc::Receiver<notify::Result<notify::Event>>,
    config: WatchEventBufferConfig,
    backend_label: &str,
    active_roots: &[RootConfig],
) -> Result<WatchEventBufferOutcome> {
    let started = Instant::now();
    let mut last_event = started;
    let mut events = 1usize;
    loop {
        if events >= config.max_events {
            return settle_before_flush(
                paths,
                rx,
                config,
                backend_label,
                active_roots,
                started,
                events,
                "max-events",
            );
        }
        let elapsed = started.elapsed();
        if elapsed >= config.max_latency {
            return settle_before_flush(
                paths,
                rx,
                config,
                backend_label,
                active_roots,
                started,
                events,
                "max-latency",
            );
        }
        let quiet_remaining = config
            .quiet
            .checked_sub(last_event.elapsed())
            .unwrap_or(Duration::ZERO);
        if quiet_remaining.is_zero() {
            return settle_before_flush(
                paths,
                rx,
                config,
                backend_label,
                active_roots,
                started,
                events,
                "quiet",
            );
        }
        let max_remaining = config.max_latency.saturating_sub(elapsed);
        let timeout = quiet_remaining
            .min(max_remaining)
            .max(Duration::from_millis(1));
        match rx.recv_timeout(timeout) {
            Ok(Ok(next)) => {
                if !snapshot_relevant_event(&next) {
                    continue;
                }
                record_notify_event(paths, backend_label, active_roots, &next)?;
                events += 1;
                last_event = Instant::now();
            }
            Ok(Err(err)) => return Err(err.into()),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let reason = if started.elapsed() >= config.max_latency {
                    "max-latency"
                } else {
                    "quiet"
                };
                return settle_before_flush(
                    paths,
                    rx,
                    config,
                    backend_label,
                    active_roots,
                    started,
                    events,
                    reason,
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => bail!("watch channel disconnected"),
        }
    }
}

fn settle_before_flush(
    paths: &Paths,
    rx: &mpsc::Receiver<notify::Result<notify::Event>>,
    config: WatchEventBufferConfig,
    backend_label: &str,
    active_roots: &[RootConfig],
    started: Instant,
    mut events: usize,
    reason: &'static str,
) -> Result<WatchEventBufferOutcome> {
    if !config.settle.is_zero() && reason == "quiet" {
        record_event(
            paths,
            "watch-settle",
            &format!("settle_ms={}", config.settle.as_millis()),
        )?;
        loop {
            if events >= config.max_events {
                return Ok(buffer_outcome(started, events, "max-events"));
            }
            if started.elapsed() >= config.max_latency {
                return Ok(buffer_outcome(started, events, "max-latency"));
            }
            let timeout = config
                .settle
                .min(config.max_latency.saturating_sub(started.elapsed()))
                .max(Duration::from_millis(1));
            match rx.recv_timeout(timeout) {
                Ok(Ok(next)) => {
                    if !snapshot_relevant_event(&next) {
                        continue;
                    }
                    record_notify_event(paths, backend_label, active_roots, &next)?;
                    events += 1;
                }
                Ok(Err(err)) => return Err(err.into()),
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => bail!("watch channel disconnected"),
            }
        }
    }
    Ok(buffer_outcome(started, events, reason))
}

fn buffer_outcome(
    started: Instant,
    events: usize,
    reason: &'static str,
) -> WatchEventBufferOutcome {
    WatchEventBufferOutcome {
        reason,
        events,
        elapsed_ms: started.elapsed().as_millis(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::{Event, EventKind};
    use std::path::PathBuf;
    use std::thread;

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
    fn event_buffer_flushes_after_sliding_quiet_window() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        let (tx, rx) = mpsc::channel();
        tx.send(Ok(test_event("a.txt"))).unwrap();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(Ok(test_event("b.txt"))).unwrap();
            thread::sleep(Duration::from_millis(120));
        });

        let outcome = drain_watch_event_buffer(
            &paths,
            &rx,
            WatchEventBufferConfig {
                quiet: Duration::from_millis(80),
                settle: Duration::ZERO,
                max_latency: Duration::from_millis(500),
                max_events: 100,
            },
            "test",
            &[],
        )
        .unwrap();

        assert_eq!(outcome.reason, "quiet");
        assert_eq!(outcome.events, 3);
        assert!(
            outcome.elapsed_ms >= 90,
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
            },
            "test",
            &[],
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
        thread::spawn(move || {
            for index in 0..10 {
                thread::sleep(Duration::from_millis(15));
                tx.send(Ok(test_event(&format!("file-{index}.txt"))))
                    .unwrap();
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
            },
            "test",
            &[],
        )
        .unwrap();

        assert_eq!(outcome.reason, "max-latency");
        assert!(outcome.events > 1, "{outcome:?}");
    }

    fn test_event(path: &str) -> Event {
        Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
            paths: vec![PathBuf::from(path)],
            attrs: Default::default(),
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
