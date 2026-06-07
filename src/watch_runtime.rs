use anyhow::{Result, bail};
use notify::{Config as NotifyConfig, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::sync::mpsc;
use std::time::Duration;

use crate::cli::{ResolvedWatchArgs, SnapshotArgs, WatchArgs};
use crate::config::{Paths, RootConfig, WatchConfig, read_config, validate_watch_mode};
use crate::daemon_runtime::{start_daemon_ipc, start_watch_daemon};
use crate::process_runtime::acquire_process_lock;
use crate::queue_runtime::record_event;
use crate::root_state::roots;
use crate::{
    AutoSyncResult, ensure_ready, open_db, replay_pending_journal_events, snapshot,
    sync_current_if_remote,
};

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
            "backend={} mode={} debounce_ms={} settle_ms={} periodic_rescan_secs={}",
            backend_label, args.mode, args.debounce_ms, args.settle_ms, args.periodic_rescan_secs
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
        let detail = format_notify_event(&event);
        record_event(paths, "fs-event", &detail)?;
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
        let debounce = std::time::Duration::from_millis(args.debounce_ms.max(1));
        let settle = std::time::Duration::from_millis(args.settle_ms);
        drain_watch_debounce(paths, &rx, debounce)?;
        if !settle.is_zero() {
            record_event(
                paths,
                "watch-settle",
                &format!("settle_ms={}", args.settle_ms),
            )?;
            loop {
                match rx.recv_timeout(settle) {
                    Ok(Ok(next)) => {
                        record_event(paths, "fs-event", &format_notify_event(&next))?;
                        drain_watch_debounce(paths, &rx, debounce)?;
                        record_event(
                            paths,
                            "watch-settle",
                            &format!("settle_ms={}", args.settle_ms),
                        )?;
                        continue;
                    }
                    Ok(Err(err)) => return Err(err.into()),
                    Err(mpsc::RecvTimeoutError::Timeout) => break,
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        bail!("watch channel disconnected")
                    }
                }
            }
        }
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
    let kind = match &event.kind {
        EventKind::Create(_) => "create",
        EventKind::Modify(_) => "modify",
        EventKind::Remove(_) => "remove",
        EventKind::Access(_) => "access",
        EventKind::Other => "other",
        _ => "unknown",
    };
    let paths = event
        .paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("{kind} {paths}")
}

fn drain_watch_debounce(
    paths: &Paths,
    rx: &mpsc::Receiver<notify::Result<notify::Event>>,
    debounce: Duration,
) -> Result<()> {
    loop {
        match rx.recv_timeout(debounce) {
            Ok(Ok(next)) => {
                record_event(paths, "fs-event", &format_notify_event(&next))?;
            }
            Ok(Err(err)) => return Err(err.into()),
            Err(mpsc::RecvTimeoutError::Timeout) => return Ok(()),
            Err(mpsc::RecvTimeoutError::Disconnected) => bail!("watch channel disconnected"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::{Event, EventKind};
    use std::path::PathBuf;

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
}
