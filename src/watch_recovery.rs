//! 再取得失敗でwatchを止めず、未処理journalと直前の有効headを維持する。
use crate::config::Paths;
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
struct RetryState {
    version: u32,
    code: String,
    attempts: u32,
    first_failure_at: DateTime<Utc>,
    last_failure_at: DateTime<Utc>,
    next_retry_at: DateTime<Utc>,
}
fn path(paths: &Paths) -> std::path::PathBuf {
    paths.runtime.join("watch-snapshot-retry.json")
}
fn read(paths: &Paths) -> Result<Option<RetryState>> {
    let bytes = match fs::read(path(paths)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let state: RetryState = serde_json::from_slice(&bytes)?;
    if state.version != 1
        || state.attempts == 0
        || !["transport", "missing", "decode", "config", "remote"].contains(&state.code.as_str())
    {
        bail!("watch再試行状態の形式が不正です");
    }
    Ok(Some(state))
}
fn write(paths: &Paths, state: &RetryState) -> Result<()> {
    fs::create_dir_all(&paths.runtime)?;
    crate::atomic_io::write_atomic(&path(paths), &serde_json::to_vec(state)?)
}
fn delay(attempts: u32) -> i64 {
    (5_i64 << attempts.saturating_sub(1).min(6)).min(300)
}
fn defer(paths: &Paths, error: &anyhow::Error) -> Result<bool> {
    let Some(code) = crate::hydration_error::code(&format!("{error:#}")) else {
        return Ok(false);
    };
    let previous = read(paths)?;
    let now = Utc::now();
    let attempts = previous
        .as_ref()
        .map_or(1, |s| s.attempts.saturating_add(1));
    let state = RetryState {
        version: 1,
        code: code.into(),
        attempts,
        first_failure_at: previous.map_or(now, |s| s.first_failure_at),
        last_failure_at: now,
        next_retry_at: now + chrono::Duration::seconds(delay(attempts)),
    };
    write(paths, &state)?;
    eprintln!("watch snapshot deferred: object-fetch-{code}; attempts={attempts}");
    crate::queue_runtime::record_event(
        paths,
        "watch-snapshot-deferred",
        &format!(
            "reason=object-fetch-{code} attempts={attempts} retry_after={}",
            state.next_retry_at
        ),
    )?;
    Ok(true)
}

pub(crate) fn issue_code(paths: &Paths) -> Result<Option<String>> {
    Ok(read(paths)?.map(|s| format!("snapshot-object-fetch-{}", s.code)))
}
pub(crate) fn wait_secs(paths: &Paths, regular: u64) -> Result<u64> {
    let Some(state) = read(paths)? else {
        return Ok(regular);
    };
    let remaining = (state.next_retry_at - Utc::now()).num_seconds().max(1) as u64;
    Ok(if regular == 0 {
        remaining
    } else {
        remaining.min(regular)
    })
}
pub(crate) fn replay(paths: &Paths, once: bool) -> Result<bool> {
    match crate::replay_pending_journal_events(paths) {
        Ok(replayed) => Ok(replayed),
        Err(error) if !once && defer(paths, &error)? => Ok(false),
        Err(error) => Err(error),
    }
}

pub(crate) fn run(
    paths: &Paths,
    once: bool,
    action: impl FnOnce(bool) -> Result<bool>,
) -> Result<()> {
    let pending = read(paths)?;
    if !once
        && pending
            .as_ref()
            .is_some_and(|s| s.next_retry_at > Utc::now())
    {
        return Ok(());
    }
    match action(pending.is_some()) {
        Ok(true) => {
            if pending.is_some() {
                fs::remove_file(path(paths))?;
                crate::queue_runtime::record_event(
                    paths,
                    "watch-snapshot-recovered",
                    "snapshot retry completed",
                )?;
            }
            Ok(())
        }
        Ok(false) => {
            if let Some(mut state) = pending {
                state.next_retry_at = Utc::now() + chrono::Duration::seconds(5);
                write(paths, &state)?;
            }
            Ok(())
        }
        Err(error) if !once && defer(paths, &error)? => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
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
    #[test]
    fn retry_retains_work_backs_off_and_clears_only_after_capture() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        crate::queue_runtime::record_event(&paths, "fs-event", "未保全の変更").unwrap();
        run(&paths, false, |_| {
            Err(crate::hydration_error::failure(
                "objects/trees/aa/bb",
                "transport",
            ))
        })
        .unwrap();
        assert_eq!(
            issue_code(&paths).unwrap().as_deref(),
            Some("snapshot-object-fetch-transport")
        );
        assert!((1..=5).contains(&wait_secs(&paths, 0).unwrap()));
        run(&paths, false, |_| {
            panic!("backoff中にsnapshotを起動しました")
        })
        .unwrap();
        let mut state = read(&paths).unwrap().unwrap();
        state.next_retry_at = Utc::now() - chrono::Duration::seconds(1);
        write(&paths, &state).unwrap();
        run(&paths, false, |full_rescan| {
            assert!(full_rescan);
            Ok(false)
        })
        .unwrap();
        assert!(issue_code(&paths).unwrap().is_some());
        state.next_retry_at = Utc::now() - chrono::Duration::seconds(1);
        write(&paths, &state).unwrap();
        run(&paths, false, |full_rescan| {
            assert!(full_rescan);
            Ok(true)
        })
        .unwrap();
        assert!(issue_code(&paths).unwrap().is_none());
        assert!(
            crate::queue_runtime::event_journal_records(&paths)
                .unwrap()
                .iter()
                .any(|e| e.kind == "fs-event")
        );
        assert_eq!(delay(u32::MAX), 300);
    }
    #[test]
    fn one_shot_and_unrelated_errors_are_not_success() {
        let temp = tempfile::tempdir().unwrap();
        let paths = test_paths(temp.path().join("home"));
        assert!(
            run(&paths, true, |_| Err(crate::hydration_error::failure(
                "objects/trees/aa/bb",
                "missing"
            )))
            .is_err()
        );
        assert!(run(&paths, false, |_| anyhow::bail!("本物のローカルエラー")).is_err());
    }
}
