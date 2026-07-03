use crate::majutsu_core::{
    FileRecord, OperationLogEntry as OperationExport, Payload, RootSnapshot, SnapshotManifest,
    TreeManifest, TreeNodeManifest, TreeNodeRef, payload_blob_ref, payload_large_ref,
};
use crate::majutsu_store::{
    host_current_ref_key, host_last_synced_ref_key, host_root_ack_ref_key,
    host_root_ack_ref_prefix, remote_host_label,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Local, NaiveTime, TimeZone, Utc};
use crossterm::cursor;
use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::queue;
use crossterm::terminal::{self, ClearType};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use similar::{ChangeTag, TextDiff};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::{self, IsTerminal, Read as _, Write as _};
#[cfg(unix)]
use std::mem;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration as StdDuration;
use walkdir::WalkDir;

use crate::atomic_io::write_atomic;
use crate::cli::{DiffArgs, HealthArgs, LogArgs, NoteArgs, OpCommand, StateArgs, StatusArgs};
use crate::config::{Config, Paths, RootConfig, read_config};
use crate::daemon_runtime::{
    DaemonHealth, DaemonHealthState, daemon_health, ensure_daemon_running,
};
use crate::operation_log::{
    query_operation_resolved, record_op, update_operation_message, update_operation_result,
};
use crate::process_runtime::{pid_alive, process_lock_owner};
use crate::queue_runtime::{
    event_journal_records, event_journal_stats, record_event, remote_event_journal_stats,
    upload_queue_stats,
};
use crate::remote_store::open_remote;
use crate::root_state::{
    roots, tombstone_tracked_paths_for_root, tracked_paths_for_root, untracked_paths_for_root,
};
use crate::snapshot_rules::{
    build_ignore, is_volatile, root_dir_allows_descend, root_record_is_managed,
};
use crate::snapshot_state::{
    current_snapshot, first_snapshots_for_roots, load_snapshot_by_id, load_snapshot_header_by_id,
    load_snapshot_header_by_id_optional, snapshot_contains_root, snapshot_file_map, snapshot_id_at,
    visit_tree_records,
};
use crate::sync_runtime::{SyncDeepHealthSummary, sync_deep_health_summary};
use crate::util::{
    blake3_hex, parse_duration_ago, parse_time, path_to_slash, stable_read, stable_read_in_root,
};

pub(crate) fn status_cmd(paths: &Paths, args: StatusArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    let config = read_config(paths)?;
    let roots = roots(&conn)?;
    let auto_daemon_result = ensure_daemon_running(paths);
    let auto_daemon_started = matches!(auto_daemon_result, Ok(Some(_)));
    let auto_daemon_error = auto_daemon_result.err().map(|err| format!("{err:#}"));
    let remote = read_remote_status(&config)?;
    let db_stats = read_status_db_stats(&conn)?;
    let storage = read_storage_stats(paths)?;
    let restore_queue_count = count_json_files(&paths.home.join("queue/restores"))?;
    let daemon = if auto_daemon_started {
        wait_daemon_healthy(paths, 10, StdDuration::from_millis(100))?
    } else {
        daemon_health(paths)?
    };

    // status の集計中に daemon が journal replay や sync を進めることがあるため、
    // health 判定と表示に使う揮発的な状態は出力直前に再読込する。
    let current = current_snapshot(&conn)?;
    let current_manifest = current
        .as_deref()
        .map(|id| load_snapshot_by_id(paths, &conn, id))
        .transpose()?;
    let current_label = current.as_deref().unwrap_or("(none)");
    let remote_head = read_remote_head_status(&conn, &config, &remote, current.as_deref())?;
    let remote_manifest = remote_head
        .remote_current
        .as_deref()
        .and_then(|id| load_snapshot_by_id(paths, &conn, id).ok());
    let upload_stats = upload_queue_stats(paths)?;
    let event_records = event_journal_records(paths)?;
    let event_count = event_records.len();
    let pending_event_count = pending_journal_event_count(&event_records);
    let watch_attribution_issue = watch_attribution_issue(&event_records);
    let remote_journal_stats = remote_event_journal_stats(paths)?;

    let health = build_health_report(HealthInputs {
        paths,
        config: &config,
        roots: &roots,
        current: current.as_deref(),
        remote: &remote,
        remote_head: &remote_head,
        daemon: &daemon,
        auto_daemon_error,
        upload_stats: &upload_stats,
        pending_event_count,
        durable_journal_pending: remote_journal_stats.pending,
        watch_attribution_issue,
        current_manifest: current_manifest.as_ref(),
        remote_manifest: remote_manifest.as_ref(),
        deep_remote: None,
        conn: &conn,
    })?;
    let width = terminal_width();
    let height = terminal_height();
    let ui = StatusUi::new();
    let mut output = String::new();

    writeln!(output, "{}", ui.heading("Status")).expect("write status output");
    print_kv(&mut output, width, "Protection", health.state.as_str());
    print_kv(
        &mut output,
        width,
        "Health issues",
        &health.issue_count().to_string(),
    );
    print_kv(&mut output, width, "Current snapshot", current_label);
    print_kv(&mut output, width, "Roots", &roots.len().to_string());
    print_kv(&mut output, width, "Daemon", daemon.label());
    print_kv(&mut output, width, "Remote", remote.summary());
    print_kv(&mut output, width, "Remote head", remote_head.label());
    print_kv(
        &mut output,
        width,
        "Queued uploads",
        &upload_stats.total.to_string(),
    );
    print_kv(
        &mut output,
        width,
        "State usage",
        &format_bytes(storage.state_bytes),
    );
    writeln!(output).expect("write status output");
    print_health_section(&mut output, width, &ui, &health);
    writeln!(output).expect("write status output");

    print_status_overview(
        &mut output,
        width,
        &ui,
        StatusOverview {
            current: current_label,
            roots_total: roots.len(),
            roots_active: roots.iter().filter(|root| root.status == "active").count(),
            roots_problem: roots
                .iter()
                .filter(|root| root.status != "active" || !root.path.exists())
                .count(),
            daemon: &daemon,
            remote: &remote,
            remote_head: &remote_head,
            upload_total: upload_stats.total,
            upload_retrying: upload_stats.retrying,
            upload_delayed: upload_stats.delayed,
            upload_backpressure: upload_stats.has_backpressure(),
            encryption: config.security.encryption.as_str(),
            state_bytes: storage.state_bytes,
            state_disk_bytes: storage.state_disk_bytes,
            object_bytes: storage.objects_bytes,
            object_disk_bytes: storage.objects_disk_bytes,
            queue_bytes: storage.queue_bytes,
            queue_disk_bytes: storage.queue_disk_bytes,
            blob_bytes: storage.loose_blob_bytes,
            pack_bytes: storage.pack_bytes,
            chunk_bytes: storage.large_bytes,
        },
    );
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Host")).expect("write status output");
    print_kv(&mut output, width, "Name", &config.host.name);
    print_kv(&mut output, width, "ID", &config.host.id);
    print_kv(
        &mut output,
        width,
        "Home",
        &paths.home.display().to_string(),
    );
    print_kv(
        &mut output,
        width,
        "Config",
        &paths.config.display().to_string(),
    );
    print_kv(
        &mut output,
        width,
        "Database",
        &paths.db.display().to_string(),
    );
    writeln!(output).expect("write status output");

    print_remote_section(&mut output, width, &ui, &remote);
    writeln!(output).expect("write status output");

    print_daemon_section(
        &mut output,
        width,
        &ui,
        &daemon,
        roots.iter().filter(|root| root.status == "active").count(),
    );
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Configuration")).expect("write status output");
    print_table(
        &mut output,
        width,
        &["AREA", "SETTING", "VALUE"],
        &[
            [
                "security",
                "encryption",
                config.security.encryption.as_str(),
            ],
            ["security", "hash", config.security.hash.as_str()],
            ["watch", "backend", config.watch.backend.as_str()],
            ["watch", "mode", config.watch.mode.as_str()],
            [
                "watch",
                "debounce",
                &format!("{} ms", config.watch.debounce),
            ],
            ["watch", "settle", &format!("{} ms", config.watch.settle)],
            [
                "watch",
                "buffer-max",
                &format!("{} ms", config.watch.buffer_max),
            ],
            [
                "watch",
                "buffer-events",
                &config.watch.buffer_max_events.to_string(),
            ],
            ["large", "enabled", &config.large.enabled.to_string()],
            ["large", "min-size", &format_bytes(config.large.min_size)],
            [
                "large",
                "binary-min-size",
                &format_bytes(config.large.binary_min_size),
            ],
            [
                "large",
                "chunked-min-size",
                &format_bytes(config.large.chunked_min_size),
            ],
            [
                "large",
                "chunked-chunk-size",
                &format_bytes(config.large.chunked_chunk_size as u64),
            ],
            ["large", "chunking", config.large.default_chunking.as_str()],
            [
                "large",
                "chunk-size",
                &format_bytes(config.large.chunk_size as u64),
            ],
            ["large", "multipart", &config.large.multipart.to_string()],
            [
                "pack",
                "small-target",
                &format_bytes(config.pack.small_pack_target),
            ],
            [
                "pack",
                "normal-target",
                &format_bytes(config.pack.normal_pack_target),
            ],
        ],
    );
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Roots")).expect("write status output");
    if roots.is_empty() {
        writeln!(output, "  (none)").expect("write status output");
    } else {
        if width < 64 {
            let root_rows = roots
                .iter()
                .map(|root| {
                    let state = if root.status == "active" && !root.path.exists() {
                        "missing".to_string()
                    } else {
                        root.status.clone()
                    };
                    let current_root = current_manifest
                        .as_ref()
                        .and_then(|manifest| manifest.root_trees.get(&root.id));
                    [
                        root.id.clone(),
                        state,
                        current_root
                            .map(|root_tree| root_tree.file_count.to_string())
                            .unwrap_or_else(|| "-".into()),
                        current_root
                            .map(|root_tree| shorten_middle(&root_tree.tree_id, 18))
                            .unwrap_or_else(|| "-".into()),
                        root.path.display().to_string(),
                    ]
                })
                .collect::<Vec<_>>();
            let root_row_refs = root_rows
                .iter()
                .map(|row| {
                    [
                        row[0].as_str(),
                        row[1].as_str(),
                        row[2].as_str(),
                        row[3].as_str(),
                        row[4].as_str(),
                    ]
                })
                .collect::<Vec<_>>();
            print_table(
                &mut output,
                width,
                &["ID", "STATUS", "FILES", "TREE", "PATH"],
                &root_row_refs,
            );
        } else {
            let root_rows = roots
                .iter()
                .map(|root| {
                    let state = if root.status == "active" && !root.path.exists() {
                        "missing".to_string()
                    } else {
                        root.status.clone()
                    };
                    let current_root = current_manifest
                        .as_ref()
                        .and_then(|manifest| manifest.root_trees.get(&root.id));
                    Ok([
                        root.id.clone(),
                        state,
                        root.degraded
                            .as_ref()
                            .map(|degraded| {
                                format!(
                                    "{} {}",
                                    degraded.kind,
                                    compact_timestamp(&degraded.at.to_rfc3339())
                                )
                            })
                            .unwrap_or_else(|| "-".into()),
                        current_root
                            .map(|root_tree| root_tree.file_count.to_string())
                            .unwrap_or_else(|| "-".into()),
                        current_root
                            .map(|root_tree| shorten_middle(&root_tree.tree_id, 18))
                            .unwrap_or_else(|| "-".into()),
                        current
                            .as_deref()
                            .zip(current_root)
                            .map(|(current_id, root_tree)| {
                                root_last_change(
                                    paths,
                                    &conn,
                                    current_id,
                                    &root.id,
                                    &root_tree.tree_id,
                                )
                                .map(|change| change.changed_at)
                            })
                            .transpose()?
                            .map(|changed_at| compact_timestamp(&changed_at))
                            .unwrap_or_else(|| "-".into()),
                        current_root
                            .map(|root_tree| {
                                root_remote_sync_label(
                                    remote_head.root_acks.get(&root.id),
                                    remote_manifest.as_ref(),
                                    &root.id,
                                    &root_tree.tree_id,
                                    remote_head.remote_last_synced.as_deref(),
                                )
                            })
                            .unwrap_or_else(|| "-".into()),
                        root.path.display().to_string(),
                    ])
                })
                .collect::<Result<Vec<_>>>()?;
            let root_row_refs = root_rows
                .iter()
                .map(|row| {
                    [
                        row[0].as_str(),
                        row[1].as_str(),
                        row[2].as_str(),
                        row[3].as_str(),
                        row[4].as_str(),
                        row[5].as_str(),
                        row[6].as_str(),
                        row[7].as_str(),
                    ]
                })
                .collect::<Vec<_>>();
            print_table(
                &mut output,
                width,
                &[
                    "ID", "STATUS", "ISSUE", "FILES", "TREE", "CHANGED", "REMOTE", "PATH",
                ],
                &root_row_refs,
            );
        }
    }
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Logical Metadata")).expect("write status output");
    print_table(
        &mut output,
        width,
        &["ITEM", "COUNT", "LOGICAL SIZE"],
        &[
            ["snapshots", &db_stats.snapshots.to_string(), "-"],
            ["operations", &db_stats.operations.to_string(), "-"],
            ["refs", &db_stats.refs.to_string(), "-"],
            [
                "logical blobs",
                &db_stats.blobs.to_string(),
                &format_bytes(db_stats.blob_bytes as u64),
            ],
            [
                "large objects",
                &db_stats.large_objects.to_string(),
                &format_bytes(db_stats.large_object_bytes as u64),
            ],
            [
                "chunks",
                &db_stats.chunks.to_string(),
                &format_bytes(db_stats.chunk_bytes as u64),
            ],
            [
                "packs",
                &db_stats.packs.to_string(),
                &format_bytes(db_stats.pack_bytes as u64),
            ],
            ["large pins", &db_stats.large_pins.to_string(), "-"],
            ["remote refs", &db_stats.remote_refs.to_string(), "-"],
        ],
    );
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Local Storage")).expect("write status output");
    print_table(
        &mut output,
        width,
        &["SCOPE", "FILES", "APPARENT", "DISK"],
        &[
            [
                "state",
                &storage.state_files.to_string(),
                &format_bytes(storage.state_bytes),
                &format_bytes(storage.state_disk_bytes),
            ],
            [
                "objects",
                &storage.objects_files.to_string(),
                &format_bytes(storage.objects_bytes),
                &format_bytes(storage.objects_disk_bytes),
            ],
            [
                "loose blobs",
                &storage.loose_blob_files.to_string(),
                &format_bytes(storage.loose_blob_bytes),
                &format_bytes(storage.loose_blob_disk_bytes),
            ],
            [
                "packs",
                &storage.pack_files.to_string(),
                &format_bytes(storage.pack_bytes),
                &format_bytes(storage.pack_disk_bytes),
            ],
            [
                "large/chunks",
                &storage.large_files.to_string(),
                &format_bytes(storage.large_bytes),
                &format_bytes(storage.large_disk_bytes),
            ],
            [
                "trees",
                &storage.tree_files.to_string(),
                &format_bytes(storage.tree_bytes),
                &format_bytes(storage.tree_disk_bytes),
            ],
            [
                "logs",
                &storage.logs_files.to_string(),
                &format_bytes(storage.logs_bytes),
                &format_bytes(storage.logs_disk_bytes),
            ],
            [
                "queue",
                &storage.queue_files.to_string(),
                &format_bytes(storage.queue_bytes),
                &format_bytes(storage.queue_disk_bytes),
            ],
        ],
    );
    writeln!(output).expect("write status output");

    let event_stats = event_journal_stats(paths)?;
    let remote_journal_stats = remote_event_journal_stats(paths)?;
    writeln!(output, "{}", ui.heading("Queues")).expect("write status output");
    print_table(
        &mut output,
        width,
        &["QUEUE", "ITEMS", "DETAILS"],
        &[
            [
                "uploads",
                &upload_stats.total.to_string(),
                &format!(
                    "retrying={}, delayed={}, attempts={}, max_attempts={}, next_retry={}, backpressure={}",
                    upload_stats.retrying,
                    upload_stats.delayed,
                    upload_stats.attempts,
                    upload_stats.max_attempts,
                    upload_stats
                        .next_retry_after
                        .map(|ts| ts.to_string())
                        .unwrap_or_else(|| "(none)".into()),
                    upload_stats.has_backpressure()
                ),
            ],
            [
                "event journal",
                &event_count.to_string(),
                &format!(
                    "{pending_event_count} pending, {} removable, records retained",
                    event_stats.removable
                ),
            ],
            [
                "durable journal",
                &remote_journal_stats.total.to_string(),
                &format!(
                    "{} durable, {} pending remote ack",
                    remote_journal_stats.durable, remote_journal_stats.pending
                ),
            ],
            [
                "restore jobs",
                &restore_queue_count.to_string(),
                "prepared restore jobs",
            ],
        ],
    );
    writeln!(output).expect("write status output");

    writeln!(output, "Machine").expect("write status output");
    writeln!(output, "current {current_label}").expect("write status output");
    emit_status_output(&output, height, &args)?;
    Ok(())
}

pub(crate) fn health_cmd(paths: &Paths, args: HealthArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let report = current_health_report(paths, &args)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    print_health_report(&report, args.verbose);
    Ok(())
}

pub(crate) fn refresh_runtime_health(paths: &Paths) -> Result<()> {
    let report = current_health_report(paths, &HealthArgs::default())?;
    let record = RuntimeHealthRecord {
        observed_at: Utc::now(),
        report,
    };
    fs::create_dir_all(&paths.runtime)?;
    write_atomic(
        &runtime_health_path(paths),
        &serde_json::to_vec_pretty(&record)?,
    )?;
    maybe_send_health_notice(paths, &record)?;
    Ok(())
}

fn current_health_report(paths: &Paths, args: &HealthArgs) -> Result<HealthReport> {
    let conn = crate::open_db(paths)?;
    let config = read_config(paths)?;
    let roots = roots(&conn)?;
    let current = current_snapshot(&conn)?;
    let current_manifest = current
        .as_deref()
        .map(|id| load_snapshot_by_id(paths, &conn, id))
        .transpose()?;
    let remote = read_remote_status(&config)?;
    let remote_head = read_remote_head_status(&conn, &config, &remote, current.as_deref())?;
    let remote_manifest = remote_head
        .remote_current
        .as_deref()
        .and_then(|id| load_snapshot_by_id(paths, &conn, id).ok());
    let upload_stats = upload_queue_stats(paths)?;
    let event_records = event_journal_records(paths)?;
    let pending_event_count = pending_journal_event_count(&event_records);
    let watch_attribution_issue = watch_attribution_issue(&event_records);
    let remote_journal_stats = remote_event_journal_stats(paths)?;
    let daemon = daemon_health(paths)?;
    let deep_remote = if args.deep && remote.configured && remote.open_error.is_none() {
        if let Some(remote_config) = config.remote.as_ref() {
            match open_remote(remote_config) {
                Ok(remote_store) => Some(sync_deep_health_summary(
                    paths,
                    &conn,
                    &remote_store,
                    args.sample,
                    args.timeout_secs.or(Some(15)),
                    !args.history,
                )?),
                Err(_) => None,
            }
        } else {
            None
        }
    } else {
        None
    };
    build_health_report(HealthInputs {
        paths,
        config: &config,
        roots: &roots,
        current: current.as_deref(),
        remote: &remote,
        remote_head: &remote_head,
        daemon: &daemon,
        auto_daemon_error: None,
        upload_stats: &upload_stats,
        pending_event_count,
        durable_journal_pending: remote_journal_stats.pending,
        watch_attribution_issue,
        current_manifest: current_manifest.as_ref(),
        remote_manifest: remote_manifest.as_ref(),
        deep_remote: deep_remote.as_ref(),
        conn: &conn,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ProtectionState {
    Protected,
    Degraded,
    Unprotected,
}

impl ProtectionState {
    fn as_str(self) -> &'static str {
        match self {
            ProtectionState::Protected => "protected",
            ProtectionState::Degraded => "degraded",
            ProtectionState::Unprotected => "unprotected",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum HealthSeverity {
    Info,
    Warning,
    Critical,
}

impl HealthSeverity {
    fn as_str(self) -> &'static str {
        match self {
            HealthSeverity::Info => "info",
            HealthSeverity::Warning => "warning",
            HealthSeverity::Critical => "critical",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HealthIssue {
    severity: HealthSeverity,
    code: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RootHealth {
    id: String,
    status: String,
    path: String,
    present: bool,
    current_snapshot_includes: bool,
    current_file_count: Option<usize>,
    current_tree_id: Option<String>,
    last_changed_snapshot: Option<String>,
    last_changed_at: Option<String>,
    degraded_kind: Option<String>,
    degraded_at: Option<String>,
    degraded_message: Option<String>,
    remote_snapshot_includes: bool,
    remote_tree_id: Option<String>,
    remote_synced: bool,
    remote_synced_snapshot: Option<String>,
    remote_synced_at: Option<String>,
}

#[derive(Debug, Clone)]
struct RootChange {
    snapshot_id: String,
    changed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HealthReport {
    state: ProtectionState,
    current_snapshot: Option<String>,
    active_roots: usize,
    roots_total: usize,
    daemon_state: String,
    daemon_ipc: bool,
    remote_configured: bool,
    remote_available: bool,
    remote_head_status: String,
    queued_uploads: usize,
    queued_uploads_retrying: usize,
    queued_uploads_delayed: usize,
    pending_journal_events: usize,
    durable_journal_pending: usize,
    sync_lock_pid: Option<u32>,
    encryption: String,
    deep_remote: Option<DeepRemoteHealth>,
    roots: Vec<RootHealth>,
    issues: Vec<HealthIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeepRemoteHealth {
    scope: String,
    local_objects: usize,
    checked: usize,
    missing: usize,
    limited: bool,
    source: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RuntimeHealthRecord {
    observed_at: DateTime<Utc>,
    report: HealthReport,
}

#[derive(Debug, Serialize, Deserialize)]
struct HealthNoticeMarker {
    state: ProtectionState,
    issue_codes: Vec<String>,
    sent_at: DateTime<Utc>,
}

impl HealthReport {
    fn issue_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|issue| issue.severity != HealthSeverity::Info)
            .count()
    }
}

struct HealthInputs<'a> {
    paths: &'a Paths,
    config: &'a Config,
    roots: &'a [RootConfig],
    current: Option<&'a str>,
    remote: &'a RemoteStatus,
    remote_head: &'a RemoteHeadStatus,
    daemon: &'a DaemonHealth,
    auto_daemon_error: Option<String>,
    upload_stats: &'a crate::queue_runtime::UploadQueueStats,
    pending_event_count: usize,
    durable_journal_pending: usize,
    watch_attribution_issue: Option<String>,
    current_manifest: Option<&'a crate::majutsu_core::SnapshotManifest>,
    remote_manifest: Option<&'a crate::majutsu_core::SnapshotManifest>,
    deep_remote: Option<&'a SyncDeepHealthSummary>,
    conn: &'a Connection,
}

fn build_health_report(input: HealthInputs<'_>) -> Result<HealthReport> {
    let HealthInputs {
        paths,
        config,
        roots,
        current,
        remote,
        remote_head,
        daemon,
        auto_daemon_error,
        upload_stats,
        pending_event_count,
        durable_journal_pending,
        watch_attribution_issue,
        current_manifest,
        remote_manifest,
        deep_remote,
        conn,
    } = input;
    let active_roots = roots.iter().filter(|root| root.status == "active").count();
    let mut issues = Vec::new();
    let root_health = roots
        .iter()
        .map(|root| {
            let current_root =
                current_manifest.and_then(|manifest| manifest.root_trees.get(&root.id));
            let remote_root =
                remote_manifest.and_then(|manifest| manifest.root_trees.get(&root.id));
            let remote_ack = remote_head.root_acks.get(&root.id);
            let remote_ack_synced = current_root
                .zip(remote_ack)
                .map(|(current_root, ack)| current_root.tree_id == ack.tree_id)
                .unwrap_or(false);
            let remote_manifest_synced = current_root
                .zip(remote_root)
                .map(|(current_root, remote_root)| current_root.tree_id == remote_root.tree_id)
                .unwrap_or(false);
            let remote_synced = remote_ack_synced || remote_manifest_synced;
            let last_change = current
                .zip(current_root)
                .map(|(current_id, root_tree)| {
                    root_last_change(paths, conn, current_id, &root.id, &root_tree.tree_id)
                })
                .transpose()?;
            Ok(RootHealth {
                id: root.id.clone(),
                status: root.status.clone(),
                path: root.path.display().to_string(),
                present: root.path.exists(),
                current_snapshot_includes: current_root.is_some(),
                current_file_count: current_root.map(|root_tree| root_tree.file_count),
                current_tree_id: current_root.map(|root_tree| root_tree.tree_id.clone()),
                last_changed_snapshot: last_change
                    .as_ref()
                    .map(|change| change.snapshot_id.clone()),
                last_changed_at: last_change.map(|change| change.changed_at),
                degraded_kind: root.degraded.as_ref().map(|degraded| degraded.kind.clone()),
                degraded_at: root
                    .degraded
                    .as_ref()
                    .map(|degraded| degraded.at.to_rfc3339()),
                degraded_message: root
                    .degraded
                    .as_ref()
                    .map(|degraded| degraded.message.clone()),
                remote_snapshot_includes: remote_ack.is_some() || remote_root.is_some(),
                remote_tree_id: remote_ack
                    .map(|ack| ack.tree_id.clone())
                    .or_else(|| remote_root.map(|root_tree| root_tree.tree_id.clone())),
                remote_synced,
                remote_synced_snapshot: if remote_synced {
                    remote_ack
                        .map(|ack| ack.snapshot_id.clone())
                        .or_else(|| remote_head.remote_current.clone())
                } else {
                    None
                },
                remote_synced_at: if remote_synced {
                    remote_ack
                        .and_then(|ack| ack.synced_at.clone())
                        .or_else(|| remote_head.remote_last_synced.clone())
                } else {
                    None
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if active_roots == 0 {
        issues.push(HealthIssue {
            severity: HealthSeverity::Warning,
            code: "no-active-roots".into(),
            message: "no active roots are configured".into(),
        });
    }

    for root in roots {
        if (root.status == "active" && !root.path.exists()) || root.status == "missing" {
            issues.push(HealthIssue {
                severity: HealthSeverity::Critical,
                code: "root-missing".into(),
                message: root_missing_recovery_message(root),
            });
        } else if root.status != "active" {
            issues.push(HealthIssue {
                severity: HealthSeverity::Warning,
                code: "root-not-active".into(),
                message: format!("root {} status is {}", root.id, root.status),
            });
        } else if current.is_some()
            && current_manifest
                .and_then(|manifest| manifest.root_trees.get(&root.id))
                .is_none()
        {
            issues.push(HealthIssue {
                severity: HealthSeverity::Critical,
                code: "root-missing-from-current-snapshot".into(),
                message: format!("active root {} is not present in current snapshot", root.id),
            });
        }
    }

    if active_roots > 0 && !daemon.is_healthy() {
        issues.push(HealthIssue {
            severity: HealthSeverity::Critical,
            code: "daemon-unhealthy".into(),
            message: format!("watch daemon is {}", daemon.label()),
        });
    }
    if active_roots > 0
        && let Some(message) = watch_attribution_issue
    {
        issues.push(HealthIssue {
            severity: HealthSeverity::Critical,
            code: "watch-attribution-unavailable".into(),
            message,
        });
    }
    if let Some(error) = auto_daemon_error {
        issues.push(HealthIssue {
            severity: HealthSeverity::Warning,
            code: "daemon-auto-start-failed".into(),
            message: format!("daemon auto-start failed: {error}"),
        });
    }

    if active_roots > 0 && !remote.configured {
        issues.push(HealthIssue {
            severity: HealthSeverity::Critical,
            code: "remote-not-configured".into(),
            message: "no remote backend is configured".into(),
        });
    } else if remote.open_error.is_some() {
        issues.push(HealthIssue {
            severity: HealthSeverity::Critical,
            code: "remote-unavailable".into(),
            message: remote
                .open_error
                .clone()
                .unwrap_or_else(|| "remote backend is unavailable".into()),
        });
    }

    if current.is_some() && remote.configured && remote.open_error.is_none() && !remote_head.synced
    {
        issues.push(HealthIssue {
            severity: HealthSeverity::Critical,
            code: "remote-head-lagging".into(),
            message: format!("remote head is {}", remote_head.label()),
        });
    }

    if upload_stats.total > 0 {
        let severity = if upload_stats.has_backpressure() {
            HealthSeverity::Critical
        } else {
            HealthSeverity::Warning
        };
        issues.push(HealthIssue {
            severity,
            code: "upload-queue-not-empty".into(),
            message: format!(
                "upload queue has {} item(s), retrying={}, delayed={}",
                upload_stats.total, upload_stats.retrying, upload_stats.delayed
            ),
        });
    }

    if pending_event_count > 0 && durable_journal_pending > 0 {
        issues.push(HealthIssue {
            severity: HealthSeverity::Warning,
            code: "pending-journal-events".into(),
            message: format!(
                "event journal has {pending_event_count} pending trigger event(s), {durable_journal_pending} awaiting remote ack"
            ),
        });
    }
    if durable_journal_pending > 0 {
        issues.push(HealthIssue {
            severity: HealthSeverity::Warning,
            code: "durable-journal-pending".into(),
            message: format!(
                "durable remote journal has {durable_journal_pending} pending remote ack event(s)"
            ),
        });
    }
    if let Some(deep) = deep_remote {
        if deep.missing > 0 {
            issues.push(HealthIssue {
                severity: HealthSeverity::Critical,
                code: "remote-objects-missing".into(),
                message: format!(
                    "deep remote check ({}) found {} missing object(s) after checking {}/{} via {}",
                    deep.scope, deep.missing, deep.checked, deep.local_objects, deep.source
                ),
            });
        } else if deep.limited {
            issues.push(HealthIssue {
                severity: HealthSeverity::Warning,
                code: "remote-objects-check-limited".into(),
                message: format!(
                    "deep remote check ({}) was limited after checking {}/{} object(s)",
                    deep.scope, deep.checked, deep.local_objects
                ),
            });
        }
    }

    let sync_lock_pid = process_lock_owner(&paths.sync_lock)?;
    let sync_lock_is_only_waiter =
        remote_head.synced && upload_stats.total == 0 && durable_journal_pending == 0;
    if let Some(pid) = sync_lock_pid
        && !sync_lock_is_only_waiter
    {
        issues.push(HealthIssue {
            severity: HealthSeverity::Warning,
            code: "sync-running".into(),
            message: format!("sync is currently running with pid {pid}"),
        });
    }
    let stale_running = mark_stale_running_operations(conn, 600)?;
    if stale_running > 0 {
        issues.push(HealthIssue {
            severity: HealthSeverity::Warning,
            code: "stale-running-operations".into(),
            message: format!(
                "{stale_running} operation(s) are still marked running after 600 seconds"
            ),
        });
    }

    if config.security.encryption != "none" && !paths.master_key.exists() {
        issues.push(HealthIssue {
            severity: HealthSeverity::Warning,
            code: "master-key-file-missing".into(),
            message: format!(
                "encryption is {}, but {} is not present",
                config.security.encryption,
                paths.master_key.display()
            ),
        });
    }

    let state = if issues
        .iter()
        .any(|issue| issue.severity == HealthSeverity::Critical)
    {
        ProtectionState::Unprotected
    } else if issues
        .iter()
        .any(|issue| issue.severity == HealthSeverity::Warning)
    {
        ProtectionState::Degraded
    } else {
        ProtectionState::Protected
    };

    Ok(HealthReport {
        state,
        current_snapshot: current.map(str::to_string),
        active_roots,
        roots_total: roots.len(),
        daemon_state: daemon.label().to_string(),
        daemon_ipc: daemon.ipc_available,
        remote_configured: remote.configured,
        remote_available: remote.configured && remote.open_error.is_none(),
        remote_head_status: remote_head.label().to_string(),
        queued_uploads: upload_stats.total,
        queued_uploads_retrying: upload_stats.retrying,
        queued_uploads_delayed: upload_stats.delayed,
        pending_journal_events: pending_event_count,
        durable_journal_pending,
        sync_lock_pid,
        encryption: config.security.encryption.clone(),
        deep_remote: deep_remote.map(|deep| DeepRemoteHealth {
            scope: deep.scope.to_string(),
            local_objects: deep.local_objects,
            checked: deep.checked,
            missing: deep.missing,
            limited: deep.limited,
            source: deep.source.clone(),
        }),
        roots: root_health,
        issues,
    })
}

fn root_missing_recovery_message(root: &RootConfig) -> String {
    format!(
        "root {} is missing: {}; recover by recreating the directory, running `mj root set {} --path <new-path>`, or restoring to a canonical target path with `mj restore plan --root {} --to <dir>`",
        root.id,
        root.path.display(),
        root.id,
        root.id
    )
}

fn mark_stale_running_operations(conn: &Connection, older_than_secs: i64) -> Result<usize> {
    let cutoff = (Utc::now() - chrono::Duration::seconds(older_than_secs)).to_rfc3339();
    let mut stmt = conn.prepare(
        "select id, process_id from operations where status='running' and created_at < ?1",
    )?;
    let rows = stmt.query_map(params![cutoff], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
    })?;
    let mut stale = Vec::new();
    for row in rows {
        let (id, pid) = row?;
        if pid
            .map(|pid| pid > 0 && pid_alive(pid as u32))
            .unwrap_or(false)
        {
            continue;
        }
        stale.push(id);
    }
    for id in &stale {
        update_operation_result(
            conn,
            id,
            "failed",
            Some("operation was still marked running after its process exited"),
            Some("failed"),
        )?;
    }
    Ok(stale.len())
}

fn wait_daemon_healthy(paths: &Paths, attempts: usize, delay: StdDuration) -> Result<DaemonHealth> {
    let mut latest = daemon_health(paths)?;
    for _ in 0..attempts {
        if latest.is_healthy() {
            return Ok(latest);
        }
        thread::sleep(delay);
        latest = daemon_health(paths)?;
    }
    Ok(latest)
}

fn root_last_change(
    paths: &Paths,
    conn: &Connection,
    current_id: &str,
    root_id: &str,
    current_tree_id: &str,
) -> Result<RootChange> {
    let mut candidate_id = current_id.to_string();
    let mut candidate_changed_at = snapshot_created_at(conn, current_id)?;
    let mut cursor_id = current_id.to_string();
    loop {
        let snapshot = load_snapshot_header_by_id(paths, conn, &cursor_id)?;
        let Some(parent_id) = snapshot.parent.as_deref() else {
            return Ok(RootChange {
                snapshot_id: candidate_id,
                changed_at: candidate_changed_at,
            });
        };
        if !snapshot_exists(conn, parent_id)? {
            return Ok(RootChange {
                snapshot_id: candidate_id,
                changed_at: candidate_changed_at,
            });
        }
        let parent = load_snapshot_header_by_id(paths, conn, parent_id)?;
        let parent_tree_matches = parent
            .root_trees
            .get(root_id)
            .map(|root_tree| root_tree.tree_id.as_str() == current_tree_id)
            .unwrap_or(false);
        if !parent_tree_matches {
            return Ok(RootChange {
                snapshot_id: candidate_id,
                changed_at: candidate_changed_at,
            });
        }
        candidate_id = parent_id.to_string();
        candidate_changed_at = snapshot_created_at(conn, parent_id)?;
        cursor_id = parent_id.to_string();
    }
}

fn snapshot_exists(conn: &Connection, snapshot_id: &str) -> Result<bool> {
    conn.query_row(
        "select 1 from snapshots where id=?1",
        params![snapshot_id],
        |_| Ok(()),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(Into::into)
}

fn snapshot_created_at(conn: &Connection, snapshot_id: &str) -> Result<String> {
    conn.query_row(
        "select created_at from snapshots where id=?1",
        params![snapshot_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| anyhow!("snapshot not found: {snapshot_id}"))
}

fn runtime_health_path(paths: &Paths) -> std::path::PathBuf {
    paths.runtime.join("health.json")
}

fn maybe_send_health_notice(paths: &Paths, record: &RuntimeHealthRecord) -> Result<()> {
    if record.report.state == ProtectionState::Protected {
        let _ = fs::remove_file(health_notice_marker_path(paths));
        return Ok(());
    }
    let Some(command) = std::env::var("MAJUTSU_HEALTH_NOTICE_CMD")
        .ok()
        .filter(|command| !command.trim().is_empty())
    else {
        return Ok(());
    };
    let issue_codes = record
        .report
        .issues
        .iter()
        .filter(|issue| issue.severity != HealthSeverity::Info)
        .map(|issue| issue.code.clone())
        .collect::<Vec<_>>();
    if health_notice_recently_sent(paths, record.report.state, &issue_codes) {
        return Ok(());
    }
    let status = crate::platform_runtime::shell_command(&command)
        .env("MAJUTSU_HOME", &paths.home)
        .env("MAJUTSU_HEALTH_STATE", record.report.state.as_str())
        .env(
            "MAJUTSU_HEALTH_ISSUE_COUNT",
            record.report.issue_count().to_string(),
        )
        .env("MAJUTSU_HEALTH_ISSUE_CODES", issue_codes.join(","))
        .env(
            "MAJUTSU_HEALTH_CURRENT_SNAPSHOT",
            record
                .report
                .current_snapshot
                .as_deref()
                .unwrap_or("(none)"),
        )
        .status();
    match status {
        Ok(status) if status.success() => {
            let marker = HealthNoticeMarker {
                state: record.report.state,
                issue_codes,
                sent_at: Utc::now(),
            };
            write_atomic(
                &health_notice_marker_path(paths),
                &serde_json::to_vec_pretty(&marker)?,
            )?;
            record_event(
                paths,
                "health-notice",
                &format!(
                    "state={} issues={}",
                    record.report.state.as_str(),
                    record.report.issue_count()
                ),
            )?;
        }
        Ok(status) => record_event(
            paths,
            "health-notice-error",
            &format!("notice command exited with status {status}"),
        )?,
        Err(err) => record_event(
            paths,
            "health-notice-error",
            &format!("notice command failed: {err}"),
        )?,
    }
    Ok(())
}

fn health_notice_recently_sent(
    paths: &Paths,
    state: ProtectionState,
    issue_codes: &[String],
) -> bool {
    let Ok(bytes) = fs::read(health_notice_marker_path(paths)) else {
        return false;
    };
    let Ok(marker) = serde_json::from_slice::<HealthNoticeMarker>(&bytes) else {
        return false;
    };
    if marker.state != state || marker.issue_codes != issue_codes {
        return false;
    }
    let rate_limit_secs = std::env::var("MAJUTSU_HEALTH_NOTICE_RATE_LIMIT_SECS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(3600);
    Utc::now()
        .signed_duration_since(marker.sent_at)
        .num_seconds()
        < rate_limit_secs
}

fn health_notice_marker_path(paths: &Paths) -> std::path::PathBuf {
    paths.runtime.join("health-notice.sent.json")
}

fn print_health_report(report: &HealthReport, verbose: bool) {
    println!("state {}", report.state.as_str());
    println!(
        "snapshot {}",
        report.current_snapshot.as_deref().unwrap_or("(none)")
    );
    println!("issues {}", report.issue_count());
    println!(
        "daemon state={} ipc={}",
        report.daemon_state, report.daemon_ipc
    );
    println!(
        "remote configured={} available={} head={}",
        report.remote_configured, report.remote_available, report.remote_head_status
    );
    println!(
        "queue uploads={} retrying={} delayed={} journal_pending={} durable_journal={} sync_lock={}",
        report.queued_uploads,
        report.queued_uploads_retrying,
        report.queued_uploads_delayed,
        report.pending_journal_events,
        report.durable_journal_pending,
        report
            .sync_lock_pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "(none)".into())
    );
    println!(
        "roots active={} total={} missing={} unsynced={} degraded={}",
        report.active_roots,
        report.roots_total,
        report.roots.iter().filter(|root| !root.present).count(),
        report
            .roots
            .iter()
            .filter(|root| root.status == "active" && !root.remote_synced)
            .count(),
        report
            .roots
            .iter()
            .filter(|root| root.degraded_kind.is_some())
            .count()
    );
    println!("encryption {}", report.encryption);
    if let Some(deep) = &report.deep_remote {
        println!(
            "deep_remote scope={} checked={}/{} missing={} limited={} source={}",
            deep.scope, deep.checked, deep.local_objects, deep.missing, deep.limited, deep.source
        );
    }
    for issue in &report.issues {
        println!(
            "issue {} {} {}",
            issue.severity.as_str(),
            issue.code,
            issue.message
        );
    }
    if verbose {
        for root in &report.roots {
            println!(
                "root {} status={} present={} current={} files={} tree={}",
                root.id,
                root.status,
                root.present,
                root.current_snapshot_includes,
                root.current_file_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
                root.current_tree_id.as_deref().unwrap_or("-")
            );
            if let Some(changed_at) = &root.last_changed_at {
                println!(
                    "root_last_changed {} snapshot={} at={}",
                    root.id,
                    root.last_changed_snapshot.as_deref().unwrap_or("-"),
                    changed_at
                );
            }
            if let Some(kind) = &root.degraded_kind {
                println!(
                    "root_degraded {} kind={} at={} message={}",
                    root.id,
                    kind,
                    root.degraded_at.as_deref().unwrap_or("-"),
                    root.degraded_message.as_deref().unwrap_or("-")
                );
            }
            println!(
                "root_remote {} current={} synced={} snapshot={} at={} tree={}",
                root.id,
                root.remote_snapshot_includes,
                root.remote_synced,
                root.remote_synced_snapshot.as_deref().unwrap_or("-"),
                root.remote_synced_at.as_deref().unwrap_or("-"),
                root.remote_tree_id.as_deref().unwrap_or("-")
            );
        }
    }
}

fn print_health_section(out: &mut String, width: usize, ui: &StatusUi, report: &HealthReport) {
    writeln!(out, "{}", ui.heading("Protection")).expect("write status output");
    let severity = match report.state {
        ProtectionState::Protected => Severity::Good,
        ProtectionState::Degraded => Severity::Warn,
        ProtectionState::Unprotected => Severity::Bad,
    };
    print_kv(
        out,
        width,
        "State",
        &ui.severity(report.state.as_str(), severity),
    );
    print_kv(out, width, "Issues", &report.issue_count().to_string());
    if report.issues.is_empty() {
        print_kv(
            out,
            width,
            "Summary",
            "daemon, queue, and remote head are healthy",
        );
    } else {
        for issue in &report.issues {
            let severity = match issue.severity {
                HealthSeverity::Info => Severity::Good,
                HealthSeverity::Warning => Severity::Warn,
                HealthSeverity::Critical => Severity::Bad,
            };
            print_kv(
                out,
                width,
                &issue.code,
                &ui.severity(&issue.message, severity),
            );
        }
    }
}

pub(crate) fn state_cmd(paths: &Paths, args: StateArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    let config = read_config(paths)?;
    let current = current_snapshot(&conn)?;
    let configured_roots = roots(&conn)?;
    let state_scope = resolve_state_scope(&configured_roots, &args)?;
    if args.untrack && !state_scope.single_root_context {
        bail!("mj state --untrack requires --root or running inside a configured root");
    }
    let filter = StateChangeFilter::from_args(&args)?;
    let path_matcher =
        PathspecMatcher::for_state(&args.pathspecs, &configured_roots, &state_scope)?;
    let basis_set = if let Some(reference) = args.reference.as_deref() {
        StateBasisSet::from_single(
            resolve_state_basis_optional(paths, &conn, reference)?,
            &state_scope.roots,
        )
    } else {
        StateBasisSet {
            explicit_reference: false,
            entries: Vec::new(),
        }
    };
    let options = StateChangeOptions {
        show_diff: args.diff,
        include_meta: args.meta,
        include_untracked: args.untrack,
        filter,
        path_matcher,
        local_root: state_scope.local_root.clone(),
        reference_since: basis_set.reference_since(),
    };
    if args.reference.is_none() && !args.json {
        stream_state_lifecycle_changes(paths, &conn, &state_scope, options)?;
        return Ok(());
    }
    if !basis_set.entries.is_empty() && !args.json {
        stream_state_short_changes(paths, &conn, &basis_set, &state_scope, options)?;
        return Ok(());
    }
    if args.reference.is_some() && basis_set.entries.is_empty() && !args.json {
        return Ok(());
    }
    let active_branch = ref_value_for_state(&conn, "current-branch")?;
    let latest = latest_snapshot_for_state(&conn)?;
    let refs = state_refs(&conn)?;
    let branches = state_branches(&refs, active_branch.as_deref());
    let db_stats = read_status_db_stats(&conn)?;
    let storage = read_storage_stats(paths)?;
    let upload_stats = upload_queue_stats(paths)?;
    let event_records = event_journal_records(paths)?;
    let event_count = event_records.len();
    let pending_event_count = pending_journal_event_count(&event_records);
    let remote_journal_stats = remote_event_journal_stats(paths)?;
    let restore_queue_count = count_json_files(&paths.home.join("queue/restores"))?;
    let remote = read_remote_status(&config)?;
    let remote_head = read_remote_head_status(&conn, &config, &remote, current.as_deref())?;
    let daemon = daemon_health(paths)?;
    let changes = if args.reference.is_none() {
        Some(state_change_report(
            state_lifecycle_file_changes(
                paths,
                &conn,
                &state_scope.roots,
                state_scope.local_paths,
                args.meta,
                args.untrack,
            )?,
            current.as_deref().unwrap_or("(none)").to_string(),
            &options,
        ))
    } else if !basis_set.entries.is_empty() {
        Some(state_change_report(
            state_live_file_changes_for_basis_set(
                paths,
                &basis_set,
                false,
                args.meta,
                args.untrack,
            )?,
            current.as_deref().unwrap_or("(none)").to_string(),
            &options,
        ))
    } else {
        None
    };

    let state = StateReport {
        host: StateHost {
            name: config.host.name.clone(),
            id: config.host.id.clone(),
        },
        paths: StatePaths {
            home: paths.home.display().to_string(),
            config: paths.config.display().to_string(),
            database: paths.db.display().to_string(),
            objects: paths.objects.display().to_string(),
            logs: paths.logs.display().to_string(),
            runtime: paths.runtime.display().to_string(),
            upload_queue: paths.upload_queue.display().to_string(),
            event_queue: paths.event_queue.display().to_string(),
        },
        timeline: StateTimeline {
            current_snapshot: current.clone(),
            current_branch: active_branch.clone(),
            latest_snapshot: latest.as_ref().map(|snapshot| snapshot.id.clone()),
            latest_created_at: latest.as_ref().map(|snapshot| snapshot.created_at.clone()),
            latest_parent: latest.and_then(|snapshot| snapshot.parent_id),
            branch_count: branches.len(),
        },
        remote: StateRemote {
            configured: remote.configured,
            backend: remote.backend.clone(),
            url: remote.url.clone(),
            resolved: remote.resolved.clone(),
            available: remote.configured && remote.open_error.is_none(),
            open_error: remote.open_error.clone(),
            head_status: remote_head.label().to_string(),
            current: remote_head.current.clone(),
            remote_current: remote_head.remote_current.clone(),
        },
        daemon: StateDaemon {
            state: daemon_state_label(&daemon).to_string(),
            pid: daemon.pid,
            ipc_available: daemon.ipc_available,
            healthy: daemon.is_healthy(),
        },
        security: StateSecurity {
            encryption: config.security.encryption.clone(),
            hash: config.security.hash.clone(),
            master_key_path: paths.master_key.display().to_string(),
        },
        metadata: StateMetadata {
            snapshots: db_stats.snapshots,
            operations: db_stats.operations,
            refs: db_stats.refs,
            remote_refs: db_stats.remote_refs,
            blobs: db_stats.blobs,
            blob_bytes: db_stats.blob_bytes,
            large_objects: db_stats.large_objects,
            large_object_bytes: db_stats.large_object_bytes,
            chunks: db_stats.chunks,
            chunk_bytes: db_stats.chunk_bytes,
            packs: db_stats.packs,
            pack_bytes: db_stats.pack_bytes,
            large_pins: db_stats.large_pins,
        },
        storage: StateStorage {
            state_files: storage.state_files,
            state_bytes: storage.state_bytes,
            state_disk_bytes: storage.state_disk_bytes,
            objects_files: storage.objects_files,
            objects_bytes: storage.objects_bytes,
            objects_disk_bytes: storage.objects_disk_bytes,
            logs_files: storage.logs_files,
            logs_bytes: storage.logs_bytes,
            logs_disk_bytes: storage.logs_disk_bytes,
            queue_files: storage.queue_files,
            queue_bytes: storage.queue_bytes,
            queue_disk_bytes: storage.queue_disk_bytes,
        },
        queues: StateQueues {
            uploads: upload_stats.total as u64,
            uploads_retrying: upload_stats.retrying as u64,
            uploads_delayed: upload_stats.delayed as u64,
            upload_backpressure: upload_stats.has_backpressure(),
            event_journal: event_count as u64,
            event_journal_pending: pending_event_count as u64,
            durable_journal: remote_journal_stats.total as u64,
            durable_journal_pending: remote_journal_stats.pending as u64,
            restore_jobs: restore_queue_count as u64,
        },
        basis: basis_set.report_basis(),
        basis_roots: basis_set.report_root_bases(),
        changes,
        branches,
        refs,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&state)?);
    } else {
        print_state_report(&state)?;
    }
    Ok(())
}

fn print_state_report(state: &StateReport) -> Result<()> {
    let width = terminal_width();
    let height = terminal_height();
    let ui = StatusUi::new();
    let mut output = String::new();

    if state.basis.is_some() {
        print_state_short_changes(&mut output, state, &ui);
        emit_status_output_auto(&output, height)?;
        return Ok(());
    }

    writeln!(output, "{}", ui.heading("State")).expect("write state output");
    print_kv(&mut output, width, "Home", &state.paths.home);
    print_kv(
        &mut output,
        width,
        "Host",
        &format!("{} {}", state.host.name, state.host.id),
    );
    print_kv(
        &mut output,
        width,
        "Current",
        state
            .timeline
            .current_snapshot
            .as_deref()
            .unwrap_or("(none)"),
    );
    print_kv(
        &mut output,
        width,
        "Branch",
        state.timeline.current_branch.as_deref().unwrap_or("(none)"),
    );
    print_kv(
        &mut output,
        width,
        "Remote",
        if state.remote.configured {
            if state.remote.available {
                "configured"
            } else {
                "configured, unavailable"
            }
        } else {
            "not configured"
        },
    );
    print_kv(&mut output, width, "Daemon", &state.daemon.state);
    print_kv(&mut output, width, "Remote head", &state.remote.head_status);
    print_kv(&mut output, width, "Encryption", &state.security.encryption);
    writeln!(output).expect("write state output");

    if let Some(basis) = state.basis.as_ref() {
        writeln!(output, "{}", ui.heading("Basis")).expect("write state output");
        print_table(
            &mut output,
            width,
            &["ITEM", "VALUE"],
            &[
                ["input", basis.input.as_str()],
                ["kind", basis.kind.as_str()],
                ["snapshot", basis.snapshot.as_str()],
                ["snapshot created", basis.snapshot_created_at.as_str()],
                ["operation", basis.operation.as_deref().unwrap_or("(none)")],
                [
                    "operation created",
                    basis.operation_created_at.as_deref().unwrap_or("(none)"),
                ],
                [
                    "resolved at",
                    basis.resolved_at.as_deref().unwrap_or("(none)"),
                ],
            ],
        );
        writeln!(output).expect("write state output");

        writeln!(output, "{}", ui.heading("Changes Since Basis")).expect("write state output");
        if let Some(changes) = state.changes.as_ref() {
            let summary = format!(
                "total={} A={} M={} D={}",
                changes.total, changes.added, changes.modified, changes.deleted
            );
            print_table(
                &mut output,
                width,
                &["ITEM", "VALUE"],
                &[
                    ["current snapshot", changes.current_snapshot.as_str()],
                    ["summary", summary.as_str()],
                ],
            );
            if changes.files.is_empty() {
                writeln!(output, "  clean").expect("write state output");
            } else {
                let rows = changes
                    .files
                    .iter()
                    .map(|change| {
                        [
                            change.status.as_str(),
                            change.root.as_str(),
                            change.path.as_str(),
                        ]
                    })
                    .collect::<Vec<_>>();
                print_table(&mut output, width, &["S", "ROOT", "PATH"], &rows);
            }
        } else {
            writeln!(output, "  (not requested)").expect("write state output");
        }
        writeln!(output).expect("write state output");
    }

    writeln!(output, "{}", ui.heading("Paths")).expect("write state output");
    print_table(
        &mut output,
        width,
        &["ITEM", "PATH"],
        &[
            ["config", state.paths.config.as_str()],
            ["database", state.paths.database.as_str()],
            ["objects", state.paths.objects.as_str()],
            ["logs", state.paths.logs.as_str()],
            ["runtime", state.paths.runtime.as_str()],
            ["upload queue", state.paths.upload_queue.as_str()],
            ["event queue", state.paths.event_queue.as_str()],
            ["master key", state.security.master_key_path.as_str()],
        ],
    );
    writeln!(output).expect("write state output");

    writeln!(output, "{}", ui.heading("Timeline")).expect("write state output");
    print_table(
        &mut output,
        width,
        &["ITEM", "VALUE"],
        &[
            [
                "current snapshot",
                state
                    .timeline
                    .current_snapshot
                    .as_deref()
                    .unwrap_or("(none)"),
            ],
            [
                "current branch",
                state.timeline.current_branch.as_deref().unwrap_or("(none)"),
            ],
            [
                "latest snapshot",
                state
                    .timeline
                    .latest_snapshot
                    .as_deref()
                    .unwrap_or("(none)"),
            ],
            [
                "latest created",
                state
                    .timeline
                    .latest_created_at
                    .as_deref()
                    .unwrap_or("(none)"),
            ],
            [
                "latest parent",
                state.timeline.latest_parent.as_deref().unwrap_or("(none)"),
            ],
            ["branches", &state.timeline.branch_count.to_string()],
            [
                "remote current",
                state.remote.remote_current.as_deref().unwrap_or("(none)"),
            ],
        ],
    );
    writeln!(output).expect("write state output");

    writeln!(output, "{}", ui.heading("Daemon")).expect("write state output");
    let daemon_pid = state
        .daemon
        .pid
        .map(|pid| pid.to_string())
        .unwrap_or_else(|| "(none)".into());
    let daemon_ipc = state.daemon.ipc_available.to_string();
    let daemon_healthy = state.daemon.healthy.to_string();
    print_table(
        &mut output,
        width,
        &["ITEM", "VALUE"],
        &[
            ["state", state.daemon.state.as_str()],
            ["pid", daemon_pid.as_str()],
            ["ipc", daemon_ipc.as_str()],
            ["healthy", daemon_healthy.as_str()],
        ],
    );
    writeln!(output).expect("write state output");

    writeln!(output, "{}", ui.heading("Branches")).expect("write state output");
    if state.branches.is_empty() {
        writeln!(output, "  (none)").expect("write state output");
    } else {
        let rows = state
            .branches
            .iter()
            .map(|branch| {
                [
                    if branch.active { "*" } else { " " },
                    branch.name.as_str(),
                    branch.snapshot.as_str(),
                ]
            })
            .collect::<Vec<_>>();
        print_table(&mut output, width, &["", "NAME", "SNAPSHOT"], &rows);
    }
    writeln!(output).expect("write state output");

    writeln!(output, "{}", ui.heading("Refs")).expect("write state output");
    if state.refs.is_empty() {
        writeln!(output, "  (none)").expect("write state output");
    } else {
        let rows = state
            .refs
            .iter()
            .map(|reference| [reference.name.as_str(), reference.value.as_str()])
            .collect::<Vec<_>>();
        print_table(&mut output, width, &["NAME", "VALUE"], &rows);
    }
    writeln!(output).expect("write state output");

    writeln!(output, "{}", ui.heading("Metadata")).expect("write state output");
    print_table(
        &mut output,
        width,
        &["ITEM", "COUNT", "SIZE"],
        &[
            ["snapshots", &state.metadata.snapshots.to_string(), "-"],
            ["operations", &state.metadata.operations.to_string(), "-"],
            ["refs", &state.metadata.refs.to_string(), "-"],
            ["remote refs", &state.metadata.remote_refs.to_string(), "-"],
            [
                "blobs",
                &state.metadata.blobs.to_string(),
                &format_bytes(state.metadata.blob_bytes.max(0) as u64),
            ],
            [
                "large objects",
                &state.metadata.large_objects.to_string(),
                &format_bytes(state.metadata.large_object_bytes.max(0) as u64),
            ],
            [
                "chunks",
                &state.metadata.chunks.to_string(),
                &format_bytes(state.metadata.chunk_bytes.max(0) as u64),
            ],
            [
                "packs",
                &state.metadata.packs.to_string(),
                &format_bytes(state.metadata.pack_bytes.max(0) as u64),
            ],
            ["large pins", &state.metadata.large_pins.to_string(), "-"],
        ],
    );
    writeln!(output).expect("write state output");

    writeln!(output, "{}", ui.heading("Storage")).expect("write state output");
    print_table(
        &mut output,
        width,
        &["SCOPE", "FILES", "APPARENT", "DISK"],
        &[
            [
                "state",
                &state.storage.state_files.to_string(),
                &format_bytes(state.storage.state_bytes),
                &format_bytes(state.storage.state_disk_bytes),
            ],
            [
                "objects",
                &state.storage.objects_files.to_string(),
                &format_bytes(state.storage.objects_bytes),
                &format_bytes(state.storage.objects_disk_bytes),
            ],
            [
                "logs",
                &state.storage.logs_files.to_string(),
                &format_bytes(state.storage.logs_bytes),
                &format_bytes(state.storage.logs_disk_bytes),
            ],
            [
                "queue",
                &state.storage.queue_files.to_string(),
                &format_bytes(state.storage.queue_bytes),
                &format_bytes(state.storage.queue_disk_bytes),
            ],
        ],
    );
    writeln!(output).expect("write state output");

    writeln!(output, "{}", ui.heading("Queues")).expect("write state output");
    print_table(
        &mut output,
        width,
        &["QUEUE", "COUNT", "STATE"],
        &[
            [
                "uploads",
                &state.queues.uploads.to_string(),
                &format!(
                    "retrying={}, delayed={}, backpressure={}",
                    state.queues.uploads_retrying,
                    state.queues.uploads_delayed,
                    state.queues.upload_backpressure
                ),
            ],
            [
                "event journal",
                &state.queues.event_journal.to_string(),
                &format!(
                    "{} pending, records retained",
                    state.queues.event_journal_pending
                ),
            ],
            [
                "durable journal",
                &state.queues.durable_journal.to_string(),
                &format!(
                    "{} pending remote ack",
                    state.queues.durable_journal_pending
                ),
            ],
            [
                "restore jobs",
                &state.queues.restore_jobs.to_string(),
                "prepared jobs",
            ],
        ],
    );

    emit_status_output_auto(&output, height)?;
    Ok(())
}

fn print_state_short_changes(output: &mut String, state: &StateReport, ui: &StatusUi) {
    let Some(changes) = state.changes.as_ref() else {
        return;
    };
    for change in &changes.files {
        let path = if change.path.is_empty() {
            change.root.clone()
        } else {
            format!("{}/{}", change.root, change.path)
        };
        writeln!(
            output,
            " {} {}",
            color_change_status(ui, &change.status),
            path
        )
        .expect("write state output");
    }
}

#[derive(Serialize)]
struct StateReport {
    host: StateHost,
    paths: StatePaths,
    timeline: StateTimeline,
    remote: StateRemote,
    daemon: StateDaemon,
    security: StateSecurity,
    metadata: StateMetadata,
    storage: StateStorage,
    queues: StateQueues,
    #[serde(skip_serializing_if = "Option::is_none")]
    basis: Option<StateBasis>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    basis_roots: Vec<StateRootBasis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    changes: Option<StateChangeReport>,
    branches: Vec<StateBranch>,
    refs: Vec<StateRef>,
}

#[derive(Serialize)]
struct StateHost {
    name: String,
    id: String,
}

#[derive(Serialize)]
struct StatePaths {
    home: String,
    config: String,
    database: String,
    objects: String,
    logs: String,
    runtime: String,
    upload_queue: String,
    event_queue: String,
}

#[derive(Serialize)]
struct StateTimeline {
    current_snapshot: Option<String>,
    current_branch: Option<String>,
    latest_snapshot: Option<String>,
    latest_created_at: Option<String>,
    latest_parent: Option<String>,
    branch_count: usize,
}

#[derive(Serialize)]
struct StateRemote {
    configured: bool,
    backend: String,
    url: Option<String>,
    resolved: Option<String>,
    available: bool,
    open_error: Option<String>,
    head_status: String,
    current: Option<String>,
    remote_current: Option<String>,
}

#[derive(Serialize)]
struct StateDaemon {
    state: String,
    pid: Option<u32>,
    ipc_available: bool,
    healthy: bool,
}

#[derive(Serialize)]
struct StateSecurity {
    encryption: String,
    hash: String,
    master_key_path: String,
}

#[derive(Serialize)]
struct StateMetadata {
    snapshots: i64,
    operations: i64,
    refs: i64,
    remote_refs: i64,
    blobs: i64,
    blob_bytes: i64,
    large_objects: i64,
    large_object_bytes: i64,
    chunks: i64,
    chunk_bytes: i64,
    packs: i64,
    pack_bytes: i64,
    large_pins: i64,
}

#[derive(Serialize)]
struct StateStorage {
    state_files: u64,
    state_bytes: u64,
    state_disk_bytes: u64,
    objects_files: u64,
    objects_bytes: u64,
    objects_disk_bytes: u64,
    logs_files: u64,
    logs_bytes: u64,
    logs_disk_bytes: u64,
    queue_files: u64,
    queue_bytes: u64,
    queue_disk_bytes: u64,
}

#[derive(Serialize)]
struct StateQueues {
    uploads: u64,
    uploads_retrying: u64,
    uploads_delayed: u64,
    upload_backpressure: bool,
    event_journal: u64,
    event_journal_pending: u64,
    durable_journal: u64,
    durable_journal_pending: u64,
    restore_jobs: u64,
}

#[derive(Clone, Serialize)]
struct StateBasis {
    input: String,
    kind: String,
    snapshot: String,
    snapshot_created_at: String,
    operation: Option<String>,
    operation_created_at: Option<String>,
    resolved_at: Option<String>,
}

#[derive(Serialize)]
struct StateRootBasis {
    root: String,
    input: String,
    kind: String,
    snapshot: String,
    snapshot_created_at: String,
    operation: Option<String>,
    operation_created_at: Option<String>,
    resolved_at: Option<String>,
}

#[derive(Clone)]
struct StateRootBasisEntry {
    root: RootConfig,
    basis: StateBasis,
}

struct StateBasisSet {
    explicit_reference: bool,
    entries: Vec<StateRootBasisEntry>,
}

impl StateBasisSet {
    fn from_single(basis: Option<StateBasis>, roots: &[RootConfig]) -> Self {
        let entries = basis
            .clone()
            .map(|basis| {
                roots
                    .iter()
                    .cloned()
                    .map(|root| StateRootBasisEntry {
                        root,
                        basis: basis.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            explicit_reference: true,
            entries,
        }
    }

    fn report_basis(&self) -> Option<StateBasis> {
        if self.explicit_reference {
            return self.entries.first().map(|entry| entry.basis.clone());
        }
        let first = self.entries.first()?.basis.clone();
        if self
            .entries
            .iter()
            .all(|entry| entry.basis.snapshot == first.snapshot)
        {
            return Some(first);
        }
        None
    }

    fn report_root_bases(&self) -> Vec<StateRootBasis> {
        if self.entries.is_empty() {
            return Vec::new();
        }
        self.entries
            .iter()
            .map(|entry| StateRootBasis {
                root: entry.root.id.clone(),
                input: entry.basis.input.clone(),
                kind: entry.basis.kind.clone(),
                snapshot: entry.basis.snapshot.clone(),
                snapshot_created_at: entry.basis.snapshot_created_at.clone(),
                operation: entry.basis.operation.clone(),
                operation_created_at: entry.basis.operation_created_at.clone(),
                resolved_at: entry.basis.resolved_at.clone(),
            })
            .collect()
    }

    fn reference_since(&self) -> Option<String> {
        if !self.explicit_reference {
            return None;
        }
        let first = self.entries.first()?.basis.reference_since()?;
        self.entries
            .iter()
            .all(|entry| entry.basis.reference_since().as_deref() == Some(first.as_str()))
            .then_some(first)
    }
}

impl StateBasis {
    fn reference_since(&self) -> Option<String> {
        matches!(self.kind.as_str(), "local-time" | "relative-time" | "time")
            .then(|| self.resolved_at.clone())
            .flatten()
    }
}

#[derive(Serialize)]
struct StateChangeReport {
    current_snapshot: String,
    added: usize,
    modified: usize,
    deleted: usize,
    untracked: usize,
    total: usize,
    files: Vec<StateFileChange>,
}

#[derive(Serialize)]
struct StateFileChange {
    status: String,
    root: String,
    path: String,
}

#[derive(Clone)]
struct StateChangeFilter {
    statuses: Option<BTreeSet<String>>,
}

impl StateChangeFilter {
    fn from_args(args: &StateArgs) -> Result<Self> {
        let mut statuses = BTreeSet::new();
        if args.deleted {
            statuses.insert("D".to_string());
        }
        for raw in &args.status {
            for part in raw.split(',') {
                let status = part.trim();
                if status.is_empty() {
                    continue;
                }
                if !matches!(status, "A" | "M" | "D" | "m" | "?") {
                    bail!("invalid state status filter: {status}; expected one of A, M, D, m, ?");
                }
                statuses.insert(status.to_string());
            }
        }
        Ok(Self {
            statuses: if statuses.is_empty() {
                None
            } else {
                Some(statuses)
            },
        })
    }

    fn allows(&self, status: &str) -> bool {
        self.statuses
            .as_ref()
            .is_none_or(|statuses| statuses.contains(status))
    }
}

#[derive(Clone, Default)]
struct PathspecMatcher {
    specs: Vec<Pathspec>,
}

#[derive(Clone)]
struct Pathspec {
    root: Option<String>,
    path: String,
}

impl PathspecMatcher {
    fn for_state(raw_specs: &[PathBuf], roots: &[RootConfig], scope: &StateScope) -> Result<Self> {
        let single_root = scope.single_root_id();
        let cwd_prefix = scope.cwd_root_prefix.as_deref();
        Self::from_raw(
            raw_specs,
            roots,
            single_root,
            cwd_prefix,
            !scope.single_root_context,
        )
    }

    fn for_log(raw_specs: &[PathBuf], roots: &[RootConfig], root: Option<&str>) -> Result<Self> {
        let selected_root_prefix = if let Some(root_id) = root {
            roots
                .iter()
                .find(|configured| configured.id == root_id)
                .map(current_root_prefix)
                .transpose()?
                .flatten()
        } else {
            None
        };
        let cwd_root = if root.is_none() {
            infer_current_root_with_prefix(roots)?.map(|(root, prefix)| (root.id, prefix))
        } else {
            None
        };
        let single_root = root
            .map(str::to_string)
            .or_else(|| cwd_root.as_ref().map(|(root_id, _)| root_id.clone()));
        let cwd_prefix = selected_root_prefix.as_deref().or_else(|| {
            cwd_root
                .as_ref()
                .map(|(_, prefix)| prefix.as_str())
                .filter(|_| root.is_none())
        });
        Self::from_raw(
            raw_specs,
            roots,
            single_root.as_deref(),
            cwd_prefix,
            single_root.is_none(),
        )
    }

    fn from_raw(
        raw_specs: &[PathBuf],
        roots: &[RootConfig],
        single_root: Option<&str>,
        cwd_prefix: Option<&str>,
        global_display: bool,
    ) -> Result<Self> {
        let mut specs = Vec::new();
        for raw in raw_specs {
            if raw.is_absolute() {
                specs.push(resolve_absolute_pathspec(raw, roots)?);
                continue;
            }
            let normalized = normalize_pathspec(raw);
            if let Some(root_id) = single_root {
                let path = strip_root_prefix(&normalized, root_id)
                    .unwrap_or_else(|| join_slash(cwd_prefix.unwrap_or_default(), &normalized));
                specs.push(Pathspec {
                    root: Some(root_id.to_string()),
                    path,
                });
            } else if global_display {
                specs.push(Pathspec {
                    root: None,
                    path: normalized,
                });
            } else {
                specs.push(Pathspec {
                    root: single_root.map(str::to_string),
                    path: join_slash(cwd_prefix.unwrap_or_default(), &normalized),
                });
            }
        }
        Ok(Self { specs })
    }

    fn allows_display_path(&self, display_path: &str, local_root: Option<&str>) -> bool {
        if self.specs.is_empty() {
            return true;
        }
        let (candidate_root, candidate_rel) = if let Some(root) = local_root {
            (Some(root), display_path)
        } else if let Some((root, rel)) = display_path.split_once('/') {
            (Some(root), rel)
        } else {
            (None, display_path)
        };
        self.specs.iter().any(|spec| match spec.root.as_deref() {
            Some(root) => {
                candidate_root == Some(root) && pathspec_matches(candidate_rel, &spec.path)
            }
            None => pathspec_matches(display_path, &spec.path),
        })
    }
}

fn normalize_pathspec(path: &Path) -> String {
    let mut value = path_to_slash(path);
    while value == "." || value.starts_with("./") {
        if value == "." {
            value.clear();
            break;
        }
        value = value[2..].to_string();
    }
    while value.ends_with('/') {
        value.pop();
    }
    value
}

fn resolve_absolute_pathspec(path: &Path, roots: &[RootConfig]) -> Result<Pathspec> {
    let canonical = absolutize_existing_path(path)?;
    let mut matches = roots
        .iter()
        .filter(|root| root.status == "active")
        .filter_map(|root| {
            let root_path = absolutize_existing_path(&root.path).ok()?;
            if canonical.starts_with(&root_path) {
                let rel = canonical.strip_prefix(&root_path).ok()?;
                Some((
                    root_path.components().count(),
                    root.id.clone(),
                    normalize_pathspec(rel),
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    matches.sort_by_key(|(depth, _, _)| *depth);
    let Some((_, root, path)) = matches.pop() else {
        bail!("pathspec is outside configured roots: {}", path.display());
    };
    Ok(Pathspec {
        root: Some(root),
        path,
    })
}

fn strip_root_prefix(path: &str, root_id: &str) -> Option<String> {
    path.strip_prefix(root_id)
        .and_then(|rest| rest.strip_prefix('/'))
        .map(str::to_string)
}

fn join_slash(prefix: &str, path: &str) -> String {
    match (prefix.is_empty(), path.is_empty()) {
        (true, true) => String::new(),
        (true, false) => path.to_string(),
        (false, true) => prefix.to_string(),
        (false, false) => format!("{prefix}/{path}"),
    }
}

fn pathspec_matches(candidate: &str, spec: &str) -> bool {
    spec.is_empty() || candidate == spec || candidate.starts_with(&format!("{spec}/"))
}

#[derive(Clone)]
struct StateChangeOptions {
    show_diff: bool,
    include_meta: bool,
    include_untracked: bool,
    filter: StateChangeFilter,
    path_matcher: PathspecMatcher,
    local_root: Option<String>,
    reference_since: Option<String>,
}

fn state_change_report(
    mut changes: Vec<FileChange>,
    current_snapshot: String,
    options: &StateChangeOptions,
) -> StateChangeReport {
    sort_state_changes(&mut changes);
    let mut files = Vec::with_capacity(changes.len());
    let mut added = 0usize;
    let mut modified = 0usize;
    let mut deleted = 0usize;
    let mut untracked = 0usize;
    for change in changes {
        if !options.allows(&change) {
            continue;
        }
        match change.status {
            "A" => added += 1,
            "M" => modified += 1,
            "D" => deleted += 1,
            "?" => untracked += 1,
            _ => {}
        }
        let (root, path) = if let Some(root) = options.local_root.as_ref() {
            (root.clone(), change.path.clone())
        } else {
            split_state_change_path(&change.path)
        };
        files.push(StateFileChange {
            status: change.status.to_string(),
            root,
            path,
        });
    }
    StateChangeReport {
        current_snapshot,
        total: files.len(),
        added,
        modified,
        deleted,
        untracked,
        files,
    }
}

impl StateChangeOptions {
    fn allows(&self, change: &FileChange) -> bool {
        self.filter.allows(change.status)
            && self
                .path_matcher
                .allows_display_path(&change.path, self.local_root.as_deref())
            && self.reference_since.as_deref().is_none_or(|since| {
                change
                    .order_time
                    .as_deref()
                    .is_none_or(|time| time >= since)
            })
    }
}

fn stream_state_short_changes(
    paths: &Paths,
    conn: &Connection,
    basis_set: &StateBasisSet,
    scope: &StateScope,
    options: StateChangeOptions,
) -> Result<()> {
    let _ = conn;
    let entries = basis_set.entries.clone();
    let local_paths = scope.local_paths;
    let paths = paths.clone();
    let (tx, rx) = mpsc::channel::<LogProducerMessage>();
    thread::spawn(move || {
        let mut handles = Vec::with_capacity(entries.len());
        for entry in entries {
            let tx = tx.clone();
            let paths = paths.clone();
            let snapshot_id = entry.basis.snapshot.clone();
            let root = entry.root;
            let options = options.clone();
            handles.push(thread::spawn(move || {
                let ui = StatusUi::new();
                let result = state_stream_live_file_changes_for_root(
                    &paths,
                    &snapshot_id,
                    &root,
                    local_paths,
                    options,
                    |change, diff_lines| {
                        let line = format!(
                            " {} {}{}",
                            color_change_status(&ui, change.status),
                            color_root_path(&ui, &change.path, local_paths),
                            format_change_tags(&ui, &change)
                        );
                        tx.send(LogProducerMessage::Line(line))
                            .map_err(|err| anyhow!("send state line: {err}"))?;
                        for diff_line in diff_lines {
                            let line = color_state_diff_line(&ui, &diff_line);
                            tx.send(LogProducerMessage::Line(line))
                                .map_err(|err| anyhow!("send state diff line: {err}"))?;
                        }
                        Ok(())
                    },
                );
                if let Err(err) = result {
                    let _ = tx.send(LogProducerMessage::Error(format!("{err:#}")));
                }
            }));
        }
        for handle in handles {
            if handle.join().is_err() {
                let _ = tx.send(LogProducerMessage::Error(
                    "state worker thread panicked".into(),
                ));
            }
        }
        let _ = tx.send(LogProducerMessage::Done);
    });

    if should_use_log_viewer() {
        stream_state_lines_viewer(rx)
    } else {
        stream_state_lines_direct(rx)
    }
}

fn stream_state_lines_direct(rx: mpsc::Receiver<LogProducerMessage>) -> Result<()> {
    stream_state_lines_direct_with_initial(Vec::new(), rx)
}

fn stream_state_lines_direct_with_initial(
    initial: Vec<String>,
    rx: mpsc::Receiver<LogProducerMessage>,
) -> Result<()> {
    let mut stdout = io::stdout();
    for line in initial {
        if let Err(err) = writeln!(stdout, "{line}") {
            if err.kind() == io::ErrorKind::BrokenPipe {
                return Ok(());
            }
            return Err(err.into());
        }
    }
    if let Err(err) = stdout.flush() {
        if err.kind() == io::ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(err.into());
    }
    for message in rx {
        match message {
            LogProducerMessage::Line(line) => {
                if let Err(err) = writeln!(stdout, "{line}") {
                    if err.kind() == io::ErrorKind::BrokenPipe {
                        return Ok(());
                    }
                    return Err(err.into());
                }
                if let Err(err) = stdout.flush() {
                    if err.kind() == io::ErrorKind::BrokenPipe {
                        return Ok(());
                    }
                    return Err(err.into());
                }
            }
            LogProducerMessage::Done => break,
            LogProducerMessage::Error(err) => bail!("{err}"),
        }
    }
    Ok(())
}

fn stream_state_lines_viewer(rx: mpsc::Receiver<LogProducerMessage>) -> Result<()> {
    let height = terminal_height();
    let mut lines = Vec::new();

    if prefetch_state_lines_for_viewer(&rx, height, &mut lines)? == StateStreamMode::Direct {
        return stream_state_lines_direct_with_initial(lines, rx);
    }

    run_log_viewer(lines, rx, "mj state")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StateStreamMode {
    Direct,
    Viewer,
}

fn prefetch_state_lines_for_viewer(
    rx: &mpsc::Receiver<LogProducerMessage>,
    height: usize,
    lines: &mut Vec<String>,
) -> Result<StateStreamMode> {
    loop {
        if state_lines_need_viewer(lines.len(), height) {
            return Ok(StateStreamMode::Viewer);
        }
        match rx.recv() {
            Ok(LogProducerMessage::Line(line)) => {
                lines.push(line);
            }
            Ok(LogProducerMessage::Done) => {
                return Ok(if state_lines_need_viewer(lines.len(), height) {
                    StateStreamMode::Viewer
                } else {
                    StateStreamMode::Direct
                });
            }
            Ok(LogProducerMessage::Error(err)) => bail!("{err}"),
            Err(_) => {
                return Ok(if state_lines_need_viewer(lines.len(), height) {
                    StateStreamMode::Viewer
                } else {
                    StateStreamMode::Direct
                });
            }
        }
    }
}

fn state_lines_need_viewer(line_count: usize, height: usize) -> bool {
    line_count > height
}

fn split_state_change_path(path: &str) -> (String, String) {
    path.split_once('/')
        .map(|(root, rest)| (root.to_string(), rest.to_string()))
        .unwrap_or_else(|| (path.to_string(), String::new()))
}

#[derive(Clone)]
struct StateScope {
    roots: Vec<RootConfig>,
    local_paths: bool,
    local_root: Option<String>,
    cwd_root_prefix: Option<String>,
    single_root_context: bool,
    selected_root: Option<String>,
}

impl StateScope {
    fn single_root_id(&self) -> Option<&str> {
        self.selected_root.as_deref().or(self.local_root.as_deref())
    }
}

fn resolve_state_scope(roots: &[RootConfig], args: &StateArgs) -> Result<StateScope> {
    if let Some(selected) = args.root.as_deref() {
        let root = roots
            .iter()
            .find(|root| root.id == selected)
            .ok_or_else(|| anyhow!("unknown root: {selected}"))?;
        let cwd_root_prefix = current_root_prefix(root)?;
        return Ok(StateScope {
            roots: vec![root.clone()],
            local_paths: false,
            local_root: None,
            cwd_root_prefix,
            single_root_context: true,
            selected_root: Some(root.id.clone()),
        });
    }
    if !args.global
        && let Some((root, prefix)) = infer_current_root_with_prefix(roots)?
    {
        return Ok(StateScope {
            roots: vec![root.clone()],
            local_paths: true,
            local_root: Some(root.id),
            cwd_root_prefix: Some(prefix),
            single_root_context: true,
            selected_root: None,
        });
    }
    Ok(StateScope {
        roots: roots
            .iter()
            .filter(|root| root.status == "active")
            .cloned()
            .collect(),
        local_paths: false,
        local_root: None,
        cwd_root_prefix: None,
        single_root_context: false,
        selected_root: None,
    })
}

fn infer_current_root_with_prefix(roots: &[RootConfig]) -> Result<Option<(RootConfig, String)>> {
    let cwd = std::env::current_dir()?;
    let mut matches = roots
        .iter()
        .filter(|root| root.status == "active")
        .filter_map(|root| {
            let root_path = absolutize_existing_path(&root.path).ok()?;
            if cwd.starts_with(&root_path) {
                let rel = cwd.strip_prefix(&root_path).ok()?;
                Some((
                    root_path.components().count(),
                    root.clone(),
                    normalize_pathspec(rel),
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    matches.sort_by_key(|(depth, _, _)| *depth);
    Ok(matches.pop().map(|(_, root, prefix)| (root, prefix)))
}

fn current_root_prefix(root: &RootConfig) -> Result<Option<String>> {
    let cwd = std::env::current_dir()?;
    let root_path = absolutize_existing_path(&root.path)?;
    if cwd.starts_with(&root_path) {
        Ok(Some(normalize_pathspec(cwd.strip_prefix(&root_path)?)))
    } else {
        Ok(None)
    }
}

fn absolutize_existing_path(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return Ok(path.canonicalize()?);
    }
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn state_live_file_changes_for_basis_set(
    paths: &Paths,
    basis_set: &StateBasisSet,
    local_paths: bool,
    include_meta: bool,
    include_untracked: bool,
) -> Result<Vec<FileChange>> {
    let conn = crate::open_db(paths)?;
    let mut snapshots = BTreeMap::<String, SnapshotManifest>::new();
    let mut changes = Vec::new();
    for entry in &basis_set.entries {
        if !snapshots.contains_key(&entry.basis.snapshot) {
            snapshots.insert(
                entry.basis.snapshot.clone(),
                load_snapshot_by_id(paths, &conn, &entry.basis.snapshot)?,
            );
        }
        let from = snapshots
            .get(&entry.basis.snapshot)
            .expect("snapshot inserted before use");
        changes.extend(state_live_file_changes_for_root(
            paths,
            from,
            &entry.root,
            local_paths,
            include_meta,
            include_untracked,
        )?);
    }
    sort_state_changes(&mut changes);
    Ok(changes)
}

fn state_live_file_changes_for_root(
    paths: &Paths,
    from: &SnapshotManifest,
    root: &RootConfig,
    local_paths: bool,
    include_meta: bool,
    include_untracked: bool,
) -> Result<Vec<FileChange>> {
    let conn = crate::open_db(paths)?;
    let untracked_paths = untracked_paths_for_root(&conn, &root.id)?;
    let path_times = tracked_path_times_for_root(&conn, &root.id)?;
    let mut from_files = root_file_map(paths, Some(from), &root.id)?;
    from_files.retain(|path, _| !untracked_paths.contains(path));
    for path in tombstone_tracked_paths_for_root(&conn, root)? {
        if !state_record_path_is_file_relative(&path) {
            continue;
        }
        from_files
            .entry(path.clone())
            .or_insert_with(|| tracked_tombstone_record(root, path));
    }
    let mut live_files = scan_live_root_for_state(root)?;
    live_files.retain(|path, _| !untracked_paths.contains(path));
    let mut known_files_for_untracked = from_files.keys().cloned().collect::<BTreeSet<_>>();
    known_files_for_untracked.extend(live_files.keys().cloned());
    let mut paths_all = from_files.keys().cloned().collect::<Vec<_>>();
    paths_all.extend(
        live_files
            .keys()
            .filter(|key| !from_files.contains_key(*key))
            .cloned(),
    );
    paths_all.sort();
    let mut changes = Vec::new();
    for path in paths_all {
        let display_path = if local_paths {
            path.clone()
        } else {
            format!("{}/{}", root.id, path)
        };
        match (from_files.get(&path), live_files.get(&path)) {
            (None, Some(live)) => {
                let order_time = added_state_order_time(path_times.get(&path), live);
                changes.push(
                    FileChange::from_record("A", display_path, Some(root), &path, Some(live))
                        .with_state_order(1, order_time),
                );
            }
            (Some(previous), None) => changes.push(
                FileChange::from_record("D", display_path, Some(root), &path, Some(previous))
                    .with_state_order(1, deleted_state_order_time(path_times.get(&path), previous)),
            ),
            (Some(a), Some(b)) => match state_record_change_status(a, b) {
                Some("M") => {
                    let order_time = modified_state_order_time(b);
                    changes.push(
                        FileChange::from_records(
                            "M",
                            display_path,
                            Some(root),
                            &path,
                            Some(a),
                            Some(b),
                        )
                        .with_state_order(1, order_time),
                    );
                }
                Some("m") if include_meta => {
                    let order_time = modified_state_order_time(b);
                    changes.push(
                        FileChange::from_records(
                            "m",
                            display_path,
                            Some(root),
                            &path,
                            Some(a),
                            Some(b),
                        )
                        .with_state_order(1, order_time),
                    );
                }
                _ => {}
            },
            _ => {}
        }
    }
    if include_untracked {
        append_state_untracked_changes(
            root,
            local_paths,
            &known_files_for_untracked,
            &untracked_paths,
            &mut changes,
        )?;
    }
    sort_state_changes(&mut changes);
    Ok(changes)
}

fn state_stream_live_file_changes_for_root(
    paths: &Paths,
    snapshot_id: &str,
    root: &RootConfig,
    local_paths: bool,
    options: StateChangeOptions,
    mut emit: impl FnMut(FileChange, Vec<String>) -> Result<()>,
) -> Result<()> {
    let mut from_files = root_file_map_by_snapshot_id(paths, snapshot_id, &root.id)?;
    let conn = crate::open_db(paths)?;
    let untracked_paths = untracked_paths_for_root(&conn, &root.id)?;
    let path_times = tracked_path_times_for_root(&conn, &root.id)?;
    from_files.retain(|path, _| !untracked_paths.contains(path));
    for path in tombstone_tracked_paths_for_root(&conn, root)? {
        if !state_record_path_is_file_relative(&path) {
            continue;
        }
        from_files
            .entry(path.clone())
            .or_insert_with(|| tracked_tombstone_record(root, path));
    }
    let mut known_files_for_untracked = from_files.keys().cloned().collect::<BTreeSet<_>>();
    let mut seen_live_paths = BTreeSet::new();
    let mut records = Vec::new();
    scan_live_root_for_state_each(root, false, |path, live| {
        if untracked_paths.contains(&path) {
            return Ok(());
        }
        seen_live_paths.insert(path.clone());
        let display_path = state_display_path(root, &path, local_paths);
        match from_files.remove(&path) {
            None => {
                let order_time = added_state_order_time(path_times.get(&path), &live);
                records.push(StateFileChangeRecord {
                    change: FileChange::from_record(
                        "A",
                        display_path,
                        Some(root),
                        &path,
                        Some(&live),
                    )
                    .with_state_order(1, order_time),
                    previous: None,
                    current: Some(live),
                });
            }
            Some(previous) if previous.kind == "tombstone" => {
                let order_time = added_state_order_time(path_times.get(&path), &live);
                records.push(StateFileChangeRecord {
                    change: FileChange::from_record(
                        "A",
                        display_path,
                        Some(root),
                        &path,
                        Some(&live),
                    )
                    .with_state_order(1, order_time),
                    previous: None,
                    current: Some(live),
                });
            }
            Some(previous) => {
                let fast_status = state_stream_record_fast_status(&previous, &live);
                let content_changed = fast_status == Some("M")
                    || (fast_status != Some("M")
                        && state_stream_payload_changed(root, &path, &previous)?);
                if content_changed {
                    let order_time = modified_state_order_time(&live);
                    let change = FileChange::from_records(
                        "M",
                        display_path,
                        Some(root),
                        &path,
                        Some(&previous),
                        Some(&live),
                    )
                    .with_state_order(1, order_time);
                    records.push(StateFileChangeRecord {
                        change,
                        previous: Some(previous),
                        current: Some(live),
                    });
                } else if fast_status == Some("m") && options.include_meta {
                    let order_time = modified_state_order_time(&live);
                    let change = FileChange::from_records(
                        "m",
                        display_path,
                        Some(root),
                        &path,
                        Some(&previous),
                        Some(&live),
                    )
                    .with_state_order(1, order_time);
                    records.push(StateFileChangeRecord {
                        change,
                        previous: Some(previous),
                        current: Some(live),
                    });
                }
            }
        }
        Ok(())
    })?;
    known_files_for_untracked.extend(seen_live_paths);
    for (path, previous) in &from_files {
        let change = FileChange::from_record(
            "D",
            state_display_path(root, path, local_paths),
            Some(root),
            path,
            Some(previous),
        )
        .with_state_order(1, deleted_state_order_time(path_times.get(path), previous));
        records.push(StateFileChangeRecord {
            change,
            previous: Some(previous.clone()),
            current: None,
        });
    }
    if options.include_untracked {
        let mut untracked_changes = Vec::new();
        append_state_untracked_changes(
            root,
            local_paths,
            &known_files_for_untracked,
            &untracked_paths,
            &mut untracked_changes,
        )?;
        for change in untracked_changes {
            records.push(StateFileChangeRecord {
                change,
                previous: None,
                current: None,
            });
        }
    }
    sort_state_change_records(&mut records);
    for record in records {
        if !options.allows(&record.change) {
            continue;
        }
        let diff = state_live_change_diff(
            paths,
            root,
            state_change_rel_path(&record.change.path, root, local_paths),
            record.previous.as_ref(),
            record.current.as_ref(),
            options.show_diff && record.change.status != "?",
        )?;
        emit(record.change, diff)?;
    }
    Ok(())
}

struct StateFileChangeRecord {
    change: FileChange,
    previous: Option<FileRecord>,
    current: Option<FileRecord>,
}

fn stream_state_lifecycle_changes(
    paths: &Paths,
    conn: &Connection,
    scope: &StateScope,
    options: StateChangeOptions,
) -> Result<()> {
    let _ = conn;
    let roots = scope.roots.clone();
    let local_paths = scope.local_paths;
    let paths = paths.clone();
    let (tx, rx) = mpsc::channel::<LogProducerMessage>();
    thread::spawn(move || {
        let result = (|| -> Result<()> {
            let ui = StatusUi::new();
            let mut all_records = Vec::<(RootConfig, StateFileChangeRecord)>::new();
            for root in roots {
                let records = state_lifecycle_file_change_records_for_root(
                    &paths,
                    &root,
                    local_paths,
                    options.include_meta,
                    options.include_untracked,
                )?;
                all_records.extend(records.into_iter().map(|record| (root.clone(), record)));
            }
            all_records.sort_by(|(_, a), (_, b)| compare_state_change_order(&a.change, &b.change));
            for (root, record) in all_records {
                if !options.allows(&record.change) {
                    continue;
                }
                let diff = state_live_change_diff(
                    &paths,
                    &root,
                    state_change_rel_path(&record.change.path, &root, local_paths),
                    record.previous.as_ref(),
                    record.current.as_ref(),
                    options.show_diff && record.change.status != "?",
                )?;
                let line = format!(
                    " {} {}{}",
                    color_change_status(&ui, record.change.status),
                    color_root_path(&ui, &record.change.path, local_paths),
                    format_change_tags(&ui, &record.change)
                );
                tx.send(LogProducerMessage::Line(line))
                    .map_err(|err| anyhow!("send state line: {err}"))?;
                for diff_line in diff {
                    let line = color_state_diff_line(&ui, &diff_line);
                    tx.send(LogProducerMessage::Line(line))
                        .map_err(|err| anyhow!("send state diff line: {err}"))?;
                }
            }
            Ok(())
        })();
        if let Err(err) = result {
            let _ = tx.send(LogProducerMessage::Error(format!("{err:#}")));
        }
        let _ = tx.send(LogProducerMessage::Done);
    });

    if should_use_log_viewer() {
        stream_state_lines_viewer(rx)
    } else {
        stream_state_lines_direct(rx)
    }
}

fn state_lifecycle_file_changes(
    paths: &Paths,
    conn: &Connection,
    roots: &[RootConfig],
    local_paths: bool,
    include_meta: bool,
    include_untracked: bool,
) -> Result<Vec<FileChange>> {
    let _ = conn;
    let mut changes = Vec::new();
    for root in roots {
        changes.extend(
            state_lifecycle_file_change_records_for_root(
                paths,
                root,
                local_paths,
                include_meta,
                include_untracked,
            )?
            .into_iter()
            .map(|record| record.change),
        );
    }
    sort_state_changes(&mut changes);
    Ok(changes)
}

fn state_lifecycle_file_change_records_for_root(
    paths: &Paths,
    root: &RootConfig,
    local_paths: bool,
    include_meta: bool,
    include_untracked: bool,
) -> Result<Vec<StateFileChangeRecord>> {
    let conn = crate::open_db(paths)?;
    let untracked_paths = untracked_paths_for_root(&conn, &root.id)?;
    let path_times = tracked_path_times_for_root(&conn, &root.id)?;
    let mut live_files = scan_live_root_for_state_metadata(root)?;
    live_files.retain(|path, record| {
        state_record_path_is_file_relative(path) && record.kind != "directory"
    });

    let mut root_added_files = first_root_snapshot_file_map(paths, &conn, root)?;
    root_added_files.retain(|path, record| {
        state_record_path_is_file_relative(path)
            && record.kind != "directory"
            && !untracked_paths.contains(path)
    });

    let tombstones = tombstone_tracked_paths_for_root(&conn, root)?;
    let mut managed_paths = tracked_paths_for_root(&conn, &root.id)?;
    managed_paths.extend(root_added_files.keys().cloned());
    managed_paths.extend(tombstones.iter().cloned());
    for path in &untracked_paths {
        managed_paths.remove(path);
    }
    let known_paths_for_untracked = managed_paths.clone();

    let mut records = Vec::new();
    for path in managed_paths {
        let display_path = state_display_path(root, &path, local_paths);
        match live_files.get(&path) {
            Some(live) if live.kind != "directory" => {
                if let Some(previous) = root_added_files.get(&path) {
                    match state_lifecycle_initial_change_status(root, &path, previous, live)? {
                        Some("M") => {
                            records.push(StateFileChangeRecord {
                                change: FileChange::from_records(
                                    "M",
                                    display_path,
                                    Some(root),
                                    &path,
                                    Some(previous),
                                    Some(live),
                                )
                                .with_state_order(1, modified_state_order_time(live)),
                                previous: Some(previous.clone()),
                                current: Some(live.clone()),
                            });
                        }
                        Some("m") if include_meta => {
                            records.push(StateFileChangeRecord {
                                change: FileChange::from_records(
                                    "m",
                                    display_path,
                                    Some(root),
                                    &path,
                                    Some(previous),
                                    Some(live),
                                )
                                .with_state_order(1, modified_state_order_time(live)),
                                previous: Some(previous.clone()),
                                current: Some(live.clone()),
                            });
                        }
                        _ => {
                            let order_time = added_state_order_time(path_times.get(&path), live);
                            records.push(StateFileChangeRecord {
                                change: FileChange::from_record(
                                    "A",
                                    display_path,
                                    Some(root),
                                    &path,
                                    Some(live),
                                )
                                .with_state_order(1, order_time),
                                previous: None,
                                current: Some(live.clone()),
                            });
                        }
                    }
                } else {
                    let order_time = added_state_order_time(path_times.get(&path), live);
                    records.push(StateFileChangeRecord {
                        change: FileChange::from_record(
                            "A",
                            display_path,
                            Some(root),
                            &path,
                            Some(live),
                        )
                        .with_state_order(1, order_time),
                        previous: None,
                        current: Some(live.clone()),
                    });
                }
            }
            None if root_added_files.contains_key(&path) || tombstones.contains(&path) => {
                let previous = root_added_files
                    .get(&path)
                    .cloned()
                    .unwrap_or_else(|| tracked_tombstone_record(root, path.clone()));
                records.push(StateFileChangeRecord {
                    change: FileChange::from_record(
                        "D",
                        display_path,
                        Some(root),
                        &path,
                        Some(&previous),
                    )
                    .with_state_order(
                        1,
                        deleted_state_order_time(path_times.get(&path), &previous),
                    ),
                    previous: Some(previous),
                    current: None,
                });
            }
            _ => {}
        }
    }

    if include_untracked {
        let mut untracked_changes = Vec::new();
        append_state_untracked_changes(
            root,
            local_paths,
            &known_paths_for_untracked,
            &untracked_paths,
            &mut untracked_changes,
        )?;
        records.extend(
            untracked_changes
                .into_iter()
                .map(|change| StateFileChangeRecord {
                    change,
                    previous: None,
                    current: None,
                }),
        );
    }
    sort_state_change_records(&mut records);
    Ok(records)
}

fn first_root_snapshot_file_map(
    paths: &Paths,
    conn: &Connection,
    root: &RootConfig,
) -> Result<BTreeMap<String, FileRecord>> {
    let root_ids = BTreeSet::from([root.id.clone()]);
    let first = first_snapshots_for_roots(paths, conn, &root_ids)?;
    let Some(snapshot_id) = first.get(&root.id) else {
        return Ok(BTreeMap::new());
    };
    let snapshot = load_snapshot_header_by_id(paths, conn, snapshot_id)?;
    root_file_map(paths, Some(&snapshot), &root.id)
}

fn state_change_rel_path<'a>(
    display_path: &'a str,
    root: &RootConfig,
    local_paths: bool,
) -> &'a str {
    if local_paths {
        display_path
    } else {
        display_path
            .strip_prefix(&format!("{}/", root.id))
            .unwrap_or(display_path)
    }
}

fn scan_live_root_for_state_metadata(root: &RootConfig) -> Result<BTreeMap<String, FileRecord>> {
    let mut records = BTreeMap::new();
    scan_live_root_for_state_each(root, false, |path, record| {
        records.insert(path, record);
        Ok(())
    })?;
    Ok(records)
}

fn append_state_untracked_changes(
    root: &RootConfig,
    local_paths: bool,
    known_paths: &BTreeSet<String>,
    explicit_untracked: &BTreeSet<String>,
    changes: &mut Vec<FileChange>,
) -> Result<()> {
    let mut live = scan_live_root_for_state_metadata(root)?;
    for path in explicit_untracked {
        if !live.contains_key(path)
            && let Some(record) = state_live_record_for_relative_path(root, path)?
        {
            live.insert(path.clone(), record);
        }
    }
    let mut skipped_dirs = Vec::<String>::new();
    for (path, record) in live {
        if path_is_under_any(&path, &skipped_dirs) || known_paths.contains(&path) {
            continue;
        }
        if record.kind == "directory" {
            if known_paths
                .iter()
                .any(|known| known.starts_with(&format!("{path}/")))
            {
                continue;
            }
            let display_path = format!("{}/", state_display_path(root, &path, local_paths));
            changes.push(
                FileChange::from_record("?", display_path, Some(root), &path, Some(&record))
                    .with_state_order(2, state_record_order_time(&record)),
            );
            skipped_dirs.push(path);
            continue;
        }
        if explicit_untracked.contains(&path) || !known_paths.contains(&path) {
            changes.push(
                FileChange::from_record(
                    "?",
                    state_display_path(root, &path, local_paths),
                    Some(root),
                    &path,
                    Some(&record),
                )
                .with_state_order(2, state_record_order_time(&record)),
            );
        }
    }
    Ok(())
}

fn state_live_record_for_relative_path(
    root: &RootConfig,
    rel_s: &str,
) -> Result<Option<FileRecord>> {
    let rel = Path::new(rel_s);
    if rel.is_absolute()
        || rel.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Ok(None);
    }
    let path = root.path.join(rel);
    let link_meta = match fs::symlink_metadata(&path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if link_meta.is_dir() {
        return Ok(Some(FileRecord {
            root_id: root.id.clone(),
            path: rel_s.to_string(),
            kind: "directory".into(),
            size: 0,
            mode: crate::fs_meta::file_mode(&link_meta),
            modified: crate::util::modified_secs(&link_meta),
            uid: crate::fs_meta::file_uid(&link_meta),
            gid: crate::fs_meta::file_gid(&link_meta),
            xattrs: crate::fs_meta::read_xattrs(&path),
            payload: Payload::Directory,
        }));
    }
    if link_meta.file_type().is_symlink() && !root.follow_symlinks {
        return Ok(Some(FileRecord {
            root_id: root.id.clone(),
            path: rel_s.to_string(),
            kind: "symlink".into(),
            size: 0,
            mode: crate::fs_meta::file_mode(&link_meta),
            modified: crate::util::modified_secs(&link_meta),
            uid: crate::fs_meta::file_uid(&link_meta),
            gid: crate::fs_meta::file_gid(&link_meta),
            xattrs: BTreeMap::new(),
            payload: Payload::Symlink {
                target: fs::read_link(&path)?.to_string_lossy().to_string(),
            },
        }));
    }
    if let Some(special_kind) = crate::fs_meta::special_file_kind(&link_meta) {
        return Ok(Some(FileRecord {
            root_id: root.id.clone(),
            path: rel_s.to_string(),
            kind: "special".into(),
            size: 0,
            mode: crate::fs_meta::file_mode(&link_meta),
            modified: crate::util::modified_secs(&link_meta),
            uid: crate::fs_meta::file_uid(&link_meta),
            gid: crate::fs_meta::file_gid(&link_meta),
            xattrs: crate::fs_meta::read_xattrs(&path),
            payload: Payload::Special { special_kind },
        }));
    }
    let meta = if link_meta.file_type().is_symlink() {
        fs::metadata(&path)?
    } else {
        link_meta
    };
    if !meta.is_file() {
        return Ok(None);
    }
    Ok(Some(FileRecord {
        root_id: root.id.clone(),
        path: rel_s.to_string(),
        kind: "file".into(),
        size: meta.len(),
        mode: crate::fs_meta::file_mode(&meta),
        modified: crate::util::modified_secs(&meta),
        uid: crate::fs_meta::file_uid(&meta),
        gid: crate::fs_meta::file_gid(&meta),
        xattrs: crate::fs_meta::read_xattrs(&path),
        payload: Payload::NormalBlob {
            oid: String::new(),
            object_key: String::new(),
        },
    }))
}

fn path_is_under_any(path: &str, dirs: &[String]) -> bool {
    dirs.iter().any(|dir| path.starts_with(&format!("{dir}/")))
}

fn tracked_tombstone_record(root: &RootConfig, path: String) -> FileRecord {
    FileRecord {
        root_id: root.id.clone(),
        path,
        kind: "tombstone".into(),
        size: 0,
        mode: 0,
        modified: None,
        uid: None,
        gid: None,
        xattrs: BTreeMap::new(),
        payload: Payload::Directory,
    }
}

fn state_display_path(root: &RootConfig, path: &str, local_paths: bool) -> String {
    if local_paths {
        path.to_string()
    } else {
        format!("{}/{}", root.id, path)
    }
}

fn state_live_change_diff(
    paths: &Paths,
    root: &RootConfig,
    path: &str,
    previous: Option<&FileRecord>,
    live: Option<&FileRecord>,
    show_diff: bool,
) -> Result<Vec<String>> {
    const STATE_DIFF_MAX_BYTES: u64 = 1024 * 1024;
    if !show_diff {
        return Ok(Vec::new());
    }
    if previous.is_some_and(|record| record.kind != "file")
        || live.is_some_and(|record| record.kind != "file")
    {
        return Ok(Vec::new());
    }
    if previous.is_some_and(|record| record.size > STATE_DIFF_MAX_BYTES)
        || live.is_some_and(|record| record.size > STATE_DIFF_MAX_BYTES)
    {
        return Ok(vec!["    (diff omitted: file is larger than 1 MiB)".into()]);
    }
    let old_bytes = previous
        .map(|record| state_record_payload_bytes(paths, record))
        .transpose()?
        .unwrap_or_default();
    let new_bytes = if live.is_some() {
        if root.follow_symlinks {
            stable_read(
                &root.path.join(Path::new(path)),
                root.snapshot_mode.as_str(),
            )?
        } else {
            stable_read_in_root(&root.path, Path::new(path), root.snapshot_mode.as_str())?
        }
    } else {
        Vec::new()
    };
    let Some(old_text) = state_diff_text(old_bytes) else {
        return Ok(vec!["    (diff omitted: previous file is binary)".into()]);
    };
    let Some(new_text) = state_diff_text(new_bytes) else {
        return Ok(vec!["    (diff omitted: current file is binary)".into()]);
    };
    Ok(state_unified_diff_lines(&old_text, &new_text))
}

fn state_record_payload_bytes(paths: &Paths, record: &FileRecord) -> Result<Vec<u8>> {
    let conn = crate::open_db(paths)?;
    if let Some((oid, object_key)) = payload_blob_ref(&record.payload) {
        return crate::read_blob_payload(paths, &conn, oid, object_key);
    }
    if let Some((_, manifest_key, chunk_count)) = payload_large_ref(&record.payload) {
        let manifest = crate::restore_runtime::read_large_manifest_for_restore(paths, manifest_key)
            .with_context(|| format!("read large manifest {manifest_key}"))?;
        if manifest.chunks.len() != chunk_count {
            bail!(
                "large manifest chunk count mismatch for {manifest_key}: payload={chunk_count} manifest={}",
                manifest.chunks.len()
            );
        }
        let mut bytes = Vec::with_capacity(record.size.min(usize::MAX as u64) as usize);
        for chunk in &manifest.chunks {
            bytes.extend(crate::read_large_chunk(paths, chunk)?);
        }
        return Ok(bytes);
    }
    Ok(Vec::new())
}

fn state_diff_text(bytes: Vec<u8>) -> Option<String> {
    if bytes.contains(&0) {
        return None;
    }
    String::from_utf8(bytes).ok()
}

fn state_unified_diff_lines(old: &str, new: &str) -> Vec<String> {
    if old == new {
        return Vec::new();
    }
    const DIFF_CONTEXT_LINES: usize = 3;
    let diff = TextDiff::from_lines(old, new);
    let mut out = Vec::new();
    for group in diff.grouped_ops(DIFF_CONTEXT_LINES) {
        out.push("    @@".into());
        for op in group {
            for change in diff.iter_changes(&op) {
                let sign = match change.tag() {
                    ChangeTag::Delete => "-",
                    ChangeTag::Insert => "+",
                    ChangeTag::Equal => " ",
                };
                out.push(format!(
                    "    {sign}{}",
                    change.value().trim_end_matches(['\r', '\n'])
                ));
            }
        }
    }
    out
}

fn state_stream_payload_changed(
    root: &RootConfig,
    path: &str,
    previous: &FileRecord,
) -> Result<bool> {
    let Some(previous_oid) = state_payload_oid(&previous.payload) else {
        return Ok(false);
    };
    let bytes = if root.follow_symlinks {
        stable_read(
            &root.path.join(Path::new(path)),
            root.snapshot_mode.as_str(),
        )?
    } else {
        stable_read_in_root(&root.path, Path::new(path), root.snapshot_mode.as_str())?
    };
    Ok(blake3_hex(&bytes) != previous_oid)
}

fn scan_live_root_for_state(root: &RootConfig) -> Result<BTreeMap<String, FileRecord>> {
    let mut records = BTreeMap::new();
    scan_live_root_for_state_each(root, true, |path, record| {
        records.insert(path, record);
        Ok(())
    })?;
    Ok(records)
}

fn scan_live_root_for_state_each(
    root: &RootConfig,
    hash_files: bool,
    mut visit: impl FnMut(String, FileRecord) -> Result<()>,
) -> Result<()> {
    if root.status != "active" {
        return Ok(());
    }
    if !root.path.exists() {
        return Ok(());
    }
    let ignore = build_ignore(root)?;
    let walker = WalkDir::new(&root.path)
        .follow_links(root.follow_symlinks)
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
    for entry in walker {
        let entry = entry?;
        if entry.path() == root.path {
            continue;
        }
        let rel = entry.path().strip_prefix(&root.path)?.to_path_buf();
        if !root_record_is_managed(root, &ignore, &rel, entry.file_type().is_dir()) {
            continue;
        }
        let rel_s = path_to_slash(&rel);
        let link_meta = fs::symlink_metadata(entry.path())?;
        let record = if entry.file_type().is_dir() {
            FileRecord {
                root_id: root.id.clone(),
                path: rel_s.clone(),
                kind: "directory".into(),
                size: 0,
                mode: crate::fs_meta::file_mode(&link_meta),
                modified: crate::util::modified_secs(&link_meta),
                uid: crate::fs_meta::file_uid(&link_meta),
                gid: crate::fs_meta::file_gid(&link_meta),
                xattrs: crate::fs_meta::read_xattrs(entry.path()),
                payload: Payload::Directory,
            }
        } else if link_meta.file_type().is_symlink() && !root.follow_symlinks {
            FileRecord {
                root_id: root.id.clone(),
                path: rel_s.clone(),
                kind: "symlink".into(),
                size: 0,
                mode: crate::fs_meta::file_mode(&link_meta),
                modified: crate::util::modified_secs(&link_meta),
                uid: crate::fs_meta::file_uid(&link_meta),
                gid: crate::fs_meta::file_gid(&link_meta),
                xattrs: BTreeMap::new(),
                payload: Payload::Symlink {
                    target: fs::read_link(entry.path())?.to_string_lossy().to_string(),
                },
            }
        } else if let Some(special_kind) = crate::fs_meta::special_file_kind(&link_meta) {
            FileRecord {
                root_id: root.id.clone(),
                path: rel_s.clone(),
                kind: "special".into(),
                size: 0,
                mode: crate::fs_meta::file_mode(&link_meta),
                modified: crate::util::modified_secs(&link_meta),
                uid: crate::fs_meta::file_uid(&link_meta),
                gid: crate::fs_meta::file_gid(&link_meta),
                xattrs: crate::fs_meta::read_xattrs(entry.path()),
                payload: Payload::Special { special_kind },
            }
        } else {
            let meta = if link_meta.file_type().is_symlink() {
                fs::metadata(entry.path())?
            } else {
                link_meta
            };
            if !meta.is_file() {
                continue;
            }
            let oid = if hash_files {
                let bytes = if root.follow_symlinks {
                    stable_read(entry.path(), root.snapshot_mode.as_str())?
                } else {
                    stable_read_in_root(&root.path, &rel, root.snapshot_mode.as_str())?
                };
                blake3_hex(&bytes)
            } else {
                String::new()
            };
            FileRecord {
                root_id: root.id.clone(),
                path: rel_s.clone(),
                kind: "file".into(),
                size: meta.len(),
                mode: crate::fs_meta::file_mode(&meta),
                modified: crate::util::modified_secs(&meta),
                uid: crate::fs_meta::file_uid(&meta),
                gid: crate::fs_meta::file_gid(&meta),
                xattrs: crate::fs_meta::read_xattrs(entry.path()),
                payload: Payload::NormalBlob {
                    oid,
                    object_key: String::new(),
                },
            }
        };
        visit(rel_s, record)?;
    }
    Ok(())
}

fn state_record_change_status(a: &FileRecord, b: &FileRecord) -> Option<&'static str> {
    if a.kind != b.kind || !state_payloads_match(&a.payload, &b.payload) {
        return Some("M");
    }
    if state_metadata_changed(a, b) {
        return Some("m");
    }
    None
}

fn state_stream_record_fast_status(a: &FileRecord, b: &FileRecord) -> Option<&'static str> {
    if a.kind != b.kind {
        return Some("M");
    }
    if a.kind == "file" && a.size != b.size {
        return Some("M");
    }
    match (&a.payload, &b.payload) {
        (Payload::Symlink { target: a }, Payload::Symlink { target: b }) if a != b => {
            return Some("M");
        }
        (Payload::Special { special_kind: a }, Payload::Special { special_kind: b }) if a != b => {
            return Some("M");
        }
        _ => {}
    }
    if state_metadata_changed(a, b) {
        return Some("m");
    }
    None
}

fn state_lifecycle_initial_change_status(
    root: &RootConfig,
    path: &str,
    previous: &FileRecord,
    live: &FileRecord,
) -> Result<Option<&'static str>> {
    let fast_status = state_stream_record_fast_status(previous, live);
    if fast_status == Some("M") {
        return Ok(Some("M"));
    }
    if previous.kind == "file"
        && live.kind == "file"
        && state_stream_payload_changed(root, path, previous)?
    {
        return Ok(Some("M"));
    }
    Ok(fast_status)
}

fn state_metadata_changed(a: &FileRecord, b: &FileRecord) -> bool {
    a.size != b.size
        || a.mode != b.mode
        || a.modified != b.modified
        || a.uid != b.uid
        || a.gid != b.gid
        || a.xattrs != b.xattrs
}

fn state_payloads_match(a: &Payload, b: &Payload) -> bool {
    match (a, b) {
        (Payload::Directory, Payload::Directory) => true,
        (Payload::Symlink { target: a }, Payload::Symlink { target: b }) => a == b,
        (Payload::Special { special_kind: a }, Payload::Special { special_kind: b }) => a == b,
        _ => state_payload_oid(a).is_some_and(|a| state_payload_oid(b) == Some(a)),
    }
}

fn state_payload_oid(payload: &Payload) -> Option<&str> {
    match payload {
        Payload::InlineSmall { oid, .. }
        | Payload::NormalBlob { oid, .. }
        | Payload::ChunkedBlob { oid, .. }
        | Payload::LargeObject { oid, .. }
        | Payload::Blob { oid, .. }
        | Payload::Large { oid, .. } => Some(oid),
        Payload::Directory | Payload::Symlink { .. } | Payload::Special { .. } => None,
    }
}

fn resolve_state_basis(paths: &Paths, conn: &Connection, input: &str) -> Result<StateBasis> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("state reference must not be empty");
    }
    if trimmed.starts_with("op-") {
        return resolve_state_operation_basis(paths, conn, trimmed);
    }
    if trimmed.starts_with("snap-") {
        let snapshot = resolve_snapshot_id(conn, trimmed)?;
        return state_basis_from_snapshot(paths, conn, trimmed, "snapshot", snapshot, None, None);
    }
    if let Some(resolved_at) = resolve_state_clock_time(trimmed)? {
        let snapshot = snapshot_id_at(conn, &resolved_at)?;
        return state_basis_from_snapshot(
            paths,
            conn,
            trimmed,
            "local-time",
            snapshot,
            None,
            Some(resolved_at),
        );
    }
    if let Ok(resolved) = parse_duration_ago(trimmed) {
        let resolved_at = resolved.to_rfc3339();
        let snapshot = snapshot_id_at(conn, &resolved_at)?;
        return state_basis_from_snapshot(
            paths,
            conn,
            trimmed,
            "relative-time",
            snapshot,
            None,
            Some(resolved_at),
        );
    }
    let resolved_at = parse_time(trimmed)?;
    let snapshot = snapshot_id_at(conn, &resolved_at)?;
    state_basis_from_snapshot(
        paths,
        conn,
        trimmed,
        "time",
        snapshot,
        None,
        Some(resolved_at),
    )
}

fn earliest_state_basis(paths: &Paths, conn: &Connection) -> Result<Option<StateBasis>> {
    let snapshot = conn
        .query_row(
            "select id from snapshots order by created_at asc, id asc limit 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    snapshot
        .map(|snapshot| {
            state_basis_from_snapshot(
                paths,
                conn,
                "init",
                "initial-snapshot",
                snapshot,
                None,
                None,
            )
        })
        .transpose()
}

fn resolve_state_basis_optional(
    paths: &Paths,
    conn: &Connection,
    input: &str,
) -> Result<Option<StateBasis>> {
    match resolve_state_basis(paths, conn, input) {
        Ok(basis) => Ok(Some(basis)),
        Err(err) if is_state_reference_before_first_snapshot(&err) => {
            earliest_state_basis(paths, conn)
        }
        Err(err) => Err(err),
    }
}

fn is_state_reference_before_first_snapshot(err: &anyhow::Error) -> bool {
    err.to_string().starts_with("no snapshot at or before ")
}

fn resolve_state_operation_basis(
    paths: &Paths,
    conn: &Connection,
    input: &str,
) -> Result<StateBasis> {
    let op = resolve_operation(conn, input)?;
    let snapshot = op
        .after_snapshot
        .clone()
        .or_else(|| op.before_snapshot.clone())
        .ok_or_else(|| anyhow!("operation has no snapshot: {input}"))?;
    state_basis_from_snapshot(
        paths,
        conn,
        input,
        "operation",
        snapshot,
        Some(op.id),
        Some(op.created_at),
    )
}

fn state_basis_from_snapshot(
    paths: &Paths,
    conn: &Connection,
    input: &str,
    kind: &str,
    snapshot: String,
    operation: Option<String>,
    resolved_at: Option<String>,
) -> Result<StateBasis> {
    let manifest = load_snapshot_header_by_id(paths, conn, &snapshot)?;
    Ok(StateBasis {
        input: input.to_string(),
        kind: kind.to_string(),
        snapshot,
        snapshot_created_at: manifest.timestamp.to_rfc3339(),
        operation,
        operation_created_at: if kind == "operation" {
            resolved_at.clone()
        } else {
            None
        },
        resolved_at,
    })
}

fn resolve_operation(conn: &Connection, input: &str) -> Result<OperationExport> {
    query_operation_resolved(conn, input)
}

fn resolve_snapshot_id(conn: &Connection, input: &str) -> Result<String> {
    let mut stmt = conn.prepare("select id from snapshots where id like ?1 order by created_at")?;
    let rows = stmt.query_map(params![format!("{input}%")], |row| row.get(0))?;
    let matches = rows.collect::<rusqlite::Result<Vec<String>>>()?;
    match matches.as_slice() {
        [id] => Ok(id.clone()),
        [] => bail!("unknown snapshot: {input}"),
        _ => bail!("ambiguous snapshot prefix: {input}"),
    }
}

fn resolve_state_clock_time(input: &str) -> Result<Option<String>> {
    let time = NaiveTime::parse_from_str(input, "%H:%M")
        .or_else(|_| NaiveTime::parse_from_str(input, "%H:%M:%S"));
    let Ok(time) = time else {
        return Ok(None);
    };
    let now = Local::now();
    let mut date = now.date_naive();
    let mut local = local_datetime(date.and_time(time), input)?;
    if local > now {
        date = date
            .pred_opt()
            .ok_or_else(|| anyhow!("invalid local date for time reference: {input}"))?;
        local = local_datetime(date.and_time(time), input)?;
    }
    Ok(Some(local.with_timezone(&Utc).to_rfc3339()))
}

fn local_datetime(naive: chrono::NaiveDateTime, input: &str) -> Result<DateTime<Local>> {
    match Local.from_local_datetime(&naive) {
        chrono::LocalResult::Single(value) => Ok(value),
        chrono::LocalResult::Ambiguous(earliest, _) => Ok(earliest),
        chrono::LocalResult::None => bail!("invalid local time reference: {input}"),
    }
}

fn pending_journal_event_count(records: &[crate::majutsu_db::EventJournalRecord]) -> usize {
    let last_snapshot_finish = records
        .iter()
        .filter(|event| event.is_snapshot_finish())
        .map(|event| event.observed_at)
        .max();
    records
        .iter()
        .filter(|event| {
            event.is_pending_trigger()
                && event.remote_journal_synced_at.is_none()
                && last_snapshot_finish
                    .map(|finished_at| event.observed_at > finished_at)
                    .unwrap_or(true)
        })
        .count()
}

fn watch_attribution_issue(records: &[crate::majutsu_db::EventJournalRecord]) -> Option<String> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let latest_watch_event = records
        .iter()
        .filter(|event| matches!(event.kind.as_str(), "watch-start" | "watch-backend-error"))
        .max_by_key(|event| event.observed_at)?;
    if latest_watch_event.kind == "watch-backend-error" {
        return Some(format!(
            "fanotify watch failed: {}; run a root-owned fanotify daemon to record the mutating process pid",
            latest_watch_event.detail
        ));
    }
    if latest_watch_event.detail.contains("backend=fanotify") {
        None
    } else {
        Some(format!(
            "watch is not using fanotify: {}; run a root-owned fanotify daemon to record the mutating process pid",
            latest_watch_event.detail
        ))
    }
}

#[derive(Serialize)]
struct StateBranch {
    name: String,
    snapshot: String,
    active: bool,
}

#[derive(Serialize)]
struct StateRef {
    name: String,
    value: String,
}

struct StateSnapshot {
    id: String,
    created_at: String,
    parent_id: Option<String>,
}

fn state_refs(conn: &Connection) -> Result<Vec<StateRef>> {
    let mut stmt = conn.prepare("select name, value from refs order by name")?;
    let rows = stmt.query_map([], |row| {
        Ok(StateRef {
            name: row.get(0)?,
            value: row.get(1)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn state_branches(refs: &[StateRef], active_branch: Option<&str>) -> Vec<StateBranch> {
    refs.iter()
        .filter_map(|reference| {
            let name = reference.name.strip_prefix("branches/")?;
            Some(StateBranch {
                name: name.to_string(),
                snapshot: reference.value.clone(),
                active: active_branch == Some(name),
            })
        })
        .collect()
}

fn ref_value_for_state(conn: &Connection, name: &str) -> Result<Option<String>> {
    conn.query_row(
        "select value from refs where name=?1",
        params![name],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn latest_snapshot_for_state(conn: &Connection) -> Result<Option<StateSnapshot>> {
    conn.query_row(
        "select id, created_at, parent_id from snapshots order by created_at desc limit 1",
        [],
        |row| {
            Ok(StateSnapshot {
                id: row.get(0)?,
                created_at: row.get(1)?,
                parent_id: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

struct StatusUi {
    color: bool,
}

impl StatusUi {
    fn new() -> Self {
        let color = std::env::var_os("NO_COLOR").is_none()
            && std::env::var("MJ_COLOR").as_deref() != Ok("never")
            && (std::env::var("MJ_COLOR").as_deref() == Ok("always")
                || (io::stdout().is_terminal() && std::env::var("TERM").as_deref() != Ok("dumb")));
        Self { color }
    }

    fn heading(&self, value: &str) -> String {
        self.paint(value, "1;36")
    }

    fn severity(&self, value: &str, severity: Severity) -> String {
        let code = match severity {
            Severity::Good => "1;32",
            Severity::Warn => "1;33",
            Severity::Bad => "1;31",
            Severity::Info => "1;34",
        };
        self.paint(value, code)
    }

    fn paint(&self, value: &str, code: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{value}\x1b[0m")
        } else {
            value.to_string()
        }
    }
}

#[derive(Clone, Copy)]
enum Severity {
    Good,
    Warn,
    Bad,
    Info,
}

struct StatusOverview<'a> {
    current: &'a str,
    roots_total: usize,
    roots_active: usize,
    roots_problem: usize,
    daemon: &'a DaemonHealth,
    remote: &'a RemoteStatus,
    remote_head: &'a RemoteHeadStatus,
    upload_total: usize,
    upload_retrying: usize,
    upload_delayed: usize,
    upload_backpressure: bool,
    encryption: &'a str,
    state_bytes: u64,
    state_disk_bytes: u64,
    object_bytes: u64,
    object_disk_bytes: u64,
    queue_bytes: u64,
    queue_disk_bytes: u64,
    blob_bytes: u64,
    pack_bytes: u64,
    chunk_bytes: u64,
}

struct StatusCard {
    title: String,
    state: String,
    detail: String,
    severity: Severity,
}

fn print_status_overview(
    out: &mut String,
    width: usize,
    ui: &StatusUi,
    overview: StatusOverview<'_>,
) {
    writeln!(out, "{}", ui.heading("Overview")).expect("write status output");
    let remote_severity = if !overview.remote.configured {
        Severity::Warn
    } else if overview.remote.open_error.is_some() {
        Severity::Bad
    } else if !overview.remote_head.synced {
        Severity::Warn
    } else {
        Severity::Good
    };
    let upload_severity = if overview.upload_backpressure {
        Severity::Bad
    } else if overview.upload_total > 0 {
        Severity::Warn
    } else {
        Severity::Good
    };
    let encryption_severity = if overview.encryption == "none" {
        Severity::Warn
    } else {
        Severity::Good
    };
    let root_severity = if overview.roots_problem > 0 {
        Severity::Warn
    } else {
        Severity::Good
    };
    let daemon_severity = daemon_severity(overview.daemon, overview.roots_active);
    let cards = [
        StatusCard {
            title: "snapshot".into(),
            state: shorten_middle(overview.current, 24),
            detail: "current ref".into(),
            severity: Severity::Info,
        },
        StatusCard {
            title: "daemon".into(),
            state: daemon_state_label(overview.daemon).into(),
            detail: overview
                .daemon
                .pid
                .map(|pid| format!("pid={pid} ipc={}", overview.daemon.ipc_available))
                .unwrap_or_else(|| "no process".into()),
            severity: daemon_severity,
        },
        StatusCard {
            title: "roots".into(),
            state: format!("{}/{} active", overview.roots_active, overview.roots_total),
            detail: format!("problem={}", overview.roots_problem),
            severity: root_severity,
        },
        StatusCard {
            title: "remote".into(),
            state: overview.remote_head.label().into(),
            detail: if overview.remote.configured {
                overview.remote.backend.clone()
            } else {
                overview.remote.summary().into()
            },
            severity: remote_severity,
        },
        StatusCard {
            title: "uploads".into(),
            state: if overview.upload_total == 0 {
                "clear".into()
            } else {
                format!("{} queued", overview.upload_total)
            },
            detail: format!(
                "retrying={} delayed={}",
                overview.upload_retrying, overview.upload_delayed
            ),
            severity: upload_severity,
        },
        StatusCard {
            title: "encryption".into(),
            state: overview.encryption.into(),
            detail: if overview.encryption == "none" {
                "unencrypted state".into()
            } else {
                "encrypted state".into()
            },
            severity: encryption_severity,
        },
        StatusCard {
            title: "state".into(),
            state: format_bytes_compact(overview.state_bytes),
            detail: format!("disk {}", format_bytes_compact(overview.state_disk_bytes)),
            severity: Severity::Info,
        },
    ];
    print_card_grid(out, width, ui, &cards);
    print_usage_bars(
        out,
        width,
        &[
            ("state", overview.state_bytes),
            ("objects", overview.object_bytes),
            ("queue", overview.queue_bytes),
        ],
    );
    print_usage_bars(
        out,
        width,
        &[
            ("disk", overview.state_disk_bytes),
            ("obj-d", overview.object_disk_bytes),
            ("que-d", overview.queue_disk_bytes),
        ],
    );
    print_usage_bars(
        out,
        width,
        &[
            ("blobs", overview.blob_bytes),
            ("packs", overview.pack_bytes),
            ("chunks", overview.chunk_bytes),
        ],
    );
}

fn print_card_grid(out: &mut String, width: usize, ui: &StatusUi, cards: &[StatusCard]) {
    let columns = if width >= 108 {
        3
    } else if width >= 74 {
        2
    } else {
        1
    };
    let gap = if columns > 1 { 2 } else { 0 };
    let card_width = ((width.saturating_sub(2 + gap * (columns - 1))) / columns).max(28);
    for (row_index, row) in cards.chunks(columns).enumerate() {
        let first_line = if row_index == 0 { 0 } else { 1 };
        for line_index in first_line..4 {
            write!(out, "  ").expect("write status output");
            for (i, card) in row.iter().enumerate() {
                if i > 0 {
                    write!(out, "{:gap$}", "", gap = gap).expect("write status output");
                }
                let line = card_line(card, card_width, line_index);
                let rendered = if line_index == 1 {
                    color_card_state(ui, card, &line)
                } else {
                    line
                };
                write!(out, "{rendered}").expect("write status output");
            }
            writeln!(out).expect("write status output");
        }
    }
}

fn card_line(card: &StatusCard, width: usize, line_index: usize) -> String {
    let inner = width.saturating_sub(2).max(10);
    match line_index {
        0 => format!("+{}+", "-".repeat(inner)),
        1 => {
            let title = truncate_text(&card.title.to_uppercase(), 10);
            let state_space = inner.saturating_sub(title.len() + 1);
            let state = truncate_text(&card.state, state_space);
            format!("|{title} {state:<state_space$}|")
        }
        2 => {
            let detail = truncate_text(&card.detail, inner);
            format!("|{detail:<inner$}|")
        }
        _ => format!("+{}+", "-".repeat(inner)),
    }
}

fn color_card_state(ui: &StatusUi, card: &StatusCard, line: &str) -> String {
    if !ui.color {
        return line.to_string();
    }
    let trimmed_state = card.state.trim();
    if trimmed_state.is_empty() {
        return line.to_string();
    }
    line.replacen(trimmed_state, &ui.severity(trimmed_state, card.severity), 1)
}

fn print_usage_bars(out: &mut String, width: usize, values: &[(&str, u64)]) {
    let max_value = values
        .iter()
        .map(|(_, value)| *value)
        .max()
        .unwrap_or(0)
        .max(1);
    let label_width = values
        .iter()
        .map(|(label, _)| label.len())
        .max()
        .unwrap_or(6)
        .max(6);
    let value_width = values
        .iter()
        .map(|(_, value)| format_bytes_compact(*value).len())
        .max()
        .unwrap_or(5)
        .max(5);
    let bar_width = width
        .saturating_sub(2 + label_width + 2 + value_width + 3)
        .clamp(8, 32);
    for (label, value) in values {
        let filled = ((*value as f64 / max_value as f64) * bar_width as f64).round() as usize;
        let filled = filled.min(bar_width);
        writeln!(
            out,
            "  {label:<label_width$} [{}{}] {:>value_width$}",
            "#".repeat(filled),
            "-".repeat(bar_width - filled),
            format_bytes_compact(*value),
            label_width = label_width,
            value_width = value_width
        )
        .expect("write status output");
    }
}

fn truncate_text(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_string();
    }
    if width <= 1 {
        return value.chars().take(width).collect();
    }
    let mut out = value.chars().take(width - 1).collect::<String>();
    out.push('~');
    out
}

fn shorten_middle(value: &str, width: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= width {
        return value.to_string();
    }
    if width <= 3 {
        return truncate_text(value, width);
    }
    let prefix = (width - 1) / 2;
    let suffix = width - 1 - prefix;
    let left = chars.iter().take(prefix).collect::<String>();
    let right = chars
        .iter()
        .skip(chars.len().saturating_sub(suffix))
        .collect::<String>();
    format!("{left}~{right}")
}

fn compact_timestamp(value: &str) -> String {
    if value.len() >= 19 {
        let month_day = &value[5..10];
        let time = &value[11..19];
        format!("{month_day} {time}")
    } else {
        value.to_string()
    }
}

fn root_remote_sync_label(
    remote_ack: Option<&RemoteRootAck>,
    remote_manifest: Option<&crate::majutsu_core::SnapshotManifest>,
    root_id: &str,
    current_tree_id: &str,
    remote_last_synced: Option<&str>,
) -> String {
    if let Some(remote_ack) = remote_ack {
        if remote_ack.tree_id != current_tree_id {
            return "lagging".into();
        }
        return remote_ack
            .synced_at
            .as_deref()
            .or(remote_last_synced)
            .map(compact_timestamp)
            .unwrap_or_else(|| "synced".into());
    }
    let Some(remote_manifest) = remote_manifest else {
        return "-".into();
    };
    let Some(remote_root) = remote_manifest.root_trees.get(root_id) else {
        return "missing".into();
    };
    if remote_root.tree_id != current_tree_id {
        return "lagging".into();
    }
    remote_last_synced
        .map(compact_timestamp)
        .unwrap_or_else(|| "synced".into())
}

fn print_kv(out: &mut String, width: usize, key: &str, value: &str) {
    let prefix = format!("  {key:<18} ");
    print_wrapped(out, &prefix, value, width);
}

fn print_table<const N: usize>(
    out: &mut String,
    width: usize,
    headers: &[&str; N],
    rows: &[[&str; N]],
) {
    let mut widths = [0usize; N];
    for (i, column_width) in widths.iter_mut().enumerate() {
        *column_width = headers[i].len();
    }
    for row in rows {
        for (i, column_width) in widths.iter_mut().enumerate() {
            *column_width = (*column_width).max(row[i].len());
        }
    }
    let available_width = width.saturating_sub(2 + ((N.saturating_sub(1)) * 2));
    while widths.iter().sum::<usize>() > available_width {
        let Some((index, _)) = widths
            .iter()
            .enumerate()
            .filter(|(index, column_width)| **column_width > headers[*index].len().max(8))
            .max_by_key(|(_, column_width)| **column_width)
        else {
            break;
        };
        widths[index] = widths[index].saturating_sub(1);
    }
    if N > 1 {
        let fixed_width: usize = widths[..N - 1].iter().sum::<usize>() + ((N - 1) * 2) + 2;
        let max_last = width
            .saturating_sub(fixed_width)
            .max(headers[N - 1].len())
            .max(4);
        widths[N - 1] = widths[N - 1].min(max_last);
    }
    write!(out, "  ").expect("write status output");
    for (i, column_width) in widths.iter().enumerate() {
        if i > 0 {
            write!(out, "  ").expect("write status output");
        }
        write!(out, "{:<width$}", headers[i], width = *column_width).expect("write status output");
    }
    writeln!(out).expect("write status output");
    write!(out, "  ").expect("write status output");
    for (i, column_width) in widths.iter().enumerate() {
        if i > 0 {
            write!(out, "  ").expect("write status output");
        }
        write!(
            out,
            "{:<width$}",
            "-".repeat(*column_width),
            width = *column_width
        )
        .expect("write status output");
    }
    writeln!(out).expect("write status output");
    for row in rows {
        print_table_row(out, row, &widths, width);
    }
}

fn print_table_row<const N: usize>(
    out: &mut String,
    row: &[&str; N],
    widths: &[usize; N],
    terminal_width: usize,
) {
    let mut line_prefix = String::from("  ");
    for i in 0..N.saturating_sub(1) {
        if i > 0 {
            line_prefix.push_str("  ");
        }
        line_prefix.push_str(&format!(
            "{:<width$}",
            truncate_text(row[i], widths[i]),
            width = widths[i]
        ));
    }
    if N > 1 {
        line_prefix.push_str("  ");
        print_wrapped(out, &line_prefix, row[N - 1], terminal_width);
    } else if let Some(value) = row.first() {
        print_wrapped(out, &line_prefix, value, terminal_width);
    }
}

fn print_wrapped(out: &mut String, prefix: &str, value: &str, width: usize) {
    let available = width.saturating_sub(prefix.len()).max(4);
    let lines = wrap_text(value, available);
    if let Some((first, rest)) = lines.split_first() {
        writeln!(out, "{prefix}{first}").expect("write status output");
        let continuation = " ".repeat(prefix.len());
        for line in rest {
            writeln!(out, "{continuation}{line}").expect("write status output");
        }
    } else {
        writeln!(out, "{prefix}").expect("write status output");
    }
}

fn wrap_text(value: &str, width: usize) -> Vec<String> {
    if value.len() <= width {
        return vec![value.to_string()];
    }
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in value.split_whitespace() {
        if line.is_empty() {
            line.push_str(word);
        } else if line.len() + 1 + word.len() <= width {
            line.push(' ');
            line.push_str(word);
        } else {
            lines.push(line);
            line = word.to_string();
        }
        while line.len() > width {
            let rest = line.split_off(width);
            lines.push(line);
            line = rest;
        }
    }
    if !line.is_empty() {
        lines.push(line);
    }
    if lines.is_empty() {
        vec![String::new()]
    } else {
        lines
    }
}

fn terminal_width() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|width| *width >= 40)
        .or_else(|| detect_terminal_size().map(|size| size.cols))
        .unwrap_or(100)
}

fn terminal_height() -> usize {
    std::env::var("LINES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|height| *height >= 5)
        .or_else(|| detect_terminal_size().map(|size| size.rows))
        .unwrap_or(24)
}

fn emit_status_output(output: &str, height: usize, args: &StatusArgs) -> Result<()> {
    if !args.no_pager
        && should_page_status(output, height, args.pager)
        && write_to_pager(output).is_ok()
    {
        return Ok(());
    }
    print!("{output}");
    Ok(())
}

fn emit_status_output_auto(output: &str, height: usize) -> Result<()> {
    if should_page_status(output, height, false) && write_to_pager(output).is_ok() {
        return Ok(());
    }
    print!("{output}");
    Ok(())
}

fn should_page_status(output: &str, height: usize, force: bool) -> bool {
    should_page_status_with_tty(output, height, force, io::stdout().is_terminal())
}

fn should_page_status_with_tty(
    output: &str,
    height: usize,
    force: bool,
    stdout_is_terminal: bool,
) -> bool {
    force || (stdout_is_terminal && output.lines().count() > height)
}

fn write_to_pager(output: &str) -> Result<()> {
    let pager = std::env::var("MJ_PAGER")
        .or_else(|_| std::env::var("PAGER"))
        .unwrap_or_else(|_| "less -R".into());
    let mut parts = pager.split_whitespace();
    let Some(program) = parts.next() else {
        bail!("pager command is empty");
    };
    let mut child = Command::new(program)
        .args(parts)
        .env(
            "LESS",
            std::env::var("LESS").unwrap_or_else(|_| "-R".into()),
        )
        .stdin(Stdio::piped())
        .spawn()
        .with_context(|| format!("start pager: {pager}"))?;
    if let Some(stdin) = child.stdin.as_mut() {
        std::io::Write::write_all(stdin, output.as_bytes()).context("write status to pager")?;
    }
    let status = child.wait().context("wait for pager")?;
    if !status.success() {
        bail!("pager exited with {status}");
    }
    Ok(())
}

struct TerminalSize {
    cols: usize,
    rows: usize,
}

#[cfg(unix)]
fn detect_terminal_size() -> Option<TerminalSize> {
    #[repr(C)]
    struct Winsize {
        ws_row: libc::c_ushort,
        ws_col: libc::c_ushort,
        ws_xpixel: libc::c_ushort,
        ws_ypixel: libc::c_ushort,
    }

    let mut winsize: Winsize = unsafe { mem::zeroed() };
    let result = unsafe {
        libc::ioctl(
            io::stdout().as_raw_fd(),
            libc::TIOCGWINSZ,
            &mut winsize as *mut Winsize,
        )
    };
    if result == 0 && winsize.ws_col >= 40 && winsize.ws_row >= 5 {
        Some(TerminalSize {
            cols: winsize.ws_col as usize,
            rows: winsize.ws_row as usize,
        })
    } else {
        None
    }
}

#[cfg(not(unix))]
fn detect_terminal_size() -> Option<TerminalSize> {
    None
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {} ({bytes} B)", UNITS[unit])
    }
}

fn format_bytes_compact(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else if value >= 10.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[derive(Default)]
struct StatusDbStats {
    snapshots: i64,
    operations: i64,
    refs: i64,
    blobs: i64,
    blob_bytes: i64,
    large_objects: i64,
    large_object_bytes: i64,
    chunks: i64,
    chunk_bytes: i64,
    packs: i64,
    pack_bytes: i64,
    large_pins: i64,
    remote_refs: i64,
}

fn read_status_db_stats(conn: &Connection) -> Result<StatusDbStats> {
    Ok(StatusDbStats {
        snapshots: count_table(conn, "snapshots")?,
        operations: count_table(conn, "operations")?,
        refs: count_table(conn, "refs")?,
        blobs: count_table(conn, "blobs")?,
        blob_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from blobs")?,
        large_objects: count_table(conn, "large_objects")?,
        large_object_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from large_objects")?,
        chunks: count_table(conn, "chunks")?,
        chunk_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from chunks")?,
        packs: count_table(conn, "packs")?,
        pack_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from packs")?,
        large_pins: count_table(conn, "large_pins")?,
        remote_refs: count_table(conn, "remote_refs")?,
    })
}

fn count_table(conn: &Connection, table: &str) -> Result<i64> {
    let sql = format!("select count(*) from {table}");
    sum_i64(conn, &sql)
}

fn sum_i64(conn: &Connection, sql: &str) -> Result<i64> {
    conn.query_row(sql, [], |row| row.get(0))
        .map_err(Into::into)
}

struct StorageStats {
    state_files: u64,
    state_bytes: u64,
    state_disk_bytes: u64,
    objects_files: u64,
    objects_bytes: u64,
    objects_disk_bytes: u64,
    loose_blob_files: u64,
    loose_blob_bytes: u64,
    loose_blob_disk_bytes: u64,
    pack_files: u64,
    pack_bytes: u64,
    pack_disk_bytes: u64,
    large_files: u64,
    large_bytes: u64,
    large_disk_bytes: u64,
    tree_files: u64,
    tree_bytes: u64,
    tree_disk_bytes: u64,
    logs_files: u64,
    logs_bytes: u64,
    logs_disk_bytes: u64,
    queue_files: u64,
    queue_bytes: u64,
    queue_disk_bytes: u64,
}

fn read_storage_stats(paths: &Paths) -> Result<StorageStats> {
    let state = dir_stats(&paths.home)?;
    let objects = dir_stats(&paths.home.join("objects"))?;
    let loose_blobs = dir_stats(&paths.home.join("objects/blobs"))?;
    let packs = dir_stats(&paths.home.join("objects/packs"))?;
    let large = dir_stats(&paths.home.join("objects/large"))?;
    let trees = dir_stats(&paths.home.join("objects/trees"))?;
    let logs = dir_stats(&paths.logs)?;
    let queue = dir_stats(&paths.home.join("queue"))?;
    Ok(StorageStats {
        state_files: state.files,
        state_bytes: state.bytes,
        state_disk_bytes: state.disk_bytes,
        objects_files: objects.files,
        objects_bytes: objects.bytes,
        objects_disk_bytes: objects.disk_bytes,
        loose_blob_files: loose_blobs.files,
        loose_blob_bytes: loose_blobs.bytes,
        loose_blob_disk_bytes: loose_blobs.disk_bytes,
        pack_files: packs.files,
        pack_bytes: packs.bytes,
        pack_disk_bytes: packs.disk_bytes,
        large_files: large.files,
        large_bytes: large.bytes,
        large_disk_bytes: large.disk_bytes,
        tree_files: trees.files,
        tree_bytes: trees.bytes,
        tree_disk_bytes: trees.disk_bytes,
        logs_files: logs.files,
        logs_bytes: logs.bytes,
        logs_disk_bytes: logs.disk_bytes,
        queue_files: queue.files,
        queue_bytes: queue.bytes,
        queue_disk_bytes: queue.disk_bytes,
    })
}

#[derive(Default)]
struct DirStats {
    files: u64,
    bytes: u64,
    disk_bytes: u64,
}

fn dir_stats(path: &Path) -> Result<DirStats> {
    if !path.exists() {
        return Ok(DirStats::default());
    }
    let mut stats = DirStats::default();
    for entry in WalkDir::new(path).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_file() {
            stats.files += 1;
            let metadata = entry.metadata()?;
            stats.bytes += metadata.len();
            stats.disk_bytes += file_disk_bytes(&metadata);
        }
    }
    Ok(stats)
}

#[cfg(unix)]
fn file_disk_bytes(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn file_disk_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}

fn count_json_files(path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(|ext| ext.to_str()) == Some("json")
        {
            count += 1;
        }
    }
    Ok(count)
}

struct RemoteStatus {
    configured: bool,
    backend: String,
    url: Option<String>,
    resolved: Option<String>,
    open_error: Option<String>,
    lifecycle_rules: Option<bool>,
    object_tags: Option<bool>,
    storage_class_on_put: Option<bool>,
    restore_archived_object: Option<bool>,
    multipart_upload: Option<bool>,
    range_get: Option<bool>,
    conditional_put: Option<bool>,
}

struct RemoteHeadStatus {
    current: Option<String>,
    remote_current: Option<String>,
    remote_last_synced: Option<String>,
    root_acks: BTreeMap<String, RemoteRootAck>,
    synced: bool,
    detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteRootAck {
    snapshot_id: String,
    tree_id: String,
    tree_key: String,
    file_count: usize,
    synced_at: Option<String>,
}

impl RemoteHeadStatus {
    fn label(&self) -> &str {
        if self.synced {
            "synced (cached)"
        } else {
            self.detail.as_str()
        }
    }
}

impl RemoteStatus {
    fn summary(&self) -> &str {
        if !self.configured {
            return "not configured";
        }
        if self.open_error.is_some() {
            return "configured, unavailable";
        }
        "configured"
    }
}

fn read_remote_head_status(
    conn: &Connection,
    config: &Config,
    remote: &RemoteStatus,
    current: Option<&str>,
) -> Result<RemoteHeadStatus> {
    if !remote.configured {
        return Ok(RemoteHeadStatus {
            current: current.map(str::to_string),
            remote_current: None,
            remote_last_synced: None,
            root_acks: BTreeMap::new(),
            synced: false,
            detail: "not configured".into(),
        });
    }
    if remote.open_error.is_some() {
        return Ok(RemoteHeadStatus {
            current: current.map(str::to_string),
            remote_current: None,
            remote_last_synced: None,
            root_acks: BTreeMap::new(),
            synced: false,
            detail: "remote unavailable".into(),
        });
    }
    let Some(remote_name) = remote.resolved.as_deref() else {
        return Ok(RemoteHeadStatus {
            current: current.map(str::to_string),
            remote_current: None,
            remote_last_synced: None,
            root_acks: BTreeMap::new(),
            synced: false,
            detail: "unknown".into(),
        });
    };
    let host_prefix = remote_host_label(&config.host.name);
    let ref_name = host_current_ref_key(&host_prefix);
    let last_synced_ref_name = host_last_synced_ref_key(&host_prefix);
    let remote_current = conn
        .query_row(
            "select value from remote_refs where remote=?1 and name=?2",
            params![remote_name, ref_name],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let remote_last_synced = conn
        .query_row(
            "select value from remote_refs where remote=?1 and name=?2",
            params![remote_name, last_synced_ref_name],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let root_acks = read_remote_root_acks(conn, remote_name, &host_prefix)?;
    let synced = current.is_some() && current.map(str::to_string) == remote_current;
    let detail = match (current, remote_current.as_deref()) {
        (Some(_), Some(_)) if synced => "synced".into(),
        (Some(_), Some(_)) => "lagging (cached)".into(),
        (Some(_), None) => "not synced (cached)".into(),
        (None, Some(_)) => "remote only (cached)".into(),
        (None, None) => "no snapshot".into(),
    };
    Ok(RemoteHeadStatus {
        current: current.map(str::to_string),
        remote_current,
        remote_last_synced,
        root_acks,
        synced,
        detail,
    })
}

fn read_remote_root_acks(
    conn: &Connection,
    remote_name: &str,
    host_id: &str,
) -> Result<BTreeMap<String, RemoteRootAck>> {
    let prefix = host_root_ack_ref_prefix(host_id);
    let suffix = "/ack";
    let mut stmt = conn.prepare(
        "select name, value from remote_refs
         where remote=?1 and name like ?2
         order by name",
    )?;
    let rows = stmt.query_map(params![remote_name, format!("{prefix}%")], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut acks = BTreeMap::new();
    for row in rows {
        let (name, value) = row?;
        let Some(root_id) = name
            .strip_prefix(&prefix)
            .and_then(|name| name.strip_suffix(suffix))
        else {
            continue;
        };
        let expected = host_root_ack_ref_key(host_id, root_id);
        if name != expected {
            continue;
        }
        let ack: RemoteRootAck = serde_json::from_str(&value)
            .with_context(|| format!("parse cached remote root ack {name}"))?;
        acks.insert(root_id.to_string(), ack);
    }
    Ok(acks)
}

fn read_remote_status(config: &Config) -> Result<RemoteStatus> {
    let Some(remote_config) = &config.remote else {
        return Ok(RemoteStatus {
            configured: false,
            backend: "none".into(),
            url: None,
            resolved: None,
            open_error: None,
            lifecycle_rules: None,
            object_tags: None,
            storage_class_on_put: None,
            restore_archived_object: None,
            multipart_upload: None,
            range_get: None,
            conditional_put: None,
        });
    };
    let remote_url = remote_config.url().context("resolve remote URL")?;
    let backend = if remote_url.starts_with("file://") {
        "file"
    } else if remote_url.starts_with("s3://") {
        "s3"
    } else {
        "unknown"
    };
    match open_remote(remote_config) {
        Ok(remote) => {
            let capabilities = remote.capabilities();
            Ok(RemoteStatus {
                configured: true,
                backend: backend.into(),
                url: Some(remote_url),
                resolved: Some(remote.describe()),
                open_error: None,
                lifecycle_rules: Some(capabilities.lifecycle_rules),
                object_tags: Some(capabilities.object_tags),
                storage_class_on_put: Some(capabilities.storage_class_on_put),
                restore_archived_object: Some(capabilities.restore_archived_object),
                multipart_upload: Some(capabilities.multipart_upload),
                range_get: Some(capabilities.range_get),
                conditional_put: Some(capabilities.conditional_put),
            })
        }
        Err(err) => Ok(RemoteStatus {
            configured: true,
            backend: backend.into(),
            url: Some(remote_url),
            resolved: None,
            open_error: Some(format!("{err:#}")),
            lifecycle_rules: None,
            object_tags: None,
            storage_class_on_put: None,
            restore_archived_object: None,
            multipart_upload: None,
            range_get: None,
            conditional_put: None,
        }),
    }
}

fn daemon_state_label(daemon: &DaemonHealth) -> &'static str {
    match daemon.state {
        DaemonHealthState::Running => {
            if daemon.ipc_available {
                "running"
            } else {
                "running, ipc unavailable"
            }
        }
        DaemonHealthState::Stopped => "stopped",
        DaemonHealthState::Stale => "stale pid",
    }
}

fn daemon_severity(daemon: &DaemonHealth, active_roots: usize) -> Severity {
    match daemon.state {
        DaemonHealthState::Running if daemon.ipc_available => Severity::Good,
        DaemonHealthState::Running => Severity::Warn,
        DaemonHealthState::Stopped | DaemonHealthState::Stale if active_roots > 0 => Severity::Bad,
        DaemonHealthState::Stopped | DaemonHealthState::Stale => Severity::Warn,
    }
}

fn print_daemon_section(
    out: &mut String,
    width: usize,
    ui: &StatusUi,
    daemon: &DaemonHealth,
    root_count: usize,
) {
    writeln!(out, "{}", ui.heading("Daemon")).expect("write status output");
    let severity = daemon_severity(daemon, root_count);
    print_kv(
        out,
        width,
        "State",
        &ui.severity(daemon_state_label(daemon), severity),
    );
    print_kv(
        out,
        width,
        "PID",
        &daemon
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "(none)".into()),
    );
    print_kv(out, width, "IPC", &daemon.ipc_available.to_string());
    if !daemon.is_healthy() && root_count > 0 {
        print_kv(
            out,
            width,
            "Attention",
            "active roots are not protected by the watch daemon",
        );
    }
}

fn print_remote_section(out: &mut String, width: usize, ui: &StatusUi, remote: &RemoteStatus) {
    writeln!(out, "{}", ui.heading("Remote")).expect("write status output");
    print_kv(out, width, "Configured", &remote.configured.to_string());
    print_kv(out, width, "Backend", &remote.backend);
    if let Some(url) = &remote.url {
        print_kv(out, width, "URL", url);
    }
    if let Some(resolved) = &remote.resolved {
        print_kv(out, width, "Resolved", resolved);
    }
    if let Some(error) = &remote.open_error {
        print_kv(out, width, "Open error", error);
    }
    if remote.lifecycle_rules.is_some() {
        print_table(
            out,
            width,
            &["CAPABILITY", "SUPPORTED"],
            &[
                [
                    "lifecycle rules",
                    &display_option_bool(remote.lifecycle_rules),
                ],
                ["object tags", &display_option_bool(remote.object_tags)],
                [
                    "storage class on put",
                    &display_option_bool(remote.storage_class_on_put),
                ],
                [
                    "restore archived object",
                    &display_option_bool(remote.restore_archived_object),
                ],
                [
                    "multipart upload",
                    &display_option_bool(remote.multipart_upload),
                ],
                ["range get", &display_option_bool(remote.range_get)],
                [
                    "conditional put",
                    &display_option_bool(remote.conditional_put),
                ],
            ],
        );
    }
}

fn display_option_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".into())
}

pub(crate) fn log_cmd(paths: &Paths, args: LogArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    if !args.operations && should_use_log_viewer() {
        return print_change_log_viewer(paths, args);
    }
    let conn = crate::open_db(paths)?;
    let mut args = args;
    if args.limit.is_none() {
        args.limit = Some(DEFAULT_LOG_LIMIT);
    }
    if args.operations {
        print_op_log(paths, &conn, &args)
    } else {
        print_change_log(paths, &conn, &args)
    }
}

const DEFAULT_LOG_LIMIT: usize = 20;

fn should_use_log_viewer() -> bool {
    io::stdout().is_terminal() && std::env::var("TERM").as_deref() != Ok("dumb")
}

fn print_change_log(paths: &Paths, conn: &Connection, args: &LogArgs) -> Result<()> {
    let mut output = String::new();
    write_change_log_lines(paths, conn, args, |line| {
        output.push_str(line);
        output.push('\n');
        Ok(())
    })?;
    print!("{output}");
    Ok(())
}

fn write_change_log_lines<F>(
    paths: &Paths,
    conn: &Connection,
    args: &LogArgs,
    mut write_line: F,
) -> Result<()>
where
    F: FnMut(&str) -> Result<()>,
{
    let mut printed = 0usize;
    let ui = StatusUi::new();
    let file_limit = if args.full { usize::MAX } else { 120 };
    let limit = args.limit.unwrap_or(usize::MAX);
    let batch_size = limit.max(20).saturating_mul(4).min(500);
    let configured_root_list = roots(conn)?;
    let path_matcher =
        PathspecMatcher::for_log(&args.pathspecs, &configured_root_list, args.root.as_deref())?;
    let configured_roots = configured_root_list
        .into_iter()
        .map(|root| (root.id.clone(), root))
        .collect::<BTreeMap<_, _>>();
    let mut offset = 0usize;
    while printed < limit {
        let operations = recent_change_operations(conn, batch_size, offset)?;
        if operations.is_empty() {
            break;
        }
        offset += operations.len();
        for op in operations {
            if printed >= limit {
                break;
            }
            let changes = operation_file_changes(
                paths,
                conn,
                &op,
                args.root.as_deref(),
                args.full,
                &configured_roots,
            )?;
            let changes = changes
                .into_iter()
                .filter(|change| path_matcher.allows_display_path(&change.path, None))
                .collect::<Vec<_>>();
            if changes.is_empty() {
                continue;
            }
            let summary = summarize_changes(&changes);
            write_line(&format!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                ui.paint(&display_log_timestamp(&op.created_at), "1;34"),
                display_op_id(&op.id),
                ui.paint(&op.kind, "36"),
                operation_origin_label(&op),
                summary,
                op.message.as_deref().unwrap_or_default()
            ))?;
            let change_count = changes.len();
            for change in changes.into_iter().take(file_limit) {
                write_line(&format!(
                    "{}\t{}{}",
                    color_change_status(&ui, change.status),
                    color_root_path(&ui, &change.path, false),
                    format_change_tags(&ui, &change)
                ))?;
            }
            if change_count > file_limit {
                write_line(&ui.paint(
                    &format!(
                        "... {} more changed files hidden; use --full to show all",
                        change_count - file_limit
                    ),
                    "2",
                ))?;
            }
            printed += 1;
        }
    }
    if args.root.is_none() && args.limit.is_some() && printed >= limit {
        write_line(&StatusUi::new().paint(
            &format!(
                "... showing {printed} change operations; use `mj log --limit N` for more or `mj log --operations` for internal operation records"
            ),
            "2",
        ))?;
    }
    Ok(())
}

fn print_change_log_viewer(paths: &Paths, args: LogArgs) -> Result<()> {
    let height = terminal_height();
    let likely_needs_viewer = log_viewer_likely_needed(paths, &args, height).unwrap_or(true);
    let (tx, rx) = mpsc::channel::<LogProducerMessage>();
    let producer_paths = paths.clone();
    let producer_args = args.clone();
    thread::spawn(move || {
        let result = (|| -> Result<()> {
            let conn = crate::open_db(&producer_paths)?;
            write_change_log_lines(&producer_paths, &conn, &producer_args, |line| {
                tx.send(LogProducerMessage::Line(line.to_string()))
                    .map_err(|err| anyhow!("send log line: {err}"))?;
                Ok(())
            })?;
            Ok(())
        })();
        let _ = tx.send(match result {
            Ok(()) => LogProducerMessage::Done,
            Err(err) => LogProducerMessage::Error(format!("{err:#}")),
        });
    });

    let mut lines = Vec::new();
    let mut done = false;
    let prefetch_timeout = if likely_needs_viewer {
        StdDuration::from_millis(120)
    } else {
        StdDuration::from_millis(250)
    };
    let prefetch_started = std::time::Instant::now();
    while lines.len() <= height {
        let elapsed = prefetch_started.elapsed();
        if elapsed >= prefetch_timeout {
            break;
        }
        match rx.recv_timeout(prefetch_timeout - elapsed) {
            Ok(LogProducerMessage::Line(line)) => lines.push(line),
            Ok(LogProducerMessage::Done) => {
                done = true;
                break;
            }
            Ok(LogProducerMessage::Error(err)) => bail!("{err}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                break;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                done = true;
                break;
            }
        }
    }

    if done && lines.len() <= height {
        for line in lines {
            println!("{line}");
        }
        return Ok(());
    }

    run_log_viewer(lines, rx, "mj log")
}

fn log_viewer_likely_needed(paths: &Paths, args: &LogArgs, height: usize) -> Result<bool> {
    if args.limit.is_none() {
        return Ok(true);
    }
    let Some(limit) = args.limit else {
        return Ok(true);
    };
    if limit == 0 {
        return Ok(false);
    }
    if args.root.is_some() {
        return Ok(limit.saturating_mul(2) > height);
    }
    let conn = crate::open_db(paths)?;
    let count = count_recent_change_operations(&conn)?;
    Ok(count.min(limit).saturating_mul(2) > height)
}

enum LogProducerMessage {
    Line(String),
    Done,
    Error(String),
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self> {
        terminal::enable_raw_mode().context("enable raw mode")?;
        execute!(
            io::stdout(),
            terminal::EnterAlternateScreen,
            terminal::DisableLineWrap,
            terminal::Clear(ClearType::All),
            cursor::Hide
        )
        .context("enter log viewer")?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(
            io::stdout(),
            cursor::Show,
            terminal::EnableLineWrap,
            terminal::LeaveAlternateScreen
        );
        let _ = terminal::disable_raw_mode();
    }
}

fn run_log_viewer(
    initial_lines: Vec<String>,
    rx: mpsc::Receiver<LogProducerMessage>,
    label: &'static str,
) -> Result<()> {
    let _guard = TerminalGuard::enter()?;
    let mut lines = initial_lines;
    let mut done = false;
    let mut offset = 0usize;
    let mut needs_redraw = true;
    loop {
        if needs_redraw {
            draw_log_viewer(&lines, offset, done, label)?;
            needs_redraw = false;
        }
        let old_offset = offset;
        if event::poll(StdDuration::from_millis(30)).context("poll log viewer input")?
            && let Event::Key(key) = event::read().context("read log viewer input")?
        {
            let page = terminal_height().saturating_sub(1).max(1);
            let max_offset = max_log_viewer_offset(lines.len(), page);
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Char('j') | KeyCode::Down => {
                    offset = offset.saturating_add(1).min(max_offset);
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    offset = offset.saturating_sub(1);
                }
                KeyCode::Char(' ') | KeyCode::PageDown => {
                    offset = offset.saturating_add(page).min(max_offset);
                }
                KeyCode::Char('b') | KeyCode::PageUp => {
                    offset = offset.saturating_sub(page);
                }
                KeyCode::Char('g') | KeyCode::Home => {
                    offset = 0;
                }
                KeyCode::Char('G') | KeyCode::End if done => {
                    offset = max_offset;
                }
                _ => {}
            }
            if offset != old_offset {
                needs_redraw = true;
            }
        }
        let changed = drain_log_messages(&rx, &mut lines, &mut done, 256)?;
        if changed {
            needs_redraw = true;
        } else if needs_redraw
            && offset.abs_diff(old_offset) == 1
            && draw_log_viewer_single_scroll(&lines, old_offset, offset, done, label).is_ok()
        {
            needs_redraw = false;
        }
        if done && lines.is_empty() {
            break;
        }
    }
    Ok(())
}

fn max_log_viewer_offset(line_count: usize, page_height: usize) -> usize {
    line_count.saturating_sub(page_height.max(1))
}

fn drain_log_messages(
    rx: &mpsc::Receiver<LogProducerMessage>,
    lines: &mut Vec<String>,
    done: &mut bool,
    max_messages: usize,
) -> Result<bool> {
    let mut changed = false;
    for _ in 0..max_messages {
        let Ok(message) = rx.try_recv() else {
            break;
        };
        match message {
            LogProducerMessage::Line(line) => {
                lines.push(line);
                changed = true;
            }
            LogProducerMessage::Done => {
                *done = true;
                changed = true;
            }
            LogProducerMessage::Error(err) => bail!("{err}"),
        }
    }
    Ok(changed)
}

fn draw_log_viewer(lines: &[String], offset: usize, done: bool, label: &str) -> Result<()> {
    let size = log_viewer_terminal_size();
    let width = usize::from(size.0).max(1);
    let height = usize::from(size.1).max(1);
    let body_height = height.saturating_sub(1);
    let text_width = width.saturating_sub(1).max(1);
    let mut stdout = io::stdout();
    for row in 0..body_height {
        queue_log_viewer_line(&mut stdout, lines, offset + row, row, text_width)?;
    }
    queue_log_viewer_status(&mut stdout, lines, offset, body_height, done, size.1, label)?;
    io::Write::flush(&mut stdout).context("flush log viewer")?;
    Ok(())
}

fn draw_log_viewer_single_scroll(
    lines: &[String],
    old_offset: usize,
    offset: usize,
    done: bool,
    label: &str,
) -> Result<()> {
    let size = log_viewer_terminal_size();
    let width = usize::from(size.0).max(1);
    let height = usize::from(size.1).max(1);
    let body_height = height.saturating_sub(1);
    if body_height == 0 {
        return draw_log_viewer(lines, offset, done, label);
    }
    let text_width = width.saturating_sub(1).max(1);
    let mut stdout = io::stdout();

    write!(stdout, "\x1b[1;{}r", body_height).context("set log viewer scroll region")?;
    if offset > old_offset {
        queue!(
            stdout,
            cursor::MoveTo(0, body_height.saturating_sub(1) as u16)
        )
        .context("move to log viewer scroll bottom")?;
        write!(stdout, "\x1bD").context("scroll log viewer body up")?;
        write!(stdout, "\x1b[r").context("reset log viewer scroll region")?;
        queue_log_viewer_line(
            &mut stdout,
            lines,
            offset + body_height.saturating_sub(1),
            body_height.saturating_sub(1),
            text_width,
        )?;
    } else {
        queue!(stdout, cursor::MoveTo(0, 0)).context("move to log viewer scroll top")?;
        write!(stdout, "\x1bM").context("scroll log viewer body down")?;
        write!(stdout, "\x1b[r").context("reset log viewer scroll region")?;
        queue_log_viewer_line(&mut stdout, lines, offset, 0, text_width)?;
    }
    queue_log_viewer_status(&mut stdout, lines, offset, body_height, done, size.1, label)?;
    io::Write::flush(&mut stdout).context("flush log viewer scroll")?;
    Ok(())
}

fn queue_log_viewer_line(
    stdout: &mut io::Stdout,
    lines: &[String],
    line_index: usize,
    row: usize,
    text_width: usize,
) -> Result<()> {
    queue!(
        stdout,
        cursor::MoveTo(0, row as u16),
        terminal::Clear(ClearType::CurrentLine)
    )
    .context("draw log viewer line")?;
    if let Some(line) = lines.get(line_index) {
        let rendered = truncate_for_terminal(line, text_width);
        write!(stdout, "{rendered}").context("write log viewer line")?;
    }
    Ok(())
}

fn queue_log_viewer_status(
    stdout: &mut io::Stdout,
    lines: &[String],
    offset: usize,
    body_height: usize,
    done: bool,
    terminal_rows: u16,
    label: &str,
) -> Result<()> {
    let width = usize::from(log_viewer_terminal_size().0).max(1);
    let text_width = width.saturating_sub(1).max(1);
    let status = if done {
        format!(
            "{label} {}/{}  j/k scroll  space/b page  g/G top/bottom  q quit",
            lines.len().min(offset + body_height),
            lines.len()
        )
    } else {
        format!(
            "{label} {}/{}+  loading...  j/k scroll  space/b page  q quit",
            lines.len().min(offset + body_height),
            lines.len()
        )
    };
    queue!(
        stdout,
        cursor::MoveTo(0, terminal_rows.saturating_sub(1)),
        terminal::Clear(ClearType::CurrentLine)
    )
    .context("draw log viewer status")?;
    write!(stdout, "{}", truncate_for_terminal(&status, text_width))
        .context("write log viewer status")?;
    Ok(())
}

fn log_viewer_terminal_size() -> (u16, u16) {
    terminal::size()
        .ok()
        .filter(|(cols, rows)| *cols >= 20 && *rows >= 5)
        .unwrap_or((100, 24))
}

fn truncate_for_terminal(line: &str, width: usize) -> String {
    let mut out = String::new();
    let mut columns = 0usize;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            out.push(ch);
            if chars.peek().is_some_and(|next| *next == '[') {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
                for next in chars.by_ref() {
                    out.push(next);
                    if ('@'..='~').contains(&next) {
                        break;
                    }
                }
            } else {
                for next in chars.by_ref() {
                    out.push(next);
                    if ('@'..='~').contains(&next) {
                        break;
                    }
                }
            }
            continue;
        }
        let char_width = if ch == '\t' {
            8 - (columns % 8)
        } else if ch.is_control() {
            0
        } else {
            1
        };
        if columns + char_width > width {
            break;
        }
        out.push(ch);
        columns += char_width;
    }
    out
}

fn count_recent_change_operations(conn: &Connection) -> Result<usize> {
    conn.query_row(
        "select count(*)
         from operations
         where after_snapshot is not null
           and (before_snapshot is null or before_snapshot != after_snapshot)
           and kind in ('initial-scan', 'manual-snapshot', 'file-events-batch')",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map(|count| count.max(0) as usize)
    .context("count recent change operations")
}

fn recent_change_operations(
    conn: &Connection,
    limit: usize,
    offset: usize,
) -> Result<Vec<OperationExport>> {
    let mut stmt = conn.prepare(
        "select id, parent_op, kind, actor, session_id, session_label, process_id, process_path, origin_label, origin_session_id, origin_process_id, origin_process_path, origin_exe, origin_confidence, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state
         from operations
         where after_snapshot is not null
           and (before_snapshot is null or before_snapshot != after_snapshot)
           and kind in ('initial-scan', 'manual-snapshot', 'file-events-batch')
         order by rowid desc
         limit ?1 offset ?2",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![limit as i64, offset as i64],
        operation_from_row,
    )?;
    let mut operations = Vec::new();
    for row in rows {
        operations.push(row?);
    }
    Ok(operations)
}

fn recent_operations(conn: &Connection) -> Result<Vec<OperationExport>> {
    let mut stmt = conn.prepare(
        "select id, parent_op, kind, actor, session_id, session_label, process_id, process_path, origin_label, origin_session_id, origin_process_id, origin_process_path, origin_exe, origin_confidence, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state
         from operations order by rowid desc",
    )?;
    let rows = stmt.query_map([], operation_from_row)?;
    let mut operations = Vec::new();
    for row in rows {
        operations.push(row?);
    }
    Ok(operations)
}

fn operation_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OperationExport> {
    let process_path_json: Option<String> = row.get(7)?;
    let origin_process_path_json: Option<String> = row.get(11)?;
    Ok(OperationExport {
        id: row.get(0)?,
        parent_op: row.get(1)?,
        kind: row.get(2)?,
        actor: row.get(3)?,
        session_id: row.get(4)?,
        session_label: row.get(5)?,
        process_id: row.get::<_, Option<i64>>(6)?.map(|pid| pid as u32),
        process_path: process_path_json
            .and_then(|value| serde_json::from_str::<Vec<u32>>(&value).ok())
            .filter(|tree| !tree.is_empty()),
        origin_label: row.get(8)?,
        origin_session_id: row.get(9)?,
        origin_process_id: row.get::<_, Option<i64>>(10)?.map(|pid| pid as u32),
        origin_process_path: origin_process_path_json
            .and_then(|value| serde_json::from_str::<Vec<u32>>(&value).ok())
            .filter(|tree| !tree.is_empty()),
        origin_exe: row.get(12)?,
        origin_confidence: row.get(13)?,
        status: row.get(14)?,
        before_snapshot: row.get(15)?,
        after_snapshot: row.get(16)?,
        created_at: row.get(17)?,
        message: row.get(18)?,
        error: row.get(19)?,
        remote_sync_state: row.get(20)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileChange {
    status: &'static str,
    path: String,
    large: bool,
    volatile_mode: Option<String>,
    warning: Option<String>,
    order_priority: u8,
    order_time: Option<String>,
}

impl FileChange {
    fn plain(status: &'static str, path: String) -> Self {
        Self {
            status,
            path,
            large: false,
            volatile_mode: None,
            warning: None,
            order_priority: 0,
            order_time: None,
        }
    }

    fn warning(status: &'static str, path: String, warning: impl Into<String>) -> Self {
        Self {
            status,
            path,
            large: false,
            volatile_mode: None,
            warning: Some(warning.into()),
            order_priority: 0,
            order_time: None,
        }
    }

    fn from_record(
        status: &'static str,
        path: String,
        root: Option<&RootConfig>,
        rel_path: &str,
        record: Option<&FileRecord>,
    ) -> Self {
        Self::from_records(status, path, root, rel_path, None, record)
    }

    fn from_records(
        status: &'static str,
        path: String,
        root: Option<&RootConfig>,
        rel_path: &str,
        previous: Option<&FileRecord>,
        current: Option<&FileRecord>,
    ) -> Self {
        let large = previous.or(current).is_some_and(record_is_large);
        let volatile_mode = root
            .filter(|root| is_volatile(root, Path::new(rel_path)))
            .and_then(|root| root.volatile.as_ref().map(|volatile| volatile.mode.clone()));
        Self {
            status,
            path,
            large,
            volatile_mode,
            warning: None,
            order_priority: 0,
            order_time: current.or(previous).and_then(state_record_order_time),
        }
    }

    fn with_state_order(mut self, priority: u8, time: Option<String>) -> Self {
        self.order_priority = priority;
        self.order_time = time.or(self.order_time);
        self
    }
}

fn sort_state_changes(changes: &mut [FileChange]) {
    changes.sort_by(compare_state_change_order);
}

fn sort_state_change_records(records: &mut [StateFileChangeRecord]) {
    records.sort_by(|a, b| compare_state_change_order(&a.change, &b.change));
}

fn compare_state_change_order(a: &FileChange, b: &FileChange) -> std::cmp::Ordering {
    b.order_priority
        .cmp(&a.order_priority)
        .then_with(|| b.order_time.cmp(&a.order_time))
        .then_with(|| a.path.cmp(&b.path))
}

fn state_record_order_time(record: &FileRecord) -> Option<String> {
    record
        .modified
        .and_then(|secs| Utc.timestamp_opt(secs, 0).single())
        .map(|time| time.to_rfc3339())
}

fn added_state_order_time(tracked: Option<&TrackedPathTimes>, live: &FileRecord) -> Option<String> {
    merge_order_time(
        tracked.and_then(|times| times.first_seen_at.clone()),
        state_record_order_time(live),
    )
}

fn modified_state_order_time(live: &FileRecord) -> Option<String> {
    state_record_order_time(live)
}

fn deleted_state_order_time(
    tracked: Option<&TrackedPathTimes>,
    previous: &FileRecord,
) -> Option<String> {
    tracked
        .and_then(|times| times.untracked_at.clone().or(times.last_seen_at.clone()))
        .or_else(|| state_record_order_time(previous))
}

fn merge_order_time(a: Option<String>, b: Option<String>) -> Option<String> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[derive(Clone, Debug, Default)]
struct TrackedPathTimes {
    first_seen_at: Option<String>,
    last_seen_at: Option<String>,
    untracked_at: Option<String>,
}

fn tracked_path_times_for_root(
    conn: &Connection,
    root_id: &str,
) -> Result<BTreeMap<String, TrackedPathTimes>> {
    let mut stmt = conn.prepare(
        "select path, first_seen_at, last_seen_at, untracked_at
         from tracked_paths
         where root_id=?1",
    )?;
    let rows = stmt.query_map(params![root_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            TrackedPathTimes {
                first_seen_at: row.get::<_, Option<String>>(1)?,
                last_seen_at: row.get::<_, Option<String>>(2)?,
                untracked_at: row.get::<_, Option<String>>(3)?,
            },
        ))
    })?;
    let mut times = BTreeMap::new();
    for row in rows {
        let (path, path_times) = row?;
        times.insert(path, path_times);
    }
    Ok(times)
}

fn record_is_large(record: &FileRecord) -> bool {
    payload_large_ref(&record.payload).is_some()
}

fn operation_file_changes(
    paths: &Paths,
    conn: &Connection,
    op: &OperationExport,
    root: Option<&str>,
    full: bool,
    configured_roots: &BTreeMap<String, RootConfig>,
) -> Result<Vec<FileChange>> {
    let Some(after_id) = op.after_snapshot.as_deref() else {
        return Ok(Vec::new());
    };
    let Some(after) = load_snapshot_header_by_id_optional(paths, conn, after_id)? else {
        return Ok(snapshot_metadata_unavailable_changes(
            root, after_id, "after",
        ));
    };
    let before = if let Some(before_id) = op.before_snapshot.as_deref() {
        match load_snapshot_header_by_id_optional(paths, conn, before_id)? {
            Some(snapshot) => Some(snapshot),
            None => {
                return Ok(snapshot_metadata_unavailable_changes(
                    root, before_id, "before",
                ));
            }
        }
    } else {
        None
    };
    snapshot_file_changes(paths, before.as_ref(), &after, root, full, configured_roots)
}

fn snapshot_metadata_unavailable_changes(
    root: Option<&str>,
    snapshot_id: &str,
    position: &str,
) -> Vec<FileChange> {
    if root.is_some() {
        return Vec::new();
    }
    vec![FileChange::warning(
        "M",
        format!(
            "** (snapshot metadata unavailable for {position} {snapshot_id}; use `mj fsck` and `mj log --operations`)"
        ),
        format!("snapshot metadata unavailable: {position} {snapshot_id}"),
    )]
}

fn snapshot_file_changes(
    paths: &Paths,
    from: Option<&crate::majutsu_core::SnapshotManifest>,
    to: &crate::majutsu_core::SnapshotManifest,
    root: Option<&str>,
    full: bool,
    configured_roots: &BTreeMap<String, RootConfig>,
) -> Result<Vec<FileChange>> {
    if from.is_some_and(|snapshot| !snapshot.root_trees.is_empty()) || !to.root_trees.is_empty() {
        return snapshot_file_changes_from_root_trees(
            paths,
            from,
            to,
            root,
            full,
            configured_roots,
        );
    }
    let from_files = from.map(snapshot_file_map).transpose()?.unwrap_or_default();
    let to_files = snapshot_file_map(to)?;
    let mut paths_all = from_files.keys().cloned().collect::<Vec<_>>();
    paths_all.extend(
        to_files
            .keys()
            .filter(|key| !from_files.contains_key(*key))
            .cloned(),
    );
    paths_all.sort();
    let mut changes = Vec::new();
    for key in paths_all {
        if let Some(root) = root
            && !key.starts_with(&format!("{root}/"))
        {
            continue;
        }
        match (from_files.get(&key), to_files.get(&key)) {
            (None, Some(record)) => changes.push(change_from_full_path(
                "A",
                key,
                configured_roots,
                None,
                Some(record),
            )),
            (Some(record), None) => changes.push(change_from_full_path(
                "D",
                key,
                configured_roots,
                Some(record),
                None,
            )),
            (Some(a), Some(b)) if a != b => {
                changes.push(change_from_full_path(
                    "M",
                    key,
                    configured_roots,
                    Some(a),
                    Some(b),
                ));
            }
            _ => {}
        }
    }
    Ok(changes)
}

fn change_from_full_path(
    status: &'static str,
    path: String,
    configured_roots: &BTreeMap<String, RootConfig>,
    previous: Option<&FileRecord>,
    current: Option<&FileRecord>,
) -> FileChange {
    let (root_id, rel_path) = path.split_once('/').unwrap_or((path.as_str(), ""));
    let root = configured_roots.get(root_id);
    let rel_path = rel_path.to_string();
    FileChange::from_records(status, path, root, &rel_path, previous, current)
}

fn snapshot_file_changes_from_root_trees(
    paths: &Paths,
    from: Option<&crate::majutsu_core::SnapshotManifest>,
    to: &crate::majutsu_core::SnapshotManifest,
    root: Option<&str>,
    full: bool,
    configured_roots: &BTreeMap<String, RootConfig>,
) -> Result<Vec<FileChange>> {
    const DEFAULT_TREE_DETAIL_LIMIT: usize = 1_000;
    let mut roots = Vec::new();
    if let Some(root) = root {
        roots.push(root.to_string());
    } else {
        if let Some(from) = from {
            roots.extend(from.root_trees.keys().cloned());
            roots.extend(from.roots.keys().cloned());
        }
        roots.extend(to.root_trees.keys().cloned());
        roots.extend(to.roots.keys().cloned());
        roots.sort();
        roots.dedup();
    }

    let mut changes = Vec::new();
    for root_id in roots {
        let from_tree = from.and_then(|snapshot| snapshot.root_trees.get(&root_id));
        let to_tree = to.root_trees.get(&root_id);
        if from_tree == to_tree {
            continue;
        }
        if !full && large_tree_fold_required(paths, from_tree, to_tree, DEFAULT_TREE_DETAIL_LIMIT) {
            changes.push(FileChange::plain(
                folded_root_status(from_tree, to_tree),
                format!("{root_id}/** (large tree folded; use --full for file list)"),
            ));
            continue;
        }
        if let Err(err) =
            append_root_file_changes(paths, from, to, &root_id, configured_roots, &mut changes)
        {
            changes.push(FileChange::warning(
                folded_root_status(from_tree, to_tree),
                format!("{root_id}/** (tree metadata unavailable; use `mj fsck` and `mj remote fsck --objects`)"),
                format!("{err:#}"),
            ));
        }
    }
    Ok(changes)
}

fn append_root_file_changes(
    paths: &Paths,
    from: Option<&SnapshotManifest>,
    to: &SnapshotManifest,
    root_id: &str,
    configured_roots: &BTreeMap<String, RootConfig>,
    changes: &mut Vec<FileChange>,
) -> Result<()> {
    let from_tree = from.and_then(|snapshot| snapshot.root_trees.get(root_id));
    let to_tree = to.root_trees.get(root_id);
    match (from_tree, to_tree) {
        (None, Some(tree)) if !snapshot_root_has_flat_records(to, root_id) => {
            append_tree_records_with_status(paths, root_id, tree, "A", configured_roots, changes)?;
            return Ok(());
        }
        (Some(tree), None)
            if from.is_some_and(|snapshot| !snapshot_root_has_flat_records(snapshot, root_id)) =>
        {
            append_tree_records_with_status(paths, root_id, tree, "D", configured_roots, changes)?;
            return Ok(());
        }
        (Some(from_tree), Some(to_tree))
            if from.is_some_and(|snapshot| !snapshot_root_has_flat_records(snapshot, root_id))
                && !snapshot_root_has_flat_records(to, root_id)
                && append_v2_tree_diff(
                    paths,
                    root_id,
                    from_tree,
                    to_tree,
                    configured_roots,
                    changes,
                )? =>
        {
            return Ok(());
        }
        _ => {}
    }

    let from_files = root_file_map(paths, from, root_id)?;
    let to_files = root_file_map(paths, Some(to), root_id)?;
    let mut paths_all = from_files.keys().cloned().collect::<Vec<_>>();
    paths_all.extend(
        to_files
            .keys()
            .filter(|key| !from_files.contains_key(*key))
            .cloned(),
    );
    paths_all.sort();
    for path in paths_all {
        let full_path = format!("{root_id}/{path}");
        let root = configured_roots.get(root_id);
        match (from_files.get(&path), to_files.get(&path)) {
            (None, Some(record)) => changes.push(FileChange::from_record(
                "A",
                full_path,
                root,
                &path,
                Some(record),
            )),
            (Some(record), None) => changes.push(FileChange::from_record(
                "D",
                full_path,
                root,
                &path,
                Some(record),
            )),
            (Some(a), Some(b)) if a != b => {
                changes.push(FileChange::from_records(
                    "M",
                    full_path,
                    root,
                    &path,
                    Some(a),
                    Some(b),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn snapshot_root_has_flat_records(snapshot: &SnapshotManifest, root_id: &str) -> bool {
    snapshot.roots.contains_key(root_id)
}

fn append_tree_records_with_status(
    paths: &Paths,
    root_id: &str,
    root_tree: &RootSnapshot,
    status: &'static str,
    configured_roots: &BTreeMap<String, RootConfig>,
    changes: &mut Vec<FileChange>,
) -> Result<()> {
    crate::snapshot_state::visit_tree_records(paths, root_tree, |record| {
        changes.push(FileChange::from_record(
            status,
            format!("{root_id}/{}", record.path),
            configured_roots.get(root_id),
            &record.path,
            Some(record),
        ));
        Ok(())
    })
}

fn append_v2_tree_diff(
    paths: &Paths,
    root_id: &str,
    from_tree: &RootSnapshot,
    to_tree: &RootSnapshot,
    configured_roots: &BTreeMap<String, RootConfig>,
    changes: &mut Vec<FileChange>,
) -> Result<bool> {
    let from = read_tree_manifest(paths, from_tree)?;
    let to = read_tree_manifest(paths, to_tree)?;
    let (Some(from_node), Some(to_node)) = (from.root_node.as_ref(), to.root_node.as_ref()) else {
        return Ok(false);
    };
    if !from.entries.is_empty() || !to.entries.is_empty() {
        return Ok(false);
    }
    append_node_diff(
        paths,
        root_id,
        Some(from_node),
        Some(to_node),
        configured_roots,
        changes,
    )?;
    Ok(true)
}

fn read_tree_manifest(paths: &Paths, root_tree: &RootSnapshot) -> Result<TreeManifest> {
    let bytes = crate::read_object(paths, &root_tree.tree_key)
        .with_context(|| format!("read root tree {}", root_tree.tree_key))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse root tree {}", root_tree.tree_key))
}

fn append_node_diff(
    paths: &Paths,
    root_id: &str,
    from: Option<&TreeNodeRef>,
    to: Option<&TreeNodeRef>,
    configured_roots: &BTreeMap<String, RootConfig>,
    changes: &mut Vec<FileChange>,
) -> Result<()> {
    match (from, to) {
        (Some(a), Some(b)) if a.node_key == b.node_key => return Ok(()),
        (None, Some(node)) => {
            return append_node_records_with_status(
                paths,
                root_id,
                node,
                "A",
                configured_roots,
                changes,
            );
        }
        (Some(node), None) => {
            return append_node_records_with_status(
                paths,
                root_id,
                node,
                "D",
                configured_roots,
                changes,
            );
        }
        (None, None) => return Ok(()),
        _ => {}
    }
    let from_node = read_tree_node(paths, &from.expect("matched above").node_key)?;
    let to_node = read_tree_node(paths, &to.expect("matched above").node_key)?;
    append_entry_map_diff(
        root_id,
        &from_node.entries,
        &to_node.entries,
        configured_roots,
        changes,
    );
    let child_paths = from_node
        .child_nodes
        .keys()
        .chain(to_node.child_nodes.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for path in child_paths {
        append_node_diff(
            paths,
            root_id,
            from_node.child_nodes.get(&path),
            to_node.child_nodes.get(&path),
            configured_roots,
            changes,
        )?;
    }
    Ok(())
}

fn append_entry_map_diff(
    root_id: &str,
    from: &BTreeMap<String, FileRecord>,
    to: &BTreeMap<String, FileRecord>,
    configured_roots: &BTreeMap<String, RootConfig>,
    changes: &mut Vec<FileChange>,
) {
    let paths = from
        .keys()
        .chain(to.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for path in paths {
        let full_path = format!("{root_id}/{path}");
        let root = configured_roots.get(root_id);
        match (from.get(&path), to.get(&path)) {
            (None, Some(record)) => changes.push(FileChange::from_record(
                "A",
                full_path,
                root,
                &path,
                Some(record),
            )),
            (Some(record), None) => changes.push(FileChange::from_record(
                "D",
                full_path,
                root,
                &path,
                Some(record),
            )),
            (Some(a), Some(b)) if a != b => changes.push(FileChange::from_records(
                "M",
                full_path,
                root,
                &path,
                Some(a),
                Some(b),
            )),
            _ => {}
        }
    }
}

fn append_node_records_with_status(
    paths: &Paths,
    root_id: &str,
    node: &TreeNodeRef,
    status: &'static str,
    configured_roots: &BTreeMap<String, RootConfig>,
    changes: &mut Vec<FileChange>,
) -> Result<()> {
    let node = read_tree_node(paths, &node.node_key)?;
    for record in node.entries.values() {
        changes.push(FileChange::from_record(
            status,
            format!("{root_id}/{}", record.path),
            configured_roots.get(root_id),
            &record.path,
            Some(record),
        ));
    }
    for child in node.child_nodes.values() {
        append_node_records_with_status(paths, root_id, child, status, configured_roots, changes)?;
    }
    Ok(())
}

fn read_tree_node(paths: &Paths, node_key: &str) -> Result<TreeNodeManifest> {
    let bytes = crate::read_object(paths, node_key)
        .with_context(|| format!("read tree node {node_key}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse tree node {node_key}"))
}

fn large_tree_fold_required(
    paths: &Paths,
    from_tree: Option<&crate::majutsu_core::RootSnapshot>,
    to_tree: Option<&crate::majutsu_core::RootSnapshot>,
    limit: usize,
) -> bool {
    const DEFAULT_TREE_OBJECT_DETAIL_LIMIT: u64 = 128 * 1024;
    from_tree.is_some_and(|tree| tree.file_count > limit)
        || to_tree.is_some_and(|tree| tree.file_count > limit)
        || from_tree.is_some_and(|tree| {
            tree_object_bytes(paths, &tree.tree_key) > DEFAULT_TREE_OBJECT_DETAIL_LIMIT
        })
        || to_tree.is_some_and(|tree| {
            tree_object_bytes(paths, &tree.tree_key) > DEFAULT_TREE_OBJECT_DETAIL_LIMIT
        })
}

fn tree_object_bytes(paths: &Paths, key: &str) -> u64 {
    if let Ok(metadata) = paths.home.join(key).metadata() {
        return metadata.len();
    }
    let Some(rest) = key
        .strip_prefix("trees/")
        .and_then(|rest| rest.strip_suffix(".cbor.zst.enc"))
    else {
        return 0;
    };
    paths
        .home
        .join("objects/trees")
        .join(rest)
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn folded_root_status(
    from_tree: Option<&crate::majutsu_core::RootSnapshot>,
    to_tree: Option<&crate::majutsu_core::RootSnapshot>,
) -> &'static str {
    match (from_tree, to_tree) {
        (None, Some(_)) => "A",
        (Some(_), None) => "D",
        _ => "M",
    }
}

fn root_file_map(
    paths: &Paths,
    snapshot: Option<&crate::majutsu_core::SnapshotManifest>,
    root_id: &str,
) -> Result<BTreeMap<String, crate::majutsu_core::FileRecord>> {
    let Some(snapshot) = snapshot else {
        return Ok(BTreeMap::new());
    };
    if let Some(records) = snapshot.roots.get(root_id) {
        return Ok(records
            .iter()
            .filter(|record| state_record_path_is_file_relative(&record.path))
            .map(|record| (record.path.clone(), record.clone()))
            .collect());
    }
    let Some(root_tree) = snapshot.root_trees.get(root_id) else {
        return Ok(BTreeMap::new());
    };
    let mut records = BTreeMap::new();
    visit_tree_records(paths, root_tree, |record| {
        if state_record_path_is_file_relative(&record.path) {
            records.insert(record.path.clone(), record.clone());
        }
        Ok(())
    })?;
    Ok(records)
}

fn root_file_map_by_snapshot_id(
    paths: &Paths,
    snapshot_id: &str,
    root_id: &str,
) -> Result<BTreeMap<String, crate::majutsu_core::FileRecord>> {
    let conn = crate::open_db(paths)?;
    let snapshot = load_snapshot_header_by_id(paths, &conn, snapshot_id)?;
    if let Some(records) = snapshot.roots.get(root_id) {
        return Ok(records
            .iter()
            .filter(|record| state_record_path_is_file_relative(&record.path))
            .map(|record| (record.path.clone(), record.clone()))
            .collect());
    }
    let Some(root_tree) = snapshot.root_trees.get(root_id) else {
        return Ok(BTreeMap::new());
    };
    let mut records = BTreeMap::new();
    visit_tree_records(paths, root_tree, |record| {
        if state_record_path_is_file_relative(&record.path) {
            records.insert(record.path.clone(), record.clone());
        }
        Ok(())
    })?;
    Ok(records)
}

fn state_record_path_is_file_relative(path: &str) -> bool {
    !path.is_empty() && path != "."
}

fn summarize_changes(changes: &[FileChange]) -> String {
    let added = changes.iter().filter(|change| change.status == "A").count();
    let modified = changes.iter().filter(|change| change.status == "M").count();
    let deleted = changes.iter().filter(|change| change.status == "D").count();
    format!("A:{added} M:{modified} D:{deleted}")
}

fn color_change_status(ui: &StatusUi, status: &str) -> String {
    let severity = match status {
        "A" => Severity::Good,
        "M" => Severity::Warn,
        "m" => Severity::Info,
        "D" => Severity::Bad,
        _ => Severity::Info,
    };
    ui.severity(status, severity)
}

fn color_root_path(ui: &StatusUi, path: &str, local_paths: bool) -> String {
    if local_paths {
        return path.to_string();
    }
    let Some((root, rest)) = path.split_once('/') else {
        return path.to_string();
    };
    format!("{}/{}", ui.paint(root, "1;96"), rest)
}

fn format_change_tags(ui: &StatusUi, change: &FileChange) -> String {
    let mut tags = Vec::new();
    if change.large {
        tags.push(ui.paint("[large]", "35"));
    }
    if let Some(mode) = &change.volatile_mode {
        tags.push(ui.paint(&format!("[volatile:{mode}]"), "36"));
    }
    if change.warning.is_some() {
        tags.push(ui.paint("[metadata-unavailable]", "33"));
    }
    if tags.is_empty() {
        String::new()
    } else {
        format!(" {}", tags.join(" "))
    }
}

fn color_state_diff_line(ui: &StatusUi, line: &str) -> String {
    let trimmed = line.trim_start();
    if trimmed.starts_with('+') {
        ui.paint(line, "32")
    } else if trimmed.starts_with('-') {
        ui.paint(line, "31")
    } else if trimmed.starts_with("@@") {
        ui.paint(line, "36")
    } else {
        line.to_string()
    }
}

fn print_op_log(paths: &Paths, conn: &Connection, args: &LogArgs) -> Result<()> {
    let rows = recent_operations(conn)?;
    let mut printed = 0usize;
    let limit = args.limit.unwrap_or(DEFAULT_LOG_LIMIT);
    let mut output = String::new();
    let ui = StatusUi::new();
    for row in rows {
        let session = operation_origin_label(&row);
        let id = row.id;
        let kind = row.kind;
        let before = row.before_snapshot;
        let after = row.after_snapshot;
        let created = row.created_at;
        let message = row.message;
        let status = row.status;
        let remote_sync_state = row.remote_sync_state;
        if let Some(root) = &args.root {
            let matches_root = message.as_deref() == Some(root)
                || before
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(paths, conn, snapshot, root).ok())
                    .unwrap_or(false)
                || after
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(paths, conn, snapshot, root).ok())
                    .unwrap_or(false);
            if !matches_root {
                continue;
            }
        }
        if printed >= limit {
            break;
        }
        let created = ui.paint(&created, "1;34");
        let kind = ui.paint(&kind, "36");
        writeln!(
            output,
            "{id}\t{created}\t{kind}\t{status}\t{}\t{session}\t{} -> {}\t{}",
            remote_sync_state.unwrap_or_else(|| "-".into()),
            before.unwrap_or_default(),
            after.unwrap_or_default(),
            message.unwrap_or_default()
        )?;
        printed += 1;
    }
    if args.root.is_none() && printed >= limit {
        let note = ui.paint(
            &format!("... showing {printed} operation records; use `mj log --operations --limit N` for more\n"),
            "2",
        );
        writeln!(output, "{note}")?;
    }
    emit_status_output_auto(&output, terminal_height())
}

fn display_log_timestamp(value: &str) -> String {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| {
            timestamp
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S %:z")
                .to_string()
        })
        .unwrap_or_else(|_| value.to_string())
}

fn display_op_id(value: &str) -> String {
    value
        .strip_prefix("op-")
        .map(|suffix| {
            let short = suffix.chars().take(8).collect::<String>();
            format!("op-{short}")
        })
        .unwrap_or_else(|| value.chars().take(12).collect())
}

fn operation_session_label(op: &OperationExport) -> String {
    op.session_label
        .as_deref()
        .zip(op.session_id.as_deref())
        .map(|(label, id)| format!("{label}:{id}"))
        .or_else(|| op.session_id.clone())
        .or_else(|| op.process_id.map(|pid| format!("pid:{pid}")))
        .unwrap_or_else(|| "-".into())
}

fn operation_origin_label(op: &OperationExport) -> String {
    op.origin_label
        .as_deref()
        .zip(op.origin_session_id.as_deref())
        .map(|(label, id)| format!("{label}:{id}"))
        .or_else(|| op.origin_session_id.clone())
        .or_else(|| op.origin_process_id.map(|pid| format!("origin-pid:{pid}")))
        .or_else(|| {
            if op.kind == "file-events-batch" && op.session_label.as_deref() == Some("daemon") {
                if op
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("journal replay"))
                {
                    Some("unattributed:watch-replay".into())
                } else {
                    Some("unattributed:watch".into())
                }
            } else {
                None
            }
        })
        .unwrap_or_else(|| operation_session_label(op))
}

pub(crate) fn note_cmd(paths: &Paths, args: NoteArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    let op_id = resolve_note_operation_id(&conn, &args.reference)?;
    if args.message.is_none() && !args.stdin && !args.clear {
        let op = query_operation_resolved(&conn, &op_id)?;
        if let Some(message) = op.message {
            println!("{message}");
        }
        return Ok(());
    }
    let message = if args.clear {
        None
    } else if args.stdin {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        let trimmed = input.trim_end_matches(['\r', '\n']).to_string();
        (!trimmed.is_empty()).then_some(trimmed)
    } else {
        args.message.filter(|message| !message.is_empty())
    };
    let op = update_operation_message(&conn, &op_id, message.as_deref())?;
    println!("noted {}", display_op_id(&op.id));
    if let Some(message) = op.message {
        println!("{message}");
    }
    Ok(())
}

fn resolve_note_operation_id(conn: &Connection, reference: &str) -> Result<String> {
    let reference = reference.trim();
    if reference.is_empty() {
        bail!("note reference must not be empty");
    }
    if reference.starts_with("snap-") {
        let snapshot = resolve_snapshot_id(conn, reference)?;
        return conn
            .query_row(
                "select op_id from snapshots where id=?1",
                params![snapshot],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| anyhow!("snapshot has no operation: {reference}"));
    }
    Ok(query_operation_resolved(conn, reference)?.id)
}

pub(crate) fn op_cmd(paths: &Paths, command: OpCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    match command {
        OpCommand::Log(args) => print_op_log(paths, &conn, &args),
        OpCommand::Show(args) => {
            let op = query_operation_resolved(&conn, &args.op_id)?;
            println!("id {}", op.id);
            println!("parent {}", op.parent_op.as_deref().unwrap_or("(none)"));
            println!("kind {}", op.kind);
            println!("actor {}", op.actor);
            println!("session_id {}", op.session_id.as_deref().unwrap_or(""));
            println!(
                "session_label {}",
                op.session_label.as_deref().unwrap_or("")
            );
            println!(
                "process_id {}",
                op.process_id.map(|pid| pid.to_string()).unwrap_or_default()
            );
            println!(
                "process_path {}",
                op.process_path
                    .as_ref()
                    .map(|tree| {
                        tree.iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .unwrap_or_default()
            );
            println!("origin_label {}", op.origin_label.as_deref().unwrap_or(""));
            println!(
                "origin_session_id {}",
                op.origin_session_id.as_deref().unwrap_or("")
            );
            println!(
                "origin_process_id {}",
                op.origin_process_id
                    .map(|pid| pid.to_string())
                    .unwrap_or_default()
            );
            println!(
                "origin_process_path {}",
                op.origin_process_path
                    .as_ref()
                    .map(|tree| {
                        tree.iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .unwrap_or_default()
            );
            println!("origin_exe {}", op.origin_exe.as_deref().unwrap_or(""));
            println!(
                "origin_confidence {}",
                op.origin_confidence.as_deref().unwrap_or("")
            );
            println!("status {}", op.status);
            println!(
                "before {}",
                op.before_snapshot.as_deref().unwrap_or("(none)")
            );
            println!("after {}", op.after_snapshot.as_deref().unwrap_or("(none)"));
            println!("created_at {}", op.created_at);
            println!("message {}", op.message.as_deref().unwrap_or_default());
            println!("error {}", op.error.as_deref().unwrap_or_default());
            println!(
                "remote_sync_state {}",
                op.remote_sync_state.as_deref().unwrap_or_default()
            );
            if args.files {
                println!("files");
                print_op_diff(paths, &conn, &op, args.root.as_deref())?;
            }
            Ok(())
        }
        OpCommand::Diff(args) => {
            let op = query_operation_resolved(&conn, &args.op_id)?;
            print_op_diff(paths, &conn, &op, args.root.as_deref())?;
            Ok(())
        }
        OpCommand::Restore { op_id } => {
            let op = query_operation_resolved(&conn, &op_id)?;
            let resolved_op_id = op.id.clone();
            let before = current_snapshot(&conn)?;
            let snapshot = op
                .before_snapshot
                .or(op.after_snapshot)
                .ok_or_else(|| anyhow!("operation has no snapshot to restore: {op_id}"))?;
            conn.execute(
                "insert into refs(name, value) values ('current', ?1)
                 on conflict(name) do update set value=excluded.value",
                params![snapshot],
            )?;
            crate::branch_runtime::update_active_branch_head(&conn, &snapshot)?;
            record_op(
                &conn,
                "op-restore",
                before.as_deref(),
                Some(&snapshot),
                Some(&resolved_op_id),
            )?;
            println!("current {}", snapshot);
            Ok(())
        }
    }
}

fn print_op_diff(
    paths: &Paths,
    conn: &Connection,
    op: &OperationExport,
    root: Option<&str>,
) -> Result<()> {
    let Some(after_id) = op.after_snapshot.as_deref() else {
        bail!("operation has no after snapshot to diff: {}", op.id);
    };
    let after = load_snapshot_by_id(paths, conn, after_id)?;
    let before = op
        .before_snapshot
        .as_deref()
        .map(|id| load_snapshot_by_id(paths, conn, id))
        .transpose()?;
    print_snapshot_diff(paths, before.as_ref(), &after, root)
}

pub(crate) fn diff_cmd(paths: &Paths, args: DiffArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    if args.at.is_some() && args.from.is_some() {
        bail!("use either a positional from snapshot or --at, not both");
    }
    let to_id = args
        .to
        .clone()
        .or_else(|| current_snapshot(&conn).ok().flatten())
        .ok_or_else(|| anyhow!("no target snapshot"))?;
    let to = load_snapshot_by_id(paths, &conn, &to_id)?;
    let from_id = if let Some(at) = &args.at {
        Some(snapshot_id_at(&conn, at)?)
    } else {
        args.from.or_else(|| to.parent.clone())
    };
    let from = if let Some(from_id) = from_id {
        Some(load_snapshot_by_id(paths, &conn, &from_id)?)
    } else {
        None
    };
    print_snapshot_diff(paths, from.as_ref(), &to, args.root.as_deref())
}

fn print_snapshot_diff(
    paths: &Paths,
    from: Option<&crate::majutsu_core::SnapshotManifest>,
    to: &crate::majutsu_core::SnapshotManifest,
    root: Option<&str>,
) -> Result<()> {
    let configured_roots = BTreeMap::new();
    let ui = StatusUi::new();
    for change in snapshot_file_changes(paths, from, to, root, true, &configured_roots)? {
        println!(
            "{}\t{}",
            color_change_status(&ui, change.status),
            color_root_path(&ui, &change.path, false)
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        LogProducerMessage, StateStreamMode, prefetch_state_lines_for_viewer,
        read_remote_root_acks, should_page_status_with_tty, state_lines_need_viewer,
        truncate_for_terminal,
    };
    use rusqlite::{Connection, params};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn status_pager_decision_respects_tty_height_and_force() {
        let short = "one\ntwo\n";
        let tall = "one\ntwo\nthree\n";

        assert!(!should_page_status_with_tty(short, 2, false, true));
        assert!(should_page_status_with_tty(tall, 2, false, true));
        assert!(!should_page_status_with_tty(tall, 2, false, false));
        assert!(should_page_status_with_tty(short, 100, true, false));
    }

    #[test]
    fn terminal_truncation_keeps_ansi_escape_sequences_intact() {
        let line = "\x1b[1;96msample\x1b[0m/path/to/file";
        let truncated = truncate_for_terminal(line, 8);

        assert!(truncated.starts_with("\x1b[1;96m"));
        assert!(truncated.contains("\x1b[0m"));
        assert!(truncated.ends_with("/p"));
        assert!(!truncated.contains("path/to"));
    }

    #[test]
    fn state_prefetch_enters_viewer_only_after_height_overflow() {
        assert!(!state_lines_need_viewer(0, 5));
        assert!(!state_lines_need_viewer(5, 5));
        assert!(state_lines_need_viewer(6, 5));
    }

    #[test]
    fn state_prefetch_waits_for_delayed_overflow() {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            tx.send(LogProducerMessage::Line("one".into())).unwrap();
            thread::sleep(Duration::from_millis(220));
            tx.send(LogProducerMessage::Line("two".into())).unwrap();
            tx.send(LogProducerMessage::Done).unwrap();
        });
        let mut lines = Vec::new();
        let mode = prefetch_state_lines_for_viewer(&rx, 1, &mut lines).unwrap();

        assert_eq!(mode, StateStreamMode::Viewer);
        assert_eq!(lines, vec!["one".to_string(), "two".to_string()]);
    }

    #[test]
    fn state_prefetch_prints_delayed_short_output_directly() {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            tx.send(LogProducerMessage::Line("one".into())).unwrap();
            thread::sleep(Duration::from_millis(220));
            tx.send(LogProducerMessage::Done).unwrap();
        });
        let mut lines = Vec::new();
        let mode = prefetch_state_lines_for_viewer(&rx, 5, &mut lines).unwrap();

        assert_eq!(mode, StateStreamMode::Direct);
        assert_eq!(lines, vec!["one".to_string()]);
    }

    #[test]
    fn cached_remote_root_acks_are_read_from_host_prefix_refs() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "create table remote_refs (remote text not null, name text not null, value text not null)",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into remote_refs (remote, name, value) values (?1, ?2, ?3)",
            params![
                "remote",
                "workstation/roots/sample/ack",
                r#"{"snapshot_id":"snap-1","tree_id":"tree-1","tree_key":"objects/trees/tree-1","file_count":1,"synced_at":"2026-07-04T00:00:00Z"}"#
            ],
        )
        .unwrap();
        conn.execute(
            "insert into remote_refs (remote, name, value) values (?1, ?2, ?3)",
            params![
                "remote",
                "host-uuid/roots/sample/ack",
                r#"{"snapshot_id":"snap-old","tree_id":"tree-old","tree_key":"objects/trees/tree-old","file_count":1,"synced_at":"2026-07-04T00:00:00Z"}"#
            ],
        )
        .unwrap();

        let by_prefix = read_remote_root_acks(&conn, "remote", "workstation").unwrap();
        assert_eq!(by_prefix["sample"].tree_id, "tree-1");
        let by_id = read_remote_root_acks(&conn, "remote", "host-uuid").unwrap();
        assert_eq!(by_id["sample"].tree_id, "tree-old");
    }
}
