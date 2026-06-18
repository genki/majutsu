use crate::majutsu_core::{
    FileRecord, OperationLogEntry as OperationExport, RootSnapshot, SnapshotManifest, TreeManifest,
    TreeNodeManifest, TreeNodeRef,
};
use crate::majutsu_store::{
    host_current_ref_key, host_last_synced_ref_key, host_root_ack_ref_key, host_root_ack_ref_prefix,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use crossterm::cursor;
use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::queue;
use crossterm::terminal::{self, ClearType};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::{self, IsTerminal, Write as _};
#[cfg(unix)]
use std::mem;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration as StdDuration;
use walkdir::WalkDir;

use crate::atomic_io::write_atomic;
use crate::cli::{DiffArgs, HealthArgs, LogArgs, OpCommand, StateArgs, StatusArgs};
use crate::config::{Config, Paths, RootConfig, read_config};
use crate::daemon_runtime::{DaemonHealth, DaemonHealthState, daemon_health};
use crate::operation_log::{query_operation, record_op};
use crate::process_runtime::process_lock_owner;
use crate::queue_runtime::{
    event_journal_records, event_journal_stats, record_event, upload_queue_stats,
};
use crate::remote_store::open_remote;
use crate::root_state::roots;
use crate::snapshot_state::{
    current_snapshot, load_root_tree_entries, load_snapshot_by_id, load_snapshot_header_by_id,
    snapshot_contains_root, snapshot_file_map, snapshot_id_at,
};

pub(crate) fn status_cmd(paths: &Paths, args: StatusArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    let config = read_config(paths)?;
    let roots = roots(&conn)?;
    let current = current_snapshot(&conn)?;
    let current_manifest = current
        .as_deref()
        .map(|id| load_snapshot_by_id(paths, &conn, id))
        .transpose()?;
    let current_label = current.as_deref().unwrap_or("(none)");
    let remote = read_remote_status(&config)?;
    let remote_head = read_remote_head_status(&conn, &config, &remote, current.as_deref())?;
    let remote_manifest = remote_head
        .remote_current
        .as_deref()
        .and_then(|id| load_snapshot_by_id(paths, &conn, id).ok());
    let db_stats = read_status_db_stats(&conn)?;
    let storage = read_storage_stats(paths)?;
    let upload_stats = upload_queue_stats(paths)?;
    let event_records = event_journal_records(paths)?;
    let event_count = event_records.len();
    let pending_event_count = pending_journal_event_count(&event_records);
    let restore_queue_count = count_json_files(&paths.home.join("queue/restores"))?;
    let daemon = daemon_health(paths)?;
    let health = build_health_report(HealthInputs {
        paths,
        config: &config,
        roots: &roots,
        current: current.as_deref(),
        remote: &remote,
        remote_head: &remote_head,
        daemon: &daemon,
        upload_stats: &upload_stats,
        pending_event_count,
        current_manifest: current_manifest.as_ref(),
        remote_manifest: remote_manifest.as_ref(),
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
    let report = current_health_report(paths)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    print_health_report(&report);
    Ok(())
}

pub(crate) fn refresh_runtime_health(paths: &Paths) -> Result<()> {
    let report = current_health_report(paths)?;
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

fn current_health_report(paths: &Paths) -> Result<HealthReport> {
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
    let daemon = daemon_health(paths)?;
    build_health_report(HealthInputs {
        paths,
        config: &config,
        roots: &roots,
        current: current.as_deref(),
        remote: &remote,
        remote_head: &remote_head,
        daemon: &daemon,
        upload_stats: &upload_stats,
        pending_event_count,
        current_manifest: current_manifest.as_ref(),
        remote_manifest: remote_manifest.as_ref(),
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
    sync_lock_pid: Option<u32>,
    encryption: String,
    roots: Vec<RootHealth>,
    issues: Vec<HealthIssue>,
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
    upload_stats: &'a crate::queue_runtime::UploadQueueStats,
    pending_event_count: usize,
    current_manifest: Option<&'a crate::majutsu_core::SnapshotManifest>,
    remote_manifest: Option<&'a crate::majutsu_core::SnapshotManifest>,
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
        upload_stats,
        pending_event_count,
        current_manifest,
        remote_manifest,
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
        if root.status == "active" && !root.path.exists() {
            issues.push(HealthIssue {
                severity: HealthSeverity::Critical,
                code: "root-missing".into(),
                message: format!(
                    "active root {} is missing: {}",
                    root.id,
                    root.path.display()
                ),
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

    if pending_event_count > 0 {
        issues.push(HealthIssue {
            severity: HealthSeverity::Warning,
            code: "pending-journal-events".into(),
            message: format!("event journal has {pending_event_count} pending trigger event(s)"),
        });
    }

    let sync_lock_pid = process_lock_owner(&paths.sync_lock)?;
    if let Some(pid) = sync_lock_pid {
        issues.push(HealthIssue {
            severity: HealthSeverity::Warning,
            code: "sync-running".into(),
            message: format!("sync is currently running with pid {pid}"),
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
        sync_lock_pid,
        encryption: config.security.encryption.clone(),
        roots: root_health,
        issues,
    })
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

fn print_health_report(report: &HealthReport) {
    println!("state {}", report.state.as_str());
    println!(
        "current_snapshot {}",
        report.current_snapshot.as_deref().unwrap_or("(none)")
    );
    println!("active_roots {}", report.active_roots);
    println!("roots_total {}", report.roots_total);
    println!("daemon {}", report.daemon_state);
    println!("daemon_ipc {}", report.daemon_ipc);
    println!("remote_configured {}", report.remote_configured);
    println!("remote_available {}", report.remote_available);
    println!("remote_head {}", report.remote_head_status);
    println!("queued_uploads {}", report.queued_uploads);
    println!("queued_uploads_retrying {}", report.queued_uploads_retrying);
    println!("queued_uploads_delayed {}", report.queued_uploads_delayed);
    println!("pending_journal_events {}", report.pending_journal_events);
    println!(
        "sync_lock_pid {}",
        report
            .sync_lock_pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "(none)".into())
    );
    println!("encryption {}", report.encryption);
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
    println!("issue_count {}", report.issue_count());
    for issue in &report.issues {
        println!(
            "issue {} {} {}",
            issue.severity.as_str(),
            issue.code,
            issue.message
        );
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
    let restore_queue_count = count_json_files(&paths.home.join("queue/restores"))?;
    let remote = read_remote_status(&config)?;
    let remote_head = read_remote_head_status(&conn, &config, &remote, current.as_deref())?;
    let daemon = daemon_health(paths)?;

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
            restore_jobs: restore_queue_count as u64,
        },
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
                "restore jobs",
                &state.queues.restore_jobs.to_string(),
                "prepared jobs",
            ],
        ],
    );

    emit_status_output_auto(&output, height)?;
    Ok(())
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
    restore_jobs: u64,
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
                && last_snapshot_finish
                    .map(|finished_at| event.observed_at > finished_at)
                    .unwrap_or(true)
        })
        .count()
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
    force || (io::stdout().is_terminal() && output.lines().count() > height)
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
    let ref_name = host_current_ref_key(&config.host.id);
    let last_synced_ref_name = host_last_synced_ref_key(&config.host.id);
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
    let root_acks = read_remote_root_acks(conn, remote_name, &config.host.id)?;
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
            let changes =
                operation_file_changes(paths, conn, &op, args.root.as_deref(), args.full)?;
            if changes.is_empty() {
                continue;
            }
            let summary = summarize_changes(&changes);
            write_line(&format!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                ui.paint(&op.created_at, "1;34"),
                op.id,
                ui.paint(&op.kind, "36"),
                operation_session_label(&op),
                summary,
                op.message.as_deref().unwrap_or_default()
            ))?;
            let change_count = changes.len();
            for change in changes.into_iter().take(file_limit) {
                write_line(&format!(
                    "{}\t{}",
                    color_change_status(&ui, change.status),
                    change.path
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

    run_log_viewer(lines, rx)
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
) -> Result<()> {
    let _guard = TerminalGuard::enter()?;
    let mut lines = initial_lines;
    let mut done = false;
    let mut offset = 0usize;
    let mut needs_redraw = true;
    loop {
        if needs_redraw {
            draw_log_viewer(&lines, offset, done)?;
            needs_redraw = false;
        }
        let old_offset = offset;
        if event::poll(StdDuration::from_millis(30)).context("poll log viewer input")?
            && let Event::Key(key) = event::read().context("read log viewer input")?
        {
            let page = terminal_height().saturating_sub(1).max(1);
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Char('j') | KeyCode::Down => {
                    offset = offset.saturating_add(1).min(lines.len().saturating_sub(1));
                    needs_redraw = true;
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    offset = offset.saturating_sub(1);
                    needs_redraw = true;
                }
                KeyCode::Char(' ') | KeyCode::PageDown => {
                    offset = offset
                        .saturating_add(page)
                        .min(lines.len().saturating_sub(1));
                    needs_redraw = true;
                }
                KeyCode::Char('b') | KeyCode::PageUp => {
                    offset = offset.saturating_sub(page);
                    needs_redraw = true;
                }
                KeyCode::Char('g') | KeyCode::Home => {
                    offset = 0;
                    needs_redraw = true;
                }
                KeyCode::Char('G') | KeyCode::End if done => {
                    offset = lines.len().saturating_sub(page);
                    needs_redraw = true;
                }
                _ => {}
            }
        }
        let changed = drain_log_messages(&rx, &mut lines, &mut done, 256)?;
        if changed {
            needs_redraw = true;
        } else if needs_redraw
            && offset.abs_diff(old_offset) == 1
            && draw_log_viewer_single_scroll(&lines, old_offset, offset, done).is_ok()
        {
            needs_redraw = false;
        }
        if done && lines.is_empty() {
            break;
        }
    }
    Ok(())
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

fn draw_log_viewer(lines: &[String], offset: usize, done: bool) -> Result<()> {
    let size = log_viewer_terminal_size();
    let width = usize::from(size.0).max(1);
    let height = usize::from(size.1).max(1);
    let body_height = height.saturating_sub(1);
    let text_width = width.saturating_sub(1).max(1);
    let mut stdout = io::stdout();
    for row in 0..body_height {
        queue_log_viewer_line(&mut stdout, lines, offset + row, row, text_width)?;
    }
    queue_log_viewer_status(&mut stdout, lines, offset, body_height, done, size.1)?;
    io::Write::flush(&mut stdout).context("flush log viewer")?;
    Ok(())
}

fn draw_log_viewer_single_scroll(
    lines: &[String],
    old_offset: usize,
    offset: usize,
    done: bool,
) -> Result<()> {
    let size = log_viewer_terminal_size();
    let width = usize::from(size.0).max(1);
    let height = usize::from(size.1).max(1);
    let body_height = height.saturating_sub(1);
    if body_height == 0 {
        return draw_log_viewer(lines, offset, done);
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
    queue_log_viewer_status(&mut stdout, lines, offset, body_height, done, size.1)?;
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
) -> Result<()> {
    let width = usize::from(log_viewer_terminal_size().0).max(1);
    let text_width = width.saturating_sub(1).max(1);
    let status = if done {
        format!(
            "mj log {}/{}  j/k scroll  space/b page  g/G top/bottom  q quit",
            lines.len().min(offset + body_height),
            lines.len()
        )
    } else {
        format!(
            "mj log {}/{}+  loading...  j/k scroll  space/b page  q quit",
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
            for next in chars.by_ref() {
                out.push(next);
                if ('@'..='~').contains(&next) {
                    break;
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
        "select id, parent_op, kind, actor, session_id, session_label, process_id, process_path, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state
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
        "select id, parent_op, kind, actor, session_id, session_label, process_id, process_path, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state
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
        status: row.get(8)?,
        before_snapshot: row.get(9)?,
        after_snapshot: row.get(10)?,
        created_at: row.get(11)?,
        message: row.get(12)?,
        error: row.get(13)?,
        remote_sync_state: row.get(14)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileChange {
    status: &'static str,
    path: String,
}

fn operation_file_changes(
    paths: &Paths,
    conn: &Connection,
    op: &OperationExport,
    root: Option<&str>,
    full: bool,
) -> Result<Vec<FileChange>> {
    let Some(after_id) = op.after_snapshot.as_deref() else {
        return Ok(Vec::new());
    };
    let after = load_snapshot_header_by_id(paths, conn, after_id)?;
    let before = if let Some(before_id) = op.before_snapshot.as_deref() {
        Some(load_snapshot_header_by_id(paths, conn, before_id)?)
    } else {
        None
    };
    snapshot_file_changes(paths, before.as_ref(), &after, root, full)
}

fn snapshot_file_changes(
    paths: &Paths,
    from: Option<&crate::majutsu_core::SnapshotManifest>,
    to: &crate::majutsu_core::SnapshotManifest,
    root: Option<&str>,
    full: bool,
) -> Result<Vec<FileChange>> {
    if from.is_some_and(|snapshot| !snapshot.root_trees.is_empty()) || !to.root_trees.is_empty() {
        return snapshot_file_changes_from_root_trees(paths, from, to, root, full);
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
            (None, Some(_)) => changes.push(FileChange {
                status: "A",
                path: key,
            }),
            (Some(_), None) => changes.push(FileChange {
                status: "D",
                path: key,
            }),
            (Some(a), Some(b)) if a != b => {
                changes.push(FileChange {
                    status: "M",
                    path: key,
                });
            }
            _ => {}
        }
    }
    Ok(changes)
}

fn snapshot_file_changes_from_root_trees(
    paths: &Paths,
    from: Option<&crate::majutsu_core::SnapshotManifest>,
    to: &crate::majutsu_core::SnapshotManifest,
    root: Option<&str>,
    full: bool,
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
            changes.push(FileChange {
                status: folded_root_status(from_tree, to_tree),
                path: format!("{root_id}/** (large tree folded; use --full for file list)"),
            });
            continue;
        }
        append_root_file_changes(paths, from, to, &root_id, &mut changes)?;
    }
    Ok(changes)
}

fn append_root_file_changes(
    paths: &Paths,
    from: Option<&SnapshotManifest>,
    to: &SnapshotManifest,
    root_id: &str,
    changes: &mut Vec<FileChange>,
) -> Result<()> {
    let from_tree = from.and_then(|snapshot| snapshot.root_trees.get(root_id));
    let to_tree = to.root_trees.get(root_id);
    match (from_tree, to_tree) {
        (None, Some(tree)) if !snapshot_root_has_flat_records(to, root_id) => {
            append_tree_records_with_status(paths, root_id, tree, "A", changes)?;
            return Ok(());
        }
        (Some(tree), None)
            if from.is_some_and(|snapshot| !snapshot_root_has_flat_records(snapshot, root_id)) =>
        {
            append_tree_records_with_status(paths, root_id, tree, "D", changes)?;
            return Ok(());
        }
        (Some(from_tree), Some(to_tree))
            if from.is_some_and(|snapshot| !snapshot_root_has_flat_records(snapshot, root_id))
                && !snapshot_root_has_flat_records(to, root_id)
                && append_v2_tree_diff(paths, root_id, from_tree, to_tree, changes)? =>
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
        match (from_files.get(&path), to_files.get(&path)) {
            (None, Some(_)) => changes.push(FileChange {
                status: "A",
                path: full_path,
            }),
            (Some(_), None) => changes.push(FileChange {
                status: "D",
                path: full_path,
            }),
            (Some(a), Some(b)) if a != b => {
                changes.push(FileChange {
                    status: "M",
                    path: full_path,
                });
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
    changes: &mut Vec<FileChange>,
) -> Result<()> {
    crate::snapshot_state::visit_tree_records(paths, root_tree, |record| {
        changes.push(FileChange {
            status,
            path: format!("{root_id}/{}", record.path),
        });
        Ok(())
    })
}

fn append_v2_tree_diff(
    paths: &Paths,
    root_id: &str,
    from_tree: &RootSnapshot,
    to_tree: &RootSnapshot,
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
    append_node_diff(paths, root_id, Some(from_node), Some(to_node), changes)?;
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
    changes: &mut Vec<FileChange>,
) -> Result<()> {
    match (from, to) {
        (Some(a), Some(b)) if a.node_key == b.node_key => return Ok(()),
        (None, Some(node)) => {
            return append_node_records_with_status(paths, root_id, node, "A", changes);
        }
        (Some(node), None) => {
            return append_node_records_with_status(paths, root_id, node, "D", changes);
        }
        (None, None) => return Ok(()),
        _ => {}
    }
    let from_node = read_tree_node(paths, &from.expect("matched above").node_key)?;
    let to_node = read_tree_node(paths, &to.expect("matched above").node_key)?;
    append_entry_map_diff(root_id, &from_node.entries, &to_node.entries, changes);
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
            changes,
        )?;
    }
    Ok(())
}

fn append_entry_map_diff(
    root_id: &str,
    from: &BTreeMap<String, FileRecord>,
    to: &BTreeMap<String, FileRecord>,
    changes: &mut Vec<FileChange>,
) {
    let paths = from
        .keys()
        .chain(to.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for path in paths {
        let full_path = format!("{root_id}/{path}");
        match (from.get(&path), to.get(&path)) {
            (None, Some(_)) => changes.push(FileChange {
                status: "A",
                path: full_path,
            }),
            (Some(_), None) => changes.push(FileChange {
                status: "D",
                path: full_path,
            }),
            (Some(a), Some(b)) if a != b => changes.push(FileChange {
                status: "M",
                path: full_path,
            }),
            _ => {}
        }
    }
}

fn append_node_records_with_status(
    paths: &Paths,
    root_id: &str,
    node: &TreeNodeRef,
    status: &'static str,
    changes: &mut Vec<FileChange>,
) -> Result<()> {
    let node = read_tree_node(paths, &node.node_key)?;
    for record in node.entries.values() {
        changes.push(FileChange {
            status,
            path: format!("{root_id}/{}", record.path),
        });
    }
    for child in node.child_nodes.values() {
        append_node_records_with_status(paths, root_id, child, status, changes)?;
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
            .map(|record| (record.path.clone(), record.clone()))
            .collect());
    }
    let Some(root_tree) = snapshot.root_trees.get(root_id) else {
        return Ok(BTreeMap::new());
    };
    load_root_tree_entries(paths, root_tree)
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
        "D" => Severity::Bad,
        _ => Severity::Info,
    };
    ui.severity(status, severity)
}

fn print_op_log(paths: &Paths, conn: &Connection, args: &LogArgs) -> Result<()> {
    let rows = recent_operations(conn)?;
    let mut printed = 0usize;
    let limit = args.limit.unwrap_or(DEFAULT_LOG_LIMIT);
    let mut output = String::new();
    let ui = StatusUi::new();
    for row in rows {
        let session = operation_session_label(&row);
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

fn operation_session_label(op: &OperationExport) -> String {
    op.session_label
        .as_deref()
        .zip(op.session_id.as_deref())
        .map(|(label, id)| format!("{label}:{id}"))
        .or_else(|| op.session_id.clone())
        .or_else(|| op.process_id.map(|pid| format!("pid:{pid}")))
        .unwrap_or_else(|| "-".into())
}

pub(crate) fn op_cmd(paths: &Paths, command: OpCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    match command {
        OpCommand::Log(args) => print_op_log(paths, &conn, &args),
        OpCommand::Show(args) => {
            let op = query_operation(&conn, &args.op_id)?;
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
            let op = query_operation(&conn, &args.op_id)?;
            print_op_diff(paths, &conn, &op, args.root.as_deref())?;
            Ok(())
        }
        OpCommand::Restore { op_id } => {
            let op = query_operation(&conn, &op_id)?;
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
                Some(&op_id),
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
    for change in snapshot_file_changes(paths, from, to, root, true)? {
        println!("{}\t{}", change.status, change.path);
    }
    Ok(())
}
