use crate::majutsu_core::OperationLogEntry as OperationExport;
use anyhow::{Result, anyhow};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::atomic_io::fsync_parent_dir;
use crate::util::new_id;

pub(crate) fn record_op(
    conn: &Connection,
    kind: &str,
    before: Option<&str>,
    after: Option<&str>,
    message: Option<&str>,
) -> Result<String> {
    let id = new_id("op");
    record_op_with_id(conn, &id, kind, before, after, message)?;
    Ok(id)
}

pub(crate) fn record_op_with_id(
    conn: &Connection,
    id: &str,
    kind: &str,
    before: Option<&str>,
    after: Option<&str>,
    message: Option<&str>,
) -> Result<()> {
    record_op_with_id_and_status(conn, id, kind, before, after, "done", message)
}

pub(crate) fn record_op_with_id_and_status(
    conn: &Connection,
    id: &str,
    kind: &str,
    before: Option<&str>,
    after: Option<&str>,
    status: &str,
    message: Option<&str>,
) -> Result<()> {
    record_op_with_details(
        conn,
        OperationDetails {
            id,
            kind,
            before,
            after,
            status,
            message,
            error: None,
            remote_sync_state: None,
            origin: None,
        },
    )
}

pub(crate) struct OperationDetails<'a> {
    pub(crate) id: &'a str,
    pub(crate) kind: &'a str,
    pub(crate) before: Option<&'a str>,
    pub(crate) after: Option<&'a str>,
    pub(crate) status: &'a str,
    pub(crate) message: Option<&'a str>,
    pub(crate) error: Option<&'a str>,
    pub(crate) remote_sync_state: Option<&'a str>,
    pub(crate) origin: Option<OperationOriginOverride>,
}

#[derive(Debug, Clone)]
pub(crate) struct OperationOriginOverride {
    pub(crate) label: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) process_id: Option<u32>,
    pub(crate) process_path: Option<Vec<u32>>,
    pub(crate) exe: Option<String>,
    pub(crate) confidence: Option<String>,
}

#[cfg(target_os = "linux")]
pub(crate) fn origin_override_from_pid(
    pid: u32,
    label: Option<String>,
    session_id: Option<String>,
    confidence: &str,
) -> OperationOriginOverride {
    OperationOriginOverride {
        label,
        session_id,
        process_id: Some(pid),
        process_path: Some(process_path(pid)),
        exe: process_exe_string(pid),
        confidence: Some(confidence.to_string()),
    }
}

pub(crate) fn record_op_with_details(
    conn: &Connection,
    details: OperationDetails<'_>,
) -> Result<()> {
    let created_at = Utc::now().to_rfc3339();
    let parent_op = current_operation(conn)?;
    let actor = operation_actor();
    let session = operation_session();
    let process = operation_process();
    let origin = details
        .origin
        .clone()
        .map(OperationOrigin::from)
        .unwrap_or_else(|| operation_origin(&session, &process));
    let process_path_json = process
        .path
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let origin_process_path_json = origin
        .process_path
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    conn.execute(
        "insert into operations(id, parent_op, kind, actor, session_id, session_label, process_id, process_path, origin_label, origin_session_id, origin_process_id, origin_process_path, origin_exe, origin_confidence, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state)
         values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
        params![
            details.id,
            parent_op,
            details.kind,
            actor,
            session.id,
            session.label,
            process.id,
            process_path_json,
            origin.label,
            origin.session_id,
            origin.process_id,
            origin_process_path_json,
            origin.exe,
            origin.confidence,
            details.status,
            details.before,
            details.after,
            created_at,
            details.message,
            details.error,
            details.remote_sync_state
        ],
    )?;
    let op = OperationExport {
        id: details.id.to_string(),
        parent_op,
        kind: details.kind.to_string(),
        actor,
        session_id: session.id,
        session_label: session.label,
        process_id: process.id,
        process_path: process.path,
        origin_label: origin.label,
        origin_session_id: origin.session_id,
        origin_process_id: origin.process_id,
        origin_process_path: origin.process_path,
        origin_exe: origin.exe,
        origin_confidence: origin.confidence,
        status: details.status.to_string(),
        before_snapshot: details.before.map(str::to_string),
        after_snapshot: details.after.map(str::to_string),
        created_at,
        message: details.message.map(str::to_string),
        error: details.error.map(str::to_string),
        remote_sync_state: details.remote_sync_state.map(str::to_string),
    };
    append_local_oplog(conn, &op)?;
    append_operation_audit_log(conn, &op)?;
    Ok(())
}

impl From<OperationOriginOverride> for OperationOrigin {
    fn from(value: OperationOriginOverride) -> Self {
        Self {
            label: value.label,
            session_id: value.session_id,
            process_id: value.process_id,
            process_path: value.process_path,
            exe: value.exe,
            confidence: value.confidence,
        }
    }
}

pub(crate) fn append_local_oplog(conn: &Connection, op: &OperationExport) -> Result<()> {
    let Some(path) = local_oplog_path(conn)? else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    let bytes = serde_cbor::to_vec(op)?;
    file.write_all(&bytes)?;
    Ok(())
}

pub(crate) fn rewrite_local_oplog(conn: &Connection) -> Result<()> {
    let Some(path) = local_oplog_path(conn)? else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let operations = query_operations(conn)?;
    let tmp = path.with_extension("cborl.tmp");
    let result = (|| -> Result<()> {
        let mut file = File::create(&tmp)?;
        for op in &operations {
            file.write_all(&serde_cbor::to_vec(op)?)?;
        }
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, &path)?;
        fsync_parent_dir(&path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

pub(crate) fn update_operation_result(
    conn: &Connection,
    id: &str,
    status: &str,
    error: Option<&str>,
    remote_sync_state: Option<&str>,
) -> Result<()> {
    conn.execute(
        "update operations set status=?2, error=?3, remote_sync_state=?4 where id=?1",
        params![id, status, error, remote_sync_state],
    )?;
    rewrite_local_oplog(conn)?;
    let op = query_operation(conn, id)?;
    append_operation_audit_log(conn, &op)?;
    Ok(())
}

pub(crate) fn update_operation_message(
    conn: &Connection,
    id: &str,
    message: Option<&str>,
) -> Result<OperationExport> {
    conn.execute(
        "update operations set message=?2 where id=?1",
        params![id, message],
    )?;
    rewrite_local_oplog(conn)?;
    let op = query_operation(conn, id)?;
    append_operation_audit_log(conn, &op)?;
    Ok(op)
}

pub(crate) fn local_oplog_path(conn: &Connection) -> Result<Option<PathBuf>> {
    let db_path = conn
        .query_row(
            "select file from pragma_database_list where name='main'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(db_path) = db_path.filter(|path| !path.is_empty()) else {
        return Ok(None);
    };
    let db_path = PathBuf::from(db_path);
    let Some(home) = db_path
        .parent()
        .and_then(|parent| parent.parent())
        .map(Path::to_path_buf)
    else {
        return Ok(None);
    };
    Ok(Some(home.join("ops/local-oplog.cborl")))
}

pub(crate) fn append_operation_audit_log(conn: &Connection, op: &OperationExport) -> Result<()> {
    let Some(oplog) = local_oplog_path(conn)? else {
        return Ok(());
    };
    let Some(home) = oplog
        .parent()
        .and_then(|parent| parent.parent())
        .map(Path::to_path_buf)
    else {
        return Ok(());
    };
    let log_dir = home.join("logs");
    fs::create_dir_all(&log_dir)?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("majutsu.log"))?;
    file.write_all(&serde_json::to_vec(op)?)?;
    file.write_all(b"\n")?;
    Ok(())
}

pub(crate) fn current_operation(conn: &Connection) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "select id from operations order by rowid desc limit 1",
            [],
            |row| row.get(0),
        )
        .optional()?)
}

fn operation_actor() -> String {
    let user = env::var("USER")
        .or_else(|_| env::var("LOGNAME"))
        .unwrap_or_else(|_| "local".into());
    let host = env::var("HOSTNAME").unwrap_or_else(|_| "host".into());
    format!("{user}@{host}")
}

#[derive(Debug, Clone)]
struct OperationSession {
    id: Option<String>,
    label: Option<String>,
}

#[derive(Debug, Clone)]
struct OperationProcess {
    id: Option<u32>,
    path: Option<Vec<u32>>,
}

#[derive(Debug, Clone)]
struct OperationOrigin {
    label: Option<String>,
    session_id: Option<String>,
    process_id: Option<u32>,
    process_path: Option<Vec<u32>>,
    exe: Option<String>,
    confidence: Option<String>,
}

fn operation_session() -> OperationSession {
    if env_truthy("MAJUTSU_DAEMON") {
        return OperationSession {
            id: Some(format!("daemon-pid-{}", std::process::id())),
            label: Some("daemon".into()),
        };
    }
    let id = first_non_empty_env(&[
        "MAJUTSU_SESSION_ID",
        "CODEX_THREAD_ID",
        "CLAUDE_SESSION_ID",
        "CURSOR_SESSION_ID",
        "TERM_SESSION_ID",
    ])
    .or_else(|| Some(format!("pid-{}", std::process::id())));
    let label = first_non_empty_env(&["MAJUTSU_SESSION_LABEL", "MAJUTSU_AGENT_NAME"])
        .or_else(|| env::var("CODEX_THREAD_ID").ok().map(|_| "codex".into()))
        .or_else(|| env::var("CLAUDE_SESSION_ID").ok().map(|_| "claude".into()))
        .or_else(|| env::var("CURSOR_SESSION_ID").ok().map(|_| "cursor".into()));
    OperationSession { id, label }
}

fn operation_process() -> OperationProcess {
    let pid = std::process::id();
    OperationProcess {
        id: Some(pid),
        path: Some(process_path(pid)),
    }
}

fn operation_origin(session: &OperationSession, process: &OperationProcess) -> OperationOrigin {
    let explicit_pid =
        first_non_empty_env(&["MAJUTSU_ORIGIN_PID"]).and_then(|pid| pid.parse::<u32>().ok());
    let explicit_label = first_non_empty_env(&["MAJUTSU_ORIGIN_LABEL"]);
    let explicit_session_id = first_non_empty_env(&["MAJUTSU_ORIGIN_SESSION_ID"]);
    let explicit_exe = first_non_empty_env(&["MAJUTSU_ORIGIN_EXE"]);
    let explicit_confidence = first_non_empty_env(&["MAJUTSU_ORIGIN_CONFIDENCE"]);
    if explicit_pid.is_some()
        || explicit_label.is_some()
        || explicit_session_id.is_some()
        || explicit_exe.is_some()
    {
        let process_path = explicit_pid
            .map(process_path)
            .filter(|path| !path.is_empty());
        return OperationOrigin {
            label: explicit_label,
            session_id: explicit_session_id,
            process_id: explicit_pid,
            process_path,
            exe: explicit_exe,
            confidence: explicit_confidence.or_else(|| Some("explicit".into())),
        };
    }
    if env_truthy("MAJUTSU_DAEMON") {
        return OperationOrigin {
            label: None,
            session_id: None,
            process_id: None,
            process_path: None,
            exe: None,
            confidence: None,
        };
    }
    OperationOrigin {
        label: session.label.clone(),
        session_id: session.id.clone(),
        process_id: process.id,
        process_path: process.path.clone(),
        exe: current_exe_string(),
        confidence: Some("self".into()),
    }
}

fn current_exe_string() -> Option<String> {
    env::current_exe()
        .ok()
        .map(|path| path.to_string_lossy().into_owned())
}

#[cfg(target_os = "linux")]
fn process_exe_string(pid: u32) -> Option<String> {
    fs::read_link(format!("/proc/{pid}/exe"))
        .ok()
        .map(|path| path.to_string_lossy().into_owned())
}

fn process_path(pid: u32) -> Vec<u32> {
    let mut path = Vec::new();
    let mut current = pid;
    for _ in 0..64 {
        path.push(current);
        let Some(parent) = parent_pid(current) else {
            break;
        };
        if parent == 0 || parent == current {
            break;
        }
        current = parent;
    }
    path.reverse();
    path
}

#[cfg(target_os = "linux")]
fn parent_pid(pid: u32) -> Option<u32> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    parse_linux_stat_parent_pid(&stat)
}

#[cfg(not(target_os = "linux"))]
fn parent_pid(_pid: u32) -> Option<u32> {
    None
}

#[cfg(target_os = "linux")]
fn parse_linux_stat_parent_pid(stat: &str) -> Option<u32> {
    let close = stat.rfind(") ")?;
    let rest = stat.get(close + 2..)?;
    let mut parts = rest.split_whitespace();
    let _state = parts.next()?;
    parts.next()?.parse().ok()
}

fn first_non_empty_env(names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| env::var(name).ok().filter(|value| !value.trim().is_empty()))
}

fn env_truthy(name: &str) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

pub(crate) fn query_operation(conn: &Connection, op_id: &str) -> Result<OperationExport> {
    conn.query_row(
        "select id, parent_op, kind, actor, session_id, session_label, process_id, process_path, origin_label, origin_session_id, origin_process_id, origin_process_path, origin_exe, origin_confidence, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state from operations where id=?1",
        params![op_id],
        |row| {
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
                process_path: parse_process_path_json(process_path_json.as_deref()),
                origin_label: row.get(8)?,
                origin_session_id: row.get(9)?,
                origin_process_id: row.get::<_, Option<i64>>(10)?.map(|pid| pid as u32),
                origin_process_path: parse_process_path_json(origin_process_path_json.as_deref()),
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
        },
    )
    .optional()?
    .ok_or_else(|| anyhow!("unknown operation: {op_id}"))
}

pub(crate) fn query_operation_resolved(conn: &Connection, input: &str) -> Result<OperationExport> {
    match query_operation(conn, input) {
        Ok(op) => Ok(op),
        Err(_) => {
            let matches = operation_ids_with_prefix(conn, input)?;
            match matches.as_slice() {
                [id] => query_operation(conn, id),
                [] => Err(anyhow!("unknown operation: {input}")),
                _ => Err(anyhow!("ambiguous operation prefix: {input}")),
            }
        }
    }
}

fn operation_ids_with_prefix(conn: &Connection, prefix: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("select id from operations where id like ?1 order by created_at")?;
    let rows = stmt.query_map(params![format!("{prefix}%")], |row| row.get(0))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

pub(crate) fn query_operations(conn: &Connection) -> Result<Vec<OperationExport>> {
    let mut stmt = conn.prepare(
        "select id, parent_op, kind, actor, session_id, session_label, process_id, process_path, origin_label, origin_session_id, origin_process_id, origin_process_path, origin_exe, origin_confidence, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state from operations order by created_at",
    )?;
    let rows = stmt.query_map([], |row| {
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
            process_path: parse_process_path_json(process_path_json.as_deref()),
            origin_label: row.get(8)?,
            origin_session_id: row.get(9)?,
            origin_process_id: row.get::<_, Option<i64>>(10)?.map(|pid| pid as u32),
            origin_process_path: parse_process_path_json(origin_process_path_json.as_deref()),
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
    })?;
    let mut operations = Vec::new();
    for row in rows {
        operations.push(row?);
    }
    Ok(operations)
}

fn parse_process_path_json(value: Option<&str>) -> Option<Vec<u32>> {
    value
        .and_then(|value| serde_json::from_str::<Vec<u32>>(value).ok())
        .filter(|tree| !tree.is_empty())
}
