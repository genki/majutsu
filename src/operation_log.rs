use anyhow::{Result, anyhow};
use chrono::Utc;
use majutsu_core::OperationLogEntry as OperationExport;
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
}

pub(crate) fn record_op_with_details(
    conn: &Connection,
    details: OperationDetails<'_>,
) -> Result<()> {
    let created_at = Utc::now().to_rfc3339();
    let parent_op = current_operation(conn)?;
    let actor = operation_actor();
    conn.execute(
        "insert into operations(id, parent_op, kind, actor, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state)
         values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            details.id,
            parent_op,
            details.kind,
            actor,
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

pub(crate) fn query_operation(conn: &Connection, op_id: &str) -> Result<OperationExport> {
    conn.query_row(
        "select id, parent_op, kind, actor, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state from operations where id=?1",
        params![op_id],
        |row| {
            Ok(OperationExport {
                id: row.get(0)?,
                parent_op: row.get(1)?,
                kind: row.get(2)?,
                actor: row.get(3)?,
                status: row.get(4)?,
                before_snapshot: row.get(5)?,
                after_snapshot: row.get(6)?,
                created_at: row.get(7)?,
                message: row.get(8)?,
                error: row.get(9)?,
                remote_sync_state: row.get(10)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| anyhow!("unknown operation: {op_id}"))
}

pub(crate) fn query_operations(conn: &Connection) -> Result<Vec<OperationExport>> {
    let mut stmt = conn.prepare(
        "select id, parent_op, kind, actor, status, before_snapshot, after_snapshot, created_at, message, error, remote_sync_state from operations order by created_at",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(OperationExport {
            id: row.get(0)?,
            parent_op: row.get(1)?,
            kind: row.get(2)?,
            actor: row.get(3)?,
            status: row.get(4)?,
            before_snapshot: row.get(5)?,
            after_snapshot: row.get(6)?,
            created_at: row.get(7)?,
            message: row.get(8)?,
            error: row.get(9)?,
            remote_sync_state: row.get(10)?,
        })
    })?;
    let mut operations = Vec::new();
    for row in rows {
        operations.push(row?);
    }
    Ok(operations)
}
