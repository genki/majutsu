use crate::majutsu_store::{host_current_ref_key, host_last_synced_ref_key};
use anyhow::Result;
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;

pub(crate) fn ref_value(conn: &Connection, name: &str) -> Result<Option<String>> {
    conn.query_row(
        "select value from refs where name=?1",
        params![name],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub(crate) fn set_ref_value(conn: &Connection, name: &str, value: &str) -> Result<()> {
    conn.execute(
        "insert into refs(name, value) values (?1, ?2)
         on conflict(name) do update set value=excluded.value",
        params![name, value],
    )?;
    Ok(())
}

pub(crate) fn restore_ref_value(conn: &Connection, name: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        set_ref_value(conn, name, value)
    } else {
        conn.execute("delete from refs where name=?1", params![name])?;
        Ok(())
    }
}

pub(crate) fn set_remote_ref_value(
    conn: &Connection,
    remote: &str,
    name: &str,
    value: &str,
) -> Result<()> {
    conn.execute(
        "insert into remote_refs(remote, name, value, observed_at) values (?1, ?2, ?3, ?4)
         on conflict(remote, name) do update set value=excluded.value, observed_at=excluded.observed_at",
        params![remote, name, value, Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

pub(crate) fn persist_export_remote_refs(
    conn: &Connection,
    remote: &str,
    host_id: &str,
    refs: &BTreeMap<String, String>,
) -> Result<()> {
    if let Some(current) = refs.get("current") {
        set_remote_ref_value(conn, remote, &host_current_ref_key(host_id), current)?;
    }
    if let Some(last_synced) = refs.get("last-synced") {
        set_remote_ref_value(
            conn,
            remote,
            &host_last_synced_ref_key(host_id),
            last_synced,
        )?;
    }
    Ok(())
}
