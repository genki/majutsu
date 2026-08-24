use crate::majutsu_store::{
    host_current_ref_key, host_last_synced_ref_key, host_root_ack_ref_prefix,
};
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

pub(crate) fn prune_remote_refs_for_host(
    conn: &Connection,
    remote: &str,
    host_id: &str,
) -> Result<usize> {
    let prefix = format!("{host_id}/");
    let removed = conn.execute(
        "delete from remote_refs
         where remote != ?1 or substr(name, 1, ?2) != ?3",
        params![remote, prefix.len() as i64, prefix],
    )?;
    Ok(removed)
}

pub(crate) fn prune_remote_root_ack_refs(
    conn: &Connection,
    remote: &str,
    host_id: &str,
) -> Result<usize> {
    let prefix = host_root_ack_ref_prefix(host_id);
    let removed = conn.execute(
        "delete from remote_refs
         where remote = ?1 and substr(name, 1, ?2) = ?3",
        params![remote, prefix.len() as i64, prefix],
    )?;
    Ok(removed)
}

pub(crate) fn persist_export_remote_refs(
    conn: &Connection,
    remote: &str,
    host_id: &str,
    refs: &BTreeMap<String, String>,
) -> Result<()> {
    prune_remote_refs_for_host(conn, remote, host_id)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn remote_refs_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "create table remote_refs(
                remote text not null,
                name text not null,
                value text not null,
                observed_at text not null,
                primary key(remote, name)
            );",
        )
        .unwrap();
        conn
    }

    #[test]
    fn prune_remote_root_ack_refs_replaces_only_selected_host_ack_set() {
        let conn = remote_refs_db();
        for (remote, name) in [
            ("s3://bucket", "host-a/roots/current/ack"),
            ("s3://bucket", "host-a/roots/removed/ack"),
            ("s3://bucket", "host-a/refs/current"),
            ("s3://bucket", "host-b/roots/other/ack"),
            ("s3://other", "host-a/roots/other/ack"),
        ] {
            set_remote_ref_value(&conn, remote, name, "value").unwrap();
        }

        assert_eq!(
            prune_remote_root_ack_refs(&conn, "s3://bucket", "host-a").unwrap(),
            2
        );

        let remaining = conn
            .prepare("select remote, name from remote_refs order by remote, name")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            remaining,
            vec![
                ("s3://bucket".to_string(), "host-a/refs/current".to_string()),
                (
                    "s3://bucket".to_string(),
                    "host-b/roots/other/ack".to_string()
                ),
                (
                    "s3://other".to_string(),
                    "host-a/roots/other/ack".to_string()
                ),
            ]
        );
    }
}
