use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use majutsu_core::{SnapshotManifest, payload_large_ref};
use rusqlite::{Connection, params};
use std::collections::BTreeSet;

use crate::cli::FsckArgs;
use crate::cli::LargeCommand;
use crate::config::Paths;
use crate::fsck_runtime::fsck;
use crate::operation_log::record_op;
use crate::snapshot_state::{current_snapshot, load_snapshot_by_id, snapshot_manifest_from_parts};
use crate::util::{parse_db_time, parse_duration_ago, parse_time};
use crate::{ensure_ready, open_db};

pub(crate) fn large_cmd(paths: &Paths, command: LargeCommand) -> Result<()> {
    ensure_ready(paths)?;
    let conn = open_db(paths)?;
    match command {
        LargeCommand::List => {
            let mut stmt = conn.prepare(
                "select l.oid, l.size, l.chunk_count, l.manifest_key, p.oid is not null
                 from large_objects l left join large_pins p on p.oid = l.oid
                 order by l.rowid desc",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, usize>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            })?;
            for row in rows {
                let (oid, size, chunks, key, pinned) = row?;
                let pin = if pinned { "pinned" } else { "unpinned" };
                println!("{oid}\t{size}\t{chunks}\t{pin}\t{key}");
            }
        }
        LargeCommand::Stat => {
            let count: i64 =
                conn.query_row("select count(*) from large_objects", [], |r| r.get(0))?;
            let bytes: Option<u64> =
                conn.query_row("select sum(size) from large_objects", [], |r| r.get(0))?;
            let chunks: i64 = conn.query_row("select count(*) from chunks", [], |r| r.get(0))?;
            let pins: i64 = conn.query_row("select count(*) from large_pins", [], |r| r.get(0))?;
            println!("large_objects {count}");
            println!("logical_bytes {}", bytes.unwrap_or(0));
            println!("chunks {chunks}");
            println!("pinned {pins}");
        }
        LargeCommand::Verify => fsck(paths, FsckArgs::default())?,
        LargeCommand::Pin(args) => {
            let snapshot =
                current_snapshot(&conn)?.ok_or_else(|| anyhow!("no current snapshot"))?;
            let manifests = large_pin_snapshots(paths, &conn, args.since.as_deref(), &snapshot)?;
            let mut pinned = 0usize;
            let mut seen = BTreeSet::new();
            for manifest in manifests {
                for (root_id, records) in manifest.roots {
                    if args.root.as_deref().is_some_and(|filter| filter != root_id) {
                        continue;
                    }
                    for record in records {
                        if let Some((oid, _, _)) = payload_large_ref(&record.payload) {
                            let oid = oid.to_string();
                            if seen.insert(oid.clone()) {
                                conn.execute(
                                    "insert or replace into large_pins(oid, pinned_at, reason) values (?1, ?2, ?3)",
                                    params![
                                        oid,
                                        Utc::now().to_rfc3339(),
                                        args.since
                                            .as_ref()
                                            .map(|since| format!("pin since {since}"))
                                    ],
                                )?;
                                pinned += 1;
                            }
                        }
                    }
                }
            }
            record_op(
                &conn,
                "large-pin",
                Some(&snapshot),
                Some(&snapshot),
                Some(&format!("pinned {pinned} large objects")),
            )?;
            println!("pinned {pinned}");
        }
        LargeCommand::Unpin(args) => {
            let removed = if let Some(older_than) = args.older_than {
                let cutoff = parse_duration_ago(&older_than)?;
                conn.execute(
                    "delete from large_pins where pinned_at <= ?1",
                    params![cutoff.to_rfc3339()],
                )?
            } else {
                conn.execute("delete from large_pins", [])?
            };
            record_op(
                &conn,
                "large-unpin",
                current_snapshot(&conn)?.as_deref(),
                current_snapshot(&conn)?.as_deref(),
                Some(&format!("unpinned {removed} large objects")),
            )?;
            println!("unpinned {removed}");
        }
    }
    Ok(())
}

fn large_pin_snapshots(
    paths: &Paths,
    conn: &Connection,
    since: Option<&str>,
    current_snapshot_id: &str,
) -> Result<Vec<SnapshotManifest>> {
    let Some(since) = since else {
        return Ok(vec![load_snapshot_by_id(paths, conn, current_snapshot_id)?]);
    };
    let cutoff = parse_pin_since(since)?;
    let mut stmt = conn.prepare(
        "select id, manifest_key, manifest_json, created_at from snapshots order by created_at",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut manifests = Vec::new();
    for row in rows {
        let (id, manifest_key, manifest_json, created_at) = row?;
        if parse_db_time(&created_at)? >= cutoff {
            manifests.push(snapshot_manifest_from_parts(
                paths,
                &id,
                &manifest_key,
                &manifest_json,
            )?);
        }
    }
    Ok(manifests)
}

fn parse_pin_since(input: &str) -> Result<DateTime<Utc>> {
    parse_duration_ago(input).or_else(|_| {
        let parsed = parse_time(input)?;
        parse_db_time(&parsed)
    })
}
