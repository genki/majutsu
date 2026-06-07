use anyhow::{Result, bail};
use majutsu_core::LargeManifest;
use majutsu_large::{LargePinIssue, large_chunk_hash_matches, large_pin_issues};

use crate::config::{Paths, read_config};
use crate::object_paths::local_object_keys;
use crate::operation_log::record_op;
use crate::snapshot_state::current_snapshot;
use crate::{
    export_metadata, open_db, read_blob_payload, read_large_chunk, read_object,
    validate_config_roots, validate_event_queue, validate_host_file, validate_local_history_graph,
    validate_local_large_manifest_objects, validate_local_metadata_references,
    validate_local_oplog, validate_local_pack_objects, validate_local_refs,
    validate_local_snapshot_objects, validate_remote_refs, validate_restore_queue,
    validate_upload_queue,
};

pub(crate) fn fsck(paths: &Paths) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let mut missing = 0usize;
    let config = read_config(paths)?;
    let export = export_metadata(&conn, &config)?;
    for key in local_object_keys(&export) {
        let full = paths.home.join(&key);
        if !full.exists() {
            missing += 1;
            eprintln!("missing object {key}");
        } else if let Err(err) = read_object(paths, &key) {
            missing += 1;
            eprintln!("unreadable object {key}: {err}");
        }
    }
    let mut stmt = conn.prepare("select oid, object_key, pack_id from blobs")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    for row in rows {
        let (oid, key, pack_id) = row?;
        if pack_id.is_some() {
            if let Err(err) = read_blob_payload(paths, &conn, &oid, &key) {
                missing += 1;
                eprintln!("unreadable packed blob {oid}: {err}");
            }
        } else if !paths.home.join(&key).exists() {
            missing += 1;
            eprintln!("missing blob {oid} {key}");
        } else if let Err(err) = read_object(paths, &key) {
            missing += 1;
            eprintln!("unreadable blob {oid} {key}: {err}");
        }
    }
    let mut stmt = conn.prepare("select oid, object_key from chunks")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (oid, key) = row?;
        if !paths.home.join(&key).exists() {
            missing += 1;
            eprintln!("missing chunk {oid} {key}");
        } else if let Err(err) = read_object(paths, &key) {
            missing += 1;
            eprintln!("unreadable chunk {oid} {key}: {err}");
        }
    }
    let mut stmt = conn.prepare("select oid, manifest_key from large_objects")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (oid, manifest_key) = row?;
        match read_object(paths, &manifest_key)
            .and_then(|bytes| serde_json::from_slice::<LargeManifest>(&bytes).map_err(Into::into))
        {
            Ok(manifest) => {
                for chunk in &manifest.chunks {
                    match read_large_chunk(paths, chunk) {
                        Ok(bytes) if large_chunk_hash_matches(chunk, &bytes) => {}
                        Ok(_) => {
                            missing += 1;
                            eprintln!("large chunk hash mismatch {} {}", oid, chunk.object_key);
                        }
                        Err(err) => {
                            missing += 1;
                            eprintln!("unreadable large chunk {} {}: {err}", oid, chunk.object_key);
                        }
                    }
                }
            }
            Err(err) => {
                missing += 1;
                eprintln!("unreadable large manifest {oid} {manifest_key}: {err}");
            }
        }
    }
    for issue in large_pin_issues(&export.large_pins, &export.large_objects) {
        missing += 1;
        match issue {
            LargePinIssue::Dangling { oid, pinned_at } => {
                eprintln!("dangling large pin {oid} pinned_at={pinned_at}");
            }
            LargePinIssue::InvalidTimestamp { oid, pinned_at } => {
                eprintln!("invalid large pin timestamp {oid} pinned_at={pinned_at}");
            }
        }
    }
    validate_host_file(paths, &config, &mut missing)?;
    validate_config_roots(paths, &conn, &config, &mut missing)?;
    validate_local_refs(&conn, &mut missing)?;
    validate_remote_refs(&conn, &config, &mut missing)?;
    validate_local_history_graph(&export, &mut missing)?;
    validate_local_snapshot_objects(paths, &export, &mut missing)?;
    validate_local_large_manifest_objects(paths, &export, &mut missing)?;
    validate_local_pack_objects(paths, &export, &mut missing)?;
    validate_local_metadata_references(paths, &export, &mut missing)?;
    validate_local_oplog(&conn, &mut missing)?;
    validate_upload_queue(paths, &mut missing)?;
    validate_event_queue(paths, &mut missing)?;
    validate_restore_queue(paths, &conn, &mut missing)?;
    if missing > 0 {
        bail!("fsck found {missing} problems");
    }
    let current = current_snapshot(&conn)?;
    record_op(
        &conn,
        "fsck",
        current.as_deref(),
        current.as_deref(),
        Some("checked local state"),
    )?;
    println!("fsck ok");
    Ok(())
}
