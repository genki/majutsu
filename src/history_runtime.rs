use anyhow::{Result, anyhow, bail};
use rusqlite::{Connection, params};

use crate::cli::{DiffArgs, LogArgs, OpCommand};
use crate::config::Paths;
use crate::operation_log::{query_operation, record_op};
use crate::root_state::roots;
use crate::snapshot_state::{
    current_snapshot, load_snapshot_by_id, snapshot_contains_root, snapshot_file_map,
    snapshot_id_at,
};

pub(crate) fn status_cmd(paths: &Paths) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    let roots = roots(&conn)?;
    let current = current_snapshot(&conn)?;
    println!("home {}", paths.home.display());
    println!("roots {}", roots.len());
    for root in roots {
        let state = if root.status == "active" && !root.path.exists() {
            "missing"
        } else {
            root.status.as_str()
        };
        println!("  {}\t{}\t{}", root.id, state, root.path.display());
    }
    println!("current {}", current.unwrap_or_else(|| "(none)".into()));
    Ok(())
}

pub(crate) fn log_cmd(paths: &Paths, args: LogArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    print_op_log(&conn, &args)
}

fn print_op_log(conn: &Connection, args: &LogArgs) -> Result<()> {
    let mut stmt = conn.prepare(
        "select id, kind, before_snapshot, after_snapshot, created_at, message, status, remote_sync_state
         from operations order by rowid desc",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Option<String>>(7)?,
        ))
    })?;
    let mut printed = 0usize;
    for row in rows {
        let (id, kind, before, after, created, message, status, remote_sync_state) = row?;
        if let Some(root) = &args.root {
            let matches_root = message.as_deref() == Some(root)
                || before
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(conn, snapshot, root).ok())
                    .unwrap_or(false)
                || after
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(conn, snapshot, root).ok())
                    .unwrap_or(false);
            if !matches_root {
                continue;
            }
        }
        if printed >= args.limit {
            break;
        }
        println!(
            "{id}\t{created}\t{kind}\t{status}\t{}\t{} -> {}\t{}",
            remote_sync_state.unwrap_or_else(|| "-".into()),
            before.unwrap_or_default(),
            after.unwrap_or_default(),
            message.unwrap_or_default()
        );
        printed += 1;
    }
    Ok(())
}

pub(crate) fn op_cmd(paths: &Paths, command: OpCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    match command {
        OpCommand::Log(args) => print_op_log(&conn, &args),
        OpCommand::Show { op_id } => {
            let op = query_operation(&conn, &op_id)?;
            println!("id {}", op.id);
            println!("parent {}", op.parent_op.unwrap_or_else(|| "(none)".into()));
            println!("kind {}", op.kind);
            println!("actor {}", op.actor);
            println!("status {}", op.status);
            println!(
                "before {}",
                op.before_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!(
                "after {}",
                op.after_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!("created_at {}", op.created_at);
            println!("message {}", op.message.unwrap_or_default());
            println!("error {}", op.error.unwrap_or_default());
            println!(
                "remote_sync_state {}",
                op.remote_sync_state.unwrap_or_default()
            );
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
    let to = load_snapshot_by_id(&conn, &to_id)?;
    let from_id = if let Some(at) = &args.at {
        Some(snapshot_id_at(&conn, at)?)
    } else {
        args.from.or_else(|| to.parent.clone())
    };
    let from = if let Some(from_id) = from_id {
        Some(load_snapshot_by_id(&conn, &from_id)?)
    } else {
        None
    };
    let from_files = from
        .as_ref()
        .map(snapshot_file_map)
        .transpose()?
        .unwrap_or_default();
    let to_files = snapshot_file_map(&to)?;
    let mut paths_all = from_files.keys().cloned().collect::<Vec<_>>();
    paths_all.extend(
        to_files
            .keys()
            .filter(|key| !from_files.contains_key(*key))
            .cloned(),
    );
    paths_all.sort();
    for key in paths_all {
        if let Some(root) = &args.root {
            if !key.starts_with(&format!("{root}/")) {
                continue;
            }
        }
        match (from_files.get(&key), to_files.get(&key)) {
            (None, Some(_)) => println!("A\t{key}"),
            (Some(_), None) => println!("D\t{key}"),
            (Some(a), Some(b)) if serde_json::to_value(a)? != serde_json::to_value(b)? => {
                println!("M\t{key}");
            }
            _ => {}
        }
    }
    Ok(())
}
