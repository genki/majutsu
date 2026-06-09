use anyhow::{Result, anyhow, bail};
use rusqlite::{Connection, params};
use std::path::PathBuf;

use crate::cli::{
    BranchCommand, BranchCreateArgs, BranchDeleteArgs, BranchRenameArgs, BranchSetHeadArgs,
    BranchSwitchArgs, RestoreArgs, RestoreCommand, RestoreTopArgs,
};
use crate::config::Paths;
use crate::db_refs::{ref_value, set_ref_value};
use crate::operation_log::record_op;
use crate::restore_runtime::restore_cmd;
use crate::snapshot_state::{current_snapshot, load_snapshot_by_id, snapshot_id_at};

pub(crate) const CURRENT_BRANCH_REF: &str = "current-branch";
pub(crate) const BRANCH_REF_PREFIX: &str = "branches/";
pub(crate) const DEFAULT_BRANCH: &str = "main";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchHead {
    pub(crate) name: String,
    pub(crate) snapshot_id: String,
}

pub(crate) fn branch_cmd(paths: &Paths, command: BranchCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    ensure_default_branch(&conn)?;
    match command {
        BranchCommand::List => list_branches(&conn),
        BranchCommand::Current => show_current_branch(&conn),
        BranchCommand::Create(args) => create_branch(paths, &conn, args),
        BranchCommand::Switch(args) => switch_branch(paths, &conn, args),
        BranchCommand::SetHead(args) => set_branch_head(&conn, args),
        BranchCommand::Delete(args) => delete_branch(&conn, args),
        BranchCommand::Rename(args) => rename_branch(&conn, args),
    }
}

pub(crate) fn update_active_branch_head(conn: &Connection, snapshot_id: &str) -> Result<()> {
    let branch = active_branch(conn)?.unwrap_or_else(|| DEFAULT_BRANCH.to_string());
    validate_branch_name(&branch)?;
    set_ref_value(conn, CURRENT_BRANCH_REF, &branch)?;
    set_ref_value(conn, &branch_ref_name(&branch), snapshot_id)?;
    set_ref_value(conn, "current", snapshot_id)?;
    Ok(())
}

pub(crate) fn ensure_default_branch(conn: &Connection) -> Result<()> {
    if !branch_heads(conn)?.is_empty() {
        if ref_value(conn, CURRENT_BRANCH_REF)?.is_none() {
            let current = current_snapshot(conn)?;
            let selected = if let Some(current) = current.as_deref() {
                branch_heads(conn)?
                    .into_iter()
                    .find(|branch| branch.snapshot_id == current)
                    .map(|branch| branch.name)
            } else {
                None
            }
            .or_else(|| {
                branch_heads(conn)
                    .ok()
                    .and_then(|mut branches| branches.drain(..).next().map(|b| b.name))
            })
            .unwrap_or_else(|| DEFAULT_BRANCH.to_string());
            set_ref_value(conn, CURRENT_BRANCH_REF, &selected)?;
        }
        return Ok(());
    }
    if let Some(current) = current_snapshot(conn)? {
        set_ref_value(conn, CURRENT_BRANCH_REF, DEFAULT_BRANCH)?;
        set_ref_value(conn, &branch_ref_name(DEFAULT_BRANCH), &current)?;
    }
    Ok(())
}

fn list_branches(conn: &Connection) -> Result<()> {
    let active = active_branch(conn)?;
    let branches = branch_heads(conn)?;
    println!("branches {}", branches.len());
    for branch in branches {
        let marker = if active.as_deref() == Some(branch.name.as_str()) {
            "*"
        } else {
            " "
        };
        println!("{}\t{}\t{}", marker, branch.name, branch.snapshot_id);
    }
    Ok(())
}

fn show_current_branch(conn: &Connection) -> Result<()> {
    let active = active_branch(conn)?.unwrap_or_else(|| DEFAULT_BRANCH.to_string());
    let head = branch_head(conn, &active)?.unwrap_or_else(|| "(none)".into());
    println!("branch {}", active);
    println!("snapshot {}", head);
    Ok(())
}

fn create_branch(paths: &Paths, conn: &Connection, args: BranchCreateArgs) -> Result<()> {
    validate_branch_name(&args.name)?;
    let snapshot_id = resolve_snapshot(conn, args.snapshot.as_deref(), args.at.as_deref())?;
    let exists = branch_head(conn, &args.name)?.is_some();
    if exists && !args.force {
        bail!(
            "branch already exists: {}; use --force to move it",
            args.name
        );
    }
    load_snapshot_by_id(conn, &snapshot_id)?;
    let before = current_snapshot(conn)?;
    set_ref_value(conn, &branch_ref_name(&args.name), &snapshot_id)?;
    let mut after = before.clone();
    if active_branch(conn)?.as_deref() == Some(args.name.as_str()) {
        set_ref_value(conn, "current", &snapshot_id)?;
        after = Some(snapshot_id.clone());
    }
    record_op(
        conn,
        "config-change",
        before.as_deref(),
        after.as_deref(),
        Some(&format!("branch-create {} -> {}", args.name, snapshot_id)),
    )?;
    println!("created branch {} -> {}", args.name, snapshot_id);
    if args.switch || args.restore {
        switch_to_branch(
            paths,
            conn,
            &args.name,
            BranchRestoreOptions {
                restore: args.restore,
                to: args.to,
                force: args.force,
            },
        )?;
    }
    Ok(())
}

fn switch_branch(paths: &Paths, conn: &Connection, args: BranchSwitchArgs) -> Result<()> {
    validate_branch_name(&args.name)?;
    switch_to_branch(
        paths,
        conn,
        &args.name,
        BranchRestoreOptions {
            restore: args.restore,
            to: args.to,
            force: args.force,
        },
    )
}

fn set_branch_head(conn: &Connection, args: BranchSetHeadArgs) -> Result<()> {
    validate_branch_name(&args.name)?;
    let Some(old_head) = branch_head(conn, &args.name)? else {
        bail!("unknown branch: {}", args.name);
    };
    let snapshot_id = resolve_snapshot(conn, args.snapshot.as_deref(), args.at.as_deref())?;
    load_snapshot_by_id(conn, &snapshot_id)?;
    set_ref_value(conn, &branch_ref_name(&args.name), &snapshot_id)?;
    if active_branch(conn)?.as_deref() == Some(args.name.as_str()) {
        set_ref_value(conn, "current", &snapshot_id)?;
    }
    record_op(
        conn,
        "config-change",
        Some(&old_head),
        Some(&snapshot_id),
        Some(&format!(
            "branch-set-head {} {} -> {}",
            args.name, old_head, snapshot_id
        )),
    )?;
    println!("moved branch {} {} -> {}", args.name, old_head, snapshot_id);
    Ok(())
}

fn delete_branch(conn: &Connection, args: BranchDeleteArgs) -> Result<()> {
    validate_branch_name(&args.name)?;
    let Some(head) = branch_head(conn, &args.name)? else {
        bail!("unknown branch: {}", args.name);
    };
    let active = active_branch(conn)?;
    if active.as_deref() == Some(args.name.as_str()) && !args.force {
        bail!("cannot delete current branch {}; use --force", args.name);
    }
    conn.execute(
        "delete from refs where name=?1",
        params![branch_ref_name(&args.name)],
    )?;
    if active.as_deref() == Some(args.name.as_str()) {
        let remaining = branch_heads(conn)?;
        if let Some(next) = remaining.first() {
            set_ref_value(conn, CURRENT_BRANCH_REF, &next.name)?;
            set_ref_value(conn, "current", &next.snapshot_id)?;
        } else {
            conn.execute(
                "delete from refs where name=?1",
                params![CURRENT_BRANCH_REF],
            )?;
        }
    }
    record_op(
        conn,
        "config-change",
        Some(&head),
        current_snapshot(conn)?.as_deref(),
        Some(&format!("branch-delete {}", args.name)),
    )?;
    println!("deleted branch {}", args.name);
    Ok(())
}

fn rename_branch(conn: &Connection, args: BranchRenameArgs) -> Result<()> {
    validate_branch_name(&args.old)?;
    validate_branch_name(&args.new)?;
    if args.old == args.new {
        bail!("new branch name must differ from old branch name");
    }
    let active = active_branch(conn)?;
    let source_was_active = active.as_deref() == Some(args.old.as_str());
    let destination_was_active = active.as_deref() == Some(args.new.as_str());
    if branch_head(conn, &args.new)?.is_some() && !args.force {
        bail!(
            "branch already exists: {}; use --force to overwrite",
            args.new
        );
    }
    let Some(head) = branch_head(conn, &args.old)? else {
        bail!("unknown branch: {}", args.old);
    };
    if destination_was_active && !source_was_active && !args.force {
        bail!("cannot overwrite active branch {}; use --force", args.new);
    }
    let before = current_snapshot(conn)?;
    set_ref_value(conn, &branch_ref_name(&args.new), &head)?;
    conn.execute(
        "delete from refs where name=?1",
        params![branch_ref_name(&args.old)],
    )?;
    if source_was_active {
        set_ref_value(conn, CURRENT_BRANCH_REF, &args.new)?;
    }
    if source_was_active || destination_was_active {
        set_ref_value(conn, "current", &head)?;
    }
    let after = current_snapshot(conn)?;
    record_op(
        conn,
        "config-change",
        before.as_deref(),
        after.as_deref(),
        Some(&format!("branch-rename {} -> {}", args.old, args.new)),
    )?;
    println!("renamed branch {} -> {}", args.old, args.new);
    Ok(())
}

struct BranchRestoreOptions {
    restore: bool,
    to: Option<PathBuf>,
    force: bool,
}

fn switch_to_branch(
    paths: &Paths,
    conn: &Connection,
    name: &str,
    options: BranchRestoreOptions,
) -> Result<()> {
    let Some(snapshot_id) = branch_head(conn, name)? else {
        bail!("unknown branch: {name}");
    };
    load_snapshot_by_id(conn, &snapshot_id)?;
    let before = current_snapshot(conn)?;
    if options.restore {
        apply_branch_restore(paths, &snapshot_id, options.to, options.force).map_err(|err| {
            anyhow!(
                "branch restore failed while switching to {name}; active branch refs were left unchanged: {err:#}"
            )
        })?;
    }
    set_ref_value(conn, CURRENT_BRANCH_REF, name)?;
    set_ref_value(conn, "current", &snapshot_id)?;
    record_op(
        conn,
        "config-change",
        before.as_deref(),
        Some(&snapshot_id),
        Some(&format!("branch-switch {name}")),
    )?;
    println!("switched branch {} -> {}", name, snapshot_id);
    Ok(())
}

fn apply_branch_restore(
    paths: &Paths,
    snapshot_id: &str,
    to: Option<PathBuf>,
    force: bool,
) -> Result<()> {
    let args = RestoreArgs {
        snapshot: Some(snapshot_id.to_string()),
        at: None,
        root: None,
        path: None,
        to,
        force,
        check_conflicts: true,
    };
    restore_cmd(
        paths,
        RestoreTopArgs {
            command: Some(RestoreCommand::Apply(args.clone())),
            args,
        },
    )
}

fn resolve_snapshot(conn: &Connection, snapshot: Option<&str>, at: Option<&str>) -> Result<String> {
    match (snapshot, at) {
        (Some(_), Some(_)) => bail!("use either --snapshot or --at, not both"),
        (Some(snapshot), None) => {
            load_snapshot_by_id(conn, snapshot)?;
            Ok(snapshot.to_string())
        }
        (None, Some(at)) => snapshot_id_at(conn, at),
        (None, None) => current_snapshot(conn)?.ok_or_else(|| anyhow!("no current snapshot")),
    }
}

fn active_branch(conn: &Connection) -> Result<Option<String>> {
    ref_value(conn, CURRENT_BRANCH_REF)
}

fn branch_head(conn: &Connection, name: &str) -> Result<Option<String>> {
    ref_value(conn, &branch_ref_name(name))
}

fn branch_heads(conn: &Connection) -> Result<Vec<BranchHead>> {
    let mut stmt =
        conn.prepare("select name, value from refs where name like 'branches/%' order by name")?;
    let rows = stmt.query_map([], |row| {
        let raw: String = row.get(0)?;
        let snapshot_id: String = row.get(1)?;
        Ok(BranchHead {
            name: raw
                .strip_prefix(BRANCH_REF_PREFIX)
                .unwrap_or(raw.as_str())
                .to_string(),
            snapshot_id,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn branch_ref_name(name: &str) -> String {
    format!("{BRANCH_REF_PREFIX}{name}")
}

fn validate_branch_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        bail!("branch name must not be empty");
    }
    if name == "." || name == ".." || name.contains("..") {
        bail!("branch name must not contain '..'");
    }
    if name.contains('/') || name.contains('\\') {
        bail!("branch name must not contain path separators");
    }
    if name.chars().any(char::is_whitespace) {
        bail!("branch name must not contain whitespace");
    }
    if matches!(name, "current" | "last-synced" | "current-branch") {
        bail!("branch name is reserved: {name}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_branch_name;

    #[test]
    fn validates_branch_names() {
        validate_branch_name("main").unwrap();
        validate_branch_name("feature-1").unwrap();
        assert!(validate_branch_name("").is_err());
        assert!(validate_branch_name("feature/x").is_err());
        assert!(validate_branch_name("bad name").is_err());
        assert!(validate_branch_name("..").is_err());
    }
}
