use anyhow::{Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::{Path, PathBuf};

use crate::config::{
    Config, ConfigRoot, Paths, RootConfig, default_root_status, read_config,
    validate_large_chunking, validate_snapshot_mode, write_config,
};

pub(crate) fn roots(conn: &Connection) -> Result<Vec<RootConfig>> {
    let mut stmt = conn.prepare("select data_json from roots order by id")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(serde_json::from_str(&row?)?);
    }
    Ok(out)
}

impl From<&RootConfig> for ConfigRoot {
    fn from(root: &RootConfig) -> Self {
        Self {
            id: root.id.clone(),
            name: Some(root.name.clone()),
            path: root.path.clone(),
            include: root.include.clone(),
            exclude: root.exclude.clone(),
            follow_symlinks: root.follow_symlinks,
            require_mount: root.require_mount,
            status: Some(root.status.clone()),
            degraded: root.degraded.clone(),
            snapshot_mode: root.snapshot_mode.clone(),
            pre_snapshot: root.pre_snapshot.clone(),
            post_snapshot: root.post_snapshot.clone(),
            snapshot_source: root.snapshot_source.clone(),
            application_plugin: root.application_plugin.clone(),
            large: root.large.clone(),
        }
    }
}

pub(crate) fn sync_config_roots(paths: &Paths, conn: &Connection, config: &Config) -> Result<()> {
    if config.roots.is_empty() {
        return Ok(());
    }
    for config_root in &config.roots {
        let existing = root_by_id_optional(conn, &config_root.id)?;
        let root = config_root.to_root_config(paths, existing.as_ref())?;
        conn.execute(
            "insert into roots(id, data_json) values (?1, ?2)
             on conflict(id) do update set data_json=excluded.data_json",
            params![root.id, serde_json::to_string(&root)?],
        )?;
    }
    Ok(())
}

pub(crate) fn sync_roots_to_config(paths: &Paths, conn: &Connection) -> Result<()> {
    let mut config = read_config(paths)?;
    config.roots = roots(conn)?.iter().map(ConfigRoot::from).collect();
    write_config(paths, &config)
}

impl ConfigRoot {
    pub(crate) fn to_root_config(
        &self,
        paths: &Paths,
        existing: Option<&RootConfig>,
    ) -> Result<RootConfig> {
        validate_snapshot_mode(&self.snapshot_mode)?;
        if let Some(large) = &self.large
            && let Some(chunking) = &large.default_chunking
        {
            validate_large_chunking(chunking)?;
        }
        let snapshot_source = self
            .snapshot_source
            .as_ref()
            .map(|path| config_relative_path(paths, path))
            .transpose()?;
        if snapshot_source.is_some() && self.snapshot_mode != "transactional" {
            bail!(
                "root {} snapshot_source requires snapshot_mode transactional",
                self.id
            );
        }
        Ok(RootConfig {
            id: self.id.clone(),
            name: self.name.clone().unwrap_or_else(|| self.id.clone()),
            path: config_relative_path(paths, &self.path)?,
            include: self.include.clone(),
            exclude: self.exclude.clone(),
            follow_symlinks: self.follow_symlinks,
            require_mount: self.require_mount,
            status: self
                .status
                .clone()
                .or_else(|| existing.map(|root| root.status.clone()))
                .unwrap_or_else(default_root_status),
            degraded: self
                .degraded
                .clone()
                .or_else(|| existing.and_then(|root| root.degraded.clone())),
            snapshot_mode: self.snapshot_mode.clone(),
            pre_snapshot: self.pre_snapshot.clone(),
            post_snapshot: self.post_snapshot.clone(),
            snapshot_source,
            application_plugin: self.application_plugin.clone(),
            large: self.large.clone(),
        })
    }
}

fn config_relative_path(paths: &Paths, path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let base = paths.config.parent().unwrap_or(&paths.home);
    Ok(base.join(path))
}

pub(crate) fn root_by_id_optional(conn: &Connection, id: &str) -> Result<Option<RootConfig>> {
    let data: Option<String> = conn
        .query_row(
            "select data_json from roots where id=?1",
            params![id],
            |row| row.get(0),
        )
        .optional()?;
    data.map(|data| serde_json::from_str(&data).map_err(Into::into))
        .transpose()
}

pub(crate) fn root_by_id(conn: &Connection, id: &str) -> Result<RootConfig> {
    root_by_id_optional(conn, id)?.ok_or_else(|| anyhow!("unknown root: {id}"))
}

pub(crate) fn save_root(conn: &Connection, root: &RootConfig) -> Result<()> {
    conn.execute(
        "update roots set data_json=?2 where id=?1",
        params![root.id, serde_json::to_string(root)?],
    )?;
    Ok(())
}

pub(crate) fn update_root_status(conn: &Connection, id: &str, status: &str) -> Result<()> {
    let mut root = root_by_id(conn, id)?;
    root.status = status.to_string();
    if status == "active" {
        root.degraded = None;
    }
    save_root(conn, &root)
}

pub(crate) fn update_root_degraded(
    conn: &Connection,
    id: &str,
    status: &str,
    degraded: crate::config::RootDegraded,
) -> Result<()> {
    let mut root = root_by_id(conn, id)?;
    root.status = status.to_string();
    root.degraded = Some(degraded);
    save_root(conn, &root)
}
