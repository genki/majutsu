use crate::majutsu_core::{LargeManifest, Payload, payload_blob_ref, payload_large_ref};
use crate::majutsu_restore::validate_relative_filter_path;
use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use std::ffi::OsStr;
use std::fs::{self, File};
use walkdir::WalkDir;

use crate::atomic_io::write_atomic;
use crate::cli::{HydrateArgs, MountArgs, RestoreArgs, UnmountArgs};
use crate::config::{LazyMountEntry, MountViewMetadata, Paths};
use crate::fuse_mount::{is_mountpoint, mount_fuse_cmd, prepare_mountpoint, unmount_fuse};
use crate::operation_log::record_op;
use crate::restore_apply::{
    apply_file_metadata, prepare_directory_restore_destination, restore_special_file,
};
use crate::util::path_to_slash;
use crate::{
    build_restore_plan, open_db, read_blob_payload, read_object, write_large_chunks_atomic,
};

pub(crate) fn mount_cmd(paths: &Paths, args: MountArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let restore_args = RestoreArgs {
        snapshot: args.snapshot.clone(),
        at: args.at.clone(),
        root: args.root.clone(),
        path: args.path.clone(),
        to: Some(args.mountpoint.clone()),
        force: true,
        check_conflicts: false,
    };
    let plan = build_restore_plan(paths, &conn, &restore_args)?;
    if args.backend == "fuse" {
        return mount_fuse_cmd(paths, &conn, &plan);
    }
    if args.backend != "materialized" {
        bail!("mount backend must be materialized or fuse");
    }
    let mountpoint = plan
        .to
        .as_ref()
        .ok_or_else(|| anyhow!("mount requires a target directory"))?;
    prepare_mountpoint(mountpoint)?;
    let lazy_root = mountpoint.join(".majutsu-lazy");
    let mut lazy_files = 0usize;
    let mut hydrated_large = 0usize;
    let mut directory_metadata = Vec::new();
    for record in &plan.files {
        let dest = mountpoint.join(&record.root_id).join(&record.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        match &record.payload {
            Payload::Directory => {
                prepare_directory_restore_destination(&dest, false)?;
                fs::create_dir_all(&dest)?;
                directory_metadata.push((dest, record));
            }
            Payload::Special { special_kind } => {
                restore_special_file(&dest, record, special_kind, true)?;
            }
            Payload::Symlink { target } => {
                crate::platform_runtime::create_symlink(target, &dest)?;
            }
            payload => {
                if let Some((oid, object_key)) = payload_blob_ref(payload) {
                    write_atomic(&dest, &read_blob_payload(paths, &conn, oid, object_key)?)?;
                    apply_file_metadata(&dest, record)?;
                } else if let Some((_, manifest_key, chunk_count)) = payload_large_ref(payload) {
                    if args.hydrate_large {
                        let manifest: LargeManifest =
                            serde_json::from_slice(&read_object(paths, manifest_key)?)?;
                        write_large_chunks_atomic(paths, &dest, &manifest)?;
                        apply_file_metadata(&dest, record)?;
                        hydrated_large += 1;
                    } else {
                        let file = File::create(&dest)?;
                        file.set_len(record.size)?;
                        apply_file_metadata(&dest, record)?;
                        let sidecar = lazy_root
                            .join(&record.root_id)
                            .join(format!("{}.json", record.path));
                        if let Some(parent) = sidecar.parent() {
                            fs::create_dir_all(parent)?;
                        }
                        let entry = LazyMountEntry {
                            version: 1,
                            snapshot_id: plan.snapshot.snapshot_id.clone(),
                            root_id: record.root_id.clone(),
                            path: record.path.clone(),
                            size: record.size,
                            manifest_key: manifest_key.to_string(),
                            chunk_count,
                        };
                        fs::write(sidecar, serde_json::to_vec_pretty(&entry)?)?;
                        lazy_files += 1;
                    }
                }
            }
        }
    }
    for (dest, record) in directory_metadata {
        apply_file_metadata(&dest, record)?;
    }
    record_op(
        &conn,
        "mount",
        None,
        Some(&plan.snapshot.snapshot_id),
        Some(&format!("at {}", mountpoint.display())),
    )?;
    let mount_metadata = MountViewMetadata {
        version: 1,
        snapshot_id: plan.snapshot.snapshot_id.clone(),
        created_at: Utc::now(),
        hydrate_large: args.hydrate_large,
        files: plan.files.len(),
        lazy_large_files: lazy_files,
        hydrated_large_files: hydrated_large,
    };
    fs::write(
        mountpoint.join(".majutsu-mount.json"),
        serde_json::to_vec_pretty(&mount_metadata)?,
    )?;
    println!("mounted snapshot {}", plan.snapshot.snapshot_id);
    println!("target {}", mountpoint.display());
    println!("files {}", plan.files.len());
    println!("lazy_large_files {lazy_files}");
    println!("hydrated_large_files {hydrated_large}");
    Ok(())
}

pub(crate) fn unmount_cmd(paths: &Paths, args: UnmountArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = open_db(paths)?;
    let marker = args.mountpoint.join(".majutsu-mount.json");
    if !marker.exists() && is_mountpoint(&args.mountpoint)? {
        unmount_fuse(&args.mountpoint)?;
        record_op(
            &conn,
            "unmount-fuse",
            None,
            None,
            Some(&format!("from {}", args.mountpoint.display())),
        )?;
        println!("unmounted {}", args.mountpoint.display());
        return Ok(());
    }
    if !marker.exists() {
        bail!(
            "{} is not a majutsu mount view; missing .majutsu-mount.json",
            args.mountpoint.display()
        );
    }
    let metadata: MountViewMetadata = serde_json::from_slice(&fs::read(&marker)?)
        .with_context(|| format!("read mount metadata {}", marker.display()))?;
    fs::remove_dir_all(&args.mountpoint)
        .with_context(|| format!("remove mount view {}", args.mountpoint.display()))?;
    record_op(
        &conn,
        "unmount",
        Some(&metadata.snapshot_id),
        None,
        Some(&format!("from {}", args.mountpoint.display())),
    )?;
    println!("unmounted {}", args.mountpoint.display());
    println!("snapshot {}", metadata.snapshot_id);
    Ok(())
}

pub(crate) fn hydrate_cmd(paths: &Paths, args: HydrateArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    if let Some(path) = &args.path {
        validate_relative_filter_path(path, "hydrate --path")?;
    }
    let conn = open_db(paths)?;
    let lazy_root = args.view.join(".majutsu-lazy");
    if !lazy_root.exists() {
        bail!("lazy metadata not found: {}", lazy_root.display());
    }
    let requested_path = args.path.as_ref().map(|path| path_to_slash(path));
    let mut sidecars = Vec::new();
    for entry in WalkDir::new(&lazy_root).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() || entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let lazy: LazyMountEntry = serde_json::from_slice(&fs::read(entry.path())?)
            .with_context(|| format!("read lazy metadata {}", entry.path().display()))?;
        if args
            .root
            .as_deref()
            .is_some_and(|root| root != lazy.root_id)
        {
            continue;
        }
        if requested_path
            .as_deref()
            .is_some_and(|path| path != lazy.path)
        {
            continue;
        }
        sidecars.push((entry.path().to_path_buf(), lazy));
    }
    if sidecars.is_empty() {
        bail!("no lazy large files matched");
    }
    let mut hydrated = 0usize;
    for (sidecar, lazy) in sidecars {
        let manifest: LargeManifest =
            serde_json::from_slice(&read_object(paths, &lazy.manifest_key)?)
                .with_context(|| format!("read large manifest {}", lazy.manifest_key))?;
        let dest = args.view.join(&lazy.root_id).join(&lazy.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        write_large_chunks_atomic(paths, &dest, &manifest)?;
        fs::remove_file(sidecar)?;
        hydrated += 1;
    }
    record_op(
        &conn,
        "hydrate",
        None,
        None,
        Some(&format!("view {}", args.view.display())),
    )?;
    println!("hydrated_large_files {hydrated}");
    Ok(())
}
