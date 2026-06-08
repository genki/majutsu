use anyhow::{Context, Result, anyhow, bail};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use libc::{EIO, EISDIR, ENOENT, EROFS};
use majutsu_core::{FileRecord, LargeManifest, Payload, payload_blob_ref, payload_large_ref};
use rusqlite::Connection;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::Path;
use std::process::Command as ProcessCommand;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::Paths;
use crate::open_db;
use crate::operation_log::record_op;
use crate::read_blob_payload;
use crate::read_large_chunk;
use crate::read_object;
use crate::restore_runtime::RestorePlan;

const FUSE_TTL: Duration = Duration::from_secs(1);

#[derive(Clone)]
enum FuseNodeKind {
    Directory { children: BTreeMap<OsString, u64> },
    File { record: FileRecord },
    Symlink { target: String },
}

#[derive(Clone)]
struct FuseNode {
    parent: u64,
    attr: FileAttr,
    kind: FuseNodeKind,
}

pub(crate) struct MajutsuFuseFs {
    paths: Paths,
    nodes: BTreeMap<u64, FuseNode>,
}

impl MajutsuFuseFs {
    fn from_plan(paths: &Paths, plan: &RestorePlan) -> Result<Self> {
        let mut fs = Self {
            paths: Paths {
                home: paths.home.clone(),
                db: paths.db.clone(),
                config: paths.config.clone(),
                host: paths.host.clone(),
                objects: paths.objects.clone(),
                trees: paths.trees.clone(),
                large_chunks: paths.large_chunks.clone(),
                large_manifests: paths.large_manifests.clone(),
                packs: paths.packs.clone(),
                pack_indexes: paths.pack_indexes.clone(),
                logs: paths.logs.clone(),
                runtime: paths.runtime.clone(),
                daemon_pid: paths.daemon_pid.clone(),
                daemon_lock: paths.daemon_lock.clone(),
                snapshot_lock: paths.snapshot_lock.clone(),
                upload_queue: paths.upload_queue.clone(),
                event_queue: paths.event_queue.clone(),
                master_key: paths.master_key.clone(),
            },
            nodes: BTreeMap::new(),
        };
        fs.nodes.insert(
            1,
            FuseNode {
                parent: 1,
                attr: fuse_attr(1, FileType::Directory, 0, 0o755, None),
                kind: FuseNodeKind::Directory {
                    children: BTreeMap::new(),
                },
            },
        );
        for record in &plan.files {
            let parent = fs.ensure_dir_path(1, Path::new(&record.root_id))?;
            let rel = Path::new(&record.path);
            let file_parent = if let Some(parent_path) = rel.parent() {
                if parent_path.as_os_str().is_empty() {
                    parent
                } else {
                    fs.ensure_dir_path(parent, parent_path)?
                }
            } else {
                parent
            };
            let name = rel
                .file_name()
                .ok_or_else(|| anyhow!("invalid snapshot path: {}", record.path))?
                .to_os_string();
            let ino = fs.next_ino();
            let kind = match &record.payload {
                Payload::Directory => FuseNodeKind::Directory {
                    children: BTreeMap::new(),
                },
                Payload::Symlink { target } => FuseNodeKind::Symlink {
                    target: target.clone(),
                },
                Payload::Special { .. } => FuseNodeKind::File {
                    record: record.clone(),
                },
                _ => FuseNodeKind::File {
                    record: record.clone(),
                },
            };
            let file_type = fuse_record_file_type(record, &kind);
            fs.nodes.insert(
                ino,
                FuseNode {
                    parent: file_parent,
                    attr: fuse_attr(
                        ino,
                        file_type,
                        record.size,
                        fuse_file_perm(record.mode, file_type),
                        record.modified,
                    ),
                    kind,
                },
            );
            fs.add_child(file_parent, name, ino)?;
        }
        Ok(fs)
    }

    #[cfg(test)]
    pub(crate) fn for_test(paths: Paths) -> Self {
        Self {
            paths,
            nodes: BTreeMap::new(),
        }
    }

    fn next_ino(&self) -> u64 {
        self.nodes.keys().next_back().copied().unwrap_or(1) + 1
    }

    fn ensure_dir_path(&mut self, start: u64, path: &Path) -> Result<u64> {
        let mut current = start;
        for component in path.components() {
            let name = component.as_os_str().to_os_string();
            if name.is_empty() {
                continue;
            }
            let existing = self.nodes.get(&current).and_then(|node| match &node.kind {
                FuseNodeKind::Directory { children } => children.get(&name).copied(),
                _ => None,
            });
            if let Some(ino) = existing {
                current = ino;
                continue;
            }
            let ino = self.next_ino();
            self.nodes.insert(
                ino,
                FuseNode {
                    parent: current,
                    attr: fuse_attr(ino, FileType::Directory, 0, 0o755, None),
                    kind: FuseNodeKind::Directory {
                        children: BTreeMap::new(),
                    },
                },
            );
            self.add_child(current, name, ino)?;
            current = ino;
        }
        Ok(current)
    }

    fn add_child(&mut self, parent: u64, name: OsString, ino: u64) -> Result<()> {
        let node = self
            .nodes
            .get_mut(&parent)
            .ok_or_else(|| anyhow!("missing parent inode {parent}"))?;
        if let FuseNodeKind::Directory { children } = &mut node.kind {
            children.insert(name, ino);
            Ok(())
        } else {
            bail!("parent inode {parent} is not a directory")
        }
    }

    pub(crate) fn read_file(&self, record: &FileRecord, offset: i64, size: u32) -> Result<Vec<u8>> {
        if offset < 0 {
            return Ok(Vec::new());
        }
        let start = offset as u64;
        if start >= record.size {
            return Ok(Vec::new());
        }
        let end = (start + size as u64).min(record.size);
        if let Some((oid, object_key)) = payload_blob_ref(&record.payload) {
            let conn = open_db(&self.paths)?;
            let data = read_blob_payload(&self.paths, &conn, oid, object_key)?;
            Ok(data[start as usize..end as usize].to_vec())
        } else if let Some((_, manifest_key, _)) = payload_large_ref(&record.payload) {
            let manifest: LargeManifest =
                serde_json::from_slice(&read_object(&self.paths, manifest_key)?)?;
            let mut out = Vec::with_capacity((end - start) as usize);
            for chunk in manifest.chunks {
                let chunk_start = chunk.offset;
                let chunk_end = chunk.offset + chunk.len;
                if chunk_end <= start || chunk_start >= end {
                    continue;
                }
                let data = read_large_chunk(&self.paths, &chunk)?;
                let slice_start = start.saturating_sub(chunk_start) as usize;
                let slice_end = (end.min(chunk_end) - chunk_start) as usize;
                out.extend_from_slice(&data[slice_start..slice_end]);
            }
            Ok(out)
        } else {
            Ok(Vec::new())
        }
    }
}

impl Filesystem for MajutsuFuseFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(parent_node) = self.nodes.get(&parent) else {
            reply.error(ENOENT);
            return;
        };
        if let FuseNodeKind::Directory { children } = &parent_node.kind {
            if let Some(ino) = children.get(name).and_then(|ino| self.nodes.get(ino)) {
                reply.entry(&FUSE_TTL, &ino.attr, 0);
                return;
            }
        }
        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if let Some(node) = self.nodes.get(&ino) {
            reply.attr(&FUSE_TTL, &node.attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        reply.error(EROFS);
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            reply.error(EROFS);
            return;
        }
        match self.nodes.get(&ino).map(|node| &node.kind) {
            Some(FuseNodeKind::File { .. }) => reply.opened(0, 0),
            Some(FuseNodeKind::Directory { .. }) => reply.error(EISDIR),
            _ => reply.error(ENOENT),
        }
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn unlink(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(EROFS);
    }

    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(EROFS);
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _link_name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(EROFS);
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.nodes.get(&ino).map(|node| &node.kind) {
            Some(FuseNodeKind::File { record }) => match self.read_file(record, offset, size) {
                Ok(data) => reply.data(&data),
                Err(_) => reply.error(EIO),
            },
            Some(FuseNodeKind::Directory { .. }) => reply.error(EISDIR),
            _ => reply.error(ENOENT),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.nodes.get(&ino).map(|node| &node.kind) {
            Some(FuseNodeKind::Symlink { target }) => reply.data(target.as_bytes()),
            _ => reply.error(ENOENT),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let Some(node) = self.nodes.get(&ino) else {
            reply.error(ENOENT);
            return;
        };
        let FuseNodeKind::Directory { children } = &node.kind else {
            reply.error(ENOENT);
            return;
        };
        let mut entries = Vec::with_capacity(children.len() + 2);
        entries.push((ino, FileType::Directory, OsString::from(".")));
        entries.push((node.parent, FileType::Directory, OsString::from("..")));
        for (name, child_ino) in children {
            if let Some(child) = self.nodes.get(child_ino) {
                entries.push((*child_ino, child.attr.kind, name.clone()));
            }
        }
        for (i, (entry_ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry_ino, (i + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            reply.error(EROFS);
            return;
        }
        match self.nodes.get(&ino).map(|node| &node.kind) {
            Some(FuseNodeKind::Directory { .. }) => reply.opened(0, 0),
            _ => reply.error(ENOENT),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        reply.error(EROFS);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(EROFS);
    }

    fn removexattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(EROFS);
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, mask: i32, reply: ReplyEmpty) {
        match read_only_access(mask) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(EROFS);
    }

    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _length: i64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(EROFS);
    }

    fn copy_file_range(
        &mut self,
        _req: &Request<'_>,
        _ino_in: u64,
        _fh_in: u64,
        _offset_in: i64,
        _ino_out: u64,
        _fh_out: u64,
        _offset_out: i64,
        _len: u64,
        _flags: u32,
        reply: ReplyWrite,
    ) {
        reply.error(EROFS);
    }
}

pub(crate) fn mount_fuse_cmd(paths: &Paths, conn: &Connection, plan: &RestorePlan) -> Result<()> {
    let mountpoint = plan
        .to
        .as_ref()
        .ok_or_else(|| anyhow!("fuse mount requires a target directory"))?;
    prepare_mountpoint(mountpoint)?;
    let fs = MajutsuFuseFs::from_plan(paths, plan)?;
    record_op(
        conn,
        "mount-fuse",
        None,
        Some(&plan.snapshot.snapshot_id),
        Some(&format!("at {}", mountpoint.display())),
    )?;
    println!("mounted snapshot {}", plan.snapshot.snapshot_id);
    println!("target {}", mountpoint.display());
    println!("backend fuse");
    println!("files {}", plan.files.len());
    fuser::mount2(
        fs,
        mountpoint,
        &[
            MountOption::RO,
            MountOption::FSName("majutsu".into()),
            MountOption::Subtype("majutsu".into()),
            MountOption::DefaultPermissions,
        ],
    )
    .with_context(|| format!("mount fuse view {}", mountpoint.display()))?;
    Ok(())
}

fn fuse_attr(ino: u64, kind: FileType, size: u64, perm: u16, modified: Option<i64>) -> FileAttr {
    let time = modified
        .and_then(|seconds| u64::try_from(seconds).ok())
        .map(|seconds| UNIX_EPOCH + Duration::from_secs(seconds))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: time,
        mtime: time,
        ctime: time,
        crtime: time,
        kind,
        perm,
        nlink: if kind == FileType::Directory { 2 } else { 1 },
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

fn fuse_file_perm(mode: u32, kind: FileType) -> u16 {
    if kind == FileType::Symlink {
        return 0o777;
    }
    let perm = (mode & 0o777) as u16;
    if perm == 0 { 0o644 } else { perm }
}

fn fuse_record_file_type(record: &FileRecord, kind: &FuseNodeKind) -> FileType {
    match &record.payload {
        Payload::Special { special_kind } => match special_kind.as_str() {
            "fifo" => FileType::NamedPipe,
            "socket" => FileType::Socket,
            "block-device" => FileType::BlockDevice,
            "char-device" => FileType::CharDevice,
            _ => FileType::RegularFile,
        },
        _ => match kind {
            FuseNodeKind::Directory { .. } => FileType::Directory,
            FuseNodeKind::Symlink { .. } => FileType::Symlink,
            FuseNodeKind::File { .. } => FileType::RegularFile,
        },
    }
}

fn read_only_access(mask: i32) -> std::result::Result<(), i32> {
    if mask & libc::W_OK != 0 {
        Err(EROFS)
    } else {
        Ok(())
    }
}

pub(crate) fn prepare_mountpoint(mountpoint: &Path) -> Result<()> {
    if !mountpoint.exists() {
        fs::create_dir_all(mountpoint)?;
        return Ok(());
    }
    let meta = fs::symlink_metadata(mountpoint)?;
    if !meta.file_type().is_dir() {
        bail!("mountpoint is not a directory: {}", mountpoint.display());
    }
    if fs::read_dir(mountpoint)?.next().is_some() {
        bail!("mountpoint is not empty: {}", mountpoint.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_access_rejects_write_masks() {
        assert_eq!(read_only_access(libc::W_OK), Err(EROFS));
        assert_eq!(read_only_access(libc::R_OK | libc::W_OK), Err(EROFS));
        assert_eq!(read_only_access(libc::R_OK), Ok(()));
        assert_eq!(read_only_access(libc::R_OK | libc::X_OK), Ok(()));
    }
}

pub(crate) fn is_mountpoint(path: &Path) -> Result<bool> {
    let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let needle = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    Ok(mounts
        .lines()
        .any(|line| line.split_whitespace().nth(4).map(Path::new) == Some(needle.as_path())))
}

pub(crate) fn unmount_fuse(path: &Path) -> Result<()> {
    let status = ProcessCommand::new("fusermount3")
        .arg("-u")
        .arg(path)
        .status()
        .or_else(|_| {
            ProcessCommand::new("fusermount")
                .arg("-u")
                .arg(path)
                .status()
        })?;
    if !status.success() {
        bail!("failed to unmount {}", path.display());
    }
    Ok(())
}
