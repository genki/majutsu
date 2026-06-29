# Features

Majutsu is a host-level snapshot history agent for protecting authored local
state across multiple roots. It complements Git/Jujutsu rather than replacing
them: Git records repository commits, while Majutsu protects the working host,
including uncommitted files and selected non-repository directories.

## Core capabilities

- Host-level state under CLI `--home`, `$MAJUTSU_HOME`, XDG config, or
  `$HOME/.majutsu`.
- Separate `mj --system` state for root-owned host configuration under
  `/etc/majutsu/config.toml` or `/var/lib/majutsu`.
- Multiple roots managed from one timeline.
- SQLite metadata database and content-addressed object store.
- Append-only local operation log under `ops/local-oplog.cborl`.
- Manual snapshots and daemon/watch-created snapshots.
- Snapshot diff, state inspection, operation show/diff/restore, and branch
  switching.
- Bootstrap clone from remote metadata.
- Restore planning, restore apply, restore queue prepare/resume, and restore
  views.
- Materialized restore views on all supported platforms and kernel-backed FUSE
  views on Linux.
- Human-oriented `status`, `health`, `root list`, `root size`, and daemon
  output.

## Large and efficient storage

- Large file pointer manifests with fixed-size or content-defined chunks.
- zstd compression policy for large chunks.
- Medium-size chunked blobs for repeated edits without full re-upload.
- Normal blob pack files and pack indexes.
- Large object pin/unpin metadata for lifecycle policy inputs.
- Safe `prune --dry-run`, metadata prune, local `gc`, and remote cleanup.

## Remote protection

- File remotes for local validation.
- S3-compatible remote backend with Signature V4, range GET, conditional PUT,
  and multipart upload.
- Canonical host-scoped remote metadata layout for multi-host buckets.
- Remote fsck for metadata and object reachability.
- Lifecycle policy generation for GCS and S3.
- Persistent upload queue with retry/backoff on later `mj sync`.

## Security and recovery

- Optional age object encryption with legacy ChaCha20-Poly1305 compatibility.
- Master key export/import for remote disaster recovery.
- HMAC-derived encrypted object keys so plaintext content hashes are not exposed
  in remote paths.
- Clone-time quarantine for remote hook/plugin commands unless
  `--trust-remote-hooks` is explicitly used.
- Bounded zstd expansion for remote metadata/head objects.
- Symlink and reparse-point hardening for snapshot and restore path boundaries.

## Watch and daemon

- OS-native filesystem watch backend with debounce.
- Linux defaults to fanotify and falls back to inotify when fanotify is
  unavailable; `notify` is the cross-platform native watcher alias.
- Polling watch fallback for environments where native events are unavailable.
- Watch-created snapshots can auto-sync after each batch.
- Daemon status and metrics expose root health, upload backlog, journal replay,
  and restore queue state.
- systemd user/system service rendering and launchd plist rendering.

## Platform support

Majutsu supports Linux, macOS, and Windows for snapshot, history, remote sync,
clone, and materialized restore workflows. Linux additionally supports kernel
FUSE restore views. Windows uses native NT extended attributes and an
authenticated loopback daemon endpoint.

See [Platform Support](PLATFORM_SUPPORT.md) for details.

## Default root policy

New roots apply best-practice excludes by default for reproducible or generated
subtrees such as VCS internals (`.git`, `.jj`, `.hg`, `.svn`), dependency
directories (`node_modules`, virtualenvs), build outputs (`target`, `build`,
`dist`, `out`), and common caches.

This keeps root addition aligned with the host-loss recovery goal: protect
authored data and avoid filling backend history with data that can be
regenerated.

After the initial root scan, majutsu is conservative about unknown additions.
Small authored files can still be picked up by the realtime path, but unknown
large files and large batches of new files remain untracked until the operator
opts in with `mj track`. Use `mj state -U` inside a root, or
`mj state -r <root> -U`, to review those untracked files as `?` rows. This
avoids accidentally backing up build trees, archives, generated datasets, or
mass-copy mistakes just because they briefly appeared under a managed root.

If a root really must capture everything, opt out explicitly:

```sh
mj root add full-image /path/to/root --no-default-excludes
```

Sensitive authored files such as `.env` or kubeconfig files are not silently
excluded by default. Use encrypted state/remotes or explicit excludes when those
files should not be backed up.

For high-frequency generated state such as database WAL files, use a volatile
policy. `checkpoint` mode keeps the latest value in checkpoints without letting
every write trigger a watch snapshot, and `exclude` mode removes matching paths
from managed history.
