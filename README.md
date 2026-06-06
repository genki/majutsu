# majutsu

`majutsu` is a host-level, multi-root snapshot history tool. The installed CLI
command is `mj`.

It is designed for the failure mode where a development host loses local data:
selected directories are snapshotted into a host-level state directory
(`$HOME/.majutsu` by default), with a jj-like operation log and Git LFS-like
large object handling.

## Current MVP

- Host-level state under `$MAJUTSU_HOME` or `$HOME/.majutsu`
- Multiple roots managed from one timeline
- SQLite metadata database
- Manual snapshots
- Content-addressed local object store
- Large file pointer manifests with fixed-size chunks
- Operation log
- Restore planning and restore to an alternate directory
- Basic object-store fsck

Remote S3 sync, daemon watching, encryption, lifecycle policy generation, and
archive restore are intentionally left for later iterations.

## Install

```sh
cargo install --path .
```

This installs `mj`.

## Quick Start

```sh
mj init
mj root add home-notes ~/notes --exclude '**/.git/**'
mj snapshot --message 'first snapshot'
mj log
mj restore plan --to /tmp/majutsu-restore
mj restore apply --to /tmp/majutsu-restore
```

The restore command writes files below `<target>/<root-id>/...` so the original
root is not overwritten accidentally.

## Large Files

Files are routed through the large object pipeline when they match configured
large extensions, exceed `large.min_size`, or are binary and exceed
`large.binary_min_size`. Large object manifests are stored under:

```text
$MAJUTSU_HOME/objects/large/manifests
$MAJUTSU_HOME/objects/large/chunks/fixed
```

Use:

```sh
mj large stat
mj large list
mj large verify
```

## Safety Notes

- Missing roots are skipped and logged as `root-missing`; they are not treated
  as mass deletion.
- Restore writes to an alternate directory only in this MVP.
- Database directories, VM images, and live application state still require an
  application-consistent dump or filesystem snapshot before being watched.
