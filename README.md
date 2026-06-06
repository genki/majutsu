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
- Remote sync of metadata and objects
- S3-compatible remote backend
- Bootstrap clone from remote metadata
- Root pause/resume/missing-state handling
- Foreground periodic watch and minimal daemon start/stop/status
- Restore planning and restore to an alternate directory
- Basic object-store fsck

OS-native file event journaling, encryption, lifecycle policy generation,
archive restore, and pack compaction are intentionally left for later
iterations.

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
mj sync
mj sync status
mj remote fsck
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

## Remote Sync

`mj sync` writes hot metadata and all referenced local objects to the configured
remote:

```text
metadata/export.json
config.toml
host.toml
hosts/current
objects/...
```

This is the critical path for host-disk-loss recovery: a fresh state directory
can be reconstructed from remote metadata.

For S3-compatible storage:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT_URL=https://storage.googleapis.com

mj init --remote s3://bucket/prefix
mj root add sample /path/to/sample
mj snapshot --message 'first remote snapshot'
mj sync
mj remote check
mj remote fsck
```

To rebuild an empty state directory from remote:

```sh
mj --home /tmp/recovered-majutsu clone --remote s3://bucket/prefix
mj --home /tmp/recovered-majutsu fsck
mj --home /tmp/recovered-majutsu restore apply --to /tmp/restore
```

The current S3 backend uses path-style requests and AWS Signature V2, which is
compatible with Google Cloud Storage HMAC keys in the validation environment.

## Watch And Daemon

Foreground periodic snapshots:

```sh
mj watch --foreground --interval-secs 60
```

One-shot watch, useful for tests:

```sh
mj watch --once --interval-secs 1
```

Minimal background daemon management:

```sh
mj daemon start --interval-secs 60
mj daemon status
mj daemon stop
```

The current daemon is a small process wrapper around foreground watch. It does
not yet use OS-native file event APIs or a persistent event journal.

## Root State

Roots can be paused and resumed:

```sh
mj root pause home-notes
mj root resume home-notes
```

If an active root path disappears, `mj snapshot` records a `root-missing`
operation and marks the root `missing`; it does not snapshot the root as empty.
Use `mj root resume <id>` after the path is available again.

## Safety Notes

- Missing roots are skipped and logged as `root-missing`; they are not treated
  as mass deletion.
- Restore writes to an alternate directory only in this MVP.
- Database directories, VM images, and live application state still require an
  application-consistent dump or filesystem snapshot before being watched.
