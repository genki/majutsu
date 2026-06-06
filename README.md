# majutsu

`majutsu` is a host-level, multi-root snapshot history tool. The installed CLI
command is `mj`.

It is designed for the failure mode where a development host loses local data:
selected directories are snapshotted into a host-level state directory
(`$HOME/.majutsu` by default), with a jj-like operation log and Git LFS-like
large object handling.

## Current MVP

- Host-level state under CLI `--home`, `$MAJUTSU_HOME`, XDG config, or
  `$HOME/.majutsu`
- Multiple roots managed from one timeline
- SQLite metadata database
- Manual snapshots
- Content-addressed local object store
- Large file pointer manifests with fixed-size chunks
- Operation log
- Operation show and current-ref restore
- Remote sync of metadata and objects
- S3-compatible remote backend
- Bootstrap clone from remote metadata
- Root tree manifests stored as separate content-addressed objects
- Normal blob pack files and pack indexes
- Snapshot diff
- Safe `prune --dry-run` and local loose-object `gc`
- Persistent upload queue with retry on the next `mj sync`
- Event journal records for snapshot/watch/root availability observations
- Optional ChaCha20-Poly1305 object encryption
- Master key export/import for remote disaster recovery
- Root snapshot modes: `default`, `strict`, and `transactional`
- Transactional pre/post snapshot hooks
- Stable read retry/backoff
- Root pause/resume/missing-state handling
- OS-native filesystem watch backend with debounce
- Polling watch fallback and minimal daemon start/stop/status
- Restore planning and restore to an alternate directory
- Restore prepare/resume queue
- Lifecycle policy generation for GCS and S3
- Large object pin/unpin metadata
- Basic object-store fsck

Provider-side archive restore requests, range reads, multipart upload, and
advanced pack compaction are intentionally left for later iterations.

## Install

```sh
cargo install --path .
```

This installs `mj`.

## State Home

State home resolution order:

```text
1. mj --home /path/to/state
2. MAJUTSU_HOME=/path/to/state
3. $XDG_CONFIG_HOME/majutsu/config.toml with [state].home
4. $HOME/.majutsu
```

Example XDG config:

```toml
[state]
home = "/var/lib/majutsu"
```

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
mj large pin --root photos --since 30d
mj large unpin --older-than 180d
```

Pins are stored in metadata and preserved through sync/clone. They are intended
as lifecycle policy inputs for large objects that should remain hot.

## Remote Sync

`mj sync` writes hot metadata and all referenced local objects to the configured
remote:

```text
metadata/export.json
config.toml
host.toml
hosts/current
objects/trees/...
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

## Encryption

New state can encrypt local and remote object payloads:

```sh
mj init --encrypt --remote s3://bucket/prefix
```

Encrypted objects are written with a `MJENC1` header and ChaCha20-Poly1305
ciphertext. Existing plaintext objects remain readable for compatibility.

The master key is stored locally at:

```text
$MAJUTSU_HOME/keys/master.key
```

Export it and store it separately from the host:

```sh
mj key export
```

To recover from remote storage into a fresh state, provide the key with an
environment variable or import it first:

```sh
MAJUTSU_MASTER_KEY=<64-hex-key> mj --home /tmp/recovered clone --remote s3://bucket/prefix

mj --home /tmp/recovered key import <64-hex-key>
```

Without the master key, encrypted objects can be downloaded but cannot be
verified or restored.

## Snapshot Modes

Roots default to coalesced snapshot behavior:

```sh
mj root add docs ~/Documents --snapshot-mode default
```

Strict mode retries stable reads more aggressively:

```sh
mj root add config /etc/myapp --snapshot-mode strict
```

Transactional mode runs hooks before and after scanning the root. This is the
intended path for application state, database dumps, VM images, and other data
that needs an application-consistent checkpoint:

```sh
mj root add app-data /srv/app/data \
  --snapshot-mode transactional \
  --pre-snapshot '/usr/local/bin/app-checkpoint begin' \
  --post-snapshot '/usr/local/bin/app-checkpoint end'
```

If a hook exits non-zero, the snapshot fails. Hook execution is recorded in the
event journal.

## History And Diff

Show recent operations:

```sh
mj log
mj log --root home-notes
mj op log
mj op show <op-id>
mj op restore <op-id>
```

Diff the current snapshot against its parent:

```sh
mj diff
```

Diff explicit snapshots:

```sh
mj diff snap-old snap-new --root home-notes
```

Snapshot manifests keep a compatibility file list and also point each root at a
separate tree manifest object under `objects/trees/`.

## Watch And Daemon

Foreground OS-native filesystem watching:

```sh
mj watch --foreground --backend notify --debounce-ms 1500
```

Polling fallback:

```sh
mj watch --foreground --backend poll --interval-secs 60
```

One-shot notify watch, useful for tests:

```sh
mj watch --once --backend notify --debounce-ms 100
```

Minimal background daemon management:

```sh
mj daemon start --interval-secs 60
mj daemon status
mj daemon stop
```

The current daemon is a small process wrapper around foreground watch. It does
use the notify backend and records filesystem events in the event journal.

## Root State

Roots can be paused and resumed:

```sh
mj root pause home-notes
mj root resume home-notes
```

If an active root path disappears, `mj snapshot` records a `root-missing`
operation and marks the root `missing`; it does not snapshot the root as empty.
Use `mj root resume <id>` after the path is available again.

## Prune And GC

Prune is currently a safe planning entry point:

```sh
mj prune --dry-run --keep-daily 90 --keep-monthly 36
```

`mj gc` removes unreferenced local loose objects under `$MAJUTSU_HOME/objects`.
It does not delete referenced history or remote objects.

Generate provider lifecycle policy templates:

```sh
mj lifecycle policy --provider gcs
mj lifecycle policy --provider s3
```

The generated rules keep metadata hot while transitioning packs and large
chunks by prefix.

## Restore Jobs

Prepare records the object set needed for a restore:

```sh
mj restore prepare --snapshot snap-id --to /tmp/restore
mj restore resume restore-job-id
```

Prepared jobs are stored under `$MAJUTSU_HOME/queue/restores`. Resume applies
the prepared snapshot and target once no required objects are pending archive
hydration.

## Packs

Pack normal blob objects to reduce the number of loose object files:

```sh
mj pack
mj gc
```

`mj pack` stores unpacked normal blobs under
`$MAJUTSU_HOME/objects/packs/normal/*.mpack` and writes matching pack indexes
under `$MAJUTSU_HOME/objects/indexes/pack/*.json`. After packing, the original
loose blob objects are no longer referenced by metadata, so `mj gc` can remove
them locally.

Restore, fsck, sync, remote fsck, and clone understand pack indexes. Large
chunk objects remain separate content-addressed objects.

## Queues

`mj sync` first writes upload tasks under:

```text
$MAJUTSU_HOME/queue/uploads
```

Each successful remote write removes its queue item. If an upload fails, the
item remains with an incremented attempt count and the next `mj sync` retries
it.

Snapshot and watch observations are recorded under:

```text
$MAJUTSU_HOME/queue/events
```

This is the initial event journal used to preserve observed work across process
crashes. The notify watch backend records filesystem events here before
debounced snapshots are created.

## Safety Notes

- Missing roots are skipped and logged as `root-missing`; they are not treated
  as mass deletion.
- Restore writes to an alternate directory only in this MVP.
- Database directories, VM images, and live application state still require an
  application-consistent dump or filesystem snapshot before being watched.
