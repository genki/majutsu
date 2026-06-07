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
- Append-only local operation log under `ops/local-oplog.cborl`
- Manual snapshots
- Content-addressed local object store
- Large file pointer manifests with fixed-size or content-defined chunks
- Large chunk zstd compression policy
- Operation log
- Operation show and current-ref restore
- Remote sync of metadata and objects
- S3-compatible remote backend with Signature V4, range GET, and multipart upload
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
- Materialized and kernel-backed FUSE mount restore views
- Lifecycle policy generation for GCS and S3
- Large object pin/unpin metadata
- Basic object-store fsck

FUSE mounts run in the foreground and hydrate large-file chunks on read.

## Install

```sh
cargo install --path .
```

This installs `mj`.

## Workspace

The repository is a Cargo workspace. The current `mj` binary remains in the
root package while domain boundaries are represented by these crates:

```text
crates/majutsu-cli
crates/majutsu-daemon
crates/majutsu-core
crates/majutsu-db
crates/majutsu-watch
crates/majutsu-store
crates/majutsu-large
crates/majutsu-pack
crates/majutsu-crypto
crates/majutsu-restore
crates/majutsu-policy
```

They provide the stable model, policy, and trait surfaces used as extraction
targets while the production CLI continues to preserve compatibility.

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
mj restore --at "2026-06-06 10:30:00" --root home-notes --to /tmp/majutsu-restore
mj restore plan --to /tmp/majutsu-restore
mj restore apply --to /tmp/majutsu-restore
```

`mj restore` without a subcommand is an alias for `mj restore apply`, matching
the direct restore form used in the spec. The restore command writes files
below `<target>/<root-id>/...` so the original root is not overwritten
accidentally when `--to` is provided. If `--to` is
omitted, restore writes back to the configured original root path for the
selected root. `restore plan` reports existing destination conflicts, and
`restore apply` refuses to overwrite conflicting files unless `--force` is
provided. Files that exist in the restore target but not in the selected
snapshot are reported as deletes; apply requires `--force` before deleting
those extra files. Plans also include `restore_files`, `modify_files`,
`keep_files`, and `delete_files` counts after comparing the selected snapshot
with the target.
Plans also summarize the object set needed for restore, including large-file
chunk count, local availability, remote availability, and objects that are
missing or likely need archive hydration.
Time arguments such as `--at` accept RFC3339 timestamps, `YYYY-MM-DD HH:MM:SS`
as UTC, `YYYY-MM-DD` as midnight UTC, `now`, and relative values such as
`10 minutes ago`.

## Large Files

Files are routed through the large object pipeline when they match configured
large extensions, exceed `large.min_size`, or are binary and exceed
`large.binary_min_size`. Large object manifests are stored under:

```text
$MAJUTSU_HOME/objects/large/manifests
$MAJUTSU_HOME/objects/large/chunks/fixed
$MAJUTSU_HOME/objects/large/chunks/fastcdc
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
Without `--since`, `large pin` considers the current snapshot. With `--since`,
it considers snapshots at or after the cutoff, accepting duration values such as
`30d`, `12h`, `10m`, and `30s`, or an RFC3339 timestamp.

Large chunks are compressed with zstd when compression is enabled, the extension
is not in the skip list, and the compressed chunk beats `min_gain_ratio`.
Compression metadata is stored per chunk in the large manifest so restore and
fsck can verify the original plaintext chunk identity.

```toml
[large.compression]
enabled = true
algorithm = "zstd"
level = 3
sample_bytes = "1 MiB"
min_gain_ratio = 0.05
skip_extensions = ["*.jpg", "*.png", "*.mp4", "*.zip", "*.zst", "*.gz"]
```

Chunking defaults to fixed-size chunks. Set `large.default_chunking` to
`fastcdc` to use content-defined chunk boundaries around `large.chunk_size`:

```toml
[large]
default_chunking = "fastcdc"
target_chunk_size = "8 MiB"
```

Large size settings accept either byte integers or strings such as `"64 MiB"`,
`"16 MiB"`, and `"8 MiB"`.

Root-specific large-file policy can override the global thresholds and
patterns:

```sh
mj root add photos /mnt/photos \
  --large-min-size 8388608 \
  --large-always '*.raw' \
  --large-never '*.json'
```

Existing root settings can be changed without removing the root:

```sh
mj root set photos --exclude '**/.cache/**'
mj root set photos --clear-exclude --exclude '**/.DS_Store'
mj root set photos --large-min-size 8388608 --large-always '*.heic'
mj root set app-data --snapshot-mode transactional \
  --pre-snapshot '/usr/local/bin/app-checkpoint begin' \
  --post-snapshot '/usr/local/bin/app-checkpoint end'
```

`root add` refuses an existing root id so root status and policy history are not
silently overwritten; use `root set` for intentional changes.
`root set` records a `config-change` operation so root policy changes are
visible in the operation log.

## Remote Sync

`mj sync` writes hot metadata and all referenced local objects to the configured
remote:

```text
metadata/export.json
hosts/index.json
hosts/<host-id>/metadata/export.json
hosts/<host-id>/snapshots/<snapshot-id>.json
hosts/<host-id>/ops/<op-id>.json
config.toml
host.toml
hosts/current
hosts/<host-id>/current
hosts/<host-id>/refs/current
hosts/<host-id>/refs/last-synced
objects/trees/...
objects/...
trees/...
blobs/loose/...
packs/normal/...
indexes/pack-index/...
large/manifests/...
large/chunks/fixed-8m/...
large/chunks/fastcdc/...
```

This is the critical path for host-disk-loss recovery: a fresh state directory
can be reconstructed from remote metadata.
Metadata keeps the local object keys for backward-compatible restore and clone,
while `mj sync` also writes canonical remote-layout aliases matching the spec's
`trees/`, `blobs/loose/`, `packs/`, `indexes/`, and `large/` prefixes. `mj
remote fsck` accepts canonical-only payload storage and also validates legacy
compatibility keys when they are present.

For S3-compatible storage:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT_URL=https://storage.googleapis.com
export AWS_SIGNATURE_VERSION=s3v4
export MAJUTSU_S3_MULTIPART_THRESHOLD=$((64 * 1024 * 1024))

mj init --remote s3://bucket/prefix
mj root add sample /path/to/sample
mj snapshot --message 'first remote snapshot'
mj sync
mj remote check
mj remote capabilities
mj remote hosts
mj remote host test-host
mj remote fsck
```

`[remote]` also accepts the split config form used in the spec:

```toml
[remote]
type = "s3"
bucket = "my-majutsu-backup"
prefix = "majutsu/v1/workstation"
endpoint = "https://storage.googleapis.com"
region = "us-east-1"
signature_version = "s3v4"
```

For local validation:

```toml
[remote]
type = "file"
path = "/tmp/majutsu-remote"
```

`mj remote fsck` verifies canonical `hosts/index.json`, each host metadata
export, canonical `hosts/<host>/refs/*` values, canonical per-host
snapshot/operation exports, and every referenced object through either its
canonical remote key or its legacy compatibility key. Legacy bootstrap metadata
and JSON per-host exports are checked when present.

To rebuild an empty state directory from remote:

```sh
mj --home /tmp/recovered-majutsu clone --remote s3://bucket/prefix
mj --home /tmp/recovered-other clone --remote s3://bucket/prefix --host test-host
mj --home /tmp/recovered-majutsu fsck
mj --home /tmp/recovered-majutsu restore apply --to /tmp/restore
```

When `hosts/index.json` contains multiple hosts, clone requires `--host` even
if the legacy `metadata/export.json` bootstrap file is still present.
`metadata/export.json` remains the legacy/current-host bootstrap path.
`hosts/index.json` and `hosts/<host-id>/metadata/export.json` allow browsing and
recovering a specific host timeline from a shared remote prefix.
After local prune removes snapshot metadata, the next `mj sync` removes stale
per-host snapshot/operation export JSON files for the current host. It does not
delete referenced object payloads; lifecycle tiering remains the storage
provider's job.

The S3 backend uses path-style requests. AWS Signature V4 is the default; set
`AWS_SIGNATURE_VERSION=s3v2` only for legacy S3-compatible services that still
require the older signature style. `mj remote check` also verifies a small
range GET against remote metadata. Signature V4 uploads at or above
`MAJUTSU_S3_MULTIPART_THRESHOLD` use S3 multipart upload; the minimum effective
threshold is 5 MiB because S3 requires non-final parts to be at least that size.
For providers that support them, `MAJUTSU_S3_STORAGE_CLASS` adds
`x-amz-storage-class` on object creation and `MAJUTSU_S3_OBJECT_TAGS` adds
`x-amz-tagging` in `key=value&key=value` form. Majutsu also tags configured S3
uploads with `majutsu-class` so lifecycle policies can distinguish metadata,
refs, trees, packs, large chunks, and generic objects. Leave these variables
unset for S3-compatible providers that reject S3 storage-class or object-tagging
headers.
File remotes and S3 Signature V4 remotes support conditional put for queued CAS
objects. S3 uses `If-None-Match: *` for regular PutObject requests; multipart
uploads fall back to a preflight existence check because S3 multipart completion
does not provide the same simple create-only PutObject primitive.

## Encryption

New state can encrypt local and remote object payloads:

```sh
mj init --encrypt --remote s3://bucket/prefix
```

Encrypted objects are written with a `MJENC1` header and ChaCha20-Poly1305
ciphertext. `mj init --encrypt` writes `[security] encryption = "age"` in
`config.toml`; the older `chacha20poly1305` value is still accepted for existing
state. For encrypted state, content object paths are derived with HMAC-SHA256
from the master key and the internal content id, so remote object keys do not
expose raw plaintext hashes. Existing plaintext objects remain readable for
compatibility.

The master key is stored locally at:

```text
$MAJUTSU_HOME/keys/master.key
```

Export it and store it separately from the host:

```sh
mj key export
```

Rotate the master key and rewrite encrypted object metadata:

```sh
mj key rotate
mj key rotate --new-key <64-hex-key>
mj sync
```

Rotation currently supports unpacked encrypted objects. Run it before packing
normal blobs.

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

Stable reads compare file size, modified time, and Unix inode where available
before and after reading. If a file changes or is replaced during the read,
majutsu retries before storing it.

Transactional mode runs hooks before and after scanning the root. This is the
intended path for application state, database dumps, VM images, and other data
that needs an application-consistent checkpoint:

```sh
mj root add app-data /srv/app/data \
  --snapshot-mode transactional \
  --pre-snapshot '/usr/local/bin/app-checkpoint begin' \
  --post-snapshot '/usr/local/bin/app-checkpoint end'
```

If the pre-hook creates a filesystem snapshot or dump directory, keep the root
path as the restore destination identity and read from that snapshot source:

```sh
mj root add app-data /srv/app/data \
  --snapshot-mode transactional \
  --snapshot-source /mnt/app-data-snapshot \
  --pre-snapshot '/usr/local/bin/create-app-snapshot /mnt/app-data-snapshot' \
  --post-snapshot '/usr/local/bin/remove-app-snapshot /mnt/app-data-snapshot'
```

If a hook exits non-zero, the snapshot fails. Hook execution is recorded in the
event journal.

Transactional roots can also run an application plugin command for both phases:

```sh
mj root add sqlite-app /srv/app/data \
  --snapshot-mode transactional \
  --snapshot-source /tmp/sqlite-app-checkpoint \
  --application-plugin '/usr/local/lib/majutsu/plugins/sqlite-checkpoint'
```

Majutsu runs the plugin with `MAJUTSU_PLUGIN_PHASE=pre` before scanning and
`MAJUTSU_PLUGIN_PHASE=post` after the post hook. It also provides
`MAJUTSU_HOME`, `MAJUTSU_ROOT_ID`, `MAJUTSU_ROOT_NAME`, `MAJUTSU_ROOT_PATH`, and
`MAJUTSU_SNAPSHOT_SOURCE` when configured. A non-zero plugin exit fails the
snapshot, and each phase is recorded in the event journal.

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
mj diff --at "10 minutes ago"
```

Snapshot manifests keep a compatibility file list and also point each root at a
separate tree manifest object under `objects/trees/`.

## Watch And Daemon

Foreground OS-native filesystem watching. On Linux, `inotify` is the default:

```sh
mj watch --foreground --debounce-ms 1500 --settle-ms 500 --periodic-rescan-secs 3600
```

`--backend inotify` can be specified explicitly on Linux. `--backend notify` is
kept as the cross-platform native watcher alias.

Polling fallback:

```sh
mj watch --foreground --backend poll --interval-secs 60
```

One-shot notify watch, useful for tests:

```sh
mj watch --once --backend notify --debounce-ms 100
```

Daemonized watch can also be started through `watch`:

```sh
mj watch --foreground=false --interval-secs 60
```

Minimal background daemon management:

```sh
mj daemon start --interval-secs 60
mj daemon status
mj daemon stop
```

The daemon is a process wrapper around foreground watch. It uses the native
watch backend by default (`inotify` on Linux), records filesystem events in the
event journal, and exposes a Unix socket at `$MAJUTSU_HOME/runtime/daemon.sock`
for status IPC.
When timing flags are omitted, watch and daemon start use `[watch]` from
`config.toml`:

```toml
[watch]
mode = "default"
debounce = "1500ms"
settle = "500ms"
periodic_rescan = "1h"
interval = "60s"
```

Notify mode debounces event bursts, then waits for the configured settle window
before snapshotting. New events during the settle window restart the debounce
and settle cycle.
The native backend remains the primary change detector. Periodic rescan is only
a low-frequency safety net for missed events or long idle periods; set
`--periodic-rescan-secs 0` to disable it.

## Root State

Roots can be paused and resumed:

```sh
mj root pause home-notes
mj root resume home-notes
```

If an active root path disappears, `mj snapshot` records a `root-missing`
operation and marks the root `missing`; it does not snapshot the root as empty.
If a root cannot be scanned because access is denied, it records
`root-permission-denied` and marks the root `permission-denied` instead of
turning unreadable files into deletions. Use `mj root resume <id>` after the
path is available again.

For roots that must be backed by a mounted filesystem, use `--require-mount`:

```sh
mj root add photos /mnt/photos --require-mount
```

If that path exists but is not a mount point, `mj snapshot` records
`root-unmounted`, marks the root `unmounted`, and skips it instead of recording
mass deletion.

Symlinks are stored as symlink entries by default. Use `--follow-symlinks` when
the root should snapshot the linked file contents instead.

## Prune And GC

Prune plans or deletes snapshots according to daily/monthly retention buckets:

```sh
mj prune --dry-run --keep-daily 90 --keep-monthly 36
mj prune --dry-run=false --keep-daily 90 --keep-monthly 36
```

The current snapshot is always kept. Non-dry-run prune removes unkept snapshot
metadata and drops blob/large/chunk metadata no longer referenced by remaining
snapshots.

`mj gc` removes unreferenced local loose objects under `$MAJUTSU_HOME/objects`.
It does not delete referenced history or remote objects.

Generate provider lifecycle policy templates:

```sh
mj lifecycle policy --provider gcs
mj lifecycle policy --provider s3
```

The generated rules are derived from `[tiering]` in `config.toml`. Rules without
`after`, or rules whose storage is `standard`, are treated as keep-hot policy
inputs and are not emitted as provider transitions. Transition rules map
portable storage names such as `infrequent`, `archive`, and `deep-archive` to
provider-specific storage classes.

```toml
[tiering]
enabled = true

[[tiering.rules]]
name = "keep-host-metadata-hot"
prefix = "hosts/"
storage = "standard"

[[tiering.rules]]
name = "packs-to-ia"
prefix = "packs/normal/"
after = "30d"
transition_to = "infrequent"

[[tiering.rules]]
name = "fixed-large-chunks-to-archive"
prefix = "large/chunks/fixed-8m/"
after = "180d"
storage = "archive"

[[tiering.rules]]
name = "fastcdc-large-chunks-to-archive"
prefix = "large/chunks/fastcdc/"
after = "180d"
storage = "archive"
```

## Restore Jobs

Prepare records the object set needed for a restore:

```sh
mj restore prepare --snapshot snap-id --to /tmp/restore
mj restore resume restore-job-id
```

Prepared jobs are stored under `$MAJUTSU_HOME/queue/restores`. Resume applies
the prepared snapshot and target once no required objects are pending archive
hydration. Completed jobs are marked `done` and cannot be resumed again.

Restored regular and large files preserve stored Unix mode bits, extended
attributes, and modified time. Symlink metadata is left unchanged so restore
does not mutate the link target.

If `restore prepare` finds required objects missing from local state, it checks
the configured remote. Objects that exist remotely are tracked as
`archived_objects` and receive provider-side archive restore requests; objects
missing both locally and remotely are tracked as `missing_objects` and block
`restore resume` until the data is repaired. S3 remotes use `POST ?restore`
with a 7-day Standard restore request; file remotes record the request as a
no-op for local validation. On resume, majutsu tries to download pending remote
objects into the local object store before applying the prepared restore.

## Mount Views

Create a read-only restore view without overwriting the original roots:

```sh
mj mount --at 2026-06-06T10:30:00Z /tmp/majutsu-view
mj hydrate /tmp/majutsu-view --root sample --path large.bin
mj unmount /tmp/majutsu-view
mj mount --hydrate-large /tmp/majutsu-view-full
mj mount --backend fuse --at 2026-06-06T10:30:00Z /tmp/majutsu-fuse
```

The default `materialized` backend writes a restore view directory. Without
`--hydrate-large`, normal files are materialized and large files are represented
by sparse placeholders plus metadata under `.majutsu-lazy/`.
Use `mj hydrate` to assemble selected lazy large files into an existing view.
With `--hydrate-large`, large files are fully assembled while creating the view.
`mj unmount` removes views that contain a majutsu mount marker.

The `fuse` backend is a kernel-backed read-only mount. It serves normal files
from the object store and reads only the large-file chunks overlapping each read
request. The mount command stays in the foreground; unmount it from another
terminal with `mj unmount /tmp/majutsu-fuse`.

## Packs

Pack normal blob objects to reduce the number of loose object files:

```sh
mj pack
mj pack --compact
mj gc
```

Pack target sizes are configurable:

```toml
[pack]
small_pack_target = "64 MiB"
normal_pack_target = "256 MiB"
```

`mj pack` stores unpacked normal blobs under
`$MAJUTSU_HOME/objects/packs/normal/*.mpack` and writes matching pack indexes
under `$MAJUTSU_HOME/objects/indexes/pack/*.json`. After packing, the original
loose blob objects are no longer referenced by metadata, so `mj gc` can remove
them locally.

`mj pack --compact` rewrites currently referenced packed blobs into one fresh
pack/index pair and drops older pack metadata, allowing `mj gc` to remove stale
pack files.

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
debounced snapshots are created. On watch startup, majutsu scans the journal for
filesystem events newer than the last `snapshot-finish` record and creates a
replay snapshot before waiting for new events.

## Safety Notes

- Missing roots are skipped and logged as `root-missing`; they are not treated
  as mass deletion.
- Prefer `restore plan` before writing back to original roots.
- Database directories, VM images, and live application state still require an
  application-consistent dump or filesystem snapshot before being watched.
