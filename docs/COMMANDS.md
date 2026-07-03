# Command Guide

This page summarizes the daily CLI surface. Run `mj --help` and
`mj help <command>` for the exact option list.

## Command layout

The stable top-level commands are grouped by purpose in `mj --help`;
maintenance commands remain top-level for Git-style discoverability.

```text
Setup: init, root
Daily use: status, health, state, log, diff, snapshot, commit, note, track, untrack
History: branch, switch, op
Recovery: restore, restore mount, restore unmount, restore hydrate, mount, unmount, hydrate, clone
Remote: sync, remote, lifecycle
Service: watch, daemon
Security: key
Storage maintenance: large, cache, pack, prune, gc, fsck
Advanced/debug: version, event
```

`mj commit` is a visible alias for `mj snapshot`, intended for users familiar
with Git/Jujutsu terminology. The canonical term remains `snapshot` because
Majutsu first preserves changes through the daemon/event journal and remote
sync; snapshots are durable timeline checkpoints and compaction boundaries.

`mj switch` is a top-level alias for `mj branch switch`.

`mj version` prints the installed binary identity, target platform, build
number, git commit when available, and a capability list. Use it instead of
`mj --version` when comparing installs across Linux, macOS, and Windows:

```sh
mj version
mj version --json
```

## State and diff inspection

`mj state` prints a Git `status -s`-style view of managed file changes.
Without a reference it treats the reference as infinite past and shows the
lifecycle of every tracked path since the root was registered: live tracked
paths appear as `A`, deleted tracked paths appear as `D`, and explicitly
untracked paths are hidden. Passing a reference compares that snapshot,
operation id, absolute time, or relative time with the live filesystem.
Rows are ordered by newest tracked operation time first. When `-U/--untrack`
is enabled, `?` rows are treated as the newest rows and appear at the top.

```sh
mj state
mj state 1d
mj state 1d -r home-notes
mj state 03:40 -r home-notes -d
mj state op-123456789abc -g
mj state -D
mj state --status A,M
mj state -r home-notes -U --status '?'
mj state -r home-notes -- src README.md
mj track path/to/file
mj untrack path/to/file
```

Markers:

```text
A  added file
M  content or restore-significant change
D  deleted file
m  metadata-only change, shown only with --meta
?  untracked live file, shown only with -U/--untrack
```

Metadata-only changes such as directory mtime updates are hidden by default so
file additions do not also produce noisy parent-directory rows. Use `--meta`
when mode, owner, xattrs, or directory mtime changes matter:

```sh
mj state 1d -r home-notes --meta
```

`-d/--diff` appends colored unified-diff-style hunks after each changed text file.
Binary, special, and files larger than 1 MiB keep the status row but omit the
diff body.

Use `-D/--deleted` to list only paths that are still managed but no longer
exist in the live root. It is equivalent to `--status D`.
Use `-s/--status` for arbitrary status filtering; it may be repeated or
comma-separated. Add `-- <path>...` to limit output to matching files or
subtrees, following the same root-relative style as Git pathspecs.

```sh
mj state --deleted
mj state -D -r home-notes
mj state --deleted -r home-notes
mj state --status D
mj state --status A,M
mj state -s A -s D
mj state 1d -r home-notes -- src
mj state -g -- home-notes/src
```

`mj track` and `mj untrack` separate deletion from management changes.
Files that become managed remain managed when removed from the working tree;
they appear as `D` and can be restored from a suitable snapshot or durable
journal point. `mj untrack <path>` is the explicit operation for removing a
path from future snapshots and backend retention. It does not delete the working
file. `mj track <path>` explicitly brings a path back under management, including
paths that would otherwise be excluded by root rules.

When running inside a configured root, or when `-r/--root` selects exactly one
root, `mj state -U` also lists untracked live files as `?`. Untracked
directories are summarized as one `?/dir/` row and are not expanded
recursively.

```sh
mj track notes/keep.md
mj untrack tmp/local.db
mj track -r home-notes excluded/keep.md
mj untrack -r home-notes old/generated.bin
mj untrack -r home-notes --path-file paths.txt --summary
mj untrack -r home-notes --excluded --dry-run --summary
mj untrack -r home-notes --excluded --summary
```

## History and operations

Show recent managed file changes:

```sh
mj log
mj log --root home-notes
mj log --root home-notes -- src
mj log -- home-notes/src
```

Inspect operation records:

```sh
mj op log
mj op show <op-id>
mj op show <op-id> --files
mj op diff <op-id>
mj op restore <op-id>
```

Show or edit an operation note:

```sh
mj note op-12345678
mj note op-12345678 -m "before migration"
mj note snap-12345678 -m "checkpoint before dependency upgrade"
mj note op-12345678 --stdin
mj note op-12345678 --clear
```

`mj note` accepts operation ids and snapshot ids. A `snap-...` reference is
resolved to the operation that created the snapshot, so release checkpoints and
manual snapshots can be annotated after the fact without rewriting file data.

The first baseline snapshot is recorded as `initial-scan`. Later manual
snapshots are recorded as `manual-snapshot`, while watch-created snapshots are
recorded as `file-events-batch`.

Diff the current snapshot against its parent:

```sh
mj diff
```

Diff explicit snapshots or times:

```sh
mj diff snap-old snap-new --root home-notes
mj diff --at "10 minutes ago"
```

## Restore commands

`mj restore` without a subcommand is an alias for `mj restore apply`, matching
the direct restore form used in the spec.

```sh
mj restore plan --ago 2h --root home-notes --to /tmp/majutsu-restore
mj restore apply --ago 2h --root home-notes --to /tmp/majutsu-restore
mj restore plan --op op-e0b88514 --root home-notes --to /tmp/majutsu-restore
mj restore --at "2026-06-06 10:30:00" --root home-notes --to /tmp/majutsu-restore
```

When `--to` is provided, restore writes files below
`<target>/<root-id>/...`. If `--to` is omitted, restore writes back to the
configured original root path for the selected root.

`restore plan` reports existing destination conflicts. `restore apply` refuses
to overwrite conflicting files unless `--force` is provided.

Time arguments such as `--at` accept RFC3339 timestamps, `YYYY-MM-DD HH:MM:SS`
in the local timezone, `YYYY-MM-DD` as local midnight, `now`, and relative
values such as `10 minutes ago`. Use RFC3339 with an explicit offset when
exchanging commands across hosts.

Restore views can be managed through the restore namespace:

```sh
mj restore mount /tmp/majutsu-view
mj restore hydrate /tmp/majutsu-view --root home-notes --path README.md
mj restore unmount /tmp/majutsu-view
```

The older top-level `mj mount`, `mj hydrate`, and `mj unmount` forms remain
supported as compatibility aliases.

See [Restore Tutorial](RESTORE_TUTORIAL.md) for practical recovery flows.

## Root commands

List roots:

```sh
mj root list
mj root list --json
mj root list --no-truncate
```

Use `--json` for scripts and dashboards. Text output prints complete root
paths by default. When the table is wider than an interactive terminal, `mj`
opens a pager with horizontal scrolling; `--no-truncate` remains as a
compatibility alias and is normally unnecessary.

Size roots and remote usage:

```sh
mj root size
mj root size --json
mj root size --no-remote-cache
```

`mj root size` separates client-side file size from remote backend accounting:

```text
root  files  dirs  client  |  backend  used  payload  metadata  objects  missing
```

Columns to the left of `|` are local client-side data. Columns to the right are
remote-side data. `backend` is the full remote object set required to restore
the root, while `used` is the pack-slice-adjusted amount actually used by that
root.

By default, `mj root size` prefers the local current root-size summary cache.
This keeps the command responsive even when the S3 prefix contains many
objects. Current snapshot `backend` and `used` values are shown from the local
summary or local object metadata. The physical S3 prefix total is shown from a
cached object listing when one exists; otherwise it is marked `exact: false`
with `scope: not-scanned:no-cached-prefix-list`.

New roots apply best-practice excludes before the first snapshot. Besides VCS
and dependency directories, the defaults also avoid generated `artifacts/**`
trees and transient sqlite sidecar files such as `*.sqlite-wal`,
`*.sqlite-shm`, `*.db-wal`, and `*.db-shm`. Add an explicit include or use
`--no-default-excludes` only when those generated files are intentionally part
of the recovery target.

Use `mj root size --no-remote-cache` when you specifically need a fresh exact
S3 object listing. That path can take much longer on large buckets because it
lists the configured remote prefix directly.

When multiple hosts share the same bucket, the remote URL path is the Majutsu
remote root. The intended layout is one host/environment directory directly
under that remote root. The host summary table shows per-host current snapshot
totals so the local environment can be distinguished from other environments:

```text
host      id        roots      client        used     backend  objects  snapshot
--------  --------  -----  ----------  ----------  ----------  -------  -------------
vagrant*  c071a4f3     18  720.92 MiB  242.87 MiB  337.12 MiB    5,459  snap-e0a015ff
winvr     0a88f6e2      8    7.58 MiB   16.55 MiB  246.74 MiB      152  snap-25457225
```

`*` marks the current local host. For responsiveness, the default cached command
shows the current host. `mj root size --no-remote-cache` already scans the
remote prefix, so it also reads other hosts' last published summaries by
default. Set `MAJUTSU_ROOT_SIZE_CROSS_HOST_SUMMARY=1` when you want the cached
path to read those remote summaries too.

The `S3 physical size breakdown` section explains the remote root total as a
sum of object categories. `local-current` is the current host's current
snapshot restore set. `local-history` is the current host's retained historical
restore data. Host metadata, journal, GC state, legacy aliases, and
`other-payload-or-metadata` account for the remaining physical objects.
`host:<name>` rows are objects below another host's top-level prefix. Remaining
`other-payload-or-metadata` rows are objects that do not belong to a known host
prefix and are the first place to look when the shared prefix is much larger
than host current totals. This breakdown is produced on exact or cached remote
object listings; it is skipped
when the remote listing is not available.

The long-term remote layout is host-scoped: a bucket may be shared, but durable
payload, metadata, journal, and GC objects should live directly under a
`<host-prefix>/` directory below the configured remote root and should not be
physically shared across host boundaries. If the bucket is also used for other
purposes, pass a path in the S3 URL such as `s3://bucket/path-to-mj`; Majutsu
then treats `path-to-mj/` as the remote root.

Pause and resume roots:

```sh
mj root pause home-notes
mj root resume home-notes
```

If an active root path disappears, `mj snapshot` records `root-missing` and
marks the root `missing`; it does not snapshot the root as empty. If a root
cannot be scanned because access is denied, it records
`root-permission-denied` and marks the root `permission-denied` instead of
turning unreadable files into deletions.

For roots that must be backed by a mounted filesystem, use `--require-mount`:

```sh
mj root add photos /mnt/photos --require-mount
```

Symlinks are stored as symlink entries by default. Use `--follow-symlinks` when
the root should snapshot linked file contents instead.

New roots apply best-practice excludes for VCS internals, dependency
directories, build outputs, and caches. A path matched by `--include` is treated
as an explicit exception and remains reachable even when an excluded parent
directory would otherwise be pruned:

```sh
mj root add sample ./sample --exclude 'secret/**' --include 'secret/keep.txt'
```

## Large files

Large object commands:

```sh
mj large stat
mj large list
mj large verify
mj large pin --root photos --since 30d
mj large unpin --older-than 180d
```

Pins are stored in metadata and preserved through sync/clone. They are intended
as lifecycle policy inputs for large objects that should remain hot.

Root-specific large-file policy can override global thresholds and patterns:

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
mj root set photos --exclude '*.tmp' --skip-history-rewrite
```

`--skip-history-rewrite` is a recovery option for installations with unreadable
old history objects. It applies the current root metadata/configuration change
without rewriting older snapshots. After repairing history, rerun the cleanup
without this option or use `mj untrack -r <root> --excluded`.

High-frequency files can be marked volatile. The default `checkpoint` mode
suppresses watch-triggered immediate snapshots for matching paths, while manual
or periodic checkpoints still capture the latest content. Use `exclude` when a
path should be removed from snapshots and future history:

```sh
mj root add app ./app --volatile '**/*.sqlite-wal'
mj root set app --volatile 'runtime/**' --volatile-mode exclude
mj root set app --clear-volatile
```

## Watch and daemon

Foreground OS-native filesystem watching:

```sh
mj watch --foreground --mode default --debounce-ms 1500 --settle-ms 500 --periodic-rescan-secs 3600
```

On Linux, the default backend is `fanotify`. It is intended for root-owned
system daemons because fanotify events include the originating pid. If fanotify
is unavailable or the daemon is not running as root, majutsu records a
`watch-backend-error` event and fails instead of silently losing process
attribution. Use `inotify` only when explicitly accepting unattributed watch
events.

```sh
sudo mj --system watch --foreground true --backend fanotify
mj watch --foreground true --backend inotify
```

Polling fallback:

```sh
mj watch --foreground --backend poll --interval-secs 60
```

Daemon management:

```sh
mj daemon start --interval-secs 60 --debounce-ms 1500
mj daemon status
mj daemon metrics
mj daemon stop
mj daemon service --provider systemd --scope user > ~/.config/systemd/user/majutsu.service
mj daemon service --provider systemd --scope user --style forking > ~/.config/systemd/user/majutsu.service
```

The daemon is a process wrapper around foreground watch. It records filesystem
events in the event journal and exposes status IPC under
`$MAJUTSU_HOME/runtime/daemon.sock`. On Linux, prefer a root-owned
`mj --system` daemon so `mj log` can show `fanotify:pid-...` for changes whose
source process is observable.

The default generated systemd unit supervises `mj watch --foreground true`
directly. Use `--style forking` when the host should delegate lifecycle to
`mj daemon start` / `mj daemon stop` and track `runtime/daemon.pid`.

## Prune, pack, and GC

Prune old snapshot metadata:

```sh
mj prune --dry-run --keep-daily 0 --keep-monthly 0
mj prune --dry-run=false --keep-daily 0 --keep-monthly 0
mj prune --dry-run --keep-daily 90 --keep-monthly 36
mj prune --dry-run=false --keep-daily 90 --keep-monthly 36
mj prune --dry-run --drop-missing-remote-history
```

The current snapshot is always kept. Non-dry-run prune removes unkept snapshot
metadata and drops blob/large/chunk metadata no longer referenced by remaining
snapshots.

`mj daemon` runs the same retention prune after a successful watch snapshot sync.
The daemon default is `keep-daily 0` / `keep-monthly 0`, so remote storage
converges toward the current restore set. Set `MAJUTSU_WATCH_PRUNE_KEEP_DAILY`,
`MAJUTSU_WATCH_PRUNE_KEEP_MONTHLY`, or `MAJUTSU_WATCH_AUTO_PRUNE=0` when the
backend should keep longer snapshot history.

Use `--drop-missing-remote-history` when `mj health --deep --history` reports
missing remote objects for old history, while current health remains protected.
The option scans retained, unprotected history snapshots against the remote
payload index and adds snapshots with missing remote objects to the prune
candidate set. The current snapshot and snapshots protected by refs or branches
remain protected.

Pack normal blob objects:

```sh
mj pack
mj pack --compact
mj gc
```

`mj gc` removes unreferenced local loose objects under `$MAJUTSU_HOME/objects`.
It does not delete referenced history or remote objects.

## Branching

Majutsu supports lightweight logical branches on host snapshots:

```sh
mj branch list
mj branch create experiment --at "2026-06-06 10:30:00" --switch --restore --force
mj snapshot --message "experiment from old state"
mj switch main --restore --force
```

See [Branching](BRANCHING.md) for the full workflow.
