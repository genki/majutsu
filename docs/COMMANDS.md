# Command Guide

This page summarizes the daily CLI surface. Run `mj --help` and
`mj help <command>` for the exact option list.

## Command layout

The stable top-level commands are grouped by purpose in `mj --help`;
maintenance commands remain top-level for Git-style discoverability.

```text
Setup: init, root
Daily use: status, health, state, log, diff, snapshot, commit
History: branch, switch, op
Recovery: restore, restore mount, restore unmount, restore hydrate, mount, unmount, hydrate, clone
Remote: sync, remote, lifecycle
Service: watch, daemon
Security: key
Storage maintenance: large, cache, pack, prune, gc, fsck
Advanced/debug: event
```

`mj commit` is a visible alias for `mj snapshot`, intended for users familiar
with Git/Jujutsu terminology. The canonical term remains `snapshot` because
Majutsu first preserves changes through the daemon/event journal and remote
sync; snapshots are durable timeline checkpoints and compaction boundaries.

`mj switch` is a top-level alias for `mj branch switch`.

## State and diff inspection

`mj state <ref>` prints a Git `status -s`-style view of managed file changes
since a snapshot, operation id, absolute time, or relative time.

```sh
mj state 1d
mj state 1d -r home-notes
mj state 03:40 -r home-notes --diff
mj state op-123456789abc -g
```

Markers:

```text
A  added file
M  content or restore-significant change
D  deleted file
m  metadata-only change, shown only with --meta
```

Metadata-only changes such as directory mtime updates are hidden by default so
file additions do not also produce noisy parent-directory rows. Use `--meta`
when mode, owner, xattrs, or directory mtime changes matter:

```sh
mj state 1d -r home-notes --meta
```

`--diff` appends colored unified-diff-style lines after each changed text file.
Binary, special, and files larger than 1 MiB keep the status row but omit the
diff body.

## History and operations

Show recent managed file changes:

```sh
mj log
mj log --root home-notes
```

Inspect operation records:

```sh
mj op log
mj op show <op-id>
mj op show <op-id> --files
mj op diff <op-id>
mj op restore <op-id>
```

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

Use `--json` for scripts and dashboards. Use `--no-truncate` when you need the
complete root paths in a narrow terminal or captured log.

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
```

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
mj prune --dry-run --keep-daily 90 --keep-monthly 36
mj prune --dry-run=false --keep-daily 90 --keep-monthly 36
```

The current snapshot is always kept. Non-dry-run prune removes unkept snapshot
metadata and drops blob/large/chunk metadata no longer referenced by remaining
snapshots.

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
