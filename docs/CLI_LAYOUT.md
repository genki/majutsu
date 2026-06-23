# CLI layout

The stable top-level commands remain compatible. New names are additive aliases
or namespace conveniences; maintenance commands stay top-level and are not
moved under a `maintenance` parent command.

## Language

Human-facing help and command descriptions respect `LC_ALL`, `LC_MESSAGES`,
and `LANG` for English, Japanese, Chinese, Spanish, and French. Empty locale
variables are ignored so `LANG=ja_JP.UTF-8 mj --help` works in a clean shell.

Machine-oriented output remains stable: JSON field names, tabular keys,
operation kinds, status markers, and `key value` reports stay in English so
scripts do not break when the user's locale changes.

## Command groups

```text
Setup:
  init
  root

Daily use:
  status
  health
  state
  log
  diff
  snapshot
  commit    alias: snapshot
  note
  track
  untrack

History:
  branch
  switch    alias: branch switch
  op        jj-style operation log

Recovery:
  restore
  restore mount
  restore unmount
  restore hydrate
  mount      compatibility top-level form
  unmount    compatibility top-level form
  hydrate    compatibility top-level form
  clone

Remote:
  sync
  remote
  lifecycle

Service:
  watch
  daemon

Security:
  key

Storage maintenance:
  large
  cache
  pack
  prune
  gc
  fsck

Advanced/debug:
  event
```

## Git/Jujutsu familiarity

`status`, `log`, `diff`, `branch`, `restore`, `fsck`, and `gc` intentionally
use familiar names. `op` is explicitly a jj-style operation log for internal
operation history.

`mj commit` is a user-facing alias for `mj snapshot`, but `snapshot` remains
the canonical term. Majutsu preserves changes through the daemon/event journal
and remote sync path; a snapshot is a durable checkpoint in the host timeline.

`mj switch` is a top-level alias for `mj branch switch`.

`mj note` edits the human note/message on an existing operation. It also accepts
`snap-...` references and resolves them to the operation that created the
snapshot.

```sh
mj note op-12345678
mj note op-12345678 -m "before migration"
mj note snap-12345678 --clear
```

## State inspection

`mj state` is the Git `status -s`-like command for managed files. Without a
reference it compares the first snapshot with the live filesystem. With a
reference it compares that point in time with the live filesystem.

```sh
mj state
mj state 1d
mj state 1d -r moon
mj state 03:40 -r moon --diff
mj state op-123456789abc -g
mj state --deleted
mj state --status A,M
mj track path/to/file
mj untrack path/to/file
```

Markers:

```text
A  added file
M  content or restore-significant change
D  deleted file
m  metadata-only change; shown only with --meta
```

`--diff` prints colored unified-diff-style text hunks after file rows.
`--meta` includes metadata-only changes such as directory mtime, mode, owner,
or xattrs.
`--deleted` filters the output to `D` rows and is equivalent to `--status D`.
`-s/--status` accepts repeated or comma-separated `A`, `M`, `D`, and `m`.
`mj track` explicitly protects a path even if root excludes would normally hide
it. `mj untrack` is the explicit operation for removing a path from management;
plain `rm` remains a tracked deletion.

## Restore view namespace

Prefer the restore namespace for restore views:

```sh
mj restore mount /tmp/majutsu-view
mj restore hydrate /tmp/majutsu-view --root moon --path README.md
mj restore unmount /tmp/majutsu-view
```

Existing `mj mount`, `mj hydrate`, and `mj unmount` forms remain supported.
