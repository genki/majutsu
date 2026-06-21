# Restore Tutorial

This tutorial shows the common restore flows for a majutsu-managed host. The
examples use a single root named `notes`, but the same commands work with any
configured root.

## 1. Inspect the Protected State

Start with the operational view:

```sh
mj status
mj root list
mj state -r notes 1d
```

When you run `mj state` from inside a managed root, majutsu scopes the output to
that root by default. Use `-g` to inspect every root:

```sh
mj state -g 2h
```

## 2. Restore to a Temporary Directory

Always restore into a temporary directory first when you are checking an older
point in time:

```sh
mj restore plan --root notes --ago 2h --to /tmp/mj-restore
mj restore apply --root notes --ago 2h --to /tmp/mj-restore
```

`--ago` accepts compact durations such as `30m`, `2h`, or `1d`. It selects the
latest snapshot at or before that relative time.

For a wall-clock time, use `--at`:

```sh
mj restore plan --root notes --at '2026-06-22 09:30:00' --to /tmp/mj-restore
```

Offsetless times use the local timezone of the environment running `mj`. When
sharing a command across hosts, prefer an explicit RFC3339 offset:

```sh
mj restore plan --root notes --at '2026-06-22T09:30:00+09:00' --to /tmp/mj-restore
```

## 3. Restore by Operation ID

`mj log` shows file-change operations. Operation IDs can be abbreviated as long
as the prefix is unambiguous:

```sh
mj log --root notes
mj restore plan --root notes --op op-e0b88514 --to /tmp/mj-restore
mj restore apply --root notes --op op-e0b88514 --to /tmp/mj-restore
```

`mj restore --op` restores the state produced by that operation. This is useful
when you can identify the operation that introduced or captured a desired file
state.

The lower-level operation command also accepts abbreviated IDs:

```sh
mj op show op-e0b88514 --files
mj op diff op-e0b88514
```

`mj op restore <op>` moves majutsu's current ref to the operation's rollback
point. Use it when you intentionally want to move the active timeline reference,
not just materialize files into a restore directory.

## 4. Write Back to the Original Root

After checking the temporary restore output, write back to the configured root
only when you are ready:

```sh
mj restore plan --root notes --ago 2h
mj restore apply --root notes --ago 2h --force
```

Without `--to`, restore writes to the original configured root path. `plan`
reports conflicts and deletes first. `apply` refuses destructive changes unless
`--force` is present.

## 5. After Restore

Check the state and let the daemon/sync path protect the restored files:

```sh
mj state -r notes 1h
mj sync --wait
mj status
```

If the host state itself was lost and recovered from remote storage, run a quick
integrity check before restoring:

```sh
mj clone --remote "$MAJUTSU_REMOTE"
mj fsck --quick
mj restore apply --to /tmp/recovered-files
```
