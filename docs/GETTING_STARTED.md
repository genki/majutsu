# Getting Started

This guide walks through a small majutsu setup from an empty state directory to
a verified restore. The examples use the installed CLI name, `mj`.

Majutsu is not a Git replacement. It protects selected host directories as
roots, records file changes in a host-level timeline, and syncs the data needed
to recover the host after local disk loss. It can safely manage a Git working
tree in parallel. New roots exclude VCS internals, dependency directories, build
outputs, and common caches by default; use `--no-default-excludes` only when
those generated files must be backed up too.

## 1. Install

```sh
cargo install majutsu
mj --version
```

For a development checkout:

```sh
cargo install --path .
```

## 2. Choose a Remote

Remote sync is the critical path for host-loss recovery. In production, start
with an S3-compatible backend and encryption:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT_URL=https://storage.googleapis.com
export AWS_SIGNATURE_VERSION=s3v4

export MAJUTSU_REMOTE=s3://bucket/prefix
```

Use a dedicated backend prefix per host or per majutsu instance unless you
explicitly want to share a remote for multi-host recovery browsing.

For a local-only trial, use a file remote instead:

```sh
export MAJUTSU_REMOTE=file:///tmp/mj-demo/remote
```

On Windows, file remotes accept the usual URL-style drive form as well:

```powershell
$env:MAJUTSU_REMOTE = "file:///C:/Users/me/AppData/Local/Temp/mj-demo/remote"
```

The rest of this guide uses `$MAJUTSU_REMOTE` so the same commands work with
either backend.

## 3. Create a Local State

Start with a disposable directory so the first run is easy to inspect:

```sh
rm -rf /tmp/mj-demo
mkdir -p /tmp/mj-demo/root
echo "hello" > /tmp/mj-demo/root/README.md

mj --home /tmp/mj-demo/state init --encrypt --remote "$MAJUTSU_REMOTE"
mj --home /tmp/mj-demo/state root add demo /tmp/mj-demo/root
SNAPSHOT=$(
  mj --home /tmp/mj-demo/state snapshot --message 'initial demo snapshot' |
    awk '/^snapshot / { print $2 }'
)
```

Check the state:

```sh
mj --home /tmp/mj-demo/state status
mj --home /tmp/mj-demo/state root list
mj --home /tmp/mj-demo/state log
echo "$SNAPSHOT"
```

`mj status` is the operational dashboard. `mj log` shows file-change operation
history. `mj root list` shows configured roots.

Export the encryption master key and store it somewhere separate from the host:

```sh
mj --home /tmp/mj-demo/state key export
```

## 4. Inspect Changes

Edit the root and compare it with a recent point in time:

```sh
echo "next line" >> /tmp/mj-demo/root/README.md
touch /tmp/mj-demo/root/new.txt

mj --home /tmp/mj-demo/state state "$SNAPSHOT" -r demo --diff
```

The output uses Git-style markers:

```text
M README.md
A new.txt
```

Metadata-only changes such as directory mtime updates are hidden by default.
Use `--meta` when owner, mode, xattrs, or directory metadata matter:

```sh
mj --home /tmp/mj-demo/state state "$SNAPSHOT" -r demo --meta
```

Create a new durable checkpoint:

```sh
mj --home /tmp/mj-demo/state snapshot --message 'edited demo files'
```

`mj commit` is also available as an alias for users who prefer Git/Jujutsu
terminology:

```sh
mj --home /tmp/mj-demo/state commit --message 'edited demo files'
```

The canonical term remains snapshot because majutsu first preserves changes
through its event journal and sync path; snapshots are durable timeline
checkpoints and compaction boundaries.

## 5. Sync to the Remote

Upload metadata and objects to the configured remote:

```sh
mj --home /tmp/mj-demo/state sync --wait
mj --home /tmp/mj-demo/state sync status
mj --home /tmp/mj-demo/state remote fsck
```

## 6. Keep the Daemon Running

For crash protection, the daemon should be running after setup so filesystem
changes are observed and synced without relying on manual snapshots.

Foreground check:

```sh
mj watch --foreground --backend notify --debounce-ms 1500
```

User-level systemd service:

```sh
mkdir -p ~/.config/systemd/user
mj daemon service --provider systemd --scope user > ~/.config/systemd/user/majutsu.service
systemctl --user daemon-reload
systemctl --user enable --now majutsu.service

mj daemon status
mj health
```

`mj health` is the lightweight signal to monitor. It reports whether active
roots, daemon state, pending events, upload queue, and cached remote head are in
a protected state.

For root-owned host configuration, use a separate system instance:

```sh
sudo mj --system init --encrypt --remote s3://bucket/prefix/system
sudo mj --system root add systemd-system /etc/systemd/system --include '**'
sudo mj --system daemon service --provider systemd --scope system \
  > /etc/systemd/system/majutsu.service
sudo systemctl enable --now majutsu.service
```

Do not run the user instance with sudo to protect both user repos and system
files. Keep user and system state homes, backend prefixes, and encryption keys
separate.

## 7. Verify Restore

Restore into a temporary directory before trusting a new setup:

```sh
mj --home /tmp/mj-demo/state restore plan --to /tmp/mj-demo/restore
mj --home /tmp/mj-demo/state restore apply --to /tmp/mj-demo/restore
find /tmp/mj-demo/restore -maxdepth 3 -type f -print
```

Recover from an empty state using the remote:

```sh
MAJUTSU_MASTER_KEY=<64-hex-key> \
  mj --home /tmp/mj-demo/recovered clone --remote "$MAJUTSU_REMOTE"
mj --home /tmp/mj-demo/recovered fsck --quick
mj --home /tmp/mj-demo/recovered restore apply --to /tmp/mj-demo/recovered-files
```

For file remote trials, encryption still works the same way. Use the key printed
by `mj key export` when cloning into the empty recovered state.

## 8. Common Next Steps

- Use `mj root size` to compare local root size with remote restore data and
  billed backend prefix size.
- Use `mj root add <id> <path> --no-default-excludes` only for roots where VCS
  internals, dependency directories, build outputs, and caches are part of the
  data you intentionally need to recover.
- Use `mj state 1d -r <root> --diff` for Git-style inspection of recent file
  changes.
- Use `mj branch create <name> --at <time> --switch --restore --force` to branch
  from an older host timeline point.
- Use `mj switch <name> --restore --force` to switch a majutsu timeline branch.
- Use `mj prune --dry-run` before shortening retained history.

## Related Documentation

- [CLI layout](CLI_LAYOUT.md)
- [Operations runbook](OPERATIONS.md)
- [Branching](BRANCHING.md)
- [Platform support](PLATFORM_SUPPORT.md)
- [Remote metadata storage](REMOTE_METADATA_STORAGE.md)
- [Encrypted production state](ENCRYPTED_PRODUCTION_STATE.md)
