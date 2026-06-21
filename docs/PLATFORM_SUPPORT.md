# Platform support

Majutsu's snapshot, history, remote sync, clone, and materialized restore paths
are supported on Linux, macOS, and Windows.

| Capability | Linux | macOS | Windows |
|---|---|---|---|
| native watch | inotify/notify | notify/FSEvents | notify/ReadDirectoryChangesW |
| daemon IPC | Unix socket | Unix socket | authenticated loopback endpoint |
| service renderer | systemd | launchd | Task Scheduler PowerShell |
| extended attributes | xattr | xattr/resource forks | native NT EAs |
| atomic replacement | rename | rename | MoveFileExW replace/write-through |
| materialized restore | yes | yes | yes |
| kernel FUSE restore view | yes | no | no |

## Windows

User state defaults to `%USERPROFILE%\.majutsu`. System state uses
`%PROGRAMDATA%\Majutsu\state` and `%PROGRAMDATA%\Majutsu\config.toml`.

File remotes accept both native Windows paths and URL-style drive paths:

```powershell
mj init --remote file://C:\Users\me\majutsu-remote
mj init --remote file:///C:/Users/me/majutsu-remote
```

Generate a user task:

```powershell
mj daemon service --provider windows-task --scope user |
  powershell -NoProfile -Command -
```

Generate a SYSTEM task from elevated PowerShell:

```powershell
mj --system daemon service --provider windows-task --scope system |
  powershell -NoProfile -Command -
```

Symlink restore requires Developer Mode or the **Create symbolic links**
privilege. Unix FIFO/device/socket nodes cannot be materialized. Native NTFS or
ReFS EAs are preserved; NTFS alternate data streams and Windows ACL/security
descriptors are not yet part of snapshot metadata.

## macOS

The notify backend uses the native event mechanism. Materialized restore views
and xattrs/resource forks are supported. FUSE remains Linux-only.

```sh
mj daemon service --provider launchd --scope user \
  > ~/Library/LaunchAgents/dev.majutsu.watch.plist
launchctl bootstrap gui/"$(id -u)" \
  ~/Library/LaunchAgents/dev.majutsu.watch.plist
```

System launchd output can be installed below `/Library/LaunchDaemons` by root.
