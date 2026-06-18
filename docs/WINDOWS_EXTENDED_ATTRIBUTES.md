# Windows native extended attributes

Majutsu uses native Windows extended attributes (EAs) on Windows rather than
pretending that NTFS alternate data streams are Unix xattrs.

The implementation calls the NT native `NtQueryEaFile` and `NtSetEaFile`
interfaces and parses `FILE_FULL_EA_INFORMATION` records. It is informed by
Andre Gleichner's MIT-licensed
[`xattr-win`](https://github.com/AndreGleichner/xattr-win), but is an
independent Rust implementation designed for majutsu's snapshot/restore path.

## Semantics

- Files and directories are supported on EA-capable volumes such as NTFS.
- Reparse points are opened with `FILE_FLAG_OPEN_REPARSE_POINT`, so attributes
  belong to the link/reparse point rather than its target.
- Windows EA names are 8-bit names, at most 254 bytes, and are normalized to
  uppercase by Windows.
- A value is limited to 65,535 bytes.
- `$KERNEL.*` attributes cannot be created from user mode and are not restored.
- Unsupported filesystems are handled with majutsu's existing best-effort xattr
  policy: payload restore continues even when an EA cannot be represented.
- WSL2 does not propagate these EAs between its Linux and Windows sides.

Cross-platform snapshots can contain Linux/macOS attributes whose name or value
cannot be represented as a Windows EA. They remain preserved in the historical
snapshot metadata, but are skipped when materializing that snapshot on Windows.

## Validation on Windows

```powershell
cargo check --workspace --all-targets --locked
cargo test --workspace --all-targets --locked windows_ea -- --test-threads=1
```

For a manual check, snapshot a file with a native EA and restore it to another
NTFS directory, then compare with:

```powershell
fsutil file queryEA .\restored-file
```
