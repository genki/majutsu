#!/usr/bin/env python3
from pathlib import Path
import sys

root = Path(sys.argv[1] if len(sys.argv) > 1 else '.').resolve()
errors = []
cargo = (root / 'Cargo.toml').read_text(encoding='utf-8')
if "[target.'cfg(unix)'.dependencies]" not in cargo or 'xattr = "1"' not in cargo:
    errors.append('xattr must be target-specific to cfg(unix)')
if "[target.'cfg(windows)'.dependencies]" not in cargo or 'windows-sys' not in cargo:
    errors.append('windows-sys target dependency is missing')

for relative in [
    'src/process_runtime.rs', 'src/daemon_runtime.rs', 'src/watch_runtime.rs',
    'src/atomic_io.rs', 'src/restore_apply.rs'
]:
    path = root / relative
    if not path.exists():
        continue
    text = path.read_text(encoding='utf-8')
    if 'ProcessCommand::new("kill")' in text or 'Command::new("kill")' in text:
        errors.append(f'{relative}: external kill remains')
    if 'ProcessCommand::new("sh")' in text or 'Command::new("sh")' in text:
        errors.append(f'{relative}: direct sh invocation remains')
    if 'std::os::unix::net' in text:
        errors.append(f'{relative}: hard-coded Unix daemon socket remains')

if not (root / 'src/fs_meta/windows_ea.rs').exists():
    errors.append('Windows native EA module is missing')

if errors:
    for error in errors:
        print('ERROR:', error, file=sys.stderr)
    raise SystemExit(1)
print('cross-platform source audit ok')
