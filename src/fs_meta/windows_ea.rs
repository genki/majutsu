//! Native Windows extended-attribute support.
//!
//! Windows EAs are exposed through the NT native `NtQueryEaFile` and
//! `NtSetEaFile` entry points. This module intentionally keeps the unsafe FFI
//! surface small and presents an ordinary `std::io::Result` API to the rest of
//! majutsu.
//!
//! The implementation is informed by Andre Gleichner's MIT-licensed
//! `xattr-win` utility, but is written independently in Rust.

use std::ffi::c_void;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::ptr;

type NtStatus = i32;

const FILE_READ_EA: u32 = 0x0008;
const FILE_WRITE_EA: u32 = 0x0010;
const SYNCHRONIZE: u32 = 0x0010_0000;

const FILE_SHARE_READ: u32 = 0x0000_0001;
const FILE_SHARE_WRITE: u32 = 0x0000_0002;
const FILE_SHARE_DELETE: u32 = 0x0000_0004;

const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

const STATUS_BUFFER_OVERFLOW: NtStatus = 0x8000_0005_u32 as NtStatus;
const STATUS_NO_MORE_EAS: NtStatus = 0x8000_0012_u32 as NtStatus;
const STATUS_NONEXISTENT_EA_ENTRY: NtStatus = 0xC000_0051_u32 as NtStatus;
const STATUS_NO_EAS_ON_FILE: NtStatus = 0xC000_0052_u32 as NtStatus;

const MAX_EA_NAME_BYTES: usize = 254;
const MAX_EA_VALUE_BYTES: usize = u16::MAX as usize;
const FILE_FULL_EA_HEADER_BYTES: usize = 8;
const QUERY_BUFFER_BYTES: usize =
    FILE_FULL_EA_HEADER_BYTES + MAX_EA_NAME_BYTES + 1 + MAX_EA_VALUE_BYTES + 4;

#[repr(C)]
union IoStatusValue {
    status: NtStatus,
    pointer: *mut c_void,
}

#[repr(C)]
struct IoStatusBlock {
    value: IoStatusValue,
    information: usize,
}

impl Default for IoStatusBlock {
    fn default() -> Self {
        Self {
            value: IoStatusValue { status: 0 },
            information: 0,
        }
    }
}

#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtQueryEaFile(
        file_handle: *mut c_void,
        io_status_block: *mut IoStatusBlock,
        buffer: *mut c_void,
        length: u32,
        return_single_entry: u8,
        ea_list: *mut c_void,
        ea_list_length: u32,
        ea_index: *mut u32,
        restart_scan: u8,
    ) -> NtStatus;

    fn NtSetEaFile(
        file_handle: *mut c_void,
        io_status_block: *mut IoStatusBlock,
        buffer: *mut c_void,
        length: u32,
    ) -> NtStatus;

    fn RtlNtStatusToDosError(status: NtStatus) -> u32;
}

/// Returns all native EAs attached to `path`.
///
/// The file or directory is opened with `FILE_FLAG_OPEN_REPARSE_POINT`, so an
/// EA on a symlink/reparse point is read from the link itself rather than from
/// its target.
pub(crate) fn list(path: &Path) -> io::Result<Vec<(String, Vec<u8>)>> {
    let file = open_for_ea(path, FILE_READ_EA)?;
    let handle = file.as_raw_handle().cast::<c_void>();
    let mut restart_scan = 1_u8;
    let mut out = Vec::new();

    loop {
        let mut bytes = vec![0_u8; QUERY_BUFFER_BYTES];
        let mut iosb = IoStatusBlock::default();
        let status = unsafe {
            NtQueryEaFile(
                handle,
                &mut iosb,
                bytes.as_mut_ptr().cast(),
                u32::try_from(bytes.len()).expect("EA query buffer fits u32"),
                1,
                ptr::null_mut(),
                0,
                ptr::null_mut(),
                restart_scan,
            )
        };
        restart_scan = 0;

        if matches!(
            status,
            STATUS_NO_MORE_EAS | STATUS_NO_EAS_ON_FILE | STATUS_NONEXISTENT_EA_ENTRY
        ) {
            break;
        }
        if !nt_success(status) && status != STATUS_BUFFER_OVERFLOW {
            return Err(ntstatus_error("query Windows extended attributes", status));
        }

        let used = iosb.information.min(bytes.len());
        if used == 0 {
            if status == STATUS_BUFFER_OVERFLOW {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Windows EA entry exceeded the maximum supported EA size",
                ));
            }
            break;
        }
        parse_full_ea_entries(&bytes[..used], &mut out)?;

        // With ReturnSingleEntry=TRUE the next call advances the handle's EA
        // enumeration cursor. A warning status may still carry a complete
        // entry, so continue after parsing it.
    }

    Ok(out)
}

/// Creates or replaces one native Windows EA.
pub(crate) fn set(path: &Path, name: &str, value: &[u8]) -> io::Result<()> {
    if value.len() > MAX_EA_VALUE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Windows EA value is too large: {} bytes (maximum {})",
                value.len(),
                MAX_EA_VALUE_BYTES
            ),
        ));
    }

    let name = encode_name(name)?;
    let raw_len = FILE_FULL_EA_HEADER_BYTES + name.len() + 1 + value.len();
    let aligned_len = align4(raw_len);
    let mut buffer = vec![0_u8; aligned_len];

    // FILE_FULL_EA_INFORMATION:
    //   ULONG NextEntryOffset
    //   UCHAR Flags
    //   UCHAR EaNameLength
    //   USHORT EaValueLength
    //   CHAR EaName[1], NUL, value
    buffer[0..4].copy_from_slice(&0_u32.to_le_bytes());
    buffer[4] = 0;
    buffer[5] = u8::try_from(name.len()).expect("EA name validated to u8");
    buffer[6..8].copy_from_slice(
        &u16::try_from(value.len())
            .expect("EA value validated to u16")
            .to_le_bytes(),
    );
    let name_start = FILE_FULL_EA_HEADER_BYTES;
    let name_end = name_start + name.len();
    buffer[name_start..name_end].copy_from_slice(&name);
    buffer[name_end] = 0;
    buffer[name_end + 1..name_end + 1 + value.len()].copy_from_slice(value);

    let file = open_for_ea(path, FILE_WRITE_EA)?;
    let mut iosb = IoStatusBlock::default();
    let status = unsafe {
        NtSetEaFile(
            file.as_raw_handle().cast::<c_void>(),
            &mut iosb,
            buffer.as_mut_ptr().cast(),
            u32::try_from(buffer.len()).expect("EA set buffer fits u32"),
        )
    };
    if nt_success(status) {
        Ok(())
    } else {
        Err(ntstatus_error("set Windows extended attribute", status))
    }
}

/// Removes one native Windows EA.
///
/// Windows represents deletion as `NtSetEaFile` with a zero-length value.
#[allow(dead_code)]
pub(crate) fn remove(path: &Path, name: &str) -> io::Result<()> {
    set(path, name, &[])
}

fn open_for_ea(path: &Path, access: u32) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options
        .access_mode(access | SYNCHRONIZE)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    options.open(path)
}

fn parse_full_ea_entries(bytes: &[u8], out: &mut Vec<(String, Vec<u8>)>) -> io::Result<()> {
    let mut offset = 0_usize;
    loop {
        let header_end = offset
            .checked_add(FILE_FULL_EA_HEADER_BYTES)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Windows EA offset overflow")
            })?;
        if header_end > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated FILE_FULL_EA_INFORMATION header",
            ));
        }

        let next = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .expect("validated EA header"),
        ) as usize;
        let name_len = bytes[offset + 5] as usize;
        let value_len = u16::from_le_bytes(
            bytes[offset + 6..offset + 8]
                .try_into()
                .expect("validated EA header"),
        ) as usize;

        let name_start = header_end;
        let name_end = name_start.checked_add(name_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Windows EA name overflow")
        })?;
        let value_start = name_end.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Windows EA value offset overflow",
            )
        })?;
        let value_end = value_start.checked_add(value_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Windows EA value overflow")
        })?;
        if value_end > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated FILE_FULL_EA_INFORMATION entry",
            ));
        }
        if bytes.get(name_end).copied() != Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Windows EA name is not NUL terminated",
            ));
        }

        let name = decode_name(&bytes[name_start..name_end]);
        out.push((name, bytes[value_start..value_end].to_vec()));

        if next == 0 {
            break;
        }
        if next < FILE_FULL_EA_HEADER_BYTES || !next.is_multiple_of(4) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Windows EA NextEntryOffset",
            ));
        }
        offset = offset.checked_add(next).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Windows EA offset overflow")
        })?;
        if offset >= bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Windows EA NextEntryOffset is outside the result buffer",
            ));
        }
    }
    Ok(())
}

fn encode_name(name: &str) -> io::Result<Vec<u8>> {
    if name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows EA name must not be empty",
        ));
    }

    let mut bytes = Vec::with_capacity(name.len());
    for ch in name.chars() {
        let code = u32::from(ch);
        if code > u8::MAX as u32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows EA names must contain only 8-bit characters",
            ));
        }
        let byte = code as u8;
        if byte <= 0x1f || b"\\/:*?\"<>|,+=[];".contains(&byte) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Windows EA name contains an invalid character: {name:?}"),
            ));
        }
        bytes.push(byte);
    }

    if bytes.len() > MAX_EA_NAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Windows EA name is too long: {} bytes (maximum {})",
                bytes.len(),
                MAX_EA_NAME_BYTES
            ),
        ));
    }
    if name.to_ascii_uppercase().starts_with("$KERNEL.") {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "user-mode processes cannot create $KERNEL.* Windows EAs",
        ));
    }
    Ok(bytes)
}

fn decode_name(name: &[u8]) -> String {
    // Native EA names are an 8-bit byte sequence. Map byte-for-byte through
    // U+0000..U+00FF so the representation is reversible in majutsu JSON.
    name.iter().copied().map(char::from).collect()
}

fn align4(value: usize) -> usize {
    (value + 3) & !3
}

fn nt_success(status: NtStatus) -> bool {
    status >= 0
}

fn ntstatus_error(action: &str, status: NtStatus) -> io::Error {
    let win32 = unsafe { RtlNtStatusToDosError(status) };
    let source = io::Error::from_raw_os_error(win32 as i32);
    io::Error::new(
        source.kind(),
        format!("{action}: NTSTATUS 0x{:08X}: {source}", status as u32),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_name_rules() {
        assert_eq!(encode_name("MAJUTSU.TEST").unwrap(), b"MAJUTSU.TEST");
        assert!(encode_name("").is_err());
        assert!(encode_name("bad/name").is_err());
        assert!(encode_name("$KERNEL.TEST").is_err());
        assert!(encode_name(&"x".repeat(255)).is_err());
    }

    #[test]
    fn native_ea_round_trip_on_file_and_directory() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("ea-file");
        std::fs::write(&file, b"payload").unwrap();

        let value = [0_u8, 1, 2, 0x7f, 0xff];
        set(&file, "MAJUTSU.TEST", &value).unwrap();
        let entries = list(&file).unwrap();
        assert!(
            entries
                .iter()
                .any(|(name, got)| { name.eq_ignore_ascii_case("MAJUTSU.TEST") && got == &value })
        );
        remove(&file, "MAJUTSU.TEST").unwrap();
        assert!(
            !list(&file)
                .unwrap()
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("MAJUTSU.TEST"))
        );

        let directory = temp.path().join("ea-dir");
        std::fs::create_dir(&directory).unwrap();
        set(&directory, "MAJUTSU.DIR", b"directory").unwrap();
        assert!(list(&directory).unwrap().iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("MAJUTSU.DIR") && value == b"directory"
        }));
    }
}
