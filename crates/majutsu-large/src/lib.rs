use anyhow::Result;
use majutsu_core::ObjectKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Chunking {
    Fixed { size: usize },
    FastCdc { average_size: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeObjectPointer {
    pub version: u32,
    pub oid: String,
    pub size: u64,
    pub binary: bool,
    pub chunking: Chunking,
    pub chunks_manifest: ObjectKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeChunkRef {
    pub index: usize,
    pub offset: u64,
    pub len: u64,
    pub object_key: ObjectKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLargeChunk {
    pub bytes: Vec<u8>,
    pub compression: String,
}

pub fn chunk_ranges_for_bytes(
    chunking: &str,
    chunk_size: usize,
    bytes: &[u8],
) -> Vec<(usize, usize)> {
    if chunking == "fastcdc" {
        content_defined_ranges(bytes, chunk_size)
    } else {
        fixed_ranges(bytes.len(), chunk_size)
    }
}

pub fn fixed_ranges(len: usize, chunk_size: usize) -> Vec<(usize, usize)> {
    let chunk_size = chunk_size.max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    while start < len {
        let end = (start + chunk_size).min(len);
        ranges.push((start, end));
        start = end;
    }
    ranges
}

pub fn content_defined_ranges(bytes: &[u8], target: usize) -> Vec<(usize, usize)> {
    let len = bytes.len();
    let target = target.max(1024);
    let min = (target / 4).max(1024).min(target);
    let max = (target * 4).max(min + 1);
    let mask = target.next_power_of_two().saturating_sub(1).max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    let mut rolling = 0u64;
    while start < len {
        let hard_end = (start + max).min(len);
        let mut end = hard_end;
        let mut i = start;
        while i < hard_end {
            rolling = rolling
                .rotate_left(1)
                .wrapping_add(bytes[i] as u64)
                .wrapping_mul(0x9E37_79B1_85EB_CA87);
            let current_len = i + 1 - start;
            if current_len >= min && ((rolling as usize) & mask) == 0 {
                end = i + 1;
                break;
            }
            i += 1;
        }
        ranges.push((start, end));
        start = end;
    }
    ranges
}

pub fn compress_chunk_if_useful(
    bytes: &[u8],
    enabled: bool,
    algorithm: &str,
    level: i32,
    min_gain_ratio: f64,
    skip_extensions: &[String],
    file_name: &str,
) -> Result<StoredLargeChunk> {
    if !should_compress(enabled, algorithm, skip_extensions, file_name) {
        return Ok(StoredLargeChunk {
            bytes: bytes.to_vec(),
            compression: "none".into(),
        });
    }
    let compressed = zstd::stream::encode_all(bytes, level)?;
    let gain = 1.0 - (compressed.len() as f64 / bytes.len().max(1) as f64);
    if gain >= min_gain_ratio {
        Ok(StoredLargeChunk {
            bytes: compressed,
            compression: "zstd".into(),
        })
    } else {
        Ok(StoredLargeChunk {
            bytes: bytes.to_vec(),
            compression: "none".into(),
        })
    }
}

pub fn should_compress(
    enabled: bool,
    algorithm: &str,
    skip_extensions: &[String],
    file_name: &str,
) -> bool {
    enabled
        && algorithm == "zstd"
        && !skip_extensions
            .iter()
            .any(|pattern| glob_match(pattern, file_name))
}

fn glob_match(pattern: &str, name: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix("*.") {
        return name.ends_with(&format!(".{suffix}"));
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return name.starts_with(prefix);
    }
    pattern == name
}
