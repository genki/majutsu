use anyhow::Result;
use majutsu_core::ObjectKey;

pub const DEFAULT_LARGE_MIN_SIZE: u64 = 64 * 1024 * 1024;
pub const DEFAULT_LARGE_BINARY_MIN_SIZE: u64 = 16 * 1024 * 1024;
pub const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;
pub const DEFAULT_MAX_PARALLEL_UPLOADS: usize = 8;
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 3;
pub const DEFAULT_COMPRESSION_SAMPLE_BYTES: usize = 1024 * 1024;
pub const DEFAULT_COMPRESSION_MIN_GAIN_RATIO: f64 = 0.05;

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
    sample_bytes: usize,
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
    let sample_len = sample_bytes.min(bytes.len());
    if sample_len > 0 && sample_len < bytes.len() {
        let sample = &bytes[..sample_len];
        let compressed_sample = zstd::stream::encode_all(sample, level)?;
        let sample_gain = compression_gain(sample.len(), compressed_sample.len());
        if sample_gain < min_gain_ratio {
            return Ok(StoredLargeChunk {
                bytes: bytes.to_vec(),
                compression: "none".into(),
            });
        }
    }
    let compressed = zstd::stream::encode_all(bytes, level)?;
    let gain = compression_gain(bytes.len(), compressed.len());
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

fn compression_gain(original_len: usize, stored_len: usize) -> f64 {
    1.0 - (stored_len as f64 / original_len.max(1) as f64)
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

pub fn default_large_min_size() -> u64 {
    DEFAULT_LARGE_MIN_SIZE
}

pub fn default_large_binary_min_size() -> u64 {
    DEFAULT_LARGE_BINARY_MIN_SIZE
}

pub fn default_chunk_size() -> usize {
    DEFAULT_CHUNK_SIZE
}

pub fn default_chunking() -> &'static str {
    "fixed"
}

pub fn default_max_parallel_uploads() -> usize {
    DEFAULT_MAX_PARALLEL_UPLOADS
}

pub fn default_compression_algorithm() -> &'static str {
    "zstd"
}

pub fn default_compression_level() -> i32 {
    DEFAULT_COMPRESSION_LEVEL
}

pub fn default_compression_sample_bytes() -> usize {
    DEFAULT_COMPRESSION_SAMPLE_BYTES
}

pub fn default_compression_min_gain_ratio() -> f64 {
    DEFAULT_COMPRESSION_MIN_GAIN_RATIO
}

pub fn default_large_always_patterns() -> Vec<String> {
    [
        "*.mp4",
        "*.mov",
        "*.mkv",
        "*.zip",
        "*.tar",
        "*.tar.zst",
        "*.parquet",
        "*.sqlite",
        "*.db",
        "*.vmdk",
        "*.qcow2",
        "*.iso",
        "*.psd",
        "*.blend",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn default_large_never_patterns() -> Vec<String> {
    ["*.rs", "*.toml", "*.yaml", "*.json", "*.md"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

pub fn default_compression_skip_extensions() -> Vec<String> {
    [
        "*.jpg",
        "*.jpeg",
        "*.png",
        "*.heic",
        "*.mp4",
        "*.mov",
        "*.zip",
        "*.gz",
        "*.zst",
        "*.xz",
        "*.parquet",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_sample_can_skip_full_chunk_compression() {
        let mut bytes = Vec::new();
        let mut value = 0x1234_5678u64;
        for _ in 0..2048 {
            value ^= value << 13;
            value ^= value >> 7;
            value ^= value << 17;
            bytes.push((value & 0xff) as u8);
        }
        bytes.extend(std::iter::repeat_n(b'a', 32 * 1024));

        let stored =
            compress_chunk_if_useful(&bytes, true, "zstd", 3, 2048, 0.05, &[], "payload.dat")
                .unwrap();

        assert_eq!(stored.compression, "none");
        assert_eq!(stored.bytes, bytes);
    }

    #[test]
    fn compression_sample_allows_useful_full_chunk_compression() {
        let bytes = vec![b'a'; 32 * 1024];

        let stored =
            compress_chunk_if_useful(&bytes, true, "zstd", 3, 2048, 0.05, &[], "payload.dat")
                .unwrap();

        assert_eq!(stored.compression, "zstd");
        assert!(stored.bytes.len() < bytes.len());
    }

    #[test]
    fn defaults_match_large_pipeline_spec() {
        assert_eq!(default_large_min_size(), 64 * 1024 * 1024);
        assert_eq!(default_large_binary_min_size(), 16 * 1024 * 1024);
        assert_eq!(default_chunk_size(), 8 * 1024 * 1024);
        assert_eq!(default_chunking(), "fixed");
        assert_eq!(default_max_parallel_uploads(), 8);
        assert_eq!(default_compression_algorithm(), "zstd");
        assert_eq!(default_compression_level(), 3);
        assert_eq!(default_compression_sample_bytes(), 1024 * 1024);
        assert_eq!(default_compression_min_gain_ratio(), 0.05);
    }

    #[test]
    fn default_patterns_route_binary_archives_and_skip_precompressed_media() {
        let always = default_large_always_patterns();
        assert!(always.contains(&"*.mp4".into()));
        assert!(always.contains(&"*.tar.zst".into()));
        assert!(always.contains(&"*.sqlite".into()));

        let never = default_large_never_patterns();
        assert!(never.contains(&"*.rs".into()));
        assert!(never.contains(&"*.md".into()));

        let skip = default_compression_skip_extensions();
        assert!(skip.contains(&"*.jpg".into()));
        assert!(skip.contains(&"*.parquet".into()));
        assert!(!should_compress(
            true,
            default_compression_algorithm(),
            &skip,
            "payload.zip"
        ));
    }
}
