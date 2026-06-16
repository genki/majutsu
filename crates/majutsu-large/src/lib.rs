#[cfg(not(feature = "standalone-crate"))]
use crate::majutsu_core;
use anyhow::Result;
use chrono::{DateTime, Utc};
#[cfg(feature = "standalone-crate")]
use majutsu_core;
use majutsu_core::ObjectKey;
use serde::{Deserialize, Serialize};

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

pub struct CompressionInput<'a> {
    pub bytes: &'a [u8],
    pub enabled: bool,
    pub algorithm: &'a str,
    pub level: i32,
    pub sample_bytes: usize,
    pub min_gain_ratio: f64,
    pub skip_extensions: &'a [String],
    pub file_name: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargeObjectExport {
    pub oid: String,
    pub size: u64,
    pub chunk_count: usize,
    pub manifest_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LargeManifestIssue {
    OidMismatch { expected: String, actual: String },
    SizeMismatch { expected: u64, actual: u64 },
    ChunkCountMismatch { expected: usize, actual: usize },
}

pub fn large_manifest_issues(
    actual: &majutsu_core::LargeManifest,
    expected: &LargeObjectExport,
) -> Vec<LargeManifestIssue> {
    let mut issues = Vec::new();
    if actual.oid != expected.oid {
        issues.push(LargeManifestIssue::OidMismatch {
            expected: expected.oid.clone(),
            actual: actual.oid.clone(),
        });
    }
    if actual.size != expected.size {
        issues.push(LargeManifestIssue::SizeMismatch {
            expected: expected.size,
            actual: actual.size,
        });
    }
    if actual.chunks.len() != expected.chunk_count {
        issues.push(LargeManifestIssue::ChunkCountMismatch {
            expected: expected.chunk_count,
            actual: actual.chunks.len(),
        });
    }
    issues
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargePinExport {
    pub oid: String,
    pub pinned_at: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkExport {
    pub oid: String,
    pub size: u64,
    pub object_key: String,
}

impl ChunkExport {
    pub fn new(oid: String, size: u64, object_key: String) -> Self {
        Self {
            oid,
            size,
            object_key,
        }
    }

    pub fn matches_chunk_ref(&self, chunk: &majutsu_core::LargeChunk) -> bool {
        self.oid == chunk.oid && self.size == chunk.stored_len.unwrap_or(chunk.len)
    }
}

impl LargePinExport {
    pub fn new(oid: String, pinned_at: DateTime<Utc>, reason: Option<String>) -> Self {
        Self {
            oid,
            pinned_at: pinned_at.to_rfc3339(),
            reason,
        }
    }

    pub fn pinned_time(&self) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(&self.pinned_at)?.with_timezone(&Utc))
    }

    pub fn references_known_object(&self, large_objects: &[LargeObjectExport]) -> bool {
        large_objects.iter().any(|large| large.oid == self.oid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LargePinIssue {
    Dangling { oid: String, pinned_at: String },
    InvalidTimestamp { oid: String, pinned_at: String },
}

pub fn large_pin_issues(
    pins: &[LargePinExport],
    large_objects: &[LargeObjectExport],
) -> Vec<LargePinIssue> {
    let mut issues = Vec::new();
    for pin in pins {
        if !pin.references_known_object(large_objects) {
            issues.push(LargePinIssue::Dangling {
                oid: pin.oid.clone(),
                pinned_at: pin.pinned_at.clone(),
            });
        }
        if pin.pinned_time().is_err() {
            issues.push(LargePinIssue::InvalidTimestamp {
                oid: pin.oid.clone(),
                pinned_at: pin.pinned_at.clone(),
            });
        }
    }
    issues
}

pub fn large_chunk_hash_matches(chunk: &majutsu_core::LargeChunk, bytes: &[u8]) -> bool {
    blake3::hash(bytes).to_hex().as_str() == chunk.oid
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

pub fn validate_chunking(chunking: &str) -> Result<()> {
    match chunking {
        "fixed" | "fastcdc" => Ok(()),
        _ => anyhow::bail!("large chunking must be fixed or fastcdc"),
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

pub fn compress_chunk_if_useful(input: CompressionInput<'_>) -> Result<StoredLargeChunk> {
    if !should_compress(
        input.enabled,
        input.algorithm,
        input.skip_extensions,
        input.file_name,
    ) {
        return Ok(StoredLargeChunk {
            bytes: input.bytes.to_vec(),
            compression: "none".into(),
        });
    }
    let sample_len = input.sample_bytes.min(input.bytes.len());
    if sample_len > 0 && sample_len < input.bytes.len() {
        let sample = &input.bytes[..sample_len];
        let compressed_sample = zstd::stream::encode_all(sample, input.level)?;
        let sample_gain = compression_gain(sample.len(), compressed_sample.len());
        if sample_gain < input.min_gain_ratio {
            return Ok(StoredLargeChunk {
                bytes: input.bytes.to_vec(),
                compression: "none".into(),
            });
        }
    }
    let compressed = zstd::stream::encode_all(input.bytes, input.level)?;
    let gain = compression_gain(input.bytes.len(), compressed.len());
    if gain >= input.min_gain_ratio {
        Ok(StoredLargeChunk {
            bytes: compressed,
            compression: "zstd".into(),
        })
    } else {
        Ok(StoredLargeChunk {
            bytes: input.bytes.to_vec(),
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

pub fn default_chunked_min_size() -> u64 {
    512 * 1024
}

pub fn default_chunked_chunk_size() -> usize {
    64 * 1024
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

        let stored = compress_chunk_if_useful(CompressionInput {
            bytes: &bytes,
            enabled: true,
            algorithm: "zstd",
            level: 3,
            sample_bytes: 2048,
            min_gain_ratio: 0.05,
            skip_extensions: &[],
            file_name: "payload.dat",
        })
        .unwrap();

        assert_eq!(stored.compression, "none");
        assert_eq!(stored.bytes, bytes);
    }

    #[test]
    fn compression_sample_allows_useful_full_chunk_compression() {
        let bytes = vec![b'a'; 32 * 1024];

        let stored = compress_chunk_if_useful(CompressionInput {
            bytes: &bytes,
            enabled: true,
            algorithm: "zstd",
            level: 3,
            sample_bytes: 2048,
            min_gain_ratio: 0.05,
            skip_extensions: &[],
            file_name: "payload.dat",
        })
        .unwrap();

        assert_eq!(stored.compression, "zstd");
        assert!(stored.bytes.len() < bytes.len());
    }

    #[test]
    fn defaults_match_large_pipeline_spec() {
        assert_eq!(default_large_min_size(), 64 * 1024 * 1024);
        assert_eq!(default_large_binary_min_size(), 16 * 1024 * 1024);
        assert_eq!(default_chunked_min_size(), 512 * 1024);
        assert_eq!(default_chunked_chunk_size(), 64 * 1024);
        assert_eq!(default_chunk_size(), 8 * 1024 * 1024);
        assert_eq!(default_chunking(), "fixed");
        assert_eq!(default_max_parallel_uploads(), 8);
        assert_eq!(default_compression_algorithm(), "zstd");
        assert_eq!(default_compression_level(), 3);
        assert_eq!(default_compression_sample_bytes(), 1024 * 1024);
        assert_eq!(default_compression_min_gain_ratio(), 0.05);
        validate_chunking(default_chunking()).unwrap();
        validate_chunking("fastcdc").unwrap();
        assert_eq!(
            validate_chunking("rolling").unwrap_err().to_string(),
            "large chunking must be fixed or fastcdc"
        );
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

    #[test]
    fn large_export_json_shape_is_stable() {
        let large = LargeObjectExport {
            oid: "large-1".into(),
            size: 1024,
            chunk_count: 2,
            manifest_key: "objects/large/manifests/large-1".into(),
        };

        let value = serde_json::to_value(large).unwrap();

        assert_eq!(value["oid"], "large-1");
        assert_eq!(value["size"], 1024);
        assert_eq!(value["chunk_count"], 2);
        assert_eq!(value["manifest_key"], "objects/large/manifests/large-1");
    }

    #[test]
    fn large_manifest_validation_reports_export_metadata_drift() {
        let manifest = majutsu_core::LargeManifest {
            version: 1,
            oid: "actual".into(),
            size: 11,
            media_type: None,
            binary: true,
            chunking: "fixed".into(),
            chunk_size: 8,
            chunks: vec![majutsu_core::LargeChunk {
                index: 0,
                offset: 0,
                len: 11,
                stored_len: None,
                compression: "none".into(),
                oid: "chunk-a".into(),
                object_key: "objects/chunks/chunk-a".into(),
            }],
        };
        let export = LargeObjectExport {
            oid: "expected".into(),
            size: 10,
            chunk_count: 2,
            manifest_key: "objects/large/expected.json".into(),
        };

        assert_eq!(
            large_manifest_issues(&manifest, &export),
            vec![
                LargeManifestIssue::OidMismatch {
                    expected: "expected".into(),
                    actual: "actual".into(),
                },
                LargeManifestIssue::SizeMismatch {
                    expected: 10,
                    actual: 11,
                },
                LargeManifestIssue::ChunkCountMismatch {
                    expected: 2,
                    actual: 1,
                },
            ]
        );
    }

    #[test]
    fn large_pin_tracks_time_and_referenced_object() {
        let large = LargeObjectExport {
            oid: "large-1".into(),
            size: 1024,
            chunk_count: 2,
            manifest_key: "objects/large/manifests/large-1".into(),
        };
        let pinned_at = DateTime::<Utc>::UNIX_EPOCH;
        let pin = LargePinExport::new("large-1".into(), pinned_at, Some("manual".into()));

        assert_eq!(pin.pinned_time().unwrap(), pinned_at);
        assert!(pin.references_known_object(std::slice::from_ref(&large)));
        assert!(
            !LargePinExport {
                oid: "missing".into(),
                pinned_at: pinned_at.to_rfc3339(),
                reason: None,
            }
            .references_known_object(&[large])
        );
    }

    #[test]
    fn large_pin_rejects_invalid_timestamp() {
        let pin = LargePinExport {
            oid: "large-1".into(),
            pinned_at: "not-a-time".into(),
            reason: None,
        };

        assert!(pin.pinned_time().is_err());
    }

    #[test]
    fn large_pin_validation_reports_dangling_and_invalid_pins() {
        let objects = vec![LargeObjectExport {
            oid: "large-1".into(),
            size: 1024,
            chunk_count: 1,
            manifest_key: "objects/large/manifests/large-1".into(),
        }];
        let pins = vec![
            LargePinExport {
                oid: "missing".into(),
                pinned_at: DateTime::<Utc>::UNIX_EPOCH.to_rfc3339(),
                reason: None,
            },
            LargePinExport {
                oid: "large-1".into(),
                pinned_at: "not-time".into(),
                reason: None,
            },
        ];

        assert_eq!(
            large_pin_issues(&pins, &objects),
            vec![
                LargePinIssue::Dangling {
                    oid: "missing".into(),
                    pinned_at: DateTime::<Utc>::UNIX_EPOCH.to_rfc3339(),
                },
                LargePinIssue::InvalidTimestamp {
                    oid: "large-1".into(),
                    pinned_at: "not-time".into(),
                },
            ]
        );
    }

    #[test]
    fn large_chunk_hash_validation_uses_decompressed_bytes() {
        let bytes = b"payload";
        let oid = blake3::hash(bytes).to_hex().to_string();
        let chunk = majutsu_core::LargeChunk {
            index: 0,
            offset: 0,
            len: bytes.len() as u64,
            stored_len: None,
            compression: "none".into(),
            oid,
            object_key: "objects/large/chunks/chunk-1".into(),
        };

        assert!(large_chunk_hash_matches(&chunk, bytes));
        assert!(!large_chunk_hash_matches(&chunk, b"changed"));
    }

    #[test]
    fn chunk_export_json_shape_and_chunk_ref_match_are_stable() {
        let export = ChunkExport::new("chunk-1".into(), 64, "objects/large/chunks/chunk-1".into());
        let chunk = majutsu_core::LargeChunk {
            index: 0,
            offset: 0,
            len: 128,
            stored_len: Some(64),
            compression: "zstd".into(),
            oid: "chunk-1".into(),
            object_key: "objects/large/chunks/chunk-1".into(),
        };

        let value = serde_json::to_value(&export).unwrap();

        assert_eq!(value["oid"], "chunk-1");
        assert_eq!(value["size"], 64);
        assert_eq!(value["object_key"], "objects/large/chunks/chunk-1");
        assert!(export.matches_chunk_ref(&chunk));
    }
}
