use anyhow::Result;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::cli::{RootAddArgs, RootSetArgs};
use crate::config::{Config, LargeConfig, RootConfig, RootLargeConfig, validate_large_chunking};
use crate::util::path_to_slash;

pub fn build_ignore(root: &RootConfig) -> Result<Gitignore> {
    let mut builder = GitignoreBuilder::new(&root.path);
    for pattern in &root.exclude {
        builder.add_line(None, pattern)?;
    }
    Ok(builder.build()?)
}

pub fn is_included(patterns: &[String], rel: &Path) -> bool {
    if patterns.is_empty() {
        return true;
    }
    let rel = path_to_slash(rel);
    patterns
        .iter()
        .any(|pattern| path_pattern_match(pattern, &rel))
}

pub fn is_ignored(ignore: &Gitignore, rel: &Path, is_dir: bool) -> bool {
    ignore.matched_path_or_any_parents(rel, is_dir).is_ignore()
}

pub fn effective_large_config(config: &Config, root: &RootConfig) -> LargeConfig {
    let mut large = LargeConfig {
        enabled: config.large.enabled,
        min_size: config.large.min_size,
        binary_min_size: config.large.binary_min_size,
        default_chunking: config.large.default_chunking.clone(),
        chunk_size: config.large.chunk_size,
        max_parallel_uploads: config.large.max_parallel_uploads,
        multipart: config.large.multipart,
        always: config.large.always.clone(),
        never: config.large.never.clone(),
        compression: config.large.compression.clone(),
    };
    if let Some(root_large) = &root.large {
        if let Some(min_size) = root_large.min_size {
            large.min_size = min_size;
        }
        if let Some(binary_min_size) = root_large.binary_min_size {
            large.binary_min_size = binary_min_size;
        }
        if let Some(default_chunking) = &root_large.default_chunking {
            large.default_chunking = default_chunking.clone();
        }
        if let Some(chunk_size) = root_large.chunk_size {
            large.chunk_size = chunk_size;
        }
        if !root_large.always.is_empty() {
            large.always = root_large.always.clone();
        }
        if !root_large.never.is_empty() {
            large.never = root_large.never.clone();
        }
    }
    large
}

pub fn classify_large(config: &LargeConfig, rel: &Path, size: u64, binary: bool) -> bool {
    if !config.enabled {
        return false;
    }
    let name = rel.file_name().and_then(OsStr::to_str).unwrap_or_default();
    if config.never.iter().any(|p| glob_match(p, name)) {
        return false;
    }
    if config.always.iter().any(|p| glob_match(p, name)) {
        return true;
    }
    size >= config.min_size || (binary && size >= config.binary_min_size)
}

fn glob_match(pattern: &str, name: &str) -> bool {
    if let Some(ext) = pattern.strip_prefix("*.") {
        return name
            .to_ascii_lowercase()
            .ends_with(&format!(".{}", ext.to_ascii_lowercase()));
    }
    pattern == name
}

fn path_pattern_match(pattern: &str, rel: &str) -> bool {
    if pattern == "**" || pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix("/**") {
        return rel == prefix || rel.starts_with(&format!("{prefix}/"));
    }
    if let Some(ext) = pattern.strip_prefix("*.") {
        return rel
            .to_ascii_lowercase()
            .ends_with(&format!(".{}", ext.to_ascii_lowercase()));
    }
    if let Some(suffix) = pattern.strip_prefix("**/") {
        if let Some(middle) = suffix.strip_suffix("/**") {
            return rel == middle
                || rel.starts_with(&format!("{middle}/"))
                || rel.contains(&format!("/{middle}/"));
        }
        return rel == suffix || rel.ends_with(&format!("/{suffix}"));
    }
    rel == pattern || rel.starts_with(&format!("{pattern}/"))
}

pub fn root_large_override(args: &RootAddArgs) -> Option<RootLargeConfig> {
    if args.large_min_size.is_none()
        && args.large_binary_min_size.is_none()
        && args.large_chunk_size.is_none()
        && args.large_chunking.is_none()
        && args.large_always.is_empty()
        && args.large_never.is_empty()
    {
        return None;
    }
    Some(RootLargeConfig {
        min_size: args.large_min_size,
        binary_min_size: args.large_binary_min_size,
        default_chunking: args.large_chunking.clone(),
        chunk_size: args.large_chunk_size,
        always: args.large_always.clone(),
        never: args.large_never.clone(),
    })
}

pub fn apply_root_large_set(root: &mut RootConfig, args: &RootSetArgs) -> Result<()> {
    if let Some(chunking) = &args.large_chunking {
        validate_large_chunking(chunking)?;
    }
    if args.clear_large_policy {
        root.large = None;
    }
    let wants_large = args.large_min_size.is_some()
        || args.large_binary_min_size.is_some()
        || args.large_chunk_size.is_some()
        || args.large_chunking.is_some()
        || !args.large_always.is_empty()
        || !args.large_never.is_empty()
        || args.clear_large_always
        || args.clear_large_never;
    if !wants_large {
        return Ok(());
    }
    let large = root.large.get_or_insert_with(|| RootLargeConfig {
        min_size: None,
        binary_min_size: None,
        default_chunking: None,
        chunk_size: None,
        always: Vec::new(),
        never: Vec::new(),
    });
    if let Some(min_size) = args.large_min_size {
        large.min_size = Some(min_size);
    }
    if let Some(binary_min_size) = args.large_binary_min_size {
        large.binary_min_size = Some(binary_min_size);
    }
    if let Some(chunk_size) = args.large_chunk_size {
        large.chunk_size = Some(chunk_size);
    }
    if let Some(chunking) = &args.large_chunking {
        large.default_chunking = Some(chunking.clone());
    }
    if args.clear_large_always {
        large.always.clear();
    }
    large.always.extend(args.large_always.clone());
    if args.clear_large_never {
        large.never.clear();
    }
    large.never.extend(args.large_never.clone());
    Ok(())
}

pub fn looks_binary(path: &Path) -> Result<bool> {
    let mut f = File::open(path)?;
    let mut buf = [0u8; 8192];
    let n = f.read(&mut buf)?;
    Ok(buf[..n].contains(&0))
}

pub fn large_pointer_compression(config: &LargeConfig) -> String {
    if config.compression.enabled {
        format!("per-chunk:{}", config.compression.algorithm)
    } else {
        "none".into()
    }
}
