use anyhow::{Result, bail};
use globset::GlobBuilder;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::cli::{RootAddArgs, RootSetArgs};
use crate::config::{
    Config, LargeConfig, RootConfig, RootLargeConfig, RootVolatileConfig, validate_large_chunking,
};
use crate::util::path_to_slash;

pub fn build_ignore(root: &RootConfig) -> Result<Gitignore> {
    let mut builder = GitignoreBuilder::new(&root.path);
    for pattern in &root.exclude {
        builder.add_line(None, pattern)?;
        for expanded in expanded_directory_exclude_patterns(pattern) {
            builder.add_line(None, &expanded)?;
        }
    }
    Ok(builder.build()?)
}

pub fn expanded_directory_exclude_patterns(pattern: &str) -> Vec<String> {
    let pattern = pattern.trim();
    let Some(dir_pattern) = pattern.strip_suffix("/**") else {
        return Vec::new();
    };
    let base = dir_pattern.trim_end_matches('/');
    if base.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    push_unique(&mut out, base.to_string());
    let unanchored = base.trim_start_matches('/');
    if !unanchored.is_empty() {
        push_unique(&mut out, unanchored.to_string());
    }
    if let Some(inner) = unanchored.strip_prefix("**/") {
        if !inner.is_empty() {
            push_unique(&mut out, inner.to_string());
            push_unique(&mut out, format!("/{inner}"));
        }
    } else if !unanchored.is_empty() {
        push_unique(&mut out, format!("**/{unanchored}"));
    }
    out
}

pub fn root_preset_excludes(preset: &str) -> Result<Vec<String>> {
    match preset {
        "default" | "best-practice" => Ok(default_root_excludes()),
        "git" | "git-working-tree" => Ok(vec![
            ".git/**",
            "/.git/**",
            "**/.git/**",
            "node_modules/**",
            "/node_modules/**",
            "**/node_modules/**",
            "target/**",
            "/target/**",
            "**/target/**",
            "tmp/**",
            "/tmp/**",
            ".infracost/**",
            "/.infracost/**",
            ".backup-kubeconfig/**",
            "/.backup-kubeconfig/**",
            ".kubeconfig*",
            "/.kubeconfig*",
            "etc/keys/**",
            "/etc/keys/**",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()),
        "rust" => Ok(vec!["target/**", "/target/**", "**/target/**"]
            .into_iter()
            .map(str::to_string)
            .collect()),
        "node" => Ok(
            vec!["node_modules/**", "/node_modules/**", "**/node_modules/**"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        other => {
            bail!(
                "unknown root preset {other}; supported presets: default, git-working-tree, rust, node"
            )
        }
    }
}

pub fn default_root_excludes() -> Vec<String> {
    [
        ".git/**",
        "/.git/**",
        "**/.git/**",
        ".hg/**",
        "/.hg/**",
        "**/.hg/**",
        ".svn/**",
        "/.svn/**",
        "**/.svn/**",
        ".jj/**",
        "/.jj/**",
        "**/.jj/**",
        "node_modules/**",
        "/node_modules/**",
        "**/node_modules/**",
        "target/**",
        "/target/**",
        "**/target/**",
        "build/**",
        "/build/**",
        "**/build/**",
        "dist/**",
        "/dist/**",
        "**/dist/**",
        "out/**",
        "/out/**",
        "**/out/**",
        "tmp/**",
        "/tmp/**",
        "**/tmp/**",
        "*.tmp",
        "**/*.tmp",
        "tmp_*",
        "**/tmp_*",
        "temp/**",
        "/temp/**",
        "**/temp/**",
        ".cache/**",
        "/.cache/**",
        "**/.cache/**",
        ".next/**",
        "/.next/**",
        "**/.next/**",
        ".nuxt/**",
        "/.nuxt/**",
        "**/.nuxt/**",
        ".svelte-kit/**",
        "/.svelte-kit/**",
        "**/.svelte-kit/**",
        ".turbo/**",
        "/.turbo/**",
        "**/.turbo/**",
        ".parcel-cache/**",
        "/.parcel-cache/**",
        "**/.parcel-cache/**",
        ".vite/**",
        "/.vite/**",
        "**/.vite/**",
        "coverage/**",
        "/coverage/**",
        "**/coverage/**",
        "__pycache__/**",
        "/__pycache__/**",
        "**/__pycache__/**",
        ".pytest_cache/**",
        "/.pytest_cache/**",
        "**/.pytest_cache/**",
        ".mypy_cache/**",
        "/.mypy_cache/**",
        "**/.mypy_cache/**",
        ".ruff_cache/**",
        "/.ruff_cache/**",
        "**/.ruff_cache/**",
        ".tox/**",
        "/.tox/**",
        "**/.tox/**",
        ".venv/**",
        "/.venv/**",
        "**/.venv/**",
        "venv/**",
        "/venv/**",
        "**/venv/**",
        ".DS_Store",
        "**/.DS_Store",
        "Thumbs.db",
        "**/Thumbs.db",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn apply_default_root_excludes(excludes: &mut Vec<String>) {
    for pattern in default_root_excludes() {
        push_unique(excludes, pattern);
    }
    dedup_patterns(excludes);
}

pub fn apply_root_presets(excludes: &mut Vec<String>, presets: &[String]) -> Result<()> {
    for preset in presets {
        for pattern in root_preset_excludes(preset)? {
            push_unique(excludes, pattern);
        }
    }
    dedup_patterns(excludes);
    Ok(())
}

pub fn warn_sensitive_root_defaults(root_path: &Path, excludes: &[String]) {
    for sensitive in [".git", ".infracost", ".backup-kubeconfig", "etc/keys"] {
        if root_path.join(sensitive).exists() && !exclude_covers_path(excludes, sensitive) {
            eprintln!(
                "warning: root contains {sensitive}; use encryption or add an explicit --exclude if this should not be backed up"
            );
        }
    }
    if !exclude_covers_path(excludes, ".kubeconfig") {
        let has_kubeconfig = std::fs::read_dir(root_path)
            .ok()
            .into_iter()
            .flat_map(|entries| entries.flatten())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .any(|name| name.starts_with(".kubeconfig"));
        if has_kubeconfig {
            eprintln!(
                "warning: root contains .kubeconfig*; use encryption or add an explicit --exclude if this should not be backed up"
            );
        }
    }
}

fn exclude_covers_path(excludes: &[String], rel: &str) -> bool {
    excludes.iter().any(|pattern| {
        let pattern = pattern.trim();
        pattern == rel
            || pattern == format!("/{rel}")
            || pattern == format!("{rel}/**")
            || pattern == format!("/{rel}/**")
            || pattern == format!("**/{rel}/**")
            || path_pattern_match(pattern, rel)
    })
}

fn push_unique(out: &mut Vec<String>, value: String) {
    if !value.is_empty() && !out.iter().any(|existing| existing == &value) {
        out.push(value);
    }
}

pub fn dedup_patterns(patterns: &mut Vec<String>) {
    let mut deduped = Vec::with_capacity(patterns.len());
    for pattern in patterns.drain(..) {
        push_unique(&mut deduped, pattern);
    }
    *patterns = deduped;
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

pub fn explicitly_included(patterns: &[String], rel: &Path) -> bool {
    if patterns.is_empty() {
        return false;
    }
    let rel = path_to_slash(rel);
    patterns
        .iter()
        .filter(|pattern| !is_catch_all_include_pattern(pattern))
        .any(|pattern| path_pattern_match(pattern, &rel))
}

pub fn include_may_match_inside_dir(patterns: &[String], rel: &Path) -> bool {
    if patterns.is_empty()
        || patterns
            .iter()
            .all(|pattern| is_catch_all_include_pattern(pattern))
    {
        return false;
    }
    let dir = path_to_slash(rel);
    let dir = dir.trim_matches('/');
    if dir.is_empty() {
        return true;
    }
    patterns
        .iter()
        .any(|pattern| include_pattern_may_match_inside_dir(pattern, dir))
}

pub fn include_allows_descend(patterns: &[String], rel: &Path) -> bool {
    if patterns.is_empty()
        || patterns
            .iter()
            .all(|pattern| is_catch_all_include_pattern(pattern))
    {
        return true;
    }
    is_included(patterns, rel) || include_may_match_inside_dir(patterns, rel)
}

fn is_catch_all_include_pattern(pattern: &str) -> bool {
    matches!(pattern.trim().trim_start_matches('/'), "**" | "*")
}

pub fn is_ignored(ignore: &Gitignore, rel: &Path, is_dir: bool) -> bool {
    ignore.matched_path_or_any_parents(rel, is_dir).is_ignore()
}

pub fn explicitly_tracked(root: &RootConfig, rel: &Path) -> bool {
    path_list_covers(&root.explicit_track, rel)
}

pub fn explicitly_untracked(root: &RootConfig, rel: &Path) -> bool {
    path_list_covers(&root.explicit_untrack, rel)
}

pub fn explicit_track_may_match_inside_dir(root: &RootConfig, rel: &Path) -> bool {
    path_list_may_match_inside_dir(&root.explicit_track, rel)
}

pub fn root_record_is_managed(
    root: &RootConfig,
    ignore: &Gitignore,
    rel: &Path,
    is_dir: bool,
) -> bool {
    if explicitly_untracked(root, rel) {
        return false;
    }
    if explicitly_tracked(root, rel) {
        return true;
    }
    if !is_included(&root.include, rel) {
        return false;
    }
    if is_volatile_excluded(root, rel) {
        return false;
    }
    !is_ignored(ignore, rel, is_dir) || explicitly_included(&root.include, rel)
}

pub fn root_dir_allows_descend(root: &RootConfig, ignore: &Gitignore, rel: &Path) -> bool {
    if explicitly_untracked(root, rel) {
        return false;
    }
    if include_allows_descend(&root.include, rel) && !is_volatile_excluded(root, rel) {
        if !is_ignored(ignore, rel, true) {
            return true;
        }
        if include_may_match_inside_dir(&root.include, rel) {
            return true;
        }
    }
    explicit_track_may_match_inside_dir(root, rel) || !root.explicit_track.is_empty()
}

fn path_list_covers(patterns: &[String], rel: &Path) -> bool {
    if patterns.is_empty() {
        return false;
    }
    let rel = path_to_slash(rel);
    let rel = rel.trim_matches('/');
    patterns.iter().any(|pattern| {
        let pattern = pattern.trim().trim_matches('/');
        !pattern.is_empty()
            && (pattern == rel
                || rel
                    .strip_prefix(pattern)
                    .is_some_and(|suffix| suffix.starts_with('/'))
                || path_pattern_match(pattern, rel))
    })
}

fn path_list_may_match_inside_dir(patterns: &[String], rel: &Path) -> bool {
    if patterns.is_empty() {
        return false;
    }
    let dir = path_to_slash(rel);
    let dir = dir.trim_matches('/');
    patterns.iter().any(|pattern| {
        let pattern = pattern.trim().trim_matches('/');
        !pattern.is_empty()
            && (pattern
                .strip_prefix(dir)
                .is_some_and(|suffix| suffix.starts_with('/'))
                || include_pattern_may_match_inside_dir(pattern, dir))
    })
}

pub fn validate_volatile_mode(mode: &str) -> Result<()> {
    match mode {
        "checkpoint" | "exclude" => Ok(()),
        other => bail!("unsupported volatile mode {other}; supported modes: checkpoint, exclude"),
    }
}

pub fn root_volatile_override(args: &RootAddArgs) -> Result<Option<RootVolatileConfig>> {
    validate_volatile_mode(&args.volatile_mode)?;
    if args.volatile.is_empty() && args.volatile_mode != "checkpoint" {
        bail!("--volatile-mode requires at least one --volatile pattern");
    }
    if args.volatile.is_empty() && args.volatile_mode == "checkpoint" {
        return Ok(None);
    }
    Ok(Some(RootVolatileConfig {
        patterns: args.volatile.clone(),
        mode: args.volatile_mode.clone(),
    }))
}

pub fn apply_root_volatile_set(root: &mut RootConfig, args: &RootSetArgs) -> Result<()> {
    if args.clear_volatile {
        root.volatile = None;
    }
    if let Some(mode) = &args.volatile_mode {
        validate_volatile_mode(mode)?;
        let volatile = root.volatile.get_or_insert_with(|| RootVolatileConfig {
            patterns: Vec::new(),
            mode: "checkpoint".into(),
        });
        volatile.mode = mode.clone();
    }
    if !args.volatile.is_empty() {
        let volatile = root.volatile.get_or_insert_with(|| RootVolatileConfig {
            patterns: Vec::new(),
            mode: "checkpoint".into(),
        });
        volatile.patterns.extend(args.volatile.clone());
        dedup_patterns(&mut volatile.patterns);
    }
    if let Some(volatile) = &root.volatile
        && volatile.patterns.is_empty()
    {
        root.volatile = None;
    }
    Ok(())
}

pub fn is_volatile(root: &RootConfig, rel: &Path) -> bool {
    let Some(volatile) = &root.volatile else {
        return false;
    };
    let rel = path_to_slash(rel);
    volatile
        .patterns
        .iter()
        .any(|pattern| path_pattern_match(pattern, &rel))
}

pub fn is_volatile_excluded(root: &RootConfig, rel: &Path) -> bool {
    root.volatile
        .as_ref()
        .is_some_and(|volatile| volatile.mode == "exclude")
        && is_volatile(root, rel)
}

pub fn volatile_allows_watch_snapshot(root: &RootConfig, rel: &Path) -> bool {
    !is_volatile(root, rel)
}

pub fn effective_large_config(config: &Config, root: &RootConfig) -> LargeConfig {
    let mut large = LargeConfig {
        enabled: config.large.enabled,
        min_size: config.large.min_size,
        binary_min_size: config.large.binary_min_size,
        chunked_min_size: config.large.chunked_min_size,
        chunked_chunk_size: config.large.chunked_chunk_size,
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
        if let Some(chunked_min_size) = root_large.chunked_min_size {
            large.chunked_min_size = chunked_min_size;
        }
        if let Some(chunked_chunk_size) = root_large.chunked_chunk_size {
            large.chunked_chunk_size = chunked_chunk_size;
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
    let pattern = pattern.trim().trim_start_matches('/');
    if pattern.is_empty() {
        return false;
    }
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
    if pattern_has_glob_meta(pattern) && glob_path_match(pattern, rel) {
        return true;
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

fn include_pattern_may_match_inside_dir(pattern: &str, dir: &str) -> bool {
    let pattern = pattern.trim().trim_start_matches('/');
    if pattern.is_empty() {
        return false;
    }
    if pattern == "**" || pattern == "*" || pattern.starts_with("**/") {
        return true;
    }
    if path_pattern_match(pattern, dir) {
        return true;
    }
    if pattern_has_glob_meta(pattern) && glob_pattern_may_match_inside_dir(pattern, dir) {
        return true;
    }
    let literal_prefix = pattern
        .split(['*', '?', '['])
        .next()
        .unwrap_or_default()
        .trim_start_matches('/')
        .trim_end_matches('/');
    if literal_prefix.is_empty() {
        return true;
    }
    literal_prefix == dir
        || literal_prefix.starts_with(&format!("{dir}/"))
        || dir.starts_with(&format!("{literal_prefix}/"))
}

fn pattern_has_glob_meta(pattern: &str) -> bool {
    pattern
        .as_bytes()
        .iter()
        .any(|byte| matches!(byte, b'*' | b'?' | b'[' | b'{'))
}

fn glob_path_match(pattern: &str, rel: &str) -> bool {
    GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(true)
        .build()
        .map(|glob| glob.compile_matcher().is_match(rel))
        .unwrap_or(false)
}

fn glob_pattern_may_match_inside_dir(pattern: &str, dir: &str) -> bool {
    let pattern_parts = pattern
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let dir_parts = dir
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if pattern_parts.len() <= dir_parts.len() {
        return false;
    }
    for (pattern_part, dir_part) in pattern_parts.iter().zip(dir_parts.iter()) {
        if *pattern_part == "**" {
            return true;
        }
        if !glob_path_match(pattern_part, dir_part) {
            return false;
        }
    }
    true
}

pub fn root_large_override(args: &RootAddArgs) -> Option<RootLargeConfig> {
    if args.large_min_size.is_none()
        && args.large_binary_min_size.is_none()
        && args.large_chunked_min_size.is_none()
        && args.large_chunked_chunk_size.is_none()
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
        chunked_min_size: args.large_chunked_min_size,
        chunked_chunk_size: args.large_chunked_chunk_size,
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
        || args.large_chunked_min_size.is_some()
        || args.large_chunked_chunk_size.is_some()
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
        chunked_min_size: None,
        chunked_chunk_size: None,
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
    if let Some(chunked_min_size) = args.large_chunked_min_size {
        large.chunked_min_size = Some(chunked_min_size);
    }
    if let Some(chunked_chunk_size) = args.large_chunked_chunk_size {
        large.chunked_chunk_size = Some(chunked_chunk_size);
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

#[cfg(test)]
mod moon_root_tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn expands_directory_child_globs_to_directory_entries() {
        let expanded = expanded_directory_exclude_patterns("**/.git/**");
        assert!(expanded.contains(&".git".to_string()));
        assert!(expanded.contains(&"/.git".to_string()));
        assert!(expanded.contains(&"**/.git".to_string()));
    }

    #[test]
    fn git_working_tree_preset_covers_moon_sensitive_paths() {
        let mut excludes = Vec::new();
        apply_root_presets(&mut excludes, &["git-working-tree".into()]).unwrap();
        for path in [".git", ".infracost", ".backup-kubeconfig", "etc/keys"] {
            assert!(
                exclude_covers_path(&excludes, path),
                "preset should cover {path}"
            );
        }
        assert!(excludes.iter().any(|pattern| pattern == ".kubeconfig*"));
        assert!(is_included(&["**".into()], Path::new("src/main.rs")));
    }

    #[test]
    fn default_root_excludes_cover_reproducible_subtrees_not_secrets() {
        let mut excludes = Vec::new();
        apply_default_root_excludes(&mut excludes);
        for path in [
            ".git",
            ".hg",
            ".svn",
            ".jj",
            "node_modules",
            "target",
            ".venv",
            "state.json.123.abc.tmp",
            "work/state.json.123.abc.tmp",
            "tmp_runtime.1",
            "work/tmp_runtime.1",
        ] {
            assert!(
                exclude_covers_path(&excludes, path),
                "default excludes should cover {path}"
            );
        }
        assert!(
            !exclude_covers_path(&excludes, ".env"),
            "default excludes must not silently drop authored secret files"
        );
        assert!(
            !exclude_covers_path(&excludes, ".kubeconfig"),
            "default excludes must warn about credentials instead of dropping them"
        );
    }

    #[test]
    fn explicit_track_allows_descending_into_excluded_parent() {
        let root_path = tempfile::tempdir().unwrap();
        let root = RootConfig {
            id: "sample".into(),
            name: "sample".into(),
            path: root_path.path().to_path_buf(),
            include: vec!["**".into()],
            exclude: vec!["ignored/**".into()],
            explicit_track: vec!["ignored/keep.txt".into()],
            explicit_untrack: Vec::new(),
            follow_symlinks: false,
            require_mount: false,
            status: "active".into(),
            degraded: None,
            snapshot_mode: "default".into(),
            pre_snapshot: None,
            post_snapshot: None,
            snapshot_source: None,
            application_plugin: None,
            large: None,
            volatile: None,
        };
        let ignore = build_ignore(&root).unwrap();
        assert!(root_dir_allows_descend(
            &root,
            &ignore,
            Path::new("ignored")
        ));
        assert!(root_record_is_managed(
            &root,
            &ignore,
            Path::new("ignored/keep.txt"),
            false
        ));
    }
}
