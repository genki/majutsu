use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt, symlink};
use std::process::{Command, Stdio};
use std::thread;
#[cfg(unix)]
use std::time::UNIX_EPOCH;
use std::time::{Duration, Instant};

fn mj() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mj"));
    command.env("MAJUTSU_AUTO_DAEMON", "0");
    command
}

#[allow(dead_code)]
fn mj_auto() -> Command {
    Command::new(env!("CARGO_BIN_EXE_mj"))
}

fn run(mut command: Command) {
    let output = command.output().expect("run command");
    if !output.status.success() {
        panic!(
            "command failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn output(mut command: Command) -> String {
    let output = command.output().expect("run command");
    if !output.status.success() {
        panic!(
            "command failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn synced_object_count(output: &str) -> usize {
    output
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix("synced ")?;
            rest.split_whitespace().next()?.parse().ok()
        })
        .unwrap_or_else(|| panic!("missing synced object count in output:\n{output}"))
}

fn output_metric(output: &str, name: &str) -> usize {
    output
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix(name)?.trim_start();
            rest.split_whitespace().next()?.parse().ok()
        })
        .unwrap_or_else(|| panic!("missing metric {name} in output:\n{output}"))
}

#[cfg(target_os = "linux")]
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(target_os = "linux")]
fn shell_quote_path(path: &std::path::Path) -> String {
    shell_quote(&path.to_string_lossy())
}

fn set_watch_backend(state: &std::path::Path, backend: &str) {
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace(
            "backend = \"fanotify\"",
            &format!("backend = \"{backend}\""),
        ),
    )
    .unwrap();
}

fn set_host_id(state: &std::path::Path, host_id: &str) {
    for name in ["config.toml", "host.toml"] {
        let path = state.join(name);
        let updated = fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| {
                if line.starts_with("id = ") {
                    format!("id = \"{host_id}\"")
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, format!("{updated}\n")).unwrap();
    }
}

fn current_host_id(state: &std::path::Path) -> String {
    let config = fs::read_to_string(state.join("config.toml")).unwrap();
    let parsed: toml::Value = toml::from_str(&config).unwrap();
    parsed["host"]["id"].as_str().unwrap().to_string()
}

#[test]
fn top_level_help_groups_commands_without_hiding_maintenance_commands() {
    let help = output({
        let mut c = mj();
        c.arg("--help");
        c
    });
    for heading in [
        "Setup:",
        "Daily use:",
        "History:",
        "Recovery:",
        "Remote:",
        "Service:",
        "Security:",
        "Storage maintenance:",
        "Advanced/debug:",
    ] {
        assert!(help.contains(heading), "{help}");
    }
    for command in ["large", "cache", "pack", "prune", "gc", "fsck"] {
        assert!(help.contains(command), "{help}");
    }
    assert!(!help.contains("maintenance "), "{help}");
}

#[cfg(unix)]
#[test]
fn cli_treats_broken_pipe_as_success() {
    let status = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "{} --help | head -n 1 >/dev/null",
            env!("CARGO_BIN_EXE_mj")
        ))
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn root_list_supports_json_and_no_truncate() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let root = tmp
        .path()
        .join("very")
        .join("long")
        .join("path")
        .join("for")
        .join("root")
        .join("list")
        .join("display");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.txt"), b"hello\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("demo")
            .arg(&root);
        c
    });

    let json = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("list")
            .arg("--json");
        c
    });
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["total"], serde_json::Value::from(1));
    assert_eq!(value["active"], serde_json::Value::from(1));
    assert_eq!(value["issues"], serde_json::Value::from(0));
    assert_eq!(value["roots"][0]["id"], "demo");
    assert_eq!(value["roots"][0]["path"], root.display().to_string());

    let narrow = output({
        let mut c = mj();
        c.env("COLUMNS", "40")
            .arg("--home")
            .arg(&state)
            .arg("root")
            .arg("list");
        c
    });
    assert!(!narrow.contains(&root.display().to_string()), "{narrow}");

    let full = output({
        let mut c = mj();
        c.env("COLUMNS", "40")
            .arg("--home")
            .arg(&state)
            .arg("root")
            .arg("list")
            .arg("--no-truncate");
        c
    });
    assert!(full.contains(&root.display().to_string()), "{full}");
}

#[test]
fn volatile_exclude_is_omitted_from_snapshots_and_root_json() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let source = tmp.path().join("source");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("app.sqlite"), b"db\n").unwrap();
    fs::write(source.join("app.sqlite-wal"), b"wal\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("db")
            .arg(&source)
            .arg("--volatile")
            .arg("**/*.sqlite-wal")
            .arg("--volatile-mode")
            .arg("exclude");
        c
    });
    let roots = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("list")
            .arg("--json");
        c
    });
    assert!(roots.contains("\"volatile\""), "{roots}");
    assert!(roots.contains("**/*.sqlite-wal"), "{roots}");
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(fs::read(restore.join("db/app.sqlite")).unwrap(), b"db\n");
    assert!(!restore.join("db/app.sqlite-wal").exists());
}

#[test]
fn explicit_include_can_reach_files_below_excluded_directories() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let root = tmp.path().join("root");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(root.join("secret")).unwrap();
    fs::write(root.join("secret").join("keep.txt"), b"keep\n").unwrap();
    fs::write(root.join("secret").join("drop.txt"), b"drop\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&root)
            .arg("--exclude")
            .arg("secret/**")
            .arg("--include")
            .arg("secret/keep.txt");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("include override");
        c
    });
    fs::remove_dir_all(&root).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read_to_string(restore.join("sample").join("secret").join("keep.txt")).unwrap(),
        "keep\n"
    );
    assert!(
        !restore
            .join("sample")
            .join("secret")
            .join("drop.txt")
            .exists()
    );
}

#[test]
fn selective_include_prunes_unmatched_dirs_and_supports_relative_globs() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let root = tmp.path().join("root");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(root.join("session-a")).unwrap();
    fs::create_dir_all(root.join("unrelated").join("nested")).unwrap();
    fs::write(root.join("app-service-prod.env"), b"service\n").unwrap();
    fs::write(root.join("app-daemon-prod.env"), b"daemon\n").unwrap();
    fs::write(
        root.join("session-a").join(".preview-context.json"),
        b"{}\n",
    )
    .unwrap();
    fs::write(
        root.join("unrelated").join("nested").join("ignored.txt"),
        b"ignored\n",
    )
    .unwrap();

    #[cfg(unix)]
    {
        fs::set_permissions(root.join("unrelated"), fs::Permissions::from_mode(0o000)).unwrap();
    }

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&root)
            .arg("--include")
            .arg("app-service*.env")
            .arg("--include")
            .arg("app-daemon*.env")
            .arg("--include")
            .arg("session-*/.preview-context.json");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("selective include");
        c
    });

    #[cfg(unix)]
    {
        fs::set_permissions(root.join("unrelated"), fs::Permissions::from_mode(0o755)).unwrap();
    }
    fs::remove_dir_all(&root).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore);
        c
    });

    let restored = restore.join("sample");
    assert_eq!(
        fs::read_to_string(restored.join("app-service-prod.env")).unwrap(),
        "service\n"
    );
    assert_eq!(
        fs::read_to_string(restored.join("app-daemon-prod.env")).unwrap(),
        "daemon\n"
    );
    assert_eq!(
        fs::read_to_string(restored.join("session-a").join(".preview-context.json")).unwrap(),
        "{}\n"
    );
    assert!(!restored.join("unrelated").exists());
}

#[test]
fn one_level_include_glob_matches_relative_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let root = tmp.path().join("root");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(root.join("session-a")).unwrap();
    fs::create_dir_all(root.join("session-a").join("nested")).unwrap();
    fs::write(root.join("session-a").join(".preview-ws.json"), b"[]\n").unwrap();
    fs::write(
        root.join("session-a")
            .join("nested")
            .join(".preview-ws.json"),
        b"ignored\n",
    )
    .unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&root)
            .arg("--include")
            .arg("*/.preview-ws.json");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("one level include");
        c
    });
    fs::remove_dir_all(&root).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore);
        c
    });

    let restored = restore.join("sample");
    assert_eq!(
        fs::read_to_string(restored.join("session-a").join(".preview-ws.json")).unwrap(),
        "[]\n"
    );
    assert!(
        !restored
            .join("session-a")
            .join("nested")
            .join(".preview-ws.json")
            .exists()
    );
}

#[test]
fn clone_quarantines_remote_hooks_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.txt"), b"safe\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--pre-snapshot")
            .arg("true")
            .arg("--post-snapshot")
            .arg("true")
            .arg("--application-plugin")
            .arg("dangerous-plugin");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let clone_out = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(
        clone_out.contains("quarantined_remote_hooks 3"),
        "{clone_out}"
    );
    let cloned_config = fs::read_to_string(clone.join("config.toml")).unwrap();
    assert!(!cloned_config.contains("pre_snapshot"), "{cloned_config}");
    assert!(!cloned_config.contains("post_snapshot"), "{cloned_config}");
    assert!(
        !cloned_config.contains("application_plugin"),
        "{cloned_config}"
    );
}

#[test]
fn commit_and_switch_top_level_aliases_follow_git_like_flow() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let committed = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("commit")
            .arg("--message")
            .arg("via commit alias");
        c
    });
    assert!(committed.contains("snapshot "), "{committed}");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("branch")
            .arg("create")
            .arg("side");
        c
    });
    let switched = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("switch").arg("side");
        c
    });
    assert!(switched.contains("switched branch side"), "{switched}");
    let current = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("branch").arg("current");
        c
    });
    assert!(current.contains("branch side"), "{current}");
}

#[cfg(unix)]
#[test]
fn snapshot_does_not_read_symlink_target_when_follow_symlinks_is_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    let outside = tmp.path().join("outside-secret.txt");
    fs::create_dir_all(&source).unwrap();
    fs::write(&outside, b"outside secret\n").unwrap();
    symlink(&outside, source.join("link")).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    let restored_link = restore.join("sample/link");
    assert_eq!(fs::read_link(&restored_link).unwrap(), outside);
}

#[cfg(unix)]
#[test]
fn restore_rejects_symlink_escape() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    let outside = tmp.path().join("outside");
    fs::create_dir_all(source.join("dir")).unwrap();
    fs::create_dir_all(&outside).unwrap();
    fs::write(source.join("dir/file.txt"), b"inside\n").unwrap();
    fs::write(outside.join("file.txt"), b"outside\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::create_dir_all(restore.join("sample")).unwrap();
    symlink(&outside, restore.join("sample/dir")).unwrap();

    let failed = {
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore)
            .arg("--force");
        c.output().unwrap()
    };
    assert!(!failed.status.success());
    let stderr = String::from_utf8_lossy(&failed.stderr);
    assert!(stderr.contains("restore target is a symlink"), "{stderr}");
    assert_eq!(fs::read(outside.join("file.txt")).unwrap(), b"outside\n");
}

#[cfg(unix)]
#[test]
fn encrypted_init_restricts_state_and_master_key_permissions() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init").arg("--encrypt");
        c
    });

    assert_eq!(
        fs::metadata(&state).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(state.join("keys"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(state.join("keys/master.key"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

fn root_list_has(root_list: &str, id: &str, status: &str) -> bool {
    root_list.lines().any(|line| {
        let mut columns = line.split_whitespace();
        columns.next() == Some(id) && columns.next() == Some(status)
    })
}

fn root_list_has_path(root_list: &str, id: &str, status: &str, path: &std::path::Path) -> bool {
    root_list.lines().any(|line| {
        let mut columns = line.split_whitespace();
        columns.next() == Some(id)
            && columns.next() == Some(status)
            && line.contains(&path.display().to_string())
    })
}

fn fails(mut command: Command) {
    let output = command.output().expect("run command");
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn find_file_ending(root: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .find(|entry| {
            entry.file_type().is_file() && entry.path().to_string_lossy().ends_with(suffix)
        })
        .map(|entry| entry.path().to_path_buf())
        .unwrap_or_else(|| panic!("missing file ending with {suffix} under {}", root.display()))
}

fn host_metadata_export_path(remote: &std::path::Path) -> std::path::PathBuf {
    walkdir::WalkDir::new(remote)
        .min_depth(3)
        .max_depth(3)
        .into_iter()
        .filter_map(Result::ok)
        .find(|entry| {
            entry.file_type().is_file()
                && entry
                    .path()
                    .strip_prefix(remote)
                    .ok()
                    .and_then(|path| path.to_str())
                    .is_some_and(|path| {
                        path.ends_with("/metadata/export.json")
                            || path.ends_with("/metadata/export.json.zst")
                    })
        })
        .map(|entry| entry.path().to_path_buf())
        .unwrap_or_else(|| panic!("missing host metadata export under {}", remote.display()))
}

fn first_remote_host_dir(remote: &std::path::Path) -> std::path::PathBuf {
    remote_host_dirs(remote)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("missing remote host directory under {}", remote.display()))
}

fn remote_host_dirs(remote: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut dirs = fs::read_dir(remote)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.join("metadata/export.json").exists()
                || path.join("metadata/export.json.zst").exists()
        })
        .collect::<Vec<_>>();
    dirs.sort();
    dirs
}

fn host_remote_path(remote: &std::path::Path, key: &str) -> std::path::PathBuf {
    first_remote_host_dir(remote).join(key)
}

fn read_json_maybe_zstd(path: &std::path::Path) -> serde_json::Value {
    let bytes = fs::read(path).unwrap();
    let decoded = if path.to_string_lossy().ends_with(".zst") {
        zstd::stream::decode_all(bytes.as_slice()).unwrap()
    } else {
        bytes
    };
    serde_json::from_slice(&decoded).unwrap()
}

fn write_json_maybe_zstd(path: &std::path::Path, value: &serde_json::Value) {
    let bytes = serde_json::to_vec_pretty(value).unwrap();
    let encoded = if path.to_string_lossy().ends_with(".zst") {
        zstd::stream::encode_all(bytes.as_slice(), 3).unwrap()
    } else {
        bytes
    };
    fs::write(path, encoded).unwrap();
}

fn read_remote_metadata(remote: &std::path::Path) -> serde_json::Value {
    read_json_maybe_zstd(&host_metadata_export_path(remote))
}

fn write_remote_metadata(remote: &std::path::Path, value: &serde_json::Value) {
    write_json_maybe_zstd(&host_metadata_export_path(remote), value)
}

fn write_remote_metadata_at_prefix(
    remote: &std::path::Path,
    prefix: &str,
    value: &serde_json::Value,
) -> std::path::PathBuf {
    let path = remote.join(prefix).join("metadata/export.json");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    write_json_maybe_zstd(&path, value);
    path
}

fn first_blob_object_key(state: &std::path::Path) -> String {
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.query_row("select object_key from blobs limit 1", [], |row| row.get(0))
        .unwrap()
}

fn remove_remote_payload_aliases(remote: &std::path::Path, key: &str) -> Vec<std::path::PathBuf> {
    let oid = key
        .rsplit('/')
        .next()
        .unwrap_or(key)
        .trim_end_matches(".enc");
    let mut removed = Vec::new();
    for entry in walkdir::WalkDir::new(remote)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
    {
        let path = entry.path();
        let name = path.file_name().unwrap().to_string_lossy();
        if path.strip_prefix(remote).unwrap() == std::path::Path::new(key) || name.contains(oid) {
            removed.push(path.to_path_buf());
        }
    }
    for path in &removed {
        fs::remove_file(path).unwrap();
    }
    removed
}

fn local_snapshot_manifest(state: &std::path::Path, order: &str) -> serde_json::Value {
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let (manifest_key, manifest_json): (String, String) = conn
        .query_row(
            &format!("select manifest_key, manifest_json from snapshots order by created_at {order} limit 1"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    local_snapshot_manifest_from_record(state, &manifest_key, &manifest_json)
}

fn local_snapshot_manifest_by_id(state: &std::path::Path, snapshot_id: &str) -> serde_json::Value {
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let (manifest_key, manifest_json): (String, String) = conn
        .query_row(
            "select manifest_key, manifest_json from snapshots where id=?1",
            [snapshot_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    local_snapshot_manifest_from_record(state, &manifest_key, &manifest_json)
}

fn local_snapshot_manifest_from_record(
    state: &std::path::Path,
    manifest_key: &str,
    manifest_json: &str,
) -> serde_json::Value {
    let mut manifest: serde_json::Value = if !manifest_json.trim().is_empty() {
        serde_json::from_str(manifest_json).unwrap()
    } else {
        serde_json::from_slice(&fs::read(state.join(manifest_key)).unwrap()).unwrap()
    };
    if manifest["roots"]
        .as_object()
        .is_some_and(|roots| roots.is_empty())
    {
        let mut roots = serde_json::Map::new();
        for (root_id, root_tree) in manifest["root_trees"].as_object().unwrap() {
            let tree_key = root_tree["tree_key"].as_str().unwrap();
            let tree: serde_json::Value =
                serde_json::from_slice(&fs::read(state.join(tree_key)).unwrap()).unwrap();
            let entries = if let Some(entries) = tree["entries"].as_object() {
                entries.values().cloned().collect()
            } else {
                let node_key = tree["root_node"]["node_key"].as_str().unwrap();
                let node: serde_json::Value =
                    serde_json::from_slice(&fs::read(state.join(node_key)).unwrap()).unwrap();
                local_tree_node_entries(state, &node)
            };
            roots.insert(root_id.clone(), serde_json::Value::Array(entries));
        }
        manifest["roots"] = serde_json::Value::Object(roots);
    }
    manifest
}

fn local_snapshot_file_object_key(
    state: &std::path::Path,
    snapshot_id: &str,
    root_id: &str,
    path: &str,
) -> String {
    let manifest = local_snapshot_manifest_by_id(state, snapshot_id);
    manifest["roots"][root_id]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["path"] == path)
        .and_then(|entry| entry["payload"]["object_key"].as_str())
        .unwrap_or_else(|| panic!("missing object key for {root_id}/{path} in {snapshot_id}"))
        .to_string()
}

fn local_tree_node_entries(
    state: &std::path::Path,
    node: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let mut entries = node["entries"]
        .as_object()
        .map(|entries| entries.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    if let Some(children) = node["child_nodes"].as_object() {
        for child in children.values() {
            let node_key = child["node_key"].as_str().unwrap();
            let child_node: serde_json::Value =
                serde_json::from_slice(&fs::read(state.join(node_key)).unwrap()).unwrap();
            entries.extend(local_tree_node_entries(state, &child_node));
        }
    }
    entries
}

fn remote_snapshot_manifest(
    remote: &std::path::Path,
    snapshot: &serde_json::Value,
) -> serde_json::Value {
    if let Some(manifest_json) = snapshot["manifest_json"].as_str()
        && !manifest_json.trim().is_empty()
    {
        return serde_json::from_str(manifest_json).unwrap();
    }
    let manifest_key = snapshot["manifest_key"].as_str().unwrap();
    serde_json::from_slice(&fs::read(remote.join(manifest_key)).unwrap()).unwrap()
}

fn write_test_remote_head(
    remote: &std::path::Path,
    metadata: &serde_json::Value,
    root_acks: serde_json::Value,
) {
    let host = &metadata["config"]["host"];
    let host_id = host["id"].as_str().unwrap();
    let host_prefix = host["name"].as_str().unwrap();
    let head = serde_json::json!({
        "version": 1,
        "host_id": host_id,
        "host_name": host["name"].as_str().unwrap(),
        "current_snapshot": metadata["refs"]["current"].as_str(),
        "last_synced": metadata["refs"]["last-synced"].as_str(),
        "root_acks": root_acks,
        "metadata_key": format!("{host_prefix}/metadata/export.json"),
        "host_index_key": null,
        "gc_mark_key": format!("{host_prefix}/gc/mark.json"),
        "latest_snapshot_key": metadata["refs"]["current"]
            .as_str()
            .map(|snapshot| format!("{host_prefix}/snapshots/{snapshot}.cbor.zst.enc")),
        "latest_operation_key": null,
        "updated_at": Utc::now().to_rfc3339(),
    });
    let cbor = serde_cbor::to_vec(&head).unwrap();
    let compressed = zstd::stream::encode_all(cbor.as_slice(), 3).unwrap();
    let head_path = remote.join(format!("{host_prefix}/head.cbor.zst.enc"));
    fs::create_dir_all(head_path.parent().unwrap()).unwrap();
    fs::write(head_path, compressed).unwrap();
}

fn canonical_loose_blob_key(object_key: &str) -> String {
    let rest = object_key
        .strip_prefix("objects/blobs/")
        .unwrap_or_else(|| panic!("unexpected blob object key {object_key}"));
    format!("blobs/loose/{rest}.blob.enc")
}

fn canonical_object_key(object_key: &str) -> String {
    if let Some(rest) = object_key.strip_prefix("objects/trees/") {
        format!("trees/{rest}.cbor.zst.enc")
    } else if let Some(rest) = object_key.strip_prefix("objects/blobs/") {
        format!("blobs/loose/{rest}.blob.enc")
    } else if let Some(rest) = object_key.strip_prefix("objects/snapshots/") {
        format!("snapshots/{rest}.cbor.zst.enc")
    } else if let Some(rest) = object_key.strip_prefix("objects/large/manifests/") {
        format!("large/manifests/{rest}.cbor.zst.enc")
    } else if let Some(rest) = object_key.strip_prefix("objects/indexes/pack/") {
        let rest = rest.strip_suffix(".json").unwrap_or(rest);
        format!("indexes/pack-index/{rest}.cbor.zst.enc")
    } else {
        panic!("unsupported canonical object key {object_key}")
    }
}

fn assert_canonical_cbor_zstd(path: &std::path::Path) {
    let compressed = fs::read(path).unwrap();
    let cbor = zstd::stream::decode_all(compressed.as_slice()).unwrap();
    let value: serde_cbor::Value = serde_cbor::from_slice(&cbor).unwrap();
    assert!(matches!(value, serde_cbor::Value::Map(_)));
}

fn rewrite_canonical_cbor_zstd(
    path: &std::path::Path,
    mutate: impl FnOnce(&mut serde_json::Value),
) {
    let compressed = fs::read(path).unwrap();
    let cbor = zstd::stream::decode_all(compressed.as_slice()).unwrap();
    let mut value: serde_json::Value = serde_cbor::from_slice(&cbor).unwrap();
    mutate(&mut value);
    let cbor = serde_cbor::to_vec(&value).unwrap();
    let compressed = zstd::stream::encode_all(cbor.as_slice(), 3).unwrap();
    fs::write(path, compressed).unwrap();
}

fn count_files_ending(root: &std::path::Path, suffix: &str) -> usize {
    walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_file() && entry.path().to_string_lossy().ends_with(suffix)
        })
        .count()
}

fn db_ref(state: &std::path::Path, name: &str) -> Option<String> {
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.query_row("select value from refs where name=?1", [name], |row| {
        row.get(0)
    })
    .ok()
}

fn db_remote_ref_count(state: &std::path::Path) -> i64 {
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.query_row("select count(*) from remote_refs", [], |row| row.get(0))
        .unwrap()
}

fn db_remote_ref_value(state: &std::path::Path, name: &str) -> Option<String> {
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.query_row(
        "select value from remote_refs where name=?1",
        [name],
        |row| row.get(0),
    )
    .ok()
}

fn db_operation_count(state: &std::path::Path, kind: &str) -> i64 {
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.query_row(
        "select count(*) from operations where kind=?1",
        [kind],
        |row| row.get(0),
    )
    .unwrap()
}

fn db_total_operation_count(state: &std::path::Path) -> i64 {
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.query_row("select count(*) from operations", [], |row| row.get(0))
        .unwrap()
}

fn local_oplog_record_count(state: &std::path::Path) -> usize {
    cborl_record_count(&state.join("ops/local-oplog.cborl"))
}

fn cborl_record_count(path: &std::path::Path) -> usize {
    let bytes = fs::read(path).unwrap();
    let mut deserializer = serde_cbor::de::Deserializer::from_slice(&bytes);
    let mut count = 0usize;
    while serde::Deserialize::deserialize(&mut deserializer)
        .map(|_: serde_cbor::Value| ())
        .is_ok()
    {
        count += 1;
    }
    count
}

#[test]
fn file_remote_clone_restores_normal_and_large_files() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let host_clone = tmp.path().join("host-clone");
    let restore = tmp.path().join("restore");
    let host_restore = tmp.path().join("host-restore");
    fs::create_dir_all(source.join("sub")).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("sub/beta.txt"), b"beta\n").unwrap();
    let mut medium = Vec::new();
    for i in 0..256 * 1024 {
        medium.push(b'a' + (i % 26) as u8);
    }
    fs::write(source.join("medium.dat"), &medium).unwrap();
    fs::write(source.join("payload.zip"), vec![0u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("test-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("binary_min_size = 16777216", "binary_min_size = 131072")
        .replace("chunked_min_size = 524288", "chunked_min_size = 131072")
        .replace("chunk_size = 8388608", "chunk_size = 65536");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--exclude")
            .arg("**/.git/**");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    let sync_status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("status");
        c
    });
    assert!(sync_status.contains("remote_last_synced "));
    let host_dir = first_remote_host_dir(&remote);
    let host_id = host_dir.file_name().unwrap().to_string_lossy();
    let current_ref_name = format!("{host_id}/refs/current");
    assert_eq!(
        db_remote_ref_value(&state, &current_ref_name),
        db_ref(&state, "current")
    );
    assert_eq!(db_remote_ref_count(&state), 2);
    assert!(
        state
            .join("queue/uploads")
            .read_dir()
            .unwrap()
            .next()
            .is_none()
    );
    assert!(
        state
            .join("queue/events")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
    let remote_check = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("check");
        c
    });
    assert!(remote_check.contains("remote_type file"));
    assert!(remote_check.contains("remote file://"));
    assert!(remote_check.contains("metadata ok"));
    assert!(remote_check.contains("range_get 1"));
    let capabilities = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("capabilities");
        c
    });
    assert!(capabilities.contains("range_get true"));
    assert!(capabilities.contains("multipart_upload false"));
    let hosts = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("hosts");
        c
    });
    assert!(hosts.contains("hosts 1"));
    assert!(hosts.contains("test-host"));
    let host = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("host")
            .arg("test-host");
        c
    });
    assert!(host.contains("name test-host"));
    assert!(host.contains("roots 1"));
    assert!(host.contains("snapshots 1"));
    assert!(remote.join("objects/trees").exists());
    assert_canonical_cbor_zstd(&find_file_ending(&remote.join("trees"), ".cbor.zst.enc"));
    assert!(find_file_ending(&remote.join("blobs/loose"), ".blob.enc").exists());
    assert_canonical_cbor_zstd(&find_file_ending(
        &remote.join("large/manifests"),
        ".cbor.zst.enc",
    ));
    assert!(find_file_ending(&remote.join("large/chunks/fixed-8m"), ".chunk.enc").exists());
    assert_canonical_cbor_zstd(&host_remote_path(
        &remote,
        "indexes/chunk-index/shard-0000.cbor.zst.enc",
    ));
    let host_ref_dirs = remote_host_dirs(&remote)
        .into_iter()
        .filter_map(|path| {
            if path.join("refs/current").exists() && path.join("refs/last-synced").exists() {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(host_ref_dirs.len(), 1);
    assert!(
        host_ref_dirs[0]
            .join("snapshots")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
    );
    assert!(
        fs::read_dir(host_ref_dirs[0].join("snapshots"))
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("enc"))
    );
    assert!(
        fs::read_dir(host_ref_dirs[0].join("ops"))
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("enc"))
    );
    assert!(
        host_ref_dirs[0]
            .join("ops")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&host_clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()))
            .arg("--host")
            .arg("test-host");
        c
    });
    assert_eq!(
        local_oplog_record_count(&clone) as i64,
        db_total_operation_count(&clone)
    );
    assert_eq!(
        local_oplog_record_count(&host_clone) as i64,
        db_total_operation_count(&host_clone)
    );
    assert_eq!(db_remote_ref_count(&clone), 2);
    assert_eq!(db_remote_ref_count(&host_clone), 2);
    run({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&host_clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&host_restore);
        c
    });

    assert_eq!(
        fs::read(source.join("alpha.txt")).unwrap(),
        fs::read(restore.join("sample/alpha.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("sub/beta.txt")).unwrap(),
        fs::read(restore.join("sample/sub/beta.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("payload.zip")).unwrap(),
        fs::read(restore.join("sample/payload.zip")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("medium.dat")).unwrap(),
        fs::read(restore.join("sample/medium.dat")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("payload.zip")).unwrap(),
        fs::read(host_restore.join("sample/payload.zip")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("medium.dat")).unwrap(),
        fs::read(host_restore.join("sample/medium.dat")).unwrap()
    );
}

#[test]
fn multi_root_sync_clone_restore_preserves_host_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let docs = tmp.path().join("docs");
    let projects = tmp.path().join("projects");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&docs).unwrap();
    fs::create_dir_all(projects.join("data")).unwrap();
    fs::write(docs.join("notes.txt"), b"docs v1\n").unwrap();
    fs::write(projects.join("app.rs"), b"fn main() {}\n").unwrap();
    fs::write(projects.join("data/payload.zip"), vec![7u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("multi-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("docs")
            .arg(&docs);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("projects")
            .arg(&projects)
            .arg("--exclude")
            .arg("**/target/**");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("initial multi-root");
        c
    });
    fs::write(
        projects.join("app.rs"),
        b"fn main() { println!(\"v2\"); }\n",
    )
    .unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("project changed");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });

    let export: serde_json::Value =
        serde_json::from_slice(&fs::read(host_metadata_export_path(&remote)).unwrap()).unwrap();
    assert_eq!(export["roots"].as_array().unwrap().len(), 2);
    assert_eq!(export["snapshots"].as_array().unwrap().len(), 2);
    let first_manifest = remote_snapshot_manifest(&remote, &export["snapshots"][0]);
    let second_manifest = remote_snapshot_manifest(&remote, &export["snapshots"][1]);
    assert!(first_manifest["root_trees"].get("docs").is_some());
    assert!(first_manifest["root_trees"].get("projects").is_some());
    assert_eq!(
        first_manifest["root_trees"]["docs"]["tree_id"],
        second_manifest["root_trees"]["docs"]["tree_id"]
    );
    assert_ne!(
        first_manifest["root_trees"]["projects"]["tree_id"],
        second_manifest["root_trees"]["projects"]["tree_id"]
    );

    let host = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("host")
            .arg("multi-host");
        c
    });
    assert!(host.contains("roots 2"));
    assert!(host.contains("snapshots 2"));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read(docs.join("notes.txt")).unwrap(),
        fs::read(restore.join("docs/notes.txt")).unwrap()
    );
    assert_eq!(
        fs::read(projects.join("app.rs")).unwrap(),
        fs::read(restore.join("projects/app.rs")).unwrap()
    );
    assert_eq!(
        fs::read(projects.join("data/payload.zip")).unwrap(),
        fs::read(restore.join("projects/data/payload.zip")).unwrap()
    );
}

#[test]
fn disaster_recovery_e2e_preserves_multi_root_large_dedup_and_packed_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let docs = tmp.path().join("docs");
    let assets = tmp.path().join("assets");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(docs.join("notes")).unwrap();
    fs::create_dir_all(&assets).unwrap();
    fs::write(docs.join("README.md"), b"# project\n").unwrap();
    fs::write(docs.join("notes/todo.txt"), b"- snapshot\n- sync\n").unwrap();
    let shared = vec![9u8; 4096];
    let mut alpha = shared.clone();
    alpha.extend(vec![1u8; 4096]);
    let mut beta = shared;
    beta.extend(vec![2u8; 4096]);
    fs::write(assets.join("alpha.bin"), &alpha).unwrap();
    fs::write(assets.join("beta.bin"), &beta).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("dr-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 4096")
        .replace("small_pack_target = 67108864", "small_pack_target = 64")
        .replace("normal_pack_target = 268435456", "normal_pack_target = 64");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("docs")
            .arg(&docs);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("assets")
            .arg(&assets);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("disaster recovery baseline");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    assert!(find_file_ending(&state.join("objects/packs"), ".mpack").exists());
    assert_eq!(
        count_files_ending(&state.join("objects/large/chunks/fixed"), ""),
        3,
        "shared large chunk should be stored once locally"
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
    assert!(find_file_ending(&remote.join("packs/small"), ".mpack").exists());
    assert_eq!(
        count_files_ending(&remote.join("large/chunks/fixed-8m"), ".chunk.enc"),
        3,
        "remote should contain the deduplicated large chunk set"
    );

    let _ = fs::remove_dir_all(remote.join("objects"));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("large").arg("verify");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read(docs.join("README.md")).unwrap(),
        fs::read(restore.join("docs/README.md")).unwrap()
    );
    assert_eq!(
        fs::read(docs.join("notes/todo.txt")).unwrap(),
        fs::read(restore.join("docs/notes/todo.txt")).unwrap()
    );
    assert_eq!(
        fs::read(assets.join("alpha.bin")).unwrap(),
        fs::read(restore.join("assets/alpha.bin")).unwrap()
    );
    assert_eq!(
        fs::read(assets.join("beta.bin")).unwrap(),
        fs::read(restore.join("assets/beta.bin")).unwrap()
    );
}

#[test]
fn remote_recovery_preserves_paused_resumed_and_missing_roots() {
    let tmp = tempfile::tempdir().unwrap();
    let docs = tmp.path().join("docs");
    let photos = tmp.path().join("photos");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&docs).unwrap();
    fs::create_dir_all(&photos).unwrap();
    fs::write(docs.join("notes.txt"), b"docs v1\n").unwrap();
    fs::write(photos.join("image.txt"), b"photo v1\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("root-state-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("docs")
            .arg(&docs);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("photos")
            .arg(&photos);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("initial roots");
        c
    });

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("pause")
            .arg("docs");
        c
    });
    fs::write(docs.join("notes.txt"), b"docs while paused\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("docs paused");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("resume")
            .arg("docs");
        c
    });
    fs::write(docs.join("notes.txt"), b"docs resumed\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("docs resumed");
        c
    });

    fs::remove_dir_all(&photos).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("photos missing");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });

    assert_eq!(db_operation_count(&state, "root-paused"), 1);
    assert_eq!(db_operation_count(&state, "root-resumed"), 1);
    assert_eq!(db_operation_count(&state, "root-missing"), 1);
    let root_list = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(root_list_has(&root_list, "docs", "active"));
    assert!(root_list_has(&root_list, "photos", "missing"));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("fsck");
        c
    });
    let clone_root_list = output({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("root").arg("list");
        c
    });
    assert!(root_list_has(&clone_root_list, "docs", "active"));
    assert!(root_list_has(&clone_root_list, "photos", "missing"));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("docs/notes.txt")).unwrap(),
        b"docs resumed\n"
    );
    assert_eq!(
        fs::read(restore.join("photos/image.txt")).unwrap(),
        b"photo v1\n"
    );
}

#[test]
fn encrypted_multi_root_remote_recovery_uses_exported_master_key() {
    let tmp = tempfile::tempdir().unwrap();
    let docs = tmp.path().join("docs");
    let media = tmp.path().join("media");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&docs).unwrap();
    fs::create_dir_all(&media).unwrap();
    fs::write(docs.join("secret.txt"), b"multi root secret\n").unwrap();
    fs::write(media.join("payload.zip"), vec![9u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--encrypt")
            .arg("--host-name")
            .arg("encrypted-multi")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("docs")
            .arg(&docs);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("media")
            .arg(&media);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let exported_key = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("key").arg("export");
        c
    })
    .trim()
    .to_string();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });

    let plain_oid = blake3::hash(b"multi root secret\n").to_hex().to_string();
    let export: serde_json::Value = read_remote_metadata(&remote);
    assert_eq!(export["roots"].as_array().unwrap().len(), 2);
    let object_key = export["blobs"][0]["object_key"].as_str().unwrap();
    assert!(!object_key.contains(&plain_oid));
    assert!(
        fs::read(remote.join(object_key))
            .unwrap()
            .starts_with(b"age-encryption.org/v1")
    );
    assert!(host_remote_path(&remote, "keys/recipients.toml").exists());

    fs::remove_dir_all(&state).unwrap();
    run({
        let mut c = mj();
        c.env("MAJUTSU_MASTER_KEY", &exported_key)
            .arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert_eq!(
        fs::read_to_string(clone.join("keys/master.key"))
            .unwrap()
            .trim(),
        exported_key
    );
    run({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read(docs.join("secret.txt")).unwrap(),
        fs::read(restore.join("docs/secret.txt")).unwrap()
    );
    assert_eq!(
        fs::read(media.join("payload.zip")).unwrap(),
        fs::read(restore.join("media/payload.zip")).unwrap()
    );
}

#[test]
fn file_remote_clone_preserves_restore_archive_config() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("days = 7", "days = 4")
        .replace("tier = \"Standard\"", "tier = \"Bulk\"");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });

    let cloned_config = fs::read_to_string(clone.join("config.toml")).unwrap();
    assert!(cloned_config.contains("[restore.archive]"));
    assert!(cloned_config.contains("days = 4"));
    assert!(cloned_config.contains("tier = \"Bulk\""));
}

#[test]
fn invalid_restore_archive_config_is_rejected_when_reading_config() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("days = 7", "days = 0");
    fs::write(&config_path, config).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status");
        c
    });
}

#[test]
fn invalid_tiering_config_is_rejected_when_reading_config() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap().replacen(
        "after = \"30d\"",
        "after = \"not-a-duration\"",
        1,
    );
    fs::write(&config_path, config).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status");
        c
    });
}

#[test]
fn invalid_large_and_pack_config_is_rejected_when_reading_config() {
    let tmp = tempfile::tempdir().unwrap();
    let large_state = tmp.path().join("large-state");
    let pack_state = tmp.path().join("pack-state");

    run({
        let mut c = mj();
        c.arg("--home").arg(&large_state).arg("init");
        c
    });
    let config_path = large_state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("chunk_size = 8388608", "chunk_size = 0");
    fs::write(&config_path, config).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&large_state).arg("status");
        c
    });

    run({
        let mut c = mj();
        c.arg("--home").arg(&pack_state).arg("init");
        c
    });
    let config_path = pack_state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("small_pack_target = 67108864", "small_pack_target = 0");
    fs::write(&config_path, config).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&pack_state).arg("status");
        c
    });
}

#[test]
fn clone_can_restore_from_canonical_object_aliases() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("payload.zip"), vec![3u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    let _ = fs::remove_dir_all(remote.join("objects"));
    let status = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .arg("status")
            .arg("--deep");
        c
    });
    assert!(status.contains("missing_remote_objects 0"));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read(source.join("alpha.txt")).unwrap(),
        fs::read(restore.join("sample/alpha.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("payload.zip")).unwrap(),
        fs::read(restore.join("sample/payload.zip")).unwrap()
    );
}

#[test]
fn encrypted_clone_can_restore_from_canonical_object_aliases() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("secret.txt"), b"secret\n").unwrap();
    fs::write(source.join("payload.zip"), vec![9u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--encrypt")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let exported_key = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("key").arg("export");
        c
    })
    .trim()
    .to_string();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    let _ = fs::remove_dir_all(remote.join("objects"));

    run({
        let mut c = mj();
        c.env("MAJUTSU_MASTER_KEY", &exported_key)
            .arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read(source.join("secret.txt")).unwrap(),
        fs::read(restore.join("sample/secret.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("payload.zip")).unwrap(),
        fs::read(restore.join("sample/payload.zip")).unwrap()
    );
}

#[test]
fn remote_fsck_detects_missing_canonical_host_ref() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("test-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    let host_ref = remote_host_dirs(&remote)
        .into_iter()
        .find(|path| path.join("refs/current").exists())
        .unwrap()
        .join("refs/current");
    fs::remove_file(host_ref).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_unexpected_canonical_host_ref() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });

    fs::write(host_remote_path(&remote, "refs/legacy"), b"legacy").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_check_accepts_direct_host_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    let check = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("check");
        c
    });
    assert!(check.contains("metadata ok"));
    assert!(check.contains("/metadata/export.json"), "{check}");
    assert!(!check.contains("hosts/index.json"), "{check}");
    assert!(check.contains("range_get 1"));

    let check = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("check");
        c
    });
    assert!(check.contains("metadata ok"));
    assert!(check.contains("/metadata/export.json"), "{check}");
    assert!(!check.contains("hosts/index.json"), "{check}");
    assert!(check.contains("range_get 1"));
}

#[test]
fn remote_host_index_last_synced_matches_metadata_ref() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export = read_remote_metadata(&remote);
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(export["refs"]["last-synced"].as_str().unwrap())
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap(),
        chrono::DateTime::parse_from_rfc3339(export["refs"]["last-synced"].as_str().unwrap())
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    );
}

#[test]
fn remote_hosts_can_browse_multi_host_timeline() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source_alpha = tmp.path().join("source-alpha");
    let source_beta = tmp.path().join("source-beta");
    let state_alpha = tmp.path().join("state-alpha");
    let state_beta = tmp.path().join("state-beta");
    fs::create_dir_all(&source_alpha).unwrap();
    fs::create_dir_all(&source_beta).unwrap();
    fs::write(source_alpha.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source_beta.join("beta.txt"), b"beta\n").unwrap();

    for (state, source, host, root) in [
        (&state_alpha, &source_alpha, "alpha-host", "alpha-root"),
        (&state_beta, &source_beta, "beta-host", "beta-root"),
    ] {
        run({
            let mut c = mj();
            c.arg("--home")
                .arg(state)
                .arg("init")
                .arg("--host-name")
                .arg(host)
                .arg("--remote")
                .arg(format!("file://{}", remote.display()));
            c
        });
        run({
            let mut c = mj();
            c.arg("--home")
                .arg(state)
                .arg("root")
                .arg("add")
                .arg(root)
                .arg(source);
            c
        });
        run({
            let mut c = mj();
            c.arg("--home").arg(state).arg("snapshot");
            c
        });
        run({
            let mut c = mj();
            c.arg("--home").arg(state).arg("sync");
            c
        });
    }

    let hosts = output({
        let mut c = mj();
        c.arg("--home").arg(&state_alpha).arg("remote").arg("hosts");
        c
    });
    assert!(hosts.contains("hosts 2"), "{hosts}");
    assert!(hosts.contains("\talpha-host\t"), "{hosts}");
    assert!(hosts.contains("\tbeta-host\t"), "{hosts}");
    assert!(!hosts.contains("hosts/"), "{hosts}");
    assert!(hosts.contains("/metadata/export.json"), "{hosts}");

    let beta = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state_alpha)
            .arg("remote")
            .arg("host")
            .arg("beta-host")
            .arg("--snapshots")
            .arg("--operations");
        c
    });
    assert!(beta.contains("name beta-host"), "{beta}");
    assert!(beta.contains("roots 1"), "{beta}");
    assert!(beta.contains("snapshots 1"), "{beta}");
    assert!(
        beta.contains("snapshot_id\tcreated_at\tparent\top_id"),
        "{beta}"
    );
    assert!(
        beta.contains("op_id\tcreated_at\tkind\tstatus\tbefore\tafter\tmessage"),
        "{beta}"
    );
    assert!(beta.contains("\tdone\t"), "{beta}");
    assert!(beta.contains("beta-root"), "{beta}");
}

#[test]
fn remote_host_index_duplicate_id_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("dup-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export = read_remote_metadata(&remote);
    let host_id = export["config"]["host"]["id"].as_str().unwrap().to_string();
    write_remote_metadata_at_prefix(&remote, "dup-host-copy", &export);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()))
            .arg("--host")
            .arg(&host_id);
        c
    });
}

#[test]
fn remote_host_index_duplicate_metadata_key_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("dup-key-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mut duplicate = read_remote_metadata(&remote);
    duplicate["config"]["host"]["id"] = serde_json::Value::String("other-host-id".into());
    duplicate["config"]["host"]["name"] = serde_json::Value::String("dup-key-host".into());
    write_remote_metadata_at_prefix(&remote, "dup-key-host-copy", &duplicate);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_host_index_noncanonical_metadata_key_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("bad-key-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export = read_remote_metadata(&remote);
    write_remote_metadata_at_prefix(&remote, "wrong-host", &export);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_accepts_canonical_only_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let _ = fs::remove_dir_all(remote.join("objects"));
    for path in remote_host_dirs(&remote) {
        let snapshots = path.join("snapshots");
        if snapshots.exists() {
            for export in fs::read_dir(snapshots).unwrap() {
                let export = export.unwrap().path();
                if export.extension().and_then(|ext| ext.to_str()) == Some("json") {
                    fs::remove_file(export).unwrap();
                }
            }
        }
        let ops = path.join("ops");
        if ops.exists() {
            for export in fs::read_dir(ops).unwrap() {
                let export = export.unwrap().path();
                if export.extension().and_then(|ext| ext.to_str()) == Some("json") {
                    fs::remove_file(export).unwrap();
                }
            }
        }
    }

    let fsck = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
    assert!(fsck.contains("remote fsck ok"));
    assert_eq!(db_operation_count(&state, "fsck"), 1);
    assert_eq!(
        local_oplog_record_count(&state) as i64,
        db_total_operation_count(&state)
    );
}

#[test]
fn clone_can_use_single_host_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("only-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "alpha\n"
    );
}

#[test]
fn clone_requires_host_for_multi_host_remote() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source_a = tmp.path().join("source-a");
    let source_b = tmp.path().join("source-b");
    let state_a = tmp.path().join("state-a");
    let state_b = tmp.path().join("state-b");
    let clone_without_host = tmp.path().join("clone-without-host");
    let clone_b = tmp.path().join("clone-b");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source_a).unwrap();
    fs::create_dir_all(&source_b).unwrap();
    fs::write(source_a.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source_b.join("beta.txt"), b"beta\n").unwrap();

    for (state, host_name, source, root) in [
        (&state_a, "host-a", &source_a, "a"),
        (&state_b, "host-b", &source_b, "b"),
    ] {
        run({
            let mut c = mj();
            c.arg("--home")
                .arg(state)
                .arg("init")
                .arg("--host-name")
                .arg(host_name)
                .arg("--remote")
                .arg(format!("file://{}", remote.display()));
            c
        });
        run({
            let mut c = mj();
            c.arg("--home")
                .arg(state)
                .arg("root")
                .arg("add")
                .arg(root)
                .arg(source);
            c
        });
        run({
            let mut c = mj();
            c.arg("--home").arg(state).arg("snapshot");
            c
        });
        run({
            let mut c = mj();
            c.arg("--home").arg(state).arg("sync");
            c
        });
    }

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone_without_host)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone_b)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()))
            .arg("--host")
            .arg("host-b");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone_b)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("b/beta.txt")).unwrap(),
        "beta\n"
    );
    assert!(!restore.join("a/alpha.txt").exists());
}

#[test]
fn clone_rejects_ambiguous_host_name_but_accepts_host_id() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source_a = tmp.path().join("source-a");
    let source_b = tmp.path().join("source-b");
    let state_a = tmp.path().join("state-a");
    let state_b = tmp.path().join("state-b");
    let clone_by_name = tmp.path().join("clone-by-name");
    fs::create_dir_all(&source_a).unwrap();
    fs::create_dir_all(&source_b).unwrap();
    fs::write(source_a.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source_b.join("beta.txt"), b"beta\n").unwrap();

    for (state, source, root, host_name, host_id) in [
        (&state_a, &source_a, "a", "shared", "a"),
        (&state_b, &source_b, "b", "shared-copy", "b"),
    ] {
        run({
            let mut c = mj();
            c.arg("--home")
                .arg(state)
                .arg("init")
                .arg("--host-name")
                .arg(host_name)
                .arg("--remote")
                .arg(format!("file://{}", remote.display()));
            c
        });
        set_host_id(state, host_id);
        run({
            let mut c = mj();
            c.arg("--home")
                .arg(state)
                .arg("root")
                .arg("add")
                .arg(root)
                .arg(source);
            c
        });
        run({
            let mut c = mj();
            c.arg("--home").arg(state).arg("snapshot");
            c
        });
        run({
            let mut c = mj();
            c.arg("--home").arg(state).arg("sync");
            c
        });
    }
    let mut corrupt_b = read_json_maybe_zstd(&remote.join("shared-copy/metadata/export.json"));
    corrupt_b["config"]["host"]["name"] = serde_json::Value::String("shared".into());
    write_json_maybe_zstd(&remote.join("shared-copy/metadata/export.json"), &corrupt_b);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone_by_name)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()))
            .arg("--host")
            .arg("shared");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state_a)
            .arg("remote")
            .arg("host")
            .arg("shared");
        c
    });

    let hosts = output({
        let mut c = mj();
        c.arg("--home").arg(&state_a).arg("remote").arg("hosts");
        c
    });
    assert!(hosts.contains("hosts 2"));
    assert_eq!(hosts.matches("\tshared\t").count(), 2);
    let host_b_id = remote_host_dirs(&remote)
        .into_iter()
        .find_map(|dir| {
            let metadata = read_json_maybe_zstd(&dir.join("metadata/export.json"));
            if metadata["config"]["host"]["id"].as_str() == Some("b") {
                Some("b".to_string())
            } else {
                None
            }
        })
        .unwrap();
    assert!(hosts.contains(&host_b_id));

    let host_b = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state_a)
            .arg("remote")
            .arg("host")
            .arg(&host_b_id)
            .arg("--snapshots")
            .arg("--operations");
        c
    });
    assert!(host_b.contains(&format!("id {host_b_id}")));
    assert!(host_b.contains("name shared"));
    assert!(host_b.contains("roots 1"));
    assert!(host_b.contains("snapshots 1"));
    assert!(host_b.contains("operations "));
    assert!(host_b.contains("snapshot_id\tcreated_at\tparent\top_id"));
    assert!(host_b.contains("op_id\tcreated_at\tkind\tstatus\tbefore\tafter\tmessage"));
    assert!(host_b.contains("initial-scan"));
    assert!(host_b.contains("\tdone\t"));
    assert!(host_b.contains("root-added"));

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(tmp.path().join("clone-by-id"))
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()))
            .arg("--host")
            .arg(&host_b_id);
        c
    });
}

#[test]
fn remote_fsck_detects_missing_canonical_object_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let alias_shard = fs::read_dir(remote.join("trees"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.is_dir())
        .unwrap();
    let alias = fs::read_dir(alias_shard)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.is_file())
        .unwrap();
    fs::remove_file(alias).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_missing_chunk_index_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.zip"), vec![7u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    fs::remove_file(host_remote_path(
        &remote,
        "indexes/chunk-index/shard-0000.cbor.zst.enc",
    ))
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_missing_remote_chunk_index_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.bin"), vec![5u8; 8192]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 4096");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    fs::remove_file(host_remote_path(
        &remote,
        "indexes/chunk-index/shard-0000.cbor.zst.enc",
    ))
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_unexpected_chunk_index_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.bin"), vec![6u8; 8192]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 4096");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    fs::copy(
        host_remote_path(&remote, "indexes/chunk-index/shard-0000.cbor.zst.enc"),
        host_remote_path(&remote, "indexes/chunk-index/shard-extra.cbor.zst.enc"),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_unexpected_chunk_index_shard_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.bin"), vec![6u8; 8192]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 4096");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    fs::copy(
        host_remote_path(&remote, "indexes/chunk-index/shard-0000.cbor.zst.enc"),
        host_remote_path(&remote, "indexes/chunk-index/shard-extra.cbor.zst.enc"),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn clone_rejects_corrupt_canonical_large_manifest_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.bin"), vec![8u8; 8192]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 4096");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let manifest = find_file_ending(&remote.join("large/manifests"), ".cbor.zst.enc");
    rewrite_canonical_cbor_zstd(&manifest, |value| {
        value["oid"] = serde_json::Value::String("corrupt-large-oid".into());
    });

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_and_clone_reject_corrupt_canonical_large_chunk_payload() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.bin"), vec![8u8; 8192]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 4096");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let chunk = find_file_ending(&remote.join("large/chunks/fixed-8m"), ".chunk.enc");
    fs::write(chunk, b"corrupt\n").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_corrupt_chunk_index_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.zip"), vec![7u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    rewrite_canonical_cbor_zstd(
        &host_remote_path(&remote, "indexes/chunk-index/shard-0000.cbor.zst.enc"),
        |value| {
            value["chunks"][0]["canonical_key"] =
                serde_json::Value::String("large/chunks/corrupt.chunk.enc".into());
        },
    );

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_missing_canonical_host_export() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(find_file_ending(&first_remote_host_dir(&remote).join("gc"), ".json").exists());
    let host_dir = first_remote_host_dir(&remote);
    let canonical_snapshot = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.to_string_lossy().ends_with(".cbor.zst.enc"))
        .unwrap();
    fs::remove_file(canonical_snapshot).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_corrupt_gc_mark() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let mark = find_file_ending(&first_remote_host_dir(&remote).join("gc"), ".json");
    fs::write(mark, b"{not valid json").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_unexpected_gc_mark_object() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mark_path = find_file_ending(&first_remote_host_dir(&remote).join("gc"), ".json");
    let mut mark: serde_json::Value =
        serde_json::from_slice(&fs::read(&mark_path).unwrap()).unwrap();
    mark["object_keys"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::Value::String("objects/stale/object".into()));
    fs::write(&mark_path, serde_json::to_vec_pretty(&mark).unwrap()).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_unexpected_gc_mark_object_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mark_path = find_file_ending(&first_remote_host_dir(&remote).join("gc"), ".json");
    let mut mark: serde_json::Value =
        serde_json::from_slice(&fs::read(&mark_path).unwrap()).unwrap();
    mark["object_keys"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::Value::String("objects/stale/object".into()));
    fs::write(&mark_path, serde_json::to_vec_pretty(&mark).unwrap()).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_missing_canonical_operation_log() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let host_dir = remote_host_dirs(&remote)
        .into_iter()
        .find(|path| path.join("ops/local-oplog.cborl.zst.enc").exists())
        .unwrap();
    fs::remove_file(host_dir.join("ops/local-oplog.cborl.zst.enc")).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_missing_remote_operation_log_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let host_dir = remote_host_dirs(&remote)
        .into_iter()
        .find(|path| path.join("ops/local-oplog.cborl.zst.enc").exists())
        .unwrap();
    fs::remove_file(host_dir.join("ops/local-oplog.cborl.zst.enc")).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn clone_rejects_missing_remote_timeline_export_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let host_dir = remote_host_dirs(&remote)
        .into_iter()
        .find(|path| path.join("snapshots").exists())
        .unwrap();
    let snapshot_export = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("enc"))
        .unwrap();
    fs::remove_file(snapshot_export).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_unexpected_host_snapshot_export() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let host_dir = remote_host_dirs(&remote)
        .into_iter()
        .find(|path| path.join("snapshots").exists())
        .unwrap();
    let snapshot_export = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.to_string_lossy().ends_with(".cbor.zst.enc"))
        .unwrap();
    fs::copy(
        &snapshot_export,
        host_dir.join("snapshots").join("snap-stale.cbor.zst.enc"),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_unexpected_host_snapshot_export_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let host_dir = remote_host_dirs(&remote)
        .into_iter()
        .find(|path| path.join("snapshots").exists())
        .unwrap();
    let snapshot_export = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.to_string_lossy().ends_with(".cbor.zst.enc"))
        .unwrap();
    fs::copy(
        &snapshot_export,
        host_dir.join("snapshots").join("snap-stale.cbor.zst.enc"),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_unexpected_host_operation_export() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let host_dir = remote_host_dirs(&remote)
        .into_iter()
        .find(|path| path.join("ops/local-oplog.cborl.zst.enc").exists())
        .unwrap();
    let operation_export = fs::read_dir(host_dir.join("ops"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.to_string_lossy().ends_with(".cbor.zst.enc")
                && !path
                    .to_string_lossy()
                    .ends_with("local-oplog.cborl.zst.enc")
        })
        .unwrap();
    fs::copy(
        &operation_export,
        host_dir.join("ops").join("op-stale.cbor.zst.enc"),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_corrupt_canonical_host_snapshot_export() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let host_dir = first_remote_host_dir(&remote);
    let canonical_snapshot = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.to_string_lossy().ends_with(".cbor.zst.enc"))
        .unwrap();
    rewrite_canonical_cbor_zstd(&canonical_snapshot, |value| {
        value["manifest_key"] = serde_json::Value::String("objects/snapshots/corrupt.json".into());
    });

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_corrupt_snapshot_manifest_object() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export: serde_json::Value = read_remote_metadata(&remote);
    let manifest_key = export["snapshots"][0]["manifest_key"].as_str().unwrap();
    fs::write(remote.join(manifest_key), b"{not valid json").unwrap();
    fs::write(
        remote.join(canonical_object_key(manifest_key)),
        b"{not valid json",
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_corrupt_snapshot_manifest_object_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export: serde_json::Value = read_remote_metadata(&remote);
    let manifest_key = export["snapshots"][0]["manifest_key"].as_str().unwrap();
    fs::write(remote.join(manifest_key), b"{not valid json").unwrap();
    fs::write(
        remote.join(canonical_object_key(manifest_key)),
        b"{not valid json",
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_dangling_blob_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export_path = host_metadata_export_path(&remote);
    let mut export: serde_json::Value =
        serde_json::from_slice(&fs::read(&export_path).unwrap()).unwrap();
    let mut dangling = export["blobs"][0].clone();
    dangling["oid"] = serde_json::Value::String("dangling-remote-blob".into());
    export["blobs"].as_array_mut().unwrap().push(dangling);
    fs::write(&export_path, serde_json::to_vec_pretty(&export).unwrap()).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_and_clone_reject_corrupt_loose_blob_object() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export: serde_json::Value = read_remote_metadata(&remote);
    let object_key = export["blobs"][0]["object_key"].as_str().unwrap();
    fs::write(remote.join(object_key), b"corrupt\n").unwrap();
    fs::write(
        remote.join(object_key.replacen("objects/blobs/", "blobs/loose/", 1) + ".blob.enc"),
        b"corrupt\n",
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_unsupported_metadata_export_version() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    for export_path in [
        host_metadata_export_path(&remote),
        host_metadata_export_path(&remote),
    ] {
        let mut export: serde_json::Value =
            serde_json::from_slice(&fs::read(&export_path).unwrap()).unwrap();
        export["version"] = serde_json::Value::Number(999.into());
        fs::write(&export_path, serde_json::to_vec_pretty(&export).unwrap()).unwrap();
    }

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_invalid_metadata_refs() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export_path = host_metadata_export_path(&remote);
    let mut export: serde_json::Value =
        serde_json::from_slice(&fs::read(&export_path).unwrap()).unwrap();
    export["refs"]["legacy"] = serde_json::Value::String("snap-legacy".into());
    fs::write(&export_path, serde_json::to_vec_pretty(&export).unwrap()).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_invalid_remote_head_root_ack() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export_path = host_metadata_export_path(&remote);
    let metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(&export_path).unwrap()).unwrap();
    let current = metadata["refs"]["current"].as_str().unwrap();
    let snapshot = metadata["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|snapshot| snapshot["id"] == current)
        .unwrap();
    let manifest = remote_snapshot_manifest(&remote, snapshot);
    let root_tree = &manifest["root_trees"]["sample"];
    let valid_acks = serde_json::json!({
        "sample": {
            "snapshot_id": current,
            "tree_id": root_tree["tree_id"].as_str().unwrap(),
            "tree_key": root_tree["tree_key"].as_str().unwrap(),
            "file_count": root_tree["file_count"].as_u64().unwrap() as usize,
            "synced_at": metadata["refs"]["last-synced"].as_str().unwrap(),
        }
    });
    write_test_remote_head(&remote, &metadata, valid_acks);
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("fsck");
        c
    });

    let invalid_acks = serde_json::json!({
        "sample": {
            "snapshot_id": current,
            "tree_id": "tree-corrupt",
            "tree_key": root_tree["tree_key"].as_str().unwrap(),
            "file_count": root_tree["file_count"].as_u64().unwrap() as usize,
            "synced_at": metadata["refs"]["last-synced"].as_str().unwrap(),
        }
    });
    write_test_remote_head(&remote, &metadata, invalid_acks);
    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("fsck");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_invalid_restore_archive_config() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export_path = host_metadata_export_path(&remote);
    let mut export: serde_json::Value =
        serde_json::from_slice(&fs::read(&export_path).unwrap()).unwrap();
    export["config"]["restore"]["archive"]["days"] = serde_json::Value::Number(0.into());
    fs::write(&export_path, serde_json::to_vec_pretty(&export).unwrap()).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_invalid_remote_restore_archive_config_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mut export = read_remote_metadata(&remote);
    export["config"]["restore"]["archive"]["tier"] = serde_json::Value::String(" ".into());
    write_remote_metadata(&remote, &export);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn clone_rejects_missing_remote_objects_before_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::remove_dir_all(remote.join("objects/blobs")).unwrap();
    if remote.join("blobs").exists() {
        fs::remove_dir_all(remote.join("blobs")).unwrap();
    }

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
    assert!(
        fs::read_dir(tmp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .all(|name| !name.starts_with(".clone.clone-"))
    );
}

#[test]
fn clone_rejects_inconsistent_remote_history_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mut export = read_remote_metadata(&remote);
    export["snapshots"][0]["op_id"] = serde_json::Value::String("op-missing".into());
    write_remote_metadata(&remote, &export);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn clone_rejects_dangling_remote_metadata_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mut export = read_remote_metadata(&remote);
    let mut dangling = export["blobs"][0].clone();
    dangling["oid"] = serde_json::Value::String("dangling-remote-blob".into());
    export["blobs"].as_array_mut().unwrap().push(dangling);
    write_remote_metadata(&remote, &export);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn clone_rejects_dangling_remote_large_pin_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.zip"), vec![7u8; 16 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("pin");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mut export = read_remote_metadata(&remote);
    export["large_pins"][0]["oid"] = serde_json::Value::String("missing-large-object".into());
    write_remote_metadata(&remote, &export);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn clone_rejects_host_index_metadata_mismatch_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--host-name")
            .arg("index-host")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mut export = read_remote_metadata(&remote);
    export["config"]["host"]["name"] = serde_json::Value::String("wrong-host".into());
    write_remote_metadata(&remote, &export);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn clone_rejects_remote_ref_mismatch_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::write(host_remote_path(&remote, "refs/current"), b"snap-wrong").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn clone_rejects_unexpected_canonical_host_ref_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::write(host_remote_path(&remote, "refs/legacy"), b"legacy").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_corrupt_canonical_tree_manifest_object() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let manifest = local_snapshot_manifest(&state, "asc");
    let tree_key = manifest["root_trees"]["sample"]["tree_key"]
        .as_str()
        .unwrap();
    let canonical_tree = remote.join(canonical_object_key(tree_key));
    fs::write(&canonical_tree, b"not valid cbor zstd").unwrap();
    fs::remove_file(remote.join(tree_key)).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_corrupt_canonical_tree_manifest_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let manifest = local_snapshot_manifest(&state, "asc");
    let tree_key = manifest["root_trees"]["sample"]["tree_key"]
        .as_str()
        .unwrap();
    let canonical_tree = remote.join(canonical_object_key(tree_key));
    fs::write(&canonical_tree, b"not valid cbor zstd").unwrap();
    fs::remove_file(remote.join(tree_key)).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn unchanged_snapshot_is_skipped_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let second = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(second.contains("snapshot unchanged snap-"), "{second}");
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export: serde_json::Value = read_remote_metadata(&remote);
    let snapshots = export["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 1);

    let third = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(third.contains("snapshot unchanged snap-"), "{third}");
    let cache = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("cache")
            .arg("stat")
            .arg("--metadata");
        c
    });
    assert!(cache.contains("payload_cache_candidates 0"), "{cache}");
    assert!(cache.contains("metadata_cache_candidates 0"), "{cache}");
}

#[test]
fn tree_manifests_are_stored_as_compact_json() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("nested/beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let manifest = local_snapshot_manifest(&state, "asc");
    let tree_key = manifest["root_trees"]["sample"]["tree_key"]
        .as_str()
        .unwrap();
    let tree_bytes = fs::read(state.join(tree_key)).unwrap();
    let tree_text = String::from_utf8(tree_bytes.clone()).unwrap();
    let tree: serde_json::Value = serde_json::from_slice(&tree_bytes).unwrap();
    let pretty = serde_json::to_vec_pretty(&tree).unwrap();

    assert!(!tree_text.contains('\n'), "{tree_text}");
    assert!(tree_bytes.len() < pretty.len());
    assert_eq!(tree["entries"].as_object().unwrap().len(), 3);
}

#[test]
fn tree_format_v2_omits_flat_entries_and_restores_from_root_node() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("nested/beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.env("MAJUTSU_TREE_FORMAT", "v2")
            .arg("--home")
            .arg(&state)
            .arg("snapshot");
        c
    });

    let manifest = local_snapshot_manifest(&state, "asc");
    let tree_key = manifest["root_trees"]["sample"]["tree_key"]
        .as_str()
        .unwrap();
    let tree_bytes = fs::read(state.join(tree_key)).unwrap();
    let tree: serde_json::Value = serde_json::from_slice(&tree_bytes).unwrap();
    assert_eq!(tree["version"], 2);
    assert!(tree.get("entries").is_none(), "{tree}");
    let node_key = tree["root_node"]["node_key"].as_str().unwrap();
    let node_bytes = fs::read(state.join(node_key)).unwrap();
    let node: serde_json::Value = serde_json::from_slice(&node_bytes).unwrap();
    assert_eq!(node["entries"].as_object().unwrap().len(), 2);
    assert_eq!(node["child_nodes"].as_object().unwrap().len(), 1);
    assert_eq!(local_tree_node_entries(&state, &node).len(), 3);

    run({
        let mut c = mj();
        c.env("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE", "0")
            .env("MAJUTSU_SYNC_LOCAL_OBJECT_PRUNE", "0")
            .env("MAJUTSU_SYNC_WAIT_DEEP_REPAIR", "1")
            .arg("--home")
            .arg(&state)
            .arg("sync")
            .arg("--wait");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "alpha\n"
    );
    assert_eq!(
        fs::read_to_string(restore.join("sample/nested/beta.txt")).unwrap(),
        "beta\n"
    );

    fs::write(source.join("nested/beta.txt"), b"changed\n").unwrap();
    run({
        let mut c = mj();
        c.env("MAJUTSU_TREE_FORMAT", "v2")
            .arg("--home")
            .arg(&state)
            .arg("snapshot");
        c
    });
    let diff = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("diff");
        c
    });
    assert!(diff.contains("M\tsample/nested/beta.txt"), "{diff}");
    assert!(!diff.contains("sample/alpha.txt"), "{diff}");
}

#[test]
fn root_set_uses_remote_tree_metadata_when_local_cache_is_unreadable() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();
    fs::write(source.join("generated.dat"), b"generated\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.env("MAJUTSU_TREE_FORMAT", "v2")
            .arg("--home")
            .arg(&state)
            .arg("snapshot");
        c
    });
    let manifest = local_snapshot_manifest(&state, "asc");
    let tree_key = manifest["root_trees"]["sample"]["tree_key"]
        .as_str()
        .unwrap()
        .to_string();
    run({
        let mut c = mj();
        c.env("MAJUTSU_SYNC_LOCAL_OBJECT_PRUNE", "0")
            .arg("--home")
            .arg(&state)
            .arg("sync");
        c
    });

    fs::write(state.join(&tree_key), b"not valid metadata").unwrap();

    let root_set = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("set")
            .arg("sample")
            .arg("--exclude")
            .arg("*.dat");
        c
    });
    assert!(
        root_set.contains("forgotten_unmanaged_records"),
        "root set should rewrite history using remote metadata:\n{root_set}"
    );
}

#[test]
fn log_folds_change_when_historical_tree_metadata_is_unavailable() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.env("MAJUTSU_TREE_FORMAT", "v2")
            .arg("--home")
            .arg(&state)
            .arg("snapshot");
        c
    });
    let first = local_snapshot_manifest(&state, "asc");
    let first_tree_key = first["root_trees"]["sample"]["tree_key"]
        .as_str()
        .unwrap()
        .to_string();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::write(source.join("note.txt"), b"two\n").unwrap();
    run({
        let mut c = mj();
        c.env("MAJUTSU_TREE_FORMAT", "v2")
            .arg("--home")
            .arg(&state)
            .arg("snapshot");
        c
    });
    let _ = remove_remote_payload_aliases(&remote, &first_tree_key);
    let _ = fs::remove_file(state.join(&first_tree_key));

    let log = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--limit")
            .arg("10")
            .arg("--full");
        c
    });
    assert!(
        log.contains("sample/** (tree metadata unavailable"),
        "log should fold unavailable tree detail:\n{log}"
    );
    assert!(
        log.contains("[metadata-unavailable]"),
        "log should mark folded metadata issue:\n{log}"
    );
}

#[test]
fn unchanged_snapshot_can_be_forced_for_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.env("MAJUTSU_SNAPSHOT_ALLOW_NOOP", "1")
            .arg("--home")
            .arg(&state)
            .arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export: serde_json::Value = read_remote_metadata(&remote);
    let snapshots = export["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 2);
    let first_manifest = remote_snapshot_manifest(&remote, &snapshots[0]);
    let second_manifest = remote_snapshot_manifest(&remote, &snapshots[1]);
    assert_eq!(
        first_manifest["root_trees"]["sample"]["tree_id"],
        second_manifest["root_trees"]["sample"]["tree_id"]
    );
    assert_eq!(
        first_manifest["root_trees"]["sample"]["tree_key"],
        second_manifest["root_trees"]["sample"]["tree_key"]
    );
}

#[test]
fn sync_retry_queue_preserves_attempt_count_across_reenqueue() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote-file");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(&remote, b"not a directory\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let queued = fs::read_dir(state.join("queue/uploads"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    assert!(queued.iter().any(|item| item.contains("\"attempts\": 2")));
    assert!(queued.iter().any(|item| item.contains("\"retry_after\"")));

    fs::remove_file(&remote).unwrap();
    fs::create_dir_all(&remote).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert_eq!(
        fs::read_dir(state.join("queue/uploads")).unwrap().count(),
        0
    );
    assert!(host_metadata_export_path(&remote).exists());
}

#[test]
fn sync_status_reports_upload_queue_retry_state() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::create_dir_all(state.join("queue/uploads")).unwrap();
    fs::write(
        state.join("queue/uploads/retry-upload.json"),
        serde_json::json!({
            "id": "retry-upload",
            "key": "objects/blobs/retry",
            "source": null,
            "inline": [114, 101, 116, 114, 121],
            "created_at": "2026-06-07T00:00:00Z",
            "attempts": 2,
            "retry_after": "2999-01-01T00:00:00Z"
        })
        .to_string(),
    )
    .unwrap();

    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("status");
        c
    });

    assert!(status.contains("queued_uploads 1"));
    assert!(status.contains("queued_uploads_retrying 1"));
    assert!(status.contains("queued_uploads_delayed 1"));
    assert!(status.contains("queued_upload_next_retry_after 2999-01-01T00:00:00+00:00"));
    assert!(status.contains("queued_upload_attempts 2"));
    assert!(status.contains("queued_upload_max_attempts 2"));
    assert!(status.contains("upload_queue_backpressure true"));
}

#[test]
fn failed_sync_does_not_advance_last_synced_or_record_success_operation() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote-file");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(&remote, b"not a directory\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    assert_eq!(db_ref(&state, "last-synced"), None);
    assert_eq!(db_remote_ref_count(&state), 0);
    assert_eq!(db_operation_count(&state, "remote-sync"), 1);
    assert_eq!(
        local_oplog_record_count(&state) as i64,
        db_total_operation_count(&state)
    );
    let failed_sync_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    let failed_sync_line = failed_sync_log
        .lines()
        .find(|line| line.contains("remote-sync"))
        .unwrap()
        .to_string();
    assert!(failed_sync_line.contains("\tfailed\tfailed\t"));
    let failed_sync_op = failed_sync_line.split('\t').next().unwrap().to_string();
    let failed_sync_show = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("op")
            .arg("show")
            .arg(&failed_sync_op);
        c
    });
    assert!(failed_sync_show.contains("status failed"));
    assert!(failed_sync_show.contains("remote_sync_state failed"));

    fs::remove_file(&remote).unwrap();
    fs::create_dir_all(&remote).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    assert!(db_ref(&state, "last-synced").is_some());
    assert_eq!(db_remote_ref_count(&state), 2);
    assert_eq!(db_operation_count(&state, "remote-sync"), 2);
    assert_eq!(
        local_oplog_record_count(&state) as i64,
        db_total_operation_count(&state)
    );
    let sync_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    let successful_sync_line = sync_log
        .lines()
        .find(|line| line.contains("remote-sync"))
        .unwrap()
        .to_string();
    assert!(successful_sync_line.contains("\tdone\tsynced\t"));
    let successful_sync_op = successful_sync_line.split('\t').next().unwrap().to_string();
    let successful_sync_show = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("op")
            .arg("show")
            .arg(&successful_sync_op);
        c
    });
    assert!(successful_sync_show.contains("status done"));
    assert!(successful_sync_show.contains("remote_sync_state synced"));
}

#[test]
fn log_defaults_to_managed_file_changes_not_sync_operations() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("baseline");
        c
    });
    let mut config = fs::read_to_string(state.join("config.toml")).unwrap();
    config.push_str(&format!(
        "\n[remote]\ntype = \"file\"\npath = \"{}\"\n",
        remote.display()
    ));
    fs::write(state.join("config.toml"), config).unwrap();
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("edit alpha");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let change_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("log");
        c
    });
    assert!(change_log.contains("edit alpha"));
    assert!(change_log.contains("M\tsample/alpha.txt"));
    assert!(!change_log.contains("remote-sync"));

    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("log").arg("--operations");
        c
    });
    assert!(op_log.contains("remote-sync"));

    let colored_log = output({
        let mut c = mj();
        c.env("MJ_COLOR", "always")
            .env_remove("NO_COLOR")
            .arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--limit")
            .arg("1");
        c
    });
    assert!(colored_log.contains("\u{1b}[1;34m20"));
    assert!(
        colored_log.contains("\u{1b}[1;33mM\u{1b}[0m\t\u{1b}[1;96msample\u{1b}[0m/alpha.txt"),
        "{colored_log:?}"
    );
}

#[test]
fn log_filters_file_changes_by_pathspec() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(source.join("dir")).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("dir/beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("baseline");
        c
    });

    fs::write(source.join("alpha.txt"), b"alpha changed\n").unwrap();
    fs::write(source.join("dir/beta.txt"), b"beta changed\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("changed both");
        c
    });

    let root_relative = output({
        let mut c = mj();
        c.current_dir(&source)
            .arg("--home")
            .arg(&state)
            .arg("log")
            .arg("-r")
            .arg("sample")
            .arg("--limit")
            .arg("1")
            .arg("--")
            .arg("dir");
        c
    });
    assert!(root_relative.contains("changed both"), "{root_relative}");
    assert!(
        root_relative.contains("M\tsample/dir/beta.txt"),
        "{root_relative}"
    );
    assert!(!root_relative.contains("alpha.txt"), "{root_relative}");

    let inferred_root_relative = output({
        let mut c = mj();
        c.current_dir(&source)
            .arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--limit")
            .arg("1")
            .arg("--")
            .arg("dir");
        c
    });
    assert!(
        inferred_root_relative.contains("M\tsample/dir/beta.txt"),
        "{inferred_root_relative}"
    );
    assert!(
        !inferred_root_relative.contains("alpha.txt"),
        "{inferred_root_relative}"
    );

    let global_path = output({
        let mut c = mj();
        c.current_dir(tmp.path())
            .arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--limit")
            .arg("1")
            .arg("--")
            .arg("sample/dir");
        c
    });
    assert!(
        global_path.contains("M\tsample/dir/beta.txt"),
        "{global_path}"
    );
    assert!(!global_path.contains("alpha.txt"), "{global_path}");
}

#[test]
fn log_folds_large_change_sets_unless_full_is_requested() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    for i in 0..130 {
        fs::write(source.join(format!("file-{i:03}.txt")), format!("{i}\n")).unwrap();
    }

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("large baseline");
        c
    });

    let folded = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--limit")
            .arg("1");
        c
    });
    assert!(folded.contains("more changed files hidden"));
    assert!(!folded.contains("sample/file-129.txt"));

    let full = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--limit")
            .arg("1")
            .arg("--full");
        c
    });
    assert!(full.contains("sample/file-129.txt"));
    assert!(!full.contains("more changed files hidden"));
}

#[test]
fn sync_wait_reports_status_when_existing_sync_lock_is_held() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    fs::create_dir_all(state.join("locks")).unwrap();
    fs::write(
        state.join("locks/sync.lock"),
        std::process::id().to_string(),
    )
    .unwrap();

    let waited = {
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .arg("--wait")
            .arg("--timeout-secs")
            .arg("1");
        c.output().expect("run command")
    };
    assert!(
        !waited.status.success(),
        "sync --wait should time out while the sync lock is still held"
    );
    let waited = format!(
        "{}{}",
        String::from_utf8_lossy(&waited.stdout),
        String::from_utf8_lossy(&waited.stderr)
    );
    assert!(waited.contains("sync already running pid"));
    assert!(waited.contains("status_mode quick"));
    assert!(waited.contains("queued_uploads 0"));
    assert!(waited.contains("event_journal_pending 0"));
    assert!(waited.contains("durable_journal_pending 0"));
    assert!(waited.contains("timed out waiting for sync target"));
}

#[test]
fn split_remote_config_supports_file_and_s3_forms() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    let base = config.split("\n[large]").next().unwrap();
    let large_and_after = config.split("\n[large]").nth(1).unwrap();
    fs::write(
        &config_path,
        format!(
            r#"{base}
[remote]
type = "file"
path = "{}"

[large]{large_and_after}"#,
            remote.display()
        ),
    )
    .unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });

    let s3_state = tmp.path().join("s3-state");
    run({
        let mut c = mj();
        c.arg("--home").arg(&s3_state).arg("init");
        c
    });
    let s3_config = fs::read_to_string(s3_state.join("config.toml")).unwrap();
    let s3_base = s3_config.split("\n[large]").next().unwrap();
    let s3_large_and_after = s3_config.split("\n[large]").nth(1).unwrap();
    fs::write(
        s3_state.join("config.toml"),
        format!(
            r#"{s3_base}
[remote]
type = "s3"
bucket = "split-bucket"
prefix = "majutsu/v1"
endpoint = "https://example.invalid"
region = "us-test-1"
signature_version = "s3v4"

[large]{s3_large_and_after}"#
        ),
    )
    .unwrap();
    let capabilities = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&s3_state)
            .arg("remote")
            .arg("capabilities")
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy");
        c
    });
    assert!(capabilities.contains("remote s3://split-bucket/majutsu/v1"));
    assert!(capabilities.contains("lifecycle_rules true"));
}

#[test]
fn s3_remote_capabilities_honor_large_multipart_config() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg("s3://split-bucket/majutsu/v1");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("multipart = true", "multipart = false");
    fs::write(&config_path, config).unwrap();
    let capabilities = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("capabilities")
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy");
        c
    });
    assert!(capabilities.contains("multipart_upload false"));
}

#[test]
fn remote_check_uses_s3_range_get_probe() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        let metadata = serde_json::json!({
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "config": {
                "host": {"id": "range-host-id", "name": "vagrant"},
                "remote": null,
                "roots": [],
                "large": {"enabled": true, "always": [], "never": []},
                "pack": {},
                "watch": {},
                "security": {},
                "tiering": {},
                "restore": {}
            },
            "roots": [],
            "snapshots": [],
            "operations": [],
            "refs": {},
            "blobs": [],
            "large_objects": [],
            "chunks": [],
            "packs": [],
            "large_pins": []
        });
        let metadata_bytes =
            zstd::stream::encode_all(serde_json::to_vec(&metadata).unwrap().as_slice(), 3).unwrap();
        listener.set_nonblocking(true).unwrap();
        let started = std::time::Instant::now();
        let mut seen = Vec::new();
        loop {
            let Ok((mut stream, _)) = listener.accept() else {
                if started.elapsed() > Duration::from_secs(5) {
                    break;
                }
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            stream.set_nonblocking(false).unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
            }
            let header_end = request
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|idx| idx + 4)
                .unwrap_or(request.len());
            let header = String::from_utf8_lossy(&request[..header_end]).to_string();
            let first = header.lines().next().unwrap_or("").to_string();
            let range = line_header_value(&header, "Range")
                .map(|value| value.trim().to_string())
                .unwrap_or_default();
            seen.push((first.clone(), range.clone()));
            if first.starts_with("GET ") && first.contains("list-type=2") {
                let body = concat!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                    "<ListBucketResult>",
                    "<Contents><Key>majutsu/v1/vagrant/metadata/export.json.zst</Key></Contents>",
                    "</ListBucketResult>"
                );
                write_mock_http_response(&mut stream, "200 OK", body.as_bytes()).unwrap();
            } else if first.starts_with("HEAD ") {
                write_mock_http_response(&mut stream, "200 OK", b"").unwrap();
            } else if first.starts_with("GET ")
                && first.contains("/range-bucket/majutsu/v1/vagrant/metadata/export.json.zst")
            {
                if range == "bytes=0-0" {
                    write_mock_http_response(
                        &mut stream,
                        "206 Partial Content",
                        &metadata_bytes[..1],
                    )
                    .unwrap();
                    break;
                }
                write_mock_http_response(&mut stream, "200 OK", &metadata_bytes).unwrap();
            } else {
                write_mock_http_response(&mut stream, "500 Internal Server Error", b"").unwrap();
            }
        }
        tx.send(seen).unwrap();
    });

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg("s3://range-bucket/majutsu/v1");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    let before_remote = config.split("\n[remote]\n").next().unwrap();
    let after_remote = config.split("\n[large]\n").nth(1).unwrap();
    fs::write(
        &config_path,
        format!(
            r#"{before_remote}
[remote]
type = "s3"
bucket = "range-bucket"
prefix = "majutsu/v1"
endpoint = "http://{addr}"
region = "us-test-1"
signature_version = "s3v4"

[large]
{after_remote}"#
        ),
    )
    .unwrap();

    let check = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("check")
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy");
        c
    });
    assert!(check.contains("remote_type s3"));
    assert!(check.contains("endpoint_source config.remote.endpoint"));
    assert!(check.contains("region_source config.remote.region"));
    assert!(check.contains("access_key_source env:AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"));
    assert!(check.contains("secret_key_source env:AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"));
    assert!(check.contains("metadata ok"));
    assert!(check.contains("range_get 1"));

    let seen = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    server.join().unwrap();
    assert!(seen.iter().any(|(line, range)| line.starts_with("GET ")
        && line.contains("/range-bucket/majutsu/v1/vagrant/metadata/export.json.zst")
        && range == "bytes=0-0"));
}

#[cfg(unix)]
#[test]
fn restore_preserves_file_mode_and_mtime() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    let file = source.join("mode.txt");
    fs::write(&file, b"mode and mtime\n").unwrap();

    let mut perms = fs::metadata(&file).unwrap().permissions();
    perms.set_mode(0o640);
    fs::set_permissions(&file, perms).unwrap();
    filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(1_700_000_000, 0)).unwrap();
    let xattr_supported = xattr::set(&file, "user.majutsu_test", b"xattr-value").is_ok();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    let restored = restore.join("sample/mode.txt");
    let metadata = fs::metadata(&restored).unwrap();
    let original_metadata = fs::metadata(&file).unwrap();
    assert_eq!(fs::read(&restored).unwrap(), b"mode and mtime\n");
    assert_eq!(metadata.permissions().mode() & 0o777, 0o640);
    assert_eq!(metadata.uid(), original_metadata.uid());
    assert_eq!(metadata.gid(), original_metadata.gid());
    if xattr_supported {
        assert_eq!(
            xattr::get(&restored, "user.majutsu_test").unwrap(),
            Some(b"xattr-value".to_vec())
        );
    }
    assert_eq!(
        metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        1_700_000_000
    );
    let manifest = local_snapshot_manifest(&state, "desc");
    let record = manifest["roots"]["sample"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["path"] == "mode.txt")
        .unwrap();
    assert_eq!(
        record["uid"],
        serde_json::Value::from(original_metadata.uid())
    );
    assert_eq!(
        record["gid"],
        serde_json::Value::from(original_metadata.gid())
    );
}

#[cfg(unix)]
#[test]
fn snapshot_skips_unreadable_excluded_directory_before_descending() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let excluded = source.join(".devdata/postgres");
    fs::create_dir_all(&excluded).unwrap();
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();
    let mut perms = fs::metadata(&excluded).unwrap().permissions();
    perms.set_mode(0o000);
    fs::set_permissions(&excluded, perms).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--exclude")
            .arg(".devdata/**");
        c
    });
    let snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(snapshot.contains("snapshot "));

    let mut restored_perms = fs::metadata(&excluded).unwrap().permissions();
    restored_perms.set_mode(0o700);
    fs::set_permissions(&excluded, restored_perms).unwrap();
}

#[cfg(unix)]
#[test]
fn restore_preserves_empty_directory_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    let parent_dir = source.join("empty");
    let dir = source.join("empty/subdir");
    fs::create_dir_all(&dir).unwrap();

    let mut parent_perms = fs::metadata(&parent_dir).unwrap().permissions();
    parent_perms.set_mode(0o751);
    fs::set_permissions(&parent_dir, parent_perms).unwrap();
    filetime::set_file_mtime(
        &parent_dir,
        filetime::FileTime::from_unix_time(1_709_999_000, 0),
    )
    .unwrap();
    let mut perms = fs::metadata(&dir).unwrap().permissions();
    perms.set_mode(0o750);
    fs::set_permissions(&dir, perms).unwrap();
    filetime::set_file_mtime(&dir, filetime::FileTime::from_unix_time(1_710_000_000, 0)).unwrap();
    let xattr_supported = xattr::set(&dir, "user.majutsu_dir_test", b"dir-xattr").is_ok();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(snapshot.contains("files 0, large 0"));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    let restored_parent = restore.join("sample/empty");
    let restored = restore.join("sample/empty/subdir");
    let parent_metadata = fs::metadata(&restored_parent).unwrap();
    let metadata = fs::metadata(&restored).unwrap();
    assert!(parent_metadata.is_dir());
    assert!(metadata.is_dir());
    assert_eq!(parent_metadata.permissions().mode() & 0o777, 0o751);
    assert_eq!(metadata.permissions().mode() & 0o777, 0o750);
    if xattr_supported {
        assert_eq!(
            xattr::get(&restored, "user.majutsu_dir_test").unwrap(),
            Some(b"dir-xattr".to_vec())
        );
    }
    assert_eq!(
        parent_metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        1_709_999_000
    );
    assert_eq!(
        metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        1_710_000_000
    );
}

#[cfg(unix)]
#[test]
fn restore_preserves_fifo_special_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    let fifo = source.join("pipe");
    let status = Command::new("mkfifo").arg(&fifo).status().unwrap();
    assert!(status.success());

    let mut perms = fs::symlink_metadata(&fifo).unwrap().permissions();
    perms.set_mode(0o620);
    fs::set_permissions(&fifo, perms).unwrap();
    let status = Command::new("touch")
        .env("TZ", "UTC")
        .arg("-t")
        .arg("202407030946.40")
        .arg(&fifo)
        .status()
        .unwrap();
    assert!(status.success());

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(snapshot.contains("files 1, large 0"));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    let restored = restore.join("sample/pipe");
    let metadata = fs::symlink_metadata(&restored).unwrap();
    assert!(metadata.file_type().is_fifo());
    assert_eq!(metadata.permissions().mode() & 0o777, 0o620);
    assert_eq!(
        metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        1_720_000_000
    );
}

#[test]
fn restore_atomic_writes_do_not_clobber_legacy_temp_names() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("payload.bin"), vec![b'Z'; 16 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 4096");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fs::create_dir_all(restore.join("sample")).unwrap();
    fs::write(restore.join("sample/alpha.mjtmp"), b"keep blob temp\n").unwrap();
    fs::write(restore.join("sample/payload.mjtmp"), b"keep large temp\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--path")
            .arg("alpha.txt")
            .arg("--to")
            .arg(&restore);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--path")
            .arg("payload.bin")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
    assert_eq!(
        fs::read(restore.join("sample/payload.bin")).unwrap(),
        vec![b'Z'; 16 * 1024]
    );
    assert_eq!(
        fs::read(restore.join("sample/alpha.mjtmp")).unwrap(),
        b"keep blob temp\n"
    );
    assert_eq!(
        fs::read(restore.join("sample/payload.mjtmp")).unwrap(),
        b"keep large temp\n"
    );
}

#[test]
fn restore_rejects_unsafe_path_filters() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--path")
            .arg("../outside")
            .arg("--to")
            .arg(&restore);
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--path")
            .arg("/tmp")
            .arg("--to")
            .arg(&restore);
        c
    });
}

#[cfg(unix)]
#[test]
fn follow_symlinks_controls_snapshot_payload_kind() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let plain_state = tmp.path().join("plain-state");
    let follow_state = tmp.path().join("follow-state");
    let plain_restore = tmp.path().join("plain-restore");
    let follow_restore = tmp.path().join("follow-restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("target.txt"), b"target\n").unwrap();
    std::os::unix::fs::symlink("target.txt", source.join("link.txt")).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&plain_state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&plain_state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--include")
            .arg("link.txt");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&plain_state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&plain_state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&plain_restore);
        c
    });
    assert_eq!(
        fs::read_link(plain_restore.join("sample/link.txt")).unwrap(),
        std::path::PathBuf::from("target.txt")
    );

    run({
        let mut c = mj();
        c.arg("--home").arg(&follow_state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&follow_state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--include")
            .arg("link.txt")
            .arg("--follow-symlinks");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&follow_state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&follow_state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&follow_restore);
        c
    });
    let restored = follow_restore.join("sample/link.txt");
    assert!(fs::symlink_metadata(&restored).unwrap().is_file());
    assert_eq!(fs::read(&restored).unwrap(), b"target\n");
}

#[cfg(unix)]
#[test]
fn restore_force_can_replace_file_with_symlink() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("target.txt"), b"target\n").unwrap();
    std::os::unix::fs::symlink("target.txt", source.join("link.txt")).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::create_dir_all(restore.join("sample")).unwrap();
    fs::write(restore.join("sample/link.txt"), b"existing file\n").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--path")
            .arg("link.txt")
            .arg("--to")
            .arg(&restore);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--path")
            .arg("link.txt")
            .arg("--to")
            .arg(&restore)
            .arg("--force");
        c
    });
    let restored = restore.join("sample/link.txt");
    assert!(
        fs::symlink_metadata(&restored)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        fs::read_link(restored).unwrap(),
        std::path::PathBuf::from("target.txt")
    );
}

#[test]
fn restore_force_can_replace_empty_directory_with_file() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let conflicting_dir = restore.join("sample/alpha.txt");
    fs::create_dir_all(&conflicting_dir).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--path")
            .arg("alpha.txt")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(conflicting_dir.is_dir());

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--path")
            .arg("alpha.txt")
            .arg("--to")
            .arg(&restore)
            .arg("--force");
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "alpha\n"
    );
}

#[test]
fn restore_force_can_replace_file_with_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(source.join("empty")).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::create_dir_all(restore.join("sample")).unwrap();
    fs::write(restore.join("sample/empty"), b"existing file\n").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--path")
            .arg("empty")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(restore.join("sample/empty").is_file());

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--path")
            .arg("empty")
            .arg("--to")
            .arg(&restore)
            .arg("--force");
        c
    });
    assert!(restore.join("sample/empty").is_dir());
}

#[test]
fn encrypted_file_remote_clone_restores_with_exported_key() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let rotated_clone = tmp.path().join("rotated-clone");
    let restore = tmp.path().join("restore");
    let rotated_restore = tmp.path().join("rotated-restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("secret.txt"), b"secret\n").unwrap();
    fs::write(source.join("payload.zip"), vec![7u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--encrypt")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config = fs::read_to_string(state.join("config.toml")).unwrap();
    assert!(config.contains("encryption = \"age\""));
    assert!(config.contains("key_id = \"default\""));
    assert!(config.contains("hash = \"blake3-keyed\""));
    let recipients = fs::read_to_string(state.join("keys/recipients.toml")).unwrap();
    assert!(recipients.contains("age1"));
    assert!(recipients.contains("AGE-SECRET-KEY-"));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let exported_key = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("key").arg("export");
        c
    })
    .trim()
    .to_string();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let plain_oid = blake3::hash(b"secret\n").to_hex().to_string();
    assert!(
        !remote
            .join("objects/blobs")
            .join(&plain_oid[..2])
            .join(&plain_oid[2..])
            .exists()
    );
    let export: serde_json::Value = read_remote_metadata(&remote);
    let object_key = export["blobs"][0]["object_key"].as_str().unwrap();
    assert!(!object_key.contains(&plain_oid));
    let object = fs::read(remote.join(object_key)).unwrap();
    assert!(object.starts_with(b"age-encryption.org/v1"));
    let large_oid = export["large_objects"][0]["oid"].as_str().unwrap();
    let large_manifest_key = export["large_objects"][0]["manifest_key"].as_str().unwrap();
    assert!(!large_manifest_key.contains(large_oid));
    assert!(remote.join(large_manifest_key).exists());
    let manifest_alias = large_manifest_key
        .strip_prefix("objects/large/manifests/")
        .map(|rest| format!("large/manifests/{rest}.cbor.zst.enc"))
        .unwrap();
    assert!(remote.join(&manifest_alias).exists());
    let chunk_oid = export["chunks"][0]["oid"].as_str().unwrap();
    let chunk_key = export["chunks"][0]["object_key"].as_str().unwrap();
    assert!(!chunk_key.contains(chunk_oid));
    assert!(!remote.join(chunk_key).exists());
    let chunk_alias = chunk_key
        .strip_prefix("objects/large/chunks/fixed/")
        .map(|rest| format!("large/chunks/fixed-8m/{rest}.chunk.enc"))
        .unwrap();
    assert!(remote.join(&chunk_alias).exists());
    assert!(
        !remote
            .join("objects/large/chunks/fixed")
            .join(chunk_oid)
            .exists()
    );
    assert!(
        !remote
            .join("large/chunks/fixed-8m")
            .join(format!("{chunk_oid}.chunk.enc"))
            .exists()
    );
    assert!(host_remote_path(&remote, "keys/recipients.toml").exists());

    let missing_key_clone = tmp.path().join("missing-key-clone");
    let missing_key_status = mj()
        .arg("--home")
        .arg(&missing_key_clone)
        .arg("clone")
        .arg("--remote")
        .arg(format!("file://{}", remote.display()))
        .status()
        .unwrap();
    assert!(!missing_key_status.success());
    assert!(!missing_key_clone.exists());

    let status = mj()
        .env("MAJUTSU_MASTER_KEY", &exported_key)
        .arg("--home")
        .arg(&clone)
        .arg("clone")
        .arg("--remote")
        .arg(format!("file://{}", remote.display()))
        .status()
        .unwrap();
    assert!(status.success());
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(source.join("secret.txt")).unwrap(),
        fs::read(restore.join("sample/secret.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("payload.zip")).unwrap(),
        fs::read(restore.join("sample/payload.zip")).unwrap()
    );

    let new_key = "1111111111111111111111111111111111111111111111111111111111111111";
    let rotated = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("key")
            .arg("rotate")
            .arg("--new-key")
            .arg(new_key);
        c
    });
    assert!(rotated.contains("rotated master key"));
    assert!(rotated.contains(new_key));
    assert_eq!(db_operation_count(&state, "key-rotation"), 1);
    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(op_log.contains("key-rotation"), "{op_log}");
    assert!(op_log.contains("done"), "{op_log}");
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let rotated_export: serde_json::Value = read_remote_metadata(&remote);
    let rotated_object_key = rotated_export["blobs"][0]["object_key"].as_str().unwrap();
    assert_ne!(object_key, rotated_object_key);
    assert!(!rotated_object_key.contains(&plain_oid));

    let status = mj()
        .env("MAJUTSU_MASTER_KEY", new_key)
        .arg("--home")
        .arg(&rotated_clone)
        .arg("clone")
        .arg("--remote")
        .arg(format!("file://{}", remote.display()))
        .status()
        .unwrap();
    assert!(status.success());
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&rotated_clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&rotated_restore);
        c
    });
    assert_eq!(
        fs::read(source.join("secret.txt")).unwrap(),
        fs::read(rotated_restore.join("sample/secret.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("payload.zip")).unwrap(),
        fs::read(rotated_restore.join("sample/payload.zip")).unwrap()
    );
}

#[test]
fn encrypted_key_rotation_rewrites_packed_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    let clone_restore = tmp.path().join("clone-restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("secret.txt"), b"secret\n").unwrap();
    fs::write(source.join("note.txt"), b"note\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--encrypt")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    assert!(find_file_ending(&state.join("objects/packs"), ".mpack").exists());

    let new_key = "2222222222222222222222222222222222222222222222222222222222222222";
    let rotated = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("key")
            .arg("rotate")
            .arg("--new-key")
            .arg(new_key);
        c
    });
    assert!(rotated.contains("rotated master key"));
    assert!(rotated.contains("objects_rewritten "));
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let packed_blobs: i64 = conn
        .query_row(
            "select count(*) from blobs where pack_id is not null",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let packs: i64 = conn
        .query_row("select count(*) from packs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(packed_blobs, 0);
    assert_eq!(packs, 0);
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(source.join("secret.txt")).unwrap(),
        fs::read(restore.join("sample/secret.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("note.txt")).unwrap(),
        fs::read(restore.join("sample/note.txt")).unwrap()
    );
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.env("MAJUTSU_MASTER_KEY", new_key)
            .arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&clone_restore);
        c
    });
    assert_eq!(
        fs::read(source.join("secret.txt")).unwrap(),
        fs::read(clone_restore.join("sample/secret.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("note.txt")).unwrap(),
        fs::read(clone_restore.join("sample/note.txt")).unwrap()
    );
}

#[test]
fn init_creates_spec_state_layout() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });

    for path in [
        "db",
        "ops",
        "queue/events",
        "queue/uploads",
        "queue/restores",
        "cache/blobs",
        "cache/large",
        "cache/packs",
        "cache/indexes",
        "keys",
        "locks",
        "runtime",
        "logs",
    ] {
        assert!(state.join(path).is_dir(), "missing directory {path}");
    }
    assert_eq!(
        fs::read_to_string(state.join("keys/recipients.toml")).unwrap(),
        "recipients = []\n"
    );
    let log = fs::read_to_string(state.join("logs/majutsu.log")).unwrap();
    assert!(log.contains("\"kind\":\"init\""));
    assert!(log.contains("\"status\":\"done\""));
}

#[test]
fn snapshot_manifest_uses_spec_payload_variants() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    let mut chunked = Vec::new();
    for i in 0..256 * 1024 {
        chunked.push(b'a' + (i % 26) as u8);
    }
    fs::write(source.join("medium.dat"), &chunked).unwrap();
    fs::write(source.join("payload.zip"), vec![7u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("binary_min_size = 16777216", "binary_min_size = 131072")
        .replace("chunked_min_size = 524288", "chunked_min_size = 131072")
        .replace("chunk_size = 8388608", "chunk_size = 65536");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let tree_path = find_file_ending(&state.join("objects/trees"), "");
    let tree: serde_json::Value = serde_json::from_slice(&fs::read(tree_path).unwrap()).unwrap();
    assert_eq!(
        tree["entries"]["alpha.txt"]["payload"]["type"],
        "inline-small"
    );
    assert_eq!(
        tree["entries"]["medium.dat"]["payload"]["type"],
        "chunked-blob"
    );
    assert_eq!(
        tree["entries"]["medium.dat"]["payload"]["chunk_count"],
        serde_json::Value::from(4)
    );
    assert_eq!(
        tree["entries"]["payload.zip"]["payload"]["type"],
        "large-object"
    );
    assert_eq!(
        tree["entries"]["payload.zip"]["payload"]["media_type"],
        "application/zip"
    );
    assert_eq!(
        tree["entries"]["payload.zip"]["payload"]["chunking"],
        "fixed"
    );
    assert_eq!(
        tree["entries"]["payload.zip"]["payload"]["compression"],
        "per-chunk:zstd"
    );
    assert_eq!(
        tree["entries"]["payload.zip"]["payload"]["encryption"],
        "none"
    );
    assert_eq!(
        tree["entries"]["payload.zip"]["payload"]["storage_tier_hint"],
        "hot-manifest-cold-chunks"
    );
    assert_eq!(
        tree["entries"]["payload.zip"]["payload"]["hydrate_policy"],
        "on-demand"
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/medium.dat")).unwrap(),
        chunked
    );
}

#[test]
fn snapshot_lock_blocks_concurrent_snapshot_and_recovers_stale_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });

    fs::write(
        state.join("locks/snapshot.lock"),
        std::process::id().to_string(),
    )
    .unwrap();
    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fs::write(state.join("locks/snapshot.lock"), "999999").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(!state.join("locks/snapshot.lock").exists());
}

#[test]
fn diff_reports_added_modified_and_deleted_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let first_snapshot = db_ref(&state, "current").unwrap();
    let first_at = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--operations")
            .arg("--limit")
            .arg("1");
        c
    })
    .lines()
    .next()
    .and_then(|line| line.split('\t').nth(1))
    .unwrap()
    .to_string();
    thread::sleep(Duration::from_millis(20));
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();
    fs::remove_file(source.join("alpha.txt")).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let second_snapshot = db_ref(&state, "current").unwrap();
    let diff_output = mj().arg("--home").arg(&state).arg("diff").output().unwrap();
    assert!(diff_output.status.success());
    let stdout = String::from_utf8_lossy(&diff_output.stdout);
    assert!(stdout.contains("D\tsample/alpha.txt"));
    assert!(stdout.contains("A\tsample/beta.txt"));
    let colored_diff = output({
        let mut c = mj();
        c.env("MJ_COLOR", "always")
            .env_remove("NO_COLOR")
            .arg("--home")
            .arg(&state)
            .arg("diff");
        c
    });
    assert!(
        colored_diff.contains("\u{1b}[1;31mD\u{1b}[0m\t\u{1b}[1;96msample\u{1b}[0m/alpha.txt"),
        "{colored_diff:?}"
    );
    assert!(
        colored_diff.contains("\u{1b}[1;32mA\u{1b}[0m\t\u{1b}[1;96msample\u{1b}[0m/beta.txt"),
        "{colored_diff:?}"
    );
    let positional = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("diff")
            .arg(&first_snapshot)
            .arg(&second_snapshot);
        c
    });
    assert!(positional.contains("D\tsample/alpha.txt"));
    assert!(positional.contains("A\tsample/beta.txt"));
    let at_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("diff")
            .arg("--at")
            .arg(first_at);
        c
    });
    assert!(at_output.contains("D\tsample/alpha.txt"));
    assert!(at_output.contains("A\tsample/beta.txt"));
}

#[test]
fn log_root_filter_applies_limit_after_filtering() {
    let tmp = tempfile::tempdir().unwrap();
    let source_a = tmp.path().join("source-a");
    let source_b = tmp.path().join("source-b");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source_a).unwrap();
    fs::create_dir_all(&source_b).unwrap();
    fs::write(source_a.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source_b.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source_a);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("other")
            .arg(&source_b);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("pause")
            .arg("other");
        c
    });

    let log = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--root")
            .arg("sample")
            .arg("--operations")
            .arg("--limit")
            .arg("1");
        c
    });
    assert_eq!(log.lines().count(), 1);
    assert!(log.contains("root-added"));
    assert!(log.contains("sample"));
}

#[test]
fn log_root_filter_skips_operations_with_pruned_snapshot_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let before_snapshot: String = conn
        .query_row(
            "select before_snapshot
             from operations
             where kind='manual-snapshot'
               and before_snapshot is not null
               and after_snapshot is not null
             order by rowid desc
             limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    conn.execute("delete from snapshots where id=?1", [&before_snapshot])
        .unwrap();
    drop(conn);

    let log = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--root")
            .arg("sample")
            .arg("--limit")
            .arg("20")
            .arg("--full");
        c
    });
    assert!(!log.contains("Query returned no rows"), "{log}");
}

#[test]
fn log_reports_pruned_snapshot_metadata_without_root_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let before_snapshot: String = conn
        .query_row(
            "select before_snapshot
             from operations
             where kind='manual-snapshot'
               and before_snapshot is not null
               and after_snapshot is not null
             order by rowid desc
             limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    conn.execute("delete from snapshots where id=?1", [&before_snapshot])
        .unwrap();
    drop(conn);

    let log = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--limit")
            .arg("20")
            .arg("--full");
        c
    });
    assert!(log.contains("manual-snapshot"), "{log}");
    assert!(log.contains("snapshot metadata unavailable"), "{log}");
    assert!(log.contains("[metadata-unavailable]"), "{log}");
}

#[test]
fn prune_dry_run_and_gc_are_safe_entry_points() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("prune").arg("--dry-run");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn restore_without_to_can_write_back_to_original_root() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let first = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    fs::write(source.join("beta.txt"), b"extra\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let plan = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--snapshot")
            .arg(&first)
            .arg("--root")
            .arg("sample");
        c
    });
    assert!(plan.contains("target original-roots"));
    assert!(plan.contains("conflicts 1"));
    assert!(plan.contains("delete 1 files"));
    assert!(plan.contains("restore_files 0"));
    assert!(plan.contains("modify_files 1"));
    assert!(plan.contains("keep_files 0"));
    assert!(plan.contains("delete_files 1"));
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--snapshot")
            .arg(&first)
            .arg("--root")
            .arg("sample");
        c
    });
    assert_eq!(fs::read(source.join("alpha.txt")).unwrap(), b"two\n");
    assert_eq!(fs::read(source.join("beta.txt")).unwrap(), b"extra\n");
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--snapshot")
            .arg(&first)
            .arg("--root")
            .arg("sample")
            .arg("--force");
        c
    });
    assert_eq!(fs::read(source.join("alpha.txt")).unwrap(), b"one\n");
    assert!(!source.join("beta.txt").exists());
}

#[test]
fn restore_can_explicitly_skip_conflict_checks() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let first = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    fs::write(source.join("beta.txt"), b"extra\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--snapshot")
            .arg(&first)
            .arg("--root")
            .arg("sample");
        c
    });

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--snapshot")
            .arg(&first)
            .arg("--root")
            .arg("sample")
            .arg("--check-conflicts=false");
        c
    });

    assert_eq!(fs::read(source.join("alpha.txt")).unwrap(), b"one\n");
    assert!(!source.join("beta.txt").exists());
}

#[test]
fn restore_without_subcommand_applies_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
}

#[test]
fn restore_at_accepts_spec_datetime_formats() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore_datetime = tmp.path().join("restore-datetime");
    let restore_date = tmp.path().join("restore-date");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--at")
            .arg("2999-01-01 00:00:00")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore_datetime);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--at")
            .arg("2999-01-01")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore_date);
        c
    });

    assert_eq!(
        fs::read(restore_datetime.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
    assert_eq!(
        fs::read(restore_date.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
}

#[test]
fn relative_time_arguments_work_for_diff_and_restore() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    let restore_ago = tmp.path().join("restore-ago");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let first_snapshot = db_ref(&state, "current").unwrap();
    let old_created_at = (Utc::now() - ChronoDuration::minutes(20)).to_rfc3339();
    Connection::open(state.join("db/majutsu.sqlite"))
        .unwrap()
        .execute(
            "update snapshots set created_at=?2 where id=?1",
            rusqlite::params![first_snapshot, old_created_at],
        )
        .unwrap();
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let diff = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("diff")
            .arg("--at")
            .arg("10 minutes ago");
        c
    });
    assert!(diff.contains("M\tsample/alpha.txt"));
    assert!(diff.contains("A\tsample/beta.txt"));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--ago")
            .arg("10m")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore_ago);
        c
    });
    assert_eq!(
        fs::read(restore_ago.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
    assert!(!restore_ago.join("sample/beta.txt").exists());

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--at")
            .arg("now")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"changed\n"
    );
    assert_eq!(
        fs::read(restore.join("sample/beta.txt")).unwrap(),
        b"beta\n"
    );
}

#[test]
fn prune_can_delete_unkept_snapshots_and_gc_their_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let first = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    let second = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    fs::write(source.join("alpha.txt"), b"three\n").unwrap();
    let third = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();

    let dry_run = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run")
            .arg("--keep-daily")
            .arg("1")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    assert!(dry_run.contains("dry_run true"));
    assert!(dry_run.contains("candidate_snapshots 1"));
    assert_eq!(db_operation_count(&state, "prune"), 0);
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--snapshot")
            .arg(&first)
            .arg("--to")
            .arg(&restore);
        c
    });

    let prune = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--keep-daily")
            .arg("1")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    assert!(prune.contains("deleted_snapshots 1"));
    assert_eq!(db_operation_count(&state, "prune"), 1);
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--snapshot")
            .arg(&first)
            .arg("--to")
            .arg(&restore);
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--snapshot")
            .arg(&second)
            .arg("--to")
            .arg(&restore);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    assert_eq!(db_operation_count(&state, "gc"), 1);
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--snapshot")
            .arg(&third)
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"three\n"
    );
}

#[test]
fn prune_can_drop_unprotected_history_with_missing_remote_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let first = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    let second = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    assert_ne!(first, second);
    let old_blob = local_snapshot_file_object_key(&state, &second, "sample", "alpha.txt");
    fs::write(source.join("alpha.txt"), b"three\n").unwrap();
    let third = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    assert_ne!(second, third);
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let removed = remove_remote_payload_aliases(&remote, &old_blob);
    assert!(
        !removed.is_empty(),
        "expected old blob aliases to be removed"
    );

    let dry_run = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run")
            .arg("--keep-daily")
            .arg("90")
            .arg("--keep-monthly")
            .arg("36")
            .arg("--drop-missing-remote-history");
        c
    });
    assert!(dry_run.contains("dry_run true"), "{dry_run}");
    assert!(
        dry_run.contains("missing_remote_history_snapshots 1"),
        "{dry_run}"
    );
    assert!(
        dry_run.contains(&format!("missing_remote_history_snapshot {second}")),
        "{dry_run}"
    );

    let prune = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run=false")
            .arg("--keep-daily")
            .arg("90")
            .arg("--keep-monthly")
            .arg("36")
            .arg("--drop-missing-remote-history");
        c
    });
    assert!(prune.contains("deleted_snapshots 1"), "{prune}");
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--snapshot")
            .arg(&first)
            .arg("--to")
            .arg(tmp.path().join("restore-old"));
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--snapshot")
            .arg(&second)
            .arg("--to")
            .arg(tmp.path().join("restore-middle"));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--snapshot")
            .arg(&third)
            .arg("--to")
            .arg(tmp.path().join("restore-current"));
        c
    });
}

#[test]
fn sync_prunes_stale_remote_host_exports_after_prune() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"three\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let host_dir = first_remote_host_dir(&remote);
    let before = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            let path = entry.path();
            path.extension().and_then(|ext| ext.to_str()) == Some("json")
                || path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".cbor.zst.enc"))
        })
        .count();
    assert!(before >= 2);

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run=false")
            .arg("--keep-daily")
            .arg("0")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("pruned_remote_exports "));
    fs::create_dir_all(
        host_remote_path(&remote, "indexes/chunk-index/stale-shard.cbor.zst.enc")
            .parent()
            .unwrap(),
    )
    .unwrap();
    fs::write(
        host_remote_path(&remote, "indexes/chunk-index/stale-shard.cbor.zst.enc"),
        b"stale chunk index",
    )
    .unwrap();
    let sync = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE_FORCE", "1");
        c
    });
    assert!(output_metric(&sync, "pruned_remote_objects") > 0, "{sync}");
    assert!(!host_remote_path(&remote, "indexes/chunk-index/stale-shard.cbor.zst.enc").exists());
    let after = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            let path = entry.path();
            path.extension().and_then(|ext| ext.to_str()) == Some("json")
                || path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".cbor.zst.enc"))
        })
        .count();
    assert!(after < before, "before={before} after={after}");
    assert!(
        find_file_ending(
            &first_remote_host_dir(&remote).join("gc/tombstones"),
            ".json"
        )
        .exists()
    );
}

#[test]
fn prune_remote_cleanup_prunes_stale_remote_exports_immediately() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"three\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let host_dir = first_remote_host_dir(&remote);
    let before = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .filter_map(Result::ok)
        .count();
    assert!(before >= 2);

    let prune = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run=false")
            .arg("--keep-daily")
            .arg("0")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    assert!(prune.contains("remote_cleanup true"));
    assert!(prune.contains("pruned_remote_exports "));
    assert!(
        find_file_ending(
            &first_remote_host_dir(&remote).join("gc/tombstones"),
            ".json"
        )
        .exists()
    );
}

#[test]
fn remote_fsck_detects_corrupt_gc_tombstone() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"three\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run=false")
            .arg("--keep-daily")
            .arg("0")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    let tombstone = find_file_ending(
        &first_remote_host_dir(&remote).join("gc/tombstones"),
        ".json",
    );
    fs::write(tombstone, b"{not valid json").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_unknown_host_gc_mark() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::create_dir_all(remote.join("ghost-host/gc")).unwrap();
    fs::write(
        remote.join("ghost-host/gc/mark.json"),
        serde_json::json!({
            "version": 1,
            "host_id": "ghost-host",
            "marked_at": "2026-06-08T00:00:00Z",
            "current_snapshot": null,
            "object_keys": []
        })
        .to_string(),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_unknown_host_prefix_export() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::create_dir_all(remote.join("ghost-host/metadata")).unwrap();
    fs::write(remote.join("ghost-host/metadata/export.json"), b"{}").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_detects_unknown_host_gc_tombstone() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::create_dir_all(remote.join("ghost-host/gc/tombstones")).unwrap();
    fs::write(
        remote.join("ghost-host/gc/tombstones/tombstone-1.json"),
        serde_json::json!({
            "version": 1,
            "host_id": "ghost-host",
            "deleted_at": "2026-06-08T00:00:00Z",
            "key": "objects/deleted"
        })
        .to_string(),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_corrupt_gc_tombstone_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"three\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run=false")
            .arg("--keep-daily")
            .arg("0")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    let tombstone = find_file_ending(
        &first_remote_host_dir(&remote).join("gc/tombstones"),
        ".json",
    );
    fs::write(tombstone, b"{not valid json").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn clone_accepts_valid_gc_tombstone_after_remote_prune() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"three\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run=false")
            .arg("--keep-daily")
            .arg("0")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE", "1");
        c
    });
    assert!(
        find_file_ending(
            &first_remote_host_dir(&remote).join("gc/tombstones"),
            ".json"
        )
        .exists()
    );

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "three\n"
    );
}

#[test]
fn pack_gc_and_remote_clone_restore_packed_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    assert!(
        find_file_ending(&state.join("objects/packs/small"), ".mpack")
            .to_string_lossy()
            .contains("objects/packs/small/")
    );
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    assert!(find_file_ending(&state.join("objects/packs"), ".mpack").exists());
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(find_file_ending(&remote.join("packs/small"), ".mpack").exists());
    let pack_index = find_file_ending(&remote.join("indexes/pack-index"), ".cbor.zst.enc");
    assert_canonical_cbor_zstd(&pack_index);
    assert!(!pack_index.to_string_lossy().ends_with(".json"));
    fs::remove_dir_all(remote.join("objects")).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(source.join("alpha.txt")).unwrap(),
        fs::read(restore.join("sample/alpha.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("beta.txt")).unwrap(),
        fs::read(restore.join("sample/beta.txt")).unwrap()
    );
}

#[test]
fn sync_prunes_remote_loose_blobs_after_pack() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let object_key = first_blob_object_key(&state);
    let canonical_key = canonical_loose_blob_key(&object_key);
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_AUTO_PACK", "0")
            .env("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE", "0");
        c
    });
    assert!(remote.join(&object_key).exists());
    assert!(remote.join(&canonical_key).exists());

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(
        output_metric(&sync, "pruned_remote_objects") > 0,
        "sync should prune stale loose blob aliases by default:\n{sync}"
    );
    assert!(!remote.join(&object_key).exists());
    assert!(!remote.join(&canonical_key).exists());

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
    assert_eq!(
        fs::read(restore.join("sample/beta.txt")).unwrap(),
        b"beta\n"
    );
}

#[test]
fn sync_prunes_local_loose_blobs_after_auto_pack() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let object_key = first_blob_object_key(&state);
    assert!(state.join(&object_key).exists());

    let sync = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_AUTO_PACK_MIN_BLOBS", "1")
            .env("MAJUTSU_SYNC_LOCAL_PRUNE_MIN_AGE_SECS", "0");
        c
    });
    assert!(sync.contains("auto_pack unpacked_small_blobs "));
    assert!(sync.contains("pruned_local_objects "));
    assert!(sync.contains("pruned_payload_cache_objects "));
    assert!(!state.join(&object_key).exists());
    assert!(
        walkdir::WalkDir::new(state.join("objects/packs/small"))
            .into_iter()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_type().is_file())
    );
    assert!(find_file_ending(&remote.join("packs/small"), ".mpack").exists());

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
    assert_eq!(
        fs::read(restore.join("sample/beta.txt")).unwrap(),
        b"beta\n"
    );
}

#[test]
fn sync_auto_pack_ignores_missing_unreferenced_loose_blob_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let missing_key = "objects/blobs/ff/missing-auto-pack-test";
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.execute(
        "insert into blobs(oid, size, object_key) values (?1, ?2, ?3)",
        rusqlite::params!["missing-auto-pack-test", 7_i64, missing_key],
    )
    .unwrap();
    drop(conn);
    assert!(!state.join(missing_key).exists());

    let sync = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_AUTO_PACK_MIN_BLOBS", "1");
        c
    });
    assert!(
        sync.contains("skipped_missing_loose_blobs 1"),
        "sync should not fail on stale loose blob metadata:\n{sync}"
    );
    assert!(
        sync.contains("removed_missing_unreferenced_blobs 1"),
        "sync should clean unreferenced stale loose blob metadata:\n{sync}"
    );
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let remaining: i64 = conn
        .query_row(
            "select count(*) from blobs where oid='missing-auto-pack-test'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 0);
}

#[test]
fn sync_keeps_remote_loose_blob_referenced_by_gc_mark() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let object_key = first_blob_object_key(&state);
    let canonical_key = canonical_loose_blob_key(&object_key);
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_AUTO_PACK", "0");
        c
    });
    let host_id = current_host_id(&state);
    let host_gc_mark = first_remote_host_dir(&remote).join("gc/mark.json");
    fs::create_dir_all(host_gc_mark.parent().unwrap()).unwrap();
    fs::write(
        &host_gc_mark,
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "host_id": host_id,
            "marked_at": Utc::now(),
            "current_snapshot": null,
            "object_keys": [object_key.clone(), canonical_key.clone()],
        }))
        .unwrap(),
    )
    .unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("pruned_remote_objects "), "{sync}");
    assert!(remote.join(first_blob_object_key(&state)).exists());
    assert!(remote.join(canonical_key).exists());
}

#[test]
fn fsck_quick_and_timeout_are_available() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let quick = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck").arg("--quick");
        c
    });
    assert!(quick.contains("fsck ok"));

    let sampled = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("fsck")
            .arg("--sample")
            .arg("1");
        c
    });
    assert!(sampled.contains("fsck ok"));

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("fsck")
            .arg("--timeout-secs")
            .arg("0");
        c
    });
}

#[test]
fn fsck_backfill_index_rebuilds_missing_payload_index() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.execute("delete from snapshot_payloads", []).unwrap();
        conn.execute("delete from snapshot_payload_index", [])
            .unwrap();
    }

    let backfill = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("fsck")
            .arg("--backfill-index")
            .arg("--hydrate-index-objects")
            .arg("--sample")
            .arg("1");
        c
    });
    assert!(backfill.contains("backfill_indexed_snapshots 1"));
    assert!(backfill.contains("backfill index ok"));
    {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        let indexed: i64 = conn
            .query_row("select count(*) from snapshot_payload_index", [], |row| {
                row.get(0)
            })
            .unwrap();
        let payloads: i64 = conn
            .query_row("select count(*) from snapshot_payloads", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(indexed, 1);
        assert_eq!(payloads, 1);
    }
}

#[test]
fn fsck_since_limits_heavy_checks_to_recent_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("old.txt"), b"old\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let old_oid = blake3::hash(b"old\n").to_hex().to_string();
    let old_object_key: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row(
            "select object_key from blobs where oid=?1",
            [&old_oid],
            |row| row.get(0),
        )
        .unwrap()
    };

    fs::remove_file(source.join("old.txt")).unwrap();
    fs::write(source.join("new.txt"), b"new\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        let indexed: i64 = conn
            .query_row("select count(*) from snapshot_payload_index", [], |row| {
                row.get(0)
            })
            .unwrap();
        let payloads: i64 = conn
            .query_row("select count(*) from snapshot_payloads", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(indexed, 2);
        assert_eq!(payloads, 2);
    }
    let since: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row(
            "select created_at from snapshots order by created_at desc limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    fs::remove_file(state.join(&old_object_key)).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let scoped = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("fsck")
            .arg("--since")
            .arg(&since)
            .arg("--sample")
            .arg("1");
        c
    });
    assert!(scoped.contains("fsck ok"));
}

#[test]
fn fsck_detects_corrupt_local_pack_index() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let index_path = find_file_ending(&state.join("objects/indexes/pack"), ".json");
    let mut index: serde_json::Value =
        serde_json::from_slice(&fs::read(&index_path).unwrap()).unwrap();
    index["entries"][0]["offset"] = serde_json::Value::from(999_999u64);
    fs::write(&index_path, serde_json::to_vec_pretty(&index).unwrap()).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_dangling_blob_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let object_key: String = conn
        .query_row("select object_key from blobs limit 1", [], |row| row.get(0))
        .unwrap();
    conn.execute(
        "insert into blobs(oid, size, object_key) values (?1, ?2, ?3)",
        rusqlite::params!["dangling-blob", 5u64, object_key],
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_corrupt_local_snapshot_manifest_object() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let manifest_key: String = conn
        .query_row("select manifest_key from snapshots limit 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    fs::write(state.join(manifest_key), b"{not valid json").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_corrupt_local_tree_manifest_object() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let manifest = local_snapshot_manifest(&state, "asc");
    let tree_key = manifest["root_trees"]["sample"]["tree_key"]
        .as_str()
        .unwrap();
    let mut tree: serde_json::Value =
        serde_json::from_slice(&fs::read(state.join(tree_key)).unwrap()).unwrap();
    tree["tree_id"] = serde_json::Value::String("tree-corrupt".into());
    fs::write(
        state.join(tree_key),
        serde_json::to_vec_pretty(&tree).unwrap(),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_corrupt_local_large_manifest_object() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.zip"), vec![7u8; 32 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let manifest_key: String = conn
        .query_row(
            "select manifest_key from large_objects limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(state.join(&manifest_key)).unwrap()).unwrap();
    manifest["oid"] = serde_json::Value::String("corrupt-large-oid".into());
    fs::write(
        state.join(manifest_key),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn remote_fsck_detects_corrupt_canonical_pack_index() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let pack_index = find_file_ending(&remote.join("indexes/pack-index"), ".cbor.zst.enc");
    rewrite_canonical_cbor_zstd(&pack_index, |value| {
        value["entries"][0]["offset"] = serde_json::Value::from(999_999u64);
    });

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_corrupt_canonical_pack_index_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let pack_index = find_file_ending(&remote.join("indexes/pack-index"), ".cbor.zst.enc");
    rewrite_canonical_cbor_zstd(&pack_index, |value| {
        value["entries"][0]["offset"] = serde_json::Value::from(999_999u64);
    });

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_unexpected_canonical_pack_index() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let pack_index = find_file_ending(&remote.join("indexes/pack-index"), ".cbor.zst.enc");
    fs::copy(
        &pack_index,
        pack_index.parent().unwrap().join("stale.cbor.zst.enc"),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_unexpected_canonical_pack_index_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let pack_index = find_file_ending(&remote.join("indexes/pack-index"), ".cbor.zst.enc");
    fs::copy(
        &pack_index,
        pack_index.parent().unwrap().join("stale.cbor.zst.enc"),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn remote_fsck_detects_unexpected_canonical_pack_object() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let pack_object = find_file_ending(&remote.join("packs"), ".mpack");
    fs::copy(
        &pack_object,
        pack_object.parent().unwrap().join("stale.mpack"),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn clone_rejects_unexpected_canonical_pack_object_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let pack_object = find_file_ending(&remote.join("packs"), ".mpack");
    fs::copy(
        &pack_object,
        pack_object.parent().unwrap().join("stale.mpack"),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn pack_compaction_rewrites_existing_packs() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    assert!(count_files_ending(&state.join("objects/packs"), ".mpack") >= 2);
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack").arg("--compact");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    assert_eq!(
        count_files_ending(&state.join("objects/packs"), ".mpack"),
        1
    );
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(source.join("alpha.txt")).unwrap(),
        fs::read(restore.join("sample/alpha.txt")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("beta.txt")).unwrap(),
        fs::read(restore.join("sample/beta.txt")).unwrap()
    );
}

#[test]
fn pack_compaction_separates_current_blobs_from_history_only_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("kept.txt"), vec![b'a'; 4096]).unwrap();
    fs::write(source.join("old.txt"), vec![b'b'; 4096]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace(
            "small_pack_target = 67108864",
            "small_pack_target = \"16 KiB\"",
        )
        .replace(
            "normal_pack_target = 268435456",
            "normal_pack_target = \"16 KiB\"",
        );
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });

    fs::remove_file(source.join("old.txt")).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack").arg("--compact");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let current: String = conn
        .query_row("select value from refs where name='current'", [], |row| {
            row.get(0)
        })
        .unwrap();
    let mut current_stmt = conn
        .prepare(
            "select distinct b.pack_id
             from blobs b
             join snapshot_payloads sp on sp.kind='blob' and sp.oid=b.oid
             where sp.snapshot_id=?1 and b.pack_id is not null
             order by b.pack_id",
        )
        .unwrap();
    let current_pack_ids = current_stmt
        .query_map([current.as_str()], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut history_stmt = conn
        .prepare(
            "select distinct b.pack_id
             from blobs b
             where b.pack_id is not null
               and b.oid not in (
                 select oid from snapshot_payloads where snapshot_id=?1 and kind='blob'
               )
             order by b.pack_id",
        )
        .unwrap();
    let history_pack_ids = history_stmt
        .query_map([current.as_str()], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(!current_pack_ids.is_empty());
    assert!(!history_pack_ids.is_empty());
    assert!(
        current_pack_ids
            .iter()
            .all(|pack_id| !history_pack_ids.contains(pack_id)),
        "current blobs should not share packs with history-only blobs: current={current_pack_ids:?} history={history_pack_ids:?}"
    );
}

#[test]
fn pack_respects_configured_normal_pack_target() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    for idx in 0..4 {
        fs::write(
            source.join(format!("file-{idx}.txt")),
            format!("payload-{idx}-abcdefghijklmnopqrstuvwxyz\n"),
        )
        .unwrap();
    }

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace(
            "small_pack_target = 67108864",
            "small_pack_target = \"64 B\"",
        )
        .replace(
            "normal_pack_target = 268435456",
            "normal_pack_target = \"64 B\"",
        );
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let pack_output = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    assert!(pack_output.contains("pack(s)"));
    let pack_count = count_files_ending(&state.join("objects/packs"), ".mpack");
    assert!(pack_count > 1);
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/file-3.txt")).unwrap(),
        "payload-3-abcdefghijklmnopqrstuvwxyz\n"
    );
}

#[test]
fn op_restore_prepare_resume_and_lifecycle_policy_are_available() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    let snapshot_op = op_log
        .lines()
        .find(|line| line.contains("initial-scan"))
        .and_then(|line| line.split('\t').next())
        .unwrap()
        .to_string();
    let op_show = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("op")
            .arg("show")
            .arg(&snapshot_op);
        c
    });
    assert!(op_show.contains("kind initial-scan"));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("op")
            .arg("restore")
            .arg(&snapshot_op);
        c
    });

    let prepare = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(prepare.contains("required_objects "));
    assert!(prepare.contains("archived_objects 0"));
    let job_id = prepare
        .lines()
        .find_map(|line| line.strip_prefix("restore_job "))
        .unwrap()
        .to_string();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("resume")
            .arg(&job_id);
        c
    });
    assert_eq!(
        fs::read(source.join("alpha.txt")).unwrap(),
        fs::read(restore.join("sample/alpha.txt")).unwrap()
    );

    let policy = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("lifecycle")
            .arg("policy")
            .arg("--provider")
            .arg("gcs");
        c
    });
    assert!(policy.contains("packs/normal/"));
    assert!(policy.contains("large/chunks/fixed-8m/"));
}

#[test]
fn op_restore_moves_current_ref_to_operation_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    let restore_after_op = tmp.path().join("restore-after-op");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let first_status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status");
        c
    });
    let first_snapshot = first_status
        .lines()
        .find_map(|line| line.strip_prefix("current "))
        .unwrap()
        .to_string();

    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let second_status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status");
        c
    });
    let second_snapshot = second_status
        .lines()
        .find_map(|line| line.strip_prefix("current "))
        .unwrap()
        .to_string();
    assert_ne!(first_snapshot, second_snapshot);

    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    let second_snapshot_op = op_log
        .lines()
        .filter(|line| line.contains("manual-snapshot"))
        .find(|line| line.contains(&format!("{first_snapshot} -> {second_snapshot}\t")))
        .and_then(|line| line.split('\t').next())
        .unwrap()
        .to_string();
    let second_snapshot_op_prefix = &second_snapshot_op[..12];

    let op_show = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("op")
            .arg("show")
            .arg(second_snapshot_op_prefix);
        c
    });
    assert!(op_show.contains(&format!("id {second_snapshot_op}")));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--op")
            .arg(second_snapshot_op_prefix)
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore_after_op);
        c
    });
    assert_eq!(
        fs::read_to_string(restore_after_op.join("sample/alpha.txt")).unwrap(),
        "two\n"
    );

    let restore_out = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("op")
            .arg("restore")
            .arg(second_snapshot_op_prefix);
        c
    });
    assert!(restore_out.contains(&format!("current {first_snapshot}")));

    let restored_status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status");
        c
    });
    assert!(restored_status.contains(&format!("current {first_snapshot}")));
    assert!(!restored_status.contains(&format!("current {second_snapshot}")));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "one\n"
    );

    let op_log_after_restore = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(op_log_after_restore.contains("op-restore"));
    assert!(op_log_after_restore.contains(&second_snapshot));
}

#[test]
fn lifecycle_policy_uses_tiering_config_rules() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    let base = config.split("\n[tiering]").next().unwrap();
    fs::write(
        &config_path,
        format!(
            r#"{base}
[tiering]
enabled = true

[[tiering.rules]]
name = "keep-host-metadata-hot"
prefix = "host-a/metadata/"
after = "1d"
storage = "deep-archive"

[[tiering.rules]]
name = "keep-trees-hot"
prefix = "trees/"
after = "1d"
transition_to = "archive"

[[tiering.rules]]
name = "custom-packs-to-ia"
prefix = "objects/packs/normal/"
after = "14d"
transition_to = "infrequent"

[[tiering.rules]]
name = "custom-large-to-deep-archive"
prefix = "objects/large/chunks/fixed/"
after = "365d"
storage = "deep-archive"
"#
        ),
    )
    .unwrap();

    let s3_policy = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("lifecycle")
            .arg("policy")
            .arg("--provider")
            .arg("s3");
        c
    });
    assert!(s3_policy.contains("\"ID\": \"custom-packs-to-ia\""));
    assert!(s3_policy.contains("\"Days\": 14"));
    assert!(s3_policy.contains("\"StorageClass\": \"STANDARD_IA\""));
    assert!(s3_policy.contains("\"Days\": 365"));
    assert!(s3_policy.contains("\"StorageClass\": \"DEEP_ARCHIVE\""));
    assert!(!s3_policy.contains("\"Prefix\": \"host-a/metadata/\""));
    assert!(!s3_policy.contains("\"Prefix\": \"trees/\""));

    let gcs_policy = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("lifecycle")
            .arg("policy")
            .arg("--provider")
            .arg("gcs");
        c
    });
    assert!(gcs_policy.contains("\"age\": 14"));
    assert!(gcs_policy.contains("\"storageClass\": \"NEARLINE\""));
    assert!(gcs_policy.contains("\"age\": 365"));
    assert!(gcs_policy.contains("\"storageClass\": \"ARCHIVE\""));
    assert!(!gcs_policy.contains("\"host-a/metadata/\""));
    assert!(!gcs_policy.contains("\"trees/\""));

    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("lifecycle").arg("status");
        c
    });
    assert!(status.contains("remote file://"));
    assert!(status.contains("lifecycle_rules false"));
    assert!(status.contains("restore_archived_object true"));
    assert!(status.contains("multipart_upload false"));
    assert!(status.contains("range_get true"));
    assert!(status.contains("conditional_put true"));
    assert!(status.contains("policy_rules_s3 2"));
    assert!(status.contains("policy_rules_gcs 2"));

    let dry_run = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("lifecycle")
            .arg("apply")
            .arg("--provider")
            .arg("s3");
        c
    });
    assert!(dry_run.contains("dry_run true"));
    assert!(dry_run.contains("apply_hint aws s3api put-bucket-lifecycle-configuration"));
    assert!(dry_run.contains("apply_warning remote does not advertise lifecycle rule support"));
    assert_eq!(db_operation_count(&state, "lifecycle-apply"), 0);

    let applied = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("lifecycle")
            .arg("apply")
            .arg("--provider")
            .arg("s3")
            .arg("--dry-run")
            .arg("false");
        c
    });
    assert!(applied.contains("applied true"));
    assert!(remote.join("lifecycle/policy-s3.json").exists());
    assert!(remote.join("lifecycle/status.json").exists());
    let applied_policy = fs::read_to_string(remote.join("lifecycle/policy-s3.json")).unwrap();
    assert!(applied_policy.contains("\"ID\": \"custom-packs-to-ia\""));
    let applied_status: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("lifecycle/status.json")).unwrap()).unwrap();
    assert_eq!(applied_status["provider"], "s3");
    assert_eq!(applied_status["policy_key"], "lifecycle/policy-s3.json");
    assert_eq!(applied_status["provider_applied"], false);
    assert_eq!(db_operation_count(&state, "lifecycle-apply"), 1);
    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(op_log.contains("lifecycle-apply"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn lifecycle_apply_stores_gcs_policy_and_remote_fsck_validates_it() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let applied = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("lifecycle")
            .arg("apply")
            .arg("--provider")
            .arg("gcs")
            .arg("--dry-run")
            .arg("false");
        c
    });
    assert!(applied.contains("provider gcs"));
    assert!(applied.contains("policy_key lifecycle/policy-gcs.json"));
    assert!(applied.contains("provider_applied false"));
    assert!(remote.join("lifecycle/policy-gcs.json").exists());
    assert!(remote.join("lifecycle/status.json").exists());

    let policy: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("lifecycle/policy-gcs.json")).unwrap())
            .unwrap();
    assert!(policy.get("rule").is_some_and(|rule| rule.is_array()));
    let status: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("lifecycle/status.json")).unwrap()).unwrap();
    assert_eq!(status["provider"], "gcs");
    assert_eq!(status["policy_key"], "lifecycle/policy-gcs.json");
    assert_eq!(status["provider_applied"], false);

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });

    fs::write(remote.join("lifecycle/policy-gcs.json"), br#"{"rules":[]}"#).unwrap();
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn remote_fsck_validates_lifecycle_artifacts() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("lifecycle")
            .arg("apply")
            .arg("--provider")
            .arg("s3")
            .arg("--dry-run")
            .arg("false");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });

    let mut status: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("lifecycle/status.json")).unwrap()).unwrap();
    status["policy_key"] = serde_json::Value::String("lifecycle/missing.json".into());
    fs::write(
        remote.join("lifecycle/status.json"),
        serde_json::to_vec_pretty(&status).unwrap(),
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone.exists());
}

#[test]
fn lifecycle_apply_puts_s3_bucket_lifecycle_configuration() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg("s3://life-bucket/majutsu/v1");
        c
    });

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        let started = std::time::Instant::now();
        let mut seen = Vec::new();
        let mut saw_provider_apply = false;
        let mut saw_status_artifact = false;
        loop {
            let Ok((mut stream, _)) = listener.accept() else {
                if started.elapsed() > Duration::from_secs(5) {
                    break;
                }
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            stream.set_nonblocking(false).unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
            }
            let header_end = request
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|idx| idx + 4)
                .unwrap_or(request.len());
            let header = String::from_utf8_lossy(&request[..header_end]).to_string();
            let content_len = line_header_value(&header, "Content-Length")
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            while request.len() < header_end + content_len {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
            }
            let first = header.lines().next().unwrap_or("").to_string();
            let body = String::from_utf8_lossy(&request[header_end..]).to_string();
            if first.starts_with("PUT ") && first.contains("?lifecycle") {
                saw_provider_apply = true;
            }
            if first.starts_with("PUT ") && first.contains("/majutsu/v1/lifecycle/status.json") {
                saw_status_artifact = true;
            }
            seen.push((first, body));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
            if saw_provider_apply && saw_status_artifact {
                break;
            }
        }
        tx.send(seen).unwrap();
    });

    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    let before_remote = config.split("\n[remote]\n").next().unwrap();
    let after_remote = config.split("\n[large]\n").nth(1).unwrap();
    fs::write(
        &config_path,
        format!(
            r#"{before_remote}
[remote]
type = "s3"
bucket = "life-bucket"
prefix = "majutsu/v1"
endpoint = "http://{addr}"
region = "us-test-1"
signature_version = "s3v4"

[large]
{after_remote}

[[tiering.rules]]
name = "custom-packs-to-ia"
prefix = "objects/packs/normal/"
after = "14d"
storage = "infrequent"
"#
        ),
    )
    .unwrap();

    let applied = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("lifecycle")
            .arg("apply")
            .arg("--provider")
            .arg("s3")
            .arg("--dry-run")
            .arg("false")
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy");
        c
    });
    assert!(applied.contains("applied true"));
    assert!(applied.contains("provider_applied true"));

    let seen = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    server.join().unwrap();
    let lifecycle_put = seen
        .iter()
        .find(|(line, _)| line.starts_with("PUT ") && line.contains("?lifecycle"))
        .expect("missing S3 lifecycle PUT request");
    assert!(lifecycle_put.0.contains("/life-bucket/"));
    assert!(lifecycle_put.1.contains("<LifecycleConfiguration>"));
    assert!(lifecycle_put.1.contains("<ID>custom-packs-to-ia</ID>"));
    assert!(
        lifecycle_put
            .1
            .contains("<Prefix>majutsu/v1/objects/packs/normal/</Prefix>")
    );
    assert!(lifecycle_put.1.contains("<Days>14</Days>"));
    assert!(
        lifecycle_put
            .1
            .contains("<StorageClass>STANDARD_IA</StorageClass>")
    );

    let status_put = seen
        .iter()
        .find(|(line, _)| line.contains("/majutsu/v1/lifecycle/status.json"))
        .expect("missing lifecycle status artifact");
    assert!(status_put.1.contains("\"provider_applied\": true"));
}

#[test]
fn restore_prepare_requests_archive_for_missing_local_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_AUTO_PACK", "0")
            .env("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE", "0");
        c
    });
    let alpha_oid = blake3::hash(b"alpha\n").to_hex().to_string();
    let object = state
        .join("objects/blobs")
        .join(&alpha_oid[..2])
        .join(&alpha_oid[2..]);
    fs::remove_file(object).unwrap();

    let plan = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(plan.contains("archived_objects 1"));
    assert!(plan.contains("missing_objects 0"));
    assert!(plan.contains("archive_or_missing_objects 1"));

    let prepare = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(prepare.contains("archived_objects 1"));
    assert!(prepare.contains("missing_objects 0"));
    assert!(prepare.contains("archive_requested_objects 1"));
    let job = fs::read_to_string(
        fs::read_dir(state.join("queue/restores"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path(),
    )
    .unwrap();
    assert!(job.contains("\"status\": \"archive-requested\""));
    let job_id = prepare
        .lines()
        .find_map(|line| line.strip_prefix("restore_job "))
        .unwrap()
        .to_string();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("resume")
            .arg(&job_id);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
    let job = fs::read_to_string(
        fs::read_dir(state.join("queue/restores"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path(),
    )
    .unwrap();
    assert!(job.contains("\"status\": \"done\""));
    assert!(job.contains("\"archived_objects\": []"));
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("resume")
            .arg(&job_id);
        c
    });
}

#[test]
fn restore_prepare_requests_s3_archive_restore_via_provider() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_AUTO_PACK", "0")
            .env("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE", "0");
        c
    });

    let alpha_oid = blake3::hash(b"alpha\n").to_hex().to_string();
    let object = state
        .join("objects/blobs")
        .join(&alpha_oid[..2])
        .join(&alpha_oid[2..]);
    fs::remove_file(object).unwrap();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        let started = std::time::Instant::now();
        let mut seen = Vec::new();
        loop {
            let Ok((mut stream, _)) = listener.accept() else {
                if started.elapsed() > Duration::from_secs(5) {
                    break;
                }
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            stream.set_nonblocking(false).unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            let mut headers_done = false;
            while !headers_done {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
                headers_done = request.windows(4).any(|w| w == b"\r\n\r\n");
            }
            let header_end = request
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|idx| idx + 4)
                .unwrap_or(request.len());
            let header = String::from_utf8_lossy(&request[..header_end]).to_string();
            let content_len = header
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .or_else(|| line_header_value(&header, "Content-Length"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            while request.len() < header_end + content_len {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
            }
            let first = header.lines().next().unwrap_or("").to_string();
            let body = String::from_utf8_lossy(&request[header_end..]).to_string();
            let saw_restore_request = first.starts_with("POST ") && first.contains("?restore");
            let status = if first.starts_with("HEAD ")
                && first.contains("/archive-bucket/majutsu/v1/objects/blobs/")
            {
                "404 Not Found"
            } else {
                "200 OK"
            };
            seen.push((first, body));
            stream
                .write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").as_bytes())
                .unwrap();
            if saw_restore_request {
                break;
            }
        }
        tx.send(seen).unwrap();
    });

    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    let before_remote = config.split("\n[remote]\n").next().unwrap();
    let after_remote = config.split("\n[large]\n").nth(1).unwrap();
    fs::write(
        &config_path,
        format!(
            r#"{before_remote}
[remote]
type = "s3"
bucket = "archive-bucket"
prefix = "majutsu/v1"
endpoint = "http://{addr}"
region = "us-test-1"
signature_version = "s3v4"

[large]
{after_remote}"#
        ),
    )
    .unwrap();

    let prepare = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--to")
            .arg(&restore)
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy");
        c
    });
    assert!(prepare.contains("archived_objects 1"));
    assert!(prepare.contains("archive_requested_objects 1"));

    let seen = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    server.join().unwrap();
    assert!(seen.iter().any(|(line, _)| line.starts_with("HEAD ")));
    let restore_request = seen
        .iter()
        .find(|(line, _)| line.starts_with("POST ") && line.contains("?restore"))
        .expect("missing S3 archive restore request");
    assert!(restore_request.0.contains("/archive-bucket/majutsu/v1/"));
    assert!(restore_request.0.contains("/blobs/loose/"));
    assert!(restore_request.0.contains(".blob.enc?restore"));
    assert!(restore_request.1.contains("<Days>7</Days>"));
    assert!(restore_request.1.contains("<Tier>Standard</Tier>"));
}

fn line_header_value<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        line.split_once(':').and_then(|(header, value)| {
            if header.eq_ignore_ascii_case(name) {
                Some(value)
            } else {
                None
            }
        })
    })
}

fn write_mock_http_response(
    stream: &mut std::net::TcpStream,
    status: &str,
    body: &[u8],
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;
    let _ = stream.shutdown(std::net::Shutdown::Both);
    Ok(())
}

#[test]
fn restore_prepare_can_hydrate_from_canonical_aliases() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_AUTO_PACK", "0")
            .env("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE", "0");
        c
    });
    fs::remove_dir_all(remote.join("objects")).unwrap();
    let alpha_oid = blake3::hash(b"alpha\n").to_hex().to_string();
    let object = state
        .join("objects/blobs")
        .join(&alpha_oid[..2])
        .join(&alpha_oid[2..]);
    fs::remove_file(object).unwrap();

    let prepare = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(prepare.contains("archived_objects 1"));
    assert!(prepare.contains("missing_objects 0"));
    let job_id = prepare
        .lines()
        .find_map(|line| line.strip_prefix("restore_job "))
        .unwrap()
        .to_string();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("resume")
            .arg(&job_id);
        c
    });

    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
}

#[test]
fn restore_resume_uses_s3_range_get_for_packed_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("pack");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_LOCAL_OBJECT_PRUNE", "0")
            .env("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE", "0");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let pack_key: String = conn
        .query_row("select pack_key from packs limit 1", [], |row| row.get(0))
        .unwrap();
    let index_key: String = conn
        .query_row("select index_key from packs limit 1", [], |row| row.get(0))
        .unwrap();
    let alpha_oid = blake3::hash(b"alpha\n").to_hex().to_string();
    let alpha_object_key: String = conn
        .query_row(
            "select object_key from blobs where oid=?1",
            [&alpha_oid],
            |row| row.get(0),
        )
        .unwrap();
    fs::remove_file(state.join(&pack_key)).unwrap();
    fs::remove_file(state.join(&index_key)).unwrap();
    fs::remove_dir_all(state.join("objects/blobs")).unwrap();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let remote_root = remote.clone();
    let pack_key_for_server = pack_key.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        let started = std::time::Instant::now();
        let mut seen = Vec::new();
        let mut range_seen_at = None;
        loop {
            let Ok((mut stream, _)) = listener.accept() else {
                if started.elapsed() > Duration::from_secs(30)
                    || range_seen_at.is_some_and(|seen_at: std::time::Instant| {
                        seen_at.elapsed() > Duration::from_secs(2)
                    })
                {
                    break;
                }
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            stream.set_nonblocking(false).unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
            }
            let header_end = request
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|idx| idx + 4)
                .unwrap_or(request.len());
            let header = String::from_utf8_lossy(&request[..header_end]).to_string();
            let first = header.lines().next().unwrap_or("").to_string();
            let mut key = first
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .trim_start_matches("/range-bucket/majutsu/v1/")
                .split('?')
                .next()
                .unwrap_or("")
                .to_string();
            let range = line_header_value(&header, "Range")
                .map(str::trim)
                .map(str::to_string);
            seen.push((first.clone(), range.clone()));
            if first.starts_with("POST ") && first.contains("?restore") {
                write_mock_http_response(&mut stream, "200 OK", b"").unwrap();
                continue;
            }
            let mut path = remote_root.join(&key);
            if !path.exists()
                && let Some((_, unprefixed)) = key.split_once('/')
            {
                key = unprefixed.to_string();
                path = remote_root.join(&key);
            }
            if !path.exists() {
                write_mock_http_response(&mut stream, "404 Not Found", b"").unwrap();
                continue;
            }
            if first.starts_with("HEAD ") {
                write_mock_http_response(&mut stream, "200 OK", b"").unwrap();
                continue;
            }
            let mut body = fs::read(&path).unwrap();
            let mut status = "200 OK";
            if let Some(range) = &range {
                if key == pack_key_for_server {
                    range_seen_at = Some(std::time::Instant::now());
                }
                let spec = range.strip_prefix("bytes=").unwrap();
                let (start, end) = spec.split_once('-').unwrap();
                let start = start.parse::<usize>().unwrap();
                let end = end.parse::<usize>().unwrap();
                body = body[start..=end].to_vec();
                status = "206 Partial Content";
            }
            write_mock_http_response(&mut stream, status, &body).unwrap();
        }
        tx.send(seen).unwrap();
    });

    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    let before_remote = config.split("\n[remote]\n").next().unwrap();
    let after_remote = config.split("\n[large]\n").nth(1).unwrap();
    fs::write(
        &config_path,
        format!(
            r#"{before_remote}
[remote]
type = "s3"
bucket = "range-bucket"
prefix = "majutsu/v1"
endpoint = "http://{addr}"
region = "us-test-1"
signature_version = "s3v4"

[large]
{after_remote}"#
        ),
    )
    .unwrap();

    let prepare = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--to")
            .arg(&restore)
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy");
        c
    });
    let job_id = prepare
        .lines()
        .find_map(|line| line.strip_prefix("restore_job "))
        .unwrap()
        .to_string();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("resume")
            .arg(&job_id)
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy");
        c
    });

    let seen = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    server.join().unwrap();
    assert!(seen.iter().any(|(line, range)| {
        line.starts_with("GET ")
            && line.contains(&pack_key)
            && range
                .as_deref()
                .is_some_and(|range| range.starts_with("bytes="))
    }));
    assert!(!state.join(&pack_key).exists());
    assert!(state.join(&alpha_object_key).exists());
    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"alpha\n"
    );
}

#[test]
fn restore_prepare_can_hydrate_large_objects_from_canonical_aliases() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    let payload = (0..32768).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    fs::write(source.join("payload.zip"), &payload).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--large-min-size")
            .arg("4")
            .arg("--large-chunking")
            .arg("fixed")
            .arg("--large-chunk-size")
            .arg("32768");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    fs::remove_dir_all(remote.join("objects")).unwrap();
    fs::remove_dir_all(state.join("objects/large/manifests")).unwrap();
    fs::remove_dir_all(state.join("objects/large/chunks")).unwrap();

    let plan = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(plan.contains("required_chunks 1"));
    assert!(plan.contains("local_chunks 0"));
    assert!(plan.contains("remote_chunks 1"));
    assert!(plan.contains("archived_chunks 1"));
    assert!(plan.contains("missing_chunks 0"));

    let prepare = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(prepare.contains("required_objects 2"));
    assert!(prepare.contains("required_chunks 1"));
    assert!(prepare.contains("local_chunks 0"));
    assert!(prepare.contains("remote_chunks 1"));
    assert!(prepare.contains("archived_chunks 1"));
    assert!(prepare.contains("missing_chunks 0"));
    assert!(prepare.contains("archived_objects 2"));
    assert!(prepare.contains("missing_objects 0"));
    let job_id = prepare
        .lines()
        .find_map(|line| line.strip_prefix("restore_job "))
        .unwrap()
        .to_string();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("resume")
            .arg(&job_id);
        c
    });

    assert_eq!(
        fs::read(restore.join("sample/payload.zip")).unwrap(),
        payload
    );
}

#[test]
fn restore_prepare_resume_preserves_root_path_and_target_filters() {
    let tmp = tempfile::tempdir().unwrap();
    let docs = tmp.path().join("docs");
    let photos = tmp.path().join("photos");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&docs).unwrap();
    fs::create_dir_all(&photos).unwrap();
    fs::write(docs.join("keep.txt"), b"keep\n").unwrap();
    fs::write(docs.join("skip.txt"), b"skip\n").unwrap();
    fs::write(photos.join("photo.txt"), b"photo\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("docs")
            .arg(&docs);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("photos")
            .arg(&photos);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let prepare = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--root")
            .arg("docs")
            .arg("--path")
            .arg("keep.txt")
            .arg("--to")
            .arg(&restore);
        c
    });
    let job_id = prepare
        .lines()
        .find_map(|line| line.strip_prefix("restore_job "))
        .unwrap()
        .to_string();
    let job_path = fs::read_dir(state.join("queue/restores"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let job = fs::read_to_string(&job_path).unwrap();
    assert!(job.contains("\"root\": \"docs\""));
    assert!(job.contains("\"path\": \"keep.txt\""));
    assert!(job.contains(&format!("\"target\": \"{}\"", restore.display())));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("resume")
            .arg(&job_id);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("docs/keep.txt")).unwrap(),
        "keep\n"
    );
    assert!(!restore.join("docs/skip.txt").exists());
    assert!(!restore.join("photos/photo.txt").exists());

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("resume")
            .arg(&job_id);
        c
    });
}

#[test]
fn restore_prepare_reports_objects_missing_from_local_and_remote() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_AUTO_PACK", "0")
            .env("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE", "0");
        c
    });
    let alpha_oid = blake3::hash(b"alpha\n").to_hex().to_string();
    let object_key = format!("objects/blobs/{}/{}", &alpha_oid[..2], &alpha_oid[2..]);
    fs::remove_file(state.join(&object_key)).unwrap();
    fs::remove_file(remote.join(&object_key)).unwrap();
    fs::remove_file(remote.join(format!(
        "blobs/loose/{}/{}.blob.enc",
        &alpha_oid[..2],
        &alpha_oid[2..]
    )))
    .unwrap();

    let plan = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("plan")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(plan.contains("archived_objects 0"));
    assert!(plan.contains("missing_objects 1"));
    assert!(plan.contains("archive_or_missing_objects 1"));

    let prepare = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(prepare.contains("archived_objects 0"));
    assert!(prepare.contains("missing_objects 1"));
    assert!(prepare.contains("archive_requested_objects 0"));
    let job_id = prepare
        .lines()
        .find_map(|line| line.strip_prefix("restore_job "))
        .unwrap();
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("resume")
            .arg(job_id);
        c
    });
}

#[test]
fn xdg_config_can_select_state_home() {
    let tmp = tempfile::tempdir().unwrap();
    let config_home = tmp.path().join("xdg");
    let state = tmp.path().join("configured-state");
    fs::create_dir_all(config_home.join("majutsu")).unwrap();
    fs::write(
        config_home.join("majutsu/config.toml"),
        format!("[state]\nhome = \"{}\"\n", state.display()),
    )
    .unwrap();

    run({
        let mut c = mj();
        c.arg("init").env("XDG_CONFIG_HOME", &config_home);
        c
    });

    assert!(state.join("config.toml").exists());
    assert!(state.join("db/majutsu.sqlite").exists());
}

#[test]
fn state_home_priority_prefers_cli_then_env_then_xdg_config() {
    let tmp = tempfile::tempdir().unwrap();
    let config_home = tmp.path().join("xdg");
    let xdg_state = tmp.path().join("xdg-state");
    let env_state = tmp.path().join("env-state");
    let cli_state = tmp.path().join("cli-state");
    fs::create_dir_all(config_home.join("majutsu")).unwrap();
    fs::write(
        config_home.join("majutsu/config.toml"),
        format!("[state]\nhome = \"{}\"\n", xdg_state.display()),
    )
    .unwrap();

    run({
        let mut c = mj();
        c.arg("init")
            .env("XDG_CONFIG_HOME", &config_home)
            .env("MAJUTSU_HOME", &env_state);
        c
    });
    assert!(env_state.join("config.toml").exists());
    assert!(!xdg_state.join("config.toml").exists());

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&cli_state)
            .arg("init")
            .env("XDG_CONFIG_HOME", &config_home)
            .env("MAJUTSU_HOME", &env_state);
        c
    });
    assert!(cli_state.join("config.toml").exists());
    assert!(!xdg_state.join("config.toml").exists());
}

#[test]
fn db_compact_cli_reports_checkpoint_and_vacuum_metrics() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let compact = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("db").arg("compact");
        c
    });
    assert!(
        compact.contains(&format!("db {}", state.join("db/majutsu.sqlite").display())),
        "{compact}"
    );
    assert!(compact.contains("checkpoint_busy "), "{compact}");
    assert!(compact.contains("checkpoint_log_frames "), "{compact}");
    assert!(compact.contains("checkpointed_frames "), "{compact}");
    assert!(compact.contains("vacuum false"), "{compact}");
    assert!(output_metric(&compact, "db_bytes_after") > 0, "{compact}");

    let vacuum = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("db")
            .arg("compact")
            .arg("--vacuum");
        c
    });
    assert!(vacuum.contains("vacuum true"), "{vacuum}");
    assert!(vacuum.contains("wal_bytes_after "), "{vacuum}");
    assert!(output_metric(&vacuum, "db_bytes_after") > 0, "{vacuum}");
}

#[test]
fn operations_are_appended_to_local_oplog() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let oplog = state.join("ops/local-oplog.cborl");
    assert!(oplog.exists());
    let init_len = fs::metadata(&oplog).unwrap().len();
    assert!(init_len > 0);

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let root_add_len = fs::metadata(&oplog).unwrap().len();
    assert!(root_add_len > init_len);

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c.env("MAJUTSU_SESSION_ID", "session-test-1")
            .env("MAJUTSU_SESSION_LABEL", "agent-test");
        c
    });
    assert!(fs::metadata(&oplog).unwrap().len() > root_add_len);
    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(op_log.contains("agent-test:session-test-1"), "{op_log}");
    let snapshot_op = op_log
        .lines()
        .find(|line| line.contains("agent-test:session-test-1"))
        .and_then(|line| line.split('\t').next())
        .unwrap()
        .to_string();
    let op_show = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("op")
            .arg("show")
            .arg(&snapshot_op);
        c
    });
    assert!(op_show.contains("parent op-"));
    assert!(op_show.contains("actor "));
    assert!(op_show.contains("session_id session-test-1"));
    assert!(op_show.contains("session_label agent-test"));
    assert!(op_show.contains("process_id "));
    assert!(op_show.contains("process_path "));
    assert!(op_show.contains("origin_label agent-test"));
    assert!(op_show.contains("origin_session_id session-test-1"));
    assert!(op_show.contains("origin_process_id "));
    assert!(op_show.contains("origin_process_path "));
    assert!(op_show.contains("origin_confidence self"));
    assert!(op_show.contains("status done"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
    assert_eq!(db_operation_count(&state, "fsck"), 1);
    assert_eq!(
        local_oplog_record_count(&state) as i64,
        db_total_operation_count(&state)
    );
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let host_dir = remote_host_dirs(&remote)
        .into_iter()
        .find(|path| path.join("ops/local-oplog.cborl").exists())
        .unwrap();
    assert_eq!(
        cborl_record_count(&host_dir.join("ops/local-oplog.cborl")) as i64,
        db_total_operation_count(&state)
    );
    assert!(host_dir.join("ops/local-oplog.cborl.zst.enc").exists());
    let export: serde_json::Value = read_remote_metadata(&remote);
    let op = export["operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|op| op["id"] == snapshot_op)
        .unwrap();
    assert!(op["parent_op"].as_str().unwrap().starts_with("op-"));
    assert_eq!(op["status"], "done");
    assert!(op["actor"].as_str().unwrap().contains('@'));
    assert_eq!(op["session_id"], "session-test-1");
    assert_eq!(op["session_label"], "agent-test");
    assert!(op["process_id"].as_u64().unwrap() > 0);
    assert!(!op["process_path"].as_array().unwrap().is_empty());
    assert_eq!(op["origin_label"], "agent-test");
    assert_eq!(op["origin_session_id"], "session-test-1");
    assert!(op["origin_process_id"].as_u64().unwrap() > 0);
    assert!(!op["origin_process_path"].as_array().unwrap().is_empty());
    assert_eq!(op["origin_confidence"], "self");
}

#[test]
fn fsck_detects_corrupt_local_oplog() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert_eq!(
        local_oplog_record_count(&state) as i64,
        db_total_operation_count(&state)
    );
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    fs::write(state.join("ops/local-oplog.cborl"), b"not-cbor").unwrap();
    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    fs::write(state.join("ops/local-oplog.cborl"), b"").unwrap();
    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_corrupt_restore_queue_item() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--to")
            .arg(&restore);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let job_path = fs::read_dir(state.join("queue/restores"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    fs::write(job_path, b"{not valid json").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_corrupt_upload_queue_item() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote-file");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(&remote, b"not a directory\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let item_path = fs::read_dir(state.join("queue/uploads"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    fs::write(item_path, b"{not valid json").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_corrupt_event_journal_item() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let event_path = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    fs::write(event_path, b"{not valid json").unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_config_root_drift() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("id = \"sample\"", "id = \"drifted\"");
    fs::write(config_path, config).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_host_file_drift() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let host_path = state.join("host.toml");
    let host = fs::read_to_string(&host_path)
        .unwrap()
        .replace("id = \"", "id = \"drifted-");
    fs::write(host_path, host).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_broken_local_ref() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.execute(
        "update refs set value='snap-missing' where name='current'",
        [],
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_broken_remote_ref_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.execute(
        "update remote_refs set value='snap-missing' where name like '%/refs/current'",
        [],
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn clone_rejects_remote_metadata_with_invalid_refs_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone_state = tmp.path().join("clone-state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let metadata_path = host_metadata_export_path(&remote);
    let mut export: serde_json::Value =
        serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
    export["refs"]["legacy"] = serde_json::Value::String("snap-legacy".into());
    fs::write(&metadata_path, serde_json::to_vec_pretty(&export).unwrap()).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone_state)
            .arg("clone")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone_state.exists());
}

#[test]
fn clone_rejects_unsupported_metadata_export_version_without_creating_home() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone_state = tmp.path().join("clone-state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let metadata_path = host_metadata_export_path(&remote);
    let mut export: serde_json::Value =
        serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
    export["version"] = serde_json::Value::Number(999.into());
    fs::write(&metadata_path, serde_json::to_vec_pretty(&export).unwrap()).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone_state)
            .arg("clone")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone_state.exists());
}

#[test]
fn clone_rejects_remote_metadata_object_key_escape_without_writing_staging_parent() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone_state = tmp.path().join("clone-state");
    let escaped = tmp.path().join("escaped");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mut export = read_remote_metadata(&remote);
    export["snapshots"][0]["manifest_key"] = serde_json::Value::String("../escaped".into());
    write_remote_metadata(&remote, &export);

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone_state)
            .arg("clone")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(!clone_state.exists());
    assert!(!escaped.exists());
}

#[test]
fn fsck_detects_broken_history_graph() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"two\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.execute(
        "update snapshots set op_id='op-missing' where id=(select id from snapshots limit 1)",
        [],
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_invalid_operation_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.execute(
        "update operations set status='unknown', remote_sync_state='stale' where kind='initial-scan'",
        [],
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn fsck_detects_inconsistent_operation_state() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.execute(
        "update operations set status='failed', error=null where kind='initial-scan'",
        [],
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn large_pin_unpin_is_persisted_in_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.zip"), vec![7u8; 16 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("pin");
        c
    });
    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("pinned 1"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let cloned_stat = output({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("large").arg("stat");
        c
    });
    assert!(cloned_stat.contains("pinned 1"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("large").arg("unpin");
        c
    });
    let unpinned = output({
        let mut c = mj();
        c.arg("--home").arg(&clone).arg("large").arg("stat");
        c
    });
    assert!(unpinned.contains("pinned 0"));
}

#[test]
fn large_pin_filters_by_root_and_since() {
    let tmp = tempfile::tempdir().unwrap();
    let photos = tmp.path().join("photos");
    let docs = tmp.path().join("docs");
    let state = tmp.path().join("state");
    fs::create_dir_all(&photos).unwrap();
    fs::create_dir_all(&docs).unwrap();
    fs::write(photos.join("photo.bin"), b"photo-large").unwrap();
    fs::write(docs.join("doc.bin"), b"doc-large").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("photos")
            .arg(&photos)
            .arg("--large-always")
            .arg("*.bin");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("docs")
            .arg(&docs)
            .arg("--large-always")
            .arg("*.bin");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("large")
            .arg("pin")
            .arg("--root")
            .arg("photos")
            .arg("--since")
            .arg("1d");
        c
    });
    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("pinned 1"));
    let list = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("list");
        c
    });
    assert!(list.contains("pinned"));
    assert!(list.contains("unpinned"));

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("unpin");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("large")
            .arg("pin")
            .arg("--root")
            .arg("photos")
            .arg("--since")
            .arg("0s");
        c
    });
    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("pinned 0"));
}

#[test]
fn large_unpin_older_than_removes_only_old_pins() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.bin"), vec![7u8; 16 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--large-always")
            .arg("*.bin");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("pin");
        c
    });
    fs::write(source.join("payload.bin"), vec![8u8; 16 * 1024]).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("pin");
        c
    });
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let old_oid: String = conn
        .query_row(
            "select oid from large_pins order by oid limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    conn.execute(
        "update large_pins set pinned_at=?2 where oid=?1",
        rusqlite::params![old_oid, "2000-01-01T00:00:00+00:00"],
    )
    .unwrap();

    let unpin = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("large")
            .arg("unpin")
            .arg("--older-than")
            .arg("30d");
        c
    });
    assert!(unpin.contains("unpinned 1"));
    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("large_objects 2"));
    assert!(stat.contains("pinned 1"));
}

#[test]
fn large_verify_rejects_dangling_pins() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.execute(
        "insert into large_pins(oid, pinned_at, reason) values (?1, ?2, ?3)",
        rusqlite::params![
            "missing-large-object",
            Utc::now().to_rfc3339(),
            "corrupt test"
        ],
    )
    .unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("verify");
        c
    });
}

#[test]
fn prune_removes_pins_for_pruned_large_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.zip"), vec![7u8; 16 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("pin");
        c
    });
    fs::write(source.join("payload.zip"), vec![8u8; 16 * 1024]).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("pin");
        c
    });
    fs::write(source.join("payload.zip"), vec![9u8; 16 * 1024]).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("pin");
        c
    });
    let before = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(before.contains("large_objects 3"));
    assert!(before.contains("pinned 3"));

    let pruned = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run=false")
            .arg("--keep-daily")
            .arg("0")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    assert!(pruned.contains("removed_large_metadata 1"));
    assert!(pruned.contains("removed_large_pins 1"));
    let after = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(after.contains("large_objects 2"));
    assert!(after.contains("pinned 2"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
}

#[test]
fn remote_fsck_rejects_dangling_large_pins() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.zip"), vec![7u8; 16 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("pin");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export_path = host_metadata_export_path(&remote);
    let mut export: serde_json::Value =
        serde_json::from_slice(&fs::read(&export_path).unwrap()).unwrap();
    export["large_pins"][0]["oid"] = serde_json::Value::String("missing-large-object".into());
    fs::write(&export_path, serde_json::to_vec_pretty(&export).unwrap()).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
}

#[test]
fn root_large_policy_override_can_route_small_files_to_large_pipeline() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("tiny.dat"), b"tiny-large-policy\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--large-min-size")
            .arg("4")
            .arg("--large-chunking")
            .arg("fixed")
            .arg("--large-chunk-size")
            .arg("4");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("large_objects 1"));
    assert!(stat.contains("chunks 5"));
}

#[test]
fn default_chunked_min_size_routes_half_mib_text_to_chunked_blob() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    let mut medium = Vec::with_capacity(512 * 1024 + 128);
    for i in 0..512 * 1024 + 128 {
        medium.push(b'a' + (i % 26) as u8);
    }
    fs::write(source.join("medium.dat"), &medium).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let tree_path = find_file_ending(&state.join("objects/trees"), "");
    let tree: serde_json::Value = serde_json::from_slice(&fs::read(tree_path).unwrap()).unwrap();
    assert_eq!(
        tree["entries"]["medium.dat"]["payload"]["type"],
        "chunked-blob"
    );
    assert_eq!(
        tree["entries"]["medium.dat"]["payload"]["chunk_count"],
        serde_json::Value::from(9)
    );
}

#[test]
fn default_chunked_blob_reuses_unchanged_chunks_after_medium_edit() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    let mut medium = Vec::with_capacity(512 * 1024 + 128);
    for i in 0..512 * 1024 + 128 {
        medium.push(b'a' + (i % 26) as u8);
    }
    fs::write(source.join("medium.dat"), &medium).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let first_chunks = latest_payload_chunk_oids(&state, "sample", "medium.dat");

    medium[300 * 1024] = b'Z';
    fs::write(source.join("medium.dat"), &medium).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let second_chunks = latest_payload_chunk_oids(&state, "sample", "medium.dat");

    assert_eq!(first_chunks.len(), 9);
    assert_eq!(second_chunks.len(), 9);
    let reused = first_chunks
        .iter()
        .zip(second_chunks.iter())
        .filter(|(left, right)| left == right)
        .count();
    assert_eq!(reused, 8);
}

fn latest_payload_chunk_oids(state: &std::path::Path, root: &str, path: &str) -> Vec<String> {
    let manifest = local_snapshot_manifest(state, "desc");
    let payload = manifest["roots"][root]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["path"] == path)
        .unwrap()["payload"]
        .clone();
    assert_eq!(payload["type"], "chunked-blob");
    let manifest_key = payload["manifest_key"].as_str().unwrap();
    let large_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(state.join(manifest_key)).unwrap()).unwrap();
    large_manifest["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|chunk| chunk["oid"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn encrypted_chunked_blob_reuses_stable_payload_refs_across_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    let mut medium = Vec::with_capacity(1024 * 1024 + 128);
    for i in 0..1024 * 1024 + 128 {
        medium.push(((i * 31 + 17) % 251) as u8);
    }
    fs::write(source.join("medium.bin"), &medium).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init").arg("--encrypt");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.env("MAJUTSU_SNAPSHOT_ALLOW_NOOP", "1")
            .arg("--home")
            .arg(&state)
            .arg("snapshot");
        c
    });
    let diff = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("diff");
        c
    });
    assert!(diff.trim().is_empty(), "{diff}");
}

#[test]
fn default_large_always_patterns_route_known_binary_extensions() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    for name in [
        "movie.mp4",
        "clip.mov",
        "video.mkv",
        "archive.zip",
        "bundle.tar",
        "bundle.tar.zst",
        "dataset.parquet",
        "app.sqlite",
        "app.db",
        "disk.vmdk",
        "disk.qcow2",
        "installer.iso",
        "design.psd",
        "scene.blend",
    ] {
        fs::write(source.join(name), format!("tiny marker for {name}\n")).unwrap();
    }

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config = fs::read_to_string(state.join("config.toml")).unwrap();
    assert!(config.contains("\"*.parquet\""));
    assert!(config.contains("\"*.vmdk\""));
    assert!(config.contains("\"*.qcow2\""));
    assert!(config.contains("\"*.psd\""));
    assert!(config.contains("\"*.blend\""));
    assert!(config.contains("\"*.tar.zst\""));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("large_objects 14"));
}

#[test]
fn large_manifest_records_classification_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("dataset.parquet"), b"PAR1\0payload\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let manifest_path = find_file_ending(&state.join("objects/large/manifests"), "");
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["media_type"], "application/vnd.apache.parquet");
    assert_eq!(manifest["binary"], true);
}

#[test]
fn root_set_updates_filters_and_records_config_change() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();
    fs::write(source.join("skip.tmp"), b"skip\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("set")
            .arg("sample")
            .arg("--name")
            .arg("Sample Docs")
            .arg("--exclude")
            .arg("*.tmp");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read(restore.join("sample/keep.txt")).unwrap(),
        b"keep\n"
    );
    assert!(!restore.join("sample/skip.tmp").exists());
    let roots = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(roots.contains("Sample Docs"));
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(ops.contains("config-change"));
}

#[test]
fn root_commands_sync_config_roots() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--no-default-excludes")
            .arg("--exclude")
            .arg("*.tmp");
        c
    });
    let config = fs::read_to_string(state.join("config.toml")).unwrap();
    assert!(config.contains("[[roots]]"));
    assert!(config.contains("id = \"sample\""));
    assert!(config.contains("exclude = [\"*.tmp\"]"));
    assert!(config.contains("status = \"active\""));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("pause")
            .arg("sample");
        c
    });
    let config = fs::read_to_string(state.join("config.toml")).unwrap();
    assert!(config.contains("status = \"paused\""));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("remove")
            .arg("sample");
        c
    });
    let config = fs::read_to_string(state.join("config.toml")).unwrap();
    assert!(!config.contains("[[roots]]"));
    assert!(!config.contains("id = \"sample\""));
}

#[test]
fn root_size_reports_client_and_backend_totals() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("--wait");
        c
    });

    let sizes = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--no-remote-cache");
        c
    });
    assert!(sizes.contains("root"));
    assert!(sizes.contains("backend"));
    assert!(sizes.contains("used"));
    assert!(sizes.contains("objects"));
    assert!(sizes.contains("root") && sizes.contains("|") && sizes.contains("missing"));
    assert!(sizes.contains("左はclient側、右はremote側"));
    assert!(sizes.contains("------"));
    assert!(sizes.contains("sample"));
    assert!(sizes.contains("0.00 MiB"));
    assert!(!sizes.contains("| root |"));
    assert!(sizes.contains("全体:"));
    assert!(sizes.contains("- root別used集計合計:"));
    assert!(sizes.contains("- current snapshotのユニークused推定:"));
    assert!(sizes.contains("- current snapshotが参照するremote object全体:"));
    assert!(sizes.contains("- S3上の実サイズ共有remote prefix全体:"));
    assert!(sizes.contains("S3実サイズ明細:"));
    assert!(sizes.contains("local-current"));
    assert!(sizes.contains("環境別current snapshotサイズ:"));
    assert!(!sizes.contains("not scanned"));

    let json = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--json")
            .arg("--no-remote-cache");
        c
    });
    let report: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(report["roots"][0]["root"], "sample");
    assert_eq!(report["roots"][0]["files"], 1);
    assert_eq!(report["roots"][0]["dirs"], 0);
    assert_eq!(report["roots"][0]["client_bytes"], 5);
    assert!(report["roots"][0]["backend_bytes"].as_u64().unwrap() > 0);
    assert!(report["roots"][0]["used_bytes"].as_u64().unwrap() > 0);
    assert!(report["roots"][0]["payload_bytes"].as_u64().unwrap() > 0);
    assert!(report["roots"][0]["metadata_bytes"].as_u64().unwrap() > 0);
    assert!(report["roots"][0]["backend_objects"].as_u64().unwrap() > 0);
    assert_eq!(report["roots"][0]["missing_objects"], 0);
    assert!(report["totals"]["current_backend_bytes"].as_u64().unwrap() > 0);
    assert!(report["totals"]["row_used_bytes"].as_u64().unwrap() > 0);
    assert!(report["totals"]["unique_used_bytes"].as_u64().unwrap() > 0);
    assert!(report["totals"]["billed_bytes"].as_u64().unwrap() > 0);
    assert!(report["totals"]["billed_objects"].as_u64().unwrap() > 0);
    assert!(report["totals"]["payload_bytes"].as_u64().unwrap() > 0);
    assert!(report["totals"]["metadata_bytes"].as_u64().unwrap() > 0);
    assert!(report["totals"]["objects"].as_u64().unwrap() > 0);
    assert!(report["totals"]["backend_prefix_bytes"].as_u64().unwrap() > 0);
    assert!(report["totals"]["backend_prefix_objects"].as_u64().unwrap() > 0);
    assert_eq!(report["totals"]["backend_prefix_exact"], true);
    assert!(report["totals"]["backend_prefix_scope"].as_str().is_some());
    assert_eq!(report["host_summaries"].as_array().unwrap().len(), 1);
    assert_eq!(report["host_summaries"][0]["host_name"], "vagrant");
    assert_eq!(report["host_summaries"][0]["current"], true);
    assert!(report["host_summaries"][0]["used_bytes"].as_u64().unwrap() > 0);
    let breakdown = report["remote_breakdown"].as_array().unwrap();
    assert!(
        breakdown
            .iter()
            .any(|row| row["category"] == "local-current")
    );
    let breakdown_total = breakdown
        .iter()
        .map(|row| row["bytes"].as_u64().unwrap())
        .sum::<u64>();
    assert_eq!(
        breakdown_total,
        report["totals"]["backend_prefix_bytes"].as_u64().unwrap()
    );
}

#[test]
fn root_size_exact_breakdown_groups_other_host_prefixes() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let alpha_source = tmp.path().join("alpha-source");
    let beta_source = tmp.path().join("beta-source");
    let alpha_state = tmp.path().join("alpha-state");
    let beta_state = tmp.path().join("beta-state");
    fs::create_dir_all(&alpha_source).unwrap();
    fs::create_dir_all(&beta_source).unwrap();
    fs::write(alpha_source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(beta_source.join("beta.txt"), b"beta\n").unwrap();

    for (state, source, host, root) in [
        (&alpha_state, &alpha_source, "alpha-host", "alpha-root"),
        (&beta_state, &beta_source, "beta-host", "beta-root"),
    ] {
        run({
            let mut c = mj();
            c.arg("--home")
                .arg(state)
                .arg("init")
                .arg("--host-name")
                .arg(host)
                .arg("--remote")
                .arg(format!("file://{}", remote.display()));
            c
        });
        run({
            let mut c = mj();
            c.arg("--home")
                .arg(state)
                .arg("root")
                .arg("add")
                .arg(root)
                .arg(source);
            c
        });
        run({
            let mut c = mj();
            c.arg("--home").arg(state).arg("snapshot");
            c
        });
        run({
            let mut c = mj();
            c.arg("--home").arg(state).arg("sync").arg("--wait");
            c
        });
    }

    let json = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&alpha_state)
            .arg("root")
            .arg("size")
            .arg("--json")
            .arg("--no-remote-cache");
        c
    });
    let report: serde_json::Value = serde_json::from_str(&json).unwrap();
    let hosts = report["host_summaries"].as_array().unwrap();
    assert_eq!(hosts.len(), 2, "{json}");
    assert!(
        hosts
            .iter()
            .any(|host| host["host_name"] == "alpha-host" && host["current"] == true),
        "{json}"
    );
    assert!(
        hosts
            .iter()
            .any(|host| host["host_name"] == "beta-host" && host["current"] == false),
        "{json}"
    );
    let breakdown = report["remote_breakdown"].as_array().unwrap();
    assert!(
        breakdown
            .iter()
            .any(|row| row["category"] == "host:beta-host"),
        "{json}"
    );
    let breakdown_total = breakdown
        .iter()
        .map(|row| row["bytes"].as_u64().unwrap())
        .sum::<u64>();
    assert_eq!(
        breakdown_total,
        report["totals"]["backend_prefix_bytes"].as_u64().unwrap()
    );
}

#[test]
fn root_size_streams_local_scan_before_uncached_remote_listing() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("--wait");
        c
    });

    let started = std::time::Instant::now();
    let mut child = mj()
        .arg("--home")
        .arg(&state)
        .arg("root")
        .arg("size")
        .arg("--no-remote-cache")
        .env("MAJUTSU_ROOT_SIZE_FORCE_STREAM", "1")
        .env("MAJUTSU_ROOT_SIZE_REMOTE_LIST_DELAY_MS", "1200")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut buf = Vec::new();
    let mut byte = [0_u8; 1];
    while !String::from_utf8_lossy(&buf).contains("- S3上の実サイズ共有remote prefix全体: ...")
    {
        stdout.read_exact(&mut byte).unwrap();
        buf.push(byte[0]);
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "local root-size table was not streamed before remote listing delay elapsed:\n{}",
            String::from_utf8_lossy(&buf)
        );
    }
    let status = child.wait().unwrap();
    assert!(status.success());
    let streamed = String::from_utf8_lossy(&buf);
    assert!(streamed.contains("sample"));
    assert!(streamed.contains("0.00 MiB"));
    assert!(streamed.contains("- remote集計: ..."));
    assert!(streamed.contains("- S3上の実サイズ共有remote prefix全体: ..."));
}

#[test]
fn root_size_default_does_not_block_on_uncached_remote_listing() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("--wait");
        c
    });

    let started = std::time::Instant::now();
    let sizes = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .env("MAJUTSU_ROOT_SIZE_REMOTE_LIST_DELAY_MS", "1200");
        c
    });
    assert!(
        started.elapsed() < Duration::from_millis(900),
        "default root size blocked on uncached remote listing:\n{sizes}"
    );
    assert!(sizes.contains("sample"));
    assert!(sizes.contains("not-scanned:no-cached-prefix-list"));
    assert!(sizes.contains("exact: false"));
    assert!(sizes.contains("- current snapshotのユニークused推定:"));
    assert!(!sizes.contains("0 objects\n\n環境別current snapshotサイズ:"));
}

#[test]
fn snapshot_does_not_auto_track_new_large_file_after_initial_root_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("seed.txt"), b"seed\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--large-min-size")
            .arg("1024")
            .arg("--large-binary-min-size")
            .arg("1024");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fs::write(source.join("large.bin"), vec![b'x'; 2048]).unwrap();
    let snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(
        snapshot.contains("auto_track_skipped large=1 batch=0"),
        "{snapshot}"
    );

    let state_out = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample");
        c
    });
    assert!(!state_out.contains("large.bin"), "{state_out}");
    let untracked = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("-U")
            .arg("--status")
            .arg("?");
        c
    });
    assert!(
        untracked.contains("? sample/large.bin") || untracked.contains("? large.bin"),
        "{untracked}"
    );
}

#[test]
fn snapshot_does_not_auto_track_large_batches_of_new_files() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("seed.txt"), b"seed\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    for index in 0..3 {
        fs::write(source.join(format!("bulk-{index}.txt")), b"bulk\n").unwrap();
    }
    let snapshot = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .env("MAJUTSU_MAX_AUTO_TRACK_NEW_FILES", "2");
        c
    });
    assert!(
        snapshot.contains("auto_track_skipped large=0 batch=3"),
        "{snapshot}"
    );

    let state_out = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample");
        c
    });
    assert!(
        !state_out.contains("bulk-0.txt")
            && !state_out.contains("bulk-1.txt")
            && !state_out.contains("bulk-2.txt"),
        "{state_out}"
    );
    let untracked = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("-U")
            .arg("--status")
            .arg("?");
        c
    });
    assert!(
        untracked.contains("? sample/bulk-0.txt") || untracked.contains("? bulk-0.txt"),
        "{untracked}"
    );
    assert!(
        untracked.contains("? sample/bulk-1.txt") || untracked.contains("? bulk-1.txt"),
        "{untracked}"
    );
    assert!(
        untracked.contains("? sample/bulk-2.txt") || untracked.contains("? bulk-2.txt"),
        "{untracked}"
    );
}

#[test]
fn root_size_history_reports_payloads_not_referenced_by_current_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.txt"), b"old retained payload\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("note.txt"), b"current payload\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let sizes = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--history")
            .arg("--history-limit")
            .arg("10");
        c
    });
    assert!(sizes.contains("履歴保持payload:"));
    assert!(sizes.contains("current snapshotから外れている保持payload推定"));
    assert!(sizes.contains("note.txt"));
    assert!(sizes.contains("blob"));

    let json = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--history")
            .arg("--json");
        c
    });
    let report: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(report["history"]["retained_bytes"].as_u64().unwrap() > 0);
    assert_eq!(report["history"]["rows"][0]["path"], "note.txt");
}

#[test]
fn root_set_exclude_forgets_unmanaged_history_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(source.join("build")).unwrap();
    fs::write(source.join("build/cache.bin"), b"generated payload\n").unwrap();
    fs::write(source.join("src.txt"), b"source payload\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--no-default-excludes");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::remove_file(source.join("build/cache.bin")).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let before = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--history")
            .arg("--history-limit")
            .arg("10");
        c
    });
    assert!(before.contains("build/cache.bin"), "{before}");

    let root_set = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("set")
            .arg("sample")
            .arg("--exclude")
            .arg("build/**");
        c
    });
    assert!(
        root_set.contains("forgotten_unmanaged_records"),
        "{root_set}"
    );
    let after = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--history")
            .arg("--history-limit")
            .arg("10");
        c
    });
    assert!(!after.contains("build/cache.bin"), "{after}");
    assert!(after.contains("sample"));
}

#[test]
fn root_size_falls_back_when_remote_summary_is_corrupt() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("--wait");
        c
    });

    let summary_path = find_file_ending(&remote, "root-size-summary.cbor.zst.enc");
    fs::write(summary_path, b"corrupt summary").unwrap();

    let sizes = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--no-remote-cache");
        c
    });
    assert!(sizes.contains("sample"));
    assert!(sizes.contains("- S3上の実サイズ共有remote prefix全体:"));
    assert!(!sizes.contains("not scanned"));
}

#[test]
fn root_size_reports_full_totals_when_remote_summary_is_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("--wait");
        c
    });

    let summary_path = find_file_ending(&remote, "root-size-summary.cbor.zst.enc");
    fs::remove_file(summary_path).unwrap();

    let sizes = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--no-remote-cache");
        c
    });
    assert!(sizes.contains("sample"));
    assert!(sizes.contains("- S3上の実サイズ共有remote prefix全体:"));
    assert!(!sizes.contains("not scanned"));
}

#[test]
fn sync_root_size_summary_uses_remote_encoded_sizes_for_large_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("compressible.bin"), vec![b'x'; 1024 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--large-min-size")
            .arg("4")
            .arg("--large-chunking")
            .arg("fixed")
            .arg("--large-chunk-size")
            .arg("65536");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("--wait");
        c
    });

    let cached: serde_json::Value = serde_json::from_str(&output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--json");
        c
    }))
    .unwrap();
    let exact: serde_json::Value = serde_json::from_str(&output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("size")
            .arg("--no-remote-cache")
            .arg("--json");
        c
    }))
    .unwrap();

    assert_eq!(
        cached["totals"]["current_backend_bytes"], exact["totals"]["current_backend_bytes"],
        "cached summary must use remote encoded sizes"
    );
    assert!(
        cached["totals"]["current_backend_bytes"].as_u64().unwrap() < 1024 * 1024,
        "{}",
        serde_json::to_string_pretty(&cached["totals"]).unwrap()
    );
}

#[test]
fn root_add_rejects_duplicate_id_without_overwriting_existing_root() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let other = tmp.path().join("other");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&other).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&other);
        c
    });

    let roots = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(root_list_has_path(&roots, "sample", "active", &source));
    assert!(!roots.contains(&other.display().to_string()));
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert_eq!(ops.matches("root-added").count(), 1);
}

#[test]
fn root_include_can_select_subtree_patterns() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(source.join("docs/nested")).unwrap();
    fs::create_dir_all(source.join("logs")).unwrap();
    fs::write(source.join("docs/nested/keep.txt"), b"keep\n").unwrap();
    fs::write(source.join("logs/skip.txt"), b"skip\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--include")
            .arg("docs/**");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--root")
            .arg("sample")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read(restore.join("sample/docs/nested/keep.txt")).unwrap(),
        b"keep\n"
    );
    assert!(!restore.join("sample/logs/skip.txt").exists());
}

#[test]
fn root_set_updates_large_policy_for_existing_root() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("tiny.dat"), b"tiny-large-policy\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("set")
            .arg("sample")
            .arg("--large-min-size")
            .arg("4")
            .arg("--large-chunking")
            .arg("fixed")
            .arg("--large-chunk-size")
            .arg("4");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("large_objects 1"));
    assert!(stat.contains("chunks 5"));
}

#[test]
fn large_config_accepts_spec_size_strings() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let remote = tmp.path().join("remote");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("tiny.dat"), b"tiny-large-policy\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = \"4 B\"")
        .replace("binary_min_size = 16777216", "binary_min_size = \"16 MiB\"")
        .replace("chunk_size = 8388608", "target_chunk_size = \"4 B\"")
        .replace("max_parallel_uploads = 8", "max_parallel_uploads = 3")
        .replace("multipart = true", "multipart = false")
        .replace("sample_bytes = 1048576", "sample_bytes = \"1 KiB\"");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("large_objects 1"));
    assert!(stat.contains("chunks 5"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let export: serde_json::Value = read_remote_metadata(&remote);
    assert_eq!(export["config"]["large"]["chunk_size"], 4);
    assert_eq!(export["config"]["large"]["max_parallel_uploads"], 3);
    assert_eq!(export["config"]["large"]["multipart"], false);
    assert_eq!(
        export["config"]["large"]["compression"]["sample_bytes"],
        1024
    );
}

#[test]
fn config_roots_are_synced_into_runtime_state() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.bin"), vec![b'Q'; 2048]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let mut config = fs::read_to_string(&config_path).unwrap();
    config.push_str(&format!(
        r#"
[[roots]]
id = "cfg"
path = "{}"
exclude = ["**/.git/**"]

[roots.large]
min_size = "1 KiB"
"#,
        source.display()
    ));
    fs::write(&config_path, config).unwrap();

    let roots = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(root_list_has(&roots, "cfg", "active"));
    assert!(roots.contains(&source.display().to_string()));
    let snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(snapshot.contains("files 1, large 1"));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("pause")
            .arg("cfg");
        c
    });
    let roots = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(root_list_has(&roots, "cfg", "paused"));
}

#[test]
fn large_chunks_can_be_compressed_and_restored() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.dat"), vec![b'A'; 64 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 32768")
        .replace(
            "[large.compression]\nenabled = true\nalgorithm = \"zstd\"\nlevel = 3\nmin_gain_ratio = 0.05",
            "[large.compression]\nalgorithm = \"zstd\"\nlevel = 3",
        );
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let manifest = fs::read_to_string(
        fs::read_dir(state.join("objects/large/manifests"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
            .read_dir()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path(),
    )
    .unwrap();
    assert!(manifest.contains("\"compression\": \"zstd\""));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let plan = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("plan")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(plan.contains("large_files 1"));
    assert!(plan.contains("required_chunks 2"));
    assert!(plan.contains("local_chunks 2"));
    assert!(plan.contains("remote_chunks 2"));
    assert!(plan.contains("archived_chunks 0"));
    assert!(plan.contains("missing_chunks 0"));
    assert!(plan.contains("required_objects "));
    assert!(plan.contains("local_objects "));
    assert!(plan.contains("remote_objects "));
    assert!(plan.contains("archive_or_missing_objects 0"));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(source.join("payload.dat")).unwrap(),
        fs::read(restore.join("sample/payload.dat")).unwrap()
    );
}

#[test]
fn large_verify_checks_referenced_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("payload.bin"), vec![5u8; 64 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 32768");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let verify = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("verify");
        c
    });
    assert!(verify.contains("fsck ok"));

    let chunk = find_file_ending(&state.join("objects/large/chunks/fixed"), "");
    fs::remove_file(chunk).unwrap();
    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("verify");
        c
    });
}

#[test]
fn large_stat_reports_objects_chunks_bytes_and_pins() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    let mut payload = vec![5u8; 32 * 1024];
    payload.extend(vec![7u8; 32 * 1024]);
    fs::write(source.join("payload.bin"), payload).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 32768");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("large_objects 1"));
    assert!(stat.contains("logical_bytes 65536"));
    assert!(stat.contains("chunks 2"));
    assert!(stat.contains("pinned 0"));

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("pin");
        c
    });
    let pinned = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(pinned.contains("large_objects 1"));
    assert!(pinned.contains("logical_bytes 65536"));
    assert!(pinned.contains("chunks 2"));
    assert!(pinned.contains("pinned 1"));
}

#[test]
fn large_files_can_use_content_defined_chunking() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    let mut payload = Vec::new();
    for i in 0..128 * 1024 {
        payload.push(((i * 31) % 251) as u8);
    }
    fs::write(source.join("payload.dat"), &payload).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace(
            "default_chunking = \"fixed\"",
            "default_chunking = \"fastcdc\"",
        )
        .replace("chunk_size = 8388608", "chunk_size = 8192");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let manifest = fs::read_to_string(
        fs::read_dir(state.join("objects/large/manifests"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
            .read_dir()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path(),
    )
    .unwrap();
    assert!(manifest.contains("\"chunking\": \"fastcdc\""));
    assert!(manifest.contains("objects/large/chunks/fastcdc/"));
    assert!(manifest.matches("\"index\"").count() > 1);
    assert!(
        fs::read_dir(state.join("objects/large/chunks/fastcdc"))
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.path().is_dir())
    );
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("fsck");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(remote.join("large/chunks/fastcdc").exists());
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("remote")
            .arg("fsck")
            .arg("--deep");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(source.join("payload.dat")).unwrap(),
        fs::read(restore.join("sample/payload.dat")).unwrap()
    );
}

#[test]
fn large_chunk_dedup_sync_clone_restore_preserves_shared_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    let shared = vec![7u8; 4096];
    let mut alpha = shared.clone();
    alpha.extend(vec![1u8; 4096]);
    let mut beta = shared;
    beta.extend(vec![2u8; 4096]);
    fs::write(source.join("alpha.bin"), &alpha).unwrap();
    fs::write(source.join("beta.bin"), &beta).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 4096");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert_eq!(
        count_files_ending(&state.join("objects/large/chunks/fixed"), ""),
        3,
        "two 2-chunk large files should store the shared chunk once"
    );
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert_eq!(
        count_files_ending(&remote.join("large/chunks/fixed-8m"), ".chunk.enc"),
        3,
        "remote should upload only the deduplicated chunk set"
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(source.join("alpha.bin")).unwrap(),
        fs::read(restore.join("sample/alpha.bin")).unwrap()
    );
    assert_eq!(
        fs::read(source.join("beta.bin")).unwrap(),
        fs::read(restore.join("sample/beta.bin")).unwrap()
    );
}

#[test]
fn mount_creates_lazy_view_and_can_hydrate_large_files() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let lazy_view = tmp.path().join("lazy-view");
    let hydrated_view = tmp.path().join("hydrated-view");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("payload.bin"), vec![9u8; 64 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 32768");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let lazy = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("mount").arg(&lazy_view);
        c
    });
    assert!(lazy.contains("lazy_large_files 1"));
    assert_eq!(
        fs::read(source.join("alpha.txt")).unwrap(),
        fs::read(lazy_view.join("sample/alpha.txt")).unwrap()
    );
    assert_eq!(
        fs::metadata(lazy_view.join("sample/payload.bin"))
            .unwrap()
            .len(),
        64 * 1024
    );
    assert!(
        lazy_view
            .join(".majutsu-lazy/sample/payload.bin.json")
            .exists()
    );
    assert!(lazy_view.join(".majutsu-mount.json").exists());
    let hydrated_lazy = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("hydrate")
            .arg(&lazy_view)
            .arg("--root")
            .arg("sample")
            .arg("--path")
            .arg("payload.bin");
        c
    });
    assert!(hydrated_lazy.contains("hydrated_large_files 1"));
    assert_eq!(
        fs::read(source.join("payload.bin")).unwrap(),
        fs::read(lazy_view.join("sample/payload.bin")).unwrap()
    );
    assert!(
        !lazy_view
            .join(".majutsu-lazy/sample/payload.bin.json")
            .exists()
    );

    let hydrated = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("mount")
            .arg("--hydrate-large")
            .arg(&hydrated_view);
        c
    });
    assert!(hydrated.contains("hydrated_large_files 1"));
    assert_eq!(
        fs::read(source.join("payload.bin")).unwrap(),
        fs::read(hydrated_view.join("sample/payload.bin")).unwrap()
    );

    let unmounted = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("unmount")
            .arg(&lazy_view);
        c
    });
    assert!(unmounted.contains("unmounted"));
    assert!(!lazy_view.exists());
}

#[test]
fn mount_refuses_non_empty_materialized_mountpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let view = tmp.path().join("view");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&view).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(view.join("existing.txt"), b"existing\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("mount").arg(&view);
        c
    });
    assert_eq!(
        fs::read_to_string(view.join("existing.txt")).unwrap(),
        "existing\n"
    );
    assert!(!view.join("sample/alpha.txt").exists());
    assert!(!view.join(".majutsu-mount.json").exists());
}

#[test]
fn mount_at_uses_historical_snapshot_time() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let historical_view = tmp.path().join("historical-view");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"v1\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    thread::sleep(Duration::from_millis(20));
    let at = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
    thread::sleep(Duration::from_millis(20));
    fs::write(source.join("alpha.txt"), b"v2\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let mounted = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("mount")
            .arg("--at")
            .arg(&at)
            .arg(&historical_view);
        c
    });

    assert!(mounted.contains("mounted"));
    assert_eq!(
        fs::read(historical_view.join("sample/alpha.txt")).unwrap(),
        b"v1\n"
    );
}

#[test]
fn missing_root_is_not_snapshotted_as_deletion() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::remove_dir_all(&source).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let output = mj()
        .arg("--home")
        .arg(&state)
        .arg("root")
        .arg("list")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(root_list_has(&stdout, "sample", "missing"));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "alpha\n"
    );
}

#[test]
fn root_mark_deleted_requires_explicit_operator_action() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let marked = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("mark-deleted")
            .arg("sample");
        c
    });
    assert!(marked.contains("marked root sample deleted"));
    let roots = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(root_list_has(&roots, "sample", "deleted"));
    let snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(snapshot.contains("files 0, large 0"));
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(ops.contains("root-mark-deleted"));
}

#[test]
fn root_remove_detaches_root_and_records_operation() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let removed = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("remove")
            .arg("sample");
        c
    });
    assert!(removed.contains("removed root sample"));
    let roots = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(
        !roots
            .lines()
            .any(|line| line.split_whitespace().next() == Some("sample"))
    );
    let snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(snapshot.contains("files 0, large 0"));
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(ops.contains("root-removed"));
}

#[test]
fn root_remove_unknown_root_fails_without_operation() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("remove")
            .arg("missing");
        c
    });
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(!ops.contains("root-removed"));
}

#[test]
fn root_pause_and_resume_control_snapshot_participation() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let paused = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("pause")
            .arg("sample");
        c
    });
    assert!(paused.contains("paused root sample"));
    let roots = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(root_list_has(&roots, "sample", "paused"));
    let paused_snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(paused_snapshot.contains("files 0, large 0"));

    let resumed = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("resume")
            .arg("sample");
        c
    });
    assert!(resumed.contains("resumed root sample"));
    let active_snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(active_snapshot.contains("files 1, large 0"));
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(ops.contains("root-paused"));
    assert!(ops.contains("root-resumed"));
}

#[test]
fn status_reports_configured_root_state() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("pause")
            .arg("sample");
        c
    });

    let status = output({
        let mut c = mj();
        c.env("COLUMNS", "48")
            .env("LINES", "5")
            .arg("--home")
            .arg(&state)
            .arg("status")
            .arg("--no-pager");
        c
    });

    assert!(status.contains("Status"));
    assert!(status.contains("Protection"));
    assert!(status.contains("Health issues"));
    assert!(status.contains("Roots              1"));
    assert!(status.contains("Remote             not configured"));
    assert!(status.contains("Host"));
    assert!(status.contains("Configuration"));
    assert!(status.contains("Roots"));
    assert!(status.contains("FILES"));
    assert!(status.contains("TREE"));
    assert!(status.contains("sample"));
    assert!(status.contains("paused"));
    assert!(status.contains("Metadata"));
    assert!(status.contains("snapshots"));
    assert!(status.contains("operations"));
    assert!(status.contains("blobs"));
    assert!(status.contains("Storage"));
    assert!(status.contains("state"));
    assert!(status.contains("objects"));
    assert!(status.contains("Queues"));
    assert!(status.contains("uploads"));
    assert!(status.contains("event journal"));
    assert!(status.contains("restore jobs"));
    assert!(
        status
            .lines()
            .filter(|line| !line.starts_with("current "))
            .all(|line| line.len() <= 48),
        "{status}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn state_uses_viewer_when_tty_output_exceeds_terminal_height() {
    let tmp = tempfile::tempdir().unwrap();
    let probe = tmp.path().join("script-probe.out");
    let script_supported = Command::new("script")
        .arg("-qec")
        .arg("true")
        .arg(&probe)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()
        .is_some_and(|status| status.success());
    if !script_supported {
        return;
    }

    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    for index in 0..20 {
        fs::write(
            source.join(format!("file-{index:02}.txt")),
            format!("file {index}\n"),
        )
        .unwrap();
    }

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("baseline");
        c
    });

    let pty_output = tmp.path().join("state-pty.out");
    let command = format!(
        "env MAJUTSU_AUTO_DAEMON=0 LINES=5 COLUMNS=100 TERM=xterm-256color {} --home {} state",
        shell_quote(env!("CARGO_BIN_EXE_mj")),
        shell_quote_path(&state),
    );
    let mut child = Command::new("script")
        .arg("-qec")
        .arg(command)
        .arg(&pty_output)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start script");
    child.stdin.as_mut().unwrap().write_all(b"q").unwrap();
    let status = child.wait().expect("wait script");
    assert!(status.success());

    let bytes = fs::read(&pty_output).unwrap();
    assert!(
        bytes
            .windows(b"\x1b[?1049h".len())
            .any(|window| window == b"\x1b[?1049h"),
        "state output did not enter alternate-screen viewer:\n{}",
        String::from_utf8_lossy(&bytes)
    );
    assert!(
        bytes
            .windows(b"mj state".len())
            .any(|window| window == b"mj state"),
        "state viewer status was not rendered:\n{}",
        String::from_utf8_lossy(&bytes)
    );
}

#[test]
fn track_untrack_separate_deleted_state_from_management_removal() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--exclude")
            .arg("ignored/**");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("empty baseline");
        c
    });

    fs::write(source.join("short-lived.txt"), b"short\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("track")
            .arg("-r")
            .arg("sample")
            .arg("short-lived.txt");
        c
    });
    fs::remove_file(source.join("short-lived.txt")).unwrap();
    let deleted = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("--deleted");
        c
    });
    assert_eq!(deleted, " D sample/short-lived.txt\n");
    let deleted_json = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("--deleted")
            .arg("--json");
        c
    });
    let deleted_value: serde_json::Value = serde_json::from_str(&deleted_json).unwrap();
    assert_eq!(deleted_value["changes"]["total"], 1);
    assert_eq!(
        deleted_value["changes"]["files"][0]["path"],
        "short-lived.txt"
    );

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("untrack")
            .arg("-r")
            .arg("sample")
            .arg("short-lived.txt");
        c
    });
    let deleted_after_untrack = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("--deleted");
        c
    });
    assert_eq!(deleted_after_untrack, "");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("after untrack");
        c
    });
    let deleted_after_untrack_snapshot = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("--deleted");
        c
    });
    assert_eq!(deleted_after_untrack_snapshot, "");

    fs::create_dir_all(source.join("ignored")).unwrap();
    fs::write(source.join("ignored/keep.txt"), b"keep\n").unwrap();
    run({
        let mut c = mj();
        c.current_dir(&source)
            .arg("--home")
            .arg(&state)
            .arg("track")
            .arg("ignored/keep.txt");
        c
    });
    let added = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("--status")
            .arg("A");
        c
    });
    assert!(added.contains(" A sample/ignored/keep.txt"), "{added}");
}

#[test]
fn state_stream_does_not_report_snapshot_source_tracked_temp_dirs_as_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("empty baseline");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    conn.execute(
        "insert into tracked_paths(root_id, path, status, tracking_source, first_seen_at, last_seen_at)
         values ('sample', 'tests/XXtmpdir', 'tracked', 'snapshot', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        [],
    )
    .unwrap();

    let text = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample");
        c
    });
    assert_eq!(text, "");

    let json = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("--json");
        c
    });
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["changes"]["total"], 0, "{json}");
}

#[test]
fn state_reports_live_added_modified_and_deleted_files_before_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("modified.txt"), b"one\n").unwrap();
    fs::write(source.join("deleted.txt"), b"remove me\n").unwrap();
    fs::write(source.join("unchanged.txt"), b"same\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("baseline");
        c
    });

    fs::write(source.join("modified.txt"), b"two\n").unwrap();
    fs::remove_file(source.join("deleted.txt")).unwrap();
    fs::write(source.join("added.txt"), b"new\n").unwrap();

    let text = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("3650d");
        c
    });
    let mut lines = text.lines().collect::<Vec<_>>();
    lines.sort();
    assert_eq!(
        lines,
        vec![
            " A sample/added.txt",
            " D sample/deleted.txt",
            " M sample/modified.txt",
        ]
    );

    let json = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("3650d")
            .arg("--json");
        c
    });
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["changes"]["total"], 3, "{json}");
    assert_eq!(value["changes"]["added"], 1, "{json}");
    assert_eq!(value["changes"]["modified"], 1, "{json}");
    assert_eq!(value["changes"]["deleted"], 1, "{json}");
}

#[test]
fn state_default_uses_root_registration_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let alpha = tmp.path().join("alpha");
    let beta = tmp.path().join("beta");
    let state = tmp.path().join("state");
    fs::create_dir_all(&alpha).unwrap();
    fs::create_dir_all(&beta).unwrap();
    fs::write(alpha.join("note.txt"), b"alpha v1\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("alpha")
            .arg(&alpha);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("alpha baseline");
        c
    });

    fs::write(alpha.join("note.txt"), b"alpha v2\n").unwrap();
    fs::write(alpha.join("new.txt"), b"alpha new\n").unwrap();
    fs::write(beta.join("note.txt"), b"beta v1\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("beta")
            .arg(&beta);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("beta baseline");
        c
    });

    fs::write(alpha.join("note.txt"), b"alpha v3\n").unwrap();
    fs::write(beta.join("note.txt"), b"beta v2\n").unwrap();

    let text = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("state");
        c
    });
    let mut lines = text.lines().collect::<Vec<_>>();
    lines.sort();
    assert_eq!(
        lines,
        vec![" A alpha/new.txt", " M alpha/note.txt", " M beta/note.txt",],
        "{text}"
    );

    let json = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("state").arg("--json");
        c
    });
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["basis"], serde_json::Value::Null, "{json}");
    assert!(
        value["basis_roots"].is_null()
            || value["basis_roots"]
                .as_array()
                .is_some_and(|roots| roots.is_empty()),
        "{json}"
    );
    assert_eq!(value["changes"]["total"], 3, "{json}");
    assert_eq!(value["changes"]["added"], 1, "{json}");
    assert_eq!(value["changes"]["modified"], 2, "{json}");
}

#[test]
fn prune_preserves_each_root_first_snapshot_for_default_state() {
    let tmp = tempfile::tempdir().unwrap();
    let alpha = tmp.path().join("alpha");
    let beta = tmp.path().join("beta");
    let state = tmp.path().join("state");
    fs::create_dir_all(&alpha).unwrap();
    fs::create_dir_all(&beta).unwrap();
    fs::write(alpha.join("note.txt"), b"alpha v1\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("alpha")
            .arg(&alpha);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let alpha_basis: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row(
            "select id from snapshots order by created_at asc limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    fs::write(beta.join("note.txt"), b"beta v1\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("beta")
            .arg(&beta);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let beta_basis: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row(
            "select id from snapshots order by created_at asc limit 1 offset 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    fs::write(alpha.join("note.txt"), b"alpha v2\n").unwrap();
    fs::write(beta.join("note.txt"), b"beta v2\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let current: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row("select value from refs where name='current'", [], |row| {
            row.get(0)
        })
        .unwrap()
    };

    let prune = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--keep-daily")
            .arg("0")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    assert!(prune.contains("deleted_snapshots 0"), "{prune}");
    let remaining = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        let mut stmt = conn.prepare("select id from snapshots").unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .unwrap()
    };
    assert!(remaining.contains(&alpha_basis), "{remaining:?}");
    assert!(remaining.contains(&beta_basis), "{remaining:?}");
    assert!(remaining.contains(&current), "{remaining:?}");
}

#[test]
fn state_hides_untracked_path_even_when_basis_snapshot_contains_it() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("old.txt"), b"old\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("initial");
        c
    });

    fs::remove_file(source.join("old.txt")).unwrap();
    let deleted = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--deleted")
            .arg("-r")
            .arg("sample");
        c
    });
    assert_eq!(deleted, " D sample/old.txt\n");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("untrack")
            .arg("-r")
            .arg("sample")
            .arg("old.txt");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("after untrack");
        c
    });
    let deleted_after_untrack = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--deleted")
            .arg("-r")
            .arg("sample");
        c
    });
    assert_eq!(deleted_after_untrack, "");
}

#[test]
fn state_default_pathspec_and_untracked_files_follow_git_like_scope() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(source.join("dir/nested")).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(source.join("delete-me.txt"), b"delete\n").unwrap();
    fs::write(source.join("dir/beta.txt"), b"beta\n").unwrap();
    fs::write(source.join("dir/nested/gamma.txt"), b"gamma\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fs::remove_file(source.join("delete-me.txt")).unwrap();
    let deleted_short = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("-D");
        c
    });
    assert_eq!(deleted_short, " D sample/delete-me.txt\n");

    let dir_only = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("--")
            .arg("dir");
        c
    });
    assert!(dir_only.contains(" A sample/dir/beta.txt"), "{dir_only}");
    assert!(
        dir_only.contains(" A sample/dir/nested/gamma.txt"),
        "{dir_only}"
    );
    assert!(!dir_only.contains("alpha.txt"), "{dir_only}");
    assert!(!dir_only.contains("delete-me.txt"), "{dir_only}");

    let local_dir_only = output({
        let mut c = mj();
        c.current_dir(source.join("dir"))
            .arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--")
            .arg(".");
        c
    });
    assert!(
        local_dir_only.contains(" A dir/beta.txt"),
        "{local_dir_only}"
    );
    assert!(!local_dir_only.contains("sample/"), "{local_dir_only}");
    assert!(!local_dir_only.contains("alpha.txt"), "{local_dir_only}");
    let local_json = output({
        let mut c = mj();
        c.current_dir(source.join("dir"))
            .arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--json")
            .arg("--")
            .arg(".");
        c
    });
    let local_value: serde_json::Value = serde_json::from_str(&local_json).unwrap();
    assert_eq!(local_value["changes"]["files"][0]["root"], "sample");
    assert!(
        local_value["changes"]["files"]
            .as_array()
            .unwrap()
            .iter()
            .all(|file| file["path"].as_str().unwrap().starts_with("dir/")),
        "{local_json}"
    );

    fs::create_dir_all(source.join("loose-dir")).unwrap();
    fs::write(source.join("loose-dir/child.txt"), b"loose\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("untrack")
            .arg("-r")
            .arg("sample")
            .arg("alpha.txt");
        c
    });
    let untracked = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("-U")
            .arg("--status")
            .arg("?");
        c
    });
    let mut untracked_lines = untracked.lines().collect::<Vec<_>>();
    untracked_lines.sort();
    assert_eq!(
        untracked_lines,
        vec![" ? sample/alpha.txt", " ? sample/loose-dir/"],
        "{untracked}"
    );
    assert!(!untracked.contains("loose-dir/child.txt"), "{untracked}");
    let ordered_with_untracked = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("-U");
        c
    });
    assert!(
        ordered_with_untracked
            .lines()
            .next()
            .is_some_and(|line| line.starts_with(" ? ")),
        "{ordered_with_untracked}"
    );

    let outside_untracked = {
        let output = {
            let mut c = mj();
            c.current_dir(tmp.path())
                .arg("--home")
                .arg(&state)
                .arg("state")
                .arg("-U");
            c.output().expect("run command")
        };
        assert!(!output.status.success());
        String::from_utf8_lossy(&output.stderr).to_string()
    };
    assert!(
        outside_untracked.contains("requires --root or running inside a configured root"),
        "{outside_untracked}"
    );
}

#[test]
fn state_orders_untracked_first_then_newest_tracked_operation() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("older.txt"), b"older\n").unwrap();
    fs::write(source.join("newer.txt"), b"newer\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.execute(
            "update tracked_paths set first_seen_at='2029-01-01T00:00:00Z', last_seen_at='2029-01-01T00:00:00Z' where root_id='sample' and path='older.txt'",
            [],
        )
        .unwrap();
        conn.execute(
            "update tracked_paths set first_seen_at='2030-01-01T00:00:00Z', last_seen_at='2030-01-01T00:00:00Z' where root_id='sample' and path='newer.txt'",
            [],
        )
        .unwrap();
    }
    fs::write(source.join("loose.txt"), b"loose\n").unwrap();

    let ordered = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("-U")
            .arg("--status")
            .arg("?,A");
        c
    });
    let lines = ordered.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            " ? sample/loose.txt",
            " A sample/newer.txt",
            " A sample/older.txt",
        ],
        "{ordered}"
    );
}

#[test]
fn state_relative_reference_filters_added_paths_older_than_window() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        let old_snapshot_time = (Utc::now() - ChronoDuration::hours(2)).to_rfc3339();
        conn.execute("update snapshots set created_at=?1", [&old_snapshot_time])
            .unwrap();
    }

    fs::write(source.join("old-added.txt"), b"old\n").unwrap();
    filetime::set_file_mtime(
        source.join("old-added.txt"),
        filetime::FileTime::from_unix_time(1, 0),
    )
    .unwrap();
    fs::write(source.join("recent-added.txt"), b"recent\n").unwrap();

    let state_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("1m")
            .arg("-r")
            .arg("sample");
        c
    });
    assert!(
        state_output.contains(" A sample/recent-added.txt"),
        "{state_output}"
    );
    assert!(
        !state_output.contains("old-added.txt"),
        "old additions outside the relative window must not be shown\n{state_output}"
    );
}

#[test]
fn state_orders_recent_content_changes_before_snapshot_refreshed_adds() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("aaa-stale.txt"), b"stale\n").unwrap();
    fs::write(source.join("zzz-recent.txt"), b"recent v1\n").unwrap();
    filetime::set_file_mtime(
        source.join("aaa-stale.txt"),
        filetime::FileTime::from_unix_time(1, 0),
    )
    .unwrap();
    filetime::set_file_mtime(
        source.join("zzz-recent.txt"),
        filetime::FileTime::from_unix_time(1, 0),
    )
    .unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fs::write(source.join("zzz-recent.txt"), b"recent v2\n").unwrap();
    filetime::set_file_mtime(
        source.join("zzz-recent.txt"),
        filetime::FileTime::from_unix_time(1_800_000_000, 0),
    )
    .unwrap();
    {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.execute(
            "update tracked_paths
             set first_seen_at='2020-01-01T00:00:00Z',
                 last_seen_at='2030-01-01T00:00:00Z'
             where root_id='sample'",
            [],
        )
        .unwrap();
    }

    let ordered = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample");
        c
    });
    let lines = ordered.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![" M sample/zzz-recent.txt", " A sample/aaa-stale.txt"],
        "{ordered}"
    );
}

#[test]
fn state_diff_short_option_prints_git_style_hunks_only() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    let original = (0..220)
        .map(|i| format!("line-{i:03} original\n"))
        .collect::<String>();
    fs::write(source.join("long.txt"), original).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let baseline_op: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row(
            "select op_id from snapshots order by created_at asc limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    let updated = (0..220)
        .map(|i| {
            if i == 120 {
                "line-120 updated\n".to_string()
            } else {
                format!("line-{i:03} original\n")
            }
        })
        .collect::<String>();
    fs::write(source.join("long.txt"), updated).unwrap();

    let diff = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg(&baseline_op)
            .arg("-r")
            .arg("sample")
            .arg("-d");
        c
    });
    assert!(diff.contains(" M sample/long.txt"), "{diff}");
    assert!(diff.contains("    -line-120 original"), "{diff}");
    assert!(diff.contains("    +line-120 updated"), "{diff}");
    assert!(diff.contains("     line-117 original"), "{diff}");
    assert!(diff.contains("     line-123 original"), "{diff}");
    assert!(
        !diff.contains("line-001 original"),
        "diff should not print unrelated leading context\n{diff}"
    );
    assert!(
        !diff.contains("line-200 original"),
        "diff should not print unrelated trailing context\n{diff}"
    );
}

#[test]
fn untrack_supports_path_files_summary_and_excluded_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let path_file = tmp.path().join("paths.txt");
    fs::create_dir_all(&source).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--no-default-excludes");
        c
    });
    fs::write(source.join("keep.txt"), b"keep\n").unwrap();
    fs::write(source.join("manual.txt"), b"manual\n").unwrap();
    fs::write(source.join("skip.tmp"), b"tmp\n").unwrap();
    fs::write(source.join("tmp_runtime.1"), b"tmp\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("set")
            .arg("sample")
            .arg("--exclude")
            .arg("*.tmp")
            .arg("--exclude")
            .arg("tmp_*");
        c
    });

    let dry_run = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("untrack")
            .arg("-r")
            .arg("sample")
            .arg("--excluded")
            .arg("--dry-run")
            .arg("--summary");
        c
    });
    assert!(dry_run.contains("requested 2"), "{dry_run}");
    assert!(dry_run.contains("would_untrack 2"), "{dry_run}");

    let summary = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("untrack")
            .arg("-r")
            .arg("sample")
            .arg("--excluded")
            .arg("--summary");
        c
    });
    assert!(summary.contains("requested 2"), "{summary}");
    assert!(summary.contains("untracked 2"), "{summary}");
    let config = fs::read_to_string(state.join("config.toml")).unwrap();
    assert!(
        !config.contains("skip.tmp") && !config.contains("tmp_runtime.1"),
        "--excluded cleanup should not persist one explicit_untrack pattern per excluded path\n{config}"
    );

    let deleted = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-r")
            .arg("sample")
            .arg("--status")
            .arg("A,D");
        c
    });
    assert!(!deleted.contains("skip.tmp"), "{deleted}");
    assert!(!deleted.contains("tmp_runtime.1"), "{deleted}");

    let empty_excluded = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("untrack")
            .arg("-r")
            .arg("sample")
            .arg("--excluded")
            .arg("--dry-run")
            .arg("--summary");
        c
    });
    assert!(empty_excluded.contains("requested 0"), "{empty_excluded}");
    assert!(
        empty_excluded.contains("would_untrack 0"),
        "{empty_excluded}"
    );

    fs::write(&path_file, "manual.txt\n").unwrap();
    let path_file_summary = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("untrack")
            .arg("-r")
            .arg("sample")
            .arg("--path-file")
            .arg(&path_file)
            .arg("--summary");
        c
    });
    assert!(
        path_file_summary.contains("requested 1"),
        "{path_file_summary}"
    );
    assert!(
        path_file_summary.contains("untracked 1"),
        "{path_file_summary}"
    );

    let json = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("untrack")
            .arg("-r")
            .arg("sample")
            .arg("keep.txt")
            .arg("--json");
        c
    });
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["requested"], 1);
    assert_eq!(value["untracked"], 1);
}

#[test]
fn state_reference_reports_file_changes_since_operation() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let other = tmp.path().join("other");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&other).unwrap();
    fs::create_dir_all(source.join("memo")).unwrap();
    fs::write(source.join("modified.txt"), b"one").unwrap();
    fs::write(source.join("deleted.txt"), b"remove me").unwrap();
    fs::write(other.join("other.txt"), b"one").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("other")
            .arg(&other);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("baseline");
        c
    });
    let baseline_op: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row(
            "select op_id from snapshots order by created_at asc limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    fs::write(source.join("modified.txt"), b"two").unwrap();
    fs::remove_file(source.join("deleted.txt")).unwrap();
    fs::write(source.join("added.txt"), b"new").unwrap();
    fs::write(source.join("memo/new.md"), b"memo\n").unwrap();
    filetime::set_file_mtime(
        source.join("memo"),
        filetime::FileTime::from_unix_time(1, 0),
    )
    .unwrap();
    fs::write(other.join("other.txt"), b"two").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("changed");
        c
    });

    let state_output = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("state").arg(&baseline_op);
        c
    });
    let mut state_lines = state_output.lines().collect::<Vec<_>>();
    state_lines.sort();
    assert_eq!(
        state_lines,
        vec![
            " A sample/added.txt",
            " A sample/memo/new.md",
            " D sample/deleted.txt",
            " M other/other.txt",
            " M sample/modified.txt",
        ]
    );
    let default_state_output = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("state");
        c
    });
    let mut default_state_lines = default_state_output.lines().collect::<Vec<_>>();
    default_state_lines.sort();
    assert_eq!(
        default_state_lines,
        vec![
            " A sample/added.txt",
            " A sample/memo/new.md",
            " D sample/deleted.txt",
            " M other/other.txt",
            " M sample/modified.txt",
        ]
    );
    let before_first_snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("state").arg("3650d");
        c
    });
    let mut before_first_lines = before_first_snapshot.lines().collect::<Vec<_>>();
    before_first_lines.sort();
    assert_eq!(before_first_lines, state_lines);
    let local_relative_state_output = output({
        let mut c = mj();
        c.current_dir(&source)
            .arg("--home")
            .arg(&state)
            .arg("state")
            .arg("1h");
        c
    });
    let mut local_relative_lines = local_relative_state_output.lines().collect::<Vec<_>>();
    local_relative_lines.sort();
    assert_eq!(
        local_relative_lines,
        vec![
            " A added.txt",
            " A memo/new.md",
            " D deleted.txt",
            " M modified.txt",
        ]
    );
    let deleted_output = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("state").arg("--deleted");
        c
    });
    assert_eq!(deleted_output, " D sample/deleted.txt\n");
    let status_deleted_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--status")
            .arg("D");
        c
    });
    assert_eq!(status_deleted_output, deleted_output);
    let status_added_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--status")
            .arg("A");
        c
    });
    let mut status_added_lines = status_added_output.lines().collect::<Vec<_>>();
    status_added_lines.sort();
    assert_eq!(
        status_added_lines,
        vec![" A sample/added.txt", " A sample/memo/new.md"]
    );
    let status_added_deleted_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--status")
            .arg("A,D");
        c
    });
    let mut status_added_deleted_lines = status_added_deleted_output.lines().collect::<Vec<_>>();
    status_added_deleted_lines.sort();
    assert_eq!(
        status_added_deleted_lines,
        vec![
            " A sample/added.txt",
            " A sample/memo/new.md",
            " D sample/deleted.txt",
        ]
    );
    assert!(!state_output.contains("m sample/memo"), "{state_output}");
    let root_filtered_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--root")
            .arg("sample")
            .arg(&baseline_op);
        c
    });
    let mut root_filtered_lines = root_filtered_output.lines().collect::<Vec<_>>();
    root_filtered_lines.sort();
    assert_eq!(
        root_filtered_lines,
        vec![
            " A sample/added.txt",
            " A sample/memo/new.md",
            " D sample/deleted.txt",
            " M sample/modified.txt",
        ]
    );
    assert!(
        !root_filtered_output.contains("m sample/memo"),
        "{root_filtered_output}"
    );
    let root_meta_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--root")
            .arg("sample")
            .arg(&baseline_op)
            .arg("--meta");
        c
    });
    assert!(
        root_meta_output.contains(" m sample/memo"),
        "{root_meta_output}"
    );
    assert!(
        root_meta_output.contains(" A sample/memo/new.md"),
        "{root_meta_output}"
    );
    let short_root_filtered_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg(&baseline_op)
            .arg("-r")
            .arg("sample");
        c
    });
    assert_eq!(short_root_filtered_output, root_filtered_output);
    let root_diff_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg(&baseline_op)
            .arg("-r")
            .arg("sample")
            .arg("--diff");
        c
    });
    assert!(
        root_diff_output.contains(" A sample/added.txt"),
        "{root_diff_output}"
    );
    assert!(root_diff_output.contains("    +new"), "{root_diff_output}");
    assert!(
        root_diff_output.contains(" D sample/deleted.txt"),
        "{root_diff_output}"
    );
    assert!(
        root_diff_output.contains("    -remove me"),
        "{root_diff_output}"
    );
    assert!(
        root_diff_output.contains(" M sample/modified.txt"),
        "{root_diff_output}"
    );
    assert!(root_diff_output.contains("    -one"), "{root_diff_output}");
    assert!(root_diff_output.contains("    +two"), "{root_diff_output}");
    let colored_state_output = output({
        let mut c = mj();
        c.env_remove("NO_COLOR")
            .env("MJ_COLOR", "always")
            .arg("--home")
            .arg(&state)
            .arg("state")
            .arg(&baseline_op);
        c
    });
    assert!(
        colored_state_output
            .contains("\u{1b}[1;32mA\u{1b}[0m \u{1b}[1;96msample\u{1b}[0m/added.txt"),
        "{colored_state_output:?}"
    );
    assert!(
        colored_state_output
            .contains("\u{1b}[1;31mD\u{1b}[0m \u{1b}[1;96msample\u{1b}[0m/deleted.txt"),
        "{colored_state_output:?}"
    );
    assert!(
        colored_state_output
            .contains("\u{1b}[1;33mM\u{1b}[0m \u{1b}[1;96msample\u{1b}[0m/modified.txt"),
        "{colored_state_output:?}"
    );
    let colored_diff_output = output({
        let mut c = mj();
        c.env_remove("NO_COLOR")
            .env("MJ_COLOR", "always")
            .arg("--home")
            .arg(&state)
            .arg("state")
            .arg(&baseline_op)
            .arg("-r")
            .arg("sample")
            .arg("--diff");
        c
    });
    assert!(
        colored_diff_output.contains("\u{1b}[31m    -one\u{1b}[0m"),
        "{colored_diff_output:?}"
    );
    assert!(
        colored_diff_output.contains("\u{1b}[32m    +two\u{1b}[0m"),
        "{colored_diff_output:?}"
    );

    let json_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg("--root")
            .arg("sample")
            .arg(&baseline_op[..12])
            .arg("--json");
        c
    });
    let value: serde_json::Value = serde_json::from_str(&json_output).unwrap();
    assert_eq!(value["basis"]["kind"], "operation");
    assert_eq!(value["changes"]["total"], 4);
    assert_eq!(value["changes"]["added"], 2);
    assert_eq!(value["changes"]["modified"], 1);
    assert_eq!(value["changes"]["deleted"], 1);

    let short_json_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("state")
            .arg(&baseline_op[..12])
            .arg("-r")
            .arg("sample")
            .arg("-j");
        c
    });
    let short_value: serde_json::Value = serde_json::from_str(&short_json_output).unwrap();
    assert_eq!(short_value["changes"]["total"], 4);
}

#[test]
fn note_edits_operation_and_snapshot_messages() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let snapshot_id = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("old note");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    let op_id: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row(
            "select op_id from snapshots where id=?1",
            [&snapshot_id],
            |row| row.get(0),
        )
        .unwrap()
    };

    let initial = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("note").arg(&op_id);
        c
    });
    assert_eq!(initial, "old note\n");

    let snapshot_note = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("note")
            .arg(&snapshot_id)
            .arg("-m")
            .arg("new note");
        c
    });
    assert!(snapshot_note.contains("noted op-"), "{snapshot_note}");
    assert!(snapshot_note.contains("new note"), "{snapshot_note}");
    let shown = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("note").arg(&op_id);
        c
    });
    assert_eq!(shown, "new note\n");

    let mut stdin_command = mj();
    stdin_command
        .arg("--home")
        .arg(&state)
        .arg("note")
        .arg(&op_id)
        .arg("--stdin")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = stdin_command.spawn().expect("spawn note --stdin");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"stdin note\n")
        .unwrap();
    let stdin_output = child.wait_with_output().expect("wait note --stdin");
    assert!(
        stdin_output.status.success(),
        "note --stdin failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stdin_output.stdout),
        String::from_utf8_lossy(&stdin_output.stderr)
    );
    let stdin_stdout = String::from_utf8_lossy(&stdin_output.stdout);
    assert!(stdin_stdout.contains("noted op-"), "{stdin_stdout}");
    assert!(stdin_stdout.contains("stdin note"), "{stdin_stdout}");
    assert_eq!(
        output({
            let mut c = mj();
            c.arg("--home").arg(&state).arg("note").arg(&op_id);
            c
        }),
        "stdin note\n"
    );

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("note")
            .arg(&op_id)
            .arg("--clear");
        c
    });
    assert_eq!(
        output({
            let mut c = mj();
            c.arg("--home").arg(&state).arg("note").arg(&op_id);
            c
        }),
        ""
    );
}

#[test]
fn log_waits_for_busy_database_instead_of_failing_immediately() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let db = state.join("db/majutsu.sqlite");
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let holder = thread::spawn(move || {
        let conn = Connection::open(db).unwrap();
        conn.busy_timeout(Duration::from_secs(5)).unwrap();
        conn.execute_batch("create table if not exists busy_test(id integer)")
            .unwrap();
        conn.execute_batch("begin immediate; insert into busy_test(id) values (1)")
            .unwrap();
        ready_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(900));
        conn.execute_batch("commit").unwrap();
    });
    ready_rx.recv().unwrap();

    let started = Instant::now();
    let log = output({
        let mut c = mj();
        c.env("MAJUTSU_DB_BUSY_TIMEOUT_SECS", "5")
            .arg("--home")
            .arg(&state)
            .arg("log");
        c
    });
    holder.join().unwrap();

    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "mj log should wait for the writer lock instead of racing past it"
    );
    assert!(log.contains("initial-scan"), "{log}");
}

#[test]
fn state_and_log_mark_large_and_volatile_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("db.sqlite-wal"), b"wal-1\n").unwrap();
    fs::write(source.join("model.bin"), vec![b'a'; 64]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--volatile")
            .arg("**/*.sqlite-wal")
            .arg("--include")
            .arg("**")
            .arg("--include")
            .arg("**/*.sqlite-wal")
            .arg("--large-min-size")
            .arg("32");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("baseline");
        c
    });
    let baseline_op: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row(
            "select op_id from snapshots order by created_at asc limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    fs::write(source.join("db.sqlite-wal"), b"wal-2\n").unwrap();
    fs::write(source.join("model.bin"), vec![b'b'; 64]).unwrap();
    let state_output = output({
        let mut c = mj();
        c.env("TERM", "dumb")
            .arg("--home")
            .arg(&state)
            .arg("state")
            .arg(&baseline_op);
        c
    });
    assert!(
        state_output.contains("M sample/db.sqlite-wal [volatile:checkpoint]"),
        "{state_output}"
    );
    assert!(
        state_output.contains("M sample/model.bin [large]"),
        "{state_output}"
    );

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("changed");
        c
    });
    let log_output = output({
        let mut c = mj();
        c.env("TERM", "dumb")
            .arg("--home")
            .arg(&state)
            .arg("log")
            .arg("--limit")
            .arg("1")
            .arg("--full");
        c
    });
    assert!(
        log_output.contains("M\tsample/db.sqlite-wal [volatile:checkpoint]"),
        "{log_output}"
    );
    assert!(
        log_output.contains("M\tsample/model.bin [large]"),
        "{log_output}"
    );
}

#[test]
fn state_inside_root_uses_local_paths_and_global_flag_restores_root_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("modified.txt"), b"one").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let baseline_op: String = {
        let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
        conn.query_row(
            "select op_id from snapshots order by created_at asc limit 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };

    fs::write(source.join("modified.txt"), b"two").unwrap();
    fs::write(source.join("added.txt"), b"new").unwrap();

    let local_output = output({
        let mut c = mj();
        c.current_dir(&source)
            .arg("--home")
            .arg(&state)
            .arg("state")
            .arg(&baseline_op);
        c
    });
    let mut local_lines = local_output.lines().collect::<Vec<_>>();
    local_lines.sort();
    assert_eq!(local_lines, vec![" A added.txt", " M modified.txt"]);

    let global_output = output({
        let mut c = mj();
        c.current_dir(&source)
            .arg("--home")
            .arg(&state)
            .arg("state")
            .arg("-g")
            .arg(&baseline_op);
        c
    });
    let mut global_lines = global_output.lines().collect::<Vec<_>>();
    global_lines.sort();
    assert_eq!(
        global_lines,
        vec![" A sample/added.txt", " M sample/modified.txt"]
    );

    let outside_output = output({
        let mut c = mj();
        c.current_dir(tmp.path())
            .arg("--home")
            .arg(&state)
            .arg("state")
            .arg(&baseline_op);
        c
    });
    let mut outside_lines = outside_output.lines().collect::<Vec<_>>();
    outside_lines.sort();
    assert_eq!(
        outside_lines,
        vec![" A sample/added.txt", " M sample/modified.txt"]
    );
}

#[test]
fn sync_publishes_event_journal_as_durable_remote_journal() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.txt"), b"durable content\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });

    let event_dir = state.join("queue/events");
    fs::create_dir_all(&event_dir).unwrap();
    fs::write(
        event_dir.join("event-durable.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "event_id": "event-durable",
            "kind": "fs-event",
            "observed_at": Utc::now().to_rfc3339(),
            "detail": "sample changed",
            "root_id": "sample",
            "path": "note.txt",
            "event_kind": "modify",
            "raw_backend": "test"
        }))
        .unwrap(),
    )
    .unwrap();

    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("durable_journal_enqueued 1"), "{sync}");
    assert!(sync.contains("durable_journal_acknowledged 1"), "{sync}");
    assert!(
        find_file_ending(&remote, "journal/event-durable.json").exists(),
        "remote journal object should be published"
    );

    let local_event: serde_json::Value =
        serde_json::from_slice(&fs::read(event_dir.join("event-durable.json")).unwrap()).unwrap();
    let remote_journal_key = local_event["remote_journal_key"].as_str().unwrap();
    assert!(!remote_journal_key.starts_with("hosts/"));
    assert!(remote_journal_key.ends_with("/journal/event-durable.json"));
    assert!(local_event["remote_journal_synced_at"].as_str().is_some());
    let payload_key = local_event["durable_payload_key"].as_str().unwrap();
    assert!(
        find_file_ending(&remote, payload_key).exists(),
        "{payload_key}"
    );
    assert_eq!(
        local_event["durable_payload_size"],
        serde_json::Value::from("durable content\n".len() as u64)
    );

    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status");
        c
    });
    assert!(status.contains("durable journal"), "{status}");
    assert!(
        status.contains("1 durable, 0 pending remote ack"),
        "{status}"
    );
}

#[test]
fn restore_applies_durable_journal_after_current_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let restore = tmp.path().join("restore");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.bin"), vec![b'x'; 256 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    fs::write(source.join("note.txt"), b"durable content\n").unwrap();
    let event_dir = state.join("queue/events");
    fs::create_dir_all(&event_dir).unwrap();
    fs::write(
        event_dir.join("event-restore.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "event_id": "event-restore",
            "kind": "fs-event",
            "observed_at": Utc::now().to_rfc3339(),
            "detail": "sample changed",
            "root_id": "sample",
            "path": "note.txt",
            "event_kind": "modify",
            "raw_backend": "test"
        }))
        .unwrap(),
    )
    .unwrap();
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("durable_journal_acknowledged 1"), "{sync}");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/note.txt")).unwrap(),
        b"durable content\n"
    );
}

#[test]
fn clone_imports_durable_journal_and_restores_unsnapshotted_change() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.txt"), b"snapshot content\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::write(source.join("note.txt"), b"durable content\n").unwrap();
    let event_dir = state.join("queue/events");
    fs::create_dir_all(&event_dir).unwrap();
    fs::write(
        event_dir.join("event-clone-restore.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "event_id": "event-clone-restore",
            "kind": "fs-event",
            "observed_at": Utc::now().to_rfc3339(),
            "detail": "sample changed",
            "root_id": "sample",
            "path": "note.txt",
            "event_kind": "modify",
            "raw_backend": "test"
        }))
        .unwrap(),
    )
    .unwrap();
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("durable_journal_acknowledged 1"), "{sync}");

    let clone_output = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    assert!(
        clone_output.contains("imported_durable_journals 1"),
        "{clone_output}"
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/note.txt")).unwrap(),
        b"durable content\n"
    );
}

#[test]
fn sync_live_diff_preserves_unsnapshotted_change_without_watch_event() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.txt"), b"snapshot content\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::write(source.join("note.txt"), b"durable content\n").unwrap();
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("live_diff_journal_enqueued 1"), "{sync}");
    assert!(sync.contains("durable_journal_acknowledged 1"), "{sync}");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/note.txt")).unwrap(),
        b"durable content\n"
    );
}

#[test]
fn remote_init_validates_empty_file_remote_root() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let init = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("init");
        c
    });
    assert!(init.contains("initialized remote root"), "{init}");
    assert!(init.contains("hosts 0"), "{init}");
    assert!(!remote.join("hosts/index.json").exists());
}

#[test]
fn sync_wait_repairs_missing_remote_object_when_local_copy_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.bin"), vec![b'x'; 256 * 1024]).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.env("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE", "0")
            .env("MAJUTSU_SYNC_LOCAL_OBJECT_PRUNE", "0")
            .arg("--home")
            .arg(&state)
            .arg("sync")
            .arg("--wait");
        c
    });

    let missing_key = first_blob_object_key(&state);
    let removed = remove_remote_payload_aliases(&remote, &missing_key);
    assert!(
        !removed.is_empty(),
        "remote object missing before test setup"
    );

    let repaired = output({
        let mut c = mj();
        c.env("MAJUTSU_SYNC_LOCAL_PAYLOAD_CACHE_PRUNE", "0")
            .env("MAJUTSU_SYNC_LOCAL_OBJECT_PRUNE", "0")
            .env("MAJUTSU_SYNC_WAIT_DEEP_REPAIR", "1")
            .arg("--home")
            .arg(&state)
            .arg("sync")
            .arg("--wait");
        c
    });
    assert!(
        repaired.contains("wait_remote_repair_repaired 1"),
        "{repaired}"
    );
}

#[test]
fn sync_live_diff_preserves_unsnapshotted_large_file_via_large_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("large.bin"), b"snapshot content\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--large-min-size")
            .arg("4")
            .arg("--large-chunking")
            .arg("fixed")
            .arg("--large-chunk-size")
            .arg("4");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mut durable = Vec::new();
    for i in 0..257 {
        durable.push(b'A' + (i % 23) as u8);
    }
    fs::write(source.join("large.bin"), &durable).unwrap();
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("live_diff_journal_enqueued 1"), "{sync}");
    assert!(sync.contains("durable_journal_acknowledged 1"), "{sync}");

    let event_path = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            fs::read_to_string(path)
                .map(|text| text.contains("\"raw_backend\": \"live-diff\""))
                .unwrap_or(false)
        })
        .expect("live diff event");
    let event: serde_json::Value = serde_json::from_slice(&fs::read(&event_path).unwrap()).unwrap();
    assert!(event["durable_large_manifest_key"].as_str().is_some());
    assert!(event["durable_large_chunk_count"].as_u64().unwrap() > 1);
    assert!(event["durable_payload_key"].as_str().is_none());

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(fs::read(restore.join("sample/large.bin")).unwrap(), durable);
}

#[test]
fn sync_live_diff_preserves_unsnapshotted_chunked_file_via_large_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("medium.dat"), b"snapshot content\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 4096")
        .replace("chunked_min_size = 524288", "chunked_min_size = 64")
        .replace("chunked_chunk_size = 524288", "chunked_chunk_size = 16");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let mut durable = Vec::new();
    for i in 0..257 {
        durable.push(b'a' + (i % 26) as u8);
    }
    fs::write(source.join("medium.dat"), &durable).unwrap();
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("live_diff_journal_enqueued 1"), "{sync}");
    assert!(sync.contains("durable_journal_acknowledged 1"), "{sync}");

    let event_path = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            fs::read_to_string(path)
                .map(|text| text.contains("\"raw_backend\": \"live-diff\""))
                .unwrap_or(false)
        })
        .expect("live diff event");
    let event: serde_json::Value = serde_json::from_slice(&fs::read(&event_path).unwrap()).unwrap();
    assert!(event["durable_large_manifest_key"].as_str().is_some());
    assert!(event["durable_large_chunk_count"].as_u64().unwrap() >= 1);
    assert!(event["durable_payload_key"].as_str().is_none());

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/medium.dat")).unwrap(),
        durable
    );
}

#[cfg(unix)]
#[test]
fn sync_live_diff_preserves_unsnapshotted_symlink_without_watch_event() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    std::os::unix::fs::symlink("old-target", source.join("link")).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::remove_file(source.join("link")).unwrap();
    std::os::unix::fs::symlink("new-target", source.join("link")).unwrap();
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("live_diff_journal_enqueued 1"), "{sync}");
    assert!(sync.contains("durable_journal_acknowledged 1"), "{sync}");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_link(restore.join("sample/link")).unwrap(),
        std::path::PathBuf::from("new-target")
    );
}

#[cfg(unix)]
#[test]
fn sync_live_diff_preserves_unsnapshotted_fifo_without_watch_event() {
    use std::os::unix::fs::FileTypeExt;

    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let fifo = source.join("pipe");
    let status = Command::new("mkfifo").arg(&fifo).status().unwrap();
    assert!(status.success());
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("live_diff_journal_enqueued 1"), "{sync}");
    assert!(sync.contains("durable_journal_acknowledged 1"), "{sync}");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(
        fs::symlink_metadata(restore.join("sample/pipe"))
            .unwrap()
            .file_type()
            .is_fifo()
    );
}

#[test]
fn sync_live_diff_preserves_unsnapshotted_delete_without_watch_event() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("deleted.txt"), b"remove me\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::remove_file(source.join("deleted.txt")).unwrap();
    let sync = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(sync.contains("live_diff_journal_enqueued 1"), "{sync}");
    assert!(sync.contains("durable_journal_acknowledged 1"), "{sync}");

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(!restore.join("sample/deleted.txt").exists());
}

#[test]
fn snapshot_compacts_covered_durable_journal_and_sync_prunes_remote_journal() {
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.txt"), b"snapshot content\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    fs::write(source.join("note.txt"), b"durable content\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let live_diff_event_path = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            fs::read_to_string(path)
                .map(|text| text.contains("\"raw_backend\": \"live-diff\""))
                .unwrap_or(false)
        })
        .expect("live diff event");
    let event: serde_json::Value =
        serde_json::from_slice(&fs::read(&live_diff_event_path).unwrap()).unwrap();
    let remote_journal_key = event["remote_journal_key"].as_str().unwrap().to_string();
    assert!(
        remote.join(&remote_journal_key).exists(),
        "{remote_journal_key}"
    );

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(
        !live_diff_event_path.exists(),
        "covered durable journal should be locally compacted"
    );

    let sync = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_REMOTE_PRUNE_FORCE", "1");
        c
    });
    assert!(sync.contains("pruned_remote_journals 1"), "{sync}");
    assert!(
        !remote.join(&remote_journal_key).exists(),
        "stale remote durable journal should be pruned"
    );
}

#[test]
fn health_reports_unprotected_when_active_root_has_no_daemon_or_remote() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });

    let health = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("health").arg("--json");
        c
    });
    let value: serde_json::Value = serde_json::from_str(&health).unwrap();
    assert_eq!(value["state"], "unprotected");
    assert_eq!(value["roots"][0]["id"], "sample");
    assert_eq!(value["roots"][0]["present"], true);
    assert_eq!(value["roots"][0]["current_snapshot_includes"], false);
    assert_eq!(
        value["roots"][0]["last_changed_snapshot"],
        serde_json::Value::Null
    );
    let codes = value["issues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|issue| issue["code"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(codes.contains(&"daemon-unhealthy"), "{health}");
    assert!(codes.contains(&"remote-not-configured"), "{health}");

    let text = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("health");
        c
    });
    assert!(text.contains("state unprotected"), "{text}");
    assert!(text.contains("issues "), "{text}");
    assert!(text.contains("daemon state="), "{text}");
    assert!(text.contains("remote configured="), "{text}");
    assert!(text.contains("queue uploads="), "{text}");
    assert!(text.contains("roots active=1 total=1"), "{text}");
    assert!(text.contains("issue critical daemon-unhealthy"), "{text}");
    assert!(!text.contains("root sample status="), "{text}");

    let verbose = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("health").arg("--verbose");
        c
    });
    assert!(verbose.contains("root sample status=active"), "{verbose}");
}

#[test]
fn health_reports_root_last_changed_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"beta\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let health = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("health").arg("--json");
        c
    });
    let value: serde_json::Value = serde_json::from_str(&health).unwrap();
    let root = &value["roots"][0];
    assert_eq!(root["id"], "sample");
    assert_eq!(root["current_snapshot_includes"], true);
    assert_eq!(root["last_changed_snapshot"], value["current_snapshot"]);
    assert!(root["last_changed_at"].as_str().is_some(), "{health}");
}

#[test]
fn status_tolerates_missing_parent_snapshot_for_root_last_change() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let first = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .env("MAJUTSU_SNAPSHOT_ALLOW_NOOP", "1");
        c
    });
    Connection::open(state.join("db/majutsu.sqlite"))
        .unwrap()
        .execute(
            "delete from snapshots where id=?1",
            rusqlite::params![first],
        )
        .unwrap();

    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status");
        c
    });
    assert!(status.contains("Status"), "{status}");
    assert!(status.contains("sample"), "{status}");
}

#[test]
fn status_restarts_stale_daemon_for_active_roots() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    set_watch_backend(&state, "notify");
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    fs::create_dir_all(state.join("runtime")).unwrap();
    fs::write(state.join("runtime/daemon.pid"), b"99999999").unwrap();
    let status = output({
        let mut c = mj_auto();
        c.arg("--home").arg(&state).arg("status");
        c
    });
    assert!(status.contains("Daemon"), "{status}");
    assert!(!status.contains("stale pid"), "{status}");

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("stop");
        c
    });
}

#[test]
fn health_reports_root_remote_ack_from_cached_remote_current() {
    let tmp = tempfile::tempdir().unwrap();
    let docs = tmp.path().join("docs");
    let projects = tmp.path().join("projects");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&docs).unwrap();
    fs::create_dir_all(&projects).unwrap();
    fs::write(docs.join("notes.txt"), b"docs v1\n").unwrap();
    fs::write(projects.join("app.rs"), b"fn main() {}\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("docs")
            .arg(&docs);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("projects")
            .arg(&projects);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    fs::write(
        projects.join("app.rs"),
        b"fn main() { println!(\"v2\"); }\n",
    )
    .unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let health = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("health").arg("--json");
        c
    });
    let value: serde_json::Value = serde_json::from_str(&health).unwrap();
    let roots = value["roots"].as_array().unwrap();
    let docs_health = roots.iter().find(|root| root["id"] == "docs").unwrap();
    let projects_health = roots.iter().find(|root| root["id"] == "projects").unwrap();
    assert_eq!(docs_health["remote_snapshot_includes"], true, "{health}");
    assert_eq!(docs_health["remote_synced"], true, "{health}");
    assert!(
        docs_health["remote_synced_at"].as_str().is_some(),
        "{health}"
    );
    assert_eq!(
        projects_health["remote_snapshot_includes"], true,
        "{health}"
    );
    assert_eq!(projects_health["remote_synced"], false, "{health}");
    assert_eq!(projects_health["remote_synced_at"], serde_json::Value::Null);
}

#[test]
fn watch_records_runtime_health_and_notices_unprotected_state() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let notice = tmp.path().join("notice.txt");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("watch")
            .arg("--foreground")
            .arg("true")
            .arg("--backend")
            .arg("poll")
            .arg("--once")
            .env(
                "MAJUTSU_HEALTH_NOTICE_CMD",
                format!(
                    "printf '%s' \"$MAJUTSU_HEALTH_STATE:$MAJUTSU_HEALTH_ISSUE_CODES\" > {}",
                    notice.display()
                ),
            );
        c
    });

    let health_path = state.join("runtime/health.json");
    let health: serde_json::Value =
        serde_json::from_slice(&fs::read(&health_path).unwrap()).unwrap();
    assert_eq!(health["report"]["state"], "unprotected");
    assert!(health["observed_at"].as_str().is_some());
    let notice_text = fs::read_to_string(&notice).unwrap();
    assert!(notice_text.starts_with("unprotected:"), "{notice_text}");
    assert!(
        notice_text.contains("remote-not-configured"),
        "{notice_text}"
    );
}

#[test]
fn event_stat_and_compact_report_processed_journal_records() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("event").arg("stat");
        c
    });
    assert!(stat.contains("event_journal_records "));
    assert!(stat.contains("event_journal_pending 0"));
    assert!(stat.contains("event_journal_removable "));

    let dry_run = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("event")
            .arg("compact")
            .arg("--dry-run");
        c
    });
    assert!(dry_run.contains("dry_run true"));

    let compact = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("event").arg("compact");
        c
    });
    assert!(compact.contains("dry_run false"));
    assert!(compact.contains("event_journal_pending 0"));
}

#[test]
fn cli_help_describes_status_and_daemon_subcommands() {
    let status_help = output({
        let mut c = mj();
        c.arg("status").arg("--help");
        c
    });
    assert!(status_help.contains("--no-pager"));
    assert!(status_help.contains("--pager"));

    let daemon_help = output({
        let mut c = mj();
        c.arg("daemon").arg("--help");
        c
    });
    assert!(daemon_help.contains("Start the background watch daemon"));
    assert!(daemon_help.contains("Render a user or system service definition"));
    assert!(daemon_help.contains("Show daemon pid, IPC, queue, and journal health"));

    let fsck_help = output({
        let mut c = mj();
        c.arg("fsck").arg("--help");
        c
    });
    assert!(fsck_help.contains("--sample"));
    assert!(fsck_help.contains("--since"));
    assert!(fsck_help.contains("heavy payload or manifest phase"));
}

#[test]
fn require_mount_root_is_skipped_as_unmounted_without_mass_deletion() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("set")
            .arg("sample")
            .arg("--require-mount");
        c
    });
    let snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert!(snapshot.contains("files 0, large 0"));
    let roots = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(root_list_has(&roots, "sample", "unmounted"));
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(ops.contains("root-unmounted"));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "alpha\n"
    );
}

#[cfg(unix)]
#[test]
fn permission_denied_root_is_skipped_without_mass_deletion() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let blocked = source.join("blocked");
    let state = tmp.path().join("state");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&blocked).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    fs::write(blocked.join("secret.txt"), b"secret\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let original_mode = fs::metadata(&blocked).unwrap().permissions().mode();
    let mut perms = fs::metadata(&blocked).unwrap().permissions();
    perms.set_mode(0o0);
    fs::set_permissions(&blocked, perms).unwrap();
    let snapshot = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let mut restore_perms = fs::metadata(&blocked).unwrap().permissions();
    restore_perms.set_mode(original_mode);
    fs::set_permissions(&blocked, restore_perms).unwrap();
    assert!(snapshot.contains("files 0, large 0"));
    let roots = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("root").arg("list");
        c
    });
    assert!(root_list_has(&roots, "sample", "permission-denied"));
    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status").arg("--no-pager");
        c
    });
    assert!(status.contains("ISSUE"));
    assert!(status.contains("permission-denied"));
    let health = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("health").arg("--json");
        c
    });
    let health: serde_json::Value = serde_json::from_str(&health).unwrap();
    let root = &health["roots"][0];
    assert_eq!(root["status"], "permission-denied");
    assert_eq!(root["degraded_kind"], "permission-denied");
    assert!(root["degraded_at"].as_str().is_some());
    assert!(
        root["degraded_message"]
            .as_str()
            .unwrap()
            .contains("Permission")
    );
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(ops.contains("root-permission-denied"));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "alpha\n"
    );
}

#[test]
fn watch_once_creates_snapshot_without_daemonizing() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let mut child = mj()
        .arg("--home")
        .arg(&state)
        .arg("watch")
        .arg("--once")
        .arg("--backend")
        .arg("notify")
        .arg("--debounce-ms")
        .arg("100")
        .arg("--settle-ms")
        .arg("50")
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    let status = child.wait().unwrap();
    assert!(status.success());
    let status_output = mj()
        .arg("--home")
        .arg(&state)
        .arg("status")
        .output()
        .unwrap();
    assert!(status_output.status.success());
    assert!(String::from_utf8_lossy(&status_output.stdout).contains("current snap-"));
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("watch-buffer-flush"));
    assert!(events.contains("reason=quiet"));
    assert!(events.contains("events="));
    #[cfg(target_os = "linux")]
    {
        let log = output({
            let mut c = mj();
            c.env("TERM", "dumb").arg("--home").arg(&state).arg("log");
            c
        });
        assert!(log.contains("file-events-batch"), "{log}");
        assert!(log.contains("sample/alpha.txt"), "{log}");
        assert!(log.contains("watch:notify"), "{log}");
    }
}

#[test]
fn watch_uses_configured_timing_defaults() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("backend = \"fanotify\"", "backend = \"notify\"")
        .replace("backend = \"inotify\"", "backend = \"notify\"")
        .replace("debounce = 1500", "debounce = \"25ms\"")
        .replace("settle = 500", "settle = \"15ms\"")
        .replace("periodic_rescan = 3600", "periodic_rescan = \"0s\"");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let mut child = mj()
        .arg("--home")
        .arg(&state)
        .arg("watch")
        .arg("--once")
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    let status = child.wait().unwrap();
    assert!(status.success());
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("backend=notify"));
    assert!(events.contains("debounce_ms=25"));
    assert!(events.contains("settle_ms=15"));
    assert!(events.contains("periodic_rescan_secs=0"));
}

#[test]
fn watch_strict_mode_snapshots_each_observed_event_without_debounce() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("debounce = 1500", "debounce = \"2000ms\"")
        .replace("settle = 500", "settle = \"2000ms\"")
        .replace("periodic_rescan = 3600", "periodic_rescan = \"0s\"");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });

    let start = std::time::Instant::now();
    let mut child = mj()
        .arg("--home")
        .arg(&state)
        .arg("watch")
        .arg("--once")
        .arg("--backend")
        .arg("notify")
        .arg("--mode")
        .arg("strict")
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    let status = child.wait().unwrap();
    assert!(status.success());
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "strict watch waited for debounce/settle"
    );

    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("mode=strict"));
    assert!(!events.contains("watch-settle"));
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(ops.contains("watch strict event snapshot"));
    assert!(ops.contains("file-events-batch"));
}

#[cfg(target_os = "linux")]
#[test]
fn linux_inotify_backend_records_native_events() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let config = fs::read_to_string(state.join("config.toml")).unwrap();
    assert!(config.contains("backend = \"fanotify\""));
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let mut child = mj()
        .arg("--home")
        .arg(&state)
        .arg("watch")
        .arg("--once")
        .arg("--backend")
        .arg("inotify")
        .arg("--debounce-ms")
        .arg("100")
        .arg("--settle-ms")
        .arg("50")
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    let status = child.wait().unwrap();
    assert!(status.success());
    let status_output = mj()
        .arg("--home")
        .arg(&state)
        .arg("status")
        .output()
        .unwrap();
    assert!(status_output.status.success());
    assert!(String::from_utf8_lossy(&status_output.stdout).contains("current snap-"));
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("backend=inotify"));
    let log = output({
        let mut c = mj();
        c.env("TERM", "dumb").arg("--home").arg(&state).arg("log");
        c
    });
    assert!(log.contains("file-events-batch"), "{log}");
    assert!(log.contains("sample/alpha.txt"), "{log}");
    assert!(log.contains("watch:inotify"), "{log}");
    let health = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("health");
        c
    });
    assert!(
        health.contains("issue critical watch-attribution-unavailable"),
        "{health}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn linux_watch_defaults_to_fanotify_and_requires_root() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let output = mj()
        .arg("--home")
        .arg(&state)
        .arg("watch")
        .arg("--once")
        .arg("--debounce-ms")
        .arg("100")
        .arg("--settle-ms")
        .arg("50")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("fanotify backend requires root privileges"),
        "{stderr}"
    );
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("watch-backend-error"), "{events}");
    assert!(events.contains("backend=fanotify"), "{events}");
    assert!(!events.contains("fallback=inotify"), "{events}");
}

#[test]
fn notify_watch_can_create_periodic_rescan_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("watch")
            .arg("--once")
            .arg("--backend")
            .arg("notify")
            .arg("--periodic-rescan-secs")
            .arg("1");
        c
    });
    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status");
        c
    });
    assert!(status.contains("current snap-"));
    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(op_log.contains("watch periodic rescan"), "{op_log}");
}

#[test]
fn notify_watch_replays_pending_event_journal() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    fs::create_dir_all(state.join("queue/events")).unwrap();
    fs::write(
        state.join("queue/events/event-pending.json"),
        br#"{
  "event_id": "event-pending",
  "kind": "fs-event",
  "observed_at": "2999-01-01T00:00:00Z",
  "detail": "modify /tmp/pending"
}"#,
    )
    .unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("watch")
            .arg("--once")
            .arg("--backend")
            .arg("notify")
            .arg("--periodic-rescan-secs")
            .arg("0");
        c
    });
    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("status");
        c
    });
    assert!(status.contains("current snap-"));
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("event-journal-replay"));
}

#[test]
fn notify_watch_replay_syncs_current_snapshot_when_remote_is_configured() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    fs::create_dir_all(state.join("queue/events")).unwrap();
    fs::write(
        state.join("queue/events/event-pending.json"),
        br#"{
  "event_id": "event-pending",
  "kind": "fs-event",
  "observed_at": "2999-01-01T00:00:00Z",
  "detail": "modify /tmp/pending"
}"#,
    )
    .unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("watch")
            .arg("--once")
            .arg("--backend")
            .arg("notify")
            .arg("--periodic-rescan-secs")
            .arg("0");
        c
    });

    assert!(db_ref(&state, "current").is_some());
    assert!(db_ref(&state, "last-synced").is_some());
    assert!(host_metadata_export_path(&remote).exists());
    let host_dir = first_remote_host_dir(&remote);
    let host_id = host_dir.file_name().unwrap().to_string_lossy();
    let current_ref_name = format!("{host_id}/refs/current");
    assert_eq!(
        db_remote_ref_value(&state, &current_ref_name),
        db_ref(&state, "current")
    );
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("event-journal-replay"));
    assert!(events.contains("watch-sync"));
}

#[test]
fn daemon_service_renders_systemd_and_launchd_configs() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });

    let systemd = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("service");
        c
    });
    assert!(systemd.contains("[Service]"));
    assert!(systemd.contains("ExecStart="));
    assert!(systemd.contains("--home"));
    assert!(systemd.contains(state.to_str().unwrap()));
    assert!(systemd.contains("watch"));
    assert!(systemd.contains("--backend"));
    assert!(systemd.contains("--mode"));
    assert!(systemd.contains("EnvironmentFile="));
    assert!(systemd.contains("/daemon.env"));
    assert!(systemd.contains("/s3.env"));
    assert!(systemd.contains("MemoryMax=2048M"));
    assert!(systemd.contains("OOMPolicy=stop"));
    assert!(systemd.contains("Restart=on-failure"));
    assert!(systemd.contains("WantedBy=default.target"));

    let systemd_system = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("daemon")
            .arg("service")
            .arg("--scope")
            .arg("system");
        c
    });
    assert!(systemd_system.contains("User=root"));
    assert!(systemd_system.contains("UMask=0077"));
    assert!(systemd_system.contains("WantedBy=multi-user.target"));

    let launchd = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("daemon")
            .arg("service")
            .arg("--provider")
            .arg("launchd");
        c
    });
    assert!(launchd.contains("<key>ProgramArguments</key>"));
    assert!(launchd.contains("<string>--home</string>"));
    assert!(launchd.contains(&format!("<string>{}</string>", state.display())));
    assert!(launchd.contains("<key>KeepAlive</key>"));
}

#[test]
fn watch_memory_guard_stops_before_host_oom() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("file.txt"), b"memory guard\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("watch")
            .arg("--foreground")
            .arg("true")
            .arg("--backend")
            .arg("poll")
            .arg("--once")
            .arg("--max-rss-mib")
            .arg("1");
        c
    });
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("watch-memory-limit"), "{events}");
}

#[cfg(unix)]
#[test]
fn root_add_auto_starts_daemon_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    run({
        let mut c = mj_auto();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    set_watch_backend(&state, "notify");
    let added = output({
        let mut c = mj_auto();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    assert!(added.contains("added root sample"));
    assert!(added.contains("started daemon pid"));
    for _ in 0..50 {
        if state.join("runtime/daemon.sock").exists() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let status = output({
        let mut c = mj_auto();
        c.arg("--home").arg(&state).arg("status");
        c
    });
    assert!(status.contains("Daemon"));
    assert!(status.contains("running"));
    run({
        let mut c = mj_auto();
        c.arg("--home").arg(&state).arg("daemon").arg("stop");
        c
    });
}

#[cfg(unix)]
#[test]
fn daemon_doctor_and_restart_clean_stale_pid() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    fs::create_dir_all(state.join("runtime")).unwrap();
    fs::write(state.join("runtime/daemon.pid"), "99999999").unwrap();

    let doctor = output({
        let mut c = mj_auto();
        c.arg("--home").arg(&state).arg("daemon").arg("doctor");
        c
    });
    assert!(doctor.contains("daemon stale pid"));
    assert!(doctor.contains("action mj daemon restart"));

    let restarted = output({
        let mut c = mj_auto();
        c.arg("--home")
            .arg(&state)
            .arg("daemon")
            .arg("restart")
            .arg("--backend")
            .arg("notify");
        c
    });
    assert!(restarted.contains("cleaned stale daemon runtime"));
    assert!(restarted.contains("started daemon pid"));
    for _ in 0..50 {
        if state.join("runtime/daemon.sock").exists() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let status = output({
        let mut c = mj_auto();
        c.arg("--home").arg(&state).arg("daemon").arg("status");
        c
    });
    assert!(status.contains("running pid") || status.contains("ipc ok"));
    run({
        let mut c = mj_auto();
        c.arg("--home").arg(&state).arg("daemon").arg("stop");
        c
    });
}

#[cfg(unix)]
#[test]
fn daemon_status_uses_ipc_socket() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let restore = tmp.path().join("restore");
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let prepare = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("prepare")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert!(prepare.contains("restore_job "));
    fs::create_dir_all(state.join("queue/uploads")).unwrap();
    fs::write(
        state.join("queue/uploads/retry-upload.json"),
        serde_json::json!({
            "id": "retry-upload",
            "key": "objects/blobs/retry",
            "source": null,
            "inline": [114, 101, 116, 114, 121],
            "created_at": "2026-06-07T00:00:00Z",
            "attempts": 2,
            "retry_after": "2999-01-01T00:00:00Z"
        })
        .to_string(),
    )
    .unwrap();
    let mut child = mj()
        .arg("--home")
        .arg(&state)
        .arg("watch")
        .arg("--backend")
        .arg("poll")
        .arg("--interval-secs")
        .arg("60")
        .spawn()
        .unwrap();
    fs::create_dir_all(state.join("runtime")).unwrap();
    fs::write(state.join("runtime/daemon.pid"), child.id().to_string()).unwrap();
    for _ in 0..50 {
        if state.join("runtime/daemon.sock").exists() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("status");
        c
    });
    assert!(status.contains("Daemon"));
    assert!(status.contains("IPC                ok"));
    assert!(status.contains("RSS"));
    assert!(status.contains("VM size"));
    assert!(status.contains("Roots              1"));
    assert!(status.contains("Root Status"));
    assert!(status.contains("active             1"));
    assert!(status.contains("Journal"));
    assert!(status.contains("Pending            0"));
    assert!(status.contains("Queues"));
    assert!(status.contains("Uploads            1"));
    assert!(status.contains("Retrying           1"));
    assert!(status.contains("Delayed            1"));
    assert!(status.contains("Next retry         2999-01-01T00:00:00+00:00"));
    assert!(status.contains("Attempts           2"));
    assert!(status.contains("Max attempts       2"));
    assert!(status.contains("Backpressure       true"));
    assert!(status.contains("Restore jobs       1"));
    assert!(status.contains("Restore Status"));
    assert!(status.contains("prepared           1"));
    let metrics = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("metrics");
        c
    });
    assert!(metrics.contains("majutsu_daemon_up 1"));
    assert!(metrics.contains("majutsu_daemon_ipc_up 1"));
    assert!(metrics.contains("majutsu_daemon_rss_kib "));
    assert!(metrics.contains("majutsu_daemon_vm_size_kib "));
    assert!(metrics.contains("majutsu_daemon_roots 1"));
    assert!(metrics.contains("majutsu_daemon_queued_uploads 1"));
    assert!(metrics.contains("majutsu_daemon_queued_uploads_delayed 1"));
    assert!(metrics.contains("majutsu_daemon_upload_queue_backpressure 1"));
    assert!(metrics.contains("majutsu_daemon_restore_jobs 1"));
    assert!(metrics.contains("majutsu_daemon_root_status{status=\"active\"} 1"));
    assert!(metrics.contains("majutsu_daemon_restore_status{status=\"prepared\"} 1"));
    child.kill().unwrap();
    let _ = child.wait();
}

#[cfg(unix)]
#[test]
fn watch_can_start_daemon_when_not_foreground() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let started = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("watch")
            .arg("--foreground=false")
            .arg("--backend")
            .arg("poll")
            .arg("--interval-secs")
            .arg("60");
        c
    });
    assert!(started.contains("started daemon pid"));
    for _ in 0..50 {
        if state.join("runtime/daemon.sock").exists() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("status");
        c
    });
    assert!(status.contains("ipc ok") || status.contains("running pid"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("stop");
        c
    });
    let stopped = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("status");
        c
    });
    assert!(stopped.contains("stopped"));
    assert!(!state.join("runtime/daemon.pid").exists());
    assert!(!state.join("runtime/daemon.sock").exists());
}

#[test]
fn repeated_sync_skips_existing_snapshot_and_operation_exports() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("one.txt"), b"one\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    for i in 0..4 {
        fs::write(source.join("one.txt"), format!("one {i}\n")).unwrap();
        run({
            let mut c = mj();
            c.arg("--home")
                .arg(&state)
                .arg("snapshot")
                .arg("--message")
                .arg(format!("snapshot {i}"));
            c
        });
    }

    let first = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let second = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let first_count = synced_object_count(&first);
    let second_count = synced_object_count(&second);

    assert!(
        second_count < first_count / 2,
        "expected repeated sync to skip immutable exports: first={first_count} second={second_count}\nfirst:\n{first}\nsecond:\n{second}"
    );
}

#[test]
fn sync_reuploads_compacted_snapshot_manifest_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(source.join("dir")).unwrap();
    for i in 0..200 {
        fs::write(
            source.join("dir").join(format!("file-{i}.txt")),
            b"payload\n",
        )
        .unwrap();
    }

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("snapshot")
            .arg("--message")
            .arg("large manifest");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("sync")
            .env("MAJUTSU_SYNC_LOCAL_METADATA_CACHE_PRUNE", "0");
        c
    });

    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let manifest_key: String = conn
        .query_row("select manifest_key from snapshots limit 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    let full_manifest = serde_json::to_vec_pretty(&local_snapshot_manifest(&state, "asc")).unwrap();
    fs::write(state.join(&manifest_key), &full_manifest).unwrap();
    fs::write(remote.join(&manifest_key), &full_manifest).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let full_remote_len = fs::metadata(remote.join(&manifest_key)).unwrap().len();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let compact_remote_len = fs::metadata(remote.join(&manifest_key)).unwrap().len();
    assert!(
        compact_remote_len < full_remote_len / 2,
        "remote snapshot manifest should be overwritten with compact payload: full={full_remote_len} compact={compact_remote_len}"
    );
}

#[test]
fn prune_drop_missing_remote_history_unblocks_sync_after_compacted_tree_loss() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });

    fs::write(source.join("beta.txt"), b"beta\n").unwrap();
    let second = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    })
    .lines()
    .find_map(|line| line.strip_prefix("snapshot "))
    .unwrap()
    .to_string();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });

    let old_manifest = local_snapshot_manifest_by_id(&state, &second);
    let conn = Connection::open(state.join("db/majutsu.sqlite")).unwrap();
    let manifest_key: String = conn
        .query_row(
            "select manifest_key from snapshots where id=?1",
            [&second],
            |row| row.get(0),
        )
        .unwrap();
    let tree_key = old_manifest["root_trees"]["sample"]["tree_key"]
        .as_str()
        .unwrap()
        .to_string();

    fs::write(source.join("gamma.txt"), b"gamma\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });

    fs::remove_file(state.join(&tree_key)).unwrap();
    let _ = fs::remove_file(remote.join(&tree_key));
    let _ = fs::remove_file(remote.join(&manifest_key));

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--drop-missing-remote-history");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert!(!remote.join(&manifest_key).exists());
}

#[cfg(unix)]
#[test]
fn daemon_start_accepts_watch_timing_overrides() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let started = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("daemon")
            .arg("start")
            .arg("--backend")
            .arg("poll")
            .arg("--interval-secs")
            .arg("60")
            .arg("--debounce-ms")
            .arg("25")
            .arg("--settle-ms")
            .arg("25")
            .arg("--periodic-rescan-secs")
            .arg("0");
        c
    });
    assert!(started.contains("started daemon pid"));
    for _ in 0..50 {
        if state.join("runtime/daemon.sock").exists() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("status");
        c
    });
    assert!(status.contains("ipc ok") || status.contains("running pid"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("stop");
        c
    });
}

#[cfg(unix)]
#[test]
fn daemon_watch_snapshot_can_sync_clone_and_restore() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let started = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("daemon")
            .arg("start")
            .arg("--backend")
            .arg("notify")
            .arg("--settle-ms")
            .arg("50")
            .arg("--periodic-rescan-secs")
            .arg("3600");
        c.env("MAJUTSU_SESSION_ID", "operator-session")
            .env("MAJUTSU_SESSION_LABEL", "operator");
        c
    });
    assert!(started.contains("started daemon pid"));
    for _ in 0..50 {
        if state.join("runtime/daemon.sock").exists() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    for _ in 0..100 {
        let events = fs::read_dir(state.join("queue/events"))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| fs::read_to_string(entry.path()).ok())
            .collect::<Vec<_>>()
            .join("\n");
        if events.contains("watch-root") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    fs::write(source.join("alpha.txt"), b"daemon captured\n").unwrap();
    let mut captured = false;
    for _ in 0..100 {
        if db_ref(&state, "current").is_some() {
            captured = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        captured,
        "daemon did not create a snapshot for the file event"
    );
    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(op_log.contains("file-events-batch"), "{op_log}");
    assert!(
        op_log.contains("\twatch:notify:daemon-pid-")
            || op_log.contains("\tunattributed:watch-replay\t"),
        "{op_log}"
    );
    assert!(!op_log.contains("\tunknown\t"), "{op_log}");
    assert!(!op_log.contains("operator-session"), "{op_log}");
    let daemon_op = op_log
        .lines()
        .find(|line| line.contains("file-events-batch"))
        .and_then(|line| line.split('\t').next())
        .unwrap()
        .to_string();
    let op_show = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("op")
            .arg("show")
            .arg(daemon_op);
        c
    });
    assert!(op_show.contains("session_label daemon"), "{op_show}");
    assert!(op_show.contains("process_id "), "{op_show}");
    assert!(op_show.contains("process_path "), "{op_show}");
    assert!(op_show.contains("origin_label watch:notify"), "{op_show}");
    assert!(
        op_show.contains("origin_session_id daemon-pid-"),
        "{op_show}"
    );
    assert!(op_show.contains("origin_process_id \n"), "{op_show}");
    let mut synced = false;
    for _ in 0..100 {
        if let Some(current) = db_ref(&state, "current") {
            let remote_current = if host_metadata_export_path(&remote).exists() {
                let metadata = read_remote_metadata(&remote);
                metadata["refs"]["current"].as_str().map(str::to_string)
            } else {
                None
            };
            let queue_empty = fs::read_dir(state.join("queue/uploads"))
                .map(|entries| entries.count() == 0)
                .unwrap_or(true);
            if remote_current.as_deref() == Some(current.as_str()) && queue_empty {
                synced = true;
                break;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(synced, "daemon did not sync the captured snapshot");
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("stop");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "daemon captured\n"
    );
}

#[cfg(unix)]
#[test]
fn daemon_watch_snapshot_survives_sync_retry_and_remote_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote-file");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(&remote, b"not a directory\n").unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let started = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("daemon")
            .arg("start")
            .arg("--backend")
            .arg("notify")
            .arg("--settle-ms")
            .arg("50")
            .arg("--periodic-rescan-secs")
            .arg("3600");
        c
    });
    assert!(started.contains("started daemon pid"));
    for _ in 0..50 {
        if state.join("runtime/daemon.sock").exists() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    for _ in 0..100 {
        let events = fs::read_dir(state.join("queue/events"))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| fs::read_to_string(entry.path()).ok())
            .collect::<Vec<_>>()
            .join("\n");
        if events.contains("watch-root") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    fs::write(source.join("alpha.txt"), b"daemon retry captured\n").unwrap();
    let mut captured = false;
    for _ in 0..100 {
        if db_ref(&state, "current").is_some() {
            captured = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        captured,
        "daemon did not create a snapshot for the file event"
    );
    let mut queued = Vec::new();
    for _ in 0..100 {
        queued = fs::read_dir(state.join("queue/uploads"))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| fs::read_to_string(entry.path()).ok())
            .collect::<Vec<_>>();
        if queued.iter().any(|item| item.contains("\"attempts\": 1")) {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(queued.iter().any(|item| item.contains("\"attempts\": 1")));
    assert!(queued.iter().any(|item| item.contains("\"retry_after\"")));
    fs::write(source.join("beta.txt"), b"deferred while remote is down\n").unwrap();
    let mut deferred = false;
    for _ in 0..100 {
        let events = fs::read_dir(state.join("queue/events"))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| fs::read_to_string(entry.path()).ok())
            .collect::<Vec<_>>()
            .join("\n");
        if events.contains("watch-sync-deferred") {
            deferred = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        deferred,
        "daemon did not defer auto sync during upload backoff"
    );
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("stop");
        c
    });

    fs::remove_file(&remote).unwrap();
    fs::create_dir_all(&remote).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    assert_eq!(
        fs::read_dir(state.join("queue/uploads")).unwrap().count(),
        0
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/alpha.txt")).unwrap(),
        "daemon retry captured\n"
    );
}

#[cfg(unix)]
#[test]
fn daemon_watch_large_snapshot_sync_clone_restore_preserves_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let config_path = state.join("config.toml");
    let config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("min_size = 67108864", "min_size = 1024")
        .replace("chunk_size = 8388608", "chunk_size = 4096");
    fs::write(&config_path, config).unwrap();
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source);
        c
    });
    let started = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("daemon")
            .arg("start")
            .arg("--backend")
            .arg("notify")
            .arg("--settle-ms")
            .arg("50")
            .arg("--periodic-rescan-secs")
            .arg("3600");
        c
    });
    assert!(started.contains("started daemon pid"));
    for _ in 0..50 {
        if state.join("runtime/daemon.sock").exists() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    for _ in 0..100 {
        let events = fs::read_dir(state.join("queue/events"))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| fs::read_to_string(entry.path()).ok())
            .collect::<Vec<_>>()
            .join("\n");
        if events.contains("watch-root") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let mut payload = vec![4u8; 4096];
    payload.extend(vec![5u8; 4096]);
    fs::write(source.join("payload.bin"), &payload).unwrap();
    let mut captured = false;
    for _ in 0..100 {
        if db_ref(&state, "current").is_some() {
            captured = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("daemon").arg("stop");
        c
    });
    assert!(
        captured,
        "daemon did not create a large snapshot for the file event"
    );
    let stat = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("large").arg("stat");
        c
    });
    assert!(stat.contains("large_objects 1"));
    assert!(stat.contains("chunks 2"));

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("--wait");
        c
    });
    assert_eq!(
        count_files_ending(&remote.join("large/chunks/fixed-8m"), ".chunk.enc"),
        2
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(source.join("payload.bin")).unwrap(),
        fs::read(restore.join("sample/payload.bin")).unwrap()
    );
}

#[test]
fn transactional_snapshot_runs_pre_and_post_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--snapshot-mode")
            .arg("transactional")
            .arg("--pre-snapshot")
            .arg("printf pre > pre.txt")
            .arg("--post-snapshot")
            .arg("printf post > post.txt");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert_eq!(fs::read_to_string(source.join("pre.txt")).unwrap(), "pre");
    assert_eq!(fs::read_to_string(source.join("post.txt")).unwrap(), "post");
    let restore = tmp.path().join("restore");
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/pre.txt")).unwrap(),
        "pre"
    );
}

#[test]
fn failed_snapshot_records_failed_operation_status() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--snapshot-mode")
            .arg("transactional")
            .arg("--pre-snapshot")
            .arg("exit 7");
        c
    });
    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });

    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    let failed_op = op_log
        .lines()
        .find(|line| line.contains("initial-scan"))
        .and_then(|line| line.split('\t').next())
        .unwrap()
        .to_string();
    let op_show = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("op")
            .arg("show")
            .arg(&failed_op);
        c
    });
    assert!(op_show.contains("status failed"));
    assert!(op_show.contains("snapshot failed for root sample"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let export: serde_json::Value = read_remote_metadata(&remote);
    let op = export["operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|op| op["id"] == failed_op)
        .unwrap();
    assert_eq!(op["status"], "failed");
    assert!(
        op["error"]
            .as_str()
            .unwrap()
            .contains("snapshot failed for root sample")
    );
}

#[test]
fn transactional_snapshot_can_scan_snapshot_source() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let snapshot_source = tmp.path().join("snapshot-source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("live.txt"), b"live\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let pre = format!(
        "mkdir -p '{}' && cp live.txt '{}/live.txt' && printf dump > '{}/dump.txt'",
        snapshot_source.display(),
        snapshot_source.display(),
        snapshot_source.display()
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--snapshot-mode")
            .arg("transactional")
            .arg("--snapshot-source")
            .arg(&snapshot_source)
            .arg("--pre-snapshot")
            .arg(pre)
            .arg("--post-snapshot")
            .arg("printf post > post.txt");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert_eq!(fs::read_to_string(source.join("post.txt")).unwrap(), "post");
    let restore = tmp.path().join("restore");
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/dump.txt")).unwrap(),
        "dump"
    );
    assert!(!restore.join("sample/post.txt").exists());
}

#[test]
fn transactional_snapshot_source_sync_clone_restore_preserves_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let snapshot_source = tmp.path().join("snapshot-source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("live.txt"), b"live\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("init")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    let pre = format!(
        "rm -rf '{}' && mkdir -p '{}' && cp live.txt '{}/live.txt' && printf checkpoint > '{}/checkpoint.txt'",
        snapshot_source.display(),
        snapshot_source.display(),
        snapshot_source.display(),
        snapshot_source.display()
    );
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--snapshot-mode")
            .arg("transactional")
            .arg("--snapshot-source")
            .arg(&snapshot_source)
            .arg("--pre-snapshot")
            .arg(pre)
            .arg("--post-snapshot")
            .arg("printf post > post.txt");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("clone")
            .arg("--remote")
            .arg(format!("file://{}", remote.display()));
        c
    });
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&clone)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });

    assert_eq!(
        fs::read_to_string(restore.join("sample/checkpoint.txt")).unwrap(),
        "checkpoint"
    );
    assert_eq!(
        fs::read_to_string(restore.join("sample/live.txt")).unwrap(),
        "live\n"
    );
    assert!(!restore.join("sample/post.txt").exists());
}

#[test]
fn transactional_snapshot_runs_application_plugin_phases() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let snapshot_source = tmp.path().join("plugin-source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("live.txt"), b"live\n").unwrap();

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
        c
    });
    let plugin = "if [ \"$MAJUTSU_PLUGIN_PHASE\" = pre ]; then \
         mkdir -p \"$MAJUTSU_SNAPSHOT_SOURCE\" && \
         cp live.txt \"$MAJUTSU_SNAPSHOT_SOURCE/live.txt\" && \
         printf plugin > \"$MAJUTSU_SNAPSHOT_SOURCE/plugin.txt\"; \
         else printf cleaned > plugin-cleaned.txt; fi";
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("root")
            .arg("add")
            .arg("sample")
            .arg(&source)
            .arg("--snapshot-mode")
            .arg("transactional")
            .arg("--snapshot-source")
            .arg(&snapshot_source)
            .arg("--application-plugin")
            .arg(plugin);
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    assert_eq!(
        fs::read_to_string(source.join("plugin-cleaned.txt")).unwrap(),
        "cleaned"
    );
    let restore = tmp.path().join("restore");
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read_to_string(restore.join("sample/plugin.txt")).unwrap(),
        "plugin"
    );
    assert!(!restore.join("sample/plugin-cleaned.txt").exists());
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("application-plugin-pre"));
    assert!(events.contains("application-plugin-post"));
}
