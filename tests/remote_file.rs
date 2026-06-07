use chrono::{SecondsFormat, Utc};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::process::Command;
use std::thread;
use std::time::Duration;
#[cfg(unix)]
use std::time::UNIX_EPOCH;

fn mj() -> Command {
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

fn assert_canonical_cbor_zstd(path: &std::path::Path) {
    let compressed = fs::read(path).unwrap();
    let cbor = zstd::stream::decode_all(compressed.as_slice()).unwrap();
    let value: serde_cbor::Value = serde_cbor::from_slice(&cbor).unwrap();
    assert!(matches!(value, serde_cbor::Value::Map(_)));
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
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let sync_status = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync").arg("status");
        c
    });
    assert!(sync_status.contains("remote_last_synced "));
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
        c.arg("--home").arg(&state).arg("remote").arg("fsck");
        c
    });
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
    assert_canonical_cbor_zstd(&remote.join("indexes/chunk-index/shard-0000.cbor.zst.enc"));
    let host_ref_dirs = fs::read_dir(remote.join("hosts"))
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
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
        fs::read(source.join("payload.zip")).unwrap(),
        fs::read(host_restore.join("sample/payload.zip")).unwrap()
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
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let host_ref = fs::read_dir(remote.join("hosts"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join("refs/current").exists())
        .unwrap()
        .join("refs/current");
    fs::remove_file(host_ref).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("fsck");
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
        c.arg("--home").arg(&state).arg("remote").arg("fsck");
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
    fs::remove_file(remote.join("indexes/chunk-index/shard-0000.cbor.zst.enc")).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("fsck");
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
    assert!(find_file_ending(&remote.join("gc/marks"), ".json").exists());
    let host_dir = fs::read_dir(remote.join("hosts"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join("refs/current").exists())
        .unwrap();
    let canonical_snapshot = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.to_string_lossy().ends_with(".cbor.zst.enc"))
        .unwrap();
    fs::remove_file(canonical_snapshot).unwrap();

    fails({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("remote").arg("fsck");
        c
    });
}

#[test]
fn unchanged_root_reuses_previous_tree_object() {
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
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });

    let export: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("metadata/export.json")).unwrap()).unwrap();
    let snapshots = export["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 2);
    let first_manifest: serde_json::Value =
        serde_json::from_str(snapshots[0]["manifest_json"].as_str().unwrap()).unwrap();
    let second_manifest: serde_json::Value =
        serde_json::from_str(snapshots[1]["manifest_json"].as_str().unwrap()).unwrap();
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
    assert_eq!(fs::read(&restored).unwrap(), b"mode and mtime\n");
    assert_eq!(metadata.permissions().mode() & 0o777, 0o640);
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
        .arg("-d")
        .arg("@1720000000")
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
    let export: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("metadata/export.json")).unwrap()).unwrap();
    let object_key = export["blobs"][0]["object_key"].as_str().unwrap();
    assert!(!object_key.contains(&plain_oid));
    let object = fs::read(remote.join(object_key)).unwrap();
    assert!(object.starts_with(b"age-encryption.org/v1"));
    assert!(remote.join("keys/recipients.toml").exists());

    let status = mj()
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
    let rotated_export: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("metadata/export.json")).unwrap()).unwrap();
    let rotated_object_key = rotated_export["blobs"][0]["object_key"].as_str().unwrap();
    assert_ne!(object_key, rotated_object_key);
    assert!(!rotated_object_key.contains(&plain_oid));

    let status = mj()
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
}

#[test]
fn snapshot_manifest_uses_spec_payload_variants() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let state = tmp.path().join("state");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("alpha.txt"), b"alpha\n").unwrap();
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

    let tree_path = find_file_ending(&state.join("objects/trees"), "");
    let tree: serde_json::Value = serde_json::from_slice(&fs::read(tree_path).unwrap()).unwrap();
    assert_eq!(
        tree["entries"]["alpha.txt"]["payload"]["type"],
        "inline-small"
    );
    assert_eq!(
        tree["entries"]["payload.zip"]["payload"]["type"],
        "large-object"
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
    let first_at = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("log")
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
    let diff_output = mj().arg("--home").arg(&state).arg("diff").output().unwrap();
    assert!(diff_output.status.success());
    let stdout = String::from_utf8_lossy(&diff_output.stdout);
    assert!(stdout.contains("D\tsample/alpha.txt"));
    assert!(stdout.contains("A\tsample/beta.txt"));
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
        c.arg("--home").arg(&state).arg("prune");
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

    let prune = output({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("prune")
            .arg("--dry-run=false")
            .arg("--keep-daily")
            .arg("1")
            .arg("--keep-monthly")
            .arg("0");
        c
    });
    assert!(prune.contains("deleted_snapshots 1"));
    fails({
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
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("restore")
            .arg("apply")
            .arg("--snapshot")
            .arg(&second)
            .arg("--to")
            .arg(&restore);
        c
    });
    assert_eq!(
        fs::read(restore.join("sample/alpha.txt")).unwrap(),
        b"two\n"
    );
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
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let host_dir = fs::read_dir(remote.join("hosts"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join("metadata/export.json").exists())
        .unwrap();
    let before = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
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
    let after = fs::read_dir(host_dir.join("snapshots"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
        .count();
    assert_eq!(after, 1);
    assert!(find_file_ending(&remote.join("gc/tombstones"), ".json").exists());
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
        .find(|line| line.contains("manual-snapshot"))
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
    assert!(op_show.contains("kind manual-snapshot"));
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
fn lifecycle_policy_uses_tiering_config_rules() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");

    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("init");
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
name = "keep-hosts-hot"
prefix = "hosts/"
storage = "standard"

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
    assert!(!s3_policy.contains("\"Prefix\": \"hosts/\""));

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
    assert!(!gcs_policy.contains("\"hosts/\""));
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
        c.arg("--home").arg(&state).arg("sync");
        c
    });
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
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let alpha_oid = blake3::hash(b"alpha\n").to_hex().to_string();
    let object_key = format!("objects/blobs/{}/{}", &alpha_oid[..2], &alpha_oid[2..]);
    fs::remove_file(state.join(&object_key)).unwrap();
    fs::remove_file(remote.join(&object_key)).unwrap();

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
        c
    });
    assert!(fs::metadata(&oplog).unwrap().len() > root_add_len);
    let op_log = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    let snapshot_op = op_log
        .lines()
        .find(|line| line.contains("manual-snapshot"))
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
    assert!(op_show.contains("status done"));
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("sync");
        c
    });
    let export: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("metadata/export.json")).unwrap()).unwrap();
    let op = export["operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|op| op["id"] == snapshot_op)
        .unwrap();
    assert!(op["parent_op"].as_str().unwrap().starts_with("op-"));
    assert_eq!(op["status"], "done");
    assert!(op["actor"].as_str().unwrap().contains('@'));
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
    let export: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("metadata/export.json")).unwrap()).unwrap();
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
    assert!(roots.contains("cfg\tactive\tcfg\t"));
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
    assert!(roots.contains("cfg\tpaused\tcfg\t"));
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
    fs::write(source.join("payload.log"), vec![b'A'; 64 * 1024]).unwrap();

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
        fs::read(source.join("payload.log")).unwrap(),
        fs::read(restore.join("sample/payload.log")).unwrap()
    );
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
        c.arg("--home").arg(&state).arg("remote").arg("fsck");
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
        c.arg("--home").arg(&state).arg("unmount").arg(&lazy_view);
        c
    });
    assert!(unmounted.contains("unmounted"));
    assert!(!lazy_view.exists());
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
    assert!(stdout.contains("sample\tmissing"));
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
    assert!(roots.contains("sample\tdeleted"));
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
fn require_mount_root_is_skipped_as_unmounted_without_mass_deletion() {
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
            .arg(&source)
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
    assert!(roots.contains("sample\tunmounted"));
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(ops.contains("root-unmounted"));
}

#[cfg(unix)]
#[test]
fn permission_denied_root_is_skipped_without_mass_deletion() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let blocked = source.join("blocked");
    let state = tmp.path().join("state");
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
    let original_mode = fs::metadata(&blocked).unwrap().permissions().mode();
    let mut perms = fs::metadata(&blocked).unwrap().permissions();
    perms.set_mode(0);
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
    assert!(roots.contains("sample\tpermission-denied"));
    let ops = output({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("op").arg("log");
        c
    });
    assert!(ops.contains("root-permission-denied"));
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
    let output = mj()
        .arg("--home")
        .arg(&state)
        .arg("status")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("current snap-"));
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("watch-settle"));
    assert!(events.contains("settle_ms=50"));
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
        .replace("mode = \"default\"", "mode = \"strict\"")
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
    assert!(config.contains("backend = \"inotify\""));
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
    let output = mj()
        .arg("--home")
        .arg(&state)
        .arg("status")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("current snap-"));
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("backend=inotify"));
    assert!(events.contains("fs-event"));
}

#[cfg(target_os = "linux")]
#[test]
fn linux_watch_defaults_to_inotify_backend() {
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
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("backend=inotify"));
    assert!(events.contains("fs-event"));
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
    let events = fs::read_dir(state.join("queue/events"))
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(events.contains("periodic-rescan"));
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
    assert!(status.contains("ipc ok"));
    assert!(status.contains("roots 1"));
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
        .find(|line| line.contains("manual-snapshot"))
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
    let export: serde_json::Value =
        serde_json::from_slice(&fs::read(remote.join("metadata/export.json")).unwrap()).unwrap();
    let op = export["operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|op| op["id"] == failed_op)
        .unwrap();
    assert_eq!(op["status"], "failed");
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
