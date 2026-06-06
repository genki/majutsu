use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
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
    run({
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
    let key_output = mj()
        .arg("--home")
        .arg(&state)
        .arg("key")
        .arg("export")
        .output()
        .unwrap();
    assert!(key_output.status.success());
    let key = String::from_utf8_lossy(&key_output.stdout)
        .trim()
        .to_string();
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
    assert!(object.starts_with(b"MJENC1\n"));

    let status = mj()
        .arg("--home")
        .arg(&clone)
        .arg("clone")
        .arg("--remote")
        .arg(format!("file://{}", remote.display()))
        .env("MAJUTSU_MASTER_KEY", &key)
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
        .env("MAJUTSU_MASTER_KEY", new_key)
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
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("gc");
        c
    });
    assert!(
        state
            .join("objects/packs/normal")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
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
    assert!(
        state
            .join("objects/packs/normal")
            .read_dir()
            .unwrap()
            .filter_map(Result::ok)
            .count()
            >= 2
    );
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
        state
            .join("objects/packs/normal")
            .read_dir()
            .unwrap()
            .filter_map(Result::ok)
            .count(),
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
    assert!(policy.contains("objects/packs/normal/"));
    assert!(policy.contains("objects/large/chunks/fixed/"));
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
    assert!(manifest.matches("\"index\"").count() > 1);
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
