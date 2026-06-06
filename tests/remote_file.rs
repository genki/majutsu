use std::fs;
use std::process::Command;
use std::thread;
use std::time::Duration;

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
    let restore = tmp.path().join("restore");
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
    assert!(remote.join("objects/trees").exists());
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
}

#[test]
fn encrypted_file_remote_clone_restores_with_exported_key() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let remote = tmp.path().join("remote");
    let state = tmp.path().join("state");
    let clone = tmp.path().join("clone");
    let restore = tmp.path().join("restore");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("secret.txt"), b"secret\n").unwrap();

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
    let object = fs::read(
        fs::read_dir(remote.join("objects/blobs"))
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
    fs::write(source.join("alpha.txt"), b"changed\n").unwrap();
    fs::write(source.join("beta.txt"), b"beta\n").unwrap();
    fs::remove_file(source.join("alpha.txt")).unwrap();
    run({
        let mut c = mj();
        c.arg("--home").arg(&state).arg("snapshot");
        c
    });
    let output = mj().arg("--home").arg(&state).arg("diff").output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("D\tsample/alpha.txt"));
    assert!(stdout.contains("A\tsample/beta.txt"));
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
