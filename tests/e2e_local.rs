use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn mj_bin() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_mj")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/mj"))
}

fn run_mj<I, S>(home: &Path, args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(mj_bin())
        .arg("--home")
        .arg(home)
        .args(args)
        .output()
        .expect("mj の起動に失敗しました")
}

fn assert_success(output: Output, context: &str) {
    if !output.status.success() {
        panic!(
            "{context} が失敗しました\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn assert_file(path: &Path, expected: &[u8]) {
    let actual =
        fs::read(path).unwrap_or_else(|err| panic!("{} を読めません: {err}", path.display()));
    assert_eq!(
        actual,
        expected,
        "{} の内容が想定と異なります",
        path.display()
    );
}

#[test]
fn multi_root_snapshot_sync_clone_restore_file_remote() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let recovered_home = temp.path().join("recovered-home");
    let remote = temp.path().join("remote");
    let docs = temp.path().join("docs");
    let config = temp.path().join("config");
    let restore = temp.path().join("restore");

    fs::create_dir_all(&docs).unwrap();
    fs::create_dir_all(&config).unwrap();
    fs::write(docs.join("note.txt"), b"hello majutsu\n").unwrap();
    fs::create_dir_all(docs.join("nested")).unwrap();
    fs::write(docs.join("nested/todo.md"), b"- snapshot\n- sync\n").unwrap();
    fs::write(config.join("app.toml"), b"enabled = true\n").unwrap();

    assert_success(
        run_mj(
            &home,
            [
                "init",
                "--remote",
                &format!("file://{}", remote.display()),
                "--host-name",
                "e2e-host",
            ],
        ),
        "init",
    );
    assert_success(
        run_mj(&home, ["root", "add", "docs", docs.to_str().unwrap()]),
        "root add docs",
    );
    assert_success(
        run_mj(&home, ["root", "add", "config", config.to_str().unwrap()]),
        "root add config",
    );
    assert_success(
        run_mj(&home, ["snapshot", "--message", "e2e baseline"]),
        "snapshot",
    );
    assert_success(run_mj(&home, ["fsck"]), "fsck");
    assert_success(run_mj(&home, ["sync"]), "sync");
    assert_success(run_mj(&home, ["remote", "fsck"]), "remote fsck");

    assert_success(
        run_mj(
            &recovered_home,
            ["clone", "--remote", &format!("file://{}", remote.display())],
        ),
        "clone",
    );
    assert_success(
        run_mj(
            &recovered_home,
            ["restore", "apply", "--to", restore.to_str().unwrap()],
        ),
        "restore apply",
    );

    assert_file(&restore.join("docs/note.txt"), b"hello majutsu\n");
    assert_file(
        &restore.join("docs/nested/todo.md"),
        b"- snapshot\n- sync\n",
    );
    assert_file(&restore.join("config/app.toml"), b"enabled = true\n");
}

#[test]
fn large_object_roundtrip_uses_manifest_and_chunks() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let recovered_home = temp.path().join("recovered-home");
    let remote = temp.path().join("remote");
    let root = temp.path().join("data");
    let restore = temp.path().join("restore");
    fs::create_dir_all(&root).unwrap();

    let payload = (0..16_384u32)
        .flat_map(|value| value.to_le_bytes())
        .collect::<Vec<u8>>();
    fs::write(root.join("dataset.bin"), &payload).unwrap();

    assert_success(
        run_mj(
            &home,
            [
                "init",
                "--remote",
                &format!("file://{}", remote.display()),
                "--host-name",
                "large-e2e-host",
            ],
        ),
        "init",
    );
    assert_success(
        run_mj(
            &home,
            [
                "root",
                "add",
                "data",
                root.to_str().unwrap(),
                "--large-min-size",
                "1024",
                "--large-binary-min-size",
                "512",
                "--large-chunk-size",
                "512",
                "--large-chunking",
                "fixed",
            ],
        ),
        "root add data",
    );
    assert_success(
        run_mj(&home, ["snapshot", "--message", "large e2e"]),
        "snapshot",
    );
    assert_success(run_mj(&home, ["large", "verify"]), "large verify");
    assert_success(run_mj(&home, ["sync"]), "sync");

    assert_success(
        run_mj(
            &recovered_home,
            ["clone", "--remote", &format!("file://{}", remote.display())],
        ),
        "clone",
    );
    assert_success(
        run_mj(
            &recovered_home,
            ["restore", "apply", "--to", restore.to_str().unwrap()],
        ),
        "restore apply",
    );

    assert_file(&restore.join("data/dataset.bin"), &payload);
}

#[test]
fn lifecycle_policy_and_package_facing_commands_are_renderable() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    assert_success(
        run_mj(&home, ["init", "--host-name", "policy-e2e-host"]),
        "init",
    );
    assert_success(
        run_mj(&home, ["lifecycle", "policy", "--provider", "s3"]),
        "s3 lifecycle policy",
    );
    assert_success(
        run_mj(&home, ["daemon", "service", "--provider", "systemd"]),
        "daemon systemd service render",
    );
}

fn run_mj_with_env<I, S>(home: &Path, args: I, envs: &[(&str, String)]) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(mj_bin());
    command.arg("--home").arg(home).args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().expect("mj の起動に失敗しました")
}

fn successful_stdout(output: Output, context: &str) -> String {
    if !output.status.success() {
        panic!(
            "{context} が失敗しました\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn encrypted_state_disaster_recovery_file_remote() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let recovered_home = temp.path().join("recovered-home");
    let remote = temp.path().join("remote");
    let root = temp.path().join("secrets");
    let restore = temp.path().join("restore");

    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("secret.txt"), b"encrypted disaster recovery\n").unwrap();

    assert_success(
        run_mj(
            &home,
            [
                "init",
                "--encrypt",
                "--remote",
                &format!("file://{}", remote.display()),
                "--host-name",
                "encrypted-e2e-host",
            ],
        ),
        "encrypted init",
    );
    assert_success(
        run_mj(&home, ["root", "add", "secrets", root.to_str().unwrap()]),
        "root add secrets",
    );
    assert_success(
        run_mj(&home, ["snapshot", "--message", "encrypted e2e"]),
        "encrypted snapshot",
    );
    assert_success(run_mj(&home, ["sync"]), "encrypted sync");

    let exported_key = successful_stdout(run_mj(&home, ["key", "export"]), "key export")
        .trim()
        .to_string();
    assert_eq!(exported_key.len(), 64, "master key should be 64 hex chars");

    assert_success(
        run_mj_with_env(
            &recovered_home,
            ["clone", "--remote", &format!("file://{}", remote.display())],
            &[("MAJUTSU_MASTER_KEY", exported_key.clone())],
        ),
        "encrypted clone",
    );
    assert_success(
        run_mj(&recovered_home, ["fsck"]),
        "encrypted recovered fsck",
    );
    assert_success(
        run_mj(
            &recovered_home,
            ["restore", "apply", "--to", restore.to_str().unwrap()],
        ),
        "encrypted restore apply",
    );

    assert_file(
        &restore.join("secrets/secret.txt"),
        b"encrypted disaster recovery\n",
    );
}

#[test]
fn prune_dry_run_and_gc_preserve_current_restore() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let remote = temp.path().join("remote");
    let root = temp.path().join("docs");
    let restore = temp.path().join("restore");

    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.txt"), b"v1\n").unwrap();

    assert_success(
        run_mj(
            &home,
            [
                "init",
                "--remote",
                &format!("file://{}", remote.display()),
                "--host-name",
                "prune-gc-e2e-host",
            ],
        ),
        "init",
    );
    assert_success(
        run_mj(&home, ["root", "add", "docs", root.to_str().unwrap()]),
        "root add docs",
    );
    assert_success(
        run_mj(&home, ["snapshot", "--message", "v1"]),
        "snapshot v1",
    );

    fs::write(root.join("note.txt"), b"v2\n").unwrap();
    assert_success(
        run_mj(&home, ["snapshot", "--message", "v2"]),
        "snapshot v2",
    );

    assert_success(run_mj(&home, ["prune", "--dry-run"]), "prune dry-run");
    assert_success(run_mj(&home, ["gc"]), "gc");
    assert_success(run_mj(&home, ["fsck"]), "fsck after gc");
    assert_success(run_mj(&home, ["sync"]), "sync after gc");

    assert_success(
        run_mj(
            &home,
            ["restore", "apply", "--to", restore.to_str().unwrap()],
        ),
        "restore after gc",
    );
    assert_file(&restore.join("docs/note.txt"), b"v2\n");
}

#[cfg(unix)]
#[test]
fn daemon_status_and_metrics_smoke() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let root = temp.path().join("watched");

    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("file.txt"), b"daemon metrics\n").unwrap();

    assert_success(
        run_mj(&home, ["init", "--host-name", "daemon-e2e-host"]),
        "daemon init",
    );
    assert_success(
        run_mj(&home, ["root", "add", "watched", root.to_str().unwrap()]),
        "daemon root add",
    );

    assert_success(
        run_mj(
            &home,
            [
                "daemon",
                "start",
                "--backend",
                "poll",
                "--interval-secs",
                "60",
                "--periodic-rescan-secs",
                "0",
            ],
        ),
        "daemon start",
    );

    let mut status_ok = false;
    for _ in 0..30 {
        let status = run_mj(&home, ["daemon", "status"]);
        if status.status.success() {
            let text = String::from_utf8_lossy(&status.stdout);
            if text.contains("running pid") && text.contains("roots 1") {
                status_ok = true;
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    assert!(status_ok, "daemon status did not become ready");

    let metrics = successful_stdout(run_mj(&home, ["daemon", "metrics"]), "daemon metrics");
    assert!(metrics.contains("majutsu_daemon_up 1"), "{metrics}");
    assert!(
        metrics.contains("majutsu_daemon_roots 1"),
        "metrics should expose root count: {metrics}"
    );

    assert_success(run_mj(&home, ["daemon", "stop"]), "daemon stop");
}

#[test]
fn exclude_child_glob_hides_root_directory_entry() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let repo = temp.path().join("repo");
    let restore = temp.path().join("restore");
    fs::create_dir_all(repo.join(".git/objects")).unwrap();
    fs::write(repo.join(".git/config"), b"secret git config\n").unwrap();
    fs::write(repo.join("src.txt"), b"tracked\n").unwrap();

    assert_success(
        run_mj(&home, ["init", "--host-name", "exclude-e2e"]),
        "init",
    );
    assert_success(
        run_mj(
            &home,
            [
                "root",
                "add",
                "repo",
                repo.to_str().unwrap(),
                "--exclude",
                "**/.git/**",
            ],
        ),
        "root add repo",
    );
    assert_success(
        run_mj(&home, ["snapshot", "--message", "exclude e2e"]),
        "snapshot",
    );
    assert_success(
        run_mj(
            &home,
            ["restore", "apply", "--to", restore.to_str().unwrap()],
        ),
        "restore apply",
    );
    assert_file(&restore.join("repo/src.txt"), b"tracked\n");
    assert!(
        !restore.join("repo/.git").exists(),
        ".git directory entry must be excluded"
    );
}

#[test]
fn remote_fsck_default_is_quick_and_deep_is_available() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let remote = temp.path().join("remote");
    let root = temp.path().join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("file.txt"), b"remote fsck\n").unwrap();

    assert_success(
        run_mj(
            &home,
            [
                "init",
                "--remote",
                &format!("file://{}", remote.display()),
                "--host-name",
                "fsck-e2e",
            ],
        ),
        "init",
    );
    assert_success(
        run_mj(&home, ["root", "add", "root", root.to_str().unwrap()]),
        "root add",
    );
    assert_success(run_mj(&home, ["snapshot", "--message", "fsck"]), "snapshot");
    assert_success(run_mj(&home, ["sync"]), "sync");
    assert_success(run_mj(&home, ["remote", "fsck"]), "remote fsck quick");
    assert_success(
        run_mj(&home, ["remote", "fsck", "--deep"]),
        "remote fsck deep",
    );
}
