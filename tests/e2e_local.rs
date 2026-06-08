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
