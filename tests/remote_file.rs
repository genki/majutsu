use std::fs;
use std::process::Command;

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
    run({
        let mut c = mj();
        c.arg("--home")
            .arg(&state)
            .arg("watch")
            .arg("--once")
            .arg("--interval-secs")
            .arg("1");
        c
    });
    let output = mj()
        .arg("--home")
        .arg(&state)
        .arg("status")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("current snap-"));
}
