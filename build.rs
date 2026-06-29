use std::fs;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=BUILD_NUMBER");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/packed-refs");
    if let Ok(head) = fs::read_to_string(".git/HEAD")
        && let Some(reference) = head.trim().strip_prefix("ref: ")
    {
        println!("cargo:rerun-if-changed=.git/{reference}");
    }
    println!("cargo:rerun-if-env-changed=MAJUTSU_DEV_BUILD");
    println!("cargo:rerun-if-env-changed=MAJUTSU_GIT_COMMIT");
    let build_number = fs::read_to_string("BUILD_NUMBER")
        .unwrap_or_else(|_| "0".to_string())
        .trim()
        .to_string();
    let package_version =
        std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string());
    let version = if std::env::var("MAJUTSU_DEV_BUILD").is_ok_and(|value| value == "1") {
        format!("{package_version}+build.{build_number}")
    } else {
        package_version
    };
    let git_commit = std::env::var("MAJUTSU_GIT_COMMIT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(git_head_commit)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=MAJUTSU_BUILD_NUMBER={build_number}");
    println!("cargo:rustc-env=MAJUTSU_VERSION={version}");
    println!("cargo:rustc-env=MAJUTSU_GIT_COMMIT={git_commit}");
}

fn git_head_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?;
    let commit = commit.trim();
    if commit.is_empty() {
        None
    } else {
        Some(commit.to_string())
    }
}
