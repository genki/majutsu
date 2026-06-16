use std::fs;

fn main() {
    println!("cargo:rerun-if-changed=BUILD_NUMBER");
    println!("cargo:rerun-if-env-changed=MAJUTSU_DEV_BUILD");
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
    println!("cargo:rustc-env=MAJUTSU_BUILD_NUMBER={build_number}");
    println!("cargo:rustc-env=MAJUTSU_VERSION={version}");
}
