use std::fs;

fn main() {
    println!("cargo:rerun-if-changed=BUILD_NUMBER");
    let build_number = fs::read_to_string("BUILD_NUMBER")
        .unwrap_or_else(|_| "0".to_string())
        .trim()
        .to_string();
    println!("cargo:rustc-env=MAJUTSU_BUILD_NUMBER={build_number}");
    println!(
        "cargo:rustc-env=MAJUTSU_VERSION={}+build.{build_number}",
        std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string())
    );
}
