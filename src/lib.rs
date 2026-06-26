//! Library documentation entry point for the `majutsu` package.
//!
//! The primary public interface of this package is the `mj` command-line
//! binary. This library target exists so package documentation can be built by
//! docs.rs and other Cargo documentation tooling.

/// Package version including the majutsu build number.
pub const VERSION: &str = env!("MAJUTSU_VERSION");

/// Numeric majutsu build number embedded at compile time.
pub const BUILD_NUMBER: &str = env!("MAJUTSU_BUILD_NUMBER");

/// Git commit used for the local build, or `unknown` when built from a source package.
pub const GIT_COMMIT: &str = match option_env!("MAJUTSU_GIT_COMMIT") {
    Some(value) => value,
    None => "unknown",
};
