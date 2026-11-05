//! 再取得の失敗原因だけを保持し、接続先・認証値をエラー表示へ渡さない。
use std::fmt;

#[derive(Debug)]
pub(crate) struct ObjectFetchFailure {
    pub(crate) kind: &'static str,
    key: String,
}

impl fmt::Display for ObjectFetchFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[mj-object-fetch:{}] key={}", self.kind, self.key)
    }
}
impl std::error::Error for ObjectFetchFailure {}

pub(crate) fn failure(key: &str, kind: &'static str) -> anyhow::Error {
    ObjectFetchFailure {
        kind,
        key: key.into(),
    }
    .into()
}

pub(crate) fn remote_failure(key: &str, error: anyhow::Error) -> anyhow::Error {
    let transport = error.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|e| e.is_connect() || e.is_timeout() || e.is_body())
            || cause.downcast_ref::<std::io::Error>().is_some_and(|e| {
                matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::NotConnected
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::NetworkUnreachable
                        | std::io::ErrorKind::HostUnreachable
                )
            })
    });
    failure(key, if transport { "transport" } else { "remote" })
}

pub(crate) fn code(detail: &str) -> Option<&'static str> {
    ["transport", "missing", "decode", "config", "remote"]
        .into_iter()
        .find(|kind| detail.contains(&format!("[mj-object-fetch:{kind}]")))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn errors_keep_category_without_connection_secrets() {
        let error = anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "https://user:private-fixture@example.invalid/?token=private-fixture",
        ));
        let safe = remote_failure("objects/trees/aa/bb", error);
        assert_eq!(code(&format!("{safe:#}")), Some("transport"));
        assert!(!format!("{safe:#}").contains("private-fixture"));
        assert_eq!(
            code(&failure("objects/trees/aa/bb", "decode").to_string()),
            Some("decode")
        );
        assert!(code("ordinary local error").is_none());
    }
}
