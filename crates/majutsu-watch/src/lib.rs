use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchBackend {
    Native,
    Inotify,
    Poll,
}

impl WatchBackend {
    pub fn normalize(value: &str) -> Result<Self, String> {
        match value {
            "notify" | "native" => Ok(Self::Native),
            "poll" => Ok(Self::Poll),
            "inotify" => {
                if cfg!(target_os = "linux") {
                    Ok(Self::Inotify)
                } else {
                    Err("inotify backend is only available on Linux".into())
                }
            }
            other => Err(format!("unsupported watch backend: {other}")),
        }
    }

    pub fn default_native() -> Self {
        if cfg!(target_os = "linux") {
            Self::Inotify
        } else {
            Self::Native
        }
    }

    pub fn as_cli(&self) -> &'static str {
        match self {
            Self::Native => "notify",
            Self::Inotify => "inotify",
            Self::Poll => "poll",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchMode {
    Default,
    Strict,
    Transactional,
}

impl WatchMode {
    pub fn normalize(value: &str) -> Result<Self, String> {
        match value {
            "default" => Ok(Self::Default),
            "strict" => Ok(Self::Strict),
            "transactional" => Ok(Self::Transactional),
            _ => Err("watch mode must be default, strict, or transactional".into()),
        }
    }

    pub fn as_cli(&self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Strict => "strict",
            Self::Transactional => "transactional",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchPolicy {
    pub backend: WatchBackend,
    pub debounce: Duration,
    pub settle: Duration,
    pub periodic_rescan: Option<Duration>,
}

impl WatchPolicy {
    pub fn native_default() -> Self {
        Self {
            backend: WatchBackend::default_native(),
            debounce: default_debounce(),
            settle: default_settle(),
            periodic_rescan: Some(default_periodic_rescan()),
        }
    }
}

pub fn default_backend() -> &'static str {
    WatchBackend::default_native().as_cli()
}

pub fn default_mode() -> &'static str {
    WatchMode::Default.as_cli()
}

pub fn default_debounce() -> Duration {
    Duration::from_millis(1500)
}

pub fn default_settle() -> Duration {
    Duration::from_millis(500)
}

pub fn default_periodic_rescan() -> Duration {
    Duration::from_secs(3600)
}

pub fn default_poll_interval() -> Duration {
    Duration::from_secs(60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_native_backend_aliases() {
        assert_eq!(
            WatchBackend::normalize("native").unwrap(),
            WatchBackend::Native
        );
        assert_eq!(
            WatchBackend::normalize("notify").unwrap(),
            WatchBackend::Native
        );
        assert_eq!(WatchBackend::normalize("poll").unwrap(), WatchBackend::Poll);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_defaults_to_inotify_backend() {
        assert_eq!(WatchBackend::default_native(), WatchBackend::Inotify);
        assert_eq!(default_backend(), "inotify");
        assert_eq!(
            WatchBackend::normalize("inotify").unwrap(),
            WatchBackend::Inotify
        );
    }

    #[test]
    fn rejects_unknown_backend_and_mode() {
        assert!(WatchBackend::normalize("scan").is_err());
        assert!(WatchMode::normalize("loose").is_err());
    }

    #[test]
    fn default_policy_matches_spec_timing() {
        let policy = WatchPolicy::native_default();
        assert_eq!(policy.debounce, Duration::from_millis(1500));
        assert_eq!(policy.settle, Duration::from_millis(500));
        assert_eq!(policy.periodic_rescan, Some(Duration::from_secs(3600)));
    }
}
