use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchBackend {
    Native,
    Inotify,
    Poll,
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
            backend: if cfg!(target_os = "linux") {
                WatchBackend::Inotify
            } else {
                WatchBackend::Native
            },
            debounce: Duration::from_millis(1500),
            settle: Duration::from_millis(500),
            periodic_rescan: Some(Duration::from_secs(3600)),
        }
    }
}
