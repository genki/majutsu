#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonState {
    Running { pid: u32 },
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonStatus {
    pub state: DaemonState,
    pub socket_path: String,
}
