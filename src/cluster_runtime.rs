use crate::cli::{ClusterCommand, ClusterServeArgs};
use crate::config::{Config, Paths, read_config};
use crate::remote_store::{
    ClusterRemote, RemoteObjectStat, RemoteStore, open_remote_with_upload_policy,
    remote_traffic_metrics,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const CLUSTER_PROTOCOL_VERSION: u16 = 2;
const DEFAULT_CLUSTER_LISTEN: &str = "0.0.0.0:4581";
const DEFAULT_CLUSTER_REQUEST_TIMEOUT_SECS: u64 = 300;
const DEFAULT_CLUSTER_MAX_FRAME_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_CLUSTER_MAX_INFLIGHT_FRAME_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_CLUSTER_MAX_CLIENTS: usize = 32;
const DEFAULT_CLUSTER_MAX_BATCH_OBJECTS: usize = 1024;
const DEFAULT_CLUSTER_MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CLUSTER_HEARTBEAT_INTERVAL_SECS: u64 = 60;
const DEFAULT_CLUSTER_LIST_CACHE_TTL_SECS: u64 = 3600;
const DEFAULT_BACKEND_FLUSH_INTERVAL_SECS: u64 = 3600;
const DEFAULT_BACKEND_POLL_INTERVAL_SECS: u64 = 30;
const DEFAULT_BACKEND_FLUSH_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_BACKEND_FLUSH_MAX_OBJECTS: usize = 1024;
const DEFAULT_BACKEND_MAX_CONCURRENCY: usize = 4;
const DEFAULT_BACKEND_DEDUPE_LIST_MIN_OBJECTS: usize = 32;
const BACKEND_QUEUE_VERSION: u32 = 1;
const LOCAL_GC_VERSION: u32 = 1;
const DEFAULT_LOCAL_DELETE_GRACE_SECS: u64 = 7 * 24 * 60 * 60;

#[derive(Debug, Serialize, Deserialize)]
struct ClusterRequestFrame {
    protocol_version: u16,
    token: String,
    host_id: String,
    operation: ClusterOperation,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ClusterOperation {
    Capabilities,
    Heartbeat,
    Put {
        key: String,
        bytes: Vec<u8>,
        conditional: bool,
    },
    BatchPut {
        items: Vec<ClusterPutItem>,
    },
    Get {
        key: String,
    },
    GetOptional {
        key: String,
    },
    GetRange {
        key: String,
        start: u64,
        len: u64,
    },
    Exists {
        key: String,
    },
    BackendCommitted {
        keys: Vec<String>,
    },
    List {
        prefix: String,
    },
    ListCommonPrefixes {
        prefix: String,
    },
    ListWithSizes {
        prefix: String,
    },
    ListMasterLocalWithSizes {
        prefix: String,
    },
    Delete {
        key: String,
    },
    RestoreArchive {
        key: String,
        days: u32,
        tier: String,
    },
    ApplyLifecycle {
        policy: serde_json::Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ClusterCapabilities {
    pub(crate) protocol_version: u16,
    pub(crate) max_request_frame_bytes: usize,
    pub(crate) max_response_frame_bytes: usize,
    pub(crate) max_batch_objects: usize,
    pub(crate) max_batch_payload_bytes: usize,
    pub(crate) write_prefix: String,
    pub(crate) supported_operations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClusterPutItem {
    pub(crate) key: String,
    pub(crate) bytes: Vec<u8>,
    pub(crate) conditional: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ClusterPutResult {
    pub(crate) master_durable: bool,
    pub(crate) newly_stored: bool,
    pub(crate) stored_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum BackendQueueOperation {
    Put {
        digest: String,
        size: u64,
        conditional: bool,
    },
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackendQueueItem {
    version: u32,
    generation: String,
    key: String,
    source_host: String,
    operation: BackendQueueOperation,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Clone, Serialize)]
struct BackendQueueStats {
    objects: usize,
    bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    oldest_created_at: Option<DateTime<Utc>>,
    retrying: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalGcItem {
    version: u32,
    generation: String,
    key: String,
    source_host: String,
    digest: String,
    size: u64,
    deleted_from_backend_at: DateTime<Utc>,
    delete_after: DateTime<Utc>,
}

// payloadを変更する前に転送意図を保存する。ACK前の停止でも再生できる。
#[derive(Debug, Serialize, Deserialize)]
struct MasterWriteIntent {
    version: u32,
    source_host: String,
    item: ClusterPutItem,
}

#[derive(Debug, Serialize, Deserialize)]
struct BackendPutReceipt {
    key: String,
    digest: String,
}

struct ClientLimiter {
    active: AtomicUsize,
    max: usize,
}

impl ClientLimiter {
    fn new(max: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max: max.max(1),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Option<ClientPermit> {
        let mut active = self.active.load(Ordering::Relaxed);
        loop {
            if active >= self.max {
                return None;
            }
            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(ClientPermit {
                        limiter: Arc::clone(self),
                    });
                }
                Err(current) => active = current,
            }
        }
    }
}

struct ClientPermit {
    limiter: Arc<ClientLimiter>,
}

impl Drop for ClientPermit {
    fn drop(&mut self) {
        self.limiter.active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct FrameBudget {
    in_flight: AtomicUsize,
    limit: usize,
}

impl FrameBudget {
    fn new(limit: usize) -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            limit: limit.max(1024 * 1024),
        }
    }

    fn try_reserve(self: &Arc<Self>, bytes: usize) -> Result<FrameBudgetPermit> {
        let mut in_flight = self.in_flight.load(Ordering::Relaxed);
        loop {
            let Some(next) = in_flight.checked_add(bytes) else {
                bail!("cluster受信frameの合計サイズがoverflowしました");
            };
            if next > self.limit {
                bail!(
                    "cluster受信frameの合計サイズが上限を超えました: {} bytes (limit {})",
                    next,
                    self.limit
                );
            }
            match self.in_flight.compare_exchange_weak(
                in_flight,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(FrameBudgetPermit {
                        budget: Arc::clone(self),
                        bytes,
                    });
                }
                Err(current) => in_flight = current,
            }
        }
    }
}

struct FrameBudgetPermit {
    budget: Arc<FrameBudget>,
    bytes: usize,
}

impl Drop for FrameBudgetPermit {
    fn drop(&mut self) {
        self.budget
            .in_flight
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[derive(Debug, Default, Clone, Serialize)]
struct LocalGcStats {
    objects: usize,
    bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    oldest_deleted_from_backend_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_delete_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
struct BackendRuntimeState {
    last_success_at: Option<DateTime<Utc>>,
    last_error_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MasterMetrics {
    #[serde(default)]
    requests_total: u64,
    #[serde(default)]
    rejected_requests_total: u64,
    #[serde(default)]
    backend_puts_total: u64,
    #[serde(default)]
    backend_puts_skipped_existing_total: u64,
    #[serde(default)]
    backend_deletes_total: u64,
    #[serde(default)]
    backend_failures_total: u64,
    #[serde(default)]
    store_quota_rejections_total: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_store_quota_rejection_at: Option<DateTime<Utc>>,
    #[serde(default)]
    hosts: BTreeMap<String, HostMetrics>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct HostMetrics {
    #[serde(default)]
    requests: u64,
    #[serde(default)]
    received_batches: u64,
    #[serde(default)]
    received_objects: u64,
    #[serde(default)]
    received_bytes: u64,
    #[serde(default)]
    newly_stored_objects: u64,
    #[serde(default)]
    duplicate_objects: u64,
    #[serde(default)]
    queue_reused_objects: u64,
    #[serde(default)]
    same_key_churn: u64,
    #[serde(default)]
    delete_requests: u64,
    #[serde(default)]
    backend_puts: u64,
    #[serde(default)]
    backend_puts_skipped_existing: u64,
    #[serde(default)]
    backend_deletes: u64,
    #[serde(default)]
    backend_failures: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_seen_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_heartbeat_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_backend_success_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_backend_error_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct BackendApiMetrics {
    put_requests: u64,
    get_requests: u64,
    head_requests: u64,
    list_requests: u64,
    post_requests: u64,
    delete_requests: u64,
    upload_bytes: u64,
    download_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterNoticeMarker {
    issue_codes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sent_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_attempt_at: Option<DateTime<Utc>>,
    #[serde(default)]
    consecutive_failures: u32,
}

#[derive(Debug, Serialize)]
struct MasterStatus<'a> {
    version: u32,
    observed_at: DateTime<Utc>,
    process_id: u32,
    catalog_errors: Vec<String>,
    role: &'a str,
    protocol_version: u16,
    authentication_mode: &'a str,
    authorized_hosts: Vec<String>,
    store_dir: String,
    store_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    store_max_bytes: Option<u64>,
    backend_max_concurrency: usize,
    max_clients: usize,
    max_inflight_frame_bytes: usize,
    backend_batch_delete_enabled: bool,
    local_delete_grace_secs: u64,
    flush_requested: bool,
    queue: Option<BackendQueueStats>,
    retained_deleted: Option<LocalGcStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_backend_success_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_backend_error_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_backend_error: Option<String>,
    metrics: MasterMetrics,
    backend_api_since_start: BackendApiMetrics,
}

#[derive(Debug, Serialize, Deserialize)]
enum ClusterValue {
    Empty,
    Bool(bool),
    Bytes(Vec<u8>),
    OptionalBytes(Option<Vec<u8>>),
    Strings(Vec<String>),
    Objects(Vec<RemoteObjectStat>),
    BatchPut(Vec<ClusterPutResult>),
    Capabilities(ClusterCapabilities),
}

#[derive(Clone)]
enum CachedList {
    Strings(Vec<String>),
    Objects(Vec<RemoteObjectStat>),
}

#[derive(Debug, Serialize, Deserialize)]
enum ClusterResponse {
    Ok(ClusterValue),
    Error { code: String, message: String },
}

#[derive(Debug)]
enum ClusterAuthentication {
    LegacyShared {
        token_digest: [u8; 32],
    },
    HostTokens {
        token_digests: BTreeMap<String, [u8; 32]>,
    },
}

impl ClusterAuthentication {
    fn mode(&self) -> &'static str {
        match self {
            Self::LegacyShared { .. } => "legacy-shared-token",
            Self::HostTokens { .. } => "host-token-directory",
        }
    }

    fn authorized_hosts(&self) -> Vec<String> {
        match self {
            Self::LegacyShared { .. } => Vec::new(),
            Self::HostTokens { token_digests } => token_digests.keys().cloned().collect(),
        }
    }

    fn authenticates(&self, host_id: &str, token: &str) -> bool {
        let incoming = token_digest(token);
        let expected = match self {
            Self::LegacyShared { token_digest } => Some(token_digest),
            Self::HostTokens { token_digests } => token_digests.get(host_id),
        };
        expected.is_some_and(|expected| constant_time_digest_eq(expected, &incoming))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackendPriority {
    Foreground,
    Background,
}

#[derive(Debug, Default)]
struct BackendGateState {
    active: usize,
    foreground_waiters: usize,
}

#[derive(Debug)]
struct BackendGate {
    state: Mutex<BackendGateState>,
    wake: Condvar,
    max_concurrency: usize,
}

impl BackendGate {
    fn new(max_concurrency: usize) -> Self {
        Self {
            state: Mutex::new(BackendGateState::default()),
            wake: Condvar::new(),
            max_concurrency: max_concurrency.max(1),
        }
    }

    fn acquire(&self, priority: BackendPriority) -> Result<BackendPermit<'_>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("master backend gate lock poisoned"))?;
        if priority == BackendPriority::Foreground {
            state.foreground_waiters = state.foreground_waiters.saturating_add(1);
        }
        while state.active >= self.max_concurrency
            || (priority == BackendPriority::Background && state.foreground_waiters > 0)
        {
            state = self
                .wake
                .wait(state)
                .map_err(|_| anyhow!("master backend gate lock poisoned"))?;
        }
        if priority == BackendPriority::Foreground {
            state.foreground_waiters = state.foreground_waiters.saturating_sub(1);
        }
        state.active = state.active.saturating_add(1);
        Ok(BackendPermit { gate: self })
    }
}

struct BackendPermit<'a> {
    gate: &'a BackendGate,
}

impl Drop for BackendPermit<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.active = state.active.saturating_sub(1);
            self.gate.wake.notify_all();
        }
    }
}

pub(crate) fn cluster_token_source() -> String {
    if env::var("MAJUTSU_CLUSTER_TOKEN")
        .ok()
        .is_some_and(|token| !token.trim().is_empty())
    {
        return "env:MAJUTSU_CLUSTER_TOKEN".into();
    }
    if env::var("MAJUTSU_CLUSTER_TOKEN_FILE")
        .ok()
        .is_some_and(|path| !path.trim().is_empty())
    {
        return "file:MAJUTSU_CLUSTER_TOKEN_FILE".into();
    }
    "missing".into()
}

fn cluster_token() -> Result<String> {
    if let Ok(token) = env::var("MAJUTSU_CLUSTER_TOKEN")
        && !token.trim().is_empty()
    {
        return Ok(token.trim().to_string());
    }
    if let Ok(path) = env::var("MAJUTSU_CLUSTER_TOKEN_FILE")
        && !path.trim().is_empty()
    {
        validate_cluster_token_file_permissions(Path::new(&path))?;
        let token =
            fs::read_to_string(&path).with_context(|| format!("read cluster token file {path}"))?;
        if !token.trim().is_empty() {
            return Ok(token.trim().to_string());
        }
    }
    bail!(
        "cluster token is not configured; set MAJUTSU_CLUSTER_TOKEN or MAJUTSU_CLUSTER_TOKEN_FILE"
    )
}

fn validate_cluster_token_file_permissions(_path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mode = fs::metadata(_path)
            .with_context(|| format!("inspect cluster token file {}", _path.display()))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            bail!(
                "cluster token file must not be accessible by group or others: {} mode={:o}",
                _path.display(),
                mode & 0o777
            );
        }
    }
    Ok(())
}

pub(crate) fn cluster_remote_from_url(url: &str) -> Result<ClusterRemote> {
    let address = url
        .strip_prefix("cluster://")
        .ok_or_else(|| anyhow!("invalid cluster remote URL: {url}"))?
        .trim_end_matches('/');
    if address.is_empty() || address.contains('/') || address.contains('?') || address.contains('#')
    {
        bail!("cluster remote URL must contain only host and port: {url}");
    }
    let parsed = url::Url::parse(&format!("tcp://{address}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("cluster remote URL is missing a host: {url}"))?;
    let port = parsed
        .port()
        .ok_or_else(|| anyhow!("cluster remote URL is missing a port: {url}"))?;
    let endpoint = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    Ok(ClusterRemote {
        endpoint,
        token: cluster_token()?,
        host_id: env::var("MAJUTSU_CLUSTER_HOST_ID")
            .or_else(|_| env::var("HOSTNAME"))
            .unwrap_or_else(|_| "unknown-host".into()),
    })
}

pub(crate) fn cluster_heartbeat_interval() -> Duration {
    Duration::from_secs(
        env::var("MAJUTSU_CLUSTER_HEARTBEAT_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CLUSTER_HEARTBEAT_INTERVAL_SECS)
            .clamp(5, 3600),
    )
}

pub(crate) fn cluster_slave_heartbeat(paths: &Paths) -> Result<bool> {
    if cluster_master_mode_enabled() {
        return Ok(false);
    }
    let config = read_config(paths)?;
    let Some(remote_config) = config.remote else {
        return Ok(false);
    };
    let remote_url = remote_config.url()?;
    if !remote_url.starts_with("cluster://") {
        return Ok(false);
    }
    let remote = cluster_remote_from_url(&remote_url)?;
    remote.heartbeat_if_supported()
}

fn cluster_request_timeout() -> Duration {
    let secs = env::var("MAJUTSU_CLUSTER_REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CLUSTER_REQUEST_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

fn cluster_max_frame_bytes() -> usize {
    env::var("MAJUTSU_CLUSTER_MAX_FRAME_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value >= 1024 * 1024)
        .unwrap_or(DEFAULT_CLUSTER_MAX_FRAME_BYTES)
}

fn cluster_max_inflight_frame_bytes() -> usize {
    env::var("MAJUTSU_CLUSTER_MAX_INFLIGHT_FRAME_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value >= 1024 * 1024)
        .unwrap_or(DEFAULT_CLUSTER_MAX_INFLIGHT_FRAME_BYTES)
}

fn cluster_max_clients() -> usize {
    env::var("MAJUTSU_CLUSTER_MAX_CLIENTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map(|value| value.clamp(1, 1024))
        .unwrap_or(DEFAULT_CLUSTER_MAX_CLIENTS)
}

fn connect_cluster(endpoint: &str) -> Result<TcpStream> {
    let timeout = cluster_request_timeout();
    let mut addresses = endpoint
        .to_socket_addrs()
        .with_context(|| format!("resolve cluster endpoint {endpoint}"))?
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        bail!("cluster endpoint has no addresses: {endpoint}");
    }
    let mut last_error = None;
    for address in addresses.drain(..) {
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(stream) => {
                stream.set_read_timeout(Some(timeout))?;
                stream.set_write_timeout(Some(timeout))?;
                return Ok(stream);
            }
            Err(err) => last_error = Some(err),
        }
    }
    Err(anyhow!(
        "connect to cluster master {endpoint}: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "no address succeeded".into())
    ))
}

fn write_frame(stream: &mut TcpStream, bytes: &[u8]) -> Result<()> {
    let max = cluster_max_frame_bytes();
    if bytes.len() > max || bytes.len() > u32::MAX as usize {
        bail!(
            "cluster frame is too large: {} bytes (limit {})",
            bytes.len(),
            max
        );
    }
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()?;
    Ok(())
}

fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
    read_frame_with_limit(stream, cluster_max_frame_bytes())
}

fn read_frame_with_limit(stream: &mut TcpStream, max: usize) -> Result<Vec<u8>> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > max {
        bail!(
            "cluster frame exceeds limit: {} bytes (limit {})",
            length,
            max
        );
    }
    let mut bytes = vec![0_u8; length];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_frame_with_budget(
    stream: &mut TcpStream,
    max: usize,
    budget: &Arc<FrameBudget>,
) -> Result<(Vec<u8>, FrameBudgetPermit)> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > max {
        bail!(
            "cluster frame exceeds limit: {} bytes (limit {})",
            length,
            max
        );
    }
    let permit = budget.try_reserve(length)?;
    let mut bytes = vec![0_u8; length];
    stream.read_exact(&mut bytes)?;
    Ok((bytes, permit))
}

fn cluster_request(remote: &ClusterRemote, operation: ClusterOperation) -> Result<ClusterValue> {
    let capabilities = cluster_capabilities(remote)?;
    let operation_name = cluster_operation_name(&operation);
    if !capabilities
        .supported_operations
        .iter()
        .any(|supported| supported == operation_name)
    {
        bail!(
            "cluster master does not advertise operation {operation_name}; supported={}",
            capabilities.supported_operations.join(",")
        );
    }
    cluster_request_raw(remote, operation, Some(&capabilities))
}

fn cluster_request_raw(
    remote: &ClusterRemote,
    operation: ClusterOperation,
    capabilities: Option<&ClusterCapabilities>,
) -> Result<ClusterValue> {
    let request = ClusterRequestFrame {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        token: remote.token.clone(),
        host_id: remote.host_id.clone(),
        operation,
    };
    let request_bytes = serde_cbor::to_vec(&request)?;
    if let Some(capabilities) = capabilities
        && request_bytes.len() > capabilities.max_request_frame_bytes
    {
        bail!(
            "cluster request frame is too large for master: {} bytes (limit {})",
            request_bytes.len(),
            capabilities.max_request_frame_bytes
        );
    }
    let mut stream = connect_cluster(&remote.endpoint)?;
    write_frame(&mut stream, &request_bytes)?;
    let response_bytes = if let Some(capabilities) = capabilities {
        read_frame_with_limit(
            &mut stream,
            cluster_max_frame_bytes().min(capabilities.max_response_frame_bytes),
        )?
    } else {
        read_frame(&mut stream)?
    };
    let response: ClusterResponse =
        serde_cbor::from_slice(&response_bytes).context("decode cluster master response")?;
    match response {
        ClusterResponse::Ok(value) => Ok(value),
        ClusterResponse::Error { code, message } => {
            Err(anyhow!("cluster master {code}: {message}"))
        }
    }
}

static CLUSTER_CAPABILITIES_CACHE: OnceLock<Mutex<HashMap<String, ClusterCapabilities>>> =
    OnceLock::new();

fn cluster_capabilities_cache() -> &'static Mutex<HashMap<String, ClusterCapabilities>> {
    CLUSTER_CAPABILITIES_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cluster_capabilities_cache_key(remote: &ClusterRemote) -> String {
    format!(
        "{}\n{}\n{}",
        remote.endpoint,
        remote.host_id,
        hex::encode(token_digest(&remote.token))
    )
}

fn cluster_capabilities(remote: &ClusterRemote) -> Result<ClusterCapabilities> {
    let cache_key = cluster_capabilities_cache_key(remote);
    if let Some(capabilities) = cluster_capabilities_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(&cache_key).cloned())
    {
        return Ok(capabilities);
    }
    let capabilities = match cluster_request_raw(remote, ClusterOperation::Capabilities, None)? {
        ClusterValue::Capabilities(capabilities) => capabilities,
        other => bail!("cluster master returned an unexpected capability response: {other:?}"),
    };
    if capabilities.protocol_version != CLUSTER_PROTOCOL_VERSION {
        bail!(
            "cluster capability protocol mismatch: client={} master={}",
            CLUSTER_PROTOCOL_VERSION,
            capabilities.protocol_version
        );
    }
    if capabilities.max_request_frame_bytes < 1024 * 1024
        || capabilities.max_response_frame_bytes < 1024 * 1024
        || capabilities.max_batch_objects == 0
        || capabilities.max_batch_payload_bytes == 0
    {
        bail!("cluster master advertised invalid capability limits: {capabilities:?}");
    }
    if let Ok(mut cache) = cluster_capabilities_cache().lock() {
        cache.insert(cache_key, capabilities.clone());
    }
    Ok(capabilities)
}

fn cluster_operation_name(operation: &ClusterOperation) -> &'static str {
    match operation {
        ClusterOperation::Capabilities => "capabilities",
        ClusterOperation::Heartbeat => "heartbeat",
        ClusterOperation::Put { .. } => "put",
        ClusterOperation::BatchPut { .. } => "batch_put",
        ClusterOperation::Get { .. } => "get",
        ClusterOperation::GetOptional { .. } => "get_optional",
        ClusterOperation::GetRange { .. } => "get_range",
        ClusterOperation::Exists { .. } => "exists",
        ClusterOperation::BackendCommitted { .. } => "backend_committed",
        ClusterOperation::List { .. } => "list",
        ClusterOperation::ListCommonPrefixes { .. } => "list_common_prefixes",
        ClusterOperation::ListWithSizes { .. } => "list_with_sizes",
        ClusterOperation::ListMasterLocalWithSizes { .. } => "list_master_local_with_sizes",
        ClusterOperation::Delete { .. } => "delete",
        ClusterOperation::RestoreArchive { .. } => "restore_archive",
        ClusterOperation::ApplyLifecycle { .. } => "apply_lifecycle",
    }
}

fn advertised_cluster_capabilities(host_id: &str) -> ClusterCapabilities {
    ClusterCapabilities {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        max_request_frame_bytes: cluster_max_frame_bytes().min(cluster_max_inflight_frame_bytes()),
        max_response_frame_bytes: cluster_max_frame_bytes(),
        max_batch_objects: DEFAULT_CLUSTER_MAX_BATCH_OBJECTS,
        max_batch_payload_bytes: DEFAULT_CLUSTER_MAX_BATCH_BYTES
            .min(cluster_max_frame_bytes().saturating_sub(64 * 1024)),
        write_prefix: format!("{host_id}/"),
        supported_operations: [
            "capabilities",
            "heartbeat",
            "put",
            "batch_put",
            "get",
            "get_optional",
            "get_range",
            "exists",
            "backend_committed",
            "list",
            "list_common_prefixes",
            "list_with_sizes",
            "list_master_local_with_sizes",
            "delete",
            "restore_archive",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    }
}

impl ClusterRemote {
    pub(crate) fn heartbeat_if_supported(&self) -> Result<bool> {
        let capabilities = cluster_capabilities(self)?;
        if !capabilities
            .supported_operations
            .iter()
            .any(|operation| operation == "heartbeat")
        {
            // 旧masterとの混在期間は、heartbeat未対応でも通常同期を継続する。
            return Ok(false);
        }
        expect_empty(cluster_request_raw(
            self,
            ClusterOperation::Heartbeat,
            Some(&capabilities),
        )?)?;
        Ok(true)
    }

    pub(crate) fn backend_committed_keys(&self, keys: &[String]) -> Result<BTreeSet<String>> {
        // 旧masterと混在中は未確認としてpinし、S3確認をslaveから直接行わない。
        if !cluster_capabilities(self)?
            .supported_operations
            .iter()
            .any(|op| op == "backend_committed")
        {
            return Ok(BTreeSet::new());
        }
        let mut committed = BTreeSet::new();
        for batch in keys.chunks(DEFAULT_CLUSTER_MAX_BATCH_OBJECTS) {
            committed.extend(expect_strings(cluster_request(
                self,
                ClusterOperation::BackendCommitted {
                    keys: batch.to_vec(),
                },
            )?)?);
        }
        Ok(committed)
    }

    pub(crate) fn capabilities(&self) -> Result<ClusterCapabilities> {
        cluster_capabilities(self)
    }

    pub(crate) fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        expect_empty(cluster_request(
            self,
            ClusterOperation::Put {
                key: key.to_string(),
                bytes: bytes.to_vec(),
                conditional: false,
            },
        )?)
    }

    pub(crate) fn put_file(&self, key: &str, source: &std::path::Path) -> Result<()> {
        self.put(
            key,
            &fs::read(source)
                .with_context(|| format!("read upload source {}", source.display()))?,
        )
    }

    pub(crate) fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        expect_bool(cluster_request(
            self,
            ClusterOperation::Put {
                key: key.to_string(),
                bytes: bytes.to_vec(),
                conditional: true,
            },
        )?)
    }

    pub(crate) fn put_file_if_absent(&self, key: &str, source: &std::path::Path) -> Result<bool> {
        self.put_if_absent(
            key,
            &fs::read(source)
                .with_context(|| format!("read upload source {}", source.display()))?,
        )
    }

    pub(crate) fn batch_put(&self, items: Vec<ClusterPutItem>) -> Result<Vec<ClusterPutResult>> {
        match cluster_request(self, ClusterOperation::BatchPut { items })? {
            ClusterValue::BatchPut(results) => Ok(results),
            other => bail!("cluster master returned an unexpected batch response: {other:?}"),
        }
    }

    pub(crate) fn get(&self, key: &str) -> Result<Vec<u8>> {
        match cluster_request(
            self,
            ClusterOperation::Get {
                key: key.to_string(),
            },
        )? {
            ClusterValue::Bytes(bytes) => Ok(bytes),
            other => bail!("cluster master returned an unexpected get response: {other:?}"),
        }
    }

    pub(crate) fn get_optional(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match cluster_request(
            self,
            ClusterOperation::GetOptional {
                key: key.to_string(),
            },
        )? {
            ClusterValue::OptionalBytes(bytes) => Ok(bytes),
            other => {
                bail!("cluster master returned an unexpected optional get response: {other:?}")
            }
        }
    }

    pub(crate) fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        match cluster_request(
            self,
            ClusterOperation::GetRange {
                key: key.to_string(),
                start,
                len,
            },
        )? {
            ClusterValue::Bytes(bytes) => Ok(bytes),
            other => bail!("cluster master returned an unexpected range response: {other:?}"),
        }
    }

    pub(crate) fn exists(&self, key: &str) -> Result<bool> {
        expect_bool(cluster_request(
            self,
            ClusterOperation::Exists {
                key: key.to_string(),
            },
        )?)
    }

    pub(crate) fn list(&self, prefix: &str) -> Result<Vec<String>> {
        expect_strings(cluster_request(
            self,
            ClusterOperation::List {
                prefix: prefix.to_string(),
            },
        )?)
    }

    pub(crate) fn list_common_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        expect_strings(cluster_request(
            self,
            ClusterOperation::ListCommonPrefixes {
                prefix: prefix.to_string(),
            },
        )?)
    }

    pub(crate) fn list_with_sizes(&self, prefix: &str) -> Result<Vec<RemoteObjectStat>> {
        match cluster_request(
            self,
            ClusterOperation::ListWithSizes {
                prefix: prefix.to_string(),
            },
        )? {
            ClusterValue::Objects(objects) => Ok(objects),
            other => bail!("cluster master returned an unexpected list response: {other:?}"),
        }
    }

    pub(crate) fn list_master_local_with_sizes(
        &self,
        prefix: &str,
    ) -> Result<Vec<RemoteObjectStat>> {
        match cluster_request(
            self,
            ClusterOperation::ListMasterLocalWithSizes {
                prefix: prefix.to_string(),
            },
        )? {
            ClusterValue::Objects(objects) => Ok(objects),
            other => bail!("cluster master returned an unexpected local-list response: {other:?}"),
        }
    }

    pub(crate) fn delete(&self, key: &str) -> Result<()> {
        expect_empty(cluster_request(
            self,
            ClusterOperation::Delete {
                key: key.to_string(),
            },
        )?)
    }

    pub(crate) fn restore_archive(&self, key: &str, days: u32, tier: &str) -> Result<bool> {
        expect_bool(cluster_request(
            self,
            ClusterOperation::RestoreArchive {
                key: key.to_string(),
                days,
                tier: tier.to_string(),
            },
        )?)
    }

    pub(crate) fn apply_s3_lifecycle_policy(&self, policy: &serde_json::Value) -> Result<bool> {
        expect_bool(cluster_request(
            self,
            ClusterOperation::ApplyLifecycle {
                policy: policy.clone(),
            },
        )?)
    }
}

fn expect_empty(value: ClusterValue) -> Result<()> {
    match value {
        ClusterValue::Empty => Ok(()),
        other => bail!("cluster master returned an unexpected response: {other:?}"),
    }
}

fn expect_bool(value: ClusterValue) -> Result<bool> {
    match value {
        ClusterValue::Bool(value) => Ok(value),
        other => bail!("cluster master returned an unexpected boolean response: {other:?}"),
    }
}

fn expect_strings(value: ClusterValue) -> Result<Vec<String>> {
    match value {
        ClusterValue::Strings(value) => Ok(value),
        other => bail!("cluster master returned an unexpected list response: {other:?}"),
    }
}

struct MasterServerState {
    store: RemoteStore,
    store_dir: PathBuf,
    backend: RemoteStore,
    backend_gate: BackendGate,
    backend_batch_delete_enabled: AtomicBool,
    backend_queue_dir: PathBuf,
    write_intent_dir: PathBuf,
    backend_receipt_dir: PathBuf,
    local_gc_dir: PathBuf,
    status_path: PathBuf,
    flush_request_path: PathBuf,
    store_bytes: AtomicU64,
    store_max_bytes: Option<u64>,
    durable_lock: Mutex<()>,
    backend_runtime: Mutex<BackendRuntimeState>,
    metrics: Mutex<MasterMetrics>,
    last_status_write: Mutex<Option<Instant>>,
    client_limiter: Arc<ClientLimiter>,
    frame_budget: Arc<FrameBudget>,
    authentication: ClusterAuthentication,
    list_cache: Mutex<HashMap<String, (Instant, CachedList)>>,
}

fn token_digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn constant_time_digest_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn read_master_metrics(status_path: &Path) -> MasterMetrics {
    let Ok(bytes) = fs::read(status_path) else {
        return MasterMetrics::default();
    };
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|value| serde_json::from_value(value.get("metrics")?.clone()).ok())
        .unwrap_or_default()
}

fn cluster_authentication() -> Result<ClusterAuthentication> {
    let Some(directory) = env::var_os("MAJUTSU_CLUSTER_TOKEN_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
    else {
        return Ok(ClusterAuthentication::LegacyShared {
            token_digest: token_digest(&cluster_token()?),
        });
    };
    let mut token_digests = BTreeMap::new();
    let mut digest_owners = BTreeMap::<[u8; 32], String>::new();
    for entry in fs::read_dir(&directory)
        .with_context(|| format!("read cluster host token directory {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("token") {
            continue;
        }
        if !entry.file_type()?.is_file() {
            bail!(
                "cluster host token entry is not a regular file: {}",
                path.display()
            );
        }
        let host_id = path
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                anyhow!(
                    "cluster host token filename is not UTF-8: {}",
                    path.display()
                )
            })?;
        validate_cluster_host_id(host_id)?;
        validate_cluster_token_file_permissions(&path)?;
        let token = fs::read_to_string(&path)
            .with_context(|| format!("read cluster host token file {}", path.display()))?;
        if token.trim().is_empty() {
            bail!("cluster host token file is empty: {}", path.display());
        }
        let digest = token_digest(token.trim());
        if let Some(existing_host) = digest_owners.insert(digest, host_id.to_string()) {
            bail!(
                "cluster host token is reused by {existing_host} and {host_id}; every host requires a unique token"
            );
        }
        token_digests.insert(host_id.to_string(), digest);
    }
    if token_digests.is_empty() {
        bail!(
            "cluster host token directory contains no <host-id>.token files: {}",
            directory.display()
        );
    }
    Ok(ClusterAuthentication::HostTokens { token_digests })
}

fn master_server_state(paths: &Paths, config: &Config) -> Result<Arc<MasterServerState>> {
    let remote_config = config
        .remote
        .as_ref()
        .ok_or_else(|| anyhow!("master requires a configured S3 remote"))?;
    let remote_url = remote_config.url()?;
    if remote_url.starts_with("cluster://") {
        bail!("master remote must point directly to S3/GCS, not to another cluster master");
    }
    let backend = open_remote_with_upload_policy(
        remote_config,
        config.large.multipart,
        config.large.max_parallel_uploads,
    )?;
    if !backend.is_s3_like() {
        bail!("master remote must be an S3-compatible backend");
    }
    let store_dir = env::var_os("MAJUTSU_CLUSTER_STORE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| paths.home.join("cluster/store"));
    let backend_queue_dir = env::var_os("MAJUTSU_CLUSTER_BACKEND_QUEUE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| paths.home.join("cluster/backend-queue"));
    let local_gc_dir = env::var_os("MAJUTSU_CLUSTER_LOCAL_GC_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| paths.home.join("cluster/local-gc"));
    create_dir_all_durable(&store_dir)
        .with_context(|| format!("create cluster master store {}", store_dir.display()))?;
    create_dir_all_durable(&backend_queue_dir).with_context(|| {
        format!(
            "create cluster backend queue {}",
            backend_queue_dir.display()
        )
    })?;
    create_dir_all_durable(&local_gc_dir)
        .with_context(|| format!("create cluster local GC queue {}", local_gc_dir.display()))?;
    let write_intent_dir = backend_queue_dir.with_file_name("write-intents");
    let backend_receipt_dir = backend_queue_dir.with_file_name("backend-receipts");
    create_dir_all_durable(&write_intent_dir)?;
    create_dir_all_durable(&backend_receipt_dir)?;
    fs::create_dir_all(&paths.runtime)?;
    let store_bytes = directory_file_bytes(&store_dir)?;
    let store_max_bytes = env::var("MAJUTSU_CLUSTER_STORE_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0);
    // 容量超過でも既存データの読取りとbacklogの排出を可能にする。
    let status_path = paths.runtime.join("cluster-master-status.json");
    let state = Arc::new(MasterServerState {
        store: RemoteStore::File(crate::remote_store::FileRemote {
            root: store_dir.clone(),
        }),
        store_dir,
        backend,
        backend_gate: BackendGate::new(backend_max_concurrency()),
        backend_batch_delete_enabled: AtomicBool::new(true),
        backend_queue_dir,
        write_intent_dir,
        backend_receipt_dir,
        local_gc_dir,
        metrics: Mutex::new(read_master_metrics(&status_path)),
        last_status_write: Mutex::new(None),
        client_limiter: Arc::new(ClientLimiter::new(cluster_max_clients())),
        frame_budget: Arc::new(FrameBudget::new(cluster_max_inflight_frame_bytes())),
        status_path,
        flush_request_path: paths.runtime.join("cluster-backend-flush.request"),
        store_bytes: AtomicU64::new(store_bytes),
        store_max_bytes,
        durable_lock: Mutex::new(()),
        backend_runtime: Mutex::new(BackendRuntimeState::default()),
        authentication: cluster_authentication()?,
        list_cache: Mutex::new(HashMap::new()),
    });
    recover_master_write_intents(&state)?;
    reconcile_legacy_backend_queue(&state)?;
    write_master_status(&state)?;
    Ok(state)
}

fn bind_master_listener(listen: &str) -> Result<TcpListener> {
    TcpListener::bind(listen).with_context(|| format!("bind cluster master listener {listen}"))
}

fn serve_listener(listener: TcpListener, state: Arc<MasterServerState>) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let Some(client_permit) = state.client_limiter.try_acquire() else {
                    eprintln!(
                        "cluster masterの同時接続数上限({})に達したため接続を拒否しました",
                        state.client_limiter.max
                    );
                    drop(stream);
                    continue;
                };
                let state = Arc::clone(&state);
                if let Err(err) = thread::Builder::new()
                    .name("mj-cluster-client".into())
                    .spawn(move || {
                        let _client_permit = client_permit;
                        handle_connection(stream, state);
                    })
                {
                    eprintln!("cluster client thread error: {err}");
                }
            }
            Err(err) => eprintln!("cluster master accept error: {err}"),
        }
    }
}

fn serve_master(paths: &Paths, args: ClusterServeArgs) -> Result<()> {
    let config = read_config(paths)?;
    let state = master_server_state(paths, &config)?;
    let listener = bind_master_listener(&args.listen)?;
    start_backend_worker(Arc::clone(&state))?;
    println!(
        "cluster master listening {} store={} backend_flush_interval_secs={}",
        args.listen,
        state.store_dir.display(),
        backend_flush_interval().as_secs()
    );
    serve_listener(listener, state);
    Ok(())
}

pub(crate) fn cluster_master_mode_enabled() -> bool {
    env::var("MAJUTSU_CLUSTER_ROLE").as_deref() == Ok("master")
}

pub(crate) fn ensure_data_producer_allowed(action: &str) -> Result<()> {
    if cluster_master_mode_enabled() {
        bail!(
            "cluster master is a data-holder and cannot run `{action}`; unset MAJUTSU_CLUSTER_ROLE for the standalone profile"
        );
    }
    Ok(())
}

pub(crate) fn serve_master_from_env(paths: &Paths) -> Result<()> {
    let listen =
        env::var("MAJUTSU_CLUSTER_LISTEN").unwrap_or_else(|_| DEFAULT_CLUSTER_LISTEN.to_string());
    serve_master(paths, ClusterServeArgs { listen })
}

pub(crate) fn cluster_cmd(paths: &Paths, command: ClusterCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    match command {
        ClusterCommand::Serve(args) => serve_master(paths, args),
        ClusterCommand::Status { json } => cluster_status_cmd(paths, json),
        ClusterCommand::Flush { wait_secs } => cluster_flush_cmd(paths, wait_secs),
    }
}

pub(crate) fn cluster_status_cmd(paths: &Paths, json: bool) -> Result<()> {
    let path = paths.runtime.join("cluster-master-status.json");
    let bytes = fs::read(&path).with_context(|| {
        format!(
            "read cluster master status {}; start the cluster master first",
            path.display()
        )
    })?;
    let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
    value["stale"] = serde_json::json!(master_status_stale(&value));
    if json {
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }
    println!(
        "observed_at {}",
        json_optional_string(&value["observed_at"])
    );
    println!("process_id {}", json_u64(&value["process_id"]));
    println!("stale {}", value["stale"]);
    println!("catalog_errors {}", value["catalog_errors"]);
    let queue = &value["queue"];
    println!("role {}", json_string(&value["role"]));
    println!("protocol_version {}", json_u64(&value["protocol_version"]));
    println!(
        "authentication_mode {}",
        json_string(&value["authentication_mode"])
    );
    println!(
        "authorized_hosts {}",
        value["authorized_hosts"]
            .as_array()
            .map(|hosts| {
                hosts
                    .iter()
                    .filter_map(|host| host.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .filter(|hosts| !hosts.is_empty())
            .unwrap_or_else(|| "(legacy-any-host-id)".into())
    );
    println!("store_dir {}", json_string(&value["store_dir"]));
    println!("store_bytes {}", json_u64(&value["store_bytes"]));
    println!(
        "store_max_bytes {}",
        value["store_max_bytes"]
            .as_u64()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unlimited".into())
    );
    println!(
        "backend_max_concurrency {}",
        json_u64(&value["backend_max_concurrency"])
    );
    println!("max_clients {}", json_u64(&value["max_clients"]));
    println!(
        "max_inflight_frame_bytes {}",
        json_u64(&value["max_inflight_frame_bytes"])
    );
    println!(
        "backend_batch_delete_enabled {}",
        value["backend_batch_delete_enabled"]
            .as_bool()
            .unwrap_or(false)
    );
    println!(
        "local_delete_grace_secs {}",
        json_u64(&value["local_delete_grace_secs"])
    );
    println!(
        "backend_queue_objects {}",
        json_count_or_unknown(&queue["objects"])
    );
    println!(
        "backend_queue_bytes {}",
        json_count_or_unknown(&queue["bytes"])
    );
    println!(
        "backend_queue_retrying {}",
        json_count_or_unknown(&queue["retrying"])
    );
    println!(
        "backend_queue_oldest_created_at {}",
        json_optional_string(&queue["oldest_created_at"])
    );
    let retained = &value["retained_deleted"];
    println!(
        "retained_deleted_objects {}",
        json_count_or_unknown(&retained["objects"])
    );
    println!(
        "retained_deleted_bytes {}",
        json_count_or_unknown(&retained["bytes"])
    );
    println!(
        "retained_deleted_next_gc_at {}",
        json_optional_string(&retained["next_delete_after"])
    );
    println!(
        "backend_last_success_at {}",
        json_optional_string(&value["last_backend_success_at"])
    );
    println!(
        "backend_last_error_at {}",
        json_optional_string(&value["last_backend_error_at"])
    );
    println!(
        "backend_last_error {}",
        json_optional_string(&value["last_backend_error"])
    );
    println!(
        "backend_flush_requested {}",
        value["flush_requested"].as_bool().unwrap_or(false)
    );
    Ok(())
}

pub(crate) fn cluster_metrics_cmd(paths: &Paths) -> Result<()> {
    let path = paths.runtime.join("cluster-master-status.json");
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).with_context(|| {
            format!(
                "read cluster master status {}; start the cluster master first",
                path.display()
            )
        })?)?;
    let queue = &value["queue"];
    let retained = &value["retained_deleted"];
    println!(
        "majutsu_cluster_status_stale {}",
        u8::from(master_status_stale(&value))
    );
    println!(
        "majutsu_cluster_status_process_id {}",
        json_u64(&value["process_id"])
    );
    println!(
        "majutsu_cluster_catalog_errors {}",
        value["catalog_errors"].as_array().map_or(0, Vec::len)
    );
    println!("# TYPE majutsu_cluster_master_holder gauge");
    println!("majutsu_cluster_master_holder 1");
    println!("# TYPE majutsu_cluster_store_bytes gauge");
    println!(
        "majutsu_cluster_store_bytes {}",
        json_u64(&value["store_bytes"])
    );
    println!(
        "majutsu_cluster_store_max_bytes {}",
        value["store_max_bytes"].as_u64().unwrap_or_default()
    );
    println!(
        "majutsu_cluster_backend_queue_objects {}",
        json_count_or_unknown(&queue["objects"])
    );
    println!(
        "majutsu_cluster_backend_queue_bytes {}",
        json_count_or_unknown(&queue["bytes"])
    );
    println!(
        "majutsu_cluster_backend_queue_retrying {}",
        json_count_or_unknown(&queue["retrying"])
    );
    println!(
        "majutsu_cluster_backend_queue_oldest_age_seconds {}",
        json_age_seconds(&queue["oldest_created_at"])
    );
    println!(
        "majutsu_cluster_retained_deleted_objects {}",
        json_count_or_unknown(&retained["objects"])
    );
    println!(
        "majutsu_cluster_retained_deleted_bytes {}",
        json_count_or_unknown(&retained["bytes"])
    );
    for (name, metric) in [
        ("requests_total", "requests_total"),
        ("rejected_requests_total", "rejected_requests_total"),
        ("backend_puts_total", "backend_puts_total"),
        (
            "backend_puts_skipped_existing_total",
            "backend_puts_skipped_existing_total",
        ),
        ("backend_deletes_total", "backend_deletes_total"),
        ("backend_failures_total", "backend_failures_total"),
        (
            "store_quota_rejections_total",
            "store_quota_rejections_total",
        ),
    ] {
        println!(
            "majutsu_cluster_{name} {}",
            json_u64(&value["metrics"][metric])
        );
    }
    for (host_id, host) in value["metrics"]["hosts"].as_object().into_iter().flatten() {
        let label = prometheus_label(host_id);
        for metric in [
            "requests",
            "received_batches",
            "received_objects",
            "received_bytes",
            "newly_stored_objects",
            "duplicate_objects",
            "queue_reused_objects",
            "same_key_churn",
            "delete_requests",
            "backend_puts",
            "backend_puts_skipped_existing",
            "backend_deletes",
            "backend_failures",
        ] {
            println!(
                "majutsu_cluster_host_{metric}{{host_id=\"{label}\"}} {}",
                json_u64(&host[metric])
            );
        }
        let heartbeat_age = json_optional_age_seconds(&host["last_heartbeat_at"])
            .map(|age| age.to_string())
            .unwrap_or_else(|| "NaN".into());
        println!(
            "majutsu_cluster_host_heartbeat_age_seconds{{host_id=\"{label}\"}} {heartbeat_age}"
        );
    }
    for metric in [
        "put_requests",
        "get_requests",
        "head_requests",
        "list_requests",
        "post_requests",
        "delete_requests",
        "upload_bytes",
        "download_bytes",
    ] {
        println!(
            "majutsu_cluster_backend_api_{metric} {}",
            json_u64(&value["backend_api_since_start"][metric])
        );
    }
    Ok(())
}

fn json_age_seconds(value: &serde_json::Value) -> u64 {
    json_optional_age_seconds(value).unwrap_or_default()
}

fn json_optional_age_seconds(value: &serde_json::Value) -> Option<u64> {
    value
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|created_at| {
            Utc::now()
                .signed_duration_since(created_at.with_timezone(&Utc))
                .num_seconds()
                .max(0) as u64
        })
}

fn prometheus_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn cluster_flush_cmd(paths: &Paths, wait_secs: u64) -> Result<()> {
    fs::create_dir_all(&paths.runtime)?;
    let request = paths.runtime.join("cluster-backend-flush.request");
    crate::atomic_io::write_atomic(&request, Utc::now().to_rfc3339().as_bytes())?;
    println!("cluster backend flush requested");
    if wait_secs == 0 {
        return Ok(());
    }
    let started = Instant::now();
    let timeout = Duration::from_secs(wait_secs);
    while request.exists() {
        if started.elapsed() >= timeout {
            bail!(
                "cluster backend flush did not complete within {wait_secs} seconds; the request remains pending"
            );
        }
        thread::sleep(Duration::from_millis(200));
    }
    println!("cluster backend flush completed");
    Ok(())
}

fn json_string(value: &serde_json::Value) -> &str {
    value.as_str().unwrap_or("(none)")
}

fn json_optional_string(value: &serde_json::Value) -> &str {
    value.as_str().unwrap_or("(none)")
}

fn json_u64(value: &serde_json::Value) -> u64 {
    value.as_u64().unwrap_or_default()
}

fn json_count_or_unknown(value: &serde_json::Value) -> String {
    value
        .as_u64()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "NaN".into())
}

fn master_status_stale(value: &serde_json::Value) -> bool {
    value["observed_at"]
        .as_str()
        .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
        .is_none_or(|at| {
            let age = Utc::now().signed_duration_since(at).num_seconds();
            age < -5 || age > env_u64("MAJUTSU_CLUSTER_STATUS_STALE_SECS", 120) as i64
        })
}

fn handle_connection(mut stream: TcpStream, state: Arc<MasterServerState>) {
    let timeout = cluster_request_timeout();
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let (frame_bytes, _frame_permit) = match read_frame_with_budget(
        &mut stream,
        cluster_max_frame_bytes().min(cluster_max_inflight_frame_bytes()),
        &state.frame_budget,
    ) {
        Ok(request) => request,
        Err(err) => {
            let _ = send_error(&mut stream, "invalid_request", &format!("{err:#}"));
            return;
        }
    };
    let request = match serde_cbor::from_slice::<ClusterRequestFrame>(&frame_bytes)
        .context("decode cluster request")
    {
        Ok(request) => request,
        Err(err) => {
            let _ = send_error(&mut stream, "invalid_request", &format!("{err:#}"));
            return;
        }
    };
    if request.protocol_version != CLUSTER_PROTOCOL_VERSION {
        record_rejected_request(&state);
        let _ = send_error(
            &mut stream,
            "protocol_mismatch",
            &format!("unsupported protocol version {}", request.protocol_version),
        );
        return;
    }
    if !state
        .authentication
        .authenticates(&request.host_id, &request.token)
    {
        record_rejected_request(&state);
        let _ = send_error(&mut stream, "unauthorized", "cluster token did not match");
        return;
    }
    if let Err(err) = validate_cluster_host_id(&request.host_id) {
        record_rejected_request(&state);
        let _ = send_error(&mut stream, "invalid_host_id", &format!("{err:#}"));
        return;
    }
    if let Err(err) = authorize_cluster_operation(&request.host_id, &request.operation) {
        record_rejected_request(&state);
        let _ = send_error(&mut stream, "forbidden", &format!("{err:#}"));
        return;
    }
    let is_heartbeat = matches!(&request.operation, ClusterOperation::Heartbeat);
    record_authorized_request(&state, &request.host_id, &request.operation);
    let response = match dispatch_operation(&state, &request.host_id, request.operation) {
        Ok(value) => ClusterResponse::Ok(value),
        Err(err) => ClusterResponse::Error {
            code: "backend_error".into(),
            message: format!("{err:#}"),
        },
    };
    if is_heartbeat && let Err(err) = write_master_status_throttled(&state) {
        eprintln!("cluster master heartbeat status update failed: {err:#}");
    }
    if let Ok(bytes) = serde_cbor::to_vec(&response) {
        let _ = write_frame(&mut stream, &bytes);
    }
}

fn send_error(stream: &mut TcpStream, code: &str, message: &str) -> Result<()> {
    write_frame(
        stream,
        &serde_cbor::to_vec(&ClusterResponse::Error {
            code: code.into(),
            message: message.into(),
        })?,
    )
}

fn validate_cluster_key(key: &str) -> Result<()> {
    crate::util::validate_slash_relative_key(key)
}

fn validate_cluster_prefix(prefix: &str) -> Result<()> {
    if prefix.starts_with('/') || prefix.contains('\\') {
        bail!("cluster list prefix must be relative: {prefix}");
    }
    for component in prefix.split('/') {
        if component == "." || component == ".." {
            bail!("cluster list prefix contains an unsafe component: {prefix}");
        }
    }
    Ok(())
}

fn validate_cluster_host_id(host_id: &str) -> Result<()> {
    if host_id.trim().is_empty() || host_id.len() > 128 {
        bail!("cluster host id must contain 1..=128 bytes");
    }
    if !host_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("cluster host id contains unsupported characters: {host_id}");
    }
    Ok(())
}

fn authorize_cluster_operation(host_id: &str, operation: &ClusterOperation) -> Result<()> {
    let own_prefix = format!("{host_id}/");
    let require_own_key = |key: &str, action: &str| -> Result<()> {
        if key.starts_with(&own_prefix) {
            return Ok(());
        }
        bail!(
            "host {host_id} may only {action} keys under its own prefix {own_prefix}; requested={key}"
        )
    };
    match operation {
        ClusterOperation::Capabilities
        | ClusterOperation::Heartbeat
        | ClusterOperation::Get { .. }
        | ClusterOperation::GetOptional { .. }
        | ClusterOperation::GetRange { .. }
        | ClusterOperation::Exists { .. }
        | ClusterOperation::BackendCommitted { .. }
        | ClusterOperation::List { .. }
        | ClusterOperation::ListCommonPrefixes { .. }
        | ClusterOperation::ListWithSizes { .. }
        | ClusterOperation::ListMasterLocalWithSizes { .. } => Ok(()),
        ClusterOperation::Put { key, .. } => require_own_key(key, "write"),
        ClusterOperation::BatchPut { items } => {
            for item in items {
                require_own_key(&item.key, "write")?;
            }
            Ok(())
        }
        ClusterOperation::Delete { key } => require_own_key(key, "delete"),
        ClusterOperation::RestoreArchive { key, .. } => require_own_key(key, "restore"),
        ClusterOperation::ApplyLifecycle { .. } => {
            bail!("lifecycle policy changes are master-local maintenance operations")
        }
    }
}

fn record_rejected_request(state: &MasterServerState) {
    if let Ok(mut metrics) = state.metrics.lock() {
        metrics.rejected_requests_total = metrics.rejected_requests_total.saturating_add(1);
    }
}

fn record_authorized_request(
    state: &MasterServerState,
    host_id: &str,
    operation: &ClusterOperation,
) {
    let Ok(mut metrics) = state.metrics.lock() else {
        return;
    };
    metrics.requests_total = metrics.requests_total.saturating_add(1);
    let host = metrics.hosts.entry(host_id.to_string()).or_default();
    host.requests = host.requests.saturating_add(1);
    let now = Utc::now();
    host.last_seen_at = Some(now);
    match operation {
        ClusterOperation::Heartbeat => {
            host.last_heartbeat_at = Some(now);
        }
        ClusterOperation::Put { bytes, .. } => {
            host.received_batches = host.received_batches.saturating_add(1);
            host.received_objects = host.received_objects.saturating_add(1);
            host.received_bytes = host.received_bytes.saturating_add(bytes.len() as u64);
        }
        ClusterOperation::BatchPut { items } => {
            host.received_batches = host.received_batches.saturating_add(1);
            host.received_objects = host.received_objects.saturating_add(items.len() as u64);
            host.received_bytes = host.received_bytes.saturating_add(
                items
                    .iter()
                    .map(|item| item.bytes.len() as u64)
                    .sum::<u64>(),
            );
        }
        ClusterOperation::Delete { .. } => {
            host.delete_requests = host.delete_requests.saturating_add(1);
        }
        _ => {}
    }
}

fn dispatch_operation(
    state: &MasterServerState,
    source_host: &str,
    operation: ClusterOperation,
) -> Result<ClusterValue> {
    match operation {
        ClusterOperation::Capabilities => Ok(ClusterValue::Capabilities(
            advertised_cluster_capabilities(source_host),
        )),
        ClusterOperation::Heartbeat => Ok(ClusterValue::Empty),
        ClusterOperation::Put {
            key,
            bytes,
            conditional,
        } => {
            validate_cluster_key(&key)?;
            let result = process_puts(
                state,
                source_host,
                vec![ClusterPutItem {
                    key,
                    bytes,
                    conditional,
                }],
            )?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("cluster put returned no result"))?;
            if conditional {
                Ok(ClusterValue::Bool(result.newly_stored))
            } else {
                Ok(ClusterValue::Empty)
            }
        }
        ClusterOperation::BatchPut { items } => {
            if items.len() > DEFAULT_CLUSTER_MAX_BATCH_OBJECTS {
                bail!("cluster batch contains too many objects: {}", items.len());
            }
            let payload_bytes = items
                .iter()
                .try_fold(0_usize, |total, item| total.checked_add(item.bytes.len()))
                .ok_or_else(|| anyhow!("cluster batch payload size overflow"))?;
            if payload_bytes > DEFAULT_CLUSTER_MAX_BATCH_BYTES {
                bail!(
                    "cluster batch payload is too large: {payload_bytes} bytes (limit {})",
                    DEFAULT_CLUSTER_MAX_BATCH_BYTES
                );
            }
            for item in &items {
                validate_cluster_key(&item.key)?;
            }
            Ok(ClusterValue::BatchPut(process_puts(
                state,
                source_host,
                items,
            )?))
        }
        ClusterOperation::Get { key } => {
            validate_cluster_key(&key)?;
            master_get_optional(state, &key)?
                .map(ClusterValue::Bytes)
                .ok_or_else(|| anyhow!("cluster object does not exist: {key}"))
        }
        ClusterOperation::GetOptional { key } => {
            validate_cluster_key(&key)?;
            Ok(ClusterValue::OptionalBytes(master_get_optional(
                state, &key,
            )?))
        }
        ClusterOperation::GetRange { key, start, len } => {
            validate_cluster_key(&key)?;
            if key_is_logically_deleted(state, &key)? {
                bail!("cluster object does not exist: {key}");
            }
            if state.store.exists(&key)? {
                Ok(ClusterValue::Bytes(
                    state.store.get_range(&key, start, len)?,
                ))
            } else {
                let _permit = state.backend_gate.acquire(BackendPriority::Foreground)?;
                Ok(ClusterValue::Bytes(
                    state.backend.get_range(&key, start, len)?,
                ))
            }
        }
        ClusterOperation::Exists { key } => {
            validate_cluster_key(&key)?;
            if key_is_logically_deleted(state, &key)? {
                return Ok(ClusterValue::Bool(false));
            }
            if state.store.exists(&key)? {
                Ok(ClusterValue::Bool(true))
            } else {
                let _permit = state.backend_gate.acquire(BackendPriority::Foreground)?;
                Ok(ClusterValue::Bool(state.backend.exists(&key)?))
            }
        }
        ClusterOperation::BackendCommitted { keys } => {
            if keys.len() > DEFAULT_CLUSTER_MAX_BATCH_OBJECTS {
                bail!("backend commit照会のkey数が上限を超えました");
            }
            let _guard = state
                .durable_lock
                .lock()
                .map_err(|_| anyhow!("master durable store lock poisoned"))?;
            let mut committed = Vec::new();
            for key in keys {
                validate_cluster_key(&key)?;
                if backend_queue_path(state, &key).exists()
                    || master_write_intent_path(state, &key).exists()
                    || key_is_logically_deleted(state, &key)?
                {
                    continue;
                }
                if let Ok(bytes) = fs::read(backend_receipt_path(state, &key)) {
                    let receipt: BackendPutReceipt = serde_json::from_slice(&bytes)?;
                    let master_bytes = state.store.get_optional(&key)?;
                    if receipt.key == key
                        && master_bytes.is_some_and(|bytes| {
                            blake3::hash(&bytes).to_hex().as_str() == receipt.digest
                        })
                    {
                        committed.push(key);
                    }
                }
            }
            Ok(ClusterValue::Strings(committed))
        }
        ClusterOperation::List { prefix } => {
            validate_cluster_prefix(&prefix)?;
            let cache_key = format!("keys:{prefix}");
            if let Some(CachedList::Strings(keys)) = read_list_cache(state, &cache_key) {
                return Ok(ClusterValue::Strings(keys));
            }
            let keys = master_list(state, &prefix)?;
            write_list_cache(state, cache_key, CachedList::Strings(keys.clone()));
            Ok(ClusterValue::Strings(keys))
        }
        ClusterOperation::ListCommonPrefixes { prefix } => {
            validate_cluster_prefix(&prefix)?;
            let cache_key = format!("prefixes:{prefix}");
            if let Some(CachedList::Strings(prefixes)) = read_list_cache(state, &cache_key) {
                return Ok(ClusterValue::Strings(prefixes));
            }
            let prefixes = master_list_common_prefixes(state, &prefix)?;
            write_list_cache(state, cache_key, CachedList::Strings(prefixes.clone()));
            Ok(ClusterValue::Strings(prefixes))
        }
        ClusterOperation::ListWithSizes { prefix } => {
            validate_cluster_prefix(&prefix)?;
            let cache_key = format!("sizes:{prefix}");
            if let Some(CachedList::Objects(objects)) = read_list_cache(state, &cache_key) {
                return Ok(ClusterValue::Objects(objects));
            }
            let objects = master_list_with_sizes(state, &prefix)?;
            write_list_cache(state, cache_key, CachedList::Objects(objects.clone()));
            Ok(ClusterValue::Objects(objects))
        }
        ClusterOperation::ListMasterLocalWithSizes { prefix } => {
            validate_cluster_prefix(&prefix)?;
            let deleted = logically_deleted_keys(state, &prefix)?;
            Ok(ClusterValue::Objects(
                state
                    .store
                    .list_with_sizes(&prefix)?
                    .into_iter()
                    .filter(|object| !deleted.contains(&object.key))
                    .collect(),
            ))
        }
        ClusterOperation::Delete { key } => {
            validate_cluster_key(&key)?;
            durable_delete(state, source_host, &key)?;
            clear_list_cache(state);
            Ok(ClusterValue::Empty)
        }
        ClusterOperation::RestoreArchive { key, days, tier } => {
            validate_cluster_key(&key)?;
            let _permit = state.backend_gate.acquire(BackendPriority::Foreground)?;
            Ok(ClusterValue::Bool(
                state.backend.restore_archive(&key, days, &tier)?,
            ))
        }
        ClusterOperation::ApplyLifecycle { policy } => {
            let _permit = state.backend_gate.acquire(BackendPriority::Foreground)?;
            Ok(ClusterValue::Bool(
                state.backend.apply_s3_lifecycle_policy(&policy)?,
            ))
        }
    }
}

fn master_get_optional(state: &MasterServerState, key: &str) -> Result<Option<Vec<u8>>> {
    if key_is_logically_deleted(state, key)? {
        return Ok(None);
    }
    if let Some(bytes) = state.store.get_optional(key)? {
        return Ok(Some(bytes));
    }
    let bytes = {
        let _permit = state.backend_gate.acquire(BackendPriority::Foreground)?;
        state.backend.get_optional(key)?
    };
    if let Some(bytes) = &bytes {
        let _guard = state
            .durable_lock
            .lock()
            .map_err(|_| anyhow!("master durable store lock poisoned"))?;
        if !state.store.exists(key)? {
            let len = bytes.len() as u64;
            if enforce_store_quota(state, 0, len).is_ok() && {
                ensure_store_key_parent(state, key)?;
                state.store.put_if_absent(key, bytes)?
            } {
                update_store_bytes(state, 0, len);
            }
        }
    }
    Ok(bytes)
}

fn master_list(state: &MasterServerState, prefix: &str) -> Result<Vec<String>> {
    let deleted = logically_deleted_keys(state, prefix)?;
    let mut keys = state
        .store
        .list(prefix)?
        .into_iter()
        .filter(|key| !deleted.contains(key))
        .collect::<BTreeSet<_>>();
    let _permit = state.backend_gate.acquire(BackendPriority::Foreground)?;
    match state.backend.list(prefix) {
        Ok(backend_keys) => keys.extend(
            backend_keys
                .into_iter()
                .filter(|key| !deleted.contains(key)),
        ),
        Err(err) => eprintln!(
            "cluster backend LIST fallback unavailable prefix={prefix}: {err:#}; using master-local catalog"
        ),
    }
    Ok(keys.into_iter().collect())
}

fn master_list_common_prefixes(state: &MasterServerState, prefix: &str) -> Result<Vec<String>> {
    let mut prefixes = state
        .store
        .list_common_prefixes(prefix)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let _permit = state.backend_gate.acquire(BackendPriority::Foreground)?;
    match state.backend.list_common_prefixes(prefix) {
        Ok(backend_prefixes) => prefixes.extend(backend_prefixes),
        Err(err) => eprintln!(
            "cluster backend common-prefix fallback unavailable prefix={prefix}: {err:#}; using master-local catalog"
        ),
    }
    Ok(prefixes.into_iter().collect())
}

fn master_list_with_sizes(
    state: &MasterServerState,
    prefix: &str,
) -> Result<Vec<RemoteObjectStat>> {
    let deleted = logically_deleted_keys(state, prefix)?;
    let _permit = state.backend_gate.acquire(BackendPriority::Foreground)?;
    let mut objects = match state.backend.list_with_sizes(prefix) {
        Ok(backend_objects) => backend_objects
            .into_iter()
            .filter(|object| !deleted.contains(&object.key))
            .map(|object| (object.key.clone(), object))
            .collect::<BTreeMap<_, _>>(),
        Err(err) => {
            eprintln!(
                "cluster backend sized-LIST fallback unavailable prefix={prefix}: {err:#}; using master-local catalog"
            );
            BTreeMap::new()
        }
    };
    drop(_permit);
    for object in state
        .store
        .list_with_sizes(prefix)?
        .into_iter()
        .filter(|object| !deleted.contains(&object.key))
    {
        objects.insert(object.key.clone(), object);
    }
    Ok(objects.into_values().collect())
}

fn cluster_list_cache_ttl() -> Option<Duration> {
    let secs = env::var("MAJUTSU_CLUSTER_LIST_CACHE_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_CLUSTER_LIST_CACHE_TTL_SECS);
    (secs > 0).then_some(Duration::from_secs(secs))
}

fn read_list_cache(state: &MasterServerState, key: &str) -> Option<CachedList> {
    let ttl = cluster_list_cache_ttl()?;
    let mut cache = state.list_cache.lock().ok()?;
    let (created, value) = cache.get(key)?.clone();
    if created.elapsed() < ttl {
        Some(value)
    } else {
        cache.remove(key);
        None
    }
}

fn write_list_cache(state: &MasterServerState, key: String, value: CachedList) {
    if cluster_list_cache_ttl().is_none() {
        return;
    }
    if let Ok(mut cache) = state.list_cache.lock() {
        cache.insert(key, (Instant::now(), value));
    }
}

fn clear_list_cache(state: &MasterServerState) {
    if let Ok(mut cache) = state.list_cache.lock() {
        cache.clear();
    }
}

fn update_list_cache_after_puts(state: &MasterServerState, items: &[ClusterPutItem]) {
    if cluster_list_cache_ttl().is_none() {
        return;
    }
    let Ok(mut cache) = state.list_cache.lock() else {
        return;
    };
    let stale_prefix_cache_keys = cache
        .keys()
        .filter(|key| key.starts_with("prefixes:"))
        .cloned()
        .collect::<Vec<_>>();
    for key in stale_prefix_cache_keys {
        cache.remove(&key);
    }

    for (cache_key, (_, value)) in cache.iter_mut() {
        let Some(prefix) = cache_key
            .strip_prefix("keys:")
            .or_else(|| cache_key.strip_prefix("sizes:"))
        else {
            continue;
        };
        for item in items {
            if !item.key.starts_with(prefix) {
                continue;
            }
            match value {
                CachedList::Strings(keys) => {
                    if !keys.iter().any(|key| key == &item.key) {
                        keys.push(item.key.clone());
                    }
                }
                CachedList::Objects(objects) => {
                    if let Some(object) = objects.iter_mut().find(|object| object.key == item.key) {
                        object.size = item.bytes.len() as u64;
                    } else {
                        objects.push(RemoteObjectStat {
                            key: item.key.clone(),
                            size: item.bytes.len() as u64,
                        });
                    }
                }
            }
        }
        match value {
            CachedList::Strings(keys) => keys.sort(),
            CachedList::Objects(objects) => {
                objects.sort_by(|left, right| left.key.cmp(&right.key));
            }
        }
    }
}

fn process_puts(
    state: &MasterServerState,
    source_host: &str,
    items: Vec<ClusterPutItem>,
) -> Result<Vec<ClusterPutResult>> {
    let _guard = state
        .durable_lock
        .lock()
        .map_err(|_| anyhow!("master durable store lock poisoned"))?;
    let mut results = Vec::with_capacity(items.len());
    let mut newly_stored_objects = 0_u64;
    let mut duplicate_objects = 0_u64;
    let mut queue_reused_objects = 0_u64;
    let mut same_key_churn = 0_u64;
    for item in &items {
        recover_master_write_intent_for_key(state, &item.key)?;
        let was_tombstoned = local_gc_path(state, &item.key).exists();
        let digest = blake3::hash(&item.bytes).to_hex().to_string();
        let existing = state.store.get_optional(&item.key)?;
        let existing_len = existing.as_ref().map_or(0, |bytes| bytes.len() as u64);
        let changed = existing
            .as_ref()
            .is_none_or(|bytes| blake3::hash(bytes).to_hex().as_str() != digest);
        if item.conditional && changed && existing.is_some() {
            bail!("cluster immutable object digest collision for {}", item.key);
        }
        let needs_backup = changed
            || was_tombstoned
            || backend_queue_path(state, &item.key).exists()
            || !backend_receipt_matches(state, &item.key, &digest)?;
        if needs_backup {
            enforce_store_quota(state, existing_len, item.bytes.len() as u64)?;
            crate::atomic_io::write_atomic(
                &master_write_intent_path(state, &item.key),
                &serde_cbor::to_vec(&MasterWriteIntent {
                    version: 1,
                    source_host: source_host.to_string(),
                    item: item.clone(),
                })?,
            )?;
        }
        let newly_stored = if item.conditional {
            match existing {
                Some(existing) => {
                    let existing_digest = blake3::hash(&existing).to_hex().to_string();
                    if existing_digest != digest {
                        bail!(
                            "cluster immutable object digest collision for {}: stored={} incoming={}",
                            item.key,
                            existing_digest,
                            digest
                        );
                    }
                    false
                }
                None => {
                    enforce_store_quota(state, 0, item.bytes.len() as u64)?;
                    ensure_store_key_parent(state, &item.key)?;
                    state.store.put_if_absent(&item.key, &item.bytes)?
                }
            }
        } else {
            let incoming_len = item.bytes.len() as u64;
            enforce_store_quota(state, existing_len, incoming_len)?;
            let unchanged = existing
                .as_ref()
                .is_some_and(|bytes| blake3::hash(bytes).to_hex().as_str() == digest);
            if !unchanged {
                ensure_store_key_parent(state, &item.key)?;
                state.store.put(&item.key, &item.bytes)?;
            }
            !unchanged
        };
        if newly_stored {
            update_store_bytes(state, existing_len, item.bytes.len() as u64);
            newly_stored_objects = newly_stored_objects.saturating_add(1);
        } else {
            duplicate_objects = duplicate_objects.saturating_add(1);
        }
        if was_tombstoned {
            cancel_local_gc(state, &item.key)?;
        }
        if needs_backup {
            match enqueue_backend_put(state, source_host, item, digest)? {
                BackendEnqueueOutcome::Created => {}
                BackendEnqueueOutcome::Reused => {
                    queue_reused_objects = queue_reused_objects.saturating_add(1);
                }
                BackendEnqueueOutcome::Replaced => {
                    same_key_churn = same_key_churn.saturating_add(1);
                }
            }
            remove_backend_queue_file(&master_write_intent_path(state, &item.key))?;
        }
        results.push(ClusterPutResult {
            master_durable: true,
            newly_stored,
            stored_bytes: if newly_stored {
                item.bytes.len() as u64
            } else {
                0
            },
        });
    }
    drop(_guard);
    if let Ok(mut metrics) = state.metrics.lock() {
        let host = metrics.hosts.entry(source_host.to_string()).or_default();
        host.newly_stored_objects = host
            .newly_stored_objects
            .saturating_add(newly_stored_objects);
        host.duplicate_objects = host.duplicate_objects.saturating_add(duplicate_objects);
        host.queue_reused_objects = host
            .queue_reused_objects
            .saturating_add(queue_reused_objects);
        host.same_key_churn = host.same_key_churn.saturating_add(same_key_churn);
    }
    update_list_cache_after_puts(state, &items);
    if let Err(err) = write_master_status_throttled(state) {
        eprintln!("cluster master status update failed after durable PUT: {err:#}");
    }
    Ok(results)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackendEnqueueOutcome {
    Created,
    Reused,
    Replaced,
}

fn enforce_store_quota(state: &MasterServerState, old_len: u64, new_len: u64) -> Result<()> {
    if new_len <= old_len {
        return Ok(());
    }
    let Some(limit) = state.store_max_bytes else {
        return Ok(());
    };
    let current = state.store_bytes.load(Ordering::Relaxed);
    let projected = current.saturating_sub(old_len).saturating_add(new_len);
    if projected > limit {
        if let Ok(mut metrics) = state.metrics.lock() {
            metrics.store_quota_rejections_total =
                metrics.store_quota_rejections_total.saturating_add(1);
            metrics.last_store_quota_rejection_at = Some(Utc::now());
        }
        bail!(
            "cluster master store quota exceeded: projected={projected} limit={limit} current={current}"
        );
    }
    Ok(())
}

fn update_store_bytes(state: &MasterServerState, old_len: u64, new_len: u64) {
    let _ = state
        .store_bytes
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(old_len).saturating_add(new_len))
        });
}

fn ensure_store_key_parent(state: &MasterServerState, key: &str) -> Result<()> {
    validate_cluster_key(key)?;
    let path = state.store_dir.join(key);
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("cluster store key has no parent: {key}"))?;
    create_dir_all_durable(parent)
}

fn create_dir_all_durable(path: &Path) -> Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    let mut missing = Vec::new();
    let mut current = path;
    while !current.exists() {
        missing.push(current.to_path_buf());
        current = current
            .parent()
            .ok_or_else(|| anyhow!("directory has no existing ancestor: {}", path.display()))?;
    }
    if !current.is_dir() {
        bail!(
            "directory ancestor is not a directory: {}",
            current.display()
        );
    }
    for directory in missing.into_iter().rev() {
        match fs::create_dir(&directory) {
            Ok(()) => crate::atomic_io::fsync_parent_dir(&directory)?,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => {
                return Err(err).with_context(|| format!("create {}", directory.display()));
            }
        }
    }
    Ok(())
}

fn durable_delete(state: &MasterServerState, source_host: &str, key: &str) -> Result<()> {
    let _guard = state
        .durable_lock
        .lock()
        .map_err(|_| anyhow!("master durable store lock poisoned"))?;
    recover_master_write_intent_for_key(state, key)?;
    enqueue_backend_delete(state, source_host, key)?;
    drop(_guard);
    if let Err(err) = write_master_status_throttled(state) {
        eprintln!("cluster master status update failed after durable DELETE: {err:#}");
    }
    Ok(())
}

fn enqueue_backend_put(
    state: &MasterServerState,
    source_host: &str,
    item: &ClusterPutItem,
    digest: String,
) -> Result<BackendEnqueueOutcome> {
    write_backend_queue_item(
        state,
        source_host,
        &item.key,
        BackendQueueOperation::Put {
            digest,
            size: item.bytes.len() as u64,
            conditional: item.conditional,
        },
    )
}

fn master_write_intent_path(state: &MasterServerState, key: &str) -> PathBuf {
    state
        .write_intent_dir
        .join(format!("{}.cbor", blake3::hash(key.as_bytes()).to_hex()))
}

fn backend_receipt_path(state: &MasterServerState, key: &str) -> PathBuf {
    state
        .backend_receipt_dir
        .join(format!("{}.json", blake3::hash(key.as_bytes()).to_hex()))
}

fn backend_receipt_matches(state: &MasterServerState, key: &str, digest: &str) -> Result<bool> {
    let bytes = match fs::read(backend_receipt_path(state, key)) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err.into()),
    };
    let receipt: BackendPutReceipt = serde_json::from_slice(&bytes)?;
    Ok(receipt.key == key && receipt.digest == digest)
}

fn record_backend_put_receipt(state: &MasterServerState, item: &BackendQueueItem) -> Result<()> {
    if let BackendQueueOperation::Put { digest, .. } = &item.operation {
        crate::atomic_io::write_atomic(
            &backend_receipt_path(state, &item.key),
            &serde_json::to_vec(&BackendPutReceipt {
                key: item.key.clone(),
                digest: digest.clone(),
            })?,
        )?;
    } else {
        remove_backend_queue_file(&backend_receipt_path(state, &item.key))?;
    }
    Ok(())
}

// 呼出側がdurable_lockを保持する。失敗時はintentを消さず、次回／起動時に再生する。
fn recover_master_write_intent_for_key(state: &MasterServerState, key: &str) -> Result<()> {
    let path = master_write_intent_path(state, key);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    let intent: MasterWriteIntent = serde_cbor::from_slice(&bytes)?;
    if intent.version != 1 || intent.item.key != key {
        bail!(
            "master write intentの世代またはkeyが不正: {}",
            path.display()
        );
    }
    validate_cluster_key(key)?;
    let old = state.store.get_optional(key)?;
    let digest = blake3::hash(&intent.item.bytes).to_hex().to_string();
    let unchanged = old
        .as_ref()
        .is_some_and(|bytes| blake3::hash(bytes).to_hex().as_str() == digest);
    if !unchanged {
        if intent.item.conditional && old.is_some() {
            bail!("immutable intentとstoreの内容が競合: {key}");
        }
        enforce_store_quota(
            state,
            old.as_ref().map_or(0, |bytes| bytes.len() as u64),
            intent.item.bytes.len() as u64,
        )?;
        ensure_store_key_parent(state, key)?;
        state.store.put(key, &intent.item.bytes)?;
        update_store_bytes(
            state,
            old.as_ref().map_or(0, |v| v.len() as u64),
            intent.item.bytes.len() as u64,
        );
    }
    enqueue_backend_put(state, &intent.source_host, &intent.item, digest)?;
    cancel_local_gc(state, key)?;
    remove_backend_queue_file(&path)
}

fn recover_master_write_intents(state: &MasterServerState) -> Result<()> {
    let _guard = state
        .durable_lock
        .lock()
        .map_err(|_| anyhow!("master durable store lock poisoned"))?;
    for entry in fs::read_dir(&state.write_intent_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|v| v.to_str()) != Some("cbor") {
            continue;
        }
        let intent: MasterWriteIntent = serde_cbor::from_slice(&fs::read(&path)?)?;
        if master_write_intent_path(state, &intent.item.key) != path {
            bail!(
                "master write intentのファイル名がkeyと不一致: {}",
                path.display()
            );
        }
        recover_master_write_intent_for_key(state, &intent.item.key)?;
    }
    Ok(())
}

fn reconcile_legacy_backend_queue(state: &MasterServerState) -> Result<()> {
    let marker = state.backend_receipt_dir.join("migration-v1.complete");
    if marker.exists() {
        return Ok(());
    }
    let _guard = state
        .durable_lock
        .lock()
        .map_err(|_| anyhow!("master durable store lock poisoned"))?;
    // 旧版の「storeだけ残りqueueなし」を一度走査する。削除済みkeyは復活させない。
    for key in state.store.list("")? {
        if key.split('/').any(|part| part.starts_with('.'))
            || backend_queue_path(state, &key).exists()
            || backend_receipt_path(state, &key).exists()
            || local_gc_path(state, &key).exists()
        {
            continue;
        }
        validate_cluster_key(&key)?;
        let bytes = state.store.get(&key)?;
        let digest = blake3::hash(&bytes).to_hex().to_string();
        let host = key.split('/').next().unwrap_or("unknown-host");
        write_backend_queue_item(
            state,
            host,
            &key,
            BackendQueueOperation::Put {
                digest,
                size: bytes.len() as u64,
                conditional: backend_publish_priority(&key) == 1,
            },
        )?;
    }
    crate::atomic_io::write_atomic(&marker, b"1")
}

fn enqueue_backend_delete(state: &MasterServerState, source_host: &str, key: &str) -> Result<()> {
    write_backend_queue_item(state, source_host, key, BackendQueueOperation::Delete).map(|_| ())
}

fn write_backend_queue_item(
    state: &MasterServerState,
    source_host: &str,
    key: &str,
    operation: BackendQueueOperation,
) -> Result<BackendEnqueueOutcome> {
    let now = Utc::now();
    let path = backend_queue_path(state, key);
    let existing = read_backend_queue_item_optional(&path)?;
    if existing
        .as_ref()
        .is_some_and(|item| item.operation == operation)
    {
        return Ok(BackendEnqueueOutcome::Reused);
    }
    let outcome = if existing.is_some() {
        BackendEnqueueOutcome::Replaced
    } else {
        BackendEnqueueOutcome::Created
    };
    let item = BackendQueueItem {
        version: BACKEND_QUEUE_VERSION,
        generation: Uuid::new_v4().to_string(),
        key: key.to_string(),
        source_host: source_host.to_string(),
        operation,
        created_at: existing.as_ref().map_or(now, |item| item.created_at),
        updated_at: now,
        attempts: 0,
        retry_after: None,
    };
    crate::atomic_io::write_atomic(&path, &serde_json::to_vec_pretty(&item)?)?;
    Ok(outcome)
}

fn backend_queue_path(state: &MasterServerState, key: &str) -> PathBuf {
    let id = blake3::hash(key.as_bytes()).to_hex();
    state.backend_queue_dir.join(format!("{id}.json"))
}

fn local_gc_path(state: &MasterServerState, key: &str) -> PathBuf {
    let id = blake3::hash(key.as_bytes()).to_hex();
    state.local_gc_dir.join(format!("{id}.json"))
}

fn read_local_gc_item_optional(path: &Path) -> Result<Option<LocalGcItem>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
            format!("decode cluster local GC item {}", path.display())
        })?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
    }
}

fn local_gc_items(state: &MasterServerState) -> Result<Vec<(PathBuf, LocalGcItem)>> {
    let mut items = Vec::new();
    for entry in fs::read_dir(&state.local_gc_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_file()
            || path.extension().and_then(|value| value.to_str()) != Some("json")
        {
            continue;
        }
        // status／workerが同時に掃除するため、一覧取得後の消失は正常な競合として扱う。
        // 破損JSONや不正versionは従来どおりエラーにして、監視値を不明にする。
        let Some(item) = read_local_gc_item_optional(&path)? else {
            continue;
        };
        if item.version != LOCAL_GC_VERSION {
            bail!(
                "unsupported cluster local GC version {} in {}",
                item.version,
                path.display()
            );
        }
        if local_gc_path(state, &item.key) != path {
            bail!(
                "cluster local GC key does not match filename: {}",
                path.display()
            );
        }
        items.push((path, item));
    }
    items.sort_by(|left, right| {
        left.1
            .delete_after
            .cmp(&right.1.delete_after)
            .then_with(|| left.1.key.cmp(&right.1.key))
    });
    Ok(items)
}

fn local_gc_stats(state: &MasterServerState) -> Result<LocalGcStats> {
    let mut stats = LocalGcStats::default();
    for (_, item) in local_gc_items(state)? {
        stats.objects += 1;
        stats.bytes = stats.bytes.saturating_add(item.size);
        stats.oldest_deleted_from_backend_at = stats
            .oldest_deleted_from_backend_at
            .map(|current| current.min(item.deleted_from_backend_at))
            .or(Some(item.deleted_from_backend_at));
        stats.next_delete_after = stats
            .next_delete_after
            .map(|current| current.min(item.delete_after))
            .or(Some(item.delete_after));
    }
    Ok(stats)
}

fn local_delete_grace() -> Duration {
    Duration::from_secs(env_u64(
        "MAJUTSU_CLUSTER_LOCAL_DELETE_GRACE_SECS",
        DEFAULT_LOCAL_DELETE_GRACE_SECS,
    ))
}

fn key_is_logically_deleted(state: &MasterServerState, key: &str) -> Result<bool> {
    if read_backend_queue_item_optional(&backend_queue_path(state, key))?
        .is_some_and(|item| item.operation == BackendQueueOperation::Delete)
    {
        return Ok(true);
    }
    Ok(local_gc_path(state, key).exists())
}

fn logically_deleted_keys(state: &MasterServerState, prefix: &str) -> Result<BTreeSet<String>> {
    let mut keys = backend_queue_items(state)?
        .into_iter()
        .filter(|(_, item)| {
            item.key.starts_with(prefix) && item.operation == BackendQueueOperation::Delete
        })
        .map(|(_, item)| item.key)
        .collect::<BTreeSet<_>>();
    keys.extend(
        local_gc_items(state)?
            .into_iter()
            .filter(|(_, item)| item.key.starts_with(prefix))
            .map(|(_, item)| item.key),
    );
    Ok(keys)
}

fn cancel_local_gc(state: &MasterServerState, key: &str) -> Result<bool> {
    let path = local_gc_path(state, key);
    if !path.exists() {
        return Ok(false);
    }
    remove_backend_queue_file(&path)?;
    Ok(true)
}

fn retain_local_after_backend_delete(
    state: &MasterServerState,
    item: &BackendQueueItem,
) -> Result<()> {
    let Some(bytes) = state.store.get_optional(&item.key)? else {
        let _ = remove_backend_queue_file(&local_gc_path(state, &item.key));
        return Ok(());
    };
    let now = Utc::now();
    let grace =
        ChronoDuration::from_std(local_delete_grace()).unwrap_or_else(|_| ChronoDuration::days(7));
    let retained = LocalGcItem {
        version: LOCAL_GC_VERSION,
        generation: Uuid::new_v4().to_string(),
        key: item.key.clone(),
        source_host: item.source_host.clone(),
        digest: blake3::hash(&bytes).to_hex().to_string(),
        size: bytes.len() as u64,
        deleted_from_backend_at: now,
        delete_after: now + grace,
    };
    crate::atomic_io::write_atomic(
        &local_gc_path(state, &item.key),
        &serde_json::to_vec_pretty(&retained)?,
    )
}

fn run_local_gc(state: &MasterServerState) -> Result<usize> {
    let now = Utc::now();
    let mut removed = 0usize;
    for (path, item) in local_gc_items(state)? {
        if item.delete_after > now {
            continue;
        }
        let _guard = state
            .durable_lock
            .lock()
            .map_err(|_| anyhow!("master durable store lock poisoned"))?;
        let Some(current) = read_local_gc_item_optional(&path)? else {
            continue;
        };
        if current.generation != item.generation || backend_queue_path(state, &item.key).exists() {
            continue;
        }
        if let Some(bytes) = state.store.get_optional(&item.key)? {
            let digest = blake3::hash(&bytes).to_hex().to_string();
            if digest == item.digest {
                state.store.delete(&item.key)?;
                crate::atomic_io::fsync_parent_dir(&state.store_dir.join(&item.key))?;
                update_store_bytes(state, bytes.len() as u64, 0);
                removed += 1;
            }
        }
        remove_backend_queue_file(&path)?;
    }
    Ok(removed)
}

fn read_backend_queue_item_optional(path: &Path) -> Result<Option<BackendQueueItem>> {
    match fs::read(path) {
        Ok(bytes) => {
            let mut item: BackendQueueItem = serde_json::from_slice(&bytes)
                .with_context(|| format!("decode cluster backend queue item {}", path.display()))?;
            normalize_legacy_backend_source_host(&mut item);
            Ok(Some(item))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
    }
}

fn normalize_legacy_backend_source_host(item: &mut BackendQueueItem) {
    let Some(key_host) = item.key.split('/').next().filter(|host| !host.is_empty()) else {
        return;
    };
    let canonical = |host: &str| {
        host.chars()
            .filter(|character| character.is_ascii_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect::<String>()
    };
    if item.source_host == "unknown-host" || canonical(&item.source_host) == canonical(key_host) {
        item.source_host = key_host.to_string();
    }
}

fn backend_queue_items(state: &MasterServerState) -> Result<Vec<(PathBuf, BackendQueueItem)>> {
    let mut items = Vec::new();
    for entry in fs::read_dir(&state.backend_queue_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_file()
            || path.extension().and_then(|value| value.to_str()) != Some("json")
        {
            continue;
        }
        // PUT／DELETEの完了処理が一覧取得と同時にqueueを消せる。消失は完了済みの
        // 競合として読み飛ばし、読取り不能やkey/version不整合だけをcatalog errorにする。
        let Some(item) = read_backend_queue_item_optional(&path)? else {
            continue;
        };
        if item.version != BACKEND_QUEUE_VERSION {
            bail!(
                "unsupported cluster backend queue version {} in {}",
                item.version,
                path.display()
            );
        }
        if backend_queue_path(state, &item.key) != path {
            bail!(
                "cluster backend queue key does not match filename: {}",
                path.display()
            );
        }
        items.push((path, item));
    }
    items.sort_by(|left, right| {
        backend_publish_priority(&left.1.key)
            .cmp(&backend_publish_priority(&right.1.key))
            .then_with(|| left.1.created_at.cmp(&right.1.created_at))
            .then_with(|| left.1.key.cmp(&right.1.key))
    });
    Ok(items)
}

fn backend_queue_stats(state: &MasterServerState) -> Result<BackendQueueStats> {
    let mut stats = BackendQueueStats::default();
    for (_, item) in backend_queue_items(state)? {
        stats.objects += 1;
        if let BackendQueueOperation::Put { size, .. } = item.operation {
            stats.bytes = stats.bytes.saturating_add(size);
        }
        stats.oldest_created_at = stats
            .oldest_created_at
            .map(|current| current.min(item.created_at))
            .or(Some(item.created_at));
        if item.attempts > 0 {
            stats.retrying += 1;
        }
    }
    Ok(stats)
}

fn backend_publish_priority(key: &str) -> u8 {
    if key.starts_with("refs/")
        || key.contains("/refs/")
        || key.ends_with("/current")
        || key.ends_with("/head.cbor.zst.enc")
    {
        return 3;
    }
    if key.contains("/journal/")
        || key.starts_with("journal/")
        || key.starts_with("metadata/")
        || key.ends_with("/metadata/export.json.zst")
        || key.ends_with("/metadata/export.json")
        || key == "metadata/export.json"
    {
        return 2;
    }
    1
}

fn backend_flush_interval() -> Duration {
    Duration::from_secs(env_u64(
        "MAJUTSU_CLUSTER_BACKEND_FLUSH_INTERVAL_SECS",
        DEFAULT_BACKEND_FLUSH_INTERVAL_SECS,
    ))
}

fn backend_poll_interval() -> Duration {
    Duration::from_secs(
        env_u64(
            "MAJUTSU_CLUSTER_BACKEND_POLL_INTERVAL_SECS",
            DEFAULT_BACKEND_POLL_INTERVAL_SECS,
        )
        .max(1),
    )
}

fn backend_flush_max_bytes() -> u64 {
    env_u64(
        "MAJUTSU_CLUSTER_BACKEND_FLUSH_MAX_BYTES",
        DEFAULT_BACKEND_FLUSH_MAX_BYTES,
    )
}

fn backend_flush_max_objects() -> usize {
    env::var("MAJUTSU_CLUSTER_BACKEND_FLUSH_MAX_OBJECTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BACKEND_FLUSH_MAX_OBJECTS)
}

fn backend_max_concurrency() -> usize {
    env::var("MAJUTSU_CLUSTER_BACKEND_MAX_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BACKEND_MAX_CONCURRENCY)
        .min(64)
}

fn backend_dedupe_list_min_objects() -> usize {
    env::var("MAJUTSU_CLUSTER_BACKEND_DEDUPE_LIST_MIN_OBJECTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BACKEND_DEDUPE_LIST_MIN_OBJECTS)
}

fn backend_delete_batch_max_objects() -> usize {
    env::var("MAJUTSU_CLUSTER_BACKEND_DELETE_BATCH_MAX_OBJECTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1000)
        .min(1000)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn backend_flush_due(stats: &BackendQueueStats, now: DateTime<Utc>) -> bool {
    if stats.objects == 0 {
        return false;
    }
    if stats.bytes >= backend_flush_max_bytes() {
        return true;
    }
    let interval = ChronoDuration::from_std(backend_flush_interval())
        .unwrap_or_else(|_| ChronoDuration::hours(1));
    stats
        .oldest_created_at
        .is_some_and(|created_at| now.signed_duration_since(created_at) >= interval)
}

fn start_backend_worker(state: Arc<MasterServerState>) -> Result<()> {
    thread::Builder::new()
        .name("mj-cluster-backend".into())
        .spawn(move || backend_worker_loop(state))
        .context("start cluster backend worker")?;
    Ok(())
}

fn backend_worker_loop(state: Arc<MasterServerState>) {
    loop {
        let result = run_backend_worker_once(&state);
        if let Err(err) = result {
            record_backend_error(&state, None, &err);
            eprintln!("cluster backend worker error: {err:#}");
            let _ = write_master_status(&state);
            if let Err(notice_error) = maybe_send_cluster_notice(&state) {
                eprintln!("cluster障害通知の評価に失敗: {notice_error:#}");
            }
        }
        thread::sleep(backend_poll_interval());
    }
}

fn run_backend_worker_once(state: &MasterServerState) -> Result<()> {
    recover_master_write_intents(state)?;
    let stats = backend_queue_stats(state)?;
    let force = state.flush_request_path.exists();
    if force || backend_flush_due(&stats, Utc::now()) {
        drain_backend_queue(state, force)?;
    }
    if force && backend_queue_stats(state)?.objects == 0 {
        remove_flush_request(&state.flush_request_path)?;
    }
    run_local_gc(state)?;
    write_master_status(state)?;
    if let Err(err) = maybe_send_cluster_notice(state) {
        eprintln!("cluster notice evaluation failed: {err:#}");
    }
    Ok(())
}

fn remove_flush_request(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => crate::atomic_io::fsync_parent_dir(path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("remove {}", path.display())),
    }
}

fn drain_backend_queue(state: &MasterServerState, force: bool) -> Result<usize> {
    let now = Utc::now();
    let stats = backend_queue_stats(state)?;
    if !force && !backend_flush_due(&stats, now) {
        return Ok(0);
    }
    let selected = backend_queue_items(state)?
        .into_iter()
        .filter(|(_, item)| {
            force
                || item
                    .retry_after
                    .is_none_or(|retry_after| retry_after <= now)
        })
        .take(backend_flush_max_objects())
        .collect::<Vec<_>>();
    let existing_backend_keys = list_existing_conditional_backend_keys(state, &selected);
    let mut completed = 0usize;
    let mut first_error = None;
    let mut delete_items = Vec::new();
    for (path, item) in selected {
        if item.operation == BackendQueueOperation::Delete {
            delete_items.push((path, item));
            continue;
        }
        let skip_existing = matches!(
            item.operation,
            BackendQueueOperation::Put {
                conditional: true,
                ..
            }
        ) && existing_backend_keys
            .get(&item.source_host)
            .is_some_and(|keys| keys.contains(&item.key));
        if skip_existing {
            if complete_backend_queue_item_without_upload(state, &path, &item)? {
                completed += 1;
                record_backend_success(state, &item, true);
            }
            continue;
        }
        match upload_backend_queue_item(state, &path, &item) {
            Ok(true) => {
                completed += 1;
                record_backend_success(state, &item, false);
            }
            Ok(false) => {}
            Err(err) => {
                record_backend_queue_failure(state, &path, &item, &err)?;
                record_backend_error(state, Some(&item.source_host), &err);
                write_master_status(state)?;
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }
    completed = completed.saturating_add(upload_backend_delete_items(state, &delete_items)?);
    write_master_status(state)?;
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(completed)
}

fn upload_backend_delete_items(
    state: &MasterServerState,
    items: &[(PathBuf, BackendQueueItem)],
) -> Result<usize> {
    if items.is_empty() {
        return Ok(0);
    }
    let batch_max = backend_delete_batch_max_objects();
    if batch_max == 0
        || items.len() == 1
        || !state.backend_batch_delete_enabled.load(Ordering::Relaxed)
    {
        return upload_backend_delete_items_individually(state, items);
    }

    let mut completed = 0usize;
    for chunk in items.chunks(batch_max) {
        if !state.backend_batch_delete_enabled.load(Ordering::Relaxed) {
            completed =
                completed.saturating_add(upload_backend_delete_items_individually(state, chunk)?);
            continue;
        }
        let active = {
            let _guard = state
                .durable_lock
                .lock()
                .map_err(|_| anyhow!("master durable store lock poisoned"))?;
            let mut active = Vec::new();
            let pending_hosts = backend_queue_items(state)?
                .into_iter()
                .filter(|(_, item)| matches!(item.operation, BackendQueueOperation::Put { .. }))
                .map(|(_, item)| item.source_host)
                .collect::<BTreeSet<_>>();
            for (path, item) in chunk {
                if pending_hosts.contains(&item.source_host) {
                    continue;
                }
                let Some(current) = read_backend_queue_item_optional(path)? else {
                    continue;
                };
                if current.generation == item.generation
                    && current.operation == BackendQueueOperation::Delete
                {
                    active.push((path.clone(), item.clone()));
                }
            }
            active
        };
        if active.is_empty() {
            continue;
        }
        let keys = active
            .iter()
            .map(|(_, item)| item.key.clone())
            .collect::<Vec<_>>();
        let batch_result = (|| {
            let _permit = state.backend_gate.acquire(BackendPriority::Background)?;
            state.backend.delete_batch(&keys)
        })();
        let results = match batch_result {
            Ok(results) => results
                .into_iter()
                .map(|result| (result.key, result.error))
                .collect::<BTreeMap<_, _>>(),
            Err(err) if err.is::<crate::remote_store::BatchDeleteUnsupported>() => {
                state
                    .backend_batch_delete_enabled
                    .store(false, Ordering::Relaxed);
                eprintln!(
                    "cluster backend multi-object delete unavailable: {err:#}; using individual DELETE for this process"
                );
                completed = completed
                    .saturating_add(upload_backend_delete_items_individually(state, &active)?);
                continue;
            }
            Err(err) => {
                // 一時障害では個別DELETEへ増幅せず、batch能力を保ったまま再試行する。
                for (path, item) in &active {
                    record_backend_queue_failure(state, path, item, &err)?;
                    record_backend_error(state, Some(&item.source_host), &err);
                }
                return Err(err);
            }
        };
        let mut first_error = None::<anyhow::Error>;
        for (path, item) in active {
            match results.get(&item.key) {
                Some(None) => {
                    if complete_uploaded_backend_queue_item(state, &path, &item)? {
                        completed += 1;
                        record_backend_success(state, &item, false);
                    }
                }
                Some(Some(message)) => {
                    let err = anyhow!(
                        "backend multi-object delete failed for {}: {message}",
                        item.key
                    );
                    record_backend_queue_failure(state, &path, &item, &err)?;
                    record_backend_error(state, Some(&item.source_host), &err);
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                None => {
                    let err = anyhow!(
                        "backend multi-object delete omitted result for {}",
                        item.key
                    );
                    record_backend_queue_failure(state, &path, &item, &err)?;
                    record_backend_error(state, Some(&item.source_host), &err);
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
    }
    Ok(completed)
}

fn upload_backend_delete_items_individually(
    state: &MasterServerState,
    items: &[(PathBuf, BackendQueueItem)],
) -> Result<usize> {
    let mut completed = 0usize;
    for (path, item) in items {
        match upload_backend_queue_item(state, path, item) {
            Ok(true) => {
                completed += 1;
                record_backend_success(state, item, false);
            }
            Ok(false) => {}
            Err(err) => {
                record_backend_queue_failure(state, path, item, &err)?;
                record_backend_error(state, Some(&item.source_host), &err);
                return Err(err);
            }
        }
    }
    Ok(completed)
}

fn list_existing_conditional_backend_keys(
    state: &MasterServerState,
    selected: &[(PathBuf, BackendQueueItem)],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut counts = BTreeMap::<String, usize>::new();
    for (_, item) in selected {
        if matches!(
            item.operation,
            BackendQueueOperation::Put {
                conditional: true,
                ..
            }
        ) {
            *counts.entry(item.source_host.clone()).or_default() += 1;
        }
    }
    let minimum = backend_dedupe_list_min_objects();
    let mut existing = BTreeMap::new();
    for (host, count) in counts {
        if count < minimum {
            continue;
        }
        let prefix = format!("{host}/");
        let listed = (|| -> Result<Vec<String>> {
            let _permit = state.backend_gate.acquire(BackendPriority::Background)?;
            state.backend.list(&prefix)
        })();
        match listed {
            Ok(keys) => {
                existing.insert(host, keys.into_iter().collect());
            }
            Err(err) => eprintln!(
                "cluster backend dedupe LIST unavailable host={host}: {err:#}; falling back to conditional PUT"
            ),
        }
    }
    existing
}

fn complete_backend_queue_item_without_upload(
    state: &MasterServerState,
    path: &Path,
    item: &BackendQueueItem,
) -> Result<bool> {
    let _guard = state
        .durable_lock
        .lock()
        .map_err(|_| anyhow!("master durable store lock poisoned"))?;
    let Some(current) = read_backend_queue_item_optional(path)? else {
        return Ok(false);
    };
    if current.generation != item.generation {
        return Ok(false);
    }
    record_backend_put_receipt(state, item)?;
    remove_backend_queue_file(path)?;
    Ok(true)
}

fn upload_backend_queue_item(
    state: &MasterServerState,
    path: &Path,
    item: &BackendQueueItem,
) -> Result<bool> {
    let payload = {
        let _guard = state
            .durable_lock
            .lock()
            .map_err(|_| anyhow!("master durable store lock poisoned"))?;
        let Some(current) = read_backend_queue_item_optional(path)? else {
            return Ok(false);
        };
        if current.generation != item.generation {
            return Ok(false);
        }
        // retry待ち・選択上限の外にある依存層も含めて公開を保留する。
        // 他hostの失敗では止めず、同じhostのcontent→metadata/journal→refだけを順序付ける。
        let priority = backend_publish_priority(&item.key);
        if (priority > 1 || item.operation == BackendQueueOperation::Delete)
            && backend_queue_items(state)?.iter().any(|(_, dependency)| {
                dependency.source_host == item.source_host
                    && matches!(dependency.operation, BackendQueueOperation::Put { .. })
                    && (item.operation == BackendQueueOperation::Delete
                        || backend_publish_priority(&dependency.key) < priority)
            })
        {
            return Ok(false);
        }
        match &item.operation {
            BackendQueueOperation::Put { digest, .. } => {
                let bytes = state.store.get(&item.key).with_context(|| {
                    format!("read durable master object for backend PUT {}", item.key)
                })?;
                let actual = blake3::hash(&bytes).to_hex().to_string();
                if &actual != digest {
                    bail!(
                        "durable master object digest changed before backend PUT {}: queued={} actual={}",
                        item.key,
                        digest,
                        actual
                    );
                }
                Some(bytes)
            }
            BackendQueueOperation::Delete => None,
        }
    };

    {
        let _permit = state.backend_gate.acquire(BackendPriority::Background)?;
        match (&item.operation, payload.as_deref()) {
            (
                BackendQueueOperation::Put {
                    conditional: true, ..
                },
                Some(bytes),
            ) => {
                state.backend.put_if_absent(&item.key, bytes)?;
            }
            (BackendQueueOperation::Put { .. }, Some(bytes)) => {
                state.backend.put(&item.key, bytes)?;
            }
            (BackendQueueOperation::Delete, None) => state.backend.delete(&item.key)?,
            _ => bail!(
                "cluster backend queue payload invariant failed for {}",
                item.key
            ),
        }
    }

    complete_uploaded_backend_queue_item(state, path, item)
}

fn complete_uploaded_backend_queue_item(
    state: &MasterServerState,
    path: &Path,
    item: &BackendQueueItem,
) -> Result<bool> {
    let _guard = state
        .durable_lock
        .lock()
        .map_err(|_| anyhow!("master durable store lock poisoned"))?;
    let Some(current) = read_backend_queue_item_optional(path)? else {
        return Ok(false);
    };
    if current.generation != item.generation {
        return Ok(false);
    }
    if item.operation == BackendQueueOperation::Delete {
        retain_local_after_backend_delete(state, item)?;
    }
    record_backend_put_receipt(state, item)?;
    remove_backend_queue_file(path)?;
    Ok(true)
}

fn record_backend_queue_failure(
    state: &MasterServerState,
    path: &Path,
    attempted: &BackendQueueItem,
    error: &anyhow::Error,
) -> Result<()> {
    let _guard = state
        .durable_lock
        .lock()
        .map_err(|_| anyhow!("master durable store lock poisoned"))?;
    let Some(mut current) = read_backend_queue_item_optional(path)? else {
        return Ok(());
    };
    if current.generation != attempted.generation {
        return Ok(());
    }
    current.attempts = current.attempts.saturating_add(1);
    current.retry_after = Some(next_backend_retry_after(Utc::now(), current.attempts));
    crate::atomic_io::write_atomic(path, &serde_json::to_vec_pretty(&current)?)?;
    eprintln!(
        "cluster backend upload deferred key={} attempts={} error={error:#}",
        current.key, current.attempts
    );
    Ok(())
}

fn next_backend_retry_after(now: DateTime<Utc>, attempts: u32) -> DateTime<Utc> {
    let exponent = attempts.saturating_sub(1).min(8);
    let base = (30_i64.saturating_mul(2_i64.pow(exponent))).min(6 * 60 * 60);
    let jitter_bound = (base / 5).max(1) as u64;
    let jitter = (rand::random::<u64>() % (jitter_bound * 2 + 1)) as i64 - jitter_bound as i64;
    now + ChronoDuration::seconds((base + jitter).max(5))
}

fn remove_backend_queue_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => crate::atomic_io::fsync_parent_dir(path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("remove {}", path.display())),
    }
}

fn record_backend_success(state: &MasterServerState, item: &BackendQueueItem, skipped: bool) {
    let now = Utc::now();
    if let Ok(mut runtime) = state.backend_runtime.lock() {
        runtime.last_success_at = Some(now);
        runtime.last_error = None;
    }
    if let Ok(mut metrics) = state.metrics.lock() {
        match item.operation {
            BackendQueueOperation::Put { .. } => {
                if skipped {
                    metrics.backend_puts_skipped_existing_total = metrics
                        .backend_puts_skipped_existing_total
                        .saturating_add(1);
                } else {
                    metrics.backend_puts_total = metrics.backend_puts_total.saturating_add(1);
                }
            }
            BackendQueueOperation::Delete => {
                metrics.backend_deletes_total = metrics.backend_deletes_total.saturating_add(1);
            }
        }
        let host = metrics.hosts.entry(item.source_host.clone()).or_default();
        host.last_backend_success_at = Some(now);
        match item.operation {
            BackendQueueOperation::Put { .. } if skipped => {
                host.backend_puts_skipped_existing =
                    host.backend_puts_skipped_existing.saturating_add(1);
            }
            BackendQueueOperation::Put { .. } => {
                host.backend_puts = host.backend_puts.saturating_add(1);
            }
            BackendQueueOperation::Delete => {
                host.backend_deletes = host.backend_deletes.saturating_add(1);
            }
        }
    }
}

fn record_backend_error(
    state: &MasterServerState,
    source_host: Option<&str>,
    error: &anyhow::Error,
) {
    let now = Utc::now();
    if let Ok(mut runtime) = state.backend_runtime.lock() {
        runtime.last_error_at = Some(now);
        runtime.last_error = Some(format!("{error:#}"));
    }
    if let Ok(mut metrics) = state.metrics.lock() {
        metrics.backend_failures_total = metrics.backend_failures_total.saturating_add(1);
        if let Some(source_host) = source_host {
            let host = metrics.hosts.entry(source_host.to_string()).or_default();
            host.backend_failures = host.backend_failures.saturating_add(1);
            host.last_backend_error_at = Some(now);
        }
    }
}

fn write_master_status_throttled(state: &MasterServerState) -> Result<()> {
    let mut last = state
        .last_status_write
        .lock()
        .map_err(|_| anyhow!("status更新lockが破損しています"))?;
    if last.is_some_and(|at| at.elapsed() < Duration::from_secs(5)) {
        return Ok(());
    }
    write_master_status(state)?;
    *last = Some(Instant::now());
    Ok(())
}

fn write_master_status(state: &MasterServerState) -> Result<()> {
    // 壊れたcatalogを0件と扱わず、不明値とエラーをheartbeatに残す。
    let mut catalog_errors = Vec::new();
    let queue = backend_queue_stats(state)
        .map_err(|e| catalog_errors.push(format!("backend-queue: {e:#}")))
        .ok();
    let retained_deleted = local_gc_stats(state)
        .map_err(|e| catalog_errors.push(format!("local-gc: {e:#}")))
        .ok();
    let runtime = state
        .backend_runtime
        .lock()
        .map_err(|_| anyhow!("cluster backend runtime lock poisoned"))?;
    let metrics = state
        .metrics
        .lock()
        .map_err(|_| anyhow!("cluster metrics lock poisoned"))?
        .clone();
    let backend_api = remote_traffic_metrics();
    let status = MasterStatus {
        version: 4,
        observed_at: Utc::now(),
        process_id: std::process::id(),
        catalog_errors,
        role: "master-data-holder",
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        authentication_mode: state.authentication.mode(),
        authorized_hosts: state.authentication.authorized_hosts(),
        store_dir: state.store_dir.display().to_string(),
        store_bytes: state.store_bytes.load(Ordering::Relaxed),
        store_max_bytes: state.store_max_bytes,
        backend_max_concurrency: state.backend_gate.max_concurrency,
        max_clients: state.client_limiter.max,
        max_inflight_frame_bytes: state.frame_budget.limit,
        backend_batch_delete_enabled: state.backend_batch_delete_enabled.load(Ordering::Relaxed),
        local_delete_grace_secs: local_delete_grace().as_secs(),
        flush_requested: state.flush_request_path.exists(),
        queue,
        retained_deleted,
        last_backend_success_at: runtime.last_success_at,
        last_backend_error_at: runtime.last_error_at,
        last_backend_error: runtime.last_error.clone(),
        metrics,
        backend_api_since_start: BackendApiMetrics {
            put_requests: backend_api.put_requests,
            get_requests: backend_api.get_requests,
            head_requests: backend_api.head_requests,
            list_requests: backend_api.list_requests,
            post_requests: backend_api.post_requests,
            delete_requests: backend_api.delete_requests,
            upload_bytes: backend_api.upload_bytes,
            download_bytes: backend_api.download_bytes,
        },
    };
    drop(runtime);
    crate::atomic_io::write_atomic(&state.status_path, &serde_json::to_vec_pretty(&status)?)
}

fn maybe_send_cluster_notice(state: &MasterServerState) -> Result<()> {
    let marker_path = state.status_path.with_file_name("cluster-notice.sent.json");
    let mut issue_codes = Vec::new();
    let queue_result = backend_queue_stats(state);
    let catalog_known = queue_result.is_ok();
    let queue = queue_result.unwrap_or_else(|_| {
        issue_codes.push("backend-catalog-unreadable".into());
        BackendQueueStats::default()
    });
    if local_gc_stats(state).is_err() {
        issue_codes.push("local-gc-catalog-unreadable".into());
    }
    if state
        .backend_runtime
        .lock()
        .is_ok_and(|r| r.last_error.is_some())
    {
        issue_codes.push("backend-error".into());
    }
    let store_bytes = state.store_bytes.load(Ordering::Relaxed);
    let quota_percent = env_u64("MAJUTSU_CLUSTER_NOTICE_STORE_PERCENT", 90).min(100);
    if let Some(limit) = state.store_max_bytes
        && limit > 0
        && store_bytes.saturating_mul(100) >= limit.saturating_mul(quota_percent)
    {
        issue_codes.push("store-quota-high".to_string());
    }
    let lag_secs = queue
        .oldest_created_at
        .map(|created_at| {
            Utc::now()
                .signed_duration_since(created_at)
                .num_seconds()
                .max(0) as u64
        })
        .unwrap_or_default();
    let lag_threshold = env_u64(
        "MAJUTSU_CLUSTER_NOTICE_BACKEND_LAG_SECS",
        backend_flush_interval()
            .as_secs()
            .saturating_mul(2)
            .max(7200),
    );
    if lag_secs >= lag_threshold && queue.objects > 0 {
        issue_codes.push("backend-lag-high".to_string());
    }
    if queue.retrying > 0 {
        issue_codes.push("backend-retrying".to_string());
    }
    let metrics = state
        .metrics
        .lock()
        .map_err(|_| anyhow!("cluster metrics lock poisoned"))?
        .clone();
    if metrics
        .last_store_quota_rejection_at
        .is_some_and(|rejected_at| {
            Utc::now().signed_duration_since(rejected_at).num_seconds()
                < env_u64("MAJUTSU_CLUSTER_NOTICE_RATE_LIMIT_SECS", 3600) as i64
        })
    {
        issue_codes.push("store-quota-rejected".to_string());
    }
    issue_codes.sort();
    issue_codes.dedup();
    if issue_codes.is_empty() {
        let _ = fs::remove_file(marker_path);
        return Ok(());
    }
    let Some(command) = env::var("MAJUTSU_CLUSTER_NOTICE_CMD")
        .ok()
        .filter(|command| !command.trim().is_empty())
    else {
        return Ok(());
    };
    let now = Utc::now();
    let previous_marker = fs::read(&marker_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<ClusterNoticeMarker>(&bytes).ok());
    let same_issues = previous_marker
        .as_ref()
        .is_some_and(|marker| marker.issue_codes == issue_codes);
    let rate_limit_secs = env_u64("MAJUTSU_CLUSTER_NOTICE_RATE_LIMIT_SECS", 3600);
    if previous_marker.as_ref().is_some_and(|marker| {
        cluster_notice_is_suppressed(marker, &issue_codes, now, rate_limit_secs)
    }) {
        return Ok(());
    }
    let mut marker = ClusterNoticeMarker {
        issue_codes: issue_codes.clone(),
        sent_at: same_issues
            .then(|| previous_marker.as_ref().and_then(|marker| marker.sent_at))
            .flatten(),
        last_attempt_at: Some(now),
        consecutive_failures: if same_issues {
            previous_marker
                .as_ref()
                .map_or(0, |marker| marker.consecutive_failures)
        } else {
            0
        },
    };
    // 先にattemptを永続化し、commandが失敗・異常終了してもpollごとの再実行を防ぐ。
    crate::atomic_io::write_atomic(&marker_path, &serde_json::to_vec_pretty(&marker)?)?;
    let mut notice = crate::platform_runtime::shell_command(&command);
    notice
        .env(
            "MAJUTSU_HOME",
            state
                .status_path
                .parent()
                .and_then(Path::parent)
                .unwrap_or(&state.store_dir),
        )
        .env("MAJUTSU_CLUSTER_NOTICE_ISSUES", issue_codes.join(","))
        .env(
            "MAJUTSU_CLUSTER_BACKEND_CATALOG_KNOWN",
            catalog_known.to_string(),
        )
        .env("MAJUTSU_CLUSTER_STORE_BYTES", store_bytes.to_string())
        .env(
            "MAJUTSU_CLUSTER_STORE_MAX_BYTES",
            state.store_max_bytes.unwrap_or_default().to_string(),
        )
        .env(
            "MAJUTSU_CLUSTER_BACKEND_QUEUE_OBJECTS",
            queue.objects.to_string(),
        )
        .env(
            "MAJUTSU_CLUSTER_BACKEND_QUEUE_BYTES",
            queue.bytes.to_string(),
        )
        .env("MAJUTSU_CLUSTER_BACKEND_LAG_SECS", lag_secs.to_string());
    let status = crate::platform_runtime::command_status_with_timeout(
        &mut notice,
        Duration::from_secs(env_u64("MAJUTSU_CLUSTER_NOTICE_TIMEOUT_SECS", 10).clamp(1, 60)),
    );
    match status {
        Ok(status) if status.success() => {
            marker.sent_at = Some(Utc::now());
            marker.last_attempt_at = marker.sent_at;
            marker.consecutive_failures = 0;
            crate::atomic_io::write_atomic(&marker_path, &serde_json::to_vec_pretty(&marker)?)
        }
        Ok(status) => {
            eprintln!("cluster notice command exited with status {status}");
            marker.consecutive_failures = marker.consecutive_failures.saturating_add(1);
            crate::atomic_io::write_atomic(&marker_path, &serde_json::to_vec_pretty(&marker)?)?;
            Ok(())
        }
        Err(err) => {
            eprintln!("cluster notice command failed: {err}");
            marker.consecutive_failures = marker.consecutive_failures.saturating_add(1);
            crate::atomic_io::write_atomic(&marker_path, &serde_json::to_vec_pretty(&marker)?)?;
            Ok(())
        }
    }
}

fn cluster_notice_is_suppressed(
    marker: &ClusterNoticeMarker,
    issue_codes: &[String],
    now: DateTime<Utc>,
    rate_limit_secs: u64,
) -> bool {
    if marker.issue_codes != issue_codes {
        return false;
    }
    if marker.sent_at.is_some_and(|sent_at| {
        now.signed_duration_since(sent_at)
            .to_std()
            .is_ok_and(|elapsed| elapsed < Duration::from_secs(rate_limit_secs))
    }) {
        return true;
    }
    marker.last_attempt_at.is_some_and(|attempted_at| {
        now.signed_duration_since(attempted_at)
            .to_std()
            .is_ok_and(|elapsed| elapsed < cluster_notice_retry_delay(marker.consecutive_failures))
    })
}

fn cluster_notice_retry_delay(consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(6);
    Duration::from_secs(60_u64.saturating_mul(1_u64 << exponent).min(3600))
}

fn directory_file_bytes(root: &Path) -> Result<u64> {
    if !root.exists() {
        return Ok(0);
    }
    let mut bytes = 0_u64;
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry?;
        if entry.file_type().is_file() {
            bytes = bytes.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_cluster_notice_retries_with_bounded_backoff() {
        let now = Utc::now();
        let issues = vec!["backend-lag-high".to_string()];
        let marker = ClusterNoticeMarker {
            issue_codes: issues.clone(),
            sent_at: None,
            last_attempt_at: Some(now),
            consecutive_failures: 2,
        };

        assert!(cluster_notice_is_suppressed(
            &marker,
            &issues,
            now + ChronoDuration::seconds(119),
            3600
        ));
        assert!(!cluster_notice_is_suppressed(
            &marker,
            &issues,
            now + ChronoDuration::seconds(120),
            3600
        ));
        assert!(!cluster_notice_is_suppressed(
            &marker,
            &["backend-error".to_string()],
            now,
            3600
        ));
        assert_eq!(cluster_notice_retry_delay(1), Duration::from_secs(60));
        assert_eq!(cluster_notice_retry_delay(2), Duration::from_secs(120));
        assert_eq!(cluster_notice_retry_delay(100), Duration::from_secs(3600));
    }

    #[test]
    fn successful_cluster_notice_keeps_existing_rate_limit() {
        let now = Utc::now();
        let issues = vec!["backend-lag-high".to_string()];
        let marker = ClusterNoticeMarker {
            issue_codes: issues.clone(),
            sent_at: Some(now),
            last_attempt_at: Some(now),
            consecutive_failures: 0,
        };
        assert!(cluster_notice_is_suppressed(
            &marker,
            &issues,
            now + ChronoDuration::seconds(3599),
            3600
        ));
        assert!(!cluster_notice_is_suppressed(
            &marker,
            &issues,
            now + ChronoDuration::seconds(3600),
            3600
        ));
    }

    #[test]
    fn cluster_notice_marker_reads_previous_success_format() {
        let marker: ClusterNoticeMarker = serde_json::from_value(serde_json::json!({
            "issue_codes": ["backend-lag-high"],
            "sent_at": "2026-09-23T17:00:00Z"
        }))
        .unwrap();
        assert!(marker.sent_at.is_some());
        assert!(marker.last_attempt_at.is_none());
        assert_eq!(marker.consecutive_failures, 0);
    }

    #[test]
    fn queue_write_failure_then_retry_recovers_backend_work() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let mut state = test_master_state(root.path(), &backend_root);
        let blocked_queue = root.path().join("blocked-queue");
        fs::write(&blocked_queue, b"blocked").unwrap();
        state.backend_queue_dir = blocked_queue;
        let operation = || ClusterOperation::Put {
            key: "test-host/objects/blobs/aa/bb".into(),
            bytes: b"payload".to_vec(),
            conditional: true,
        };
        // queue登録だけを失敗させ、payload保存後・ACK前の停止と同じ永続状態を作る。
        assert!(dispatch_operation(&state, "test-host", operation()).is_err());
        assert_eq!(
            state.store.get("test-host/objects/blobs/aa/bb").unwrap(),
            b"payload"
        );
        drop(state);
        let restarted = test_master_state(root.path(), &backend_root);
        let response = dispatch_operation(&restarted, "test-host", operation()).unwrap();
        assert_eq!(backend_queue_stats(&restarted).unwrap().objects, 1);
        assert_eq!(drain_backend_queue(&restarted, true).unwrap(), 1);
        assert!(backend_root.join("test-host/objects/blobs/aa/bb").exists());
        println!("回帰確認: 再起動後の再送={response:?}、backendまで回復");
    }

    #[test]
    fn backoff_blocks_ref_until_content_is_committed() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = test_master_state(root.path(), &backend_root);
        fs::create_dir_all(backend_root.join("test-host")).unwrap();
        fs::write(backend_root.join("test-host/objects"), b"blocked").unwrap();
        dispatch_operation(
            &state,
            "test-host",
            ClusterOperation::BatchPut {
                items: vec![
                    ClusterPutItem {
                        key: "test-host/objects/blobs/aa/bb".into(),
                        bytes: b"payload".to_vec(),
                        conditional: true,
                    },
                    ClusterPutItem {
                        key: "test-host/refs/current".into(),
                        bytes: b"snapshot-needing-aa-bb".to_vec(),
                        conditional: false,
                    },
                ],
            },
        )
        .unwrap();
        assert!(drain_backend_queue(&state, true).is_err());
        for (path, mut item) in backend_queue_items(&state).unwrap() {
            item.created_at = Utc::now() - ChronoDuration::hours(2);
            if item.key.contains("/objects/") {
                item.retry_after = Some(Utc::now() + ChronoDuration::hours(1));
            }
            crate::atomic_io::write_atomic(&path, &serde_json::to_vec(&item).unwrap()).unwrap();
        }
        assert_eq!(drain_backend_queue(&state, false).unwrap(), 0);
        assert!(
            state
                .backend
                .get_optional("test-host/refs/current")
                .unwrap()
                .is_none()
        );
        assert!(!backend_root.join("test-host/objects/blobs/aa/bb").is_file());
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 2);
        fs::rename(
            backend_root.join("test-host/objects"),
            root.path().join("blocked.saved"),
        )
        .unwrap();
        assert_eq!(drain_backend_queue(&state, true).unwrap(), 2);
        assert!(state.backend.exists("test-host/refs/current").unwrap());
        println!("回帰確認: content回復前はrefs/currentを公開せず、回復後に公開");
    }

    fn test_master_state(root: &Path, backend_root: &Path) -> MasterServerState {
        let store_dir = root.join("store");
        let backend_queue_dir = root.join("backend-queue");
        let local_gc_dir = root.join("local-gc");
        let runtime_dir = root.join("runtime");
        let write_intent_dir = root.join("write-intents");
        let backend_receipt_dir = root.join("backend-receipts");
        fs::create_dir_all(&store_dir).unwrap();
        fs::create_dir_all(&backend_queue_dir).unwrap();
        fs::create_dir_all(&local_gc_dir).unwrap();
        fs::create_dir_all(&runtime_dir).unwrap();
        fs::create_dir_all(&write_intent_dir).unwrap();
        fs::create_dir_all(&backend_receipt_dir).unwrap();
        let store_bytes = directory_file_bytes(&store_dir).unwrap();
        MasterServerState {
            store: RemoteStore::File(crate::remote_store::FileRemote {
                root: store_dir.clone(),
            }),
            store_dir,
            backend: RemoteStore::File(crate::remote_store::FileRemote {
                root: backend_root.to_path_buf(),
            }),
            backend_gate: BackendGate::new(4),
            backend_batch_delete_enabled: AtomicBool::new(true),
            backend_queue_dir,
            write_intent_dir,
            backend_receipt_dir,
            local_gc_dir,
            status_path: runtime_dir.join("cluster-master-status.json"),
            flush_request_path: runtime_dir.join("cluster-backend-flush.request"),
            store_bytes: AtomicU64::new(store_bytes),
            store_max_bytes: None,
            durable_lock: Mutex::new(()),
            backend_runtime: Mutex::new(BackendRuntimeState::default()),
            metrics: Mutex::new(MasterMetrics::default()),
            last_status_write: Mutex::new(None),
            client_limiter: Arc::new(ClientLimiter::new(DEFAULT_CLUSTER_MAX_CLIENTS)),
            frame_budget: Arc::new(FrameBudget::new(DEFAULT_CLUSTER_MAX_INFLIGHT_FRAME_BYTES)),
            authentication: ClusterAuthentication::LegacyShared {
                token_digest: token_digest("test-token"),
            },
            list_cache: Mutex::new(HashMap::new()),
        }
    }

    fn batch_results(value: ClusterValue) -> Vec<ClusterPutResult> {
        match value {
            ClusterValue::BatchPut(results) => results,
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn cluster_key_validation_rejects_traversal() {
        assert!(validate_cluster_key("objects/blobs/aa/bb").is_ok());
        assert!(validate_cluster_key("../escape").is_err());
        assert!(validate_cluster_key("objects//escape").is_err());
        assert!(validate_cluster_prefix("host/").is_ok());
        assert!(validate_cluster_prefix("../").is_err());
    }

    #[test]
    fn client_and_frame_limits_bound_untrusted_input() {
        let limiter = Arc::new(ClientLimiter::new(1));
        let permit = limiter.try_acquire().expect("first client is admitted");
        assert!(limiter.try_acquire().is_none());
        drop(permit);
        assert!(limiter.try_acquire().is_some());

        let budget = Arc::new(FrameBudget::new(2 * 1024 * 1024));
        let first = budget.try_reserve(1024 * 1024).unwrap();
        assert!(budget.try_reserve(1024 * 1024 + 1).is_err());
        drop(first);
        assert!(budget.try_reserve(2 * 1024 * 1024).is_ok());
    }

    #[test]
    fn host_acl_limits_mutations_to_own_prefix() {
        assert!(
            authorize_cluster_operation(
                "host-a",
                &ClusterOperation::Put {
                    key: "host-a/objects/blobs/aa/bb".into(),
                    bytes: Vec::new(),
                    conditional: true,
                },
            )
            .is_ok()
        );
        assert!(
            authorize_cluster_operation(
                "host-a",
                &ClusterOperation::Put {
                    key: "host-b/refs/current".into(),
                    bytes: Vec::new(),
                    conditional: false,
                },
            )
            .is_err()
        );
        assert!(
            authorize_cluster_operation(
                "host-a",
                &ClusterOperation::Delete {
                    key: "host-b/refs/current".into(),
                },
            )
            .is_err()
        );
        assert!(
            authorize_cluster_operation(
                "host-a",
                &ClusterOperation::ApplyLifecycle {
                    policy: serde_json::json!({}),
                },
            )
            .is_err()
        );
        assert!(
            authorize_cluster_operation(
                "host-a",
                &ClusterOperation::Get {
                    key: "host-b/refs/current".into(),
                },
            )
            .is_ok()
        );
    }

    #[test]
    fn host_tokens_bind_token_to_claimed_host() {
        let authentication = ClusterAuthentication::HostTokens {
            token_digests: BTreeMap::from([
                ("host-a".into(), token_digest("token-a")),
                ("host-b".into(), token_digest("token-b")),
            ]),
        };
        assert!(authentication.authenticates("host-a", "token-a"));
        assert!(!authentication.authenticates("host-b", "token-a"));
        assert!(!authentication.authenticates("host-c", "token-a"));
    }

    #[test]
    fn legacy_backend_queue_hosts_follow_the_host_scoped_key() {
        let now = Utc::now();
        let mut linux = BackendQueueItem {
            version: BACKEND_QUEUE_VERSION,
            generation: "generation".into(),
            key: "vagrant/objects/blobs/aa/bb".into(),
            source_host: "unknown-host".into(),
            operation: BackendQueueOperation::Delete,
            created_at: now,
            updated_at: now,
            attempts: 0,
            retry_after: None,
        };
        normalize_legacy_backend_source_host(&mut linux);
        assert_eq!(linux.source_host, "vagrant");

        let mut windows = BackendQueueItem {
            key: "winvr/refs/current".into(),
            source_host: "WIN-VR".into(),
            ..linux
        };
        normalize_legacy_backend_source_host(&mut windows);
        assert_eq!(windows.source_host, "winvr");
    }

    #[test]
    fn cluster_url_requires_explicit_port() {
        let err = match cluster_remote_from_url("cluster://mba22.local") {
            Ok(_) => panic!("cluster URL without a port must be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("missing a port"), "{err:#}");
    }

    #[test]
    fn master_batch_put_deduplicates_conditional_objects() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = test_master_state(root.path(), &backend_root);
        let results = batch_results(
            dispatch_operation(
                &state,
                "test-host",
                ClusterOperation::BatchPut {
                    items: vec![
                        ClusterPutItem {
                            key: "objects/blobs/aa/bb".into(),
                            bytes: b"payload".to_vec(),
                            conditional: true,
                        },
                        ClusterPutItem {
                            key: "objects/blobs/aa/bb".into(),
                            bytes: b"payload".to_vec(),
                            conditional: true,
                        },
                    ],
                },
            )
            .unwrap(),
        );
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.master_durable));
        assert!(results[0].newly_stored);
        assert!(!results[1].newly_stored);
        assert_eq!(
            fs::read(root.path().join("store/objects/blobs/aa/bb")).unwrap(),
            b"payload"
        );
        assert!(!backend_root.join("objects/blobs/aa/bb").exists());
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 1);
    }

    #[test]
    fn master_ack_does_not_require_backend_and_queue_survives_failure() {
        let root = tempfile::tempdir().unwrap();
        let blocked_backend = root.path().join("backend-is-a-file");
        fs::write(&blocked_backend, b"blocked").unwrap();
        let state = test_master_state(root.path(), &blocked_backend);
        let results = batch_results(
            dispatch_operation(
                &state,
                "test-host",
                ClusterOperation::BatchPut {
                    items: vec![ClusterPutItem {
                        key: "objects/blobs/aa/bb".into(),
                        bytes: b"payload".to_vec(),
                        conditional: true,
                    }],
                },
            )
            .unwrap(),
        );
        assert!(results[0].master_durable);
        assert!(drain_backend_queue(&state, true).is_err());
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 1);
        assert_eq!(state.store.get("objects/blobs/aa/bb").unwrap(), b"payload");
        let listed = dispatch_operation(
            &state,
            "test-host",
            ClusterOperation::List {
                prefix: "objects/".into(),
            },
        )
        .unwrap();
        assert!(
            matches!(listed, ClusterValue::Strings(keys) if keys == vec!["objects/blobs/aa/bb"])
        );
    }

    #[test]
    fn backend_worker_flushes_durable_object_after_ack() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = test_master_state(root.path(), &backend_root);
        dispatch_operation(
            &state,
            "test-host",
            ClusterOperation::BatchPut {
                items: vec![ClusterPutItem {
                    key: "objects/blobs/aa/bb".into(),
                    bytes: b"payload".to_vec(),
                    conditional: true,
                }],
            },
        )
        .unwrap();
        assert_eq!(drain_backend_queue(&state, true).unwrap(), 1);
        assert_eq!(
            fs::read(backend_root.join("objects/blobs/aa/bb")).unwrap(),
            b"payload"
        );
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 0);
    }

    #[test]
    fn acknowledged_duplicate_does_not_recreate_backend_queue() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = test_master_state(root.path(), &backend_root);
        let operation = || ClusterOperation::Put {
            key: "test-host/objects/blobs/aa/bb".into(),
            bytes: b"payload".to_vec(),
            conditional: true,
        };
        dispatch_operation(&state, "test-host", operation()).unwrap();
        assert_eq!(drain_backend_queue(&state, true).unwrap(), 1);
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 0);
        dispatch_operation(&state, "test-host", operation()).unwrap();
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 0);
    }

    #[test]
    fn backend_listing_skips_duplicate_conditional_put_burst() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = test_master_state(root.path(), &backend_root);
        for index in 0..DEFAULT_BACKEND_DEDUPE_LIST_MIN_OBJECTS {
            let key = format!("test-host/objects/blobs/aa/{index:04}");
            let payload = format!("payload-{index}");
            fs::create_dir_all(backend_root.join("test-host/objects/blobs/aa")).unwrap();
            fs::write(backend_root.join(&key), &payload).unwrap();
            dispatch_operation(
                &state,
                "test-host",
                ClusterOperation::Put {
                    key,
                    bytes: payload.into_bytes(),
                    conditional: true,
                },
            )
            .unwrap();
        }
        assert_eq!(
            drain_backend_queue(&state, true).unwrap(),
            DEFAULT_BACKEND_DEDUPE_LIST_MIN_OBJECTS
        );
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 0);
        assert_eq!(
            fs::read_to_string(backend_root.join("test-host/objects/blobs/aa/0000")).unwrap(),
            "payload-0"
        );
        let metrics = state.metrics.lock().unwrap();
        assert_eq!(
            metrics.backend_puts_skipped_existing_total,
            DEFAULT_BACKEND_DEDUPE_LIST_MIN_OBJECTS as u64
        );
    }

    #[test]
    fn mutable_backend_queue_coalesces_to_latest_value() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = test_master_state(root.path(), &backend_root);
        for bytes in [b"first".as_slice(), b"second".as_slice()] {
            dispatch_operation(
                &state,
                "test-host",
                ClusterOperation::Put {
                    key: "test-host/refs/current".into(),
                    bytes: bytes.to_vec(),
                    conditional: false,
                },
            )
            .unwrap();
        }
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 1);
        assert_eq!(drain_backend_queue(&state, true).unwrap(), 1);
        assert_eq!(
            fs::read(backend_root.join("test-host/refs/current")).unwrap(),
            b"second"
        );
    }

    #[test]
    fn backend_fallback_hydrates_master_store() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let key = "legacy-host/objects/blobs/aa/bb";
        fs::create_dir_all(backend_root.join("legacy-host/objects/blobs/aa")).unwrap();
        fs::write(backend_root.join(key), b"legacy").unwrap();
        let state = test_master_state(root.path(), &backend_root);
        let local_only = dispatch_operation(
            &state,
            "test-host",
            ClusterOperation::ListMasterLocalWithSizes {
                prefix: "legacy-host/".into(),
            },
        )
        .unwrap();
        assert!(matches!(local_only, ClusterValue::Objects(objects) if objects.is_empty()));
        let with_fallback = dispatch_operation(
            &state,
            "test-host",
            ClusterOperation::ListWithSizes {
                prefix: "legacy-host/".into(),
            },
        )
        .unwrap();
        assert!(matches!(with_fallback, ClusterValue::Objects(objects) if objects.len() == 1));
        let value = dispatch_operation(
            &state,
            "test-host",
            ClusterOperation::GetOptional {
                key: key.to_string(),
            },
        )
        .unwrap();
        assert!(matches!(value, ClusterValue::OptionalBytes(Some(bytes)) if bytes == b"legacy"));
        assert_eq!(state.store.get(key).unwrap(), b"legacy");
    }

    #[test]
    fn master_store_and_backend_queue_survive_state_restart() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let key = "test-host/objects/blobs/aa/bb";
        {
            let state = test_master_state(root.path(), &backend_root);
            dispatch_operation(
                &state,
                "test-host",
                ClusterOperation::Put {
                    key: key.into(),
                    bytes: b"persisted".to_vec(),
                    conditional: true,
                },
            )
            .unwrap();
        }
        let restarted = test_master_state(root.path(), &backend_root);
        assert_eq!(restarted.store.get(key).unwrap(), b"persisted");
        assert_eq!(backend_queue_stats(&restarted).unwrap().objects, 1);
        assert_eq!(drain_backend_queue(&restarted, true).unwrap(), 1);
        assert_eq!(fs::read(backend_root.join(key)).unwrap(), b"persisted");
    }

    #[test]
    fn master_store_quota_rejects_ack_and_preserves_slave_responsibility() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let mut state = test_master_state(root.path(), &backend_root);
        state.store_max_bytes = Some(3);
        let error = dispatch_operation(
            &state,
            "test-host",
            ClusterOperation::Put {
                key: "test-host/objects/blobs/aa/bb".into(),
                bytes: b"four".to_vec(),
                conditional: true,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("quota exceeded"), "{error:#}");
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 0);
        assert!(!state.store.exists("test-host/objects/blobs/aa/bb").unwrap());
    }

    #[test]
    fn delete_is_hidden_then_retained_locally_after_backend_delete() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = test_master_state(root.path(), &backend_root);
        let key = "test-host/refs/old";
        dispatch_operation(
            &state,
            "test-host",
            ClusterOperation::Put {
                key: key.into(),
                bytes: b"old".to_vec(),
                conditional: false,
            },
        )
        .unwrap();
        drain_backend_queue(&state, true).unwrap();
        assert!(backend_root.join(key).exists());

        dispatch_operation(
            &state,
            "test-host",
            ClusterOperation::Delete { key: key.into() },
        )
        .unwrap();
        assert!(state.store.exists(key).unwrap());
        assert!(key_is_logically_deleted(&state, key).unwrap());
        assert!(matches!(
            dispatch_operation(
                &state,
                "test-host",
                ClusterOperation::GetOptional { key: key.into() }
            )
            .unwrap(),
            ClusterValue::OptionalBytes(None)
        ));
        assert!(backend_root.join(key).exists());
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 1);
        drain_backend_queue(&state, true).unwrap();
        assert!(!backend_root.join(key).exists());
        assert!(state.store.exists(key).unwrap());
        assert_eq!(local_gc_stats(&state).unwrap().objects, 1);

        let path = local_gc_path(&state, key);
        let mut retained = read_local_gc_item_optional(&path).unwrap().unwrap();
        retained.delete_after = Utc::now() - ChronoDuration::seconds(1);
        crate::atomic_io::write_atomic(&path, &serde_json::to_vec(&retained).unwrap()).unwrap();
        assert_eq!(run_local_gc(&state).unwrap(), 1);
        assert!(!state.store.exists(key).unwrap());
        assert_eq!(local_gc_stats(&state).unwrap().objects, 0);
    }

    #[test]
    fn backend_delete_batch_completes_multiple_retention_items() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = test_master_state(root.path(), &backend_root);
        let keys = ["test-host/refs/old-a", "test-host/refs/old-b"];
        for key in keys {
            dispatch_operation(
                &state,
                "test-host",
                ClusterOperation::Put {
                    key: key.into(),
                    bytes: key.as_bytes().to_vec(),
                    conditional: false,
                },
            )
            .unwrap();
        }
        assert_eq!(drain_backend_queue(&state, true).unwrap(), 2);
        for key in keys {
            dispatch_operation(
                &state,
                "test-host",
                ClusterOperation::Delete { key: key.into() },
            )
            .unwrap();
        }
        assert_eq!(drain_backend_queue(&state, true).unwrap(), 2);
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 0);
        assert_eq!(local_gc_stats(&state).unwrap().objects, 2);
        assert!(keys.iter().all(|key| !backend_root.join(key).exists()));
    }

    #[test]
    fn explicit_flush_request_drains_queue_and_updates_status() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = test_master_state(root.path(), &backend_root);
        let key = "test-host/objects/blobs/aa/bb";
        let operation = ClusterOperation::Put {
            key: key.into(),
            bytes: b"payload".to_vec(),
            conditional: true,
        };
        record_authorized_request(&state, "test-host", &operation);
        dispatch_operation(&state, "test-host", operation).unwrap();
        crate::atomic_io::write_atomic(&state.flush_request_path, b"test").unwrap();
        run_backend_worker_once(&state).unwrap();
        assert!(!state.flush_request_path.exists());
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 0);
        assert_eq!(fs::read(backend_root.join(key)).unwrap(), b"payload");
        let status: serde_json::Value =
            serde_json::from_slice(&fs::read(&state.status_path).unwrap()).unwrap();
        assert_eq!(status["role"], "master-data-holder");
        assert_eq!(status["authentication_mode"], "legacy-shared-token");
        assert_eq!(status["queue"]["objects"], 0);
        assert_eq!(status["retained_deleted"]["objects"], 0);
        assert_eq!(
            status["metrics"]["hosts"]["test-host"]["received_objects"],
            1
        );
        assert_eq!(status["flush_requested"], false);
    }

    #[test]
    fn cluster_proxy_heartbeat_round_trip_records_explicit_liveness() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = Arc::new(test_master_state(root.path(), &backend_root));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let server_state = Arc::clone(&state);
        let server = thread::spawn(move || {
            for stream in listener.incoming().take(2) {
                handle_connection(stream.unwrap(), Arc::clone(&server_state));
            }
        });
        let proxy = ClusterRemote {
            endpoint,
            token: "test-token".into(),
            host_id: "test-host".into(),
        };

        assert!(proxy.heartbeat_if_supported().unwrap());
        server.join().unwrap();

        let metrics = state.metrics.lock().unwrap();
        let host = metrics.hosts.get("test-host").unwrap();
        assert!(host.last_heartbeat_at.is_some());
        assert_eq!(host.last_seen_at, host.last_heartbeat_at);
        drop(metrics);
        let status: serde_json::Value =
            serde_json::from_slice(&fs::read(&state.status_path).unwrap()).unwrap();
        assert!(status["metrics"]["hosts"]["test-host"]["last_heartbeat_at"].is_string());
    }

    #[test]
    fn cluster_proxy_round_trip_uses_framed_tcp_protocol() {
        let root = tempfile::tempdir().unwrap();
        let backend_root = root.path().join("backend");
        let state = Arc::new(test_master_state(root.path(), &backend_root));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let server_state = Arc::clone(&state);
        let server = thread::spawn(move || {
            for stream in listener.incoming().take(4) {
                handle_connection(stream.unwrap(), Arc::clone(&server_state));
            }
        });
        let proxy = ClusterRemote {
            endpoint,
            token: "test-token".into(),
            host_id: "test-host".into(),
        };
        proxy
            .put("test-host/objects/blobs/aa/bb", b"payload")
            .unwrap();
        assert_eq!(
            proxy.get("test-host/objects/blobs/aa/bb").unwrap(),
            b"payload"
        );
        assert_eq!(
            proxy.list("test-host/objects/blobs/").unwrap(),
            vec!["test-host/objects/blobs/aa/bb".to_string()]
        );
        server.join().unwrap();
        assert!(!backend_root.join("test-host/objects/blobs/aa/bb").exists());
        assert_eq!(backend_queue_stats(&state).unwrap().objects, 1);
    }
}
