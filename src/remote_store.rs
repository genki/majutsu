use crate::majutsu_store::{
    RemoteCapabilities, archive_restore_status, s3_archive_restore_request_xml,
};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::blocking::{Body, Client};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, DATE, ETAG, HOST, RANGE};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use url::Url;
use walkdir::WalkDir;

use crate::config::{RemoteConfig, default_large_max_parallel_uploads};

pub(crate) const MIN_MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
pub(crate) const DEFAULT_MULTIPART_THRESHOLD: usize = 64 * 1024 * 1024;
pub(crate) const DEFAULT_LOCAL_MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
pub(crate) const DEFAULT_CLOUD_MULTIPART_PART_SIZE: usize = 64 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_MULTIPART_PARTS: usize = 10_000;
pub(crate) const DEFAULT_METADATA_MULTIPART_PARALLELISM: usize = 2;
pub(crate) const DEFAULT_S3_CONNECT_TIMEOUT_SECS: u64 = 10;
pub(crate) const DEFAULT_S3_REQUEST_TIMEOUT_SECS: u64 = 300;

pub(crate) fn adaptive_multipart_part_size(len: usize, endpoint: &str) -> usize {
    let requested = env::var("MAJUTSU_S3_MULTIPART_PART_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| default_part_size_for_endpoint(endpoint));
    let max_parts = env::var("MAJUTSU_S3_MAX_MULTIPART_PARTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_MULTIPART_PARTS);
    let required = len.div_ceil(max_parts).max(MIN_MULTIPART_PART_SIZE);
    requested.max(required).max(MIN_MULTIPART_PART_SIZE)
}

fn default_part_size_for_endpoint(endpoint: &str) -> usize {
    let endpoint = endpoint.to_ascii_lowercase();
    if endpoint.contains("127.0.0.1")
        || endpoint.contains("localhost")
        || endpoint.contains("minio")
    {
        DEFAULT_LOCAL_MULTIPART_PART_SIZE
    } else {
        DEFAULT_CLOUD_MULTIPART_PART_SIZE
    }
}

#[derive(Clone)]
pub(crate) enum RemoteStore {
    File(FileRemote),
    S3(Box<S3Remote>),
}

#[derive(Clone)]
pub(crate) struct FileRemote {
    pub(crate) root: PathBuf,
}

fn file_remote_fsync_enabled() -> bool {
    std::env::var("MAJUTSU_FSYNC_REMOTE_FILE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn s3_timeout_secs_env(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .with_context(|| format!("parse {name} as seconds"))
            .map(|value| value.max(1)),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err).with_context(|| format!("read {name}")),
    }
}

pub(crate) fn s3_http_client() -> Result<Client> {
    let connect_timeout = Duration::from_secs(s3_timeout_secs_env(
        "MAJUTSU_S3_CONNECT_TIMEOUT_SECS",
        DEFAULT_S3_CONNECT_TIMEOUT_SECS,
    )?);
    let request_timeout = Duration::from_secs(s3_timeout_secs_env(
        "MAJUTSU_S3_REQUEST_TIMEOUT_SECS",
        DEFAULT_S3_REQUEST_TIMEOUT_SECS,
    )?);
    Ok(Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .build()?)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RemoteTrafficMetrics {
    pub(crate) put_requests: u64,
    pub(crate) get_requests: u64,
    pub(crate) head_requests: u64,
    pub(crate) list_requests: u64,
    pub(crate) post_requests: u64,
    pub(crate) delete_requests: u64,
    pub(crate) upload_bytes: u64,
    pub(crate) download_bytes: u64,
}

impl RemoteTrafficMetrics {
    pub(crate) fn total_requests(&self) -> u64 {
        self.put_requests
            + self.get_requests
            + self.head_requests
            + self.list_requests
            + self.post_requests
            + self.delete_requests
    }
}

static S3_PUT_REQUESTS: AtomicU64 = AtomicU64::new(0);
static S3_GET_REQUESTS: AtomicU64 = AtomicU64::new(0);
static S3_HEAD_REQUESTS: AtomicU64 = AtomicU64::new(0);
static S3_LIST_REQUESTS: AtomicU64 = AtomicU64::new(0);
static S3_POST_REQUESTS: AtomicU64 = AtomicU64::new(0);
static S3_DELETE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static S3_UPLOAD_BYTES: AtomicU64 = AtomicU64::new(0);
static S3_DOWNLOAD_BYTES: AtomicU64 = AtomicU64::new(0);

pub(crate) struct RemoteTrafficTraceGuard {
    label: &'static str,
    enabled: bool,
}

impl RemoteTrafficTraceGuard {
    pub(crate) fn new(label: &'static str) -> Self {
        let enabled = remote_traffic_trace_enabled();
        if enabled {
            reset_remote_traffic_metrics();
        }
        Self { label, enabled }
    }
}

impl Drop for RemoteTrafficTraceGuard {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }
        let metrics = remote_traffic_metrics();
        eprintln!(
            "remote_trace label={} requests={} put={} get={} head={} list={} post={} delete={} upload_bytes={} download_bytes={}",
            self.label,
            metrics.total_requests(),
            metrics.put_requests,
            metrics.get_requests,
            metrics.head_requests,
            metrics.list_requests,
            metrics.post_requests,
            metrics.delete_requests,
            metrics.upload_bytes,
            metrics.download_bytes
        );
    }
}

pub(crate) fn remote_traffic_trace_enabled() -> bool {
    env_bool("MAJUTSU_TRACE_REMOTE") || env_bool("MAJUTSU_TRACE_S3")
}

pub(crate) fn reset_remote_traffic_metrics() {
    S3_PUT_REQUESTS.store(0, Ordering::Relaxed);
    S3_GET_REQUESTS.store(0, Ordering::Relaxed);
    S3_HEAD_REQUESTS.store(0, Ordering::Relaxed);
    S3_LIST_REQUESTS.store(0, Ordering::Relaxed);
    S3_POST_REQUESTS.store(0, Ordering::Relaxed);
    S3_DELETE_REQUESTS.store(0, Ordering::Relaxed);
    S3_UPLOAD_BYTES.store(0, Ordering::Relaxed);
    S3_DOWNLOAD_BYTES.store(0, Ordering::Relaxed);
}

pub(crate) fn remote_traffic_metrics() -> RemoteTrafficMetrics {
    RemoteTrafficMetrics {
        put_requests: S3_PUT_REQUESTS.load(Ordering::Relaxed),
        get_requests: S3_GET_REQUESTS.load(Ordering::Relaxed),
        head_requests: S3_HEAD_REQUESTS.load(Ordering::Relaxed),
        list_requests: S3_LIST_REQUESTS.load(Ordering::Relaxed),
        post_requests: S3_POST_REQUESTS.load(Ordering::Relaxed),
        delete_requests: S3_DELETE_REQUESTS.load(Ordering::Relaxed),
        upload_bytes: S3_UPLOAD_BYTES.load(Ordering::Relaxed),
        download_bytes: S3_DOWNLOAD_BYTES.load(Ordering::Relaxed),
    }
}

fn record_s3_put(upload_bytes: u64) {
    S3_PUT_REQUESTS.fetch_add(1, Ordering::Relaxed);
    S3_UPLOAD_BYTES.fetch_add(upload_bytes, Ordering::Relaxed);
}

fn record_s3_get(download_bytes: u64) {
    S3_GET_REQUESTS.fetch_add(1, Ordering::Relaxed);
    S3_DOWNLOAD_BYTES.fetch_add(download_bytes, Ordering::Relaxed);
}

fn record_s3_head() {
    S3_HEAD_REQUESTS.fetch_add(1, Ordering::Relaxed);
}

fn record_s3_list(download_bytes: u64) {
    S3_LIST_REQUESTS.fetch_add(1, Ordering::Relaxed);
    S3_DOWNLOAD_BYTES.fetch_add(download_bytes, Ordering::Relaxed);
}

fn record_s3_post(upload_bytes: u64, download_bytes: u64) {
    S3_POST_REQUESTS.fetch_add(1, Ordering::Relaxed);
    S3_UPLOAD_BYTES.fetch_add(upload_bytes, Ordering::Relaxed);
    S3_DOWNLOAD_BYTES.fetch_add(download_bytes, Ordering::Relaxed);
}

fn record_s3_delete() {
    S3_DELETE_REQUESTS.fetch_add(1, Ordering::Relaxed);
}

fn env_bool(name: &str) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

#[derive(Clone)]
pub(crate) struct S3Remote {
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) endpoint: String,
    pub(crate) region: String,
    pub(crate) signature_version: String,
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    pub(crate) storage_class: Option<String>,
    pub(crate) object_tags: Vec<(String, String)>,
    pub(crate) multipart_enabled: bool,
    pub(crate) max_parallel_uploads: usize,
    pub(crate) client: Client,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RemoteObjectStat {
    pub(crate) key: String,
    pub(crate) size: u64,
}

pub(crate) fn open_remote(config: &RemoteConfig) -> Result<RemoteStore> {
    open_remote_with_upload_policy(config, true, default_large_max_parallel_uploads())
}

pub(crate) fn open_remote_with_upload_policy(
    config: &RemoteConfig,
    multipart_enabled: bool,
    max_parallel_uploads: usize,
) -> Result<RemoteStore> {
    let remote_url = config.url()?;
    if let Some(path) = remote_url.strip_prefix("file://") {
        return Ok(RemoteStore::File(FileRemote {
            root: PathBuf::from(path),
        }));
    }
    if remote_url.starts_with("s3://") {
        let url = Url::parse(&remote_url)?;
        let bucket = url
            .host_str()
            .ok_or_else(|| anyhow!("s3 remote is missing bucket: {remote_url}"))?
            .to_string();
        let prefix = url.path().trim_matches('/').to_string();
        return Ok(RemoteStore::S3(Box::new(S3Remote {
            bucket,
            prefix,
            endpoint: config
                .endpoint
                .clone()
                .or_else(|| env::var("AWS_ENDPOINT_URL").ok())
                .unwrap_or_else(|| "https://storage.googleapis.com".into()),
            region: config
                .region
                .clone()
                .or_else(|| env::var("AWS_DEFAULT_REGION").ok())
                .unwrap_or_else(|| "us-east-1".into()),
            signature_version: config
                .signature_version
                .clone()
                .or_else(|| env::var("AWS_SIGNATURE_VERSION").ok())
                .unwrap_or_else(|| "s3v4".into()),
            access_key: env::var("AWS_ACCESS_KEY_ID")
                .context("AWS_ACCESS_KEY_ID is required for s3 remote")?,
            secret_key: env::var("AWS_SECRET_ACCESS_KEY")
                .context("AWS_SECRET_ACCESS_KEY is required for s3 remote")?,
            storage_class: optional_env("MAJUTSU_S3_STORAGE_CLASS")?,
            object_tags: parse_s3_object_tags_env()?,
            multipart_enabled,
            max_parallel_uploads: max_parallel_uploads.max(1),
            client: s3_http_client()?,
        })));
    }
    bail!("unsupported remote URL: {remote_url}");
}

impl RemoteStore {
    pub(crate) fn describe(&self) -> String {
        match self {
            RemoteStore::File(remote) => format!("file://{}", remote.root.display()),
            RemoteStore::S3(remote) => {
                let prefix = if remote.prefix.is_empty() {
                    String::new()
                } else {
                    format!("/{}", remote.prefix)
                };
                format!("s3://{}{}", remote.bucket, prefix)
            }
        }
    }

    pub(crate) fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        match self {
            RemoteStore::File(remote) => {
                let path = remote.root.join(key);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, bytes)?;
                Ok(())
            }
            RemoteStore::S3(remote) => remote.put(key, bytes),
        }
    }

    pub(crate) fn put_file(&self, key: &str, source: &Path) -> Result<()> {
        match self {
            RemoteStore::File(remote) => {
                let path = remote.root.join(key);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(source, path)?;
                Ok(())
            }
            RemoteStore::S3(remote) => remote.put_file(key, source),
        }
    }

    pub(crate) fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        match self {
            RemoteStore::File(remote) => {
                let path = remote.root.join(key);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                match fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                {
                    Ok(mut file) => {
                        file.write_all(bytes)?;
                        if file_remote_fsync_enabled() {
                            file.sync_all()?;
                        }
                        Ok(true)
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
                    Err(err) => Err(err.into()),
                }
            }
            RemoteStore::S3(remote) => remote.put_if_absent(key, bytes),
        }
    }

    pub(crate) fn put_file_if_absent(&self, key: &str, source: &Path) -> Result<bool> {
        match self {
            RemoteStore::File(remote) => {
                let path = remote.root.join(key);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                match fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                {
                    Ok(mut out) => {
                        let mut input = File::open(source)?;
                        std::io::copy(&mut input, &mut out)?;
                        if file_remote_fsync_enabled() {
                            out.sync_all()?;
                        }
                        Ok(true)
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
                    Err(err) => Err(err.into()),
                }
            }
            RemoteStore::S3(remote) => remote.put_file_if_absent(key, source),
        }
    }

    pub(crate) fn get(&self, key: &str) -> Result<Vec<u8>> {
        match self {
            RemoteStore::File(remote) => Ok(fs::read(remote.root.join(key))?),
            RemoteStore::S3(remote) => remote.get(key),
        }
    }

    pub(crate) fn get_optional(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self {
            RemoteStore::File(remote) => match fs::read(remote.root.join(key)) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                    ) =>
                {
                    Ok(None)
                }
                Err(err) => Err(err.into()),
            },
            RemoteStore::S3(remote) => remote.get_optional(key),
        }
    }

    pub(crate) fn delete(&self, key: &str) -> Result<()> {
        match self {
            RemoteStore::File(remote) => {
                let path = remote.root.join(key);
                if path.exists() {
                    fs::remove_file(path)?;
                }
                Ok(())
            }
            RemoteStore::S3(remote) => remote.delete(key),
        }
    }

    pub(crate) fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        match self {
            RemoteStore::File(remote) => {
                let mut file = File::open(remote.root.join(key))?;
                file.seek(SeekFrom::Start(start))?;
                let mut limited = Vec::with_capacity(len as usize);
                let mut take = file.take(len);
                take.read_to_end(&mut limited)?;
                Ok(limited)
            }
            RemoteStore::S3(remote) => remote.get_range(key, start, len),
        }
    }

    pub(crate) fn exists(&self, key: &str) -> Result<bool> {
        match self {
            RemoteStore::File(remote) => Ok(remote.root.join(key).exists()),
            RemoteStore::S3(remote) => remote.exists(key),
        }
    }

    pub(crate) fn list(&self, prefix: &str) -> Result<Vec<String>> {
        match self {
            RemoteStore::File(remote) => list_file_remote(&remote.root, prefix),
            RemoteStore::S3(remote) => remote.list(prefix),
        }
    }

    pub(crate) fn list_with_sizes(&self, prefix: &str) -> Result<Vec<RemoteObjectStat>> {
        match self {
            RemoteStore::File(remote) => list_file_remote_with_sizes(&remote.root, prefix),
            RemoteStore::S3(remote) => remote.list_with_sizes(prefix),
        }
    }

    pub(crate) fn restore_archive(&self, key: &str, days: u32, tier: &str) -> Result<bool> {
        match self {
            RemoteStore::File(_) => Ok(true),
            RemoteStore::S3(remote) => remote.restore_archive(key, days, tier),
        }
    }

    pub(crate) fn apply_s3_lifecycle_policy(&self, policy: &serde_json::Value) -> Result<bool> {
        match self {
            RemoteStore::File(_) => Ok(false),
            RemoteStore::S3(remote) => {
                let policy_xml = remote.lifecycle_configuration_xml(policy)?;
                remote.put_lifecycle_configuration(&policy_xml)?;
                Ok(true)
            }
        }
    }

    pub(crate) fn capabilities(&self) -> RemoteCapabilities {
        match self {
            RemoteStore::File(_) => RemoteCapabilities::file(),
            RemoteStore::S3(remote) => {
                RemoteCapabilities::s3(remote.uses_sigv2(), remote.multipart_enabled)
            }
        }
    }
}

impl S3Remote {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.put_object(key, bytes, false).map(|_| ())
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        if self.uses_sigv2() {
            bail!("conditional put requires S3 Signature V4");
        }
        self.put_object(key, bytes, true)
    }

    fn put_file(&self, key: &str, source: &Path) -> Result<()> {
        self.put_file_object(key, source, false).map(|_| ())
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> Result<bool> {
        if self.uses_sigv2() {
            bail!("conditional put requires S3 Signature V4");
        }
        self.put_file_object(key, source, true)
    }

    fn put_file_object(&self, key: &str, source: &Path, if_absent: bool) -> Result<bool> {
        let len = fs::metadata(source)?.len();
        if self.should_use_multipart(len as usize) {
            if if_absent && self.exists(key)? {
                return Ok(false);
            }
            self.put_multipart_file(key, source, len)?;
            return Ok(true);
        }
        let remote_key = self.remote_key(key);
        let url = self.object_url(&remote_key);
        let response = if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}", self.bucket, remote_key);
            let auth = self.auth_v2("PUT", "", "application/octet-stream", &date, &path)?;
            let file = File::open(source)?;
            self.client
                .put(url)
                .header(DATE, date)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(AUTHORIZATION, auth)
                .body(Body::sized(file, len))
                .send()?
        } else {
            let payload_hash = sha256_file(source)?;
            let mut extra_headers = self.put_object_headers(key)?;
            if if_absent {
                extra_headers.push(("if-none-match".to_string(), "*".to_string()));
            }
            let auth = self.auth_v4("PUT", &remote_key, "", &payload_hash, &extra_headers)?;
            let mut request = self
                .client
                .put(url)
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .header(CONTENT_TYPE, "application/octet-stream");
            for (name, value) in extra_headers {
                request = request.header(name.as_str(), value.as_str());
            }
            let file = File::open(source)?;
            request.body(Body::sized(file, len)).send()?
        };
        record_s3_put(len);
        advise_path_dontneed(source);
        if if_absent && matches!(response.status().as_u16(), 409 | 412) {
            return Ok(false);
        }
        if !response.status().is_success() {
            bail!("s3 put failed for {key}: HTTP {}", response.status());
        }
        Ok(true)
    }

    fn put_object(&self, key: &str, bytes: &[u8], if_absent: bool) -> Result<bool> {
        if self.should_use_multipart(bytes.len()) {
            if if_absent && self.exists(key)? {
                return Ok(false);
            }
            self.put_multipart(key, bytes)?;
            return Ok(true);
        }
        let remote_key = self.remote_key(key);
        let url = self.object_url(&remote_key);
        let response = if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}", self.bucket, remote_key);
            let auth = self.auth_v2("PUT", "", "application/octet-stream", &date, &path)?;
            self.client
                .put(url)
                .header(DATE, date)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(AUTHORIZATION, auth)
                .body(bytes.to_vec())
                .send()?
        } else {
            let payload_hash = sha256_hex(bytes);
            let mut extra_headers = self.put_object_headers(key)?;
            if if_absent {
                extra_headers.push(("if-none-match".to_string(), "*".to_string()));
            }
            let auth = self.auth_v4("PUT", &remote_key, "", &payload_hash, &extra_headers)?;
            let mut request = self
                .client
                .put(url)
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .header(CONTENT_TYPE, "application/octet-stream");
            for (name, value) in extra_headers {
                request = request.header(name.as_str(), value.as_str());
            }
            request.body(bytes.to_vec()).send()?
        };
        record_s3_put(bytes.len() as u64);
        if if_absent && matches!(response.status().as_u16(), 409 | 412) {
            return Ok(false);
        }
        if !response.status().is_success() {
            bail!("s3 put failed for {key}: HTTP {}", response.status());
        }
        Ok(true)
    }

    pub(crate) fn put_multipart(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let remote_key = self.remote_key(key);
        let upload_id = self.initiate_multipart(key, &remote_key)?;
        let result = (|| {
            let mut parts = self.upload_multipart_parts(key, &remote_key, &upload_id, bytes)?;
            parts.sort_by_key(|part| part.part_number);
            self.complete_multipart(&remote_key, &upload_id, &parts)
        })();
        if result.is_err() {
            let _ = self.abort_multipart(&remote_key, &upload_id);
        }
        result.with_context(|| format!("multipart upload failed for {key}"))
    }

    fn put_multipart_file(&self, key: &str, source: &Path, len: u64) -> Result<()> {
        let remote_key = self.remote_key(key);
        let upload_id = self.initiate_multipart(key, &remote_key)?;
        let result = (|| {
            let mut parts =
                self.upload_multipart_file_parts(key, &remote_key, &upload_id, source, len)?;
            parts.sort_by_key(|part| part.part_number);
            self.complete_multipart(&remote_key, &upload_id, &parts)
        })();
        if result.is_err() {
            let _ = self.abort_multipart(&remote_key, &upload_id);
        }
        advise_path_dontneed(source);
        result.with_context(|| format!("multipart file upload failed for {key}"))
    }

    fn put_lifecycle_configuration(&self, policy_xml: &str) -> Result<()> {
        if self.uses_sigv2() {
            bail!("S3 lifecycle apply requires S3 Signature V4");
        }
        let query = "lifecycle=".to_string();
        let payload_hash = sha256_hex(policy_xml.as_bytes());
        let auth = self.auth_v4("PUT", "", &query, &payload_hash, &[])?;
        let response = self
            .client
            .put(self.bucket_url_query(&query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .header(CONTENT_TYPE, "application/xml")
            .body(policy_xml.to_string())
            .send()?;
        record_s3_put(policy_xml.len() as u64);
        if !response.status().is_success() {
            bail!(
                "s3 put lifecycle configuration failed: HTTP {} {}",
                response.status(),
                response.text().unwrap_or_default()
            );
        }
        Ok(())
    }

    fn lifecycle_configuration_xml(&self, policy: &serde_json::Value) -> Result<String> {
        let mut prefixed = policy.clone();
        if let Some(rules) = prefixed
            .get_mut("Rules")
            .and_then(|rules| rules.as_array_mut())
        {
            for rule in rules {
                if let Some(prefix) = rule
                    .pointer_mut("/Filter/Prefix")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
                {
                    rule["Filter"]["Prefix"] = serde_json::Value::String(self.remote_key(&prefix));
                }
            }
        }
        crate::majutsu_policy::s3_lifecycle_configuration_xml(&prefixed)
    }

    fn upload_multipart_parts(
        &self,
        key: &str,
        remote_key: &str,
        upload_id: &str,
        bytes: &[u8],
    ) -> Result<Vec<CompletedPart>> {
        let part_size = self.multipart_part_size_for_len(bytes.len());
        let chunks = bytes
            .chunks(part_size)
            .enumerate()
            .map(|(idx, chunk)| (idx + 1, chunk))
            .collect::<Vec<_>>();
        let mut parts = Vec::with_capacity(chunks.len());
        let parallelism = self.multipart_parallelism_for_key(key);
        for batch in chunks.chunks(parallelism) {
            let batch_parts = std::thread::scope(|scope| {
                let handles = batch
                    .iter()
                    .map(|(part_number, chunk)| {
                        scope.spawn(move || {
                            let etag =
                                self.upload_part(remote_key, upload_id, *part_number, chunk)?;
                            Ok(CompletedPart {
                                part_number: *part_number,
                                etag,
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .map_err(|_| anyhow!("multipart upload worker panicked"))?
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            parts.extend(batch_parts);
        }
        Ok(parts)
    }

    fn upload_multipart_file_parts(
        &self,
        key: &str,
        remote_key: &str,
        upload_id: &str,
        source: &Path,
        len: u64,
    ) -> Result<Vec<CompletedPart>> {
        let part_size = self.multipart_part_size_for_len(len as usize);
        let parallelism = self.multipart_file_parallelism_for_key(key);
        let mut file = File::open(source)?;
        advise_sequential(&file);
        let mut parts = Vec::new();
        let mut batch = Vec::<(usize, Vec<u8>)>::new();
        let mut part_number = 1usize;
        loop {
            let mut chunk = vec![0; part_size];
            let mut read = 0usize;
            while read < part_size {
                let n = file.read(&mut chunk[read..])?;
                if n == 0 {
                    break;
                }
                read += n;
            }
            if read == 0 {
                break;
            }
            chunk.truncate(read);
            batch.push((part_number, chunk));
            part_number += 1;
            if batch.len() >= parallelism {
                parts.extend(self.upload_multipart_owned_batch(remote_key, upload_id, &mut batch)?);
            }
        }
        if !batch.is_empty() {
            parts.extend(self.upload_multipart_owned_batch(remote_key, upload_id, &mut batch)?);
        }
        Ok(parts)
    }

    fn upload_multipart_owned_batch(
        &self,
        remote_key: &str,
        upload_id: &str,
        batch: &mut Vec<(usize, Vec<u8>)>,
    ) -> Result<Vec<CompletedPart>> {
        let current = std::mem::take(batch);
        std::thread::scope(|scope| {
            let handles = current
                .into_iter()
                .map(|(part_number, chunk)| {
                    scope.spawn(move || {
                        let etag =
                            self.upload_part_owned(remote_key, upload_id, part_number, chunk)?;
                        Ok(CompletedPart { part_number, etag })
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .map_err(|_| anyhow!("multipart file upload worker panicked"))?
                })
                .collect::<Result<Vec<_>>>()
        })
    }

    fn multipart_part_size_for_len(&self, len: usize) -> usize {
        adaptive_multipart_part_size(len, &self.endpoint)
    }

    fn multipart_parallelism_for_key(&self, key: &str) -> usize {
        if s3_object_class(key) == "metadata" {
            return env::var("MAJUTSU_S3_METADATA_MAX_PARALLEL_UPLOADS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_METADATA_MULTIPART_PARALLELISM)
                .min(self.max_parallel_uploads.max(1));
        }
        self.max_parallel_uploads.max(1)
    }

    fn multipart_file_parallelism_for_key(&self, key: &str) -> usize {
        env::var("MAJUTSU_S3_FILE_MAX_PARALLEL_UPLOADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or_else(|| self.multipart_parallelism_for_key(key).min(2))
    }

    pub(crate) fn multipart_initiate_headers(&self, key: &str) -> Result<Vec<(String, String)>> {
        self.put_object_headers(key)
    }

    fn initiate_multipart(&self, key: &str, remote_key: &str) -> Result<String> {
        let query = "uploads=".to_string();
        let payload_hash = sha256_hex(b"");
        let extra_headers = self.multipart_initiate_headers(key)?;
        let auth = self.auth_v4("POST", remote_key, &query, &payload_hash, &extra_headers)?;
        let mut request = self
            .client
            .post(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization);
        for (name, value) in extra_headers {
            request = request.header(name.as_str(), value.as_str());
        }
        let response = request.body(Vec::new()).send()?;
        record_s3_post(0, 0);
        if !response.status().is_success() {
            bail!(
                "s3 initiate multipart failed: HTTP {} {}",
                response.status(),
                response.text().unwrap_or_default()
            );
        }
        let text = response.text()?;
        S3_DOWNLOAD_BYTES.fetch_add(text.len() as u64, Ordering::Relaxed);
        parse_xml_text(&text, "UploadId")?.ok_or_else(|| anyhow!("missing multipart UploadId"))
    }

    fn upload_part(
        &self,
        remote_key: &str,
        upload_id: &str,
        part_number: usize,
        bytes: &[u8],
    ) -> Result<String> {
        let query = canonical_query(&[
            ("partNumber", part_number.to_string()),
            ("uploadId", upload_id.to_string()),
        ]);
        let payload_hash = sha256_hex(bytes);
        let auth = self.auth_v4("PUT", remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .put(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .body(bytes.to_vec())
            .send()?;
        record_s3_put(bytes.len() as u64);
        if !response.status().is_success() {
            bail!(
                "s3 upload part {part_number} failed: HTTP {} {}",
                response.status(),
                response.text().unwrap_or_default()
            );
        }
        response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string())
            .ok_or_else(|| anyhow!("s3 upload part {part_number} response had no ETag"))
    }

    fn upload_part_owned(
        &self,
        remote_key: &str,
        upload_id: &str,
        part_number: usize,
        bytes: Vec<u8>,
    ) -> Result<String> {
        let query = canonical_query(&[
            ("partNumber", part_number.to_string()),
            ("uploadId", upload_id.to_string()),
        ]);
        let bytes_len = bytes.len() as u64;
        let payload_hash = sha256_hex(&bytes);
        let auth = self.auth_v4("PUT", remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .put(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .body(bytes)
            .send()?;
        record_s3_put(bytes_len);
        if !response.status().is_success() {
            bail!(
                "s3 upload part {part_number} failed: HTTP {} {}",
                response.status(),
                response.text().unwrap_or_default()
            );
        }
        response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string())
            .ok_or_else(|| anyhow!("s3 upload part {part_number} response had no ETag"))
    }

    fn complete_multipart(
        &self,
        remote_key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<()> {
        let query = canonical_query(&[("uploadId", upload_id.to_string())]);
        let mut body = String::from("<CompleteMultipartUpload>");
        for part in parts {
            body.push_str("<Part>");
            body.push_str(&format!("<PartNumber>{}</PartNumber>", part.part_number));
            body.push_str("<ETag>");
            body.push_str(&xml_escape(&part.etag));
            body.push_str("</ETag>");
            body.push_str("</Part>");
        }
        body.push_str("</CompleteMultipartUpload>");
        let body_len = body.len() as u64;
        let payload_hash = sha256_hex(body.as_bytes());
        let auth = self.auth_v4("POST", remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .post(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .send()?;
        record_s3_post(body_len, 0);
        if !response.status().is_success() {
            bail!(
                "s3 complete multipart failed: HTTP {} {}",
                response.status(),
                response.text().unwrap_or_default()
            );
        }
        Ok(())
    }

    fn abort_multipart(&self, remote_key: &str, upload_id: &str) -> Result<()> {
        let query = canonical_query(&[("uploadId", upload_id.to_string())]);
        let payload_hash = sha256_hex(b"");
        let auth = self.auth_v4("DELETE", remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .delete(self.object_url_query(remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .body(Vec::new())
            .send()?;
        record_s3_delete();
        if response.status().is_success() {
            Ok(())
        } else {
            bail!("s3 abort multipart failed: HTTP {}", response.status())
        }
    }

    pub(crate) fn restore_archive(&self, key: &str, days: u32, tier: &str) -> Result<bool> {
        let remote_key = self.remote_key(key);
        let query = "restore=".to_string();
        let body = s3_archive_restore_request_xml(days, tier)?;
        let body_len = body.len() as u64;
        if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}?restore", self.bucket, remote_key);
            let auth = self.auth_v2("POST", "", "application/xml", &date, &path)?;
            let response = self
                .client
                .post(self.object_url_query(&remote_key, &query))
                .header(DATE, date)
                .header(CONTENT_TYPE, "application/xml")
                .header(AUTHORIZATION, auth)
                .body(body)
                .send()?;
            record_s3_post(body_len, 0);
            return archive_restore_status(key, response.status().as_u16());
        }
        let payload_hash = sha256_hex(body.as_bytes());
        let auth = self.auth_v4("POST", &remote_key, &query, &payload_hash, &[])?;
        let response = self
            .client
            .post(self.object_url_query(&remote_key, &query))
            .header(HOST, self.host_header()?)
            .header("x-amz-date", auth.amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header(AUTHORIZATION, auth.authorization)
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .send()?;
        record_s3_post(body_len, 0);
        archive_restore_status(key, response.status().as_u16())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.get_with_range(key, None)
    }

    fn get_optional(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.get_with_range_optional(key, None)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let remote_key = self.remote_key(key);
        let response = if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}", self.bucket, remote_key);
            let auth = self.auth_v2("DELETE", "", "", &date, &path)?;
            self.client
                .delete(self.object_url(&remote_key))
                .header(DATE, date)
                .header(AUTHORIZATION, auth)
                .send()?
        } else {
            let payload_hash = sha256_hex(b"");
            let auth = self.auth_v4("DELETE", &remote_key, "", &payload_hash, &[])?;
            self.client
                .delete(self.object_url(&remote_key))
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .send()?
        };
        record_s3_delete();
        if response.status().is_success() || response.status().as_u16() == 404 {
            Ok(())
        } else {
            bail!("s3 delete failed for {key}: HTTP {}", response.status())
        }
    }

    fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        let end = start
            .checked_add(len)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| anyhow!("invalid range {start}+{len}"))?;
        self.get_with_range(key, Some(format!("bytes={start}-{end}")))
    }

    fn get_with_range(&self, key: &str, range: Option<String>) -> Result<Vec<u8>> {
        self.get_with_range_optional(key, range)?
            .ok_or_else(|| anyhow!("s3 get failed for {key}: HTTP 404"))
    }

    fn get_with_range_optional(&self, key: &str, range: Option<String>) -> Result<Option<Vec<u8>>> {
        let remote_key = self.remote_key(key);
        let response = if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}", self.bucket, remote_key);
            let auth = self.auth_v2("GET", "", "", &date, &path)?;
            let mut request = self
                .client
                .get(self.object_url(&remote_key))
                .header(DATE, date)
                .header(AUTHORIZATION, auth);
            if let Some(range) = &range {
                request = request.header(RANGE, range);
            }
            request.send()?
        } else {
            let payload_hash = sha256_hex(b"");
            let mut extra = Vec::new();
            if let Some(range) = &range {
                extra.push(("range".to_string(), range.clone()));
            }
            let auth = self.auth_v4("GET", &remote_key, "", &payload_hash, &extra)?;
            let mut request = self
                .client
                .get(self.object_url(&remote_key))
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization);
            if let Some(range) = &range {
                request = request.header(RANGE, range);
            }
            request.send()?
        };
        if !response.status().is_success() && response.status().as_u16() == 404 {
            record_s3_get(0);
            return Ok(None);
        }
        if !response.status().is_success() {
            record_s3_get(0);
            bail!("s3 get failed for {key}: HTTP {}", response.status());
        }
        let bytes = response.bytes()?.to_vec();
        record_s3_get(bytes.len() as u64);
        Ok(Some(bytes))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let remote_key = self.remote_key(key);
        let response = if self.uses_sigv2() {
            let date = http_date();
            let path = format!("/{}/{}", self.bucket, remote_key);
            let auth = self.auth_v2("HEAD", "", "", &date, &path)?;
            self.client
                .head(self.object_url(&remote_key))
                .header(DATE, date)
                .header(AUTHORIZATION, auth)
                .send()?
        } else {
            let payload_hash = sha256_hex(b"");
            let auth = self.auth_v4("HEAD", &remote_key, "", &payload_hash, &[])?;
            self.client
                .head(self.object_url(&remote_key))
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .send()?
        };
        record_s3_head();
        if response.status().is_success() {
            Ok(true)
        } else if response.status().as_u16() == 404 {
            Ok(false)
        } else {
            bail!("s3 head failed for {key}: HTTP {}", response.status());
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .list_with_sizes(prefix)?
            .into_iter()
            .map(|object| object.key)
            .collect())
    }

    fn list_with_sizes(&self, prefix: &str) -> Result<Vec<RemoteObjectStat>> {
        let mut objects = Vec::new();
        let remote_prefix = self.remote_key(prefix);
        let mut continuation_token: Option<String> = None;
        loop {
            let mut query = canonical_query(&[
                ("list-type", "2".to_string()),
                ("prefix", remote_prefix.clone()),
            ]);
            if let Some(token) = continuation_token.as_deref() {
                query = canonical_query(&[
                    ("continuation-token", token.to_string()),
                    ("list-type", "2".to_string()),
                    ("prefix", remote_prefix.clone()),
                ]);
            }
            let xml = self.list_objects_page(&query)?;
            let page = parse_s3_list_objects_v2(&xml)?;
            for object in page.objects {
                if let Some(local) = self.local_key(&object.key) {
                    objects.push(RemoteObjectStat {
                        key: local,
                        size: object.size,
                    });
                }
            }
            if !page.is_truncated {
                break;
            }
            continuation_token = page.next_continuation_token;
            if continuation_token.is_none() {
                bail!("s3 list response was truncated but did not include NextContinuationToken");
            }
        }
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        objects.dedup_by(|left, right| left.key == right.key);
        Ok(objects)
    }

    fn list_objects_page(&self, query: &str) -> Result<String> {
        let response = if self.uses_sigv2() {
            let date = http_date();
            let resource = format!("/{}/", self.bucket);
            let auth = self.auth_v2("GET", "", "", &date, &resource)?;
            self.client
                .get(self.bucket_url_query(query))
                .header(DATE, date)
                .header(AUTHORIZATION, auth)
                .send()?
        } else {
            let payload_hash = sha256_hex(b"");
            let auth = self.auth_v4("GET", "", query, &payload_hash, &[])?;
            self.client
                .get(self.bucket_url_query(query))
                .header(HOST, self.host_header()?)
                .header("x-amz-date", auth.amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header(AUTHORIZATION, auth.authorization)
                .send()?
        };
        if !response.status().is_success() {
            record_s3_list(0);
            bail!("s3 list failed: HTTP {}", response.status());
        }
        let text = response.text()?;
        record_s3_list(text.len() as u64);
        Ok(text)
    }

    fn auth_v2(
        &self,
        method: &str,
        md5: &str,
        content_type: &str,
        date: &str,
        resource: &str,
    ) -> Result<String> {
        let canonical = format!("{method}\n{md5}\n{content_type}\n{date}\n{resource}");
        let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(self.secret_key.as_bytes())?;
        mac.update(canonical.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        Ok(format!("AWS {}:{}", self.access_key, signature))
    }

    pub(crate) fn auth_v4(
        &self,
        method: &str,
        remote_key: &str,
        canonical_query: &str,
        payload_hash: &str,
        extra_headers: &[(String, String)],
    ) -> Result<SigV4Auth> {
        let now = Utc::now();
        let datestamp = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let canonical_uri = if remote_key.is_empty() {
            format!("/{}/", self.bucket)
        } else {
            format!("/{}/{}", self.bucket, uri_encode(remote_key, false))
        };
        let mut headers = vec![
            ("host".to_string(), self.host_header()?),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        headers.extend(extra_headers.iter().cloned());
        headers.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_headers = headers
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let signed_headers = headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{}/{}/s3/aws4_request", datestamp, self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let signing_key = self.sigv4_signing_key(&datestamp)?;
        let signature = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes())?;
        Ok(SigV4Auth {
            amz_date,
            authorization: format!(
                "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
                self.access_key, scope, signed_headers, signature
            ),
        })
    }

    fn sigv4_signing_key(&self, datestamp: &str) -> Result<Vec<u8>> {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.secret_key).as_bytes(),
            datestamp.as_bytes(),
        )?;
        let k_region = hmac_sha256(&k_date, self.region.as_bytes())?;
        let k_service = hmac_sha256(&k_region, b"s3")?;
        hmac_sha256(&k_service, b"aws4_request")
    }

    pub(crate) fn put_object_headers(&self, key: &str) -> Result<Vec<(String, String)>> {
        let mut headers = Vec::new();
        if let Some(storage_class) = &self.storage_class {
            headers.push(("x-amz-storage-class".to_string(), storage_class.clone()));
        }
        if !self.object_tags.is_empty() {
            let mut tags = vec![(
                "majutsu-class".to_string(),
                s3_object_class(key).to_string(),
            )];
            tags.extend(self.object_tags.iter().cloned());
            headers.push(("x-amz-tagging".to_string(), encode_s3_object_tags(&tags)?));
        }
        Ok(headers)
    }

    pub(crate) fn uses_sigv2(&self) -> bool {
        self.signature_version.contains('2')
    }

    fn host_header(&self) -> Result<String> {
        let url = Url::parse(&self.endpoint)?;
        Ok(url
            .host_str()
            .ok_or_else(|| anyhow!("endpoint has no host: {}", self.endpoint))?
            .to_string())
    }

    fn object_url(&self, remote_key: &str) -> String {
        format!(
            "{}/{}/{}",
            self.endpoint.trim_end_matches('/'),
            self.bucket,
            remote_key
        )
    }

    fn object_url_query(&self, remote_key: &str, query: &str) -> String {
        format!("{}?{}", self.object_url(remote_key), query)
    }

    fn bucket_url_query(&self, query: &str) -> String {
        format!(
            "{}/{}/?{}",
            self.endpoint.trim_end_matches('/'),
            self.bucket,
            query
        )
    }

    fn multipart_threshold(&self) -> usize {
        env::var("MAJUTSU_S3_MULTIPART_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MULTIPART_THRESHOLD)
            .max(MIN_MULTIPART_PART_SIZE)
    }

    pub(crate) fn should_use_multipart(&self, len: usize) -> bool {
        self.multipart_enabled && !self.uses_sigv2() && len >= self.multipart_threshold()
    }

    pub(crate) fn remote_key(&self, key: &str) -> String {
        let clean = key.trim_start_matches('/');
        if self.prefix.is_empty() {
            clean.to_string()
        } else if clean.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix.trim_matches('/'), clean)
        }
    }

    fn local_key(&self, remote_key: &str) -> Option<String> {
        if self.prefix.is_empty() {
            Some(remote_key.to_string())
        } else {
            remote_key
                .strip_prefix(&format!("{}/", self.prefix.trim_matches('/')))
                .map(|s| s.to_string())
        }
    }
}

pub(crate) struct SigV4Auth {
    pub(crate) amz_date: String,
    pub(crate) authorization: String,
}

struct CompletedPart {
    part_number: usize,
    etag: String,
}

fn list_file_remote(root: &Path, prefix: &str) -> Result<Vec<String>> {
    Ok(list_file_remote_with_sizes(root, prefix)?
        .into_iter()
        .map(|object| object.key)
        .collect())
}

fn list_file_remote_with_sizes(root: &Path, prefix: &str) -> Result<Vec<RemoteObjectStat>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut objects = Vec::new();
    for entry in WalkDir::new(root).sort_by_file_name() {
        let entry = entry?;
        if entry.file_type().is_file() {
            let rel = path_to_slash(entry.path().strip_prefix(root)?);
            if rel.starts_with(prefix) {
                objects.push(RemoteObjectStat {
                    key: rel,
                    size: entry.metadata()?.len(),
                });
            }
        }
    }
    Ok(objects)
}

fn path_to_slash(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn http_date() -> String {
    Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    advise_sequential(&file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    advise_dontneed(&file);
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(any(target_os = "android", target_os = "linux"))]
fn advise_sequential(file: &File) {
    use std::os::fd::AsRawFd;
    let _ = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL) };
}

#[cfg(not(any(target_os = "android", target_os = "linux")))]
fn advise_sequential(_file: &File) {}

#[cfg(any(target_os = "android", target_os = "linux"))]
fn advise_dontneed(file: &File) {
    use std::os::fd::AsRawFd;
    let _ = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
}

#[cfg(not(any(target_os = "android", target_os = "linux")))]
fn advise_dontneed(_file: &File) {}

fn advise_path_dontneed(path: &Path) {
    if let Ok(file) = File::open(path) {
        advise_dontneed(&file);
    }
}

fn hmac_sha256(key: &[u8], bytes: &[u8]) -> Result<Vec<u8>> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)?;
    mac.update(bytes);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_sha256_hex(key: &[u8], bytes: &[u8]) -> Result<String> {
    Ok(hex::encode(hmac_sha256(key, bytes)?))
}

fn canonical_query(params: &[(&str, String)]) -> String {
    let mut pairs = params
        .iter()
        .map(|(key, value)| (uri_encode(key, true), uri_encode(value, true)))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

struct S3ListPage {
    objects: Vec<RemoteObjectStat>,
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

fn parse_s3_list_objects_v2(body: &str) -> Result<S3ListPage> {
    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(true);
    let mut current = String::new();
    let mut in_contents = false;
    let mut objects = Vec::new();
    let mut object_key: Option<String> = None;
    let mut object_size: Option<u64> = None;
    let mut is_truncated = false;
    let mut next_continuation_token = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => {
                current = std::str::from_utf8(event.name().as_ref())?.to_string();
                if current == "Contents" {
                    in_contents = true;
                    object_key = None;
                    object_size = None;
                }
            }
            Ok(Event::End(event)) => {
                if event.name().as_ref() == b"Contents" {
                    if let Some(key) = object_key.take() {
                        objects.push(RemoteObjectStat {
                            key,
                            size: object_size.unwrap_or(0),
                        });
                    }
                    in_contents = false;
                }
                current.clear();
            }
            Ok(Event::Text(text)) => {
                let value = text.unescape()?.into_owned();
                match current.as_str() {
                    "Key" if in_contents => object_key = Some(value),
                    "Size" if in_contents => object_size = Some(value.parse()?),
                    "IsTruncated" => is_truncated = value == "true" || value == "1",
                    "NextContinuationToken" => next_continuation_token = Some(value),
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(err.into()),
            _ => {}
        }
    }
    Ok(S3ListPage {
        objects,
        is_truncated,
        next_continuation_token,
    })
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => {
            let value = value.trim().to_string();
            if value.is_empty() {
                Ok(None)
            } else if value.contains('\n') || value.contains('\r') {
                bail!("{name} must not contain newlines")
            } else {
                Ok(Some(value))
            }
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn parse_s3_object_tags_env() -> Result<Vec<(String, String)>> {
    let Some(value) = optional_env("MAJUTSU_S3_OBJECT_TAGS")? else {
        return Ok(Vec::new());
    };
    parse_s3_object_tags(&value)
}

fn parse_s3_object_tags(input: &str) -> Result<Vec<(String, String)>> {
    input
        .split('&')
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| anyhow!("S3 object tag must be key=value: {part}"))?;
            let key = key.trim();
            let value = value.trim();
            validate_s3_tag_part("S3 object tag key", key)?;
            validate_s3_tag_part("S3 object tag value", value)?;
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

fn validate_s3_tag_part(label: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{label} must not be empty");
    }
    if value.contains('\n') || value.contains('\r') {
        bail!("{label} must not contain newlines");
    }
    Ok(())
}

fn encode_s3_object_tags(tags: &[(String, String)]) -> Result<String> {
    tags.iter()
        .map(|(key, value)| {
            validate_s3_tag_part("S3 object tag key", key)?;
            validate_s3_tag_part("S3 object tag value", value)?;
            Ok(format!(
                "{}={}",
                uri_encode(key, true),
                uri_encode(value, true)
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("&"))
}

pub(crate) fn s3_object_class(key: &str) -> &'static str {
    let key = key.trim_start_matches('/');
    if key.starts_with("hosts/")
        || key.starts_with("metadata/")
        || key.ends_with("/metadata/export.json")
    {
        "metadata"
    } else if key.starts_with("refs/") || key.contains("/refs/") || key.ends_with("current") {
        "ref"
    } else if key.starts_with("objects/trees/") || key.starts_with("trees/") {
        "tree"
    } else if key.starts_with("objects/packs/") || key.starts_with("packs/") {
        "pack"
    } else if key.starts_with("objects/large/") || key.starts_with("large/") {
        "large"
    } else if key.starts_with("objects/indexes/") || key.starts_with("indexes/") {
        "index"
    } else if key.starts_with("objects/blobs/") || key.starts_with("blobs/") {
        "blob"
    } else {
        "object"
    }
}

fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::new();
    for byte in input.as_bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (*byte == b'/' && !encode_slash);
        if keep {
            out.push(*byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn parse_xml_text(xml: &str, tag: &str) -> Result<Option<String>> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut in_tag = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) if e.name().as_ref() == tag.as_bytes() => in_tag = true,
            Ok(Event::End(e)) if e.name().as_ref() == tag.as_bytes() => in_tag = false,
            Ok(Event::Text(e)) if in_tag => return Ok(Some(e.unescape()?.into_owned())),
            Ok(Event::Eof) => return Ok(None),
            Err(err) => return Err(err.into()),
            _ => {}
        }
    }
}

fn xml_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod storage_characteristic_tests {
    use super::*;

    #[test]
    fn adaptive_part_size_prefers_smaller_local_parts_and_obeys_part_limit() {
        assert_eq!(
            adaptive_multipart_part_size(64 * 1024 * 1024, "http://127.0.0.1:9000"),
            DEFAULT_LOCAL_MULTIPART_PART_SIZE
        );
        assert_eq!(
            adaptive_multipart_part_size(64 * 1024 * 1024, "https://storage.googleapis.com"),
            DEFAULT_CLOUD_MULTIPART_PART_SIZE
        );
        let huge = (DEFAULT_MAX_MULTIPART_PARTS + 1) * MIN_MULTIPART_PART_SIZE;
        assert!(
            adaptive_multipart_part_size(huge, "https://s3.amazonaws.com")
                > MIN_MULTIPART_PART_SIZE
        );
    }

    #[test]
    fn metadata_multipart_parallelism_is_capped_by_default() {
        let remote = S3Remote {
            bucket: "bucket".into(),
            prefix: String::new(),
            endpoint: "https://storage.googleapis.com".into(),
            region: "auto".into(),
            signature_version: "s3v4".into(),
            access_key: "key".into(),
            secret_key: "secret".into(),
            storage_class: None,
            object_tags: Vec::new(),
            multipart_enabled: true,
            max_parallel_uploads: 8,
            client: s3_http_client().unwrap(),
        };
        assert_eq!(
            remote.multipart_parallelism_for_key("metadata/export.json"),
            DEFAULT_METADATA_MULTIPART_PARALLELISM
        );
        assert_eq!(
            remote.multipart_parallelism_for_key("hosts/host-id/metadata/export.json"),
            DEFAULT_METADATA_MULTIPART_PARALLELISM
        );
        assert_eq!(
            remote.multipart_parallelism_for_key("blobs/loose/aa/blob.enc"),
            8
        );
    }

    #[test]
    fn remote_traffic_metrics_count_requests_and_body_bytes() {
        reset_remote_traffic_metrics();

        record_s3_put(10);
        record_s3_get(20);
        record_s3_head();
        record_s3_list(30);
        record_s3_post(40, 50);
        record_s3_delete();

        let metrics = remote_traffic_metrics();
        assert_eq!(metrics.total_requests(), 6);
        assert_eq!(metrics.put_requests, 1);
        assert_eq!(metrics.get_requests, 1);
        assert_eq!(metrics.head_requests, 1);
        assert_eq!(metrics.list_requests, 1);
        assert_eq!(metrics.post_requests, 1);
        assert_eq!(metrics.delete_requests, 1);
        assert_eq!(metrics.upload_bytes, 50);
        assert_eq!(metrics.download_bytes, 100);
    }

    #[test]
    fn parses_paginated_s3_list_v2_response() {
        let xml = r#"<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>prefix/a</Key><Size>123</Size></Contents><Contents><Key>prefix/b</Key><Size>456</Size></Contents><NextContinuationToken>next-token</NextContinuationToken></ListBucketResult>"#;
        let page = parse_s3_list_objects_v2(xml).unwrap();
        assert!(page.is_truncated);
        assert_eq!(
            page.objects,
            vec![
                RemoteObjectStat {
                    key: "prefix/a".to_string(),
                    size: 123,
                },
                RemoteObjectStat {
                    key: "prefix/b".to_string(),
                    size: 456,
                }
            ]
        );
        assert_eq!(page.next_continuation_token.as_deref(), Some("next-token"));
    }
}
