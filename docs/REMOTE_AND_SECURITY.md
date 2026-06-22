# Remote And Security

This page covers remote storage, shared buckets, encryption, and safety
boundaries. For first-time setup, start with [Getting Started](GETTING_STARTED.md).

## Remote sync

`mj sync` writes hot metadata and all referenced local objects to the configured
remote. Remote metadata is the critical path for host-disk-loss recovery: a
fresh state directory can be reconstructed from remote metadata.

For S3-compatible storage:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT_URL=https://storage.googleapis.com
export AWS_SIGNATURE_VERSION=s3v4

mj init --remote s3://bucket/prefix
mj root add sample /path/to/sample
mj snapshot --message 'first remote snapshot'
mj sync --wait
mj remote check
mj remote capabilities
mj remote hosts
mj remote fsck
```

Podman can run local MinIO for S3-compatible E2E validation:

```sh
scripts/e2e-minio.sh
```

`[remote]` also accepts the split config form used in the spec:

```toml
[remote]
type = "s3"
bucket = "my-majutsu-backup"
prefix = "majutsu/v1/workstation"
endpoint = "https://storage.googleapis.com"
region = "us-east-1"
signature_version = "s3v4"
```

For local validation:

```toml
[remote]
type = "file"
path = "/tmp/majutsu-remote"
```

## Shared bucket design

Majutsu can use one S3/GCS bucket for multiple environments. Each state has a
host id, and host-scoped metadata is stored under paths such as:

```text
hosts/<host-id>/metadata/export.json
hosts/<host-id>/refs/current
hosts/<host-id>/refs/last-synced
hosts/<host-id>/snapshots/...
hosts/<host-id>/ops/...
```

Payload objects are content-addressed and may be reused when identical content
appears across histories. This lets several hosts share one bucket/prefix while
remaining selectable by host:

```sh
mj remote hosts
mj --home /tmp/recovered clone --remote s3://bucket/prefix --host workstation-a
```

Use separate prefixes for unrelated projects, trust domains, or retention
policies:

```text
s3://bucket/majutsu/personal
s3://bucket/majutsu/workstations
s3://bucket/majutsu/system
```

## Remote object layout

Majutsu publishes canonical remote-layout aliases for trees, blobs, packs,
indexes, large manifests/chunks, and host operation logs. It also keeps
backward-compatible metadata where needed for clone and restore compatibility.

`mj remote fsck` verifies canonical `hosts/index.json`, each host metadata
export, canonical per-host refs, snapshot/operation exports, aggregate
operation logs, and every referenced object through canonical or legacy keys.

To rebuild an empty state directory from remote:

```sh
mj --home /tmp/recovered-majutsu clone --remote s3://bucket/prefix
mj --home /tmp/recovered-other clone --remote s3://bucket/prefix --host test-host
mj --home /tmp/recovered-majutsu fsck
mj --home /tmp/recovered-majutsu restore apply --to /tmp/restore
```

When `hosts/index.json` contains multiple hosts, clone requires `--host`. If a
host name matches multiple entries, use the host id shown by `mj remote hosts`.
Duplicate host ids or metadata keys are treated as remote metadata corruption.

## S3 security defaults

The S3 backend uses path-style requests. AWS Signature V4 is the default; set
`AWS_SIGNATURE_VERSION=s3v2` only for legacy S3-compatible services that still
require the older signature style.

Non-local plaintext HTTP endpoints are rejected by default. Use HTTPS for real
remotes. For trusted local testing, `http://127.0.0.1` and `http://localhost`
are allowed, and `MAJUTSU_ALLOW_INSECURE_REMOTE=1` can explicitly opt in to an
insecure endpoint.

HTTP redirect following is disabled for S3 requests so credentials are not
silently sent to an unexpected endpoint.

Multipart upload is controlled by `MAJUTSU_S3_MULTIPART_THRESHOLD`; the minimum
effective threshold is 5 MiB because S3 requires non-final parts to be at least
that size.

For providers that support them, `MAJUTSU_S3_STORAGE_CLASS` adds
`x-amz-storage-class` on object creation and `MAJUTSU_S3_OBJECT_TAGS` adds
`x-amz-tagging` in `key=value&key=value` form. Leave these variables unset for
providers that reject those headers.

## Encryption

New state can encrypt local and remote object payloads:

```sh
mj init --encrypt --remote s3://bucket/prefix
```

`mj init --encrypt` writes `[security] encryption = "age"` in `config.toml`.
Objects are encrypted for the generated age recipient when a keyring is
present; the older `MJENC1`/ChaCha20-Poly1305 envelope is still accepted for
existing state.

For encrypted state, content object paths are derived with HMAC-SHA256 from the
master key and the internal content id, so remote object keys do not expose raw
plaintext hashes. Existing plaintext objects remain readable for compatibility.

The master key is stored locally at:

```text
$MAJUTSU_HOME/keys/master.key
```

Export it and store it separately from the host:

```sh
mj key export
```

To recover from remote storage into a fresh state, provide the master key during
clone:

```sh
MAJUTSU_MASTER_KEY=<64-hex-key> mj --home /tmp/recovered clone --remote s3://bucket/prefix
```

Encrypted clone refuses to proceed without the master key so recovered states
can continue deriving the same HMAC object-key namespace.

Rotate the master key and rewrite encrypted object metadata:

```sh
mj key rotate
mj key rotate --new-key <64-hex-key>
mj sync
```

Rotation rewrites encrypted blobs, chunks, large manifests, and snapshot/tree
metadata. Packed blobs are unpacked during rotation so their object keys can be
derived from the new master key.

## Snapshot modes and hooks

Roots default to coalesced snapshot behavior:

```sh
mj root add docs ~/Documents --snapshot-mode default
```

Strict mode retries stable reads more aggressively:

```sh
mj root add config /etc/myapp --snapshot-mode strict
```

Transactional mode runs hooks before and after scanning the root. This is the
intended path for application state, database dumps, VM images, and other data
that needs an application-consistent checkpoint:

```sh
mj root add app-data /srv/app/data \
  --snapshot-mode transactional \
  --pre-snapshot '/usr/local/bin/app-checkpoint begin' \
  --post-snapshot '/usr/local/bin/app-checkpoint end'
```

If a hook creates a filesystem snapshot or dump directory, keep the root path as
the restore destination identity and read from that snapshot source:

```sh
mj root add app-data /srv/app/data \
  --snapshot-mode transactional \
  --snapshot-source /mnt/app-data-snapshot \
  --pre-snapshot '/usr/local/bin/create-app-snapshot /mnt/app-data-snapshot' \
  --post-snapshot '/usr/local/bin/remove-app-snapshot /mnt/app-data-snapshot'
```

Remote clone quarantines hook/plugin commands by default. Use
`mj clone --trust-remote-hooks` only when the remote metadata is trusted.

## System instance

For root-owned host configuration such as `/etc/systemd/system`,
root-readable environment files, and local service scripts, use a separate
system instance instead of running the user instance with sudo:

```sh
sudo mj --system init --encrypt --remote s3://bucket/prefix/system
sudo mj --system root add systemd-system /etc/systemd/system --include '**'
sudo mj --system daemon service --provider systemd --scope system \
  > /etc/systemd/system/majutsu.service
sudo systemctl enable --now majutsu.service
```

`mj --system` resolves state home from `/etc/majutsu/config.toml` and then
falls back to `/var/lib/majutsu`. Keep the system backend prefix and encryption
key separate from the user instance.

## Lifecycle policy

Generate provider lifecycle policy templates:

```sh
mj lifecycle policy --provider gcs
mj lifecycle policy --provider s3
mj lifecycle status
mj lifecycle apply --provider s3
mj lifecycle apply --provider s3 --dry-run=false
```

`lifecycle apply` is a dry run by default. With `--dry-run=false` on an
S3-compatible remote, Majutsu sends `PUT ?lifecycle` to apply the bucket
lifecycle configuration and records versioned lifecycle artifacts on the remote.

Rules are derived from `[tiering]` in `config.toml`. Transition rules map
portable storage names such as `infrequent`, `archive`, and `deep-archive` to
provider-specific storage classes.
