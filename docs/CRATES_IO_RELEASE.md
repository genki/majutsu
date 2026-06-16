# crates.io release

`majutsu` is published as a Cargo workspace. The user-facing install target is
the root `majutsu` crate, which provides the `mj` binary:

```sh
cargo install majutsu
```

## Current package shape

The 0.4.0 release publishes these crates because the root binary crate depends
on the internal workspace crates:

```text
majutsu-core
majutsu-cli
majutsu-crypto
majutsu-daemon
majutsu-db
majutsu-policy
majutsu-watch
majutsu-large
majutsu-pack
majutsu-restore
majutsu-store
majutsu
```

The dependency order matters. Crates that depend on `majutsu-core` must be
published after `majutsu-core`, and the root `majutsu` crate must be published
last.

## Recommended process

1. Run the local release gate.

   ```sh
   scripts/check-completion.sh
   ```

2. Verify package metadata and dependency order with dry-run publishing.

   ```sh
   scripts/publish-crates-io.sh
   ```

   The helper requires `jq` because it reads package versions from
   `cargo metadata`. It skips package versions that already exist on crates.io
   by default. Use `--no-skip-existing` only when checking the exact package
   command output for an unpublished version.

3. Publish only after the dry-run passes and the release commit is pushed.

   ```sh
   export CARGO_REGISTRY_TOKEN=...
   scripts/publish-crates-io.sh --execute
   ```

4. Verify installation from crates.io.

   ```sh
   cargo install majutsu --version 0.4.2 --locked --root "$(mktemp -d)"
   ```

## Version display policy

Published crates.io and GitHub release versions use clean SemVer package
versions, such as `0.4.2`. The released `mj --version` output should match the
package version:

```text
mj 0.4.2
```

Build metadata such as `+build.N` is reserved for local development builds and
other non-published diagnostics. To include it explicitly, build with:

```sh
MAJUTSU_DEV_BUILD=1 cargo build
```

That produces version output such as:

```text
mj 0.4.2+build.3
```

`BUILD_NUMBER` can still be incremented for local batches, but it must not be
used as the crates.io or GitHub release identifier.

## Rate limits

crates.io applies a strict rate limit to publishing many new crates in a short
period. The publish helper retries `Too Many Requests` responses in execute
mode, sleeping for `PUBLISH_RETRY_SECS` seconds, defaulting to 610 seconds.

The helper also skips already-published versions before calling
`cargo publish`, which avoids unnecessary registry requests during reruns.

For future releases of already-created crates, this limit should be less
painful than the first 0.4.0 publish.

## Future cleanup

The current split is useful inside the repository, but not every internal crate
is necessarily a public API. For a cleaner long-term publishing model, prefer
one of these directions:

- Keep publishing the workspace crates as a coordinated set and use
  `scripts/publish-crates-io.sh` or a release manager such as `cargo-release`.
- Collapse purely internal crates back into the root package before publishing.
- Keep only stable public library crates publishable and mark private internal
  crates with `publish = false`; this requires the root published crate not to
  depend on unpublished path-only packages.
