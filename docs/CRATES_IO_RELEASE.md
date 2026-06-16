# crates.io release

`majutsu` is published as a single public crate. The install target is the root
`majutsu` crate, which provides the `mj` binary:

```sh
cargo install majutsu
```

## Current package shape

Only the root `majutsu` package is publishable. The support crates under
`crates/` are repository-local workspace boundaries and are marked
`publish = false`:

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
```

The published root crate embeds the support source under `src/internal/`, so
the crates.io package has no dependency on private `majutsu-*` crates. This
avoids publishing unstable internal API crates and keeps `cargo install
majutsu` self-contained.

When changing a private support crate, refresh the embedded source before
running release checks:

```sh
scripts/sync-internal-crates.sh
```

The sync helper discovers private support crates from `cargo metadata`; any
workspace package named `majutsu-*` with `publish = false` is expected to have a
matching `src/internal/*.rs` mirror.

## Recommended process

1. Run the local release gate.

   ```sh
   scripts/check-completion.sh
   ```

   The gate includes `scripts/sync-internal-crates.sh --check` so the
   publish-facing `src/internal/` mirror cannot drift from the private
   workspace support crates. The check also rejects unexpected stale mirror
   files.

2. Verify package metadata with dry-run publishing.

   ```sh
   scripts/publish-crates-io.sh
   ```

   The helper publishes only the public `majutsu` crate. It skips a package
   version that already exists on crates.io by default. Use
   `--no-skip-existing` only when checking the exact package command output for
   an unpublished version.

   Release archives are generated under `target/dist/` by default so they do
   not make the repository dirty for `cargo publish`. Set `MAJUTSU_DIST_DIR`
   only when an explicit artifact destination is needed.

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

crates.io can apply rate limits to publish requests. The publish helper retries
`Too Many Requests` responses in execute mode, sleeping for
`PUBLISH_RETRY_SECS` seconds, defaulting to 610 seconds.

The helper also skips already-published versions before calling
`cargo publish`, which avoids unnecessary registry requests during reruns.

## Future cleanup

The current split is useful inside the repository, but not every internal crate
is a public API. Keep support crates private until a stable library API is
intentionally designed. If a public library crate is added later, release it as
a separate compatibility commitment instead of exposing the current internal
workspace crates by accident.
