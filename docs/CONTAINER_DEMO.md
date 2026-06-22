# Container Demo

Use `scripts/demo-container.sh` to run majutsu in a disposable Linux container.
The default and recommended runtime is Podman. The script is intended for clean
demos where the host's majutsu state and roots must not be touched.

## What It Does

- Builds `target/release/mj` on the host if it does not already exist.
- Starts a temporary Podman or Docker container.
- Mounts only the `mj` binary and a temporary demo directory into the
  container.
- Creates `/demo/state`, `/demo/root`, `/demo/remote`, and `/demo/restore`
  inside that temporary directory.
- Runs `init`, `root add`, `snapshot`, `log`, `state`, `sync`, `restore plan`,
  and `restore apply`.
- Deletes the temporary demo directory after the container exits.

The demo uses a file remote:

```text
file:///demo/remote
```

This keeps the demo self-contained and avoids touching S3 credentials or a
production backend.

## Run

```sh
scripts/demo-container.sh
```

The default image is `docker.io/library/ubuntu:24.04`. The demo mounts the host
build of `mj`, so the container image must provide a compatible glibc and
OpenSSL runtime. Override it when needed:

```sh
MAJUTSU_DEMO_IMAGE=docker.io/library/ubuntu:24.04 scripts/demo-container.sh
```

The script prefers Podman and falls back to Docker only when Podman is not
installed. To force a runtime:

```sh
MAJUTSU_CONTAINER_BIN=docker scripts/demo-container.sh
```

To use a specific binary:

```sh
MJ_BIN=target/debug/mj scripts/demo-container.sh
```

## Recording a Demo

Pair the script with `asciinema` when you want a reproducible terminal recording:

```sh
asciinema rec target/demo/majutsu-container.cast --command 'scripts/demo-container.sh'
```

Then render with a tool such as `agg` or convert the recording to video with
your usual pipeline.

## Isolation Notes

The container does not receive the host's `$HOME`, `~/.majutsu`, root
directories, cloud credentials, or production remote configuration. The only
mounted host inputs are:

- The `mj` executable, read-only.
- A fresh temporary directory used as `/demo`.

If you need to demonstrate an S3-compatible backend, use a separate test bucket
or the existing MinIO E2E script instead of reusing production credentials.
