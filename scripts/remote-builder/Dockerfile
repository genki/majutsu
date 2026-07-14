FROM rust:1.91-bookworm

ARG ZIG_VERSION=0.14.1
ARG ZIG_SHA256=f7a654acc967864f7a050ddacfaa778c7504a0eca8d2b678839c21eea47c992b

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl pkg-config sccache xz-utils \
    && curl -fsSLo /tmp/zig.tar.xz "https://ziglang.org/download/${ZIG_VERSION}/zig-aarch64-linux-${ZIG_VERSION}.tar.xz" \
    && echo "${ZIG_SHA256}  /tmp/zig.tar.xz" | sha256sum -c - \
    && tar -xJf /tmp/zig.tar.xz -C /opt \
    && ln -s "/opt/zig-aarch64-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig \
    && cargo install cargo-zigbuild --version 0.23.0 --locked \
    && rm -f /tmp/zig.tar.xz \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu \
    && rustup component add clippy

ENV CARGO_INCREMENTAL=0 \
    CARGO_TARGET_DIR=/target \
    RUSTC_WRAPPER=/usr/bin/sccache \
    SCCACHE_DIR=/sccache \
    SCCACHE_CACHE_SIZE=20G

WORKDIR /workspace
