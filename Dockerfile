# syntax=docker/dockerfile:1
#
# Self-contained, multi-stage, multi-arch build of the single Cairn binary, ending on a distroless
# image for a minimal, hardened runtime (CA certificates for outbound TLS, a nonroot user, no shell
# or package manager). Best-practice notes:
#   * Three stages: build the embedded React UI, cross-compile the static musl binary, ship it.
#   * The build stage is pinned to $BUILDPLATFORM and cross-compiles to $TARGETARCH with Zig
#     (cargo-zigbuild) — so an arm64 image is cross-built on the native amd64 host, NOT emulated
#     under slow QEMU. Zig carries a complete musl sysroot for both arches, so the vendored C deps
#     (aws-lc-rs, bundled SQLite, zstd) link statically with no extra toolchain wiring.
#   * The result is a fully static binary on distroless/static, so the same image runs anywhere.
#
# Build (multi-arch):
#   docker buildx build --platform linux/amd64,linux/arm64 -t cairn:latest .

# ---- Stage 1: build the embedded React management UI (ui/dist) ----
FROM --platform=$BUILDPLATFORM node:22-bookworm-slim AS ui
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# ---- Stage 2: cross-compile the static musl binary on the native build host (Zig, no QEMU) ----
FROM --platform=$BUILDPLATFORM rust:1-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*
# Zig is the C cross-compiler/linker (via cargo-zigbuild); expose it as a plain `zig` on PATH.
RUN pip install --break-system-packages ziglang \
    && printf '#!/bin/sh\nexec python3 -m ziglang "$@"\n' > /usr/local/bin/zig \
    && chmod +x /usr/local/bin/zig \
    && cargo install --locked cargo-zigbuild
ARG TARGETARCH
RUN case "$TARGETARCH" in \
      amd64) echo x86_64-unknown-linux-musl ;; \
      arm64) echo aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac > /tmp/triple \
    && rustup target add "$(cat /tmp/triple)"
WORKDIR /src
COPY . .
# Bake in the real UI bundle from stage 1 (rust-embed reads ui/dist at compile time).
COPY --from=ui /ui/dist ./ui/dist
RUN cargo zigbuild --release --bin cairn --target "$(cat /tmp/triple)" \
    && cp "target/$(cat /tmp/triple)/release/cairn" /cairn

# ---- Stage 3: minimal distroless runtime ----
# distroless/static carries CA certificates (outbound HTTPS replication), tzdata, and a nonroot
# user — no shell, no package manager, tiny attack surface. It is multi-arch, so buildx selects the
# matching base per --platform.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /cairn /usr/local/bin/cairn

# S3 API + management UI default listen port (override with CAIRN_LISTEN_ADDR).
EXPOSE 8080

# Configuration is entirely via CAIRN_* environment variables (no config file). Mount a volume for
# CAIRN_DATA_DIR. Runs unprivileged.
USER nonroot
ENTRYPOINT ["/usr/local/bin/cairn"]
CMD ["serve"]
