# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM node:22-bookworm-slim AS ui
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM rust:1-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*
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
COPY --from=ui /ui/dist ./ui/dist
RUN cargo zigbuild --release --bin cairn --target "$(cat /tmp/triple)" \
    && cp "target/$(cat /tmp/triple)/release/cairn" /cairn \
    && mkdir -p /seed-data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /cairn /usr/local/bin/cairn
# Ship a /data owned by the nonroot user (uid 65532) so a fresh Docker volume mounted here inherits
# that ownership; the container runs as nonroot and must be able to create its database and blobs.
COPY --from=build --chown=65532:65532 /seed-data /data
ENV CAIRN_DATA_DIR=/data CAIRN_DB_PATH=/data/cairn.db
EXPOSE 7373 7374
USER nonroot
ENTRYPOINT ["/usr/local/bin/cairn"]
CMD ["serve"]
