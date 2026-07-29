FROM --platform=$BUILDPLATFORM node:22.23.1-bookworm-slim@sha256:6c74791e557ce11fc957704f6d4fe134a7bc8d6f5ca4403205b2966bd488f6b3 AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM rust:1.97.1-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa AS build
ARG BUILDARCH
RUN case "$BUILDARCH" in \
      amd64) \
        wheel_url="https://files.pythonhosted.org/packages/3e/ed/7b79023aa27ceb5d461ecf761181e7c33c57bbc1a6256a39535d1c7083d2/ziglang-0.16.0-py3-none-manylinux_2_12_x86_64.manylinux2010_x86_64.musllinux_1_1_x86_64.whl"; \
        wheel_sha="9fcda73f62b851dd72a54b710ad40a209896db14cfb13649e62191243556342b" ;; \
      arm64) \
        wheel_url="https://files.pythonhosted.org/packages/7e/ed/d6663a5e52c504944d578b9e0bfcb7857f292803bcd09ebe0d10fe2b293d/ziglang-0.16.0-py3-none-manylinux_2_17_aarch64.manylinux2014_aarch64.musllinux_1_1_aarch64.whl"; \
        wheel_sha="e27d409812b11e0fb89ed0200cf2e55b6464d43f9461553104e4a4f9a94a1fd5" ;; \
      *) echo "unsupported BUILDARCH: $BUILDARCH" >&2; exit 1 ;; \
    esac \
    && curl --proto '=https' --tlsv1.2 -fsSL -o /tmp/ziglang.whl "$wheel_url" \
    && printf '%s  %s\n' "$wheel_sha" /tmp/ziglang.whl | sha256sum -c - \
    && python3 -m zipfile -e /tmp/ziglang.whl /usr/local/lib/python3.11/dist-packages \
    && chmod +x /usr/local/lib/python3.11/dist-packages/ziglang/zig \
    && printf '#!/bin/sh\nexec python3 -m ziglang "$@"\n' > /usr/local/bin/zig \
    && chmod +x /usr/local/bin/zig \
    && cargo install --locked --version 0.23.0 cargo-zigbuild
ARG TARGETARCH
RUN case "$TARGETARCH" in \
      amd64) echo x86_64-unknown-linux-musl ;; \
      arm64) echo aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac > /tmp/triple \
    && rustup target add "$(cat /tmp/triple)"
WORKDIR /src
COPY . .
COPY --from=web /web/dist ./web/dist
RUN cargo zigbuild --release --bin cairn --target "$(cat /tmp/triple)" \
    && cp "target/$(cat /tmp/triple)/release/cairn" /cairn \
    && mkdir -p /seed-data

FROM gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35
COPY --from=build /cairn /usr/local/bin/cairn
# Ship a /data owned by the nonroot user (uid 65532) so a fresh Docker volume mounted here inherits
# that ownership; the container runs as nonroot and must be able to create its database and blobs.
COPY --from=build --chown=65532:65532 /seed-data /data
ENV CAIRN_DATA_DIR=/data CAIRN_DB_PATH=/data/cairn.db
EXPOSE 7373 7374
USER nonroot
ENTRYPOINT ["/usr/local/bin/cairn"]
CMD ["serve"]
