# Two-stage Docker build for the Cairn GitHub Action.
# Stage 1 compiles the binary; stage 2 ships a slim runtime.

FROM rust:1-slim AS build
WORKDIR /build

# Cache the dep graph first.
COPY Cargo.toml Cargo.lock ./
COPY cli/Cargo.toml cli/Cargo.toml
COPY crates ./crates
COPY cli ./cli

RUN cargo build --release -p cairn --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/cairn /usr/local/bin/cairn
ENTRYPOINT ["/usr/local/bin/cairn"]
