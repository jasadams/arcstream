FROM docker.io/library/rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc AS builder
RUN apt-get update && apt-get install -y cmake build-essential pkg-config libssl-dev libzstd-dev libsasl2-dev libcurl4-openssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY event-producer/ .
RUN cargo build --locked --release

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/event-producer /usr/local/bin/
ENTRYPOINT ["event-producer"]
