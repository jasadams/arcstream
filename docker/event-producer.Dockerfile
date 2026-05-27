FROM docker.io/library/rust:1.87-slim-bookworm AS builder
RUN apt-get update && apt-get install -y cmake build-essential pkg-config libssl-dev libzstd-dev libsasl2-dev libcurl4-openssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY event-producer/ .
RUN cargo build --release

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/event-producer /usr/local/bin/
ENTRYPOINT ["event-producer"]
