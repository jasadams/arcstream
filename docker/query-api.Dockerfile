FROM docker.io/library/rust:1.89-alpine AS builder
RUN apk add --no-cache musl-dev cmake make g++ perl curl-dev zlib-dev zlib-static
WORKDIR /app
COPY query-api/ .
RUN cargo build --release

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/release/query-api /
ENTRYPOINT ["/query-api"]
