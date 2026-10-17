FROM docker.io/library/rust:1.96.0-alpine@sha256:f87aa870663e2b57ec8c69de82c7eedf7383bee987eef7612c0359635eaadb41 AS builder
RUN apk add --no-cache musl-dev cmake make g++ perl curl-dev zlib-dev zlib-static
WORKDIR /app
COPY query-api/ .
RUN cargo build --locked --release

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/release/query-api /
ENTRYPOINT ["/query-api"]
