FROM rust:1.89-bookworm AS builder
RUN rustup target add x86_64-unknown-linux-musl wasm32-unknown-unknown && \
    apt-get update && apt-get install -y --no-install-recommends musl-tools brotli && \
    rm -rf /var/lib/apt/lists/*

# Install trunk
RUN wget -qO- https://github.com/trunk-rs/trunk/releases/download/v0.21.14/trunk-x86_64-unknown-linux-gnu.tar.gz \
    | tar -xzf - -C /usr/local/bin && chmod +x /usr/local/bin/trunk

WORKDIR /app
COPY dashboard/ .

# Build server binary (static musl)
ENV CC_x86_64_unknown_linux_musl=musl-gcc
RUN cargo build --release --features ssr --bin dashboard-server --target x86_64-unknown-linux-musl

# Build WASM frontend (skip wasm-opt — rustc -Oz is sufficient)
ENV TRUNK_TOOLS_WASM_OPT=skip
RUN mkdir -p /root/.cache/trunk/wasm-opt-skip/bin \
    && printf '#!/bin/sh\nOUT=$(echo "$1" | cut -d= -f2)\ncp "$3" "$OUT"\n' > /root/.cache/trunk/wasm-opt-skip/bin/wasm-opt \
    && chmod +x /root/.cache/trunk/wasm-opt-skip/bin/wasm-opt
RUN trunk build --release

# Create site directory with unhashed filenames for Leptos
RUN mkdir -p /site/pkg && \
    cp dist/dashboard-*_bg.wasm /site/pkg/dashboard_bg.wasm && \
    cp dist/dashboard-*.js /site/pkg/dashboard.js && \
    cp style/main.css /site/pkg/dashboard.css && \
    find /site/pkg -type f \( -name '*.wasm' -o -name '*.js' -o -name '*.css' \) \
      -exec sh -c 'brotli -q 11 -k -f "$1" && gzip -9 -k -f "$1"' _ {} \; && \
    if [ -d public ] && [ "$(ls -A public 2>/dev/null)" ]; then cp -r public/* /site/; fi

FROM scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/dashboard-server /dashboard-server
COPY --from=builder /site/ /site/
ENV LEPTOS_SITE_ROOT=/site
ENV LEPTOS_SITE_PKG_DIR=pkg
ENV LEPTOS_SITE_ADDR=0.0.0.0:3000
ENV LEPTOS_OUTPUT_NAME=dashboard
EXPOSE 3000
ENTRYPOINT ["/dashboard-server"]
