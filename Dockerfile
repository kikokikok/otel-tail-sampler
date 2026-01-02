FROM rust:1.92-slim AS builder

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libsasl2-dev \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY scripts ./scripts

ARG FEATURES="iceberg-storage"
RUN cargo build --release --locked --features "$FEATURES"

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tail-sampling-selector /usr/local/bin/
COPY --from=builder /app/target/release/simple_producer /usr/local/bin/ 2>/dev/null || true

COPY config ./config

EXPOSE 8080 9090

ENV TSS_ENVIRONMENT=production

CMD ["tail-sampling-selector"]
