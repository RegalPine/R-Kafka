# ─────────────────────────────────────────────────────────────
# Stage 1: Build
# ─────────────────────────────────────────────────────────────
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependency resolution
COPY Cargo.toml Cargo.lock ./
COPY crates/rk-core/Cargo.toml crates/rk-core/Cargo.toml
COPY crates/rk-protocol/Cargo.toml crates/rk-protocol/Cargo.toml
COPY crates/rk-network/Cargo.toml crates/rk-network/Cargo.toml
COPY crates/rk-storage/Cargo.toml crates/rk-storage/Cargo.toml
COPY crates/rk-broker/Cargo.toml crates/rk-broker/Cargo.toml
COPY crates/rk-replication/Cargo.toml crates/rk-replication/Cargo.toml
COPY crates/rk-controller/Cargo.toml crates/rk-controller/Cargo.toml
COPY crates/rk-server/Cargo.toml crates/rk-server/Cargo.toml
COPY crates/rk-client/Cargo.toml crates/rk-client/Cargo.toml
COPY crates/rk-security/Cargo.toml crates/rk-security/Cargo.toml
COPY crates/rk-observability/Cargo.toml crates/rk-observability/Cargo.toml

# Create dummy src/lib.rs for each crate to cache dependency compilation
RUN for crate in rk-core rk-protocol rk-network rk-storage rk-broker \
                 rk-replication rk-controller rk-server rk-client \
                 rk-security rk-observability; do \
        mkdir -p crates/${crate}/src && \
        echo "pub fn dummy() {}" > crates/${crate}/src/lib.rs; \
    done
RUN cargo build --release --bin r-kafka 2>/dev/null || true

# Copy real source code
COPY crates/ crates/

# Touch the real lib.rs files so cargo rebuilds only workspace code
RUN for crate in rk-core rk-protocol rk-network rk-storage rk-broker \
                 rk-replication rk-controller rk-server rk-client \
                 rk-security rk-observability; do \
        touch crates/${crate}/src/lib.rs; \
    done

# Build the release binary
RUN cargo build --release --bin r-kafka \
    && strip target/release/r-kafka

# ─────────────────────────────────────────────────────────────
# Stage 2: Runtime (distroless static + non-root)
# ─────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="R-Kafka" \
      org.opencontainers.image.description="Pure Rust Kafka-compatible distributed messaging engine" \
      org.opencontainers.image.source="https://github.com/RegalPine/R-Kafka" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /build/target/release/r-kafka /usr/local/bin/r-kafka

# Default data directory (mount a volume here)
RUN mkdir -p /data/r-kafka
VOLUME ["/data/r-kafka"]

# Default config mount point
COPY config/r-kafka.toml /etc/r-kafka/r-kafka.toml

EXPOSE 9092 9090 9093

ENTRYPOINT ["/usr/local/bin/r-kafka"]
CMD ["--config", "/etc/r-kafka/r-kafka.toml"]
