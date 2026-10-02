# R-Kafka

A high-performance, Kafka-compatible message streaming platform written in Rust.

R-Kafka implements the Apache Kafka wire protocol from scratch, enabling seamless interoperability with existing Kafka clients (Java, Python, Go, etc.) while delivering significantly improved performance through Rust's zero-cost abstractions, io_uring Direct I/O, and zero-copy networking.

## Features

- **Full Kafka Protocol Compatibility** — 45+ API handlers covering Produce, Fetch, Consumer Group, Transaction, and KRaft Controller APIs
- **High-Performance Storage Engine** — Append-only CommitLog with sparse indexing, io_uring Direct I/O, and zero-copy fetch (splice/sendfile)
- **KRaft Consensus Controller** — Built on [openraft](https://crates.io/crates/openraft), no ZooKeeper dependency
- **Replica Management** — ISR-based replication with HW/LEO tracking and automatic leader election
- **Consumer Group Protocol** — Full JoinGroup/SyncGroup/Heartbeat with Range, RoundRobin, and CooperativeSticky assignors
- **Enterprise Security** — TLS/mTLS (rustls), SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), ACL authorization, audit logging
- **Transaction Support (EOS)** — Exactly-once semantics with full transaction coordinator state machine
- **Tiered Storage** — Hot/cold data separation with remote storage offloading
- **Log Compaction** — Key-based compaction with tombstone support
- **Observability** — Prometheus metrics export and structured tracing (OpenTelemetry compatible)
- **Rust Client SDK** — Native KafkaProducer, KafkaConsumer, and KafkaAdmin implementations

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        R-Kafka Cluster                          │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │  rk-network  │  │  rk-broker  │  │    rk-controller (KRaft)│ │
│  │  TCP/Zero-Copy│  │  API Router │  │    Raft Node + Metadata │ │
│  └──────┬───────┘  └──────┬──────┘  └─────────────────────────┘ │
│         │                 │                                      │
│  ┌──────┴───────┐  ┌──────┴──────┐  ┌─────────────────────────┐ │
│  │ rk-protocol  │  │ rk-storage  │  │    rk-replication       │ │
│  │ Wire Protocol│  │ CommitLog   │  │    ISR / Leader Election │ │
│  └──────────────┘  └─────────────┘  └─────────────────────────┘ │
│                                                                 │
│  ┌──────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │  rk-security │  │rk-observabil│  │     rk-client (SDK)     │ │
│  │  TLS/SASL/ACL│  │ Metrics/Trace│  │  Producer/Consumer/Admin│ │
│  └──────────────┘  └─────────────┘  └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Crate Structure

| Crate | Description |
|:---|:---|
| `rk-core` | Common types, error model, configuration |
| `rk-protocol` | Kafka wire protocol codec (45+ API implementations) |
| `rk-network` | TCP listener, connection management, zero-copy I/O |
| `rk-storage` | CommitLog, Segment, Index, Recovery, Compaction, Tiered Storage |
| `rk-broker` | API handlers, Partition routing, Consumer Group coordinator |
| `rk-replication` | ISR management, HW/LEO, leader election, replica fetching |
| `rk-controller` | KRaft Raft node, metadata state machine, cluster bootstrap |
| `rk-security` | TLS/mTLS, SASL authentication, ACL authorization, audit |
| `rk-observability` | Prometheus metrics, structured tracing |
| `rk-client` | Rust SDK: KafkaProducer, KafkaConsumer, KafkaAdmin |
| `rk-server` | Binary entry point, configuration loading, graceful shutdown |

## Quick Start

### Prerequisites

- Rust 1.75+ (see `rust-toolchain.toml`)
- Linux recommended for io_uring support (falls back to standard I/O on macOS)

### Build

```bash
cargo build --release
```

### Run (Single Node)

```bash
# Use the default configuration
cp config/examples/single-node.toml config/r-kafka.toml

# Start the broker
cargo run --release -- --config config/r-kafka.toml
```

### Produce & Consume (using rk-client)

```rust
use rk_client::config::ClientConfig;
use rk_client::producer::KafkaProducer;
use rk_client::consumer::KafkaConsumer;

// Producer
let producer = KafkaProducer::new(
    ClientConfig::new().set("bootstrap.servers", "localhost:9092")
);
producer.send("my-topic", None, b"hello r-kafka").await?;

// Consumer
let consumer = KafkaConsumer::new(
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "my-group")
);
consumer.subscribe(&["my-topic"])?;
let records = consumer.poll(Duration::from_millis(1000)).await?;
```

### Interoperability with Java Clients

R-Kafka is wire-protocol compatible with Apache Kafka. Java clients can connect directly:

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("my-topic", "key", "hello from java")).get();
```

## Configuration

See `config/examples/` for ready-to-use configurations:

| File | Description |
|:---|:---|
| `single-node.toml` | Single-node development setup |
| `cluster-node1.toml` | Multi-node cluster (node 1 of 3) |
| `production.toml` | Production-optimized settings |
| `tls-config.toml` | TLS/mTLS enabled configuration |
| `sasl-acl-config.toml` | SASL authentication + ACL authorization |

Key configuration sections:

```toml
[broker]
broker_id = 1
log_dirs = ["/data/r-kafka/logs"]

[storage]
segment_size = 1_073_741_824       # 1 GB
index_interval_bytes = 4096

[retention]
retention_ms = 604_800_000         # 7 days
retention_bytes = 1_099_511_627_776 # 1 TB

[network]
listeners = "PLAINTEXT://0.0.0.0:9092"
max_connections = 100_000

[replication]
num_replicas = 3
min_insync_replicas = 2
```

## Development

### Run Tests

```bash
# All tests
cargo test --workspace

# Specific test suites
cargo test -p rk-protocol    # Protocol compatibility
cargo test -p rk-storage     # Storage engine
cargo test -p rk-broker      # Broker logic
cargo test -p rk-server --test e2e  # End-to-end
```

### Code Quality

```bash
cargo fmt --all              # Format
cargo clippy --workspace     # Lint
```

## Project Status

R-Kafka is currently in active development. The core protocol engine, storage layer, and API handlers are implemented. See the [architecture design document](docs/rust_kafka_architecture_design_document_v2.md) for detailed design specifications.

| Component | Status |
|:---|:---|
| Wire Protocol (45+ APIs) | Implemented |
| Storage Engine (CommitLog, Index, Recovery) | Implemented |
| io_uring Direct I/O | Implemented |
| Zero-Copy Fetch (splice/sendfile) | Implemented |
| KRaft Controller (openraft) | Implemented |
| Replica Management (ISR, HW/LEO) | Implemented |
| Consumer Group Protocol | Implemented |
| Transaction Coordinator (EOS) | Implemented |
| Security (TLS, SASL, ACL) | Implemented |
| Tiered Storage | Implemented |
| Log Compaction | Implemented |
| Rust Client SDK | Implemented |
| Cross-implementation Interop Testing | In Progress |

## Documentation

- [Architecture Design Document (v2)](docs/rust_kafka_architecture_design_document_v2.md)
- [Implementation Plan](docs/rust_kafka_implementation_plan.md)
- [Deployment Guide](docs/deployment_guide.md)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

R-Kafka is an independent implementation inspired by the [Apache Kafka](https://kafka.apache.org/) project. We aim for protocol compatibility while leveraging Rust's safety and performance characteristics.
