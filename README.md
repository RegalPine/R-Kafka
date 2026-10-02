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
| `rk-core` | Common types (`BrokerId`, `Offset`, `TopicName`, ...), unified error model (`RkError`), TOML configuration (`BrokerConfig`) |
| `rk-protocol` | Kafka wire protocol codec: `KafkaReader`/`KafkaWriter`, `RecordBatch` v2, 45+ API request/response types, Flexible version support (KIP-482) |
| `rk-network` | TCP listener (`SO_REUSEPORT`), connection state machine, SASL authentication flow, `FlowController` backpressure, zero-copy (`splice`/`sendfile`), HTTP metrics server |
| `rk-storage` | `CommitLog` (append-only), `LogSegment` (.log + .index + .timeindex), mmap sparse index, crash recovery, retention, log compaction, tiered storage, io_uring Direct I/O |
| `rk-broker` | `BrokerRouter` (45+ API handlers), `PartitionManager` (`DashMap`), `GroupManager` (consumer group state machine), `OffsetManager`, `TransactionCoordinator`, `SaslAuthenticator`, `BrokerMetrics` |
| `rk-replication` | `ReplicaManager`, `ISRTracker` (ISR shrink/expand), `HighWatermarkManager` (HW/LEO), `LeaderElector`, `ReplicaFetcher` |
| `rk-controller` | KRaft consensus via `openraft`: `RaftNode`, `MetadataStateMachine`, `BrokerRegistry`, `PartitionAllocator` (rack-aware), `ClusterBootstrap` |
| `rk-security` | TLS/mTLS (`rustls`), SASL (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512), ACL engine, API permission mapping, audit logging, `SecurityPipeline` |
| `rk-observability` | Prometheus metrics export, structured tracing (`fmt`/`json` layers), OpenTelemetry compatible |
| `rk-client` | Rust SDK: `KafkaProducer`, `KafkaConsumer`, `KafkaAdmin`, `ConnectionPool`, `MetadataCache`, partition assignors (Range/RoundRobin/CooperativeSticky) |
| `rk-server` | Binary entry point: config loading, storage recovery, component assembly, SIGHUP config reload, graceful shutdown (SIGINT/SIGTERM) |

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
use rk_client::config::{ClientConfig, ProducerConfig, ConsumerConfig};
use rk_client::producer::KafkaProducer;
use rk_client::consumer::KafkaConsumer;

// Producer
let producer_config = ProducerConfig {
    client: ClientConfig::new().bootstrap_servers("localhost:9092"),
    ..ProducerConfig::default()
};
let producer = KafkaProducer::new(producer_config);
let metadata = producer.send("my-topic", None, b"hello r-kafka").await?;

// Consumer
let consumer_config = ConsumerConfig {
    client: ClientConfig::new().bootstrap_servers("localhost:9092"),
    group_id: "my-group".to_string(),
    ..ConsumerConfig::default()
};
let mut consumer = KafkaConsumer::new(consumer_config);
consumer.subscribe(&["my-topic"])?;
let records = consumer.poll(std::time::Duration::from_millis(1000)).await?;
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

R-Kafka uses TOML configuration files with environment variable overrides (`RK_{SECTION}_{KEY}`).

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
id = 1
host = "0.0.0.0"
port = 9092

[storage]
data_dir = "/data/r-kafka"
segment_max_size = 1_073_741_824     # 1 GB
flush_mode = "hybrid"                # async | sync | hybrid

[retention]
max_bytes = 1_099_511_627_776        # 1 TB
max_ms = 604_800_000                 # 7 days
compaction_enabled = false

[network]
max_connections = 100_000
max_request_size = 104_857_600       # 100 MB
max_pending_bytes = 536_870_912      # 512 MB

[replication]
default_replication_factor = 3
min_isr_size = 2

[security]
tls_enabled = false
sasl_enabled = false

[observability]
metrics_enabled = false
metrics_port = 9090
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
cargo test -p rk-server --test e2e              # End-to-end
cargo test -p rk-server --test client_interop   # Client interop
cargo test -p rk-server --test security_interop # Security interop
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
| Wire Protocol (45+ APIs) | ✅ Implemented |
| Storage Engine (CommitLog, Index, Recovery) | ✅ Implemented |
| io_uring Direct I/O | ✅ Implemented |
| Zero-Copy Fetch (splice/sendfile) | ✅ Implemented |
| KRaft Controller (openraft) | ✅ Implemented |
| Replica Management (ISR, HW/LEO) | ✅ Implemented |
| Consumer Group Protocol | ✅ Implemented |
| Transaction Coordinator (EOS) | ✅ Implemented |
| Security (TLS, SASL, ACL) | ✅ Implemented |
| Tiered Storage | ✅ Implemented |
| Log Compaction | ✅ Implemented |
| Rust Client SDK | ✅ Implemented |
| Config Hot-Reload (SIGHUP) | ✅ Implemented |
| Graceful Shutdown (SIGINT/SIGTERM) | ✅ Implemented |
| HTTP Metrics Server (/metrics, /health, /ready) | ✅ Implemented |
| Cross-implementation Interop Testing | 🔄 In Progress |

## Documentation

- [Architecture Design Document](docs/rust_kafka_architecture_design_document_v2.md)
- [Deployment Guide](docs/deployment_guide.md)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

R-Kafka is an independent implementation inspired by the [Apache Kafka](https://kafka.apache.org/) project. We aim for protocol compatibility while leveraging Rust's safety and performance characteristics.
