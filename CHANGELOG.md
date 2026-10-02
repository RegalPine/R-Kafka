# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial open-source release
- Full Kafka wire protocol engine with 45+ API handlers
- High-performance storage engine (CommitLog, Segment, Index, io_uring Direct I/O)
- Zero-copy fetch (splice on Linux, sendfile on macOS)
- KRaft consensus controller (openraft-based, no ZooKeeper)
- ISR-based replica management with HW/LEO tracking
- Consumer Group protocol (Range, RoundRobin, CooperativeSticky assignors)
- Transaction coordinator with exactly-once semantics (EOS)
- Security: TLS/mTLS (rustls), SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), ACL, audit
- Tiered storage with remote storage offloading
- Log compaction with tombstone support
- Prometheus metrics and structured tracing
- Rust client SDK (KafkaProducer, KafkaConsumer, KafkaAdmin)
- Batch accumulator with configurable flush policy
- Crash recovery with CRC32C validation
- Configuration hot-reload (SIGHUP)
- Comprehensive test suite (~5800 lines)
