//! rk-client: R-Kafka Rust Client SDK
//!
//! 提供 Rust 原生的 Kafka 客户端，可连接 R-Kafka 或 Java Kafka Broker。
//!
//! # 组件
//!
//! - **KafkaProducer**: 消息生产者，支持 acks=0/1/all
//! - **KafkaConsumer**: 消息消费者，支持消费组和偏移量管理
//! - **KafkaAdmin**: 管理客户端，支持 Topic CRUD
//!
//! # 架构
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │              rk-client                           │
//! │                                                  │
//! │  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
//! │  │ Producer │  │ Consumer │  │ Admin         │  │
//! │  └────┬─────┘  └────┬─────┘  └──────┬───────┘  │
//! │       │             │               │           │
//! │       └─────────────┼───────────────┘           │
//! │                     │                           │
//! │            ┌────────┴────────┐                  │
//! │            │ ConnectionPool  │                  │
//! │            │ + MetadataCache │                  │
//! │            └────────┬────────┘                  │
//! └─────────────────────┼───────────────────────────┘
//!                       │ TCP
//!              ┌────────┴────────┐
//!              │ Kafka Broker(s) │
//!              └─────────────────┘
//! ```
//!
//! # 示例
//!
//! ```ignore
//! use rk_client::config::ClientConfig;
//! use rk_client::producer::KafkaProducer;
//! use rk_client::config::ProducerConfig;
//!
//! let config = ProducerConfig::default();
//! let producer = KafkaProducer::new(config);
//! let result = producer.send("my-topic", Some(b"key"), b"value").await?;
//! ```

pub mod error;
pub mod config;
pub mod connection;
pub mod metadata;
pub mod producer;
pub mod consumer;
pub mod admin;
pub mod assignor;

// Re-exports
pub use error::{ClientError, ClientResult};
pub use config::{ClientConfig, ProducerConfig, ConsumerConfig, CompressionType, OffsetReset};
pub use connection::{BrokerConnection, ConnectionPool};
pub use metadata::{MetadataCache, TopicPartition, PartitionLeader, BrokerEndpoint};
pub use producer::{KafkaProducer, ProducerRecord, RecordMetadata, Partitioner, DefaultPartitioner};
pub use consumer::{KafkaConsumer, ConsumerRecord};
pub use admin::{KafkaAdmin, TopicInfo, PartitionInfo, NewTopic};
pub use assignor::{
    PartitionAssignor, MemberSubscription, AssignmentResult,
    RangeAssignor, RoundRobinAssignor, CooperativeStickyAssignor,
    RANGE_ASSIGNOR, ROUNDROBIN_ASSIGNOR, COOPERATIVE_STICKY_ASSIGNOR,
    get_assignor, select_protocol,
};
