# R-Kafka 架构设计文档

> **纯粹 Rust 实现的 Kafka 兼容分布式消息流平台**
>
> 版本: 0.1.0 | Rust Edition: 2021 | MSRV: 1.75

---

## 1. 项目概述

R-Kafka 是从零构建的 Kafka Wire Protocol 兼容实现，使用 Rust 编写。项目目标是兼容现有 Kafka 客户端（Java、Python、Go 等），同时利用 Rust 的零成本抽象、内存安全和异步生态提供高性能的消息流引擎。

### 1.1 设计原则

| 原则 | 实现方式 |
|:---|:---|
| **Kafka 协议兼容** | 完整实现 45+ API Handler，兼容 Kafka Wire Protocol v0-v2+ (Flexible) |
| **零拷贝 I/O** | Fetch 路径使用 `splice`(Linux) / `sendfile`(macOS) 实现磁盘到网络的零拷贝传输 |
| **类型安全** | Newtype 模式封装 `BrokerId`、`PartitionId`、`Offset`、`Epoch` 等核心类型，编译期防止混用 |
| **无 ZooKeeper** | 基于 `openraft` 实现 KRaft 共识协议，控制器无外部依赖 |
| **统一错误模型** | `RkError` 枚举按类别分组（IO/协议/存储/配置/流控/副本/内部），全链路统一 |
| **12-Factor 配置** | TOML 文件 + 环境变量覆盖 (`RK_{SECTION}_{KEY}`)，支持 SIGHUP 热加载 |

### 1.2 技术栈

| 领域 | 依赖 |
|:---|:---|
| 异步运行时 | `tokio` (full features) |
| 零拷贝字节缓冲 | `bytes` |
| 网络编解码 | `tokio-util` (codec) |
| 序列化 | `serde` + `serde_json` + `toml` |
| 错误处理 | `thiserror` + `anyhow` |
| 日志/追踪 | `tracing` + `tracing-subscriber` (env-filter, json) |
| 并发容器 | `dashmap` (分片锁 HashMap) |
| 跨线程通道 | `flume` |
| 内存映射文件 | `memmap2` (稀疏索引 mmap) |
| 系统调用 | `libc` (splice/sendfile 零拷贝) |
| 共识协议 | `openraft` (KRaft) |
| TLS | `tokio-rustls` (rustls) |
| CRC 校验 | `crc32c` |
| CLI | `clap` (derive) |

---

## 2. 系统总体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Kafka Clients                                    │
│                  (Java / Go / Python / Rust / C++)                      │
└─────────────────────────────────────────────────────────────────────────┘
                         │ Kafka Wire Protocol (TCP)
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       R-Kafka Broker Node                               │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  rk-network: TCP 监听 + 连接管理 + 流控 + HTTP 监控              │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │  │
│  │  │ TcpListener  │  │ Connection   │  │ FlowController     │     │  │
│  │  │ (SO_REUSEPORT│  │ (帧解码 +    │  │ (连接数/请求大小/  │     │  │
│  │  │  + TLS)      │  │  SASL 状态机)│  │  背压控制)         │     │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘     │  │
│  └───────────────────────────┬───────────────────────────────────────┘  │
│                              │                                          │
│  ┌───────────────────────────▼───────────────────────────────────────┐  │
│  │  rk-broker: API 路由 + Handler 集合 + 业务逻辑                   │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │  │
│  │  │ BrokerRouter │  │ Partition    │  │ GroupManager       │     │  │
│  │  │ (45+ Handler │  │ Manager     │  │ (JoinGroup/Sync/   │     │  │
│  │  │  分发)       │  │ (DashMap)    │  │  Heartbeat/Leave)  │     │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘     │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │  │
│  │  │ Offset       │  │ Transaction  │  │ SaslAuthenticator  │     │  │
│  │  │ Manager      │  │ Coordinator  │  │                    │     │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘     │  │
│  └───────────────────────────┬───────────────────────────────────────┘  │
│                              │                                          │
│  ┌───────────────────────────▼───────────────────────────────────────┐  │
│  │  rk-storage: 存储引擎                                            │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │  │
│  │  │ CommitLog    │  │ LogSegment   │  │ OffsetIndex        │     │  │
│  │  │ (Append-Only │  │ (.log +      │  │ (mmap 稀疏索引)    │     │  │
│  │  │  多 Segment) │  │  .index +    │  │ TimeIndex          │     │  │
│  │  │              │  │  .timeindex) │  │ (mmap 稀疏索引)    │     │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘     │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │  │
│  │  │ Recovery     │  │ Retention    │  │ Compaction         │     │  │
│  │  │ (CRC 校验 +  │  │ (时间/大小   │  │ (Key 去重 +        │     │  │
│  │  │  截断损坏)   │  │  删除旧段)   │  │  Tombstone 支持)   │     │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘     │  │
│  │  ┌──────────────┐  ┌──────────────┐                             │  │
│  │  │ Tiered       │  │ UringWriter  │                             │  │
│  │  │ Storage      │  │ (io_uring    │                             │  │
│  │  │ (冷热分离)   │  │  Direct I/O) │                             │  │
│  │  └──────────────┘  └──────────────┘                             │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌────────────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ rk-protocol            │  │ rk-security     │  │ rk-observability│  │
│  │ (编解码引擎 + 45 API)  │  │ (TLS/SASL/ACL)  │  │ (Metrics/Trace) │  │
│  └────────────────────────┘  └─────────────────┘  └─────────────────┘  │
│                                                                         │
│  ┌────────────────────────┐  ┌─────────────────┐                       │
│  │ rk-controller (KRaft)  │  │ rk-replication  │                       │
│  │ (Raft + Metadata SM)   │  │ (ISR/HW/LEO)    │                       │
│  └────────────────────────┘  └─────────────────┘                       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Crate 模块详解

### 3.1 rk-core — 公共基础库

**职责**: 全局类型定义、统一错误模型、配置加载。所有其他 crate 均依赖此 crate。

**核心类型** (`types.rs`):

所有核心标识使用 Newtype 模式包装，防止在不同语义场景混用原始数值：

| 类型 | 内部类型 | 用途 |
|:---|:---|:---|
| `BrokerId(i32)` | Broker 节点标识 |
| `PartitionId(i32)` | Partition 编号 |
| `Offset(i64)` | 消息偏移量（单调递增） |
| `Epoch(i32)` | Leader Epoch (KRaft 纪元) |
| `TopicName(String)` | Topic 名称 |
| `TopicId(u128)` | Topic UUID (Kafka 4.0+) |
| `NodeId(i32)` | KRaft 统一节点标识 |
| `CorrelationId(i32)` | 请求-响应配对 |
| `ProducerId(i64)` | 幂等/事务 Producer |
| `ProducerEpoch(i16)` | Producer Epoch |
| `SequenceNumber(i32)` | 幂等去重序号 |
| `GroupId(String)` | Consumer Group |
| `MemberId(String)` | Consumer Group 成员 |

**ApiKey 枚举** (`types.rs`): 定义了 45+ 个 Kafka API Key，从 `Produce(0)` 到 `DescribeTopics(70)`，包含 KRaft 相关 API（`Vote(51)`、`BeginQuorumEpoch(52)`、`EndQuorumEpoch(53)`、`BrokerRegistration(54)`、`BrokerHeartbeat(55)`、`DescribeQuorum(56)`）。每个 API 实现了 `is_flexible(version)` 方法判断是否为 KIP-482 Flexible 版本。

**统一错误模型** (`error.rs`):

```rust
pub enum RkError {
    // IO 错误
    Io(std::io::Error),
    // 协议错误
    Protocol(String),
    UnsupportedApiKey(i16),
    UnsupportedApiVersion { key: i16, version: i16 },
    InvalidRequest(String),
    BufferUnderflow { need: usize, have: usize },
    InvalidDataType(String),
    // 存储错误
    Storage(String),
    SegmentNotFound(i64),
    CorruptedRecord(i64),
    LogDirNotFound(String),
    // 配置错误
    Config(String),
    ConfigParse(String),
    // 流控错误
    RequestTooLarge { size: usize, max: usize },
    TooManyConnections { current: usize, max: usize },
    BackpressureExceeded { current: usize, max: usize },
    // 副本错误
    NotLeader { topic: String, partition: i32 },
    IsrShrink(i32),
    // 内部错误
    Internal(String),
    ChannelClosed,
}
```

**配置系统** (`config.rs`):

`BrokerConfig` 从 TOML 文件加载，支持 9 个配置段：

| 段 | 关键字段 | 默认值 |
|:---|:---|:---|
| `[broker]` | `id`, `host`, `port`, `rack` | 1, 0.0.0.0, 9092 |
| `[storage]` | `data_dir`, `segment_max_size`, `flush_mode`, `io_engine` | /data/r-kafka, 1GB, hybrid, stdio |
| `[retention]` | `max_bytes`, `max_ms`, `compaction_enabled` | 1TB, 7天, false |
| `[replication]` | `default_replication_factor`, `min_isr_size`, `unclean_leader_election` | 3, 2, false |
| `[network]` | `max_connections`, `max_request_size`, `max_pending_bytes` | 100K, 100MB, 512MB |
| `[security]` | `tls_enabled`, `tls_mode`, `sasl_enabled`, `sasl_mechanisms`, `sasl_users` | — |
| `[controller]` | `quorum_peers`, `election_timeout_ms` | —, 3000 |
| `[observability]` | `metrics_enabled`, `metrics_port`, `tracing_enabled` | false, 9090, false |
| `[producer]` | `batch_size`, `linger_ms` | 1MB, 5ms |

环境变量覆盖规则：`RK_{SECTION}_{KEY}`，如 `RK_BROKER_ID`、`RK_STORAGE_DATA_DIR`。优先级：环境变量 > 配置文件 > 默认值。

---

### 3.2 rk-protocol — 协议编解码引擎

**职责**: 实现 Kafka Wire Protocol 的全部数据类型、请求/响应头、API 路由、RecordBatch v2 格式编解码和标准错误码。

**架构**:

```
rk-protocol
├── types.rs        — KafkaReader/KafkaWriter: 零拷贝协议类型读写器
├── codec.rs        — KafkaRequestDecoder/KafkaResponseEncoder trait
├── request.rs      — RequestHeader v0/v1/v2 (Legacy + Flexible)
├── response.rs     — ResponseHeader v0/v1 (Legacy + Flexible)
├── record.rs       — RecordBatch v2 头部编解码 + CRC32C 校验
├── api.rs          — ApiRouter + RequestContext + RequestHandler trait
├── api_versions.rs — 支持的 API 版本列表
├── error_codes.rs  — KafkaErrorCode 枚举
├── legacy_message.rs — 旧版 MessageSet 兼容 (v0/v1 → v2 转换)
└── apis/           — 45+ API 的请求/响应结构体 (每个 API 一个文件)
    ├── produce.rs, fetch.rs, metadata.rs, list_offsets.rs
    ├── create_topics.rs, delete_topics.rs, create_partitions.rs
    ├── join_group.rs, sync_group.rs, heartbeat.rs, leave_group.rs
    ├── offset_commit.rs, offset_fetch.rs, offset_delete.rs
    ├── find_coordinator.rs, describe_groups.rs, list_groups.rs
    ├── sasl_handshake.rs, sasl_authenticate.rs
    ├── init_producer_id.rs, add_partitions_to_txn.rs, end_txn.rs
    ├── describe_configs.rs, alter_configs.rs, incremental_alter_configs.rs
    ├── delete_records.rs, elect_leaders.rs
    ├── describe_cluster.rs, describe_topics.rs, describe_quorum.rs
    ├── describe_producers.rs, list_transactions.rs
    ├── alter_partition_reassignments.rs, list_partition_reassignments.rs
    ├── leader_and_isr.rs, stop_replica.rs, update_metadata.rs
    ├── controlled_shutdown.rs, broker_registration.rs
    ├── broker_heartbeat.rs, vote.rs
    ├── begin_quorum_epoch.rs, end_quorum_epoch.rs
    └── ...
```

**KafkaReader / KafkaWriter** (`types.rs`):

`KafkaReader<'a>` 从 `&[u8]` 切片零拷贝读取，`KafkaWriter<'a>` 向 `BytesMut` 写入。精确实现 Kafka 协议的全部基础类型：

- 基础类型: `i8/i16/i32/i64/u32/u64/bool`
- 字符串: `string/nullable_string/compact_string/compact_nullable_string`
- 字节: `bytes/nullable_bytes/compact_bytes/compact_nullable_bytes`
- 变长整数: `varint/varlong` (ZigZag 编码) + `unsigned_varint/unsigned_varlong`
- 数组: `array/nullable_array/compact_array/compact_nullable_array`
- Tagged Fields: `read_tagged_fields/write_tagged_fields` (KIP-482)

**编解码 Trait** (`codec.rs`):

```rust
pub trait KafkaRequestDecoder: Sized {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self>;
}

pub trait KafkaResponseEncoder {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()>;
}
```

每个 API 的请求/响应结构体实现这两个 trait，根据 `version` 参数处理不同版本的字段差异。

**RequestHeader** (`request.rs`): 支持 v0/v1 (Legacy) 和 v2 (Flexible) 两种格式。`is_flexible()` 方法通过 `ApiKey::is_flexible(version)` 判断，决定使用 `compact_nullable_string` 还是 `nullable_string` 编码 `client_id`。

**RecordBatch v2** (`record.rs`): 磁盘格式与 Apache Kafka 完全一致，61 字节固定头部：

```
base_offset(8) + batch_length(4) + partition_leader_epoch(4) + magic(1) + crc(4)
+ attributes(2) + last_offset_delta(4) + base_timestamp(8) + max_timestamp(8)
+ producer_id(8) + producer_epoch(2) + base_sequence(4) + records_count(4) = 61
```

支持压缩类型检测（None/Gzip/Snappy/Lz4/Zstd）、CRC32C 校验、事务/控制批次标识。

**ApiRouter** (`api.rs`): 基于 `HashMap<i16, Arc<dyn RequestHandler>>` 的请求分发器，`RequestHandler` trait 为 object-safe 的同步接口。

---

### 3.3 rk-network — 网络层

**职责**: TCP 监听、连接管理、帧解码、流控、零拷贝数据传输、HTTP 监控端点。

**模块结构**:

```
rk-network
├── server.rs        — TCP 监听 (SO_REUSEPORT) + 连接接受循环 + 优雅关闭
├── connection.rs    — Connection 状态机 + 帧读写 + SASL 状态转换
├── flow_control.rs  — FlowController: 连接数/请求大小/背压控制
├── zero_copy.rs     — splice(Linux) / sendfile(macOS) 零拷贝传输
└── http_server.rs   — HTTP 监控端点 (/metrics, /health, /ready, /status)
```

**TCP Server** (`server.rs`):

使用 `socket2` 设置 `SO_REUSEPORT` + `SO_REUSEADDR`，将连接均匀分发到多个 socket（Thread-per-Core 模型基础）。支持纯 TCP 和 TLS 两种传输模式。`run_server_with_shutdown` 通过 `watch::Receiver<bool>` 接收优雅关闭信号。

**Connection** (`connection.rs`):

`Connection` 结构体管理单个客户端连接的完整生命周期：

```rust
pub struct Connection {
    stream: TransportStream,    // Tcp(TcpStream) | Tls(TlsStream<TcpStream>)
    state: ConnectionState,     // Connected → SaslHandshaked → Authenticated → ...
    read_buf: BytesMut,         // 读缓冲区
    frame_buf: BytesMut,        // 完整帧缓冲区
    session: ConnectionSession, // 连接会话 (请求计数、认证状态)
}
```

连接状态机：`Connected → SaslHandshaked → Authenticated → Reading/Writing → Closed`

`TransportStream` 枚举统一了 TCP 和 TLS 的 `AsyncRead + AsyncWrite` 实现。

帧协议：4 字节长度前缀 (big-endian u32) + 帧数据。`read_frame` 循环读取直到凑齐完整帧，`write_frame` 自动添加长度前缀。

`handle_connection_inner` 为核心处理循环：读帧 → 提取 api_key → SASL 状态机检查 → `BrokerRouter.handle_frame()` → 写响应帧。SASL 认证后自动更新 session 状态。

**FlowController** (`flow_control.rs`):

基于 `AtomicUsize` 的无锁流控：
- `try_accept_connection()`: 检查连接数上限
- `check_request_size()`: 检查请求大小上限
- `add_pending_bytes()` / `release_pending_bytes()`: 背压控制

**Zero-Copy** (`zero_copy.rs`):

Fetch 响应路径的零拷贝优化，将磁盘文件数据直接传输到 TCP Socket：

| 平台 | 系统调用 | 机制 |
|:---|:---|:---|
| Linux | `splice(fd_file → pipe → fd_socket)` | 完全内核态零拷贝 |
| macOS | `sendfile(fd_file → fd_socket)` | 内核态直接发送 |

提供异步 (`zero_copy_send`) 和同步 (`zero_copy_send_sync`) 两个版本。`ZeroCopyStats` 记录传输字节数、操作次数和回退次数。

**HTTP Metrics Server** (`http_server.rs`):

轻量级 HTTP 监控端点，使用原生 Tokio TCP 实现，无外部 HTTP 框架依赖：

| 端点 | 功能 | 格式 |
|:---|:---|:---|
| `GET /metrics` | JSON 格式 Broker 指标 | JSON |
| `GET /metrics/prometheus` | Prometheus text exposition format | text |
| `GET /health` | 健康检查 | JSON |
| `GET /ready` | 就绪检查 | JSON |
| `GET /status` | 综合状态 (含流控信息) | JSON |

---

### 3.4 rk-storage — 存储引擎

**职责**: Append-Only CommitLog + 稀疏索引 + Crash Recovery + Retention + Compaction + Tiered Storage。

**模块结构**:

```
rk-storage
├── commitlog.rs       — CommitLog: 每个 Partition 的 append-only 日志
├── segment.rs         — LogSegment: 单个日志段 (.log + .index + .timeindex)
├── index.rs           — OffsetIndex / TimeIndex: mmap 稀疏索引
├── log_io.rs          — 底层文件 I/O + RecordBatch 字节构建
├── uring_io.rs        — io_uring Direct I/O 写入器 (Linux)
├── flush.rs           — FlushPolicy: async/sync/hybrid 刷盘策略
├── recovery.rs        — Crash Recovery: CRC 校验 + 截断损坏数据
├── retention.rs       — Retention: 按时间/大小删除旧 Segment
├── compaction.rs      — Log Compaction: Key 去重 + Tombstone 支持
└── tiered_storage.rs  — Tiered Storage: 冷热分离 + 远程存储
```

**CommitLog** (`commitlog.rs`):

每个 Partition 独占一个 `CommitLog`，管理多个 `LogSegment`：

```rust
pub struct CommitLog {
    topic: TopicName,
    partition: PartitionId,
    dir: PathBuf,                    // {data_dir}/{topic}-{partition}/
    segments: Vec<LogSegment>,       // 按 base_offset 排序
    active_segment_idx: usize,       // 当前 Active Segment
    log_start_offset: Offset,        // 第一条可用消息 (retention 推进)
    log_end_offset: Offset,          // 下一条消息的 offset
    high_watermark: Offset,          // 消费者可见最大 offset
    segment_max_size: u64,
}
```

核心操作：`append_batch()` 追加写入（自动滚动 Segment）、`read_range()` 按 offset 读取、`read_at_timestamp()` 按时间查找。

**LogSegment** (`segment.rs`):

每个 Segment 对应三个文件：
- `{base_offset}.log` — 消息日志 (append-only)
- `{base_offset}.index` — Offset → 物理位置稀疏索引
- `{base_offset}.timeindex` — Timestamp → Offset 稀疏索引

状态：`Active`（写入中）→ `Sealed`（已满，只读）。

**稀疏索引** (`index.rs`):

| 索引 | 条目大小 | 格式 | 查找 |
|:---|:---|:---|:---|
| OffsetIndex | 8 bytes | `relative_offset(u32) + physical_position(u32)` | 二分查找 O(log n) |
| TimeIndex | 12 bytes | `timestamp(i64) + relative_offset(u32)` | 二分查找 |

使用 `memmap2` 实现 mmap 映射，默认索引文件最大 10MB。每 4KB 数据写入一条索引条目（稀疏索引）。

**Crash Recovery** (`recovery.rs`):

扫描 partition 目录下所有 Segment 文件，逐个验证 CRC32C 校验和。遇到损坏数据时截断到最后一个有效 batch，保证数据完整性。

**Log Compaction** (`compaction.rs`):

支持三种清理策略：`Delete`（按时间/大小删除）、`Compact`（保留每个 Key 最新值）、`CompactDelete`（两者结合）。流程：扫描所有 batch 构建 key 索引 → 保留每个 key 最新值 → 重写压缩日志。支持 Tombstone（key 存在但 value 为 null）。

**Tiered Storage** (`tiered_storage.rs`):

冷热分离存储，将 sealed segment 异步迁移到远程存储：

```
TieredStorageManager
├── Local Tier (hot): Active Segment + 近期 Sealed Segment
└── Remote Tier (cold): RemoteLogManifest + RemoteStorage
    ├── upload_segment() — 上传到远程
    ├── fetch_segment()  — 从远程拉取
    └── RemoteStorage trait — 可对接 S3/GCS/ADLS (当前 LocalFS mock)
```

**I/O 引擎** (`uring_io.rs`):

`IoEngine` 枚举支持 `stdio`（标准文件 I/O）和 `io_uring`（Linux Direct I/O）。`UringWriter` 封装 io_uring 系统调用。`best_available_engine()` 自动检测平台选择最优引擎。

---

### 3.5 rk-broker — Broker 核心逻辑

**职责**: 45+ API Handler 实现、Partition 管理、偏移量管理、消费者组管理、SASL 认证、幂等/事务管理、请求路由。

**模块结构**:

```
rk-broker
├── router.rs              — BrokerRouter: 聚合所有 Handler，请求分发
├── partition.rs           — PartitionManager: DashMap<PartitionKey, CommitLog>
├── produce.rs             — ProduceHandler (API 0)
├── fetch.rs               — FetchHandler (API 1)
├── metadata_handler.rs    — MetadataHandler (API 3)
├── list_offsets_handler.rs — ListOffsetsHandler (API 2)
├── api_versions_handler.rs — ApiVersionsHandler (API 18)
├── create_topics_handler.rs — CreateTopicsHandler (API 19)
├── delete_topics_handler.rs — DeleteTopicsHandler (API 20)
├── create_partitions_handler.rs — CreatePartitionsHandler (API 37)
├── offset_commit_handler.rs — OffsetCommitHandler (API 8)
├── offset_fetch_handler.rs — OffsetFetchHandler (API 9)
├── offset_delete_handler.rs — OffsetDeleteHandler (API 47)
├── offset_manager.rs      — OffsetManager: 内存偏移量管理
├── persistent_offset_manager.rs — PersistentOffsetManager: 磁盘持久化
├── find_coordinator_handler.rs — FindCoordinatorHandler (API 10)
├── join_group_handler.rs  — JoinGroupHandler (API 11)
├── sync_group_handler.rs  — SyncGroupHandler (API 14)
├── heartbeat_handler.rs   — HeartbeatHandler (API 12)
├── leave_group_handler.rs — LeaveGroupHandler (API 13)
├── describe_groups_handler.rs — DescribeGroupsHandler (API 15)
├── list_groups_handler.rs — ListGroupsHandler (API 16)
├── group_manager.rs       — GroupManager: 消费者组状态机
├── sasl_handshake_handler.rs — SaslHandshakeHandler (API 17)
├── sasl_authenticate_handler.rs — SaslAuthenticateHandler (API 36)
├── sasl_authenticator.rs  — SaslAuthenticator: 认证逻辑
├── init_producer_id_handler.rs — InitProducerIdHandler (API 22)
├── add_partitions_to_txn_handler.rs — AddPartitionsToTxnHandler (API 24)
├── end_txn_handler.rs     — EndTxnHandler (API 26)
├── producer_state_manager.rs — ProducerStateManager: 事务状态机
├── transaction_coordinator.rs — TransactionCoordinator: 事务日志
├── describe_configs_handler.rs — DescribeConfigsHandler (API 32)
├── alter_configs_handler.rs — AlterConfigsHandler (API 33)
├── incremental_alter_configs_handler.rs — IncrementalAlterConfigsHandler (API 44)
├── delete_records_handler.rs — DeleteRecordsHandler (API 21)
├── elect_leaders_handler.rs — ElectLeadersHandler (API 43)
├── offset_for_leader_epoch_handler.rs — OffsetForLeaderEpochHandler (API 23)
├── describe_cluster_handler.rs — DescribeClusterHandler (API 60)
├── describe_topics_handler.rs — DescribeTopicsHandler (API 70)
├── describe_producers_handler.rs — DescribeProducersHandler (API 61)
├── list_transactions_handler.rs — ListTransactionsHandler (API 65)
├── alter_partition_reassignments_handler.rs — (API 45)
├── list_partition_reassignments_handler.rs — (API 46)
├── leader_and_isr_handler.rs — LeaderAndIsrHandler (API 4)
├── stop_replica_handler.rs — StopReplicaHandler (API 5)
├── update_metadata_handler.rs — UpdateMetadataHandler (API 6)
├── controlled_shutdown_handler.rs — ControlledShutdownHandler (API 7)
├── broker_registration_handler.rs — BrokerRegistrationHandler (API 54)
├── broker_heartbeat_handler.rs — BrokerHeartbeatHandler (API 55)
├── vote_handler.rs        — VoteHandler (API 51)
├── begin_quorum_epoch_handler.rs — BeginQuorumEpochHandler (API 52)
├── end_quorum_epoch_handler.rs — EndQuorumEpochHandler (API 53)
├── describe_quorum_handler.rs — DescribeQuorumHandler (API 56)
├── connection_session.rs  — ConnectionSession: 连接级会话跟踪
├── batch_accumulator.rs   — BatchAccumulatorManager: 攒批优化
├── config_reloader.rs     — ConfigReloader: SIGHUP 配置热加载
└── metrics.rs             — BrokerMetrics: 运行时指标收集
```

**BrokerRouter** (`router.rs`):

`BrokerRouter` 是请求处理的核心入口，聚合所有 Handler 实例：

```rust
pub struct BrokerRouter {
    produce_handler: ProduceHandler,
    fetch_handler: FetchHandler,
    metadata_handler: MetadataHandler,
    // ... 45+ handler 实例
    sasl_enabled: bool,
    authenticator: Arc<SaslAuthenticator>,
    acl_enabled: bool,
    metrics: Arc<BrokerMetrics>,
}
```

请求处理流程：
1. `handle_frame(frame_bytes)` → 解析 `RequestHeader` → 构建 `RequestContext`
2. `handle_request(ctx, body_bytes)` → 记录指标 → `encode_response_body()`
3. `encode_response_body()` → 根据 `api_key` match 分发到对应 Handler
4. Handler: `decode request → handle → encode response`
5. 组装 `ResponseHeader + ResponseBody` → 返回完整响应字节

**PartitionManager** (`partition.rs`):

使用 `DashMap<PartitionKey, CommitLog>` 管理所有 Partition，分片锁实现并发：

```rust
pub struct PartitionManager {
    data_dir: PathBuf,
    segment_max_size: u64,
    partitions: DashMap<PartitionKey, CommitLog>,  // 分片锁
    topics: DashMap<TopicName, TopicMetadata>,
    topic_configs: DashMap<TopicName, HashMap<String, String>>,
    broker_id: i32,
}
```

核心操作：
- `recover()`: 扫描数据目录，解析 `{topic}-{partition}` 目录结构，通过 Crash Recovery 恢复
- `get_or_create_topic()`: 自动创建 Topic 及 Partition 的 CommitLog
- `append_batch()`: 追加写入 RecordBatch，Phase 1 中 HW = LEO（写入立即可见）
- `read_batches()`: 按 offset 读取 RecordBatch
- `delete_topic()`: 移除所有相关 Partition 和元数据
- `add_partitions()`: 为已有 Topic 扩展 Partition

**GroupManager** (`group_manager.rs`):

消费者组状态机，管理成员、generation、leader、分区分配：

```
GroupState: Empty → PreparingRebalance → CompletingRebalance → Stable → Dead
```

使用 `DashMap<String, ConsumerGroup>` 管理所有消费者组。

**BrokerMetrics** (`metrics.rs`):

运行时指标收集，记录请求数、响应数、错误数、Produce/Fetch 消息量和字节量、连接数等。提供 `MetricsSnapshot` 快照用于 HTTP 端点导出。

---

### 3.6 rk-controller — KRaft 控制器

**职责**: KRaft 模式的集群控制层，基于 `openraft` 实现共识。

**模块结构**:

```
rk-controller
├── raft_node.rs          — RaftNode: openraft 类型配置和节点封装
├── metadata_sm.rs        — MetadataStateMachine: 应用日志条目到内存 Metadata
├── metadata_record.rs    — MetadataRecord/MetadataLog: 元数据日志
├── broker_reg.rs         — BrokerRegistry: Broker 注册表和心跳追踪
├── partition_alloc.rs    — PartitionAllocator: 副本分配策略 (轮询/机架感知)
├── rack_awareness.rs     — RackTopology: 机架感知拓扑
├── replica_placement.rs  — ReplicaPlacer: 副本放置和重分配
├── cluster_bootstrap.rs  — ClusterBootstrap: 集群初始化引导
└── feature_manager.rs    — FeatureManager: 集群功能版本管理
```

**架构**:

```
┌─────────────────────────────────────────────────┐
│              rk-controller                       │
│                                                  │
│  ┌──────────┐  ┌──────────────┐  ┌───────────┐  │
│  │ RaftNode │  │ Metadata SM  │  │ Broker    │  │
│  │(openraft)│  │ (apply log)  │  │ Registry  │  │
│  └────┬─────┘  └──────┬───────┘  └─────┬─────┘  │
│       │               │                │         │
│       └───────────────┼────────────────┘         │
│                       │                          │
│              ┌────────┴────────┐                 │
│              │ Partition Alloc │                 │
│              │ (round-robin /  │                 │
│              │  rack-aware)    │                 │
│              └─────────────────┘                 │
└─────────────────────────────────────────────────┘
```

**RaftNode**: 封装 `openraft::Raft` 实例，定义 `TypeConfig`、`LogEntry` 类型。`NodeRole` 表示 Leader/Follower/Candidate 角色。`LocalNode` 封装本地节点信息。

**MetadataStateMachine**: 应用 Raft 日志条目到内存 Metadata（Topic 创建/删除、Partition 分配、Broker 注册等）。提供 `MetadataSnapshot` 快照。

**PartitionAllocator**: 支持 `AllocationStrategy`（轮询/机架感知），输入 `BrokerInfo` 列表，输出 `PartitionAssignment`。

**RackAwareness**: `RackTopology` 管理 Broker 的机架分布，`RackAwareConfig` 配置机架感知策略，检测 `RackViolation`（副本在同一机架）。

**ReplicaPlacer**: `compute_reassignment()` 计算副本重分配方案，支持 `PlacementStrategy` 策略。

---

### 3.7 rk-replication — 副本复制引擎

**职责**: Kafka 副本协议核心组件：ISR 追踪、HW/LEO 管理、Leader 选举、副本拉取。

**模块结构**:

```
rk-replication
├── replica.rs         — Replica 数据结构 (ReplicaRole, ReplicaState, PartitionReplicaSet)
├── isr.rs             — ISRTracker: ISR 扩缩容 + Lag 检测
├── hw_manager.rs      — HighWatermarkManager: HW/LEO 计算 (单调递增)
├── leader.rs          — LeaderReplica: Follower 进度追踪
├── election.rs        — LeaderElector: Leader 选举 (ElectionStrategy)
├── replica_fetcher.rs — ReplicaFetcher: 跨 Broker 副本拉取
└── replica_manager.rs — ReplicaManager: 统一管理接口
```

**架构**:

```
ReplicaManager
    ├── PartitionReplicaSet (per partition)
    │       ├── Leader Replica
    │       ├── Follower Replica(s)
    │       └── ISR Set
    ├── HighWatermarkManager (HW/LEO 计算)
    └── ISRTracker (ISR 扩缩容)
```

**ISRTracker**: 追踪每个 Partition 的 ISR（In-Sync Replicas）集合。`ISRConfig` 配置 lag 阈值，`ShrinkReason` 记录缩容原因。

**HighWatermarkManager**: 管理 HW（High Watermark）和 LEO（Log End Offset）。HW 单调递增，基于 ISR 中最小的 LEO 计算。`LagStats` 统计副本滞后信息。

**LeaderElector**: 支持多种 `ElectionStrategy`，`ElectionConfig` 配置选举参数（如是否允许 Unclean Leader Election）。

**ReplicaFetcher**: `ReplicaFetcherConfig` 配置拉取参数，`ReplicationFetchRequest/Response` 定义跨 Broker 拉取协议。

---

### 3.8 rk-security — 安全模块

**职责**: 传输加密 (TLS/mTLS)、认证 (SASL)、授权 (ACL)、审计日志。

**模块结构**:

```
rk-security
├── tls.rs              — TLS/mTLS: 基于 rustls 的传输层加密
├── sasl.rs             — SASL: PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512
├── acl.rs              — ACL: Kafka 风格的访问控制列表
├── api_permissions.rs  — API 权限映射: 每个 API 对应的操作和资源类型
├── audit.rs            — AuditLogger: 安全事件审计日志
└── auth_pipeline.rs    — SecurityPipeline: 认证/授权流水线
```

**TLS** (`tls.rs`):

基于 `rustls` 实现，支持 `OneWay`（单向 TLS）和 `Mutual`（mTLS 双向认证）两种模式。`TlsConfig` 配置证书路径、CA 路径、协议版本。`create_tls_acceptor()` 创建服务端 TLS 接受器，`create_tls_connector()` 创建客户端连接器。

**SASL** (`sasl.rs`):

支持三种机制：`PLAIN`、`SCRAM-SHA-256`、`SCRAM-SHA-512`。`SaslSession` 管理认证会话状态，`UserDatabase` 存储用户凭证，`AuthState` 跟踪认证进度。

**ACL** (`acl.rs`):

Kafka 风格的 ACL 引擎：

```rust
pub struct AclEngine {
    // AclEntry: principal + host + resource_type + resource_name + operation + permission
}
```

支持 `ResourceType`（Topic/Group/Cluster 等）、`AclOperation`（Read/Write/Create 等）、`PermissionType`（Allow/Deny）。提供便捷构造函数：`topic_read_acl()`、`topic_write_acl()`、`prefix_acl()`、`super_user_acl()`。

**API Permissions** (`api_permissions.rs`):

`api_permission(api_key)` 返回每个 API 所需的权限（`ApiPermission`），包含 `resource_type`、`operation`、`is_pre_auth` 标识。`is_pre_auth_api()` 判断认证前允许的 API。

**SecurityPipeline** (`auth_pipeline.rs`):

`SecurityPipeline` 组合 TLS + SASL + ACL 的完整认证/授权流水线。`SecurityContext` 携带连接级安全上下文。

---

### 3.9 rk-observability — 可观测性

**职责**: 统一的 Tracing / Metrics / Logging 基础设施。

**模块结构**:

```
rk-observability
├── tracing_setup.rs  — Tracing 初始化 + OpenTelemetry 集成
└── metrics.rs        — PrometheusMetrics: 全局 Prometheus 指标注册
```

**Tracing** (`tracing_setup.rs`):

`init_tracing()` / `init_tracing_with_config()` 初始化结构化日志。支持 `TracingLayer`：`fmt`（控制台）、`json`（JSON 格式）。`TracingConfig` 配置日志级别和输出层。`TracingGuard` 管理 tracing 生命周期。

**Metrics** (`metrics.rs`):

`PrometheusMetrics` 提供全局指标注册和收集。`shared_metrics()` 返回 `SharedPrometheusMetrics`（`Arc` 包装），供多个模块共享。

---

### 3.10 rk-client — Rust Client SDK

**职责**: Rust 原生的 Kafka 客户端，可连接 R-Kafka 或 Java Kafka Broker。

**模块结构**:

```
rk-client
├── producer.rs    — KafkaProducer: 消息生产者 (acks=0/1/all)
├── consumer.rs    — KafkaConsumer: 消息消费者 (消费组 + 偏移量管理)
├── admin.rs       — KafkaAdmin: 管理客户端 (Topic CRUD)
├── connection.rs  — ConnectionPool + BrokerConnection: 连接池
├── metadata.rs    — MetadataCache: 元数据缓存
├── config.rs      — ClientConfig / ProducerConfig / ConsumerConfig
├── assignor.rs    — 分区分配策略 (Range/RoundRobin/CooperativeSticky)
└── error.rs       — ClientError / ClientResult
```

**架构**:

```
┌─────────────────────────────────────────────────┐
│              rk-client                           │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │ Producer │  │ Consumer │  │ Admin         │  │
│  └────┬─────┘  └────┬─────┘  └──────┬───────┘  │
│       │             │               │           │
│       └─────────────┼───────────────┘           │
│                     │                           │
│            ┌────────┴────────┐                  │
│            │ ConnectionPool  │                  │
│            │ + MetadataCache │                  │
│            └────────┬────────┘                  │
└─────────────────────┼───────────────────────────┘
                      │ TCP (Kafka Wire Protocol)
             ┌────────┴────────┐
             │ Kafka Broker(s) │
             └─────────────────┘
```

**分区分配策略** (`assignor.rs`):

实现三种 Kafka 标准分区分配策略：
- `RangeAssignor`: 按范围分配（默认）
- `RoundRobinAssignor`: 轮询分配
- `CooperativeStickyAssignor`: 协作式粘性分配（最小化 Rebalance 时的分区迁移）

---

### 3.11 rk-server — 服务入口

**职责**: 二进制入口、配置加载、组件组装、优雅关闭。

**启动流程** (`main.rs`):

```
1. CLI 解析 (clap) → 加载配置文件路径
2. 初始化 tracing 日志 (env-filter)
3. 加载 BrokerConfig (TOML + 环境变量覆盖)
4. 打印启动 Banner (版本/配置摘要)
5. 创建 ConfigReloader (SIGHUP 热加载)
6. 恢复存储引擎: PartitionManager.recover()
7. 初始化 SASL 认证器 (如果启用)
8. 初始化 OffsetManager + PersistentOffsetManager
9. 启动偏移量定期快照任务 (每 30 秒)
10. 初始化 BrokerRouter (含 SASL 配置)
11. 创建优雅关闭信号通道 (watch)
12. 启动 HTTP 监控服务器 (如果启用)
13. 启动 TCP 服务器 (含可选 TLS)
14. 设置 SIGHUP 信号处理 (配置热加载)
15. 等待关闭信号 (SIGINT / SIGTERM)
16. 优雅关闭: 停止接受新连接 → 等待现有连接 → 保存偏移量 → 打印最终指标
```

**优雅关闭**:

监听 `SIGINT`（Ctrl-C）和 `SIGTERM`（Kubernetes），收到信号后：
1. 发送 `watch` 关闭信号
2. TCP Server 停止接受新连接
3. 等待现有连接处理完成（超时 5 秒）
4. 停止 HTTP 监控服务器
5. 最后一次保存偏移量快照
6. 打印最终 Broker 指标

---

## 4. 请求处理全链路

### 4.1 Produce 请求路径

```
Client → TCP → Connection.read_frame()
       → BrokerRouter.handle_frame()
         → RequestHeader.decode()
         → ProduceRequest.decode()
         → ProduceHandler.handle()
           → PartitionManager.append_batch()
             → CommitLog.append_batch()
               → LogSegment.write_batch()
                 → 写入 .log 文件
                 → 更新 OffsetIndex / TimeIndex
               → 检查 Segment 大小 → 自动滚动
             → HW = LEO (Phase 1)
         → ProduceResponse.encode()
       → Connection.write_frame()
       → Client
```

### 4.2 Fetch 请求路径

```
Client → TCP → Connection.read_frame()
       → BrokerRouter.handle_frame()
         → FetchRequest.decode()
         → FetchHandler.handle()
           → PartitionManager.read_batches()
             → CommitLog.read_range()
               → OffsetIndex 二分查找定位
               → LogSegment.read_batches()
                 → 从 .log 文件读取 RecordBatch
         → FetchResponse.encode()
       → Connection.write_frame()
       → Client

(零拷贝优化路径):
         → zero_copy_send(segment_file, offset, len, socket)
           → Linux: splice(file → pipe → socket)
           → macOS: sendfile(file → socket)
```

### 4.3 Consumer Group 协议流程

```
1. FindCoordinator → 获取 Group Coordinator 所在 Broker
2. JoinGroup       → 成员加入，Coordinator 选举 Leader，确定分配策略
3. Leader 客户端执行分区分配 (Range/RoundRobin/CooperativeSticky)
4. SyncGroup       → 成员提交分配结果，获取自己的分区
5. Heartbeat       → 定期心跳维持成员资格
6. OffsetCommit    → 提交消费偏移量
7. OffsetFetch     → 查询已提交偏移量
8. LeaveGroup      → 主动离开消费组
```

---

## 5. 存储格式

### 5.1 目录结构

```
{data_dir}/
├── {topic}-{partition}/
│   ├── 00000000000000000000.log          — Segment 0 消息日志
│   ├── 00000000000000000000.index        — Segment 0 Offset 稀疏索引
│   ├── 00000000000000000000.timeindex    — Segment 0 Time 稀疏索引
│   ├── 00000000000000100000.log          — Segment 1 (base_offset=100000)
│   ├── 00000000000000100000.index
│   └── 00000000000000100000.timeindex
├── offsets/                              — PersistentOffsetManager 快照
└── remote/                               — Tiered Storage 远程段本地缓存
```

### 5.2 RecordBatch v2 磁盘格式

```
┌──────────────────────────────────────────────────────────────┐
│ base_offset            (i64)   8 bytes                       │
│ batch_length           (i32)   4 bytes                       │
│ partition_leader_epoch (i32)   4 bytes                       │
│ magic                  (i8)    1 byte   (= 2)                │
│ crc                    (i32)   4 bytes  (CRC32C)             │
│ attributes             (i16)   2 bytes                       │
│ last_offset_delta      (i32)   4 bytes                       │
│ base_timestamp         (i64)   8 bytes                       │
│ max_timestamp          (i64)   8 bytes                       │
│ producer_id            (i64)   8 bytes                       │
│ producer_epoch         (i16)   2 bytes                       │
│ base_sequence          (i32)   4 bytes                       │
│ records_count          (i32)   4 bytes                       │
│ records                (...)   variable                      │
└──────────────────────────────────────────────────────────────┘
总头部固定: 61 bytes
CRC32C 覆盖范围: 从 magic 到 batch 末尾
```

### 5.3 稀疏索引格式

**OffsetIndex** (每条 8 bytes):
```
| relative_offset (u32) | physical_position (u32) |
```
- `relative_offset`: 相对于 Segment base_offset
- `physical_position`: 在 `.log` 文件中的字节偏移

**TimeIndex** (每条 12 bytes):
```
| timestamp (i64) | relative_offset (u32) |
```

---

## 6. 安全架构

### 6.1 传输层安全 (TLS/mTLS)

```
Client ──TLS──→ rk-network (TcpListener)
                  │
                  ├── TlsAcceptor (rustls)
                  │   ├── OneWay: 服务端证书
                  │   └── Mutual: 服务端证书 + 客户端证书验证
                  └── TlsStream → Connection → 正常处理
```

### 6.2 认证流程 (SASL)

```
1. SaslHandshake (API 17)
   → 协商机制 (PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512)
   → ConnectionState: Connected → SaslHandshaked

2. SaslAuthenticate (API 36)
   → PLAIN: 解析 \0username\0password 格式
   → SCRAM: 多轮质询-响应
   → ConnectionState: SaslHandshaked → Authenticated

认证前仅允许: ApiVersions(18), SaslHandshake(17), SaslAuthenticate(36)
```

### 6.3 授权 (ACL)

```
Request → BrokerRouter.check_authorization()
        → api_permission(api_key) → 获取所需权限
        → AclEngine.authorize(principal, host, resource_type, resource_name, operation)
        → Allow / Deny
```

---

## 7. 可观测性

### 7.1 指标导出

| 端点 | 格式 | 内容 |
|:---|:---|:---|
| `GET /metrics` | JSON | uptime, requests, responses, errors, produce/fetch 计数, 消息量, 字节量, 连接数 |
| `GET /metrics/prometheus` | Prometheus text | `rk_broker_requests_total`, `rk_broker_messages_produced_total` 等 |

### 7.2 结构化日志

基于 `tracing` 框架，支持 `fmt`（控制台人类可读）和 `json`（机器解析）两种输出格式。通过 `RUST_LOG` 环境变量控制日志级别。

---

## 8. 部署

### 8.1 配置示例

| 文件 | 用途 |
|:---|:---|
| `config/examples/single-node.toml` | 单节点开发环境 |
| `config/examples/cluster-node1.toml` | 多节点集群 (Node 1 of 3) |
| `config/examples/production.toml` | 生产环境优化 |
| `config/examples/tls-config.toml` | TLS/mTLS 加密 |
| `config/examples/sasl-acl-config.toml` | SASL 认证 + ACL 授权 |

### 8.2 Kubernetes 部署

`deploy/helm/r-k-kafka/` 提供完整的 Helm Chart：

| 文件 | 用途 |
|:---|:---|
| `statefulset.yaml` | StatefulSet 部署 |
| `service.yaml` | Service 暴露 |
| `configmap.yaml` | 配置注入 |
| `secret.yaml` | 敏感信息 (TLS 密钥、SASL 密码) |
| `certificate.yaml` | cert-manager 证书 |
| `pdb.yaml` | PodDisruptionBudget |
| `servicemonitor.yaml` | Prometheus ServiceMonitor |
| `serviceaccount.yaml` | ServiceAccount |

### 8.3 优雅关闭

- 监听 `SIGINT`（Ctrl-C）和 `SIGTERM`（Kubernetes）
- 停止接受新连接 → 等待现有连接（超时 5 秒）→ 保存偏移量快照 → 打印最终指标

---

## 9. 测试策略

```bash
# 全量测试
cargo test --workspace

# 按模块测试
cargo test -p rk-protocol    # 协议兼容性
cargo test -p rk-storage     # 存储引擎
cargo test -p rk-broker      # Broker 逻辑
cargo test -p rk-server --test e2e  # 端到端集成

# 代码质量
cargo fmt --all              # 格式化
cargo clippy --workspace     # Lint
```

---

## 10. 项目状态

| 组件 | 状态 |
|:---|:---|
| Wire Protocol (45+ APIs) | 已实现 |
| 存储引擎 (CommitLog, Index, Recovery) | 已实现 |
| io_uring Direct I/O | 已实现 |
| 零拷贝 Fetch (splice/sendfile) | 已实现 |
| KRaft Controller (openraft) | 已实现 |
| 副本管理 (ISR, HW/LEO) | 已实现 |
| 消费者组协议 | 已实现 |
| 事务协调器 (EOS) | 已实现 |
| 安全 (TLS, SASL, ACL) | 已实现 |
| 分层存储 (Tiered Storage) | 已实现 |
| 日志压缩 (Log Compaction) | 已实现 |
| Rust Client SDK | 已实现 |
| 跨实现互操作测试 | 进行中 |
