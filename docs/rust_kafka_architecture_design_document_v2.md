# R-Kafka 架构设计文档（精简版）

> **纯粹 Rust 实现的分布式消息流平台**
>
> 设计原则：零拷贝 · 无锁化 · 类型安全 · 最小依赖 · Kafka 协议兼容 · 双向互操作

---

## 1. 设计哲学

R-Kafka 不是 Kafka 的简单移植，而是利用 Rust 语言特性从底层重新构建的高性能消息流引擎。

### 1.1 核心原则

| 原则 | 说明 |
|:---|:---|
| **Zero-Copy** | 全链路零拷贝：网络接收 → 协议解析 → 磁盘写入 → 网络发送，数据不拷贝 |
| **Thread-per-Core** | 每个 CPU 核独立运行，无共享状态，无跨核锁竞争 |
| **Type Safety** | 利用 Rust 类型系统在编译期消除协议错误、状态机非法转换 |
| **Minimal Dependency** | 核心路径仅依赖 `tokio`、`bytes`、`io-uring`、`rustls`，拒绝臃肿框架 |
| **Kafka Compatible** | 完整兼容 Kafka Wire Protocol，现有 Kafka Client 无感迁移 |

### 1.2 移除的冗余设计

相较原版文档，本精简版做出以下裁剪：

| 移除项 | 原因 |
|:---|:---|
| Glommio 运行时 | 生态不成熟，改用 Tokio + io_uring 实现同等性能 |
| Stream Processing 引擎 | 独立产品领域，不属于消息内核职责 |
| Schema Registry | 独立服务，不应嵌入 Broker 进程 |
| Multi-Region Event Mesh | 运维拓扑问题，非内核设计关注点 |
| AI 智能运维 | 外部系统能力，非内核架构关注点 |
| 运维管理平台 UI | 独立前端项目 |
| Client SDK 详细设计（多语言） | 移除多语言 SDK，保留 Rust Client + 协议互操作设计（见第 12 章） |
| Edge Broker / 边缘计算 | 独立部署形态，非核心架构 |
| UIPA / DGF / AgentForge 集成 | 外部系统边界问题 |

---

## 2. 系统总体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Kafka Clients                                  │
│                   (Java / Go / Python / Rust / C++)                     │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                    Kafka Wire Protocol (TCP)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       R-Kafka Broker Node                               │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │              Network Layer (Tokio + io_uring)                     │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │  │
│  │  │  Core 0     │  │  Core 1     │  │  Core N     │              │  │
│  │  │  Reactor    │  │  Reactor    │  │  Reactor    │              │  │
│  │  │  + ConnMgr  │  │  + ConnMgr  │  │  + ConnMgr  │              │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘              │  │
│  └───────────────────────────┬───────────────────────────────────────┘  │
│                              │ (Message Passing)                        │
│                              ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                  Protocol & Routing Layer                         │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │  │
│  │  │ Codec Engine │  │ API Router   │  │ Partition Router   │     │  │
│  │  │ (Zero-Copy)  │  │ (Dispatch)   │  │ (Topic→Partition)  │     │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘     │  │
│  └───────────────────────────┬───────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                    Storage Engine                                 │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │  │
│  │  │  CommitLog   │  │ Offset Index │  │  Time Index        │     │  │
│  │  │  (Append)    │  │ (mmap)       │  │  (mmap)            │     │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘     │  │
│  │  ┌──────────────┐  ┌──────────────┐                             │  │
│  │  │  Segment Mgr │  │  Recovery    │                             │  │
│  │  └──────────────┘  └──────────────┘                             │  │
│  └───────────────────────────┬───────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │              Replication & Group Coordination                     │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │  │
│  │  │  ISR Manager │  │  Replica     │  │  Group Coordinator │     │  │
│  │  │  (HW/LEO)    │  │  Fetcher     │  │  (Rebalance)       │     │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘     │  │
│  └───────────────────────────┬───────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │              KRaft Metadata Consensus (OpenRaft)                  │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐     │  │
│  │  │  Raft Node   │  │  Metadata    │  │  Broker Registry   │     │  │
│  │  │  (Election)  │  │  StateMachine│  │  (Heartbeat)       │     │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘     │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.1 核心模块一览

| 模块 | Crate 名称 | 职责 | 核心依赖 |
|:---|:---|:---|:---|
| 核心类型 | `rk-core` | 公共类型、错误模型、配置 | `thiserror`, `serde` |
| 网络引擎 | `rk-network` | TCP 监听、连接管理、io_uring Reactor | `tokio`, `io-uring` |
| 协议引擎 | `rk-protocol` | Kafka Wire Protocol 编解码 | `bytes`, `tokio-util` |
| Broker 逻辑 | `rk-broker` | Produce/Fetch 处理、Partition 路由 | — |
| 存储引擎 | `rk-storage` | CommitLog、Segment、Index、Recovery | `memmap2`, `libc` |
| 副本管理 | `rk-replication` | Leader/Follower、ISR、HW/LEO | — |
| 消费者组 | `rk-broker` + `rk-client` | Group Coordinator (Broker 端)、Rebalance、Offset (注: 未独立为 crate，Coordinator 逻辑在 rk-broker，客户端逻辑在 rk-client) | — |
| 控制器 | `rk-controller` | KRaft Raft 节点、元数据状态机 | `openraft`, `serde` |
| 安全 | `rk-security` | TLS、SASL、ACL | `rustls` |
| 可观测性 | `rk-observability` | Metrics、Tracing | `prometheus`, `tracing` |
| Rust 客户端 | `rk-client` | KafkaProducer、KafkaConsumer、KafkaAdmin | `rk-protocol`, `tokio` |
| 入口 | `rk-server` | 二进制入口、配置加载 | `clap`, `toml` |

---

## 3. 存储引擎设计

存储引擎是 R-Kafka 性能的基石。采用 Append-Only CommitLog + 稀疏索引 + io_uring Direct I/O 的组合。

### 3.1 物理文件组织

每个 Topic-Partition 对应独立目录，内含多个 Segment：

```
/data/r-kafka/logs/orders-0/
├── 00000000000000000000.log        # 消息日志 (io_uring 追加写)
├── 00000000000000000000.index      # Offset → 物理位置 稀疏索引
└── 00000000000000000000.timeindex  # Timestamp → Offset 稀疏索引
```

### 3.2 数据格式

> **重要**：磁盘格式与 Kafka RecordBatch v2 **完全一致**，确保跨实现数据互通（详见第 12 章）。

#### RecordBatch 布局 (Kafka v2 兼容)

```
┌──────────────────────────────────────────────────────────────────────┐
│ base_offset (i64)          │ 8 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ batch_length (i32)         │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ partition_leader_epoch     │ 4 bytes (i32)                           │
├────────────────────────────┼─────────────────────────────────────────┤
│ magic (i8) = 2             │ 1 byte                                  │
├────────────────────────────┼─────────────────────────────────────────┤
│ crc (i32)                  │ 4 bytes  CRC32C(magic..records)         │
├────────────────────────────┼─────────────────────────────────────────┤
│ attributes (i16)           │ 2 bytes  见下方位定义                       │
├────────────────────────────┼─────────────────────────────────────────┤
│ last_offset_delta (i32)    │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ base_timestamp (i64)       │ 8 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ max_timestamp (i64)        │ 8 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ producer_id (i64)          │ 8 bytes  (-1 = 未启用)                  │
├────────────────────────────┼─────────────────────────────────────────┤
│ producer_epoch (i16)       │ 2 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ base_sequence (i32)        │ 4 bytes  (-1 = 未启用)                  │
├────────────────────────────┼─────────────────────────────────────────┤
│ record_count (i32)         │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ records ...                │ variable                                │
└────────────────────────────┴─────────────────────────────────────────┘
```

#### attributes (i16) 位定义

```
Bit 0-2  : 压缩类型 (0=None, 1=GZIP, 2=Snappy, 3=LZ4, 4=Zstd)
Bit 3    : 时间戳类型 (0=CreateTime, 1=LogAppendTime)
Bit 4    : isTransactional (0=false, 1=true)
Bit 5    : isControlBatch (0=false, 1=true)  — 控制批次 (TxnMarker 等)
Bit 6    : hasDeleteHorizonMs (0=false, 1=true) — KIP-405 删除时间线标记
Bit 7-15 : 保留未使用
```

#### 单条 Record 布局 (Kafka v2 兼容)

```
┌──────────────────────────────────────────────────────────────────────┐
│ length (varint)            │ 本条 record 字节数                       │
├────────────────────────────┼─────────────────────────────────────────┤
│ attributes (i8) = 0        │ 1 byte  (当前保留)                       │
├────────────────────────────┼─────────────────────────────────────────┤
│ timestamp_delta (varlong)  │ 相对于 base_timestamp                    │
├────────────────────────────┼─────────────────────────────────────────┤
│ offset_delta (varint)      │ 相对于 base_offset                       │
├────────────────────────────┼─────────────────────────────────────────┤
│ key_len (varint)           │ -1 = null                               │
├────────────────────────────┼─────────────────────────────────────────┤
│ key (bytes)                │ key_len bytes                           │
├────────────────────────────┼─────────────────────────────────────────┤
│ value_len (varint)         │ -1 = null (tombstone)                   │
├────────────────────────────┼─────────────────────────────────────────┤
│ value (bytes)              │ value_len bytes                         │
├────────────────────────────┼─────────────────────────────────────────┤
│ header_count (varint)      │ header 数量                             │
├────────────────────────────┼─────────────────────────────────────────┤
│ [headers...]               │ key_len(varint)+key+value_len(varint)+v │
└────────────────────────────┴─────────────────────────────────────────┘
```

#### Rust 数据结构

```rust
/// RecordBatch 头部 — 与 Kafka v2 RecordBatch 格式完全一致
/// 注意：此结构体仅用于逻辑表示，实际编解码通过 KafkaReader/KafkaWriter 完成
pub struct RecordBatchHeader {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,                     // 必须 = 2
    pub crc: i32,                      // CRC32C(magic..records)
    pub attributes: i16,               // bit 0-2: compression type
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,              // 幂等: -1 = 未启用
    pub producer_epoch: i16,
    pub base_sequence: i32,            // 幂等: -1 = 未启用
    pub record_count: i32,
}

pub struct Record {
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Bytes>,            // None 表示 Tombstone
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum CompressionType {
    None   = 0,
    Gzip   = 1,
    Snappy = 2,
    Lz4    = 3,
    Zstd   = 4,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum TimestampType {
    CreateTime   = 0,
    LogAppendTime = 1,
}
```

### 3.3 Segment 生命周期

```
  Created ──► Active (写入中) ──► Sealed (≥1GB 或 ≥30min) ──► Deleted (Retention 到期)
```

```rust
pub struct LogSegment {
    base_offset: u64,
    file: File,                    // O_DIRECT 文件句柄
    write_position: u64,           // 当前写入位置
    max_timestamp: i64,
    state: SegmentState,
}

pub enum SegmentState {
    Active,    // 正在接收写入
    Sealed,    // 已满，只读
    Deleted,   // 待清理
}
```

### 3.4 写入路径 (Write Path)

```
Producer Request
      │
      ▼
Partition Actor (Thread-per-Core 绑定)
      │
      ▼
Batch Accumulator (攒批，max 1MB 或 5ms)
      │
      ▼
io_uring SQE Submit (批量 Direct I/O 写入)
      │
      ▼
Completion Queue Event (写入完成回调)
      │
      ▼
Update Offset Index (稀疏索引，每 4KB 一条)
      │
      ▼
Replica Sync (异步通知 Follower)
      │
      ▼
ACK Response
```

核心实现：

> **Thread-per-Core 说明**：每个 Partition 的 CommitLog 由一个 Partition Actor 独占持有（`&mut self`），
> 无跨线程共享，无需 `Arc<Mutex<>>`。并发通过消息传递（channel）路由到对应 Actor。

```rust
/// 每个 Partition 独占一个 CommitLog，绑定到单个 Partition Actor (Thread-per-Core)
pub struct CommitLog {
    partition_id: PartitionId,
    segments: Vec<LogSegment>,
    active_segment: LogSegment,
    high_watermark: u64,
    log_end_offset: u64,
}

impl CommitLog {
    /// 追加写入一批 RecordBatch，利用 io_uring 批量提交
    pub async fn append_batch(&mut self, batch: &RecordBatch) -> Result<u64> {
        let segment = self.active_segment_mut();

        // 检查是否需要滚动 Segment
        if segment.write_position >= SEGMENT_MAX_SIZE {
            self.roll_segment().await?;
        }

        let offset = segment.append(batch).await?;
        self.log_end_offset = offset;

        // 更新稀疏索引
        self.index.append_entry(
            offset - segment.base_offset,
            segment.write_position,
            batch.first_timestamp,
        );

        Ok(offset)
    }
}
```

### 3.5 索引查找 (Read Path)

**Offset 查找流程**：

1. 根据目标 Offset 定位到对应 Segment
2. 在 `.index` 文件（mmap）中二分查找，找到 ≤ target 的最大条目
3. 从该物理位置开始顺序扫描 `.log`，找到精确消息

```rust
pub struct OffsetIndex {
    mmap: MmapMut,                    // memmap2 映射
    entry_count: usize,
}

#[repr(C)]
pub struct IndexEntry {
    pub relative_offset: u32,         // 相对于 Segment base_offset
    pub physical_position: u32,       // 在 .log 文件中的字节偏移
}

impl OffsetIndex {
    /// 二分查找，O(log n)
    pub fn lookup(&self, target_offset: u64) -> Option<IndexEntry> {
        let entries = self.entries();
        let idx = entries.partition_point(|e| e.relative_offset as u64 <= target_offset);
        if idx > 0 { Some(entries[idx - 1]) } else { None }
    }
}
```

**Time 查找**：在 `.timeindex` 中二分查找最接近的 timestamp，获取 offset 后走上述流程。

### 3.6 Flush 策略

| 模式 | 行为 | 适用场景 |
|:---|:---|:---|
| **Async Flush** | 写入 Page Cache 即返回，后台刷盘 | 高吞吐，允许少量丢失 |
| **Sync Flush** | `fsync` 后才返回 ACK | 强一致，金融级 |
| **Hybrid (推荐)** | io_uring 批量提交，每 5ms 或 1MB 触发一次 | 吞吐与安全的平衡 |

### 3.7 Crash Recovery

Broker 重启时执行恢复流程：

```
Broker Start
      │
      ▼
加载所有 Segment 元数据 (base_offset, file_size)
      │
      ▼
定位 Active Segment (最后一个未 Sealed 的)
      │
      ▼
从 Segment 尾部向前扫描，逐条验证 CRC32C
      │
      ├── CRC 正常 → 更新 log_end_offset
      │
      └── CRC 异常 → 截断 (truncate) 到最后一个完整 Record
      │
      ▼
重建 Offset Index 和 Time Index
      │
      ▼
恢复 High Watermark (从 Replica 状态)
      │
      ▼
Resume Append — 恢复服务
```

### 3.8 Log Retention & Compaction

**Retention 策略**（二选一或组合）：

- **Size-based**: `log.retention.bytes = 1TB`，超过后删除最老 Segment
- **Time-based**: `log.retention.ms = 7d`，超过 7 天的 Segment 删除

**Log Compaction**（用于 KV 语义 Topic）：

保留每个 Key 的最新值，删除历史版本。

```rust
pub struct LogCleaner {
    cleaner_id: u32,
    dirty_segments: Vec<SegmentId>,
    key_index: HashMap<Bytes, LogPosition>,  // Key → 最新位置
}

impl LogCleaner {
    /// 扫描 dirty segment，构建 key 索引，重写 compacted segment
    pub async fn compact(&mut self) -> Result<()> {
        self.build_key_index().await?;
        self.rewrite_segments().await?;
        self.swap_segments().await?;
        Ok(())
    }
}
```

---

## 4. 网络与协议层设计

### 4.1 网络模型

采用 **Tokio + io_uring** 混合模型：

- **Tokio** 管理连接生命周期、协程调度、TLS 握手
- **io_uring** 用于大数据量的磁盘读写（Produce 写盘、Fetch 读盘）
- **SO_REUSEPORT** 将连接均匀分发到各 Worker 线程

```
                    Client Connections
                           │
                           ▼
                  ┌─────────────────┐
                  │  SO_REUSEPORT   │
                  │  (Kernel LB)    │
                  └────────┬────────┘
               ┌───────────┼───────────┐
               ▼           ▼           ▼
         ┌──────────┐┌──────────┐┌──────────┐
         │ Worker 0 ││ Worker 1 ││ Worker N │
         │          ││          ││          │
         │ Tokio    ││ Tokio    ││ Tokio    │
         │ Runtime  ││ Runtime  ││ Runtime  │
         │          ││          ││          │
         │ io_uring ││ io_uring ││ io_uring │
         │ (disk)   ││ (disk)   ││ (disk)   │
         └──────────┘└──────────┘└──────────┘
```

### 4.2 连接生命周期

```rust
pub enum ConnectionState {
    New,
    Authenticating,   // TLS/SASL 握手中
    Ready,            // 可处理请求
    Closing,          // 优雅关闭中
    Closed,
}

pub struct Connection {
    pub id: ConnectionId,
    pub socket: TcpStream,
    pub state: ConnectionState,
    pub read_buffer: BytesMut,         // 零拷贝读缓冲
    pub write_queue: VecDeque<Bytes>,  // 写队列
    pub client_info: ClientInfo,
    pub created_at: Instant,
}
```

### 4.3 Kafka Wire Protocol 编解码

**零拷贝解码**：利用 `bytes::Bytes` 的 Slice 机制，解析 Header 和 RecordBatch 时仅移动指针，不分配内存。

```
TCP Buffer (Bytes)
      │
      │ Bytes::slice() — 零拷贝，仅移动指针
      ▼
Frame Decoder (4字节长度前缀)
      │
      ▼
Request Header Decoder (api_key, api_version, correlation_id, client_id)
      │
      ▼
API Router (根据 api_key 分发到对应 Handler)
      │
      ▼
Request Handler (ProduceHandler / FetchHandler / ...)
```

**核心 Trait**：

```rust
/// Kafka 请求解码器 — 零拷贝
pub trait KafkaRequestDecoder {
    fn decode(buf: &mut BytesMut) -> Result<Self>
    where
        Self: Sized;
}

/// Kafka 响应编码器
pub trait KafkaResponseEncoder {
    fn encode(&self, buf: &mut BytesMut) -> Result<()>;
}

/// 请求处理器
#[async_trait]
pub trait RequestHandler: Send + Sync {
    async fn handle(&self, ctx: RequestContext, request: Bytes) -> Result<Bytes>;
}
```

### 4.4 Kafka API 支持范围

**Phase 1 (MVP)**：

| API Key | 名称 | 用途 |
|:---|:---|:---|
| 0 | Produce | 写入消息 |
| 1 | Fetch | 消费消息 |
| 2 | ListOffsets | 查询 Offset |
| 3 | Metadata | Topic/Partition 元数据 |
| 18 | ApiVersions | 客户端能力协商 |

**Phase 2 (Consumer Group)**：

| API Key | 名称 | 用途 |
|:---|:---|:---|
| 8 | OffsetCommit | 提交消费位点 |
| 9 | OffsetFetch | 查询消费位点 |
| 10 | FindCoordinator | 定位 Group Coordinator |
| 11 | JoinGroup | 加入消费组 |
| 12 | Heartbeat | 保活 |
| 13 | LeaveGroup | 离开消费组 |
| 14 | SyncGroup | 同步分区分配 |
| 15 | DescribeGroups | 查询消费组状态 |
| 16 | ListGroups | 列出所有消费组 |

**Phase 3 (管理 & 安全)**：

| API Key | 名称 | 用途 |
|:---|:---|:---|
| 17 | SaslHandshake | SASL 认证协商 |
| 19 | CreateTopics | 创建 Topic |
| 20 | DeleteTopics | 删除 Topic |
| 21 | DeleteRecords | 删除记录 |
| 22 | InitProducerId | 幂等 Producer ID 分配 |
| 23 | OffsetForLeaderEpoch | 查询 Leader Epoch 对应的 Offset (副本截断) |
| 32 | DescribeConfigs | 查询配置 |
| 33 | AlterConfigs | 修改配置 |
| 36 | SaslAuthenticate | SASL 认证 |
| 37 | CreatePartitions | 增加 Partition |
| 43 | ElectLeaders | 触发 Leader 选举 |

**Phase 4 (Broker 集群互操作)**：

| API Key | 名称 | 用途 |
|:---|:---|:---|
| 4 | LeaderAndIsr | Controller→Broker：Leader/ISR 变更通知 |
| 5 | StopReplica | Controller→Broker：停止副本 |
| 6 | UpdateMetadata | Controller→Broker：元数据广播 |
| 7 | ControlledShutdown | Broker→Controller：优雅关闭 |
| 1 | Fetch (Replica) | Broker→Broker：副本复制 (同 Fetch API，replica_id ≥ 0) |
| 51 | Vote | KRaft Raft 投票 |
| 52 | BeginQuorumEpoch | KRaft 新纪元通知 |
| 53 | EndQuorumEpoch | KRaft 纪元结束 |
| 54 | BrokerRegistration | Broker→Controller：Broker 注册 |
| 55 | BrokerHeartbeat | Broker→Controller：心跳保活 |
| 56 | DescribeQuorum | KRaft Quorum 状态查询 |

### 4.5 Zero-Copy Fetch (Sendfile)

Consumer 消费数据时，利用 Linux `splice` 系统调用将磁盘 Log 直接管道传输到 Socket，无需拷贝到用户态：

```
Disk Log File ───splice()───► Socket Buffer ───► Network
     (内核态)                  (内核态)
```

```rust
/// 零拷贝 Fetch 实现
pub async fn zero_copy_fetch(
    segment: &LogSegment,
    offset: u64,
    max_bytes: u32,
    socket: &TcpStream,
) -> Result<usize> {
    let position = segment.index.lookup_position(offset)?;
    let file = segment.file.as_raw_fd();
    let sock = socket.as_raw_fd();

    // splice: 内核态直接搬运，零用户态拷贝
    let transferred = unsafe {
        libc::splice(file, &mut position as *mut _ as &mut i64, sock, std::ptr::null_mut(), max_bytes as usize, 0)
    };

    Ok(transferred as usize)
}
```

### 4.6 Backpressure 流控

```rust
pub struct FlowController {
    max_pending_bytes: u64,         // 默认 512MB
    max_request_size: u32,          // 默认 100MB
    max_connections: u32,           // 默认 100,000
    current_pending: AtomicU64,
}

impl FlowController {
    pub fn try_acquire(&self, bytes: u64) -> Result<(), BackpressureError> {
        let current = self.current_pending.load(Ordering::Relaxed);
        if current + bytes > self.max_pending_bytes {
            Err(BackpressureError::TooManyPendingBytes)
        } else {
            self.current_pending.fetch_add(bytes, Ordering::Relaxed);
            Ok(())
        }
    }

    pub fn release(&self, bytes: u64) {
        self.current_pending.fetch_sub(bytes, Ordering::Relaxed);
    }
}
```

---

## 5. 副本复制与高可用

### 5.1 Replica 模型

每个 Partition 维护一组 Replica，其中一个 Leader，其余为 Follower：

```
                Partition-0
                     │
         ┌───────────┼───────────┐
         ▼           ▼           ▼
      Leader     Follower    Follower
     Broker-1    Broker-2    Broker-3
         │           │           │
     LEO=1000    LEO=1000    LEO=980
         │
     HW = 980 (ISR 中最小的 LEO)
```

```rust
pub struct Replica {
    pub broker_id: BrokerId,
    pub partition_id: PartitionId,
    pub role: ReplicaRole,
    pub log_end_offset: u64,         // LEO: 当前副本已写入位置
    pub high_watermark: u64,         // HW: ISR 中所有副本确认的最小位置
}

pub enum ReplicaRole {
    Leader,    // 接收 Producer 写入，协调复制
    Follower,  // 从 Leader 拉取数据
    Observer,  // 数据同步中，不参与 ISR
}
```

### 5.2 HW / LEO 机制

- **LEO (Log End Offset)**：副本已写入的最后一条消息的 Offset
- **HW (High Watermark)**：ISR 中所有副本 LEO 的最小值

**关键约束**：Consumer 只能消费 `offset < HW` 的数据，保证已提交数据不会因 Leader 切换而丢失。

```rust
impl LeaderReplica {
    /// 更新 High Watermark
    pub fn update_hw(&mut self) {
        let min_leo = self.isr_members()
            .iter()
            .map(|r| r.log_end_offset)
            .min()
            .unwrap_or(0);
        self.high_watermark = min_leo;
    }

    /// ISR 管理：剔除超时 Follower
    pub fn shrink_isr(&mut self, replica_lag_time_max_ms: u64) {
        let now = current_time_ms();
        self.isr.retain(|broker_id| {
            let replica = self.replicas.get(broker_id).unwrap();
            now - replica.last_fetch_time < replica_lag_time_max_ms
        });
    }
}
```

### 5.3 ACK 语义

| acks 值 | 行为 | 保证 |
|:---|:---|:---|
| `0` | 发送即返回 | 无保证 |
| `1` | Leader 落盘即返回 | Leader 存活时不丢 |
| `all` | ISR 全部确认后才返回 | ISR 存活时不丢 |

### 5.4 Leader Election

```
Broker 崩溃
      │
      ▼
Controller 检测到 Heartbeat 超时
      │
      ▼
从 ISR 中选择新 Leader (Preferred Replica 优先)
      │
      ├── ISR 非空 → 从 ISR 中选 (数据一致)
      │
      └── ISR 为空 + unclean.election=true → 从所有 Replica 中选 (可能丢数据)
      │
      ▼
更新 Metadata Log (通过 Raft 同步到所有 Broker)
      │
      ▼
通知相关 Broker 切换角色
```

### 5.5 Partition Migration (扩容)

```
Step 1: 目标 Broker 创建空 Replica
Step 2: Follower Replica Fetch 追赶 Leader 历史数据
Step 3: 追平 HW 后加入 ISR
Step 4: (可选) Leader Transfer 到新 Broker
```

---

## 6. KRaft 控制器与元数据管理

完全摒弃 ZooKeeper，采用 Kafka KRaft 模式，基于 `openraft` 实现。

### 6.1 Controller Quorum 架构

```
┌─────────────────────────────────────────────────┐
│            Active Controller (Leader)            │
│  ┌──────────────┐     ┌──────────────────────┐  │
│  │  Controller   │────►│  Metadata Log        │  │
│  │  Engine       │     │  (__cluster_metadata) │  │
│  └──────────────┘     └──────────────────────┘  │
└─────────────────────┬───────────────────────────┘
                      │ Raft Replication (RPC)
                      ▼
┌─────────────────────────────────────────────────┐
│            Broker Node (Follower)                │
│  ┌──────────────┐     ┌──────────────────────┐  │
│  │ Metadata     │◄────│  Metadata Synchronizer│  │
│  │ Cache        │     │                      │  │
│  └──────────────┘     └──────────────────────┘  │
└─────────────────────────────────────────────────┘
```

### 6.2 Metadata 数据模型

```rust
pub struct MetadataState {
    pub brokers: HashMap<BrokerId, Broker>,
    pub topics: HashMap<String, Topic>,
    pub partitions: HashMap<PartitionId, PartitionMetadata>,
    pub acls: Vec<AclEntry>,
    pub configs: HashMap<String, String>,
}

pub struct Broker {
    pub id: BrokerId,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
    pub state: BrokerState,
}

pub enum BrokerState {
    Starting,
    Running,
    Offline,
    Dead,
}

pub struct Topic {
    pub name: String,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub configs: HashMap<String, String>,
}

pub struct PartitionMetadata {
    pub topic: String,
    pub partition_id: u32,
    pub replicas: Vec<BrokerId>,         // 所有 Replica 所在 Broker
    pub isr: Vec<BrokerId>,              // 当前 ISR 成员
    pub leader: BrokerId,                // 当前 Leader
}
```

### 6.3 Metadata 事件

所有元数据变更以事件形式写入 Metadata Log，通过 Raft 复制：

```rust
pub enum MetadataEvent {
    BrokerRegister { broker: Broker },
    BrokerRemove { broker_id: BrokerId },
    TopicCreate { topic: Topic },
    TopicDelete { topic_name: String },
    PartitionAssign { partition: PartitionMetadata },
    LeaderChange { partition_id: PartitionId, new_leader: BrokerId },
    IsrChange { partition_id: PartitionId, isr: Vec<BrokerId> },
    AclChange { acl: AclEntry },
    ConfigChange { key: String, value: String },
}
```

### 6.4 Cluster Bootstrap

首次启动流程：

```
Node-1 启动
      │
      ▼
生成 Cluster ID (UUID)
      │
      ▼
初始化 Metadata Log (空)
      │
      ▼
创建 Controller Quorum (单节点自举)
      │
      ▼
启动 Broker 服务，注册自身到 Metadata
      │
      ▼
其他节点加入，形成 Quorum
```

### 6.5 Replica Placement 策略

```rust
pub trait PlacementStrategy {
    fn assign_replicas(
        &self,
        partition_count: u32,
        replication_factor: u16,
        brokers: &[Broker],
    ) -> Vec<Vec<BrokerId>>;
}

/// Round-Robin + Rack Awareness
pub struct RackAwarePlacement;

impl PlacementStrategy for RackAwarePlacement {
    fn assign_replicas(&self, partition_count: u32, replication_factor: u16, brokers: &[Broker]) -> Vec<Vec<BrokerId>> {
        // 1. 按 Rack 分组 Broker
        // 2. 每个 Partition 的 Replica 分布在不同 Rack
        // 3. 第一个 Replica 轮询分配，保证 Leader 均匀分布
        // ...
    }
}
```

---

## 7. Consumer Group 设计

### 7.1 总体模型

```
┌─────────────────────────────────────────┐
│           Group Coordinator              │
│           (绑定到特定 Broker)             │
│                                         │
│  ┌────────────┐  ┌──────────────────┐  │
│  │ Member     │  │ Assignment       │  │
│  │ Manager    │  │ Engine           │  │
│  └────────────┘  └──────────────────┘  │
│  ┌────────────┐  ┌──────────────────┐  │
│  │ Heartbeat  │  │ Offset Manager   │  │
│  │ Monitor    │  │(__consumer_offsets)│ │
│  └────────────┘  └──────────────────┘  │
└─────────────────────────────────────────┘
         │                │
    Member-A           Member-B
    (orders-0,2)       (orders-1,3)
```

### 7.2 Group 状态机

```rust
pub enum GroupState {
    Empty,                  // 无成员
    PreparingRebalance,     // 等待所有成员 JoinGroup
    CompletingRebalance,    // Leader 正在执行分配
    Stable,                 // 正常运行
    Dead,                   // 组已销毁
}
```

状态转换：

```
Empty ──► PreparingRebalance ──► CompletingRebalance ──► Stable ──► Empty
  ▲                                    │                    │
  │                                    │ 成员离开/超时       │
  └────────────────────────────────────┴────────────────────┘
```

### 7.3 Coordinator 选择

```rust
/// 根据 group_id 确定 Group Coordinator 所在的 Broker
/// 1. 哈希 group_id → 定位 __consumer_offsets 的某个 Partition
/// 2. 查找该 Partition 的 Leader Broker → 即为 Coordinator
pub fn find_coordinator(
    group_id: &str,
    offsets_partition_count: u32,
    metadata: &MetadataState,
) -> Option<BrokerId> {
    let hash = crc32c(group_id.as_bytes());
    let partition = (hash % offsets_partition_count) as u32;

    // 查找 __consumer_offsets topic 的该 partition 的 leader
    let topic_name = "__consumer_offsets";
    metadata.partitions.values()
        .find(|p| p.topic == topic_name && p.partition_id == partition)
        .map(|p| p.leader)
}
```

### 7.4 Consumer Join 流程

```
Consumer 启动
      │
      ▼
FindCoordinator Request → 定位 Coordinator Broker
      │
      ▼
JoinGroup Request → Coordinator 注册成员
      │
      ▼
Coordinator 选举 Consumer Leader (第一个加入的成员)
      │
      ▼
Coordinator 等待所有成员 JoinGroup (session.timeout.ms)
      │
      ▼
Consumer Leader 执行 Partition Assignment
      │
      ▼
SyncGroup Request → 各成员获取自己的分配结果
      │
      ▼
进入 Stable 状态，开始消费
```

### 7.5 Partition Assignment 策略

```rust
pub trait PartitionAssignor: Send + Sync {
    fn assign(
        &self,
        members: &[Member],
        partitions: &[TopicPartition],
    ) -> HashMap<MemberId, Vec<TopicPartition>>;
}

/// Range Assignor — 连续分配，简单但可能倾斜
pub struct RangeAssignor;

/// RoundRobin Assignor — 轮询分配
pub struct RoundRobinAssignor;

/// CooperativeSticky Assignor — 推荐，减少 Rebalance 抖动
pub struct CooperativeStickyAssignor;
```

### 7.6 Offset 管理

Consumer Offset 存储在内部 Topic `__consumer_offsets`（与 Kafka 一致）：

```rust
pub struct OffsetCommit {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub metadata: String,           // 用户自定义元数据
    pub commit_timestamp: i64,
}
```

**Offset Recovery**：Broker 重启时从 `__consumer_offsets` CommitLog 回放，重建内存 Offset Cache。

### 7.7 Exactly Once Semantics (EOS) 基础

```rust
/// 幂等 Producer — 基于 ProducerId + Sequence Number
pub struct IdempotentProducerState {
    pub producer_id: ProducerId,     // 全局唯一 ID
    pub epoch: u16,                  // 代际号，Producer 重启时递增
    pub sequence: u32,               // 每个 Partition 的递增序号
}

/// Broker 端去重检查
pub struct DeduplicationCache {
    /// (ProducerId, PartitionId) → (Epoch, LastSequence)
    entries: HashMap<(ProducerId, PartitionId), (u16, u32)>,
}

impl DeduplicationCache {
    pub fn check_and_update(&mut self, pid: ProducerId, partition: PartitionId,
                            epoch: u16, seq: u32) -> Result<(), DuplicateError> {
        let key = (pid, partition);
        match self.entries.get(&key) {
            Some(&(stored_epoch, stored_seq)) if epoch == stored_epoch && seq <= stored_seq => {
                Err(DuplicateError::DuplicateSequence)
            }
            _ => {
                self.entries.insert(key, (epoch, seq));
                Ok(())
            }
        }
    }
}
```

---

## 8. 安全设计

### 8.1 安全架构总览

```
Client Connection
      │
      ▼
┌─────────────┐
│ TLS/mTLS    │ ← rustls (纯 Rust，无 OpenSSL 依赖)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ SASL Auth   │ ← PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ ACL Check   │ ← 基于 Principal + Resource + Operation
└──────┬──────┘
       │
       ▼
  Request Processing
```

### 8.2 TLS 设计

采用 `rustls` — 纯 Rust 实现，无 C 依赖，内存安全：

```rust
pub struct TlsConfig {
    pub enabled: bool,
    pub mode: TlsMode,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub ca_path: Option<PathBuf>,
}

pub enum TlsMode {
    OneWay,    // 仅服务端证书
    Mutual,    // 双向证书认证 (mTLS)
}
```

### 8.3 SASL 认证

```rust
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

/// SASL 认证流程
/// 1. Client → SaslHandshakeRequest { mechanism }
/// 2. Broker → SaslHandshakeResponse { enabled_mechanisms }
/// 3. Client → SaslAuthenticateRequest { auth_bytes }
/// 4. Broker → SaslAuthenticateResponse { auth_bytes, error }
```

### 8.4 ACL 授权模型

```rust
pub struct AclEntry {
    pub principal: String,          // 如 "User:order-service"
    pub resource: ResourcePattern,
    pub operation: Operation,
    pub permission: PermissionType,
}

pub enum ResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
}

pub enum Operation {
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
}

pub enum PermissionType {
    Allow,
    Deny,
}
```

**权限缓存**：使用 `DashMap` 缓存 ACL 查询结果，Metadata Log 变更时失效。

### 8.5 集群内部安全

- **Controller 间通信**：Raft RPC 强制 mTLS
- **Replica 间通信**：Leader ↔ Follower 复制链路 TLS 加密
- **Metadata Log**：防篡改，CRC 校验

---

## 9. 可观测性设计

### 9.1 Metrics (Prometheus)

```rust
/// 核心监控指标
pub struct BrokerMetrics {
    // 吞吐
    pub messages_in_total: Counter,
    pub messages_out_total: Counter,
    pub bytes_in_total: Counter,
    pub bytes_out_total: Counter,

    // 延迟
    pub append_latency: Histogram,       // 写入延迟
    pub fetch_latency: Histogram,        // 读取延迟
    pub flush_latency: Histogram,        // 刷盘延迟

    // 复制
    pub replication_lag: Gauge,          // Follower 落后 Leader 的消息数
    pub isr_shrink_total: Counter,
    pub isr_expand_total: Counter,

    // 存储
    pub disk_usage_bytes: Gauge,
    pub segment_count: Gauge,

    // 连接
    pub active_connections: Gauge,
    pub request_queue_size: Gauge,
}
```

### 9.2 Distributed Tracing (OpenTelemetry)

```
Produce Request Trace:
├── Decode (protocol codec)
├── Route (partition lookup)
├── Storage (append to commit log)
├── Replication (sync to followers)
└── ACK

Fetch Request Trace:
├── Decode
├── Index Lookup (mmap binary search)
├── Segment Read
└── Zero-Copy Transfer
```

### 9.3 日志

采用 `tracing` crate，结构化日志：

```rust
#[instrument(skip(self, batch))]
async fn append_batch(&self, batch: &RecordBatch) -> Result<u64> {
    tracing::debug!(
        partition = %self.partition_id,
        record_count = batch.record_count,
        batch_size = batch.batch_size,
        "appending record batch"
    );
    // ...
}
```

---

## 10. 工程结构

### 10.1 Workspace 组织

```
r-kafka/
├── Cargo.toml                    # Workspace root
├── Cargo.lock
│
├── crates/
│   ├── rk-core/                  # 公共类型、错误、配置
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── types.rs          # BrokerId, PartitionId, TopicName...
│   │   │   ├── error.rs          # 统一错误类型
│   │   │   └── config.rs         # 全局配置
│   │   └── Cargo.toml
│   │
│   ├── rk-network/               # 网络层
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── server.rs         # TcpListener, AcceptLoop (SO_REUSEPORT)
│   │   │   ├── connection.rs     # Connection, ConnectionState
│   │   │   ├── flow_control.rs   # Backpressure
│   │   │   ├── zero_copy.rs      # Zero-Copy Fetch (splice/sendfile)
│   │   │   └── http_server.rs    # Metrics HTTP 端点
│   │   └── Cargo.toml
│   │
│   ├── rk-protocol/              # Kafka 协议编解码 (Broker + Client 共用)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── codec.rs          # Encoder/Decoder traits
│   │   │   ├── types.rs          # Kafka 数据类型 (VARINT, COMPACT_STRING 等)
│   │   │   ├── request.rs        # Request Header + Body
│   │   │   ├── response.rs       # Response Header + Body
│   │   │   ├── api.rs            # API Key 定义
│   │   │   ├── api_versions.rs   # 版本协商 + SUPPORTED_API_VERSIONS
│   │   │   ├── record.rs         # Kafka RecordBatch v2 线上格式编解码 (wire format)
│   │   │   ├── legacy_message.rs # MessageSet v0/v1 向后兼容
│   │   │   ├── error_codes.rs    # Kafka 标准错误码
│   │   │   └── apis/             # 45 个 API 请求/响应实现
│   │   │       ├── produce.rs, fetch.rs, metadata.rs, ...
│   │   │       └── (每个 API Key 一个文件)
│   │   ├── tests/
│   │   │   └── protocol_fuzz.rs  # 协议 Fuzzing 测试
│   │   └── Cargo.toml
│   │
│   ├── rk-storage/               # 存储引擎
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── commitlog.rs      # CommitLog (append_batch, roll_segment, HW/LEO)
│   │   │   ├── segment.rs        # LogSegment (Active/Sealed/Deleted)
│   │   │   ├── index.rs          # OffsetIndex, TimeIndex (mmap, 二分查找)
│   │   │   ├── log_io.rs         # 磁盘级 RecordBatch 读写 (文件 I/O, mmap)
│   │   │   ├── recovery.rs       # Crash Recovery (CRC 验证 + 截断)
│   │   │   ├── retention.rs      # Log Retention (size + time)
│   │   │   ├── flush.rs          # Flush 策略 (async/sync/hybrid)
│   │   │   ├── uring_io.rs       # io_uring Direct I/O (Linux + 非 Linux 回退)
│   │   │   ├── compaction.rs     # Log Compaction (key_index, tombstone)
│   │   │   └── tiered_storage.rs # Tiered Storage (冷热分离, RemoteStorage trait)
│   │   ├── tests/
│   │   │   └── write_throughput.rs
│   │   └── Cargo.toml
│   │
│   ├── rk-broker/                # Broker 核心逻辑 (含 Consumer Group Coordinator)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── produce.rs        # Produce Handler
│   │   │   ├── fetch.rs          # Fetch Handler
│   │   │   ├── partition.rs      # Partition Manager
│   │   │   ├── router.rs         # BrokerRouter (API 分发, 1136 行)
│   │   │   ├── batch_accumulator.rs  # 攒批写入 (max 1MB / 5ms)
│   │   │   ├── group_manager.rs      # Consumer Group 状态机
│   │   │   ├── offset_manager.rs     # Offset 管理 (内存)
│   │   │   ├── persistent_offset_manager.rs  # Offset 持久化 (__consumer_offsets)
│   │   │   ├── producer_state_manager.rs   # 幂等 Producer 去重
│   │   │   ├── transaction_coordinator.rs  # Transaction Coordinator (EOS, 1018 行)
│   │   │   ├── connection_session.rs       # 连接会话管理
│   │   │   ├── config_reloader.rs          # 配置热加载 (SIGHUP)
│   │   │   ├── metrics.rs                  # Broker 运行时指标
│   │   │   ├── sasl_authenticator.rs       # SASL 认证集成
│   │   │   └── *_handler.rs        # 45 个 API Handler 文件
│   │   └── Cargo.toml
│   │
│   ├── rk-replication/           # 副本复制
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── replica.rs        # Replica, ReplicaRole (Leader/Follower/Observer)
│   │   │   ├── leader.rs         # LeaderReplica (ISR shrink, HW update)
│   │   │   ├── replica_fetcher.rs    # ReplicaFetcher (跨 Broker 复制)
│   │   │   ├── replica_manager.rs    # ReplicaManager (统一管理)
│   │   │   ├── isr.rs            # ISR Manager (扩缩容, Lag 检测)
│   │   │   ├── hw_manager.rs     # HW/LEO 管理
│   │   │   └── election.rs       # Leader Election
│   │   └── Cargo.toml
│   │
│   ├── rk-controller/            # KRaft Controller
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── raft_node.rs      # OpenRaft 集成
│   │   │   ├── metadata_sm.rs    # Metadata StateMachine
│   │   │   ├── metadata_record.rs    # Metadata Record 编解码 (1176 行)
│   │   │   ├── broker_reg.rs     # BrokerRegistry
│   │   │   ├── cluster_bootstrap.rs  # 集群引导
│   │   │   ├── partition_alloc.rs    # Partition 分配
│   │   │   ├── rack_awareness.rs     # Rack Awareness
│   │   │   ├── replica_placement.rs  # 副本放置策略
│   │   │   └── feature_manager.rs    # Feature Version 协商 (422 行)
│   │   └── Cargo.toml
│   │
│   ├── rk-security/              # 安全
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── tls.rs            # TLS/mTLS (rustls)
│   │   │   ├── sasl.rs           # SASL 认证 (PLAIN + SCRAM-SHA-256/512, 798 行)
│   │   │   ├── acl.rs            # ACL 授权 (Resource/Operation/Permission)
│   │   │   ├── audit.rs          # 审计日志
│   │   │   ├── auth_pipeline.rs  # 安全管道 (Auth Pipeline)
│   │   │   └── api_permissions.rs    # API 权限检查
│   │   └── Cargo.toml
│   │
│   ├── rk-observability/         # 可观测性
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── metrics.rs        # Prometheus metrics
│   │   │   └── tracing_setup.rs  # 结构化日志 (tracing)
│   │   └── Cargo.toml
│   │
│   ├── rk-client/                # Rust Client SDK (连接 Java Broker 或 R-Kafka Broker)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── config.rs         # ClientConfig (兼容 Kafka 配置项)
│   │   │   ├── producer.rs       # KafkaProducer (acks, partitioner, 387 行)
│   │   │   ├── consumer.rs       # KafkaConsumer (subscribe, poll, commit, 358 行)
│   │   │   ├── admin.rs          # KafkaAdmin (create/delete/list topic)
│   │   │   ├── metadata.rs       # MetadataCache
│   │   │   ├── connection.rs     # BrokerConnection / ConnectionPool
│   │   │   ├── assignor.rs       # Partition Assignor (range/roundrobin/cooperative-sticky)
│   │   │   └── error.rs          # ClientError
│   │   └── Cargo.toml
│   │
│   └── rk-server/                # 二进制入口
│       ├── src/
│       │   └── main.rs           # 配置加载、服务启动、优雅关闭 (367 行)
│       ├── tests/
│       │   ├── e2e.rs                        # 端到端测试
│       │   ├── broker_cluster_interop.rs     # Broker 集群互操作
│       │   ├── client_interop.rs             # Client 互操作
│       │   ├── cluster_integration.rs        # 集群集成
│       │   ├── compatibility_matrix.rs       # 兼容性矩阵 (1032 行)
│       │   ├── java_client_interop.rs        # Java Client 互操作
│       │   ├── protocol_fuzzing.rs           # 协议 Fuzzing
│       │   ├── security_interop.rs           # 安全互操作
│       │   └── benchmark.rs                  # 性能基准
│       └── Cargo.toml
│
├── config/
│   ├── r-kafka.toml              # 默认配置
│   └── examples/                 # 配置示例
│       ├── single-node.toml
│       ├── cluster-node1.toml
│       ├── production.toml
│       ├── tls-config.toml
│       └── sasl-acl-config.toml
│
└── tests/
    └── testdata/                 # 协议基准数据 (待补充)
```

### 10.2 核心依赖选型

| 用途 | Crate | 理由 |
|:---|:---|:---|
| 异步运行时 | `tokio` | 成熟生态，高并发连接管理 |
| 零拷贝字节 | `bytes` | Kafka 协议零拷贝解析核心 |
| 异步 IO | `io-uring` | 高性能磁盘读写 |
| 内存映射 | `memmap2` | 稀疏索引 mmap 查找 |
| Raft 共识 | `openraft` | 纯 Rust Raft 实现 |
| TLS | `rustls` | 纯 Rust，无 OpenSSL 依赖 |
| 序列化 | `serde` + `serde_json` | 配置和元数据序列化 |
| 并发容器 | `dashmap` | 无锁并发 HashMap |
| 通道 | `flume` | 高性能跨线程消息传递 |
| 错误处理 | `thiserror` + `anyhow` | 库错误 + 应用错误 |
| 日志 | `tracing` | 结构化异步日志 |
| 指标 | `prometheus` | Prometheus 指标导出 |
| CLI | `clap` | 命令行参数解析 |
| 配置 | `toml` | 配置文件解析 |
| CRC | `crc32c` | 硬件加速 CRC32C 校验 |

### 10.3 配置示例

```toml
[broker]
id = 1
host = "0.0.0.0"
port = 9092
rack = "us-east-1a"

[storage]
data_dir = "/data/r-kafka"
segment_max_size = 1073741824      # 1GB
segment_max_time_ms = 1800000      # 30min
flush_interval_ms = 5
flush_mode = "hybrid"              # async | sync | hybrid

[retention]
max_bytes = 1099511627776          # 1TB
max_ms = 604800000                 # 7 days
compaction_enabled = true

[replication]
default_replication_factor = 3
min_isr_size = 2
replica_lag_time_max_ms = 10000
unclean_leader_election = false

[network]
max_connections = 100000
max_request_size = 104857600       # 100MB
max_pending_bytes = 536870912      # 512MB

[security]
tls_enabled = true
tls_mode = "mutual"                # one-way | mutual
cert_path = "/etc/r-kafka/certs/server.crt"
key_path = "/etc/r-kafka/certs/server.key"
ca_path = "/etc/r-kafka/certs/ca.crt"
sasl_enabled = true
sasl_mechanisms = ["SCRAM-SHA-256", "SCRAM-SHA-512"]

[controller]
quorum_peers = ["node1:9093", "node2:9093", "node3:9093"]
election_timeout_ms = 3000

[observability]
metrics_enabled = true
metrics_port = 9090
tracing_enabled = true
```

---

## 11. 实施路线

### Phase 1: 单机内核 (MVP)

**目标**：单 Broker 可运行，支持基本 Kafka 协议，通过 Java Client 基础验证。

- [ ] `rk-core` 公共类型与错误模型
- [ ] `rk-protocol` Produce/Fetch/Metadata/ListOffsets/ApiVersions 编解码
- [ ] `rk-protocol` Kafka 数据类型精确实现 (VARINT, COMPACT_STRING, tagged_fields)
- [ ] `rk-protocol` MessageSet v0/v1 兼容解码 (旧客户端自动转 v2)
- [ ] `rk-protocol` 协议 Fuzzing 基准：用 Java Client 抓包数据验证解码
- [ ] `rk-network` TCP 监听、连接管理、零拷贝解码
- [ ] `rk-storage` CommitLog、Segment、Index、Recovery
- [ ] `rk-storage` RecordBatch v2 格式与 Kafka 完全一致 (含完整 attributes 位定义)
- [ ] `rk-broker` Produce Handler、Fetch Handler
- [ ] 基础配置文件加载
- [ ] **互操作验证**：Java KafkaProducer 写入 → R-Kafka Broker → Java KafkaConsumer 读取
- [ ] 基准测试：单 Partition 写入吞吐 ≥ Kafka 2x

### Phase 2: 性能优化

**目标**：达到极致单机性能。

- [ ] io_uring Direct I/O 集成
- [ ] Batch Accumulator 攒批优化
- [ ] Zero-Copy Fetch (splice/sendfile)
- [ ] Thread-per-Core 绑定优化
- [ ] CRC32C 硬件加速
- [ ] mmap 索引优化

### Phase 3: 分布式

**目标**：多 Broker 集群，高可用。

- [ ] `rk-controller` KRaft Controller (OpenRaft 日志复制 + Kafka 自定义选举协议)
- [ ] `rk-controller` KRaft Metadata Record 编解码 (与 Kafka 格式一致)
- [ ] `rk-controller` Vote (API 51) / BeginQuorumEpoch (API 52) / EndQuorumEpoch (API 53)
- [ ] `rk-replication` Leader/Follower 复制、ISR、HW/LEO
- [ ] `rk-replication` Inter-Broker Protocol: LeaderAndIsr / StopReplica / UpdateMetadata
- [ ] `rk-replication` OffsetForLeaderEpoch (API 23) — 副本截断
- [ ] `rk-replication` BrokerRegistration (API 54) / BrokerHeartbeat (API 55)
- [ ] Leader Election、Partition Migration、ControlledShutdown、ElectLeaders (API 43)
- [ ] `rk-broker` + `rk-client` Consumer Group、Rebalance、Offset 管理 (Coordinator 在 rk-broker，客户端在 rk-client)
- [ ] `rk-broker` DescribeGroups (API 15) / ListGroups (API 16)
- [ ] Cluster Bootstrap、Broker Registration
- [ ] `rk-client` Rust Client SDK 基础版：Producer + Consumer + Metadata
- [ ] **互操作验证**：Rust Client 连接 Java Kafka Broker 读写数据
- [ ] **互操作验证**：Java Consumer + Rust Consumer 混合消费组 Rebalance
- [ ] 多节点集成测试

### Phase 4: 安全与企业能力

**目标**：生产可用。

- [ ] `rk-security` TLS/mTLS (rustls)
- [ ] SASL SCRAM 认证
- [ ] ACL 授权
- [ ] 审计日志
- [ ] Prometheus Metrics 导出
- [ ] OpenTelemetry Tracing
- [ ] Log Compaction
- [ ] 幂等 Producer (Idempotent) — InitProducerId (API 22)
- [ ] DescribeConfigs (API 32) / AlterConfigs (API 33)
- [ ] DeleteRecords (API 21)
- [ ] **互操作验证**：Java Client + TLS/SASL 连接 R-Kafka Broker
- [ ] **互操作验证**：Rust Client + TLS/SASL 连接 Java Kafka Broker

### Phase 5: 生态完善

**目标**：完整产品能力。

- [ ] Transaction Coordinator (EOS)
- [ ] Admin API (CreateTopics, DeleteTopics, CreatePartitions, ...)
- [ ] Rack Awareness Replica Placement
- [ ] Tiered Storage (冷热分离)
- [ ] Metadata 响应 v12+ NodeEndpoints 格式支持
- [ ] 生产环境部署文档

### Phase 6: 互操作认证

**目标**：通过 Kafka 生态互操作验证。

- [ ] `rk-client` Rust Client SDK 完成，可连接 Java Kafka Broker
- [ ] 协议 Fuzzing 测试套件：覆盖所有 API 的 legacy + flexible 版本
- [ ] Java Producer/Consumer 官方 SDK 集成测试（2.4+ / 3.x / 4.x）
- [ ] Go sarama/franz-go 客户端互操作测试
- [ ] Python confluent-kafka 客户端互操作测试
- [ ] Consumer Group Rebalance 互操作测试（Java + Rust 混合消费组）
- [ ] SASL/SCRAM 认证互操作测试
- [ ] **Broker 集群互操作**：R-Kafka Broker 加入 Java Kafka 集群
- [ ] **Broker 集群互操作**：跨实现副本复制（Java Leader ↔ R-Kafka Follower）
- [ ] **Broker 集群互操作**：KRaft Controller Quorum 混合运行
- [ ] **Broker 集群互操作**：Metadata Record 二进制格式兼容验证
- [ ] **Broker 集群互操作**：完整滚动迁移测试（Java → R-Kafka）
- [ ] **Broker 集群互操作**：回滚测试（R-Kafka → Java）
- [ ] Consumer Protocol V2 (KIP-848/853) 基础支持评估
- [ ] 发布 Kafka 兼容性矩阵文档

---

## 12. Kafka 互操作与协议兼容设计

互操作是 R-Kafka 生存的前提。必须实现 **三个维度** 的兼容：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          互操作矩阵                                      │
│                                                                         │
│  ── Client ↔ Broker ──────────────────────────────────────────────────  │
│   Java Kafka Client ──────► R-Kafka Broker          (Broker 端兼容)     │
│   Go   Kafka Client ──────► R-Kafka Broker          (Broker 端兼容)     │
│   Python Kafka Client ────► R-Kafka Broker          (Broker 端兼容)     │
│   C++  Kafka Client ──────► R-Kafka Broker          (Broker 端兼容)     │
│   R-Kafka Rust Client ────► Java Kafka Broker       (Client 端兼容)     │
│   R-Kafka Rust Client ────► R-Kafka Broker          (原生闭环)          │
│                                                                         │
│  ── Broker ↔ Broker 集群 ─────────────────────────────────────────────  │
│   R-Kafka Broker ◄──────► Java Kafka Broker        (混合集群 / 滚动迁移) │
│   R-Kafka Controller ◄──► Java Kafka Controller    (KRaft Quorum 混合)  │
│   R-Kafka Follower ◄────► Java Kafka Leader        (跨实现副本复制)      │
│   R-Kafka Leader ◄──────► Java Kafka Follower      (跨实现副本复制)      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 12.1 Kafka Wire Protocol 精确兼容

Kafka 协议是一套 **二进制长度前缀编码**，所有字段类型必须精确匹配。

#### 12.1.1 协议帧格式

```
┌──────────────────────────────────────────────────────────────┐
│ Request Size (i32) │ Request Header │ Request Body          │
└──────────────────────────────────────────────────────────────┘

Request Header (所有版本通用):
┌──────────────────────────────────────────────────────────────┐
│ api_key (i16) │ api_version (i16) │ correlation_id (i32)    │
├──────────────────────────────────────────────────────────────┤
│ [v1+] client_id (COMPACT_STRING | STRING)                   │
├──────────────────────────────────────────────────────────────┤
│ [v9+] tagged_fields (compact array of tags)  // flexible版   │
└──────────────────────────────────────────────────────────────┘
```

#### 12.1.2 Kafka 数据类型映射

Kafka 协议定义了一套专有数据类型，Rust 必须逐一精确实现：

| Kafka 类型 | 字节数 | Rust 实现 | 说明 |
|:---|:---|:---|:---|
| `INT8` | 1 | `i8` | 有符号 |
| `INT16` | 2 | `i16` | Big-endian |
| `INT32` | 4 | `i32` | Big-endian |
| `INT64` | 8 | `i64` | Big-endian |
| `UINT32` | 4 | `u32` | 无符号 |
| `VARINT` | 1-5 | `i32` (zigzag) | 变长编码 |
| `VARLONG` | 1-10 | `i64` (zigzag) | 变长编码 |
| `STRING` | 2+N | `String` | i16 长度前缀 + UTF-8 |
| `COMPACT_STRING` | varuint+N | `String` | varuint 长度 + 1 + UTF-8 |
| `NULLABLE_STRING` | 2+N | `Option<String>` | -1 表示 null |
| `COMPACT_NULLABLE_STRING` | varuint+N | `Option<String>` | 0 表示 null |
| `BYTES` | 4+N | `Bytes` | i32 长度前缀 |
| `COMPACT_BYTES` | varuint+N | `Bytes` | varuint 长度 + 1 |
| `ARRAY` | 4+N | `Vec<T>` | i32 长度前缀，-1 为 null |
| `COMPACT_ARRAY` | varuint+N | `Vec<T>` | varuint 长度 + 1，0 为 null |
| `BOOLEAN` | 1 | `bool` | 0=false, 1=true |
| `TAGGED_FIELDS` | variable | `TaggedFields` | flexible 版本新增 |

#### 12.1.3 编解码实现

```rust
/// Kafka 协议读取器 — 从 BytesMut 中按 Kafka 类型依次读取
pub struct KafkaReader<'a> {
    buf: &'a BytesMut,
    pos: usize,
}

impl<'a> KafkaReader<'a> {
    pub fn read_i8(&mut self) -> Result<i8> { /* ... */ }
    pub fn read_i16(&mut self) -> Result<i16> { /* big-endian */ }
    pub fn read_i32(&mut self) -> Result<i32> { /* big-endian */ }
    pub fn read_i64(&mut self) -> Result<i64> { /* big-endian */ }
    pub fn read_u32(&mut self) -> Result<u32> { /* big-endian */ }

    /// VARINT — zigzag 解码
    pub fn read_varint(&mut self) -> Result<i32> {
        let raw = self.read_varuint()?;
        Ok(((raw >> 1) as i32) ^ (-((raw & 1) as i32)))
    }

    /// VARLONG — zigzag 解码
    pub fn read_varlong(&mut self) -> Result<i64> {
        let raw = self.read_varuint64()?;
        Ok(((raw >> 1) as i64) ^ (-((raw & 1) as i64)))
    }

    /// STRING — i16 长度前缀 + UTF-8
    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_i16()?;
        if len < 0 { return Err(ProtocolError::NullString); }
        self.read_utf8(len as usize)
    }

    /// COMPACT_STRING — varuint(length+1) + UTF-8
    pub fn read_compact_string(&mut self) -> Result<String> {
        let len = self.read_varuint()?;
        if len == 0 { return Err(ProtocolError::NullString); }
        self.read_utf8((len - 1) as usize)
    }

    /// NULLABLE_STRING — i16 长度前缀，-1 表示 null
    pub fn read_nullable_string(&mut self) -> Result<Option<String>> {
        let len = self.read_i16()?;
        if len < 0 { return Ok(None); }
        Ok(Some(self.read_utf8(len as usize)?))
    }

    /// BYTES — i32 长度前缀，返回零拷贝 Bytes
    pub fn read_bytes(&mut self) -> Result<Bytes> {
        let len = self.read_i32()?;
        if len < 0 { return Err(ProtocolError::NullBytes); }
        self.read_bytes_ref(len as usize)
    }
}

/// Kafka 协议写入器
pub struct KafkaWriter {
    buf: BytesMut,
}

impl KafkaWriter {
    pub fn write_i16(&mut self, v: i16) { /* big-endian */ }
    pub fn write_i32(&mut self, v: i32) { /* big-endian */ }
    pub fn write_varint(&mut self, v: i32) { /* zigzag + varuint */ }
    pub fn write_compact_string(&mut self, s: &str) { /* varuint(len+1) + utf8 */ }
    pub fn write_compact_bytes_array(&mut self, items: &[Bytes]) { /* ... */ }
}
```

#### 12.1.4 Flexible vs Legacy 版本

Kafka 从 2.4 开始引入 **Flexible Version**（tagged fields），同一 API 的 legacy 版和 flexible 版头部格式不同：

```rust
/// 根据 api_version 判断是否为 flexible 版本
pub fn is_flexible_version(api_key: i16, api_version: i16) -> bool {
    match api_key {
        // Produce: v9+ 是 flexible
        0 => api_version >= 9,
        // Fetch: v12+ 是 flexible
        1 => api_version >= 12,
        // Metadata: v9+ 是 flexible
        3 => api_version >= 9,
        // ... 每个 API 的 flexible 起始版本不同
        _ => false,
    }
}

/// Request Header — 自动适配 flexible/legacy
pub fn decode_request_header(reader: &mut KafkaReader) -> Result<RequestHeader> {
    let api_key = reader.read_i16()?;
    let api_version = reader.read_i16()?;
    let correlation_id = reader.read_i32()?;

    let client_id = if is_flexible_version(api_key, api_version) {
        // flexible: COMPACT_NULLABLE_STRING
        reader.read_compact_nullable_string()?
    } else {
        // legacy: NULLABLE_STRING
        reader.read_nullable_string()?.map(|s| s)
    };

    let tagged_fields = if is_flexible_version(api_key, api_version) {
        Some(reader.read_tagged_fields()?)
    } else {
        None
    };

    Ok(RequestHeader { api_key, api_version, correlation_id, client_id, tagged_fields })
}
```

### 12.2 RecordBatch 格式兼容

R-Kafka 的磁盘格式 **必须与 Kafka 的 RecordBatch v2 完全一致**，否则无法读取/写入 Kafka 生态工具产生的数据。

#### 12.2.1 Kafka RecordBatch 二进制布局

```
┌──────────────────────────────────────────────────────────────────────┐
│ base_offset (i64)          │ 8 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ batch_length (i32)         │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ partition_leader_epoch     │ 4 bytes (i32)                           │
├────────────────────────────┼─────────────────────────────────────────┤
│ magic (i8)                 │ 1 byte  (必须 = 2)                      │
├────────────────────────────┼─────────────────────────────────────────┤
│ crc (i32)                  │ 4 bytes  CRC32C(magic..records)         │
├────────────────────────────┼─────────────────────────────────────────┤
│ attributes (i16)           │ 2 bytes  压缩类型等                      │
├────────────────────────────┼─────────────────────────────────────────┤
│ last_offset_delta (i32)    │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ base_timestamp (i64)       │ 8 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ max_timestamp (i64)        │ 8 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ producer_id (i64)          │ 8 bytes  幂等 Producer ID               │
├────────────────────────────┼─────────────────────────────────────────┤
│ producer_epoch (i16)       │ 2 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ base_sequence (i32)        │ 4 bytes  幂等起始序号                    │
├────────────────────────────┼─────────────────────────────────────────┤
│ record_count (i32)         │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ records ...                │ variable                                │
└────────────────────────────┴─────────────────────────────────────────┘
```

#### 12.2.2 单条 Record 编码

```
┌──────────────────────────────────────────────────────────────────────┐
│ length (varint)            │ 本条 record 字节数                       │
├────────────────────────────┼─────────────────────────────────────────┤
│ attributes (i8)            │ 1 byte  (当前保留=0)                     │
├────────────────────────────┼─────────────────────────────────────────┤
│ timestamp_delta (varlong)  │ 相对于 base_timestamp                    │
├────────────────────────────┼─────────────────────────────────────────┤
│ offset_delta (varint)      │ 相对于 base_offset                       │
├────────────────────────────┼─────────────────────────────────────────┤
│ key_len (varint)           │ -1 = null                               │
├────────────────────────────┼─────────────────────────────────────────┤
│ key (bytes)                │ key_len bytes                           │
├────────────────────────────┼─────────────────────────────────────────┤
│ value_len (varint)         │ -1 = null (tombstone)                   │
├────────────────────────────┼─────────────────────────────────────────┤
│ value (bytes)              │ value_len bytes                         │
├────────────────────────────┼─────────────────────────────────────────┤
│ header_count (varint)      │ header 数量                             │
├────────────────────────────┼─────────────────────────────────────────┤
│ [headers...]               │ key_len(varint)+key+value_len(varint)+v │
└────────────────────────────┴─────────────────────────────────────────┘
```

#### 12.2.3 Rust 实现 — 精确匹配 Kafka 格式

```rust
/// Kafka RecordBatch — 磁盘格式与 Kafka 完全一致
#[repr(C)]
pub struct KafkaRecordBatchHeader {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,                     // 必须 = 2
    pub crc: i32,                      // CRC32C
    pub attributes: i16,               // bit 0-2: compression
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,              // 幂等: -1 = 未启用
    pub producer_epoch: i16,
    pub base_sequence: i32,            // 幂等: -1 = 未启用
    pub record_count: i32,
}

/// 压缩类型 — attributes 低 3 位
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum KafkaCompression {
    None   = 0,
    Gzip   = 1,
    Snappy = 2,
    Lz4    = 3,
    Zstd   = 4,
}

impl KafkaRecordBatchHeader {
    pub fn compression(&self) -> KafkaCompression {
        match self.attributes & 0x07 {
            0 => KafkaCompression::None,
            1 => KafkaCompression::Gzip,
            2 => KafkaCompression::Snappy,
            3 => KafkaCompression::Lz4,
            4 => KafkaCompression::Zstd,
            _ => KafkaCompression::None,
        }
    }

    /// 验证 CRC32C — 覆盖 magic..records 全部字节
    /// batch_bytes 为从 base_offset 开始的完整 batch 字节
    /// CRC 从 magic 字段开始 (offset 16 = 8+4+4)
    pub fn verify_crc(&self, batch_bytes: &[u8]) -> bool {
        let computed = crc32c::crc32c(&batch_bytes[16..]); // 从 magic 开始
        computed == self.crc as u32
    }
}

/// 单条 Kafka Record — 完全匹配 Kafka 编码
pub struct KafkaRecord {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(Bytes, Bytes)>,   // (key, value) 可为空
}
```

### 12.3 Kafka 错误码兼容

R-Kafka 必须返回与 Kafka 完全一致的错误码，否则客户端行为异常：

```rust
/// Kafka 标准错误码 — 值必须与 Kafka 一致
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum KafkaErrorCode {
    UnknownServerError            = -1,
    None                          = 0,
    OffsetOutOfRange              = 1,
    CorruptMessage                = 2,
    UnknownTopicOrPartition       = 3,
    InvalidFetchSize              = 4,
    LeaderNotAvailable            = 5,
    NotLeaderOrFollower           = 6,
    RequestTimedOut               = 7,
    BrokerNotAvailable            = 8,
    ReplicaNotAvailable           = 9,
    MessageTooLarge               = 10,
    StaleControllerEpoch          = 11,
    OffsetMetadataTooLarge        = 12,
    GroupLoadInProgress           = 14,
    GroupCoordinatorNotAvailable  = 15,
    NotCoordinator                = 16,
    InvalidTopic                  = 17,
    RecordListTooLarge            = 18,
    NotEnoughReplicas             = 19,
    NotEnoughReplicasAfterAppend  = 20,
    InvalidRequiredAcks           = 21,
    IllegalGeneration             = 22,
    InconsistentGroupProtocol     = 23,
    InvalidGroupId                = 24,
    UnknownMemberId               = 25,
    InvalidSessionTimeout         = 26,
    RebalanceInProgress           = 27,
    InvalidCommitOffsetSize       = 28,
    TopicAuthorizationFailed      = 29,
    GroupAuthorizationFailed      = 30,
    ClusterAuthorizationFailed    = 31,
    InvalidTimestamp              = 32,
    UnsupportedSaslMechanism      = 33,
    IllegalSaslState              = 34,
    UnsupportedVersion            = 35,
    // ... 完整列表需覆盖 Kafka 所有错误码
    DuplicateSequenceNumber       = 46,
    InvalidProducerEpoch          = 47,
    InvalidTxnState               = 52,
    InvalidProducerIdMapping      = 53,
}
```

### 12.4 ApiVersions 协商机制

客户端连接后首先发送 `ApiVersionsRequest`，Broker 返回支持的 API 及版本范围：

```rust
/// ApiVersions 响应 — 告诉客户端本 Broker 支持哪些 API 版本
pub struct ApiVersionsResponse {
    pub error_code: KafkaErrorCode,
    pub api_versions: Vec<ApiVersion>,
    // v1+ 增加:
    pub throttle_time_ms: i32,
}

pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// R-Kafka 当前支持的版本范围 (对齐 Kafka 3.7+)
///
/// 注意: Router (BrokerRouter) 实际处理全部 45 个 API (含 Broker-Control APIs 4-7 和 KRaft APIs 54-55)，
/// 但 SUPPORTED_API_VERSIONS 仅声明客户端需要的 API 版本范围。
pub const SUPPORTED_API_VERSIONS: &[ApiVersion] = &[
    // Produce
    ApiVersion { api_key: 0,  min_version: 0, max_version: 10 },
    // Fetch
    ApiVersion { api_key: 1,  min_version: 0, max_version: 16 },
    // ListOffsets
    ApiVersion { api_key: 2,  min_version: 0, max_version: 8  },
    // Metadata
    ApiVersion { api_key: 3,  min_version: 0, max_version: 13 },
    // OffsetCommit
    ApiVersion { api_key: 8,  min_version: 0, max_version: 9  },
    // OffsetFetch
    ApiVersion { api_key: 9,  min_version: 0, max_version: 9  },
    // FindCoordinator
    ApiVersion { api_key: 10, min_version: 0, max_version: 4  },
    // JoinGroup
    ApiVersion { api_key: 11, min_version: 0, max_version: 9  },
    // Heartbeat
    ApiVersion { api_key: 12, min_version: 0, max_version: 4  },
    // LeaveGroup
    ApiVersion { api_key: 13, min_version: 0, max_version: 5  },
    // SyncGroup
    ApiVersion { api_key: 14, min_version: 0, max_version: 5  },
    // DescribeGroups
    ApiVersion { api_key: 15, min_version: 0, max_version: 5  },
    // ListGroups
    ApiVersion { api_key: 16, min_version: 0, max_version: 4  },
    // SaslHandshake
    ApiVersion { api_key: 17, min_version: 0, max_version: 1  },
    // ApiVersions 自身
    ApiVersion { api_key: 18, min_version: 0, max_version: 3  },
    // CreateTopics
    ApiVersion { api_key: 19, min_version: 0, max_version: 3  },
    // DeleteTopics
    ApiVersion { api_key: 20, min_version: 0, max_version: 6  },
    // DeleteRecords
    ApiVersion { api_key: 21, min_version: 0, max_version: 3  },
    // InitProducerId
    ApiVersion { api_key: 22, min_version: 0, max_version: 4  },
    // OffsetForLeaderEpoch
    ApiVersion { api_key: 23, min_version: 0, max_version: 4  },
    // AddPartitionsToTxn
    ApiVersion { api_key: 24, min_version: 0, max_version: 3  },
    // EndTxn
    ApiVersion { api_key: 26, min_version: 0, max_version: 3  },
    // DescribeConfigs
    ApiVersion { api_key: 32, min_version: 0, max_version: 4  },
    // AlterConfigs
    ApiVersion { api_key: 33, min_version: 0, max_version: 2  },
    // SaslAuthenticate
    ApiVersion { api_key: 36, min_version: 0, max_version: 2  },
    // CreatePartitions
    ApiVersion { api_key: 37, min_version: 0, max_version: 3  },
    // ElectLeaders
    ApiVersion { api_key: 43, min_version: 0, max_version: 2  },
    // IncrementalAlterConfigs
    ApiVersion { api_key: 44, min_version: 0, max_version: 0  },
    // AlterPartitionReassignments
    ApiVersion { api_key: 45, min_version: 0, max_version: 0  },
    // ListPartitionReassignments
    ApiVersion { api_key: 46, min_version: 0, max_version: 0  },
    // OffsetDelete
    ApiVersion { api_key: 47, min_version: 0, max_version: 0  },
    // KRaft APIs
    ApiVersion { api_key: 51, min_version: 0, max_version: 0  },  // Vote
    ApiVersion { api_key: 52, min_version: 0, max_version: 0  },  // BeginQuorumEpoch
    ApiVersion { api_key: 53, min_version: 0, max_version: 0  },  // EndQuorumEpoch
    ApiVersion { api_key: 56, min_version: 0, max_version: 0  },  // DescribeQuorum
    // 扩展 APIs
    ApiVersion { api_key: 60, min_version: 0, max_version: 0  },  // DescribeCluster
    ApiVersion { api_key: 61, min_version: 0, max_version: 0  },  // DescribeProducers
    ApiVersion { api_key: 65, min_version: 0, max_version: 0  },  // ListTransactions
    ApiVersion { api_key: 70, min_version: 0, max_version: 0  },  // DescribeTopics
];
///
/// 注意: BrokerRouter 还处理以下 Broker-Control / KRaft APIs (不在 SUPPORTED_API_VERSIONS 中):
///   - LeaderAndIsr (4), StopReplica (5), UpdateMetadata (6), ControlledShutdown (7)
///   - BrokerRegistration (54), BrokerHeartbeat (55)
```

### 12.5 Consumer Group 协议兼容

Consumer Group 协议是最复杂的兼容点。必须精确实现 Kafka 的 Group Protocol：

#### 12.5.1 支持的 Partition Assignment 协议

```rust
/// Kafka Consumer Group 协议名称 — 必须与 Kafka 客户端期望的一致
pub const RANGE_ASSIGNOR: &str = "range";
pub const ROUNDROBIN_ASSIGNOR: &str = "roundrobin";
pub const COOPERATIVE_STICKY_ASSIGNOR: &str = "cooperative-sticky";

/// JoinGroup 请求中，客户端上报支持的协议
/// Broker 选择一个所有成员都支持的协议
pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,     // v1+
    pub member_id: String,             // 空=新成员
    pub group_instance_id: Option<String>, // v5+ static membership
    pub protocol_type: String,         // "consumer"
    pub protocols: Vec<ProtocolMetadata>,  // 客户端支持的 assignment 策略
}

/// 客户端上报的协议元数据 — 二进制编码
pub struct ConsumerProtocolMetadata {
    pub version: i16,
    pub subscriptions: Vec<String>,    // 订阅的 topic 列表
    pub userdata: Option<Bytes>,       // 用户自定义数据
}
```

#### 12.5.2 SyncGroup 分配结果编码

Consumer Leader 计算的分配结果必须用 Kafka 标准格式编码：

```rust
/// SyncGroup 响应中每个 member 的分配结果
/// 值部分是 ConsumerGroupProtocol 编码的 Assignment
pub struct ConsumerGroupAssignment {
    pub version: i16,                  // 必须 = 1
    pub assigned_partitions: Vec<(String, Vec<i32>)>,  // (topic, partitions)
    pub userdata: Option<Bytes>,
}

impl ConsumerGroupAssignment {
    /// 编码为 Kafka 协议字节 — 必须与 Java Client 解码格式一致
    pub fn encode(&self, writer: &mut KafkaWriter) -> Result<()> {
        writer.write_i16(self.version)?;
        writer.write_i32(self.assigned_partitions.len() as i32)?;
        for (topic, partitions) in &self.assigned_partitions {
            writer.write_string(topic)?;
            writer.write_i32(partitions.len() as i32)?;
            for &p in partitions {
                writer.write_i32(p)?;
            }
        }
        match &self.userdata {
            Some(data) => writer.write_bytes(data)?,
            None => writer.write_i32(-1)?,
        }
        Ok(())
    }
}
```

### 12.6 R-Kafka Rust Client SDK 设计

R-Kafka 提供原生 Rust Client，可连接 **Java Kafka Broker** 或 **R-Kafka Broker**。

#### 12.6.1 模块结构

```
rk-client/
├── src/
│   ├── lib.rs
│   ├── config.rs          # ClientConfig
│   ├── producer.rs        # KafkaProducer
│   ├── consumer.rs        # KafkaConsumer
│   ├── admin.rs           # KafkaAdmin
│   ├── metadata.rs        # Metadata 缓存
│   ├── connection.rs      # BrokerConnection (复用 rk-protocol)
│   └── error.rs           # ClientError
└── Cargo.toml
```

#### 12.6.2 核心 API

```rust
/// 客户端配置 — 兼容 Kafka 配置项命名
pub struct ClientConfig {
    pub bootstrap_servers: Vec<String>,     // "broker1:9092,broker2:9092"
    pub client_id: Option<String>,
    pub security_protocol: SecurityProtocol, // PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
    pub sasl_mechanism: Option<SaslMechanism>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    // ... 更多 Kafka 标准配置项
}

/// Producer — 与 Kafka Producer API 对齐
pub struct KafkaProducer {
    config: ProducerConfig,
    metadata_cache: MetadataCache,
    connections: BrokerConnectionPool,
    accumulator: RecordAccumulator,       // 攒批
}

impl KafkaProducer {
    pub async fn send(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        // 1. 获取 topic metadata (缓存或请求 Broker)
        // 2. 选择 partition (key hash 或自定义 partitioner)
        // 3. 加入 accumulator 攒批
        // 4. 批次满或超时后发送到对应 partition 的 leader broker
        // 5. 等待 ACK (acks=0/1/all)
        todo!()
    }
}

pub struct ProducerRecord {
    pub topic: String,
    pub partition: Option<i32>,           // None = 自动选择
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Bytes)>,
    pub timestamp: Option<i64>,
}

/// Consumer — 与 Kafka Consumer API 对齐
pub struct KafkaConsumer {
    config: ConsumerConfig,
    metadata_cache: MetadataCache,
    connections: BrokerConnectionPool,
    subscriptions: Subscriptions,
    group_coordinator: Option<GroupCoordinator>,
    fetcher: Fetcher,
}

impl KafkaConsumer {
    pub async fn subscribe(&mut self, topics: &[&str]) -> Result<()> { todo!() }
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>> { todo!() }
    pub async fn commit_async(&self) -> Result<()> { todo!() }
    pub async fn seek(&mut self, tp: TopicPartition, offset: u64) -> Result<()> { todo!() }
}

pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Bytes)>,
}

/// Admin Client — Topic 管理
pub struct KafkaAdmin {
    config: ClientConfig,
    connections: BrokerConnectionPool,
}

impl KafkaAdmin {
    pub async fn create_topic(&self, name: &str, partitions: i32, replication: i16) -> Result<()> { todo!() }
    pub async fn delete_topic(&self, name: &str) -> Result<()> { todo!() }
    pub async fn list_topics(&self) -> Result<Vec<TopicMetadata>> { todo!() }
    pub async fn describe_cluster(&self) -> Result<ClusterDescription> { todo!() }
}
```

#### 12.6.3 Client 连接 Java Broker 流程

```
R-Kafka Rust Client                    Java Kafka Broker
         │                                      │
         │──── ApiVersionsRequest ──────────────►│
         │◄─── ApiVersionsResponse ─────────────│
         │     (返回支持的 API 版本范围)           │
         │                                      │
         │──── MetadataRequest ─────────────────►│
         │◄─── MetadataResponse ────────────────│
         │     (返回 broker 列表、topic/partition │
         │      leader 分布)                      │
         │                                      │
         │──── ProduceRequest ──────────────────►│  (发送到 Leader Broker)
         │◄─── ProduceResponse ─────────────────│  (acks=0/1/all)
         │                                      │
         │──── FetchRequest ────────────────────►│  (从 Leader Broker 消费)
         │◄─── FetchResponse ───────────────────│  (RecordBatch 数据)
         │                                      │
```

**关键点**：Client 使用与 rk-protocol 完全相同的编解码器，只是方向相反（Client 是请求方，Broker 是响应方）。

```rust
/// Client 端连接 — 复用 rk-protocol 的编解码，但使用 Client 端请求构建器
pub struct BrokerConnection {
    broker_id: i32,
    stream: TcpStream,
    correlation_id: AtomicI32,
    api_versions: HashMap<i16, (i16, i16)>,  // api_key → (min, max)
}

impl BrokerConnection {
    /// 发送请求并等待响应 — 使用与 Broker 端完全一致的协议编解码
    pub async fn send_request(&mut self, request: &dyn KafkaRequest) -> Result<Bytes> {
        let corr_id = self.correlation_id.fetch_add(1, Ordering::SeqCst);

        // 构建请求帧 — 与 Kafka Wire Protocol 完全一致
        let mut buf = BytesMut::new();
        self.write_request_header(&mut buf, request.api_key(), request.api_version(), corr_id)?;
        request.encode(&mut buf)?;

        // 发送
        self.stream.write_all(&buf).await?;

        // 读取响应
        let response = self.read_response().await?;
        Ok(response)
    }
}
```

### 12.7 互操作测试策略

#### 12.7.1 协议级 Fuzzing

```rust
/// 协议编解码 fuzzing — 确保 R-Kafka 能解析任意合法 Kafka 字节流
#[cfg(test)]
mod protocol_fuzz_tests {
    /// 用 Kafka 官方客户端产生的真实请求字节验证 R-Kafka 解码器
    #[test]
    fn test_decode_real_kafka_produce_request() {
        // 从 Java Kafka Producer 抓包获取的真实 ProduceRequest 字节
        let raw_bytes = include_bytes!("../testdata/produce_request_v9.bin");
        let request = ProduceRequest::decode(&mut BytesMut::from(raw_bytes)).unwrap();
        assert_eq!(request.acks, -1);
        assert_eq!(request.topic_partitions.len(), 1);
    }

    /// 用 Kafka 官方客户端产生的真实响应字节验证 R-Kafka 解码器
    #[test]
    fn test_decode_real_kafka_fetch_response() {
        let raw_bytes = include_bytes!("../testdata/fetch_response_v12.bin");
        let response = FetchResponse::decode(&mut BytesMut::from(raw_bytes)).unwrap();
        assert_eq!(response.responses[0].partitions[0].error_code, 0);
    }
}
```

#### 12.7.2 端到端互操作测试

```
┌─────────────────────────────────────────────────────────────────┐
│                    互操作测试拓扑                                 │
│                                                                 │
│  ┌──────────────┐              ┌──────────────────┐            │
│  │ Java Kafka   │              │  R-Kafka Broker  │            │
│  │ Producer     │─────────────►│  (被测)           │            │
│  │ (官方 SDK)    │   TCP:9092   │                  │            │
│  └──────────────┘              └────────┬─────────┘            │
│                                         │                       │
│  ┌──────────────┐              ┌────────┴─────────┐            │
│  │ Java Kafka   │              │  R-Kafka Broker  │            │
│  │ Consumer     │◄─────────────│  (被测)           │            │
│  │ (官方 SDK)    │   TCP:9092   │                  │            │
│  └──────────────┘              └──────────────────┘            │
│                                                                 │
│  ┌──────────────┐              ┌──────────────────┐            │
│  │ R-Kafka Rust │              │  Java Kafka      │            │
│  │ Client       │─────────────►│  Broker (官方)    │            │
│  │ (被测)       │   TCP:9092   │  (Docker)        │            │
│  └──────────────┘              └──────────────────┘            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```rust
/// 端到端互操作测试
#[cfg(test)]
mod interop_tests {

    /// 测试 1: Java Producer → R-Kafka Broker → Java Consumer
    #[tokio::test]
    async fn java_to_rkbroker_to_java() {
        // 启动 R-Kafka Broker (测试容器)
        let broker = start_rkafka_test_broker().await;

        // Java Producer 写入 (通过 testcontainers 启动 Java Client)
        // ... 验证 Java Producer 可以成功连接并写入

        // Java Consumer 读取
        // ... 验证 Java Consumer 可以读到 Java Producer 写入的数据
    }

    /// 测试 2: Rust Client → Java Kafka Broker
    #[tokio::test]
    async fn rust_client_to_java_broker() {
        // 启动 Java Kafka Broker (Docker)
        let kafka = start_java_kafka_container().await;

        // R-Kafka Rust Client 连接
        let producer = KafkaProducer::new(ClientConfig {
            bootstrap_servers: vec![kafka.address()],
            ..Default::default()
        });

        // 写入数据
        let metadata = producer.send(ProducerRecord {
            topic: "test-topic".into(),
            key: Some(Bytes::from("key")),
            value: Some(Bytes::from("hello from rust")),
            ..Default::default()
        }).await.unwrap();

        // 验证 Java Broker 成功接收
        assert!(metadata.offset >= 0);
    }

    /// 测试 3: Rust Client → R-Kafka Broker (原生闭环)
    #[tokio::test]
    async fn rust_client_to_rk_broker() {
        let broker = start_rkafka_test_broker().await;

        let producer = KafkaProducer::new(ClientConfig {
            bootstrap_servers: vec![broker.address()],
            ..Default::default()
        });

        let consumer = KafkaConsumer::new(ConsumerConfig {
            bootstrap_servers: vec![broker.address()],
            group_id: Some("test-group".into()),
            ..Default::default()
        });

        // 写入
        producer.send(test_record()).await.unwrap();

        // 消费
        consumer.subscribe(&["test-topic"]).await.unwrap();
        let records = consumer.poll(Duration::from_secs(5)).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_ref().unwrap(), &Bytes::from("hello"));
    }

    /// 测试 4: Consumer Group 互操作
    #[tokio::test]
    async fn consumer_group_interop() {
        // Java Consumer 和 Rust Consumer 在同一个 Group 中
        // 验证 Rebalance 正确分配 Partition
        // 验证两者都能消费到数据
        todo!()
    }
}
```

#### 12.7.3 测试数据收集

从官方 Kafka 收集协议基准数据：

```
testdata/
├── produce_request_v0.bin     # Kafka 2.x 产生的 ProduceRequest v0
├── produce_request_v9.bin     # Kafka 3.x 产生的 ProduceRequest v9 (flexible)
├── fetch_response_v0.bin      # Kafka 2.x 产生的 FetchResponse v0
├── fetch_response_v12.bin     # Kafka 3.x 产生的 FetchResponse v12 (flexible)
├── metadata_response_v0.bin   # Metadata v0
├── metadata_response_v12.bin  # Metadata v12 (flexible)
├── join_group_request.bin     # Consumer JoinGroup
├── sync_group_request.bin     # Consumer SyncGroup
├── sasl_handshake.bin         # SASL 握手
└── ...                        # 每个 API 每个版本至少一个样本
```

### 12.8 Kafka 版本兼容性矩阵

| Kafka 版本 | Broker 兼容 (R-Kafka 作为 Broker) | Client 兼容 (R-Kafka Client 连接) |
|:---|:---|:---|
| 2.0 - 2.3 | 支持 legacy 协议 (非 flexible) | 支持 |
| 2.4 - 2.7 | 支持 flexible 协议 (部分 API) | 支持 |
| 2.8 - 3.x | 完整支持 (含 KRaft 模式) | 支持 |
| 3.3+ | 支持 finalized features 协商 | 支持 |
| 4.0+ | 跟踪最新协议变更 | 支持 |

### 12.9 互操作关键约束

| 约束 | 说明 |
|:---|:---|
| **字节序** | 全部 Big-Endian，与 Java DataOutputStream 一致 |
| **VARINT 编码** | ZigZag + Variable-Length，与 Kafka 实现完全一致 |
| **CRC32C** | 从 magic 字段开始，覆盖到 records 末尾 |
| **COMPACT 类型** | length 字段存储 `实际长度 + 1`，0 表示 null |
| **Flexible 版本** | 请求/响应末尾追加 TaggedFields (可以为空) |
| **压缩** | 压缩粒度是 RecordBatch 内的全部 records，不是单条 |
| **时间戳** | 毫秒级 Unix 时间戳，CreatePolicy=CreateTime/LogAppendTime |
| **null 语义** | key=null 合法；value=null 是 Tombstone 标记 |
| **字符串编码** | 修改版 UTF-8 (与 Java Modified UTF-8 略有差异需注意) |

### 12.10 MessageSet v0/v1 向后兼容

Kafka 在 0.11 之前使用 **MessageSet v0/v1** 格式（无 RecordBatch 概念）。虽然现代客户端默认使用 v2 RecordBatch，但 R-Kafka 必须能处理旧格式：

| 格式 | Magic | 特点 | 兼容策略 |
|:---|:---|:---|:---|
| MessageSet v0 | 0 | 无 ProducerId/Sequence，CRC32 | 内部转换为 v2 RecordBatch 存储 |
| MessageSet v1 | 1 | 增加 timestamp，CRC32 | 内部转换为 v2 RecordBatch 存储 |
| RecordBatch v2 | 2 | 完整幂等、压缩、headers，CRC32C | 原生格式 |

```rust
/// 协议层自动检测消息格式版本并转换
pub fn decode_message_set(reader: &mut KafkaReader) -> Result<Vec<RecordBatch>> {
    let magic = reader.peek_i8()?;
    match magic {
        0 | 1 => {
            // v0/v1 MessageSet → 转换为 v2 RecordBatch
            let messages = decode_legacy_message_set(reader)?;
            Ok(vec![RecordBatch::from_legacy_messages(messages, magic)])
        }
        2 => {
            // v2 RecordBatch — 直接解析
            Ok(vec![RecordBatch::decode(reader)?])
        }
        _ => Err(ProtocolError::UnknownMagic(magic)),
    }
}
```

> **注意**：Produce 请求中可能混合 v0/v1/v2 消息。Broker 统一转换为 v2 格式落盘，确保磁盘格式一致性。

### 12.11 Metadata 响应格式版本差异

Metadata API 是客户端发现集群拓扑的核心。不同版本的响应格式有重大差异：

| 版本范围 | 关键变化 |
|:---|:---|
| v0-v3 | 基础格式：brokers + topics + partitions |
| v4-v7 | 增加 `is_internal`、`offline_replicas` |
| v8 | 增加 `cluster_id`、`controller_id` |
| v9 (flexible) | 增加 `topic_authorized_operations` |
| v10-v11 | 增加 `broker.rack`、`listener_name` |
| v12 (flexible) | **重大变更**：用 `NodeEndpoints` 替代 `Broker.host/port`，支持多 Listener |
| v13+ | 增加 `topic_id` (UUID)、`topic_is_internal` |

```rust
/// Metadata 响应 — 根据版本自动适配格式
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: Option<String>,          // v8+
    pub controller_id: Option<i32>,          // v8+
    pub topics: Vec<TopicMetadata>,
    // v12+ 新增: 独立节点端点列表 (替代 brokers 中的 host/port)
    pub node_endpoints: Vec<NodeEndpoint>,   // v12+
}

/// v12+ 的节点端点 — 支持多 Listener
pub struct NodeEndpoint {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}
```

> **实现要点**：对于 v12+ 的 Metadata 响应，`brokers` 列表中的 `host/port` 字段设为空，客户端应从 `node_endpoints` 获取连接信息。

### 12.12 Consumer Protocol V2 向前兼容 (KIP-848 / KIP-853)

Kafka 4.0 引入了新一代 Consumer Group 协议：

| KIP | 名称 | 变化 |
|:---|:---|:---|
| KIP-848 | 新 Consumer Group Protocol | 引入 Generic Group Protocol，支持 Server-side Rebalance |
| KIP-853 | Consumer Protocol V2 | 新的 JoinGroup/SyncGroup 协议，使用新的 API Key 空间 |

**R-Kafka 兼容策略**：

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Consumer Group 协议演进                              │
│                                                                      │
│  V1 (当前实现)          V2 (KIP-848/853, Kafka 4.0+)                 │
│  ┌──────────────┐      ┌──────────────┐                             │
│  │ JoinGroup    │      │ JoinGroup    │  新 API Key 空间              │
│  │ SyncGroup    │  →   │ SyncGroup    │  Server-side assignment      │
│  │ Heartbeat    │      │ Heartbeat    │  支持多种 Group Protocol      │
│  └──────────────┘      └──────────────┘                             │
│                                                                      │
│  R-Kafka 策略:                                                       │
│  1. 完整实现 V1 协议 (当前)                                           │
│  2. 在 ApiVersions 中声明不支持 V2 API (客户端自动回退 V1)              │
│  3. Phase 6+ 逐步引入 V2 支持                                         │
└──────────────────────────────────────────────────────────────────────┘
```

### 12.13 openraft 与 Kafka KRaft 兼容性说明

R-Kafka 使用 `openraft` 作为 Raft 共识引擎，但 Kafka KRaft 的 Controller 选举机制与标准 Raft 有差异：

| 特性 | 标准 Raft (openraft) | Kafka KRaft |
|:---|:---|:---|
| Leader 选举 | 标准 RequestVote RPC | 自定义 Controller Election Protocol |
| 日志复制 | AppendEntries | Fetch API (key=1) 复用 |
| 成员变更 | Joint Consensus / Single-server | 通过 Metadata Record 管理 |
| 快照 | 标准 Snapshot | Metadata Log 偏移点 |

**关键兼容点**：Kafka KRaft 的 Controller 选举不是标准 Raft RequestVote，而是通过 `BeginQuorumEpoch`/`EndQuorumEpoch`/`Vote` API 实现的自定义协议。

```rust
/// R-Kafka Controller 选举 — 使用 Kafka KRaft 协议，而非 openraft 默认选举
/// openraft 仅用于日志复制和一致性保证
/// Controller 选举通过 Vote/BeginQuorumEpoch/EndQuorumEpoch API 实现
pub struct KRaftControllerElection {
    /// 当前 Controller epoch — 每次选举递增
    controller_epoch: i32,
    /// 当前 Leader (Controller) 的 Broker ID
    active_controller: Option<BrokerId>,
}

impl KRaftControllerElection {
    /// 发起选举 — 发送 Vote 请求到所有 Quorum 成员
    pub async fn start_election(&mut self) -> Result<()> {
        self.controller_epoch += 1;
        // 发送 VoteRequest (API 51) 到所有 Quorum 节点
        // 获得多数票后成为 Controller
        // 发送 BeginQuorumEpoch (API 52) 通知所有节点
        todo!()
    }
}
```

> **注意**：openraft 的 Raft 配置用于 **数据面**（Metadata Log 复制），而 **控制面**（Controller 选举）使用 Kafka 自定义的 Vote/BeginQuorumEpoch 协议。两者共存，互不干扰。

### 12.14 补充关键错误码

文档 12.3 节的错误码列表需补充以下关键错误码（Kafka 标准值，与 `rk-protocol/src/error_codes.rs` 实现一致）：

```rust
// 补充错误码 — 确保与 Kafka 完全一致
// 参考: https://kafka.apache.org/protocol.html#protocol_error_codes
pub enum KafkaErrorCode {
    // ... (已有错误码: UnknownServerError=-1 到 FeatureUpdateFailed=100)

    // 48-70: 事务、Leader Epoch、Broker 相关
    InvalidTxnState                     = 48,  // 无效的事务状态
    InvalidProducerIdMapping            = 49,  // ProducerId 映射无效
    InvalidTransactionTimeout           = 50,  // 事务超时无效
    ConcurrentTransactions              = 51,  // 并发事务冲突
    TransactionCoordinatorFenced        = 52,  // 事务 Coordinator 被隔离
    InvalidProducerId                   = 53,  // ProducerId 无效
    InvalidSequenceNumber               = 54,  // 序列号无效
    InvalidEpoch                        = 55,  // Epoch 无效
    FencedLeaderEpoch                   = 56,  // Leader Epoch 已被隔离
    UnknownLeaderEpoch                  = 57,  // 未知的 Leader Epoch
    UnsupportedCompressionType          = 58,  // 不支持的压缩类型
    StaleBrokerEpoch                    = 59,  // 过时的 Broker Epoch
    OffsetNotAvailable                  = 60,  // Offset 不可用
    MemberIdRequired                    = 61,  // JoinGroup 需要 member_id
    PreferredLeaderNotAvailable         = 62,  // 首选 Leader 不可用
    GroupMaxSizeReached                 = 63,  // Consumer Group 达到最大成员数
    FencedInstanceId                    = 64,  // 静态成员 InstanceId 被隔离
    EligibleLeadersNotAvailable         = 65,  // 无可用合格 Leader
    NoReassignmentInProgress            = 66,  // 无进行中的重分配
    GroupSubscribedToTopic              = 67,  // 有 Consumer Group 订阅了该 Topic
    InvalidRecord                       = 68,  // 无效的记录
    UnstableOffsetCommit                = 69,  // 不稳定的 Offset 提交
    ThrottlingQuotaExceeded             = 70,  // 超出配额被限流

    // 71-84: 生产、安全、Delegation Token 相关
    ProducerFenced                      = 71,  // Producer 被隔离 (Epoch 过期)
    ResourceNotFound                    = 72,  // 资源未找到
    DuplicateResource                   = 73,  // 重复的资源
    UnacceptableCredential              = 74,  // 不可接受的凭证
    InconsistentVoterSet                = 75,  // 不一致的 Voter 集合
    InvalidVotingKey                    = 76,  // 无效的投票 Key
    InvalidMetadataVersion                = 77,  // 无效的 Metadata 版本
    TopicDeletionDisabled               = 78,  // Topic 删除已禁用
    FencedLeaderEpoch2                  = 79,  // Leader Epoch 隔离 (别名, 新版本)
    KafkaStorageError                   = 80,  // Kafka 存储错误
    LogDirNotFound                      = 81,  // 日志目录未找到
    SaslAuthenticationFailed            = 82,  // SASL 认证失败
    UnknownProducerId                   = 83,  // 未知的 ProducerId
    ReassignmentInProgress              = 84,  // 重分配进行中

    // 85-100: Delegation Token、Fetch Session 等
    DelegationTokenAuthDisabled         = 85,  // Delegation Token 认证已禁用
    DelegationTokenNotFound             = 86,  // Delegation Token 未找到
    DelegationTokenOwnerMismatch        = 87,  // Delegation Token 所有者不匹配
    DelegationTokenRequestNotAllowed    = 88,  // 不允许请求 Delegation Token
    DelegationTokenAuthorizationFailed  = 89,  // Delegation Token 授权失败
    DelegationTokenExpired              = 90,  // Delegation Token 已过期
    InvalidPrincipalType                = 91,  // 无效的 Principal 类型
    NonEmptyGroup                       = 92,  // 非空 Consumer Group
    GroupIdNotFound                     = 93,  // GroupId 未找到
    FetchSessionIdNotFound              = 94,  // Fetch Session ID 未找到
    InvalidFetchSessionEpoch            = 95,  // 无效的 Fetch Session Epoch
    ListenerNotFound                    = 96,  // Listener 未找到
    TopicExistsException                = 97,  // Topic 已存在 (异常)
    InvalidMetadata                     = 98,  // 无效的 Metadata
    InvalidUpdateVersion                = 99,  // 无效的更新版本
    FeatureUpdateFailed                 = 100, // Feature 更新失败
}
```

> **注意**：代码实现中全部 102 个错误码 (`-1` 到 `100`) 均严格遵循 Kafka 标准值。`TopicAlreadyExists` (=36) 与 `SaslAuthenticate` API Key (=36) 值相同但含义完全不同（一个是错误码，一个是 API Key），在不同上下文中使用，不会冲突。

### 12.15 Broker 集群互操作

Broker 集群互操作是最复杂的兼容维度。它要求 R-Kafka Broker 能与 Java Kafka Broker **在同一个集群中混合运行**，实现零停机滚动迁移。

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     混合集群拓扑 (滚动迁移期间)                           │
│                                                                         │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐              │
│   │  Broker-1    │   │  Broker-2    │   │  Broker-3    │              │
│   │  Java Kafka  │   │  R-Kafka     │   │  Java Kafka  │              │
│   │  3.6         │   │  (Rust)      │   │  3.6         │              │
│   │              │   │              │   │              │              │
│   │  Controller  │◄─►│  Controller  │◄─►│  Controller  │              │
│   │  (KRaft)     │   │  (KRaft)     │   │  (KRaft)     │              │
│   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘              │
│          │                  │                  │                        │
│          └──────────────────┼──────────────────┘                        │
│                             │                                           │
│                    KRaft Metadata Quorum                                │
│                    (3 节点混合 Raft 集群)                                │
│                                                                         │
│   副本跨实现复制:                                                       │
│   Partition-0 Leader(Broker-1,Java) ──Fetch──► Follower(Broker-2,Rust) │
│   Partition-1 Leader(Broker-2,Rust) ──Fetch──► Follower(Broker-3,Java) │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 12.15.1 Inter-Broker Protocol (IBP) 兼容

Broker 之间通信使用的 API 与 Client 协议相同（Kafka Wire Protocol），但使用特定的 API 和版本。R-Kafka 必须实现这些 Broker-to-Broker API：

| API Key | 名称 | 用途 | 方向 |
|:---|:---|:---|:---|
| 1 | Fetch | **副本复制**：Follower 从 Leader 拉取数据 (replica_id ≥ 0) | Broker→Broker |
| 3 | Metadata | **元数据同步**：Broker 间交换集群拓扑 | Broker→Broker |
| 4 | LeaderAndIsr | **Controller→Broker**：通知 Leader/ISR 变更 | Controller→Broker |
| 5 | StopReplica | **Controller→Broker**：通知停止副本 | Controller→Broker |
| 6 | UpdateMetadata | **Controller→Broker**：广播元数据更新 | Controller→Broker |
| 7 | ControlledShutdown | **Broker→Controller**：优雅关闭协商 | Broker→Controller |
| 54 | BrokerRegistration | **Broker→Controller**：Broker 注册到 KRaft Controller | Broker→Controller |
| 55 | BrokerHeartbeat | **Broker→Controller**：Broker 心跳保活 | Broker→Controller |

```rust
/// Broker 间请求 — 复用 Client 相同的 Wire Protocol 编码
/// 但使用特殊的 "inter-broker" client_id 前缀标识
pub struct InterBrokerRequest {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,           // "rk-broker-{broker_id}" 格式
    pub body: InterBrokerBody,
}

/// Fetch 请求用于副本复制时，需要携带额外字段
pub struct ReplicaFetchRequest {
    pub replica_id: i32,             // Follower Broker ID (必须 ≥ 0)
    pub replica_state: ReplicaState, // KRaft 新增: current_leader_epoch, last_offset
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,         // 0=READ_UNCOMMITTED, 1=READ_COMMITTED
    pub topics: Vec<FetchTopic>,
}

pub struct ReplicaState {
    pub replica_id: i32,
    pub current_leader_epoch: i32,   // Follower 已知的 Leader epoch
    pub last_fetch_epoch: i32,       // 上次 fetch 时的 epoch
    pub last_fetched_offset: i64,    // 上次 fetch 到的 offset
}

/// BrokerRegistration 请求 — Broker 注册到 KRaft Controller
pub struct BrokerRegistrationRequest {
    pub broker_id: i32,
    pub cluster_id: String,
    pub incarnation_id: Uuid,        // Broker 实例唯一 ID (重启后变化)
    pub listeners: Vec<Listener>,
    pub features: Vec<BrokerFeature>,// 本 Broker 支持的特性版本
    pub racks: Option<Vec<String>>,
}

/// BrokerHeartbeat 请求 — Broker 向 Controller 报活
pub struct BrokerHeartbeatRequest {
    pub broker_id: i32,
    pub cluster_id: String,
    pub current_metadata_offset: i64, // Broker 已应用的 Metadata Log 偏移
    pub fenced: bool,                 // 是否处于隔离状态
}
```

#### 12.15.2 KRaft Controller 协议兼容

KRaft 模式下，Controller 使用 Raft 协议管理元数据。混合集群中，R-Kafka Controller 和 Java Kafka Controller 必须在 **同一个 Raft Quorum** 中。

**关键兼容点**：

```
┌─────────────────────────────────────────────────────────────────────┐
│                 KRaft Controller Quorum (混合)                       │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐           │
│  │  Node-1      │   │  Node-2      │   │  Node-3      │           │
│  │  Java Kafka  │   │  R-Kafka     │   │  Java Kafka  │           │
│  │  Controller  │   │  Controller  │   │  Controller  │           │
│  │              │   │              │   │              │           │
│  │  Raft RPC ◄───────►  Raft RPC ◄───────►  Raft RPC │           │
│  │              │   │              │   │              │           │
│  │  Metadata    │   │  Metadata    │   │  Metadata    │           │
│  │  Log (同一)  │   │  Log (同一)  │   │  Log (同一)  │           │
│  └──────────────┘   └──────────────┘   └──────────────┘           │
│                                                                     │
│  约束: 所有节点必须能编解码相同的 Metadata Record 格式                 │
└─────────────────────────────────────────────────────────────────────┘
```

**Raft RPC 兼容**：

Kafka KRaft 的 Raft RPC 使用标准 Kafka Wire Protocol 封装：

| API Key | 名称 | 用途 |
|:---|:---|:---|
| 1 | Fetch | Raft 日志复制 (Leader→Follower，复用 Fetch API) |
| 51 | Vote | Raft 投票请求 |
| 52 | BeginQuorumEpoch | Raft 新纪元开始通知 |
| 53 | EndQuorumEpoch | Raft 纪元结束通知 |
| 56 | DescribeQuorum | 查询 Quorum 状态 |

```rust
/// KRaft Raft Vote 请求 — 必须与 Kafka 实现完全一致
pub struct VoteRequest {
    pub cluster_id: String,
    pub voter_id: i32,                 // 候选者 Broker ID
    pub candidate_id: i32,             // 候选者 Broker ID (同上)
    pub candidate_epoch: i64,          // 候选者当前 epoch
    pub last_offset: i64,              // 候选者日志最后偏移
    pub last_offset_epoch: i32,        // 最后偏移的 epoch
}

/// KRaft Metadata Record — 写入 Metadata Log 的二进制记录
/// 格式必须与 Kafka 完全一致，否则混合 Quorum 无法解析
pub struct MetadataRecord {
    pub version: i16,
    pub record_type: MetadataRecordType,
    pub data: Bytes,                   // 具体记录的序列化数据
}

/// Metadata Record 类型 — 与 Kafka MetadataVersion 对应
#[repr(i16)]
pub enum MetadataRecordType {
    PartitionRecord         = 3,
    PartitionRemoveRecord   = 12,
    TopicRecord             = 2,
    TopicRemoveRecord       = 11,
    ConfigRecord            = 4,
    ConfigRemoveRecord      = 13,
    BrokerRegistrationRecord= 15,
    BrokerFencedRecord      = 23,
    BrokerUnfencedRecord    = 16,
    LeaderAndIsrRecord      = 25,
    TopicPartitionAssignmentRecord = 26,
    ProducerIdsRecord       = 17,
    AccessControlRecord     = 19,
    // ... 完整列表需覆盖 Kafka 所有 Metadata Record 类型
}
```

#### 12.15.3 Metadata Record 二进制格式兼容

Metadata Record 是 KRaft 的核心数据。每条 Record 写入 Metadata Log，所有 Controller 必须能解析：

```
Metadata Log Record 布局:
┌──────────────────────────────────────────────────────────────────────┐
│ base_offset (i64)          │ 8 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ batch_length (i32)         │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ partition_leader_epoch     │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ magic (i8) = 2             │ 1 byte                                  │
├────────────────────────────┼─────────────────────────────────────────┤
│ crc (i32)                  │ 4 bytes  CRC32C                         │
├────────────────────────────┼─────────────────────────────────────────┤
│ attributes (i16)           │ 2 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ last_offset_delta          │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ base_timestamp             │ 8 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ max_timestamp              │ 8 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ producer_id = -1           │ 8 bytes  (Metadata 不用幂等)             │
├────────────────────────────┼─────────────────────────────────────────┤
│ producer_epoch = -1        │ 2 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ base_sequence = -1         │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ record_count (i32)         │ 4 bytes                                 │
├────────────────────────────┼─────────────────────────────────────────┤
│ ┌── Record ──────────────────────────────────────────────────────┐  │
│ │ length (varint)                                                │  │
│ │ attributes (i8) = 0                                           │  │
│ │ timestamp_delta (varlong)                                      │  │
│ │ offset_delta (varint)                                          │  │
│ │ key_len (varint)                                               │  │
│ │ key: { version (i16), type (i16) }   ← MetadataRecordHeader   │  │
│ │ value_len (varint)                                             │  │
│ │ value: <具体 Record 数据>           ← 按 type 反序列化          │  │
│ │ header_count (varint) = 0                                      │  │
│ └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

```rust
/// Metadata Record Key Header — 标识记录类型
pub struct MetadataRecordKeyHeader {
    pub version: i16,       // 记录版本 (用于向前兼容)
    pub record_type: i16,   // MetadataRecordType 枚举值
}

/// PartitionRecord — 描述一个 Partition 的分配
/// 二进制格式必须与 Kafka 完全一致
pub struct PartitionRecord {
    pub partition_id: i32,
    pub topic_id: Uuid,                  // Topic UUID (Kafka 4.0+ 用 UUID 标识)
    pub replicas: Vec<i32>,              // 所有 Replica 所在 Broker ID
    pub isr: Vec<i32>,                   // ISR 成员
    pub leader: i32,                     // Leader Broker ID
    pub leader_epoch: i32,               // Leader 纪元
    pub partition_epoch: i32,            // Partition 纪元 (每次 ISR/Leader 变更递增)
    pub directories: Vec<Uuid>,          // 每个 Replica 的日志目录 UUID
}

/// BrokerRegistrationRecord — Broker 注册信息
pub struct BrokerRegistrationRecord {
    pub broker_id: i32,
    pub cluster_id: String,
    pub incarnation_id: Uuid,
    pub listeners: Vec<Listener>,
    pub features: Vec<FinalizedFeatureKey>,
    pub racks: Option<Vec<String>>,
}

impl PartitionRecord {
    /// 编码为 Kafka 兼容的字节 — 使用与 Kafka 相同的 Schema
    pub fn encode(&self, writer: &mut KafkaWriter) -> Result<()> {
        writer.write_i32(self.partition_id)?;
        writer.write_bytes(&self.topic_id.as_bytes())?;    // UUID as 16 bytes
        // replicas: compact array of i32
        writer.write_varint(self.replicas.len() as i32 + 1)?;
        for &r in &self.replicas { writer.write_i32(r)?; }
        // isr: compact array of i32
        writer.write_varint(self.isr.len() as i32 + 1)?;
        for &r in &self.isr { writer.write_i32(r)?; }
        writer.write_i32(self.leader)?;
        writer.write_i32(self.leader_epoch)?;
        writer.write_i32(self.partition_epoch)?;
        // directories: compact array of UUID
        writer.write_varint(self.directories.len() as i32 + 1)?;
        for dir in &self.directories { writer.write_bytes(&dir.as_bytes())?; }
        // tagged fields (empty)
        writer.write_varint(0)?;
        Ok(())
    }
}
```

#### 12.15.4 Feature Version 协商

混合集群中，所有 Broker 必须就 **互操作协议版本** 达成一致。Kafka 使用 Feature Version 机制：

```rust
/// 每个 Broker 上报自己支持的特性版本范围
pub struct BrokerFeature {
    pub name: String,           // 特性名称
    pub min_version: i16,       // 最低支持版本
    pub max_version: i16,       // 最高支持版本
}

/// Controller 根据所有 Broker 的能力，计算最终生效的版本
/// finalized_version = min(所有 Broker 的 max_version)
/// 如果 finalized_version < 任何 Broker 的 min_version → 集群无法启动
pub struct FeatureManager {
    broker_features: RwLock<HashMap<BrokerId, Vec<BrokerFeature>>>,
}

pub struct FinalizedFeature {
    pub name: String,
    pub version: i16,            // 集群最终生效版本
}

/// 关键 Feature 名称 — 必须与 Kafka 一致
pub const FEATURE_METADATA_VERSION: &str = "metadata.version";
pub const FEATURE_GROUP_VERSION: &str = "group.version";
pub const FEATURE_TXN_VERSION: &str = "transaction.version";

impl FeatureManager {
    /// 计算集群最终生效版本
    pub fn compute_finalized(&self) -> HashMap<String, i16> {
        let mut result = HashMap::new();
        // 对每个 feature，取所有 broker max_version 的最小值
        // 即: 所有 broker 都支持的最高公共版本
        for (feature_name, _) in &self.broker_features.values().next().unwrap() {
            let min_max = self.broker_features.values()
                .filter_map(|features| {
                    features.iter().find(|f| f.name == *feature_name).map(|f| f.max_version)
                })
                .min()
                .unwrap_or(0);
            result.insert(feature_name.clone(), min_max);
        }
        result
    }

    /// 检查集群是否可以运行 (所有 broker 的 min ≤ finalized ≤ max)
    pub fn is_compatible(&self) -> bool {
        let finalized = self.compute_finalized();
        for (broker_id, features) in &self.broker_features {
            for feature in features {
                if let Some(&fin) = finalized.get(&feature.name) {
                    if fin < feature.min_version || fin > feature.max_version {
                        return false;
                    }
                }
            }
        }
        true
    }
}
```

#### 12.15.5 跨实现副本复制

混合集群中最关键的场景：R-Kafka Follower 从 Java Leader 复制数据，反之亦然。

```
场景 A: Java Leader → R-Kafka Follower
┌────────────────┐                    ┌────────────────┐
│  Java Broker   │                    │  R-Kafka       │
│  (Leader)      │                    │  (Follower)    │
│                │                    │                │
│  Partition-0   │◄── FetchRequest ───│  Replica       │
│  LEO = 1000    │─── FetchResponse ──►  Fetcher       │
│                │    (RecordBatch)   │                │
│                │                    │  写入本地 Log   │
│                │                    │  更新 LEO       │
└────────────────┘                    └────────────────┘

场景 B: R-Kafka Leader → Java Follower
┌────────────────┐                    ┌────────────────┐
│  R-Kafka       │                    │  Java Broker   │
│  (Leader)      │                    │  (Follower)    │
│                │                    │                │
│  Partition-1   │◄── FetchRequest ───│  Replica       │
│  LEO = 500     │─── FetchResponse ──►  Fetcher       │
│                │    (RecordBatch)   │                │
│                │                    │  写入本地 Log   │
└────────────────┘                    └────────────────┘
```

**关键约束**：

| 约束 | 说明 |
|:---|:---|
| **RecordBatch 格式** | 两种实现必须读写完全相同的 RecordBatch v2 二进制格式 |
| **CRC32C** | 校验算法必须一致，否则 Follower 认为数据损坏 |
| **Leader Epoch** | Follower 在 FetchRequest 中携带 `current_leader_epoch`，Leader 必须验证 |
| **Fetch Offset** | Follower 请求的 offset 必须精确，Leader 返回从该 offset 开始的数据 |
| **HW 传播** | Leader 在 FetchResponse 中返回 HW，Follower 据此更新自己的 HW |
| **压缩** | 数据以压缩形式传输，Follower 不解压直接写入本地 Log |

```rust
/// R-Kafka Replica Fetcher — 从 Java Leader 拉取数据
pub struct ReplicaFetcher {
    leader_broker: BrokerEndpoint,
    leader_epoch: i32,
    partition: TopicPartition,
    fetch_offset: u64,
    connection: BrokerConnection,      // 复用 rk-protocol 编解码
}

impl ReplicaFetcher {
    /// 发送 Fetch 请求到 Leader (可能是 Java Broker)
    pub async fn fetch(&mut self) -> Result<FetchResult> {
        let request = ReplicaFetchRequest {
            replica_id: self.local_broker_id,
            replica_state: ReplicaState {
                replica_id: self.local_broker_id,
                current_leader_epoch: self.leader_epoch,
                last_fetch_epoch: self.last_fetch_epoch,
                last_fetched_offset: self.fetch_offset,
            },
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,         // 1MB
            isolation_level: 0,
            topics: vec![FetchTopic {
                topic: self.partition.topic.clone(),
                partitions: vec![FetchPartition {
                    partition: self.partition.partition_id,
                    fetch_offset: self.fetch_offset as i64,
                    partition_max_bytes: 1048576,
                    current_leader_epoch: self.leader_epoch,
                }],
            }],
        };

        // 使用 rk-protocol 编码 — 与 Java Broker 完全一致的格式
        let response = self.connection.send_request(&request).await?;
        let fetch_response = FetchResponse::decode(&mut response)?;

        // 写入本地 Log — RecordBatch 格式与 Kafka 完全一致
        for partition_data in &fetch_response.responses[0].partitions {
            if partition_data.error_code != 0 {
                return Err(KafkaErrorCode::from(partition_data.error_code).into());
            }
            self.append_to_local_log(&partition_data.record_set).await?;
            self.fetch_offset = partition_data.high_watermark as u64;
        }

        Ok(FetchResult { records_fetched: count, hw_updated: true })
    }
}
```

#### 12.15.6 滚动迁移策略

从 Java Kafka 集群迁移到 R-Kafka 集群的完整流程：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      滚动迁移流程 (5 Broker 集群)                        │
│                                                                         │
│  Phase 0: 准备                                                          │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                             │
│  │ J-1 │ │ J-2 │ │ J-3 │ │ J-4 │ │ J-5 │   全部 Java Kafka 3.6       │
│  │ [C] │ │     │ │     │ │     │ │     │   [C] = Controller           │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘   Controller Quorum: 1,2,3  │
│                                                                         │
│  Phase 1: 替换 Broker-5 (数据节点)                                      │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                             │
│  │ J-1 │ │ J-2 │ │ J-3 │ │ J-4 │ │ R-5 │   R-5 加入集群               │
│  │ [C] │ │     │ │     │ │     │ │     │   迁移 Partition 到 R-5       │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘   R-5 作为 Follower 追赶     │
│                                                                         │
│  Phase 2: 替换 Broker-4                                                 │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                             │
│  │ J-1 │ │ J-2 │ │ J-3 │ │ R-4 │ │ R-5 │                              │
│  │ [C] │ │     │ │     │ │     │ │     │                              │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘                              │
│                                                                         │
│  Phase 3: 替换 Controller 节点 (Broker-3)                               │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                             │
│  │ J-1 │ │ J-2 │ │ R-3 │ │ R-4 │ │ R-5 │   R-3 加入 Controller Quorum │
│  │ [C] │ │ [C] │ │ [C] │ │     │ │     │   Raft 自动同步 Metadata Log │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘                              │
│                                                                         │
│  Phase 4: 替换 Broker-2                                                 │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                             │
│  │ J-1 │ │ R-2 │ │ R-3 │ │ R-4 │ │ R-5 │                              │
│  │ [C] │ │ [C] │ │ [C] │ │     │ │     │                              │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘                              │
│                                                                         │
│  Phase 5: 替换最后节点 Broker-1                                         │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                             │
│  │ R-1 │ │ R-2 │ │ R-3 │ │ R-4 │ │ R-5 │   全部 R-Kafka               │
│  │ [C] │ │ [C] │ │ [C] │ │     │ │     │   迁移完成                    │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘                              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**每个 Broker 替换步骤**：

```rust
/// 单个 Broker 的替换流程
pub struct RollingMigrationStep {
    /// Step 1: 新 R-Kafka Broker 启动，加入集群
    pub async fn join_cluster(&self, config: BrokerConfig) -> Result<()> {
        // 1. 配置相同的 cluster.id
        // 2. 配置相同的 controller.quorum.voters
        // 3. 启动 R-Kafka Broker
        // 4. 发送 BrokerRegistration 到 Controller
        // 5. Controller 将新 Broker 加入 Metadata
        // 6. 开始发送 BrokerHeartbeat
        todo!()
    }

    /// Step 2: 将 Partition 迁移到新 Broker
    pub async fn reassign_partitions(&self, old_broker: BrokerId, new_broker: BrokerId) -> Result<()> {
        // 1. Admin 发起 Partition Reassignment (reassign-partitions 命令)
        // 2. Controller 更新 Metadata: 新 Broker 加入 Replica 列表
        // 3. 新 Broker 上的 Follower 开始从 Leader Fetch (追赶数据)
        // 4. 追赶完成后，新 Broker 加入 ISR
        // 5. (可选) Leader Transfer 到新 Broker
        // 6. 从旧 Broker 移除 Replica
        todo!()
    }

    /// Step 3: 优雅关闭旧 Broker
    pub async fn decommission_broker(&self, broker_id: BrokerId) -> Result<()> {
        // 1. 发送 ControlledShutdown 请求到 Controller
        // 2. Controller 将该 Broker 上的所有 Leader 转移走
        // 3. Controller 将该 Broker 从 ISR 中移除
        // 4. 确认所有 Partition 都有足够的 ISR 成员
        // 5. 关闭旧 Broker 进程
        // 6. Controller 标记该 Broker 为 Dead
        todo!()
    }
}
```

#### 12.15.7 Controller Quorum 混合运行

混合迁移期间，Controller Quorum 中同时有 Java 和 R-Kafka 节点：

```rust
/// R-Kafka Controller 节点 — 参与混合 Raft Quorum
pub struct KRaftControllerNode {
    raft_node: RaftNode,                // openraft 实例
    metadata_state: MetadataStateMachine,
    metadata_log: MetadataLog,          // 与 Kafka 格式完全一致的 Log
}

/// R-Kafka 作为 Raft Follower 时，必须能解析 Leader (Java) 写入的 Metadata Log
impl KRaftControllerNode {
    /// 接收 Raft AppendEntries — Leader 可能是 Java Controller
    pub async fn apply_metadata_entries(&mut self, entries: &[RaftEntry]) -> Result<()> {
        for entry in entries {
            // 解析 Metadata Record — 格式与 Kafka 完全一致
            let record = MetadataRecord::decode(&mut entry.data.clone())?;

            match record.record_type {
                MetadataRecordType::PartitionRecord => {
                    let partition = PartitionRecord::decode(&record.data)?;
                    self.metadata_state.update_partition(partition);
                }
                MetadataRecordType::BrokerRegistrationRecord => {
                    let reg = BrokerRegistrationRecord::decode(&record.data)?;
                    self.metadata_state.register_broker(reg);
                }
                // ... 处理所有 Record 类型
                _ => {
                    // 未知类型 → 跳过 (向前兼容)
                    tracing::warn!(record_type = ?record.record_type, "unknown metadata record type, skipping");
                }
            }

            self.metadata_log.advance_offset(entry.offset);
        }
        Ok(())
    }

    /// R-Kafka 作为 Raft Leader 时，写入 Metadata Record
    pub async fn write_metadata_event(&mut self, event: MetadataEvent) -> Result<()> {
        let record = MetadataRecord::from_event(&event)?;

        // 编码为 Kafka 兼容格式
        let mut buf = BytesMut::new();
        record.encode(&mut buf)?;

        // 通过 Raft 复制到所有 Follower (包括 Java Controller)
        self.raft_node.client_write(buf.freeze()).await?;
        Ok(())
    }
}
```

#### 12.15.8 回滚策略

迁移过程中发现问题时，需要能回滚到 Java Kafka：

```
回滚条件:
├── R-Kafka Broker 崩溃无法恢复
├── 数据不一致 (HW 异常)
├── 性能低于预期
└── 协议兼容性问题

回滚步骤:
1. 将 Partition 从 R-Kafka Broker 迁移回 Java Broker
   - 与正向迁移相同的 reassign-partitions 流程
2. 如果 R-Kafka 是 Controller Quorum 成员:
   - 先确保 Quorum 中有足够的 Java Controller
   - 移除 R-Kafka Controller 节点
   - 观察 Raft 重新选举 (如果 Leader 被移除)
3. 确认所有数据已迁移后，关闭 R-Kafka Broker

回滚约束:
├── Metadata Log 必须保持完整 (R-Kafka 写入的 Record Java 必须能解析)
├── RecordBatch 格式一致 (Java 能读取 R-Kafka 写入的数据)
└── Feature Version 不能升级 (迁移期间不启用 R-Kafka 独有特性)
```

#### 12.15.9 Cluster ID 与 Storage 目录兼容

```rust
/// Cluster ID — 混合集群中所有 Broker 必须使用相同的 Cluster ID
pub struct ClusterIdentity {
    pub cluster_id: String,            // UUID 格式，首次启动时生成
}

/// 存储目录结构 — 与 Kafka 一致，便于工具链互通
/// data/
/// ├── meta.properties                # cluster.id, node.id, directory.id
/// ├── __cluster_metadata-0/          # KRaft Metadata Log (Partition 0)
/// │   ├── 00000000000000000000.log
/// │   ├── 00000000000000000000.index
/// │   └── 00000000000000000000.timeindex
/// ├── my-topic-0/                    # 数据 Partition
/// │   ├── 00000000000000000000.log
/// │   ├── 00000000000000000000.index
/// │   └── 00000000000000000000.timeindex
/// └── ...

/// meta.properties 文件格式 — 与 Kafka 完全一致
/// # 可由 Java Kafka 工具 (kafka-storage.sh) 读取
/// cluster.id=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
/// node.id=1
/// directory.id=yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy
/// version=1
```

#### 12.15.10 集群互操作测试

```rust
/// 混合集群集成测试
#[cfg(test)]
mod cluster_interop_tests {

    /// 测试 1: R-Kafka Broker 加入 Java Kafka 集群
    #[tokio::test]
    async fn rkafka_join_java_cluster() {
        // 启动 3 节点 Java Kafka 集群 (KRaft 模式)
        let java_cluster = start_java_kafka_cluster(3).await;

        // 启动 R-Kafka Broker，配置相同的 cluster.id 和 quorum voters
        let rk_broker = start_rkafka_broker(BrokerConfig {
            cluster_id: java_cluster.cluster_id(),
            node_id: 4,
            controller_quorum_voters: java_cluster.quorum_voters(),
            ..Default::default()
        }).await;

        // 验证: R-Kafka Broker 成功注册到 Controller
        // 验证: R-Kafka Broker 接收到完整的 Metadata
        // 验证: R-Kafka Broker 开始发送 Heartbeat
    }

    /// 测试 2: 跨实现副本复制 (Java Leader → R-Kafka Follower)
    #[tokio::test]
    async fn cross_impl_replication_java_to_rust() {
        let cluster = start_mixed_cluster().await;

        // 创建 Topic，Partition Leader 在 Java Broker，Follower 在 R-Kafka
        cluster.create_topic("test", 1, 2, &[1, 4]).await; // broker-1=Java, broker-4=Rust

        // Java Producer 写入
        cluster.java_producer("test", "hello").await;

        // 验证: R-Kafka Follower 成功复制数据
        // 验证: R-Kafka Follower 的 LEO 追上 Leader
        // 验证: HW 正确更新
    }

    /// 测试 3: 跨实现副本复制 (R-Kafka Leader → Java Follower)
    #[tokio::test]
    async fn cross_impl_replication_rust_to_java() {
        let cluster = start_mixed_cluster().await;

        // 创建 Topic，Partition Leader 在 R-Kafka，Follower 在 Java Broker
        cluster.create_topic("test", 1, 2, &[4, 1]).await; // broker-4=Rust, broker-1=Java

        // Rust Producer 写入
        cluster.rust_producer("test", "hello").await;

        // 验证: Java Follower 成功复制数据
        // 验证: 通过 Java Consumer 可以消费到数据
    }

    /// 测试 4: KRaft Controller Quorum 混合运行
    #[tokio::test]
    async fn mixed_controller_quorum() {
        // 启动 2 Java + 1 R-Kafka Controller Quorum
        let cluster = start_mixed_quorum(2, 1).await;

        // 验证: R-Kafka Controller 能作为 Raft Follower
        // 验证: R-Kafka Controller 能解析 Java Leader 写入的 Metadata Log
        // 验证: R-Kafka Controller 能处理 BrokerRegistration/BrokerHeartbeat

        // 触发 Leader Election: 关闭 Java Controller Leader
        // 验证: R-Kafka Controller 参与投票
        // 验证: 新 Leader 选举成功 (可能是 R-Kafka)
    }

    /// 测试 5: 完整滚动迁移
    #[tokio::test]
    async fn full_rolling_migration() {
        let mut cluster = start_java_cluster(5).await;

        // 持续写入负载
        let producer = cluster.java_producer_bg("load-topic");

        // 逐个替换 Broker
        for i in (0..5).rev() {
            // 1. 启动 R-Kafka Broker
            let rk = start_rkafka_broker(cluster.config_for(i)).await;

            // 2. 迁移 Partition 到新 Broker
            cluster.reassign_to(i, rk.broker_id()).await;

            // 3. 等待 ISR 追赶完成
            cluster.wait_isr_complete(i).await;

            // 4. 关闭旧 Java Broker
            cluster.decommission(i).await;

            // 5. 验证: 数据无丢失，Producer/Consumer 无感知
            cluster.verify_data_integrity().await;
        }

        // 验证: 所有 Broker 都是 R-Kafka
        // 验证: 所有 Partition 的 ISR 完整
        // 验证: Producer/Consumer 持续可用
    }

    /// 测试 6: 回滚测试
    #[tokio::test]
    async fn rollback_test() {
        let mut cluster = start_mixed_cluster().await;  // 3 Java + 2 R-Kafka

        // 模拟 R-Kafka Broker 故障
        cluster.kill_rkafka_broker(4).await;

        // 验证: Java Broker 接管 Leader
        // 验证: 数据无丢失 (HW 之前的数据)
        // 验证: Consumer 可以继续消费

        // 回滚: 将 Partition 迁移回 Java Broker
        cluster.reassign_from_rkafka(4).await;

        // 验证: 所有数据在 Java Broker 上完整
    }
}
```

---

## 附录 A: 与原版文档的主要差异

| 维度 | 原版 (28022 行) | 精简版 |
|:---|:---|:---|
| 章节数 | 42 章 | 12 章 + 2 附录 |
| 网络运行时 | Glommio (thread-per-core) | Tokio + io_uring |
| 冗余内容 | Chapter 19 完整重复；Storage 3 次重复设计 | 合并去重 |
| Stream Processing | 独立章节 | 移除（独立产品） |
| Schema Registry | 独立章节 | 移除（独立服务） |
| Multi-Region | 独立章节 | 移除（运维拓扑） |
| AI 运维 | 独立章节 | 移除 |
| Client SDK | 多语言详细设计 | 移除多语言，保留 Rust Client + 协议互操作（第 12 章） |
| 外部系统集成 | UIPA/DGF/AgentForge | 移除（边界外） |
| 代码风格 | 大量空白行、碎片化 | 紧凑、可读、可编译 |

---

## 附录 B: 关键设计决策记录

| 决策 | 选择 | 替代方案 | 理由 |
|:---|:---|:---|:---|
| 异步运行时 | Tokio | Glommio, async-std | 生态最成熟，社区最大 |
| 磁盘 IO | io_uring | 传统 epoll+pread | 批量提交，更低延迟 |
| TLS | rustls | openssl, native-tls | 纯 Rust，无 C 依赖，内存安全 |
| Raft | openraft | raft-rs, 自研 | 纯 Rust，活跃维护 |
| 索引 | mmap (memmap2) | 用户态 BTree | 零拷贝，OS 管理换入换出 |
| 并发模型 | Thread-per-Core + Message Passing | 共享状态 + Mutex | 无锁，无缓存失效 |
| 协议兼容 | Kafka Wire Protocol | 自定义协议 | 生态兼容，无感迁移 |
| CRC | CRC32C | CRC32, SHA256 | 硬件加速，性能最优 |
| 磁盘格式 | Kafka RecordBatch v2 | 自定义格式 | 与 Kafka 工具链互通 |
| Flexible 版本 | 完整实现 tagged fields | 仅 legacy | Kafka 3.x+ 必须 |
| Client SDK | Rust 原生 + 协议兼容 | 多语言 SDK | 复用 Kafka 生态 |
| 集群互操作 | 混合集群 + 滚动迁移 | 仅纯 R-Kafka 集群 | 零停机迁移，降低采用门槛 |
| Metadata 格式 | Kafka MetadataRecord 兼容 | 自定义格式 | KRaft Quorum 可混合运行 |
| IBP 协议 | 完整实现 Broker-to-Broker API | 仅 Client API | 跨实现副本复制 |
