//! KRaft Metadata Record 编解码
//!
//! 实现与 Kafka KRaft 格式兼容的 Metadata Record 二进制编解码。
//! 每条 Record 包含: record_type (i16) + version (i16) + 数据字段。
//!
//! Record 类型 (与 Kafka Metadata Log 对齐):
//! - 0: HeaderRecord (Metadata Log 头部)
//! - 2: TopicRecord (Topic 创建)
//! - 3: PartitionRecord (Partition 分配)
//! - 4: ConfigRecord (配置变更)
//! - 12: BrokerRecord (Broker 注册)
//! - 13: FeatureRecord (Feature 版本)
//! - 15: PartitionChangeRecord (Partition 变更)
//!
//! 编码格式 (简化版 Kafka wire protocol):
//! - i16: 2 bytes big-endian
//! - i32: 4 bytes big-endian
//! - i64: 8 bytes big-endian
//! - u128: 16 bytes big-endian
//! - String: i32 length + UTF-8 bytes (-1 = null)
//! - Vec<T>: i32 count + elements
//! - bool: 1 byte (0/1)

use std::io::{self, Cursor, Read, Write};

use serde::{Deserialize, Serialize};

// ─── Record 类型常量 ──────────────────────────────────────────────────

/// Metadata Record 类型标识
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i16)]
pub enum RecordType {
    /// Metadata Log 头部 (type=0)
    Header = 0,
    /// Topic 创建 (type=2)
    Topic = 2,
    /// Partition 分配 (type=3)
    Partition = 3,
    /// 配置变更 (type=4)
    Config = 4,
    /// Topic 删除 (type=11)
    RemoveTopic = 11,
    /// Broker 注册 (type=12)
    Broker = 12,
    /// Feature 版本 (type=13)
    Feature = 13,
    /// Partition 变更 (type=15)
    PartitionChange = 15,
    /// Broker 取消围栏 (type=16)
    UnfenceBroker = 16,
    /// Producer IDs (type=17)
    ProducerIds = 17,
    /// 访问控制 / ACL (type=19)
    AccessControl = 19,
    /// Broker 围栏 (type=23)
    FenceBroker = 23,
}

impl RecordType {
    /// 从 i16 解析
    pub fn from_i16(v: i16) -> Option<Self> {
        match v {
            0 => Some(Self::Header),
            2 => Some(Self::Topic),
            3 => Some(Self::Partition),
            4 => Some(Self::Config),
            11 => Some(Self::RemoveTopic),
            12 => Some(Self::Broker),
            13 => Some(Self::Feature),
            15 => Some(Self::PartitionChange),
            16 => Some(Self::UnfenceBroker),
            17 => Some(Self::ProducerIds),
            19 => Some(Self::AccessControl),
            23 => Some(Self::FenceBroker),
            _ => None,
        }
    }
}

// ─── Metadata Record 枚举 ────────────────────────────────────────────

/// KRaft Metadata Record
///
/// 每条 Record 对应 Metadata Log 中的一个条目，编码后追加到日志。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataRecord {
    /// 头部记录 (type=0, version=0)
    Header {
        /// Metadata Log 版本
        version: i32,
    },
    /// Topic 创建 (type=2)
    Topic {
        /// Topic UUID
        topic_id: u128,
        /// Topic 名称
        name: String,
    },
    /// Partition 分配 (type=3)
    Partition {
        /// Partition UUID
        partition_id: u128,
        /// Topic UUID
        topic_id: u128,
        /// Partition 编号
        partition_index: i32,
        /// 副本列表
        replicas: Vec<i32>,
        /// ISR 列表
        isr: Vec<i32>,
        /// 是否已移除 leader
        removing_replicas: Vec<i32>,
        /// 正在添加的副本
        adding_replicas: Vec<i32>,
        /// Leader Broker ID
        leader: i32,
        /// Leader Epoch
        leader_epoch: i32,
    },
    /// 配置变更 (type=4)
    Config {
        /// 资源类型 (0=unknown, 1=topic, 2=broker)
        resource_type: i8,
        /// 资源名称
        resource_name: String,
        /// 配置键
        name: String,
        /// 配置值
        value: Option<String>,
    },
    /// Broker 注册 (type=12)
    Broker {
        /// Broker ID
        broker_id: i32,
        /// 主机地址
        host: String,
        /// 端口
        port: u16,
        /// 机架标识
        rack: Option<String>,
        /// Broker 纪元
        broker_epoch: i64,
    },
    /// Feature 版本 (type=13)
    Feature {
        /// Feature 名称
        name: String,
        /// 最低版本
        min_version: i16,
        /// 最大版本
        max_version: i16,
    },
    /// Partition 变更 (type=15)
    PartitionChange {
        /// Partition UUID
        partition_id: u128,
        /// Topic UUID
        topic_id: u128,
        /// ISR 列表
        isr: Vec<i32>,
        /// Leader Broker ID
        leader: i32,
        /// Leader Epoch
        leader_epoch: i32,
    },
    /// Topic 删除 (type=11)
    RemoveTopic {
        /// Topic UUID
        topic_id: u128,
    },
    /// Broker 取消围栏 (type=16)
    UnfenceBroker {
        /// Broker ID
        broker_id: i32,
        /// Broker 纪元
        broker_epoch: i64,
    },
    /// Producer IDs 分配 (type=17)
    ProducerIds {
        /// 已分配的最大 Producer ID
        next_id: i64,
    },
    /// 访问控制 / ACL (type=19)
    AccessControl {
        /// ACL 资源类型 (1=topic, 2=group, 3=cluster, 4=transactional_id)
        resource_type: i8,
        /// 资源名称
        resource_name: String,
        /// 资源匹配模式 (3=literal, 4=prefixed)
        pattern_type: i8,
        /// 主体 (如 "User:alice")
        principal: String,
        /// 主机 ("*" 表示任意)
        host: String,
        /// 操作类型 (2=read, 3=write, 4=create, ...)
        operation: i8,
        /// 权限类型 (2=allow, 3=deny)
        permission_type: i8,
    },
    /// Broker 围栏 (type=23)
    FenceBroker {
        /// Broker ID
        broker_id: i32,
        /// Broker 纪元
        broker_epoch: i64,
    },
}

impl MetadataRecord {
    /// 获取 Record 类型
    pub fn record_type(&self) -> RecordType {
        match self {
            Self::Header { .. } => RecordType::Header,
            Self::Topic { .. } => RecordType::Topic,
            Self::Partition { .. } => RecordType::Partition,
            Self::Config { .. } => RecordType::Config,
            Self::Broker { .. } => RecordType::Broker,
            Self::Feature { .. } => RecordType::Feature,
            Self::PartitionChange { .. } => RecordType::PartitionChange,
            Self::RemoveTopic { .. } => RecordType::RemoveTopic,
            Self::UnfenceBroker { .. } => RecordType::UnfenceBroker,
            Self::ProducerIds { .. } => RecordType::ProducerIds,
            Self::AccessControl { .. } => RecordType::AccessControl,
            Self::FenceBroker { .. } => RecordType::FenceBroker,
        }
    }

    /// 获取 Record 版本号
    pub fn version(&self) -> i16 {
        0 // 所有 record 初始版本 0
    }

    /// 编码为字节
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        self.encode_to(&mut buf);
        buf
    }

    /// 编码到 writer
    pub fn encode_to<W: Write>(&self, w: &mut W) {
        // 写入 header: record_type (i16) + version (i16)
        w.write_all(&(self.record_type() as i16).to_be_bytes())
            .unwrap();
        w.write_all(&self.version().to_be_bytes()).unwrap();

        // 写入 body
        match self {
            Self::Header { version } => {
                w.write_all(&version.to_be_bytes()).unwrap();
            }
            Self::Topic { topic_id, name } => {
                w.write_all(&topic_id.to_be_bytes()).unwrap();
                encode_string(w, name);
            }
            Self::Partition {
                partition_id,
                topic_id,
                partition_index,
                replicas,
                isr,
                removing_replicas,
                adding_replicas,
                leader,
                leader_epoch,
            } => {
                w.write_all(&partition_id.to_be_bytes()).unwrap();
                w.write_all(&topic_id.to_be_bytes()).unwrap();
                w.write_all(&partition_index.to_be_bytes()).unwrap();
                encode_i32_vec(w, replicas);
                encode_i32_vec(w, isr);
                encode_i32_vec(w, removing_replicas);
                encode_i32_vec(w, adding_replicas);
                w.write_all(&leader.to_be_bytes()).unwrap();
                w.write_all(&leader_epoch.to_be_bytes()).unwrap();
            }
            Self::Config {
                resource_type,
                resource_name,
                name,
                value,
            } => {
                w.write_all(&[*resource_type as u8]).unwrap();
                encode_string(w, resource_name);
                encode_string(w, name);
                encode_nullable_string(w, value.as_deref());
            }
            Self::Broker {
                broker_id,
                host,
                port,
                rack,
                broker_epoch,
            } => {
                w.write_all(&broker_id.to_be_bytes()).unwrap();
                encode_string(w, host);
                w.write_all(&port.to_be_bytes()).unwrap();
                encode_nullable_string(w, rack.as_deref());
                w.write_all(&broker_epoch.to_be_bytes()).unwrap();
            }
            Self::Feature {
                name,
                min_version,
                max_version,
            } => {
                encode_string(w, name);
                w.write_all(&min_version.to_be_bytes()).unwrap();
                w.write_all(&max_version.to_be_bytes()).unwrap();
            }
            Self::PartitionChange {
                partition_id,
                topic_id,
                isr,
                leader,
                leader_epoch,
            } => {
                w.write_all(&partition_id.to_be_bytes()).unwrap();
                w.write_all(&topic_id.to_be_bytes()).unwrap();
                encode_i32_vec(w, isr);
                w.write_all(&leader.to_be_bytes()).unwrap();
                w.write_all(&leader_epoch.to_be_bytes()).unwrap();
            }
            Self::RemoveTopic { topic_id } => {
                w.write_all(&topic_id.to_be_bytes()).unwrap();
            }
            Self::UnfenceBroker {
                broker_id,
                broker_epoch,
            } => {
                w.write_all(&broker_id.to_be_bytes()).unwrap();
                w.write_all(&broker_epoch.to_be_bytes()).unwrap();
            }
            Self::ProducerIds { next_id } => {
                w.write_all(&next_id.to_be_bytes()).unwrap();
            }
            Self::AccessControl {
                resource_type,
                resource_name,
                pattern_type,
                principal,
                host,
                operation,
                permission_type,
            } => {
                w.write_all(&[*resource_type as u8]).unwrap();
                encode_string(w, resource_name);
                w.write_all(&[*pattern_type as u8]).unwrap();
                encode_string(w, principal);
                encode_string(w, host);
                w.write_all(&[*operation as u8]).unwrap();
                w.write_all(&[*permission_type as u8]).unwrap();
            }
            Self::FenceBroker {
                broker_id,
                broker_epoch,
            } => {
                w.write_all(&broker_id.to_be_bytes()).unwrap();
                w.write_all(&broker_epoch.to_be_bytes()).unwrap();
            }
        }
    }

    /// 从字节解码
    pub fn decode(data: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        Self::decode_from(&mut cursor)
    }

    /// 从 reader 解码
    pub fn decode_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let record_type = read_i16(r)?;
        let _version = read_i16(r)?;

        let rt = RecordType::from_i16(record_type).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown record type: {}", record_type),
            )
        })?;

        match rt {
            RecordType::Header => {
                let version = read_i32(r)?;
                Ok(Self::Header { version })
            }
            RecordType::Topic => {
                let topic_id = read_u128(r)?;
                let name = read_string(r)?;
                Ok(Self::Topic { topic_id, name })
            }
            RecordType::Partition => {
                let partition_id = read_u128(r)?;
                let topic_id = read_u128(r)?;
                let partition_index = read_i32(r)?;
                let replicas = read_i32_vec(r)?;
                let isr = read_i32_vec(r)?;
                let removing_replicas = read_i32_vec(r)?;
                let adding_replicas = read_i32_vec(r)?;
                let leader = read_i32(r)?;
                let leader_epoch = read_i32(r)?;
                Ok(Self::Partition {
                    partition_id,
                    topic_id,
                    partition_index,
                    replicas,
                    isr,
                    removing_replicas,
                    adding_replicas,
                    leader,
                    leader_epoch,
                })
            }
            RecordType::Config => {
                let mut rt_buf = [0u8; 1];
                r.read_exact(&mut rt_buf)?;
                let resource_type = rt_buf[0] as i8;
                let resource_name = read_string(r)?;
                let name = read_string(r)?;
                let value = read_nullable_string(r)?;
                Ok(Self::Config {
                    resource_type,
                    resource_name,
                    name,
                    value,
                })
            }
            RecordType::Broker => {
                let broker_id = read_i32(r)?;
                let host = read_string(r)?;
                let port = read_u16(r)?;
                let rack = read_nullable_string(r)?;
                let broker_epoch = read_i64(r)?;
                Ok(Self::Broker {
                    broker_id,
                    host,
                    port,
                    rack,
                    broker_epoch,
                })
            }
            RecordType::Feature => {
                let name = read_string(r)?;
                let min_version = read_i16(r)?;
                let max_version = read_i16(r)?;
                Ok(Self::Feature {
                    name,
                    min_version,
                    max_version,
                })
            }
            RecordType::PartitionChange => {
                let partition_id = read_u128(r)?;
                let topic_id = read_u128(r)?;
                let isr = read_i32_vec(r)?;
                let leader = read_i32(r)?;
                let leader_epoch = read_i32(r)?;
                Ok(Self::PartitionChange {
                    partition_id,
                    topic_id,
                    isr,
                    leader,
                    leader_epoch,
                })
            }
            RecordType::RemoveTopic => {
                let topic_id = read_u128(r)?;
                Ok(Self::RemoveTopic { topic_id })
            }
            RecordType::UnfenceBroker => {
                let broker_id = read_i32(r)?;
                let broker_epoch = read_i64(r)?;
                Ok(Self::UnfenceBroker {
                    broker_id,
                    broker_epoch,
                })
            }
            RecordType::ProducerIds => {
                let next_id = read_i64(r)?;
                Ok(Self::ProducerIds { next_id })
            }
            RecordType::AccessControl => {
                let mut rt_buf = [0u8; 1];
                r.read_exact(&mut rt_buf)?;
                let resource_type = rt_buf[0] as i8;
                let resource_name = read_string(r)?;
                let mut pt_buf = [0u8; 1];
                r.read_exact(&mut pt_buf)?;
                let pattern_type = pt_buf[0] as i8;
                let principal = read_string(r)?;
                let host = read_string(r)?;
                let mut op_buf = [0u8; 1];
                r.read_exact(&mut op_buf)?;
                let operation = op_buf[0] as i8;
                let mut perm_buf = [0u8; 1];
                r.read_exact(&mut perm_buf)?;
                let permission_type = perm_buf[0] as i8;
                Ok(Self::AccessControl {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                })
            }
            RecordType::FenceBroker => {
                let broker_id = read_i32(r)?;
                let broker_epoch = read_i64(r)?;
                Ok(Self::FenceBroker {
                    broker_id,
                    broker_epoch,
                })
            }
        }
    }
}

// ─── 编解码辅助函数 ──────────────────────────────────────────────────

fn encode_string<W: Write>(w: &mut W, s: &str) {
    let bytes = s.as_bytes();
    w.write_all(&(bytes.len() as i32).to_be_bytes()).unwrap();
    w.write_all(bytes).unwrap();
}

fn encode_nullable_string<W: Write>(w: &mut W, s: Option<&str>) {
    match s {
        Some(val) => encode_string(w, val),
        None => w.write_all(&(-1i32).to_be_bytes()).unwrap(),
    }
}

fn encode_i32_vec<W: Write>(w: &mut W, v: &[i32]) {
    w.write_all(&(v.len() as i32).to_be_bytes()).unwrap();
    for &item in v {
        w.write_all(&item.to_be_bytes()).unwrap();
    }
}

fn read_i16<R: Read>(r: &mut R) -> io::Result<i16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf)?;
    Ok(i16::from_be_bytes(buf))
}

fn read_i32<R: Read>(r: &mut R) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_i64<R: Read>(r: &mut R) -> io::Result<i64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(i64::from_be_bytes(buf))
}

fn read_u16<R: Read>(r: &mut R) -> io::Result<u16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

fn read_u128<R: Read>(r: &mut R) -> io::Result<u128> {
    let mut buf = [0u8; 16];
    r.read_exact(&mut buf)?;
    Ok(u128::from_be_bytes(buf))
}

fn read_string<R: Read>(r: &mut R) -> io::Result<String> {
    let len = read_i32(r)?;
    if len < 0 {
        return Ok(String::new());
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_nullable_string<R: Read>(r: &mut R) -> io::Result<Option<String>> {
    let len = read_i32(r)?;
    if len < 0 {
        return Ok(None);
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    let s = String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(s))
}

fn read_i32_vec<R: Read>(r: &mut R) -> io::Result<Vec<i32>> {
    let len = read_i32(r)?;
    if len < 0 {
        return Ok(Vec::new());
    }
    let mut v = Vec::with_capacity(len as usize);
    for _ in 0..len {
        v.push(read_i32(r)?);
    }
    Ok(v)
}

// ─── MetadataLog — 追加式 Metadata 日志 ──────────────────────────────

/// Metadata 日志条目 (带偏移量)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataLogEntry {
    /// 日志偏移量 (单调递增)
    pub offset: u64,
    /// Metadata Record
    pub record: MetadataRecord,
}

/// 追加式 Metadata 日志
///
/// 存储所有 Metadata Record 的有序序列。
/// 支持回放 (replay) 重建 MetadataStateMachine 状态。
pub struct MetadataLog {
    /// 日志条目
    entries: Vec<MetadataLogEntry>,
    /// 下一个偏移量
    next_offset: u64,
}

impl MetadataLog {
    /// 创建空的 Metadata 日志
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_offset: 0,
        }
    }

    /// 追加一条 Record
    pub fn append(&mut self, record: MetadataRecord) -> u64 {
        let offset = self.next_offset;
        self.entries.push(MetadataLogEntry { offset, record });
        self.next_offset += 1;
        offset
    }

    /// 获取日志条目数
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 获取指定偏移量的条目
    pub fn get(&self, offset: u64) -> Option<&MetadataLogEntry> {
        self.entries.get(offset as usize)
    }

    /// 获取所有条目
    pub fn entries(&self) -> &[MetadataLogEntry] {
        &self.entries
    }

    /// 获取下一个偏移量
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// 编码全部日志为字节
    pub fn encode_all(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.entries.len() * 128);
        for entry in &self.entries {
            // 每条记录: offset (u64) + record_len (i32) + record_bytes
            buf.extend_from_slice(&entry.offset.to_be_bytes());
            let record_bytes = entry.record.encode();
            buf.extend_from_slice(&(record_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(&record_bytes);
        }
        buf
    }

    /// 从字节解码恢复日志
    pub fn decode_all(data: &[u8]) -> io::Result<Self> {
        let mut log = Self::new();
        let mut cursor = Cursor::new(data);

        while (cursor.position() as usize) < data.len() {
            let offset = {
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                u64::from_be_bytes(buf)
            };

            let record_len = {
                let mut buf = [0u8; 4];
                cursor.read_exact(&mut buf)?;
                i32::from_be_bytes(buf) as usize
            };

            let mut record_buf = vec![0u8; record_len];
            cursor.read_exact(&mut record_buf)?;

            let record = MetadataRecord::decode(&record_buf)?;
            log.entries.push(MetadataLogEntry { offset, record });
            log.next_offset = offset + 1;
        }

        Ok(log)
    }

    /// 回放到 MetadataStateMachine
    pub fn replay(&self, sm: &crate::metadata_sm::MetadataStateMachine) {
        use crate::raft_node::LogEntry;

        for entry in &self.entries {
            let log_entry = match &entry.record {
                MetadataRecord::Header { .. } => continue, // 跳过 header
                MetadataRecord::Topic { topic_id, name } => {
                    LogEntry::CreateTopic {
                        topic_name: name.clone(),
                        topic_id: *topic_id,
                        partitions: 0, // 后续由 PartitionRecord 填充
                        replication_factor: 0,
                        configs: vec![],
                    }
                }
                MetadataRecord::Partition {
                    topic_id,
                    partition_index,
                    replicas: _,
                    isr,
                    leader,
                    leader_epoch,
                    ..
                } => {
                    // 需要查找 topic name (简化: 使用 topic_id 的字符串形式)
                    LogEntry::UpdateLeaderAndIsr {
                        topic_name: format!("topic-{}", topic_id),
                        partition_id: *partition_index,
                        leader: *leader,
                        epoch: *leader_epoch,
                        isr: isr.clone(),
                    }
                }
                MetadataRecord::Config {
                    resource_type,
                    resource_name,
                    name,
                    value,
                } => LogEntry::SetConfig {
                    resource_type: format!("{}", resource_type),
                    resource_name: resource_name.clone(),
                    key: name.clone(),
                    value: value.clone().unwrap_or_default(),
                },
                MetadataRecord::Broker {
                    broker_id,
                    host,
                    port,
                    rack,
                    ..
                } => LogEntry::RegisterBroker {
                    broker_id: *broker_id,
                    rack: rack.clone(),
                    host: host.clone(),
                    port: *port,
                },
                MetadataRecord::Feature {
                    name, max_version, ..
                } => LogEntry::UpdateFeature {
                    name: name.clone(),
                    version: *max_version as u16,
                },
                MetadataRecord::PartitionChange {
                    isr,
                    leader,
                    leader_epoch,
                    ..
                } => {
                    LogEntry::UpdateLeaderAndIsr {
                        topic_name: String::new(), // 简化
                        partition_id: 0,
                        leader: *leader,
                        epoch: *leader_epoch,
                        isr: isr.clone(),
                    }
                }
                // 新增类型: 在 replay 中作为独立操作处理
                MetadataRecord::RemoveTopic { topic_id } => LogEntry::DeleteTopic {
                    topic_name: format!("topic-{}", topic_id),
                },
                MetadataRecord::UnfenceBroker { broker_id, .. } => LogEntry::RegisterBroker {
                    broker_id: *broker_id,
                    rack: None,
                    host: String::new(),
                    port: 0,
                },
                MetadataRecord::ProducerIds { .. } => {
                    // ProducerIds 不需要应用到状态机
                    continue;
                }
                MetadataRecord::AccessControl { .. } => {
                    // ACL 变更暂不通过 replay 处理
                    continue;
                }
                MetadataRecord::FenceBroker { broker_id, .. } => LogEntry::RegisterBroker {
                    broker_id: *broker_id,
                    rack: None,
                    host: String::new(),
                    port: 0,
                },
            };

            sm.apply(entry.offset, &log_entry);
        }
    }
}

impl Default for MetadataLog {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Record 类型测试 ─────────────────────────────────────────────

    #[test]
    fn test_record_type_from_i16() {
        assert_eq!(RecordType::from_i16(0), Some(RecordType::Header));
        assert_eq!(RecordType::from_i16(2), Some(RecordType::Topic));
        assert_eq!(RecordType::from_i16(3), Some(RecordType::Partition));
        assert_eq!(RecordType::from_i16(4), Some(RecordType::Config));
        assert_eq!(RecordType::from_i16(11), Some(RecordType::RemoveTopic));
        assert_eq!(RecordType::from_i16(12), Some(RecordType::Broker));
        assert_eq!(RecordType::from_i16(13), Some(RecordType::Feature));
        assert_eq!(RecordType::from_i16(15), Some(RecordType::PartitionChange));
        assert_eq!(RecordType::from_i16(16), Some(RecordType::UnfenceBroker));
        assert_eq!(RecordType::from_i16(17), Some(RecordType::ProducerIds));
        assert_eq!(RecordType::from_i16(19), Some(RecordType::AccessControl));
        assert_eq!(RecordType::from_i16(23), Some(RecordType::FenceBroker));
        assert_eq!(RecordType::from_i16(99), None);
    }

    // ─── Roundtrip 编解码测试 ────────────────────────────────────────

    #[test]
    fn test_header_roundtrip() {
        let record = MetadataRecord::Header { version: 1 };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_topic_roundtrip() {
        let record = MetadataRecord::Topic {
            topic_id: 0x1234_5678_9ABC_DEF0_1234_5678_9ABC_DEF0,
            name: "my-topic".to_string(),
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_partition_roundtrip() {
        let record = MetadataRecord::Partition {
            partition_id: 100,
            topic_id: 200,
            partition_index: 3,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            removing_replicas: vec![],
            adding_replicas: vec![4],
            leader: 1,
            leader_epoch: 5,
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_config_roundtrip() {
        let record = MetadataRecord::Config {
            resource_type: 1,
            resource_name: "my-topic".to_string(),
            name: "retention.ms".to_string(),
            value: Some("86400000".to_string()),
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_config_null_value_roundtrip() {
        let record = MetadataRecord::Config {
            resource_type: 2,
            resource_name: "1".to_string(),
            name: "log.dirs".to_string(),
            value: None,
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_broker_roundtrip() {
        let record = MetadataRecord::Broker {
            broker_id: 1,
            host: "192.168.1.1".to_string(),
            port: 9092,
            rack: Some("rack-a".to_string()),
            broker_epoch: 42,
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_broker_null_rack_roundtrip() {
        let record = MetadataRecord::Broker {
            broker_id: 2,
            host: "10.0.0.1".to_string(),
            port: 9093,
            rack: None,
            broker_epoch: 100,
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_feature_roundtrip() {
        let record = MetadataRecord::Feature {
            name: "metadata.version".to_string(),
            min_version: 0,
            max_version: 17,
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_partition_change_roundtrip() {
        let record = MetadataRecord::PartitionChange {
            partition_id: 100,
            topic_id: 200,
            isr: vec![2, 3],
            leader: 2,
            leader_epoch: 10,
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    // ─── 错误测试 ────────────────────────────────────────────────────

    #[test]
    fn test_decode_invalid_type() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&99i16.to_be_bytes()); // invalid type
        buf.extend_from_slice(&0i16.to_be_bytes()); // version
        let result = MetadataRecord::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_truncated_data() {
        let buf = vec![0u8; 2]; // 只有 type, 没有 version
        let result = MetadataRecord::decode(&buf);
        assert!(result.is_err());
    }

    // ─── MetadataLog 测试 ────────────────────────────────────────────

    #[test]
    fn test_metadata_log_new() {
        let log = MetadataLog::new();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert_eq!(log.next_offset(), 0);
    }

    #[test]
    fn test_metadata_log_append() {
        let mut log = MetadataLog::new();

        let offset0 = log.append(MetadataRecord::Header { version: 1 });
        assert_eq!(offset0, 0);

        let offset1 = log.append(MetadataRecord::Topic {
            topic_id: 42,
            name: "test".to_string(),
        });
        assert_eq!(offset1, 1);

        assert_eq!(log.len(), 2);
        assert_eq!(log.next_offset(), 2);
    }

    #[test]
    fn test_metadata_log_get() {
        let mut log = MetadataLog::new();
        log.append(MetadataRecord::Header { version: 1 });
        log.append(MetadataRecord::Topic {
            topic_id: 42,
            name: "test".to_string(),
        });

        let entry = log.get(0).unwrap();
        assert_eq!(entry.offset, 0);
        assert!(matches!(entry.record, MetadataRecord::Header { .. }));

        let entry = log.get(1).unwrap();
        assert_eq!(entry.offset, 1);
        assert!(matches!(entry.record, MetadataRecord::Topic { .. }));

        assert!(log.get(99).is_none());
    }

    #[test]
    fn test_metadata_log_encode_decode_all() {
        let mut log = MetadataLog::new();
        log.append(MetadataRecord::Header { version: 1 });
        log.append(MetadataRecord::Topic {
            topic_id: 42,
            name: "test-topic".to_string(),
        });
        log.append(MetadataRecord::Broker {
            broker_id: 1,
            host: "127.0.0.1".to_string(),
            port: 9092,
            rack: Some("rack-a".to_string()),
            broker_epoch: 100,
        });
        log.append(MetadataRecord::Config {
            resource_type: 1,
            resource_name: "test-topic".to_string(),
            name: "retention.ms".to_string(),
            value: Some("86400000".to_string()),
        });
        log.append(MetadataRecord::Feature {
            name: "metadata.version".to_string(),
            min_version: 0,
            max_version: 17,
        });

        let encoded = log.encode_all();
        let decoded = MetadataLog::decode_all(&encoded).unwrap();

        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded.next_offset(), 5);

        // 验证每条记录
        assert_eq!(decoded.get(0).unwrap().record, log.get(0).unwrap().record);
        assert_eq!(decoded.get(1).unwrap().record, log.get(1).unwrap().record);
        assert_eq!(decoded.get(2).unwrap().record, log.get(2).unwrap().record);
        assert_eq!(decoded.get(3).unwrap().record, log.get(3).unwrap().record);
        assert_eq!(decoded.get(4).unwrap().record, log.get(4).unwrap().record);
    }

    #[test]
    fn test_metadata_log_empty_encode_decode() {
        let log = MetadataLog::new();
        let encoded = log.encode_all();
        assert!(encoded.is_empty());

        let decoded = MetadataLog::decode_all(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_record_type_method() {
        assert_eq!(
            MetadataRecord::Header { version: 0 }.record_type(),
            RecordType::Header
        );
        assert_eq!(
            MetadataRecord::Topic {
                topic_id: 0,
                name: String::new()
            }
            .record_type(),
            RecordType::Topic
        );
        assert_eq!(
            MetadataRecord::Broker {
                broker_id: 0,
                host: String::new(),
                port: 0,
                rack: None,
                broker_epoch: 0
            }
            .record_type(),
            RecordType::Broker
        );
    }

    #[test]
    fn test_record_version() {
        assert_eq!(MetadataRecord::Header { version: 0 }.version(), 0);
        assert_eq!(
            MetadataRecord::Topic {
                topic_id: 0,
                name: String::new()
            }
            .version(),
            0
        );
    }

    #[test]
    fn test_metadata_log_entries_slice() {
        let mut log = MetadataLog::new();
        log.append(MetadataRecord::Header { version: 1 });
        log.append(MetadataRecord::Header { version: 2 });

        let entries = log.entries();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_metadata_log_default() {
        let log = MetadataLog::default();
        assert!(log.is_empty());
    }

    #[test]
    fn test_encode_string_empty() {
        let record = MetadataRecord::Topic {
            topic_id: 0,
            name: String::new(),
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_encode_string_unicode() {
        let record = MetadataRecord::Topic {
            topic_id: 42,
            name: "测试-topic-日本語".to_string(),
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_large_replica_list() {
        let replicas: Vec<i32> = (0..100).collect();
        let record = MetadataRecord::Partition {
            partition_id: 1,
            topic_id: 2,
            partition_index: 0,
            replicas: replicas.clone(),
            isr: replicas.clone(),
            removing_replicas: vec![],
            adding_replicas: vec![],
            leader: 0,
            leader_epoch: 0,
        };
        let encoded = record.encode();
        let decoded = MetadataRecord::decode(&encoded).unwrap();
        assert_eq!(record, decoded);
    }
}
