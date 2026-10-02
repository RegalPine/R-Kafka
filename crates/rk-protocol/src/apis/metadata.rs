//! Metadata API (Key = 3)
//!
//! 实现 MetadataRequest / MetadataResponse，支持 v0-v13。
//! v12+ 新增 NodeEndpoints 数组 (KRaft 模式节点端点)。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MetadataRequest {
    /// null = 请求全部 topics (v1+); v0 空数组 = 无 topics
    pub topics: Option<Vec<String>>,
    /// v4+: allow_auto_topic_creation
    pub allow_auto_topic_creation: bool,
}

impl KafkaRequestDecoder for MetadataRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        let is_flex = version >= 9;

        let topics = if is_flex {
            reader.read_compact_nullable_array(|r| r.read_compact_string())?
        } else if version >= 1 {
            // v1+: nullable array
            let len = reader.read_i32()?;
            if len < 0 {
                None
            } else {
                let mut v = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    v.push(reader.read_string()?);
                }
                Some(v)
            }
        } else {
            // v0: non-nullable array
            let items = reader.read_array(|r| r.read_string())?;
            Some(items)
        };

        let allow_auto_topic_creation = if version >= 4 {
            reader.read_bool()?
        } else {
            true
        };

        if is_flex {
            let _ = reader.read_tagged_fields()?;
        }

        Ok(Self {
            topics,
            allow_auto_topic_creation,
        })
    }
}

// ─── Response ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MetadataBroker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    /// v1+: rack
    pub rack: Option<String>,
}

/// v12+: NodeEndpoint — KRaft 模式下的节点端点信息
///
/// 在 KRaft 模式中，每个节点可能有多个监听器 (INTERNAL, EXTERNAL, etc.)。
/// NodeEndpoint 提供 node_id + listener_name → host:port 的映射。
#[derive(Debug, Clone)]
pub struct NodeEndpoint {
    /// 节点 ID
    pub node_id: i32,
    /// 主机地址
    pub host: String,
    /// 端口
    pub port: i32,
    /// 监听器名称 (e.g., "PLAINTEXT", "SSL", "INTERNAL")
    pub listener_name: String,
}

#[derive(Debug, Clone)]
pub struct MetadataPartition {
    pub error_code: KafkaErrorCode,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct MetadataTopic {
    pub error_code: KafkaErrorCode,
    pub name: String,
    /// v1+: is_internal
    pub is_internal: bool,
    pub partitions: Vec<MetadataPartition>,
}

#[derive(Debug, Clone)]
pub struct MetadataResponse {
    /// v3+: throttle_time_ms
    pub throttle_time_ms: i32,
    pub brokers: Vec<MetadataBroker>,
    /// v2+: cluster_id
    pub cluster_id: Option<String>,
    /// v1+: controller_id
    pub controller_id: i32,
    pub topics: Vec<MetadataTopic>,
    /// v12+: node_endpoints (KRaft 模式)
    pub node_endpoints: Vec<NodeEndpoint>,
}

impl MetadataResponse {
    /// 从 Broker 列表生成默认的 NodeEndpoints (v12+)
    ///
    /// 为每个 broker 生成一个 PLAINTEXT 监听器端点。
    pub fn default_node_endpoints(brokers: &[MetadataBroker]) -> Vec<NodeEndpoint> {
        brokers
            .iter()
            .map(|b| NodeEndpoint {
                node_id: b.node_id,
                host: b.host.clone(),
                port: b.port,
                listener_name: "PLAINTEXT".to_string(),
            })
            .collect()
    }
}

impl KafkaResponseEncoder for MetadataResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        let is_flex = version >= 9;

        if version >= 3 {
            writer.write_i32(self.throttle_time_ms);
        }

        // Brokers
        if is_flex {
            writer.write_compact_array(&self.brokers, |w, b| {
                w.write_i32(b.node_id);
                w.write_compact_string(&b.host);
                w.write_i32(b.port);
                if version >= 1 {
                    w.write_compact_nullable_string(b.rack.as_deref());
                }
                w.write_tagged_fields(&[]);
            });
        } else {
            writer.write_array(&self.brokers, |w, b| {
                w.write_i32(b.node_id);
                w.write_string(&b.host);
                w.write_i32(b.port);
                if version >= 1 {
                    w.write_nullable_string(b.rack.as_deref());
                }
            });
        }

        // Cluster ID
        if version >= 2 {
            if is_flex {
                writer.write_compact_nullable_string(self.cluster_id.as_deref());
            } else {
                writer.write_nullable_string(self.cluster_id.as_deref());
            }
        }

        // Controller ID
        if version >= 1 {
            writer.write_i32(self.controller_id);
        }

        // Topics
        if is_flex {
            writer.write_compact_array(&self.topics, |w, t| {
                w.write_i16(t.error_code.as_i16());
                w.write_compact_string(&t.name);
                if version >= 1 {
                    w.write_bool(t.is_internal);
                }
                w.write_compact_array(&t.partitions, |w, p| {
                    w.write_i16(p.error_code.as_i16());
                    w.write_i32(p.partition_index);
                    w.write_i32(p.leader_id);
                    // leader_epoch (v7+)
                    if version >= 7 {
                        w.write_i32(p.leader_epoch);
                    }
                    w.write_compact_array(&p.replica_nodes, |w, n| w.write_i32(*n));
                    w.write_compact_array(&p.isr_nodes, |w, n| w.write_i32(*n));
                    // v12+: offline_replicas
                    if version >= 12 {
                        // 当前无 offline replicas
                        w.write_compact_array::<i32, _>(&[], |w, n| w.write_i32(*n));
                    }
                    w.write_tagged_fields(&[]);
                });
                // v12+: topic_authorized_operations
                if version >= 12 {
                    w.write_i32(-2147483648); // null (int32 min) = unknown
                }
                w.write_tagged_fields(&[]);
            });
        } else {
            writer.write_array(&self.topics, |w, t| {
                w.write_i16(t.error_code.as_i16());
                w.write_string(&t.name);
                if version >= 1 {
                    w.write_bool(t.is_internal);
                }
                w.write_array(&t.partitions, |w, p| {
                    w.write_i16(p.error_code.as_i16());
                    w.write_i32(p.partition_index);
                    w.write_i32(p.leader_id);
                    if version >= 7 {
                        w.write_i32(p.leader_epoch);
                    }
                    w.write_array(&p.replica_nodes, |w, n| w.write_i32(*n));
                    w.write_array(&p.isr_nodes, |w, n| w.write_i32(*n));
                    // v12+: offline_replicas (non-flex still uses array)
                    if version >= 12 {
                        w.write_array::<i32, _>(&[], |w, n| w.write_i32(*n));
                    }
                });
                // v12+: topic_authorized_operations
                if version >= 12 {
                    w.write_i32(-2147483648);
                }
            });
        }

        // v12+: NodeEndpoints (KRaft mode)
        if version >= 12 {
            if is_flex {
                writer.write_compact_array(&self.node_endpoints, |w, ep| {
                    w.write_i32(ep.node_id);
                    w.write_compact_string(&ep.host);
                    w.write_i32(ep.port);
                    w.write_compact_string(&ep.listener_name);
                    w.write_tagged_fields(&[]);
                });
            } else {
                // v12 is always flex (>= 9), but keep for safety
                writer.write_array(&self.node_endpoints, |w, ep| {
                    w.write_i32(ep.node_id);
                    w.write_string(&ep.host);
                    w.write_i32(ep.port);
                    w.write_string(&ep.listener_name);
                });
            }
        }

        Ok(())
    }
}
