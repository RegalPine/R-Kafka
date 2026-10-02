//! OffsetFetch API (Key = 9)
//!
//! 消费者查询已提交的偏移量。
//! Phase 1: 从内存 OffsetManager 读取。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OffsetFetchRequest {
    /// v0-v7: 单个 group_id; v8+: groups (array)
    pub group_id: String,
    /// null = 查询全部 topic
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
    /// v8+: groups
    pub groups: Option<Vec<OffsetFetchRequestGroup>>,
    /// v7+: require_stable (bool)
    pub require_stable: bool,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchRequestTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchRequestGroup {
    pub group_id: String,
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
}

impl KafkaRequestDecoder for OffsetFetchRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 8 {
            // Flexible: multi-group
            let groups = reader.read_compact_array(|r| {
                let group_id = r.read_compact_string()?;
                let topics = if version >= 8 {
                    let arr = r.read_compact_array(|r2| {
                        let name = r2.read_compact_string()?;
                        let partitions = r2.read_compact_array(|r3| r3.read_i32())?;
                        let _tags = r2.read_tagged_fields()?;
                        Ok(OffsetFetchRequestTopic {
                            name,
                            partition_indexes: partitions,
                        })
                    })?;
                    if arr.is_empty() {
                        None
                    } else {
                        Some(arr)
                    }
                } else {
                    None
                };
                let _tags = r.read_tagged_fields()?;
                Ok(OffsetFetchRequestGroup { group_id, topics })
            })?;
            let require_stable = if version >= 7 {
                reader.read_bool()?
            } else {
                false
            };
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                group_id: String::new(),
                topics: None,
                groups: Some(groups),
                require_stable,
            })
        } else {
            // Legacy v0-v7: single group
            let group_id = reader.read_string()?;

            let topics = if version >= 8 {
                None // handled above
            } else {
                let arr = reader.read_array(|r| {
                    let name = r.read_string()?;
                    let partitions = r.read_array(|r2| r2.read_i32())?;
                    Ok(OffsetFetchRequestTopic {
                        name,
                        partition_indexes: partitions,
                    })
                })?;
                if arr.is_empty() {
                    None
                } else {
                    Some(arr)
                }
            };

            let require_stable = if version >= 7 {
                reader.read_bool()?
            } else {
                false
            };

            Ok(Self {
                group_id,
                topics,
                groups: None,
                require_stable,
            })
        }
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OffsetFetchResponse {
    /// v3+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    /// v0-v7: 单个 group 的结果
    pub topics: Vec<OffsetFetchResponseTopic>,
    /// v7+: error_code (全局)
    pub error_code: KafkaErrorCode,
    /// v8+: groups
    pub groups: Option<Vec<OffsetFetchResponseGroup>>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchResponseTopic {
    pub name: String,
    pub partitions: Vec<OffsetFetchResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchResponsePartition {
    pub index: i32,
    pub committed_offset: i64,
    /// v5+: leader_epoch (i32)
    pub leader_epoch: i32,
    pub metadata: Option<String>,
    pub error_code: KafkaErrorCode,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchResponseGroup {
    pub group_id: String,
    pub topics: Vec<OffsetFetchResponseTopic>,
    pub error_code: KafkaErrorCode,
}

impl KafkaResponseEncoder for OffsetFetchResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 3 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 8 {
            // Flexible: multi-group
            if let Some(groups) = &self.groups {
                writer.write_compact_array(groups, |w, g| {
                    w.write_compact_string(&g.group_id);
                    w.write_compact_array(&g.topics, |w2, t| {
                        w2.write_compact_string(&t.name);
                        w2.write_compact_array(&t.partitions, |w3, p| {
                            w3.write_i32(p.index);
                            w3.write_i64(p.committed_offset);
                            w3.write_i32(p.leader_epoch);
                            w3.write_compact_nullable_string(p.metadata.as_deref());
                            w3.write_i16(p.error_code.as_i16());
                            w3.write_tagged_fields(&[]);
                        });
                        w2.write_tagged_fields(&[]);
                    });
                    w.write_i16(g.error_code.as_i16());
                    w.write_tagged_fields(&[]);
                });
            } else {
                // Convert single-group to multi-group format
                writer.write_unsigned_varint(2); // array len+1 = 2
                writer.write_compact_string(""); // group_id placeholder
                writer.write_compact_array(&self.topics, |w2, t| {
                    w2.write_compact_string(&t.name);
                    w2.write_compact_array(&t.partitions, |w3, p| {
                        w3.write_i32(p.index);
                        w3.write_i64(p.committed_offset);
                        w3.write_i32(p.leader_epoch);
                        w3.write_compact_nullable_string(p.metadata.as_deref());
                        w3.write_i16(p.error_code.as_i16());
                        w3.write_tagged_fields(&[]);
                    });
                    w2.write_tagged_fields(&[]);
                });
                writer.write_i16(self.error_code.as_i16());
                writer.write_tagged_fields(&[]);
            }
        } else {
            // v0-v7: single group
            writer.write_array(&self.topics, |w, t| {
                w.write_string(&t.name);
                w.write_array(&t.partitions, |w2, p| {
                    w2.write_i32(p.index);
                    w2.write_i64(p.committed_offset);
                    if version >= 5 {
                        w2.write_i32(p.leader_epoch);
                    }
                    w2.write_nullable_string(p.metadata.as_deref());
                    w2.write_i16(p.error_code.as_i16());
                });
            });

            if version >= 7 {
                writer.write_i16(self.error_code.as_i16());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_offset_fetch_response_v0() {
        let resp = OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetFetchResponseTopic {
                name: "test".to_string(),
                partitions: vec![OffsetFetchResponsePartition {
                    index: 0,
                    committed_offset: 42,
                    leader_epoch: -1,
                    metadata: None,
                    error_code: KafkaErrorCode::None,
                }],
            }],
            error_code: KafkaErrorCode::None,
            groups: None,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 10);
    }

    #[test]
    fn test_offset_fetch_response_v5() {
        let resp = OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetFetchResponseTopic {
                name: "test".to_string(),
                partitions: vec![OffsetFetchResponsePartition {
                    index: 0,
                    committed_offset: 42,
                    leader_epoch: 5,
                    metadata: Some("meta".to_string()),
                    error_code: KafkaErrorCode::None,
                }],
            }],
            error_code: KafkaErrorCode::None,
            groups: None,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 5).unwrap();
        assert!(buf.len() > 10);
    }
}
