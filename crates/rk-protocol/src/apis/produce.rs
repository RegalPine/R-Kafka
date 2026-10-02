//! Produce API (Key = 0)
//!
//! 实现 ProduceRequest / ProduceResponse，支持 v0-v10。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ProduceRequestTopic {
    pub name: String,
    pub partitions: Vec<ProduceRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct ProduceRequestPartition {
    pub index: i32,
    /// Raw RecordBatch bytes (包含 4 字节长度前缀)
    pub record_set: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    /// v3+: nullable transactional_id
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceRequestTopic>,
}

impl KafkaRequestDecoder for ProduceRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        let is_flex = version >= 9;

        let transactional_id = if version >= 3 {
            if is_flex {
                reader.read_compact_nullable_string()?
            } else {
                reader.read_nullable_string()?
            }
        } else {
            None
        };

        let acks = reader.read_i16()?;
        let timeout_ms = reader.read_i32()?;

        let topics = if is_flex {
            reader.read_compact_array(|r| {
                let name = r.read_compact_string()?;
                let partitions = r.read_compact_array(|r| {
                    let index = r.read_i32()?;
                    let record_set = r.read_nullable_bytes()?.unwrap_or_default();
                    // v8+ per-partition tagged fields
                    let _ = r.read_tagged_fields();
                    Ok(ProduceRequestPartition { index, record_set })
                })?;
                // per-topic tagged fields
                let _ = r.read_tagged_fields();
                Ok(ProduceRequestTopic { name, partitions })
            })?
        } else {
            reader.read_array(|r| {
                let name = r.read_string()?;
                let partitions = r.read_array(|r| {
                    let index = r.read_i32()?;
                    let record_set = r.read_nullable_bytes()?.unwrap_or_default();
                    Ok(ProduceRequestPartition { index, record_set })
                })?;
                Ok(ProduceRequestTopic { name, partitions })
            })?
        };

        Ok(Self { transactional_id, acks, timeout_ms, topics })
    }
}

// ─── Response ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ProduceResponseTopic {
    pub name: String,
    pub partitions: Vec<ProduceResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct ProduceResponsePartition {
    pub index: i32,
    pub error_code: KafkaErrorCode,
    pub base_offset: i64,
    /// v1+: log_append_time_ms
    pub log_append_time_ms: i64,
    /// v2+: log_start_offset
    pub log_start_offset: i64,
}

#[derive(Debug, Clone)]
pub struct ProduceResponse {
    pub topics: Vec<ProduceResponseTopic>,
    /// v1+: throttle_time_ms
    pub throttle_time_ms: i32,
}

impl KafkaResponseEncoder for ProduceResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        let is_flex = version >= 9;

        if is_flex {
            writer.write_compact_array(&self.topics, |w, t| {
                w.write_compact_string(&t.name);
                w.write_compact_array(&t.partitions, |w, p| {
                    w.write_i32(p.index);
                    w.write_i16(p.error_code.as_i16());
                    w.write_i64(p.base_offset);
                    if version >= 2 {
                        w.write_i64(p.log_start_offset);
                    }
                    w.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
        } else {
            writer.write_array(&self.topics, |w, t| {
                w.write_string(&t.name);
                w.write_array(&t.partitions, |w, p| {
                    w.write_i32(p.index);
                    w.write_i16(p.error_code.as_i16());
                    w.write_i64(p.base_offset);
                    if version >= 1 {
                        w.write_i64(p.log_append_time_ms);
                    }
                });
            });
        }

        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        Ok(())
    }
}
