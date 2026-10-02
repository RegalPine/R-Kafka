//! Fetch API (Key = 1)
//!
//! 实现 FetchRequest / FetchResponse，支持 v0-v16。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FetchRequestTopic {
    pub name: String,
    pub partitions: Vec<FetchRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct FetchRequestPartition {
    pub index: i32,
    /// v9+: current_leader_epoch
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    /// v5+: log_start_offset
    pub log_start_offset: i64,
    pub max_bytes: i32,
}

#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    /// v3+: max_bytes
    pub max_bytes: i32,
    /// v4+: isolation_level
    pub isolation_level: i8,
    /// v7+: session_id
    pub session_id: i32,
    /// v7+: session_epoch
    pub session_epoch: i32,
    pub topics: Vec<FetchRequestTopic>,
    /// v11+: rack_id
    pub rack_id: Option<String>,
}

impl KafkaRequestDecoder for FetchRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        let is_flex = version >= 12;

        let replica_id = reader.read_i32()?;

        // v12+: replica_id is followed by replica_state (flexible only uses new fields)
        // For simplicity, we keep the same field layout

        let max_wait_ms = reader.read_i32()?;
        let min_bytes = reader.read_i32()?;

        let max_bytes = if version >= 3 {
            reader.read_i32()?
        } else {
            i32::MAX
        };

        let isolation_level = if version >= 4 {
            reader.read_i8()?
        } else {
            0 // READ_UNCOMMITTED
        };

        let session_id = if version >= 7 { reader.read_i32()? } else { 0 };

        let session_epoch = if version >= 7 { reader.read_i32()? } else { 0 };

        let topics = if is_flex {
            reader.read_compact_array(|r| {
                let name = r.read_compact_string()?;
                let partitions = r.read_compact_array(|r| {
                    let index = r.read_i32()?;
                    let current_leader_epoch = if version >= 9 { r.read_i32()? } else { -1 };
                    let fetch_offset = r.read_i64()?;
                    let log_start_offset = if version >= 5 { r.read_i64()? } else { -1 };
                    let max_bytes = r.read_i32()?;
                    let _ = r.read_tagged_fields();
                    Ok(FetchRequestPartition {
                        index,
                        current_leader_epoch,
                        fetch_offset,
                        log_start_offset,
                        max_bytes,
                    })
                })?;
                let _ = r.read_tagged_fields();
                Ok(FetchRequestTopic { name, partitions })
            })?
        } else {
            reader.read_array(|r| {
                let name = r.read_string()?;
                let partitions = r.read_array(|r| {
                    let index = r.read_i32()?;
                    let current_leader_epoch = if version >= 9 { r.read_i32()? } else { -1 };
                    let fetch_offset = r.read_i64()?;
                    let log_start_offset = if version >= 5 { r.read_i64()? } else { -1 };
                    let max_bytes = r.read_i32()?;
                    Ok(FetchRequestPartition {
                        index,
                        current_leader_epoch,
                        fetch_offset,
                        log_start_offset,
                        max_bytes,
                    })
                })?;
                Ok(FetchRequestTopic { name, partitions })
            })?
        };

        // v7+: forgotten_topics_data
        if version >= 7 {
            if is_flex {
                let _ = reader.read_compact_array(|_: &mut KafkaReader<'_>| {
                    Ok(()) // skip
                })?;
            } else {
                let _ = reader.read_array(|_: &mut KafkaReader<'_>| {
                    Ok(()) // skip
                })?;
            }
        }

        let rack_id = if version >= 11 {
            if is_flex {
                reader.read_compact_nullable_string()?
            } else {
                // v11 is flexible only for rack_id
                reader.read_compact_nullable_string()?
            }
        } else {
            None
        };

        // trailing tagged fields
        if is_flex {
            let _ = reader.read_tagged_fields()?;
        }

        Ok(Self {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            rack_id,
        })
    }
}

// ─── Response ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FetchResponseTopic {
    pub name: String,
    pub partitions: Vec<FetchResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct FetchResponsePartition {
    pub index: i32,
    pub error_code: KafkaErrorCode,
    pub high_watermark: i64,
    /// v4+: last_stable_offset
    pub last_stable_offset: i64,
    /// v5+: log_start_offset
    pub log_start_offset: i64,
    /// Raw RecordBatch bytes
    pub record_set: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct FetchResponse {
    pub throttle_time_ms: i32,
    /// v7+: error_code
    pub error_code: KafkaErrorCode,
    /// v7+: session_id
    pub session_id: i32,
    pub topics: Vec<FetchResponseTopic>,
}

impl KafkaResponseEncoder for FetchResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        let is_flex = version >= 12;

        writer.write_i32(self.throttle_time_ms);

        if version >= 7 {
            writer.write_i16(self.error_code.as_i16());
            writer.write_i32(self.session_id);
        }

        if is_flex {
            writer.write_compact_array(&self.topics, |w, t| {
                w.write_compact_string(&t.name);
                w.write_compact_array(&t.partitions, |w, p| {
                    w.write_i32(p.index);
                    w.write_i16(p.error_code.as_i16());
                    w.write_i64(p.high_watermark);
                    if version >= 4 {
                        w.write_i64(p.last_stable_offset);
                    }
                    if version >= 5 {
                        w.write_i64(p.log_start_offset);
                    }
                    // aborted_transactions (v4+) — empty for now
                    if version >= 4 {
                        w.write_compact_array(&[] as &[i32], |_, _| {});
                    }
                    w.write_nullable_bytes(if p.record_set.is_empty() {
                        None
                    } else {
                        Some(&p.record_set)
                    });
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
                    w.write_i64(p.high_watermark);
                    if version >= 4 {
                        w.write_i64(p.last_stable_offset);
                    }
                    if version >= 5 {
                        w.write_i64(p.log_start_offset);
                    }
                    if version >= 4 {
                        // aborted_transactions — empty array
                        w.write_array(&[] as &[i32], |_, _| {});
                    }
                    w.write_nullable_bytes(if p.record_set.is_empty() {
                        None
                    } else {
                        Some(&p.record_set)
                    });
                });
            });
        }

        Ok(())
    }
}
