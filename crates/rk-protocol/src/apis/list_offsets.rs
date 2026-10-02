//! ListOffsets API (Key = 2)
//!
//! 实现 ListOffsetsRequest / ListOffsetsResponse，支持 v0-v8。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ListOffsetsRequestTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetsRequestPartition>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsRequestPartition {
    pub index: i32,
    /// v0: current_offset (deprecated); v1+: timestamp
    /// Special values: -1 = LATEST, -2 = EARLIEST
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    pub replica_id: i32,
    /// v2+: isolation_level
    pub isolation_level: i8,
    pub topics: Vec<ListOffsetsRequestTopic>,
}

impl KafkaRequestDecoder for ListOffsetsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        let is_flex = version >= 6;

        let replica_id = reader.read_i32()?;

        let isolation_level = if version >= 2 { reader.read_i8()? } else { 0 };

        let topics = if is_flex {
            reader.read_compact_array(|r| {
                let name = r.read_compact_string()?;
                let partitions = r.read_compact_array(|r| {
                    let index = r.read_i32()?;
                    let timestamp = if version >= 1 {
                        r.read_i64()?
                    } else {
                        // v0: current_offset (ignored)
                        let _ = r.read_i64()?;
                        -1 // LATEST
                    };
                    let _ = r.read_tagged_fields();
                    Ok(ListOffsetsRequestPartition { index, timestamp })
                })?;
                let _ = r.read_tagged_fields();
                Ok(ListOffsetsRequestTopic { name, partitions })
            })?
        } else {
            reader.read_array(|r| {
                let name = r.read_string()?;
                let partitions = r.read_array(|r| {
                    let index = r.read_i32()?;
                    let timestamp = if version >= 1 {
                        r.read_i64()?
                    } else {
                        let _ = r.read_i64()?;
                        -1
                    };
                    Ok(ListOffsetsRequestPartition { index, timestamp })
                })?;
                Ok(ListOffsetsRequestTopic { name, partitions })
            })?
        };

        if is_flex {
            let _ = reader.read_tagged_fields()?;
        }

        Ok(Self {
            replica_id,
            isolation_level,
            topics,
        })
    }
}

// ─── Response ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ListOffsetsResponseTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetsResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsResponsePartition {
    pub index: i32,
    pub error_code: KafkaErrorCode,
    /// v1+: timestamp
    pub timestamp: i64,
    /// v1+: offset
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsResponse {
    /// v2+: throttle_time_ms
    pub throttle_time_ms: i32,
    pub topics: Vec<ListOffsetsResponseTopic>,
}

impl KafkaResponseEncoder for ListOffsetsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        let is_flex = version >= 6;

        if version >= 2 {
            writer.write_i32(self.throttle_time_ms);
        }

        if is_flex {
            writer.write_compact_array(&self.topics, |w, t| {
                w.write_compact_string(&t.name);
                w.write_compact_array(&t.partitions, |w, p| {
                    w.write_i32(p.index);
                    w.write_i16(p.error_code.as_i16());
                    if version >= 1 {
                        w.write_i64(p.timestamp);
                        w.write_i64(p.offset);
                    } else {
                        // v0: offsets array (single element)
                        w.write_array(&[p.offset], |w, o| w.write_i64(*o));
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
                    if version >= 1 {
                        w.write_i64(p.timestamp);
                        w.write_i64(p.offset);
                    } else {
                        w.write_array(&[p.offset], |w, o| w.write_i64(*o));
                    }
                });
            });
        }

        Ok(())
    }
}
