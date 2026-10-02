//! Kafka Response Header
//!
//! Response Header v0 (Legacy):
//!   correlation_id (i32)
//!
//! Response Header v1 (Flexible):
//!   correlation_id (i32), tagged_fields

use rk_core::error::Result;
use crate::types::{KafkaReader, KafkaWriter, TaggedField};

#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub correlation_id: i32,
    pub tagged_fields: Vec<TaggedField>,
}

impl ResponseHeader {
    pub fn new(correlation_id: i32) -> Self {
        Self {
            correlation_id,
            tagged_fields: Vec::new(),
        }
    }

    pub fn decode(reader: &mut KafkaReader<'_>, is_flexible: bool) -> Result<Self> {
        let correlation_id = reader.read_i32()?;
        let tagged_fields = if is_flexible {
            reader.read_tagged_fields()?
        } else {
            Vec::new()
        };
        Ok(Self { correlation_id, tagged_fields })
    }

    pub fn encode(&self, writer: &mut KafkaWriter<'_>, is_flexible: bool) {
        writer.write_i32(self.correlation_id);
        if is_flexible {
            writer.write_tagged_fields(&self.tagged_fields);
        }
    }
}
