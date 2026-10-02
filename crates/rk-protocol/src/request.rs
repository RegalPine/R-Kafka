//! Kafka Request Header
//!
//! Request Header v0/v1 (Legacy):
//!   api_key (i16), api_version (i16), correlation_id (i32), client_id (nullable_string)
//!
//! Request Header v2 (Flexible):
//!   api_key (i16), api_version (i16), correlation_id (i32), client_id (compact_nullable_string),
//!   tagged_fields

use crate::types::{KafkaReader, KafkaWriter, TaggedField};
use rk_core::error::Result;

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    /// v2+ (Flexible) tagged fields
    pub tagged_fields: Vec<TaggedField>,
}

impl RequestHeader {
    pub fn new(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
            tagged_fields: Vec::new(),
        }
    }

    /// 是否为 Flexible 版本
    pub fn is_flexible(&self) -> bool {
        rk_core::ApiKey::from_i16(self.api_key)
            .map(|k| k.is_flexible(self.api_version))
            .unwrap_or(false)
    }

    pub fn decode(reader: &mut KafkaReader<'_>) -> Result<Self> {
        let api_key = reader.read_i16()?;
        let api_version = reader.read_i16()?;
        let correlation_id = reader.read_i32()?;

        let is_flex = rk_core::ApiKey::from_i16(api_key)
            .map(|k| k.is_flexible(api_version))
            .unwrap_or(false);

        let client_id = if is_flex {
            reader.read_compact_nullable_string()?
        } else {
            reader.read_nullable_string()?
        };

        let tagged_fields = if is_flex {
            reader.read_tagged_fields()?
        } else {
            Vec::new()
        };

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
            tagged_fields,
        })
    }

    pub fn encode(&self, writer: &mut KafkaWriter<'_>) {
        writer.write_i16(self.api_key);
        writer.write_i16(self.api_version);
        writer.write_i32(self.correlation_id);

        if self.is_flexible() {
            writer.write_compact_nullable_string(self.client_id.as_deref());
            writer.write_tagged_fields(&self.tagged_fields);
        } else {
            writer.write_nullable_string(self.client_id.as_deref());
        }
    }
}
