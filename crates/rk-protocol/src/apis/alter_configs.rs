//! AlterConfigs API (Key = 33)
//!
//! 修改 Broker 或 Topic 运行时配置。
//! Phase 1: 仅支持 Topic 配置修改 (内存中, 不持久化到元数据日志)。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AlterConfigsRequest {
    pub resources: Vec<AlterConfigsRequestResource>,
    /// v1+: validate_only (bool)
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct AlterConfigsRequestResource {
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<AlterConfigsRequestConfig>,
}

#[derive(Debug, Clone)]
pub struct AlterConfigsRequestConfig {
    pub name: String,
    pub value: Option<String>,
}

impl KafkaRequestDecoder for AlterConfigsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        let resources = if version >= 2 {
            reader.read_compact_array(|r| {
                let rt = r.read_i8()?;
                let name = r.read_compact_string()?;
                let configs = r.read_compact_array(|r2| {
                    let cn = r2.read_compact_string()?;
                    let cv = r2.read_compact_nullable_string()?;
                    if version >= 2 {
                        let _tags = r2.read_tagged_fields()?;
                    }
                    Ok(AlterConfigsRequestConfig {
                        name: cn,
                        value: cv,
                    })
                })?;
                if version >= 2 {
                    let _tags = r.read_tagged_fields()?;
                }
                Ok(AlterConfigsRequestResource {
                    resource_type: rt,
                    resource_name: name,
                    configs,
                })
            })?
        } else {
            reader.read_array(|r| {
                let rt = r.read_i8()?;
                let name = r.read_string()?;
                let configs = r.read_array(|r2| {
                    let cn = r2.read_string()?;
                    let cv = r2.read_nullable_string()?;
                    Ok(AlterConfigsRequestConfig {
                        name: cn,
                        value: cv,
                    })
                })?;
                Ok(AlterConfigsRequestResource {
                    resource_type: rt,
                    resource_name: name,
                    configs,
                })
            })?
        };

        let validate_only = if version >= 1 {
            reader.read_bool()?
        } else {
            false
        };

        if version >= 2 {
            let _tags = reader.read_tagged_fields()?;
        }

        Ok(Self {
            resources,
            validate_only,
        })
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AlterConfigsResponse {
    /// v0: 无 throttle; v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub responses: Vec<AlterConfigsResponseResource>,
}

#[derive(Debug, Clone)]
pub struct AlterConfigsResponseResource {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
}

impl KafkaResponseEncoder for AlterConfigsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 2 {
            writer.write_compact_array(&self.responses, |w, r| {
                w.write_i16(r.error_code.as_i16());
                w.write_compact_nullable_string(r.error_message.as_deref());
                w.write_i8(r.resource_type);
                w.write_compact_string(&r.resource_name);
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_array(&self.responses, |w, r| {
                w.write_i16(r.error_code.as_i16());
                w.write_nullable_string(r.error_message.as_deref());
                w.write_i8(r.resource_type);
                w.write_string(&r.resource_name);
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_alter_configs_response_encode_v0() {
        let resp = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResponseResource {
                error_code: KafkaErrorCode::None,
                error_message: None,
                resource_type: 2,
                resource_name: "test-topic".to_string(),
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 10);
    }

    #[test]
    fn test_alter_configs_response_encode_v2_flexible() {
        let resp = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResponseResource {
                error_code: KafkaErrorCode::None,
                error_message: Some("test error".to_string()),
                resource_type: 4,
                resource_name: "1".to_string(),
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 2).unwrap();
        assert!(buf.len() > 10);
    }
}
