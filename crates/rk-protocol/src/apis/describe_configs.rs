//! DescribeConfigs API (Key = 32)
//!
//! 查询 Broker 或 Topic 配置。
//! Phase 1: 支持 Topic 和 Broker 资源类型，返回默认/运行时配置。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── 资源类型 ─────────────────────────────────────────────────────────

/// 配置资源类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum ConfigResourceType {
    Topic = 2,
    Broker = 4,
}

impl ConfigResourceType {
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            2 => Some(Self::Topic),
            4 => Some(Self::Broker),
            _ => None,
        }
    }
}

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeConfigsRequest {
    pub resources: Vec<DescribeConfigsRequestResource>,
    /// v1+: include_synonyms (bool)
    pub include_synonyms: bool,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigsRequestResource {
    pub resource_type: i8,
    pub resource_name: String,
    /// null = 返回全部配置; 否则仅返回指定配置项
    pub config_names: Option<Vec<String>>,
}

impl KafkaRequestDecoder for DescribeConfigsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        let resources = if version >= 3 {
            // Flexible: compact_array
            reader.read_compact_array(|r| {
                let rt = r.read_i8()?;
                let name = r.read_compact_string()?;
                let names = if version >= 3 {
                    r.read_compact_array(|r2| r2.read_compact_string())?
                        .into_iter()
                        .collect::<Vec<_>>()
                        .into()
                } else {
                    let arr = r.read_array(|r2| r2.read_string())?;
                    if arr.is_empty() {
                        None
                    } else {
                        Some(arr)
                    }
                };
                Ok(DescribeConfigsRequestResource {
                    resource_type: rt,
                    resource_name: name,
                    config_names: names,
                })
            })?
        } else {
            // Legacy: array
            reader.read_array(|r| {
                let rt = r.read_i8()?;
                let name = r.read_string()?;
                let names_opt = r.read_array(|r2| r2.read_string())?;
                let names = if names_opt.is_empty() {
                    None
                } else {
                    Some(names_opt)
                };
                Ok(DescribeConfigsRequestResource {
                    resource_type: rt,
                    resource_name: name,
                    config_names: names,
                })
            })?
        };

        let include_synonyms = if version >= 1 {
            reader.read_bool()?
        } else {
            false
        };

        // v3: tagged_fields
        if version >= 3 {
            let _tags = reader.read_tagged_fields()?;
        }

        Ok(Self {
            resources,
            include_synonyms,
        })
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeConfigsResponse {
    /// v0: 无 throttle; v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
    pub resources: Vec<DescribeConfigsResponseResource>,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigsResponseResource {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<DescribeConfigsResponseConfig>,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigsResponseConfig {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    /// v1+: is_default (bool)
    pub is_default: bool,
    /// v1+: is_sensitive (bool)
    pub is_sensitive: bool,
    /// v1+: config_source (i8)
    pub config_source: i8,
    /// v1+: synonyms (array)
    pub synonyms: Vec<DescribeConfigsConfigSynonym>,
}

#[derive(Debug, Clone)]
pub struct DescribeConfigsConfigSynonym {
    pub name: String,
    pub value: Option<String>,
    pub source: i8,
}

impl KafkaResponseEncoder for DescribeConfigsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        if version >= 1 {
            writer.write_i32(self.throttle_time_ms);
        }

        if version >= 3 {
            writer.write_compact_array(&self.resources, |w, r| {
                w.write_i16(r.error_code.as_i16());
                w.write_nullable_string(r.error_message.as_deref());
                w.write_i8(r.resource_type);
                w.write_compact_string(&r.resource_name);
                w.write_compact_array(&r.configs, |w2, c| {
                    w2.write_compact_string(&c.name);
                    w2.write_compact_nullable_string(c.value.as_deref());
                    w2.write_bool(c.read_only);
                    if version >= 1 {
                        w2.write_bool(c.is_default);
                        w2.write_bool(c.is_sensitive);
                        w2.write_i8(c.config_source);
                        w2.write_compact_array(&c.synonyms, |w3, s| {
                            w3.write_compact_string(&s.name);
                            w3.write_compact_nullable_string(s.value.as_deref());
                            w3.write_i8(s.source);
                            w3.write_tagged_fields(&[]);
                        });
                        w2.write_tagged_fields(&[]);
                    }
                });
                w.write_tagged_fields(&[]);
            });
            writer.write_tagged_fields(&[]);
        } else {
            writer.write_array(&self.resources, |w, r| {
                w.write_i16(r.error_code.as_i16());
                w.write_nullable_string(r.error_message.as_deref());
                w.write_i8(r.resource_type);
                w.write_string(&r.resource_name);
                w.write_array(&r.configs, |w2, c| {
                    w2.write_string(&c.name);
                    w2.write_nullable_string(c.value.as_deref());
                    w2.write_bool(c.read_only);
                    if version >= 1 {
                        w2.write_bool(c.is_default);
                        w2.write_bool(c.is_sensitive);
                        w2.write_i8(c.config_source);
                        w2.write_array(&c.synonyms, |w3, s| {
                            w3.write_string(&s.name);
                            w3.write_nullable_string(s.value.as_deref());
                            w3.write_i8(s.source);
                        });
                    }
                });
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
    fn test_describe_configs_response_encode_v0() {
        let resp = DescribeConfigsResponse {
            throttle_time_ms: 0,
            resources: vec![DescribeConfigsResponseResource {
                error_code: KafkaErrorCode::None,
                error_message: None,
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                configs: vec![DescribeConfigsResponseConfig {
                    name: "retention.ms".to_string(),
                    value: Some("604800000".to_string()),
                    read_only: false,
                    is_default: true,
                    is_sensitive: false,
                    config_source: 5,
                    synonyms: vec![],
                }],
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(buf.len() > 10);
    }

    #[test]
    fn test_describe_configs_response_encode_v3_flexible() {
        let resp = DescribeConfigsResponse {
            throttle_time_ms: 0,
            resources: vec![DescribeConfigsResponseResource {
                error_code: KafkaErrorCode::None,
                error_message: None,
                resource_type: 4,
                resource_name: "1".to_string(),
                configs: vec![],
            }],
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 3).unwrap();
        assert!(buf.len() > 5);
    }
}
