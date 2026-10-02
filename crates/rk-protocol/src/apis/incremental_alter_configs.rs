//! IncrementalAlterConfigs API (Key = 44)
//!
//! 增量修改 Broker 或 Topic 运行时配置。
//! KIP-248, v0+ 全部为 Flexible 格式。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── 配置操作类型 ────────────────────────────────────────────────────

/// 增量配置操作类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum IncrementalAlterConfigsOp {
    /// 设置配置项
    Set = 0,
    /// 删除配置项 (恢复默认)
    Delete = 1,
    /// 追加到当前值 (仅 list 类型)
    Append = 2,
    /// 从当前值中移除 (仅 list 类型)
    Subtract = 3,
}

impl IncrementalAlterConfigsOp {
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Self::Set),
            1 => Some(Self::Delete),
            2 => Some(Self::Append),
            3 => Some(Self::Subtract),
            _ => None,
        }
    }
}

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsRequest {
    pub resources: Vec<IncrementalAlterConfigsRequestResource>,
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsRequestResource {
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<IncrementalAlterConfigsRequestConfig>,
}

#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsRequestConfig {
    pub name: String,
    pub value: Option<String>,
    pub config_operation: i8,
}

impl KafkaRequestDecoder for IncrementalAlterConfigsRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        // v0+ is always flexible
        let resources = reader.read_compact_array(|r| {
            let rt = r.read_i8()?;
            let name = r.read_compact_string()?;
            let configs = r.read_compact_array(|r2| {
                let cn = r2.read_compact_string()?;
                let cv = r2.read_compact_nullable_string()?;
                let op = r2.read_i8()?;
                let _tags = r2.read_tagged_fields();
                Ok(IncrementalAlterConfigsRequestConfig {
                    name: cn,
                    value: cv,
                    config_operation: op,
                })
            })?;
            let _tags = r.read_tagged_fields();
            Ok(IncrementalAlterConfigsRequestResource {
                resource_type: rt,
                resource_name: name,
                configs,
            })
        })?;
        let validate_only = reader.read_bool()?;
        let _tags = reader.read_tagged_fields();
        Ok(Self {
            resources,
            validate_only,
        })
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsResponse {
    pub throttle_time_ms: i32,
    pub responses: Vec<IncrementalAlterConfigsResponseResource>,
}

#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsResponseResource {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
}

impl KafkaResponseEncoder for IncrementalAlterConfigsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        // Always flexible
        writer.write_i32(self.throttle_time_ms);
        writer.write_compact_array(&self.responses, |w, r| {
            w.write_i16(r.error_code.as_i16());
            w.write_compact_nullable_string(r.error_message.as_deref());
            w.write_i8(r.resource_type);
            w.write_compact_string(&r.resource_name);
            w.write_tagged_fields(&[]);
        });
        writer.write_tagged_fields(&[]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_incremental_alter_configs_response_encode() {
        let resp = IncrementalAlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![IncrementalAlterConfigsResponseResource {
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
    fn test_incremental_alter_configs_op_from_i8() {
        assert_eq!(
            IncrementalAlterConfigsOp::from_i8(0),
            Some(IncrementalAlterConfigsOp::Set)
        );
        assert_eq!(
            IncrementalAlterConfigsOp::from_i8(1),
            Some(IncrementalAlterConfigsOp::Delete)
        );
        assert_eq!(
            IncrementalAlterConfigsOp::from_i8(2),
            Some(IncrementalAlterConfigsOp::Append)
        );
        assert_eq!(
            IncrementalAlterConfigsOp::from_i8(3),
            Some(IncrementalAlterConfigsOp::Subtract)
        );
        assert_eq!(IncrementalAlterConfigsOp::from_i8(99), None);
    }
}
