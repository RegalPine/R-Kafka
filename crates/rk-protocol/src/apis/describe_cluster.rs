//! DescribeCluster API (Key = 60)
//!
//! 查询集群元数据信息。
//! KIP-700, v0+ 全部为 Flexible 格式。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeClusterRequest {
    /// 是否包含授权信息
    pub include_authorized_operations: bool,
}

impl KafkaRequestDecoder for DescribeClusterRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        // v0+ is always flexible
        let include_authorized_operations = reader.read_bool()?;
        let _tags = reader.read_tagged_fields();
        Ok(Self { include_authorized_operations })
    }
}

impl KafkaResponseEncoder for DescribeClusterRequest {
    fn encode(&self, _writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        Ok(()) // Request only
    }
}

// ─── Response ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescribeClusterResponse {
    pub throttle_time_ms: i32,
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub cluster_id: String,
    pub controller_id: i32,
    pub brokers: Vec<DescribeClusterResponseBroker>,
    pub cluster_authorized_operations: i32,
}

#[derive(Debug, Clone)]
pub struct DescribeClusterResponseBroker {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

impl KafkaResponseEncoder for DescribeClusterResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        // Always flexible
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code as i16);
        writer.write_compact_nullable_string(self.error_message.as_deref());
        writer.write_compact_string(&self.cluster_id);
        writer.write_i32(self.controller_id);
        writer.write_compact_array(&self.brokers, |w, broker| {
            w.write_i32(broker.broker_id);
            w.write_compact_string(&broker.host);
            w.write_i32(broker.port);
            w.write_compact_nullable_string(broker.rack.as_deref());
            w.write_tagged_fields(&[]);
        });
        writer.write_i32(self.cluster_authorized_operations);
        writer.write_tagged_fields(&[]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_describe_cluster_encode() {
        let resp = DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            cluster_id: "test-cluster".to_string(),
            controller_id: 1,
            brokers: vec![DescribeClusterResponseBroker {
                broker_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                rack: Some("rack-1".to_string()),
            }],
            cluster_authorized_operations: 0,
        };
        let mut buf = BytesMut::new();
        let mut writer = KafkaWriter::new(&mut buf);
        resp.encode(&mut writer, 0).unwrap();
        assert!(!buf.is_empty());
    }
}
