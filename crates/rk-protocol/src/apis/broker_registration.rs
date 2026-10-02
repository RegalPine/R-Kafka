//! BrokerRegistration API (Key = 54)
//!
//! Broker 向 Controller 注册自身信息 (KRaft 模式)。
//! Broker 启动时发送此请求，Controller 将其记录到 Metadata Log。

use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::error_codes::KafkaErrorCode;
use crate::types::{KafkaReader, KafkaWriter};
use rk_core::error::Result;

// ─── Request ─────────────────────────────────────────────────────────

/// Broker 注册的端点信息
#[derive(Debug, Clone)]
pub struct BrokerRegistrationEndpoint {
    /// 端口
    pub port: i32,
    /// 安全协议名称 (如 PLAINTEXT, SSL)
    pub security_protocol: i16,
    /// 监听器名称
    pub listener_name: String,
}

/// BrokerRegistration 请求 (Broker → Controller)
#[derive(Debug, Clone)]
pub struct BrokerRegistrationRequest {
    /// Broker ID
    pub broker_id: i32,
    /// 集群 ID (用于验证是否加入正确集群)
    pub cluster_id: String,
    /// 特性位图 (Broker 支持的特性)
    pub features: Vec<BrokerRegistrationFeature>,
    /// 机架 ID (可选，用于机架感知)
    pub rack: Option<String>,
    /// 主机名
    pub host: String,
    /// 端口
    pub port: i32,
    /// Broker Epoch (单调递增，每次启动 +1)
    pub broker_epoch: i64,
    /// 端点列表
    pub endpoints: Vec<BrokerRegistrationEndpoint>,
}

/// Broker 支持的特性
#[derive(Debug, Clone)]
pub struct BrokerRegistrationFeature {
    /// 特性名称
    pub name: String,
    /// 最低支持版本
    pub min_version: i16,
    /// 最高支持版本
    pub max_version: i16,
}

impl KafkaRequestDecoder for BrokerRegistrationRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        let broker_id = reader.read_i32()?;
        let cluster_id = reader.read_compact_string()?;
        let features = reader.read_compact_array(|r| {
            let name = r.read_compact_string()?;
            let min_version = r.read_i16()?;
            let max_version = r.read_i16()?;
            let _tags = r.read_tagged_fields()?;
            Ok(BrokerRegistrationFeature {
                name,
                min_version,
                max_version,
            })
        })?;
        let rack = reader.read_compact_nullable_string()?;
        let host = reader.read_compact_string()?;
        let port = reader.read_i32()?;
        let broker_epoch = reader.read_i64()?;
        let endpoints = reader.read_compact_array(|r| {
            let port = r.read_i32()?;
            let security_protocol = r.read_i16()?;
            let listener_name = r.read_compact_string()?;
            let _tags = r.read_tagged_fields()?;
            Ok(BrokerRegistrationEndpoint {
                port,
                security_protocol,
                listener_name,
            })
        })?;
        let _tags = reader.read_tagged_fields()?;
        Ok(Self {
            broker_id,
            cluster_id,
            features,
            rack,
            host,
            port,
            broker_epoch,
            endpoints,
        })
    }
}

// ─── Response ────────────────────────────────────────────────────────

/// BrokerRegistration 响应 (Controller → Broker)
#[derive(Debug, Clone)]
pub struct BrokerRegistrationResponse {
    /// 限流时间 (ms)
    pub throttle_time_ms: i32,
    /// 错误码
    pub error_code: KafkaErrorCode,
    /// Broker Epoch (Controller 确认的 epoch)
    pub broker_epoch: i64,
}

impl KafkaResponseEncoder for BrokerRegistrationResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code.as_i16());
        writer.write_i64(self.broker_epoch);
        writer.write_tagged_fields(&[]);
        Ok(())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_broker_registration_request_decode() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i32(1); // broker_id
        w.write_compact_string("cluster1"); // cluster_id
                                            // features (1 item)
        w.write_compact_array(
            &[("feature1".to_string(), 1i16, 3i16)],
            |w2, (name, min, max)| {
                w2.write_compact_string(name);
                w2.write_i16(*min);
                w2.write_i16(*max);
                w2.write_tagged_fields(&[]);
            },
        );
        w.write_compact_nullable_string(Some("rack1")); // rack
        w.write_compact_string("host1"); // host
        w.write_i32(9092); // port
        w.write_i64(42); // broker_epoch
                         // endpoints (1 item)
        w.write_compact_array(
            &[(9092i32, 0i16, "PLAINTEXT".to_string())],
            |w2, (port, sp, name)| {
                w2.write_i32(*port);
                w2.write_i16(*sp);
                w2.write_compact_string(name);
                w2.write_tagged_fields(&[]);
            },
        );
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = BrokerRegistrationRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.broker_id, 1);
        assert_eq!(req.cluster_id, "cluster1");
        assert_eq!(req.features.len(), 1);
        assert_eq!(req.features[0].name, "feature1");
        assert_eq!(req.rack, Some("rack1".to_string()));
        assert_eq!(req.host, "host1");
        assert_eq!(req.port, 9092);
        assert_eq!(req.broker_epoch, 42);
        assert_eq!(req.endpoints.len(), 1);
    }

    #[test]
    fn test_broker_registration_response_encode() {
        let resp = BrokerRegistrationResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            broker_epoch: 42,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_broker_registration_null_rack() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i32(2);
        w.write_compact_string("c1");
        w.write_compact_array(
            &[] as &[(&str, i16, i16)],
            |w2: &mut KafkaWriter<'_>, _: &(&str, i16, i16)| {
                let _ = w2;
            },
        );
        w.write_compact_nullable_string(None); // null rack
        w.write_compact_string("h");
        w.write_i32(9092);
        w.write_i64(1);
        w.write_compact_array(
            &[] as &[(i32, i16, String)],
            |w2: &mut KafkaWriter<'_>, _: &(i32, i16, String)| {
                let _ = w2;
            },
        );
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = BrokerRegistrationRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.broker_id, 2);
        assert_eq!(req.rack, None);
    }
}
