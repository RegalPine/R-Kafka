//! BrokerHeartbeat API (Key = 55)
//!
//! Broker 定期向 Controller 发送心跳，保持注册状态。
//! Controller 通过心跳检测 Broker 存活，并返回待执行的指令。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

// ─── Request ─────────────────────────────────────────────────────────

/// BrokerHeartbeat 请求 (Broker → Controller)
#[derive(Debug, Clone)]
pub struct BrokerHeartbeatRequest {
    /// Broker ID
    pub broker_id: i32,
    /// Broker Epoch (必须与注册时一致)
    pub broker_epoch: i64,
    /// 当前是否处于优雅关闭流程
    pub want_fence: bool,
    /// 是否想要停止发送/接受请求
    pub want_shut_down: bool,
    /// 当前已知的 Leader Epoch (可选，用于同步)
    pub current_metadata_offset: i64,
}

impl KafkaRequestDecoder for BrokerHeartbeatRequest {
    fn decode(reader: &mut KafkaReader<'_>, _version: i16) -> Result<Self> {
        let broker_id = reader.read_i32()?;
        let broker_epoch = reader.read_i64()?;
        let want_fence = reader.read_bool()?;
        let want_shut_down = reader.read_bool()?;
        let current_metadata_offset = reader.read_i64()?;
        let _tags = reader.read_tagged_fields()?;
        Ok(Self {
            broker_id,
            broker_epoch,
            want_fence,
            want_shut_down,
            current_metadata_offset,
        })
    }
}

// ─── Response ────────────────────────────────────────────────────────

/// BrokerHeartbeat 响应 (Controller → Broker)
#[derive(Debug, Clone)]
pub struct BrokerHeartbeatResponse {
    /// 限流时间 (ms)
    pub throttle_time_ms: i32,
    /// 错误码
    pub error_code: KafkaErrorCode,
    /// 当前 Leader Broker ID (-1 表示无 Leader)
    pub leader_id: i32,
    /// 当前 Leader Epoch
    pub leader_epoch: i32,
    /// 当前 Broker 是否被确认为 Controller
    pub is_controller: bool,
}

impl KafkaResponseEncoder for BrokerHeartbeatResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, _version: i16) -> Result<()> {
        writer.write_i32(self.throttle_time_ms);
        writer.write_i16(self.error_code.as_i16());
        writer.write_i32(self.leader_id);
        writer.write_i32(self.leader_epoch);
        writer.write_bool(self.is_controller);
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
    fn test_broker_heartbeat_request_decode() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i32(1);       // broker_id
        w.write_i64(42);      // broker_epoch
        w.write_bool(false);  // want_fence
        w.write_bool(false);  // want_shut_down
        w.write_i64(1000);    // current_metadata_offset
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = BrokerHeartbeatRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.broker_id, 1);
        assert_eq!(req.broker_epoch, 42);
        assert!(!req.want_fence);
        assert!(!req.want_shut_down);
        assert_eq!(req.current_metadata_offset, 1000);
    }

    #[test]
    fn test_broker_heartbeat_response_encode() {
        let resp = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            leader_id: 1,
            leader_epoch: 5,
            is_controller: true,
        };
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        assert!(!buf.is_empty());

        // 验证可以解码回来
        let mut reader = KafkaReader::new(&buf);
        let throttle = reader.read_i32().unwrap();
        let error = reader.read_i16().unwrap();
        let leader = reader.read_i32().unwrap();
        let epoch = reader.read_i32().unwrap();
        let is_ctrl = reader.read_bool().unwrap();
        assert_eq!(throttle, 0);
        assert_eq!(error, 0);
        assert_eq!(leader, 1);
        assert_eq!(epoch, 5);
        assert!(is_ctrl);
    }

    #[test]
    fn test_broker_heartbeat_shutdown_request() {
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        w.write_i32(3);       // broker_id
        w.write_i64(10);      // broker_epoch
        w.write_bool(false);  // want_fence
        w.write_bool(true);   // want_shut_down (优雅关闭)
        w.write_i64(500);     // current_metadata_offset
        w.write_tagged_fields(&[]);

        let mut reader = KafkaReader::new(&buf);
        let req = BrokerHeartbeatRequest::decode(&mut reader, 0).unwrap();
        assert_eq!(req.broker_id, 3);
        assert!(req.want_shut_down);
    }
}
