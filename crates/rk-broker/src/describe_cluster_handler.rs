//! DescribeCluster Handler
//!
//! 处理 DescribeCluster 请求 (API Key = 60)。
//! Phase 1: 返回当前 Broker 的集群信息。

use rk_core::error::Result;
use rk_protocol::apis::describe_cluster::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

/// DescribeCluster 请求处理器
pub struct DescribeClusterHandler {
    broker_id: i32,
    broker_host: String,
    broker_port: i32,
    cluster_id: String,
}

impl DescribeClusterHandler {
    pub fn new(broker_id: i32, broker_host: String, broker_port: i32, cluster_id: String) -> Self {
        Self {
            broker_id,
            broker_host,
            broker_port,
            cluster_id,
        }
    }

    /// 处理 DescribeCluster 请求
    pub fn handle(
        &self,
        request: DescribeClusterRequest,
        _version: i16,
    ) -> Result<DescribeClusterResponse> {
        debug!(
            include_authorized_operations = request.include_authorized_operations,
            "DescribeCluster request"
        );

        // Phase 1: 单 Broker，返回自身信息
        Ok(DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            cluster_id: self.cluster_id.clone(),
            controller_id: self.broker_id,
            brokers: vec![DescribeClusterResponseBroker {
                broker_id: self.broker_id,
                host: self.broker_host.clone(),
                port: self.broker_port,
                rack: None, // Phase 1: 不支持 rack
            }],
            cluster_authorized_operations: 0, // Phase 1: 不跟踪授权
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_cluster() {
        let handler = DescribeClusterHandler::new(
            1,
            "localhost".to_string(),
            9092,
            "test-cluster".to_string(),
        );
        let req = DescribeClusterRequest {
            include_authorized_operations: false,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.cluster_id, "test-cluster");
        assert_eq!(resp.controller_id, 1);
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].broker_id, 1);
    }
}
