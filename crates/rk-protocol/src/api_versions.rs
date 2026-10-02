//! ApiVersions API (Key = 18)
//!
//! 客户端通过此 API 发现 Broker 支持的 API 版本范围。
//! 这是客户端连接后发送的第一个请求。

use rk_core::error::Result;
use crate::codec::{KafkaRequestDecoder, KafkaResponseEncoder};
use crate::types::{KafkaReader, KafkaWriter};
use crate::error_codes::KafkaErrorCode;

/// R-Kafka 支持的 API 版本范围
#[derive(Debug, Clone)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// R-Kafka 当前支持的版本范围 (对齐 Kafka 3.7+)
///
/// 仅包含 Router 中实际实现了 Handler 的 API。
/// Broker-control APIs (4-7) 和 KRaft APIs (51-56) 暂未实现，不在此列表中。
pub const SUPPORTED_API_VERSIONS: &[ApiVersion] = &[
    // Produce (0)
    ApiVersion { api_key: 0,  min_version: 0, max_version: 10 },
    // Fetch (1)
    ApiVersion { api_key: 1,  min_version: 0, max_version: 16 },
    // ListOffsets (2)
    ApiVersion { api_key: 2,  min_version: 0, max_version: 8  },
    // Metadata (3)
    ApiVersion { api_key: 3,  min_version: 0, max_version: 13 },
    // OffsetCommit (8)
    ApiVersion { api_key: 8,  min_version: 0, max_version: 9  },
    // OffsetFetch (9)
    ApiVersion { api_key: 9,  min_version: 0, max_version: 9  },
    // FindCoordinator (10)
    ApiVersion { api_key: 10, min_version: 0, max_version: 4  },
    // JoinGroup (11)
    ApiVersion { api_key: 11, min_version: 0, max_version: 9  },
    // Heartbeat (12)
    ApiVersion { api_key: 12, min_version: 0, max_version: 4  },
    // LeaveGroup (13)
    ApiVersion { api_key: 13, min_version: 0, max_version: 5  },
    // SyncGroup (14)
    ApiVersion { api_key: 14, min_version: 0, max_version: 5  },
    // DescribeGroups (15)
    ApiVersion { api_key: 15, min_version: 0, max_version: 5  },
    // ListGroups (16)
    ApiVersion { api_key: 16, min_version: 0, max_version: 4  },
    // SaslHandshake (17)
    ApiVersion { api_key: 17, min_version: 0, max_version: 1  },
    // ApiVersions (18)
    ApiVersion { api_key: 18, min_version: 0, max_version: 3  },
    // CreateTopics (19)
    ApiVersion { api_key: 19, min_version: 0, max_version: 3  },
    // DeleteTopics (20)
    ApiVersion { api_key: 20, min_version: 0, max_version: 6  },
    // DeleteRecords (21)
    ApiVersion { api_key: 21, min_version: 0, max_version: 3  },
    // InitProducerId (22)
    ApiVersion { api_key: 22, min_version: 0, max_version: 4  },
    // OffsetForLeaderEpoch (23)
    ApiVersion { api_key: 23, min_version: 0, max_version: 4  },
    // AddPartitionsToTxn (24)
    ApiVersion { api_key: 24, min_version: 0, max_version: 3  },
    // EndTxn (26)
    ApiVersion { api_key: 26, min_version: 0, max_version: 3  },
    // DescribeConfigs (32)
    ApiVersion { api_key: 32, min_version: 0, max_version: 4  },
    // AlterConfigs (33)
    ApiVersion { api_key: 33, min_version: 0, max_version: 2  },
    // SaslAuthenticate (36)
    ApiVersion { api_key: 36, min_version: 0, max_version: 2  },
    // CreatePartitions (37)
    ApiVersion { api_key: 37, min_version: 0, max_version: 3  },
    // ElectLeaders (43)
    ApiVersion { api_key: 43, min_version: 0, max_version: 2  },
    // IncrementalAlterConfigs (44)
    ApiVersion { api_key: 44, min_version: 0, max_version: 0  },
    // AlterPartitionReassignments (45)
    ApiVersion { api_key: 45, min_version: 0, max_version: 0  },
    // ListPartitionReassignments (46)
    ApiVersion { api_key: 46, min_version: 0, max_version: 0  },
    // OffsetDelete (47)
    ApiVersion { api_key: 47, min_version: 0, max_version: 0  },
    // Vote (51)
    ApiVersion { api_key: 51, min_version: 0, max_version: 0  },
    // BeginQuorumEpoch (52)
    ApiVersion { api_key: 52, min_version: 0, max_version: 0  },
    // EndQuorumEpoch (53)
    ApiVersion { api_key: 53, min_version: 0, max_version: 0  },
    // DescribeQuorum (56)
    ApiVersion { api_key: 56, min_version: 0, max_version: 0  },
    // DescribeCluster (60)
    ApiVersion { api_key: 60, min_version: 0, max_version: 0  },
    // DescribeProducers (61)
    ApiVersion { api_key: 61, min_version: 0, max_version: 0  },
    // ListTransactions (65)
    ApiVersion { api_key: 65, min_version: 0, max_version: 0  },
    // DescribeTopics (70)
    ApiVersion { api_key: 70, min_version: 0, max_version: 0  },
];

// ─── ApiVersions Request ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ApiVersionsRequest {
    /// v3+: client_software_name (compact_string)
    pub client_software_name: Option<String>,
    /// v3+: client_software_version (compact_string)
    pub client_software_version: Option<String>,
}

impl KafkaRequestDecoder for ApiVersionsRequest {
    fn decode(reader: &mut KafkaReader<'_>, version: i16) -> Result<Self> {
        if version >= 3 {
            // Flexible: compact_string fields + tagged_fields
            let name = reader.read_compact_nullable_string()?;
            let ver = reader.read_compact_nullable_string()?;
            let _tags = reader.read_tagged_fields()?;
            Ok(Self {
                client_software_name: name,
                client_software_version: ver,
            })
        } else {
            // Legacy: no body fields (just header)
            Ok(Self {
                client_software_name: None,
                client_software_version: None,
            })
        }
    }
}

// ─── ApiVersions Response ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ApiVersionsResponse {
    pub error_code: KafkaErrorCode,
    pub api_versions: Vec<ApiVersion>,
    /// v1+: throttle_time_ms (i32)
    pub throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    pub fn supported() -> Self {
        Self {
            error_code: KafkaErrorCode::None,
            api_versions: SUPPORTED_API_VERSIONS.to_vec(),
            throttle_time_ms: 0,
        }
    }
}

impl KafkaResponseEncoder for ApiVersionsResponse {
    fn encode(&self, writer: &mut KafkaWriter<'_>, version: i16) -> Result<()> {
        writer.write_i16(self.error_code.as_i16());

        if version == 0 {
            // v0: array of (api_key, min_version, max_version)
            writer.write_array(&self.api_versions, |w, av| {
                w.write_i16(av.api_key);
                w.write_i16(av.min_version);
                w.write_i16(av.max_version);
            });
        } else {
            // v1+: throttle_time_ms + array
            writer.write_i32(self.throttle_time_ms);
            if version >= 3 {
                // Flexible: compact_array
                writer.write_compact_array(&self.api_versions, |w, av| {
                    w.write_i16(av.api_key);
                    w.write_i16(av.min_version);
                    w.write_i16(av.max_version);
                    w.write_tagged_fields(&[]); // per-item tagged fields
                });
                writer.write_tagged_fields(&[]); // response-level tagged fields
            } else {
                writer.write_array(&self.api_versions, |w, av| {
                    w.write_i16(av.api_key);
                    w.write_i16(av.min_version);
                    w.write_i16(av.max_version);
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_api_versions_response_encode_v0() {
        let resp = ApiVersionsResponse::supported();
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 0).unwrap();
        // error_code (2) + array_len (4) + N * (2+2+2)
        assert!(buf.len() > 6);
    }

    #[test]
    fn test_api_versions_response_encode_v3_flexible() {
        let resp = ApiVersionsResponse::supported();
        let mut buf = BytesMut::new();
        let mut w = KafkaWriter::new(&mut buf);
        resp.encode(&mut w, 3).unwrap();
        // Should include tagged fields (empty = 1 byte for num_fields=0)
        assert!(buf.len() > 6);
    }
}
