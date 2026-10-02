//! R-Kafka 统一错误模型
//!
//! 所有模块使用 [`RkError`] 作为统一错误类型，按类别分组。

use thiserror::Error;

/// R-Kafka 统一错误类型
#[derive(Debug, Error)]
pub enum RkError {
    // ─── IO 错误 ─────────────────────────────────────────────────
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    // ─── 协议错误 ─────────────────────────────────────────────────
    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Unsupported API key: {0}")]
    UnsupportedApiKey(i16),

    #[error("Unsupported API version: key={key}, version={version}")]
    UnsupportedApiVersion { key: i16, version: i16 },

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Buffer underflow: need {need} bytes, have {have}")]
    BufferUnderflow { need: usize, have: usize },

    #[error("Invalid Kafka data type: {0}")]
    InvalidDataType(String),

    // ─── 存储错误 ─────────────────────────────────────────────────
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Segment not found: offset={0}")]
    SegmentNotFound(i64),

    #[error("Corrupted record: CRC mismatch at offset {0}")]
    CorruptedRecord(i64),

    #[error("Log directory not found: {0}")]
    LogDirNotFound(String),

    // ─── 配置错误 ─────────────────────────────────────────────────
    #[error("Config error: {0}")]
    Config(String),

    #[error("Config parse error: {0}")]
    ConfigParse(String),

    // ─── 流控错误 ─────────────────────────────────────────────────
    #[error("Request too large: {size} bytes (max {max})")]
    RequestTooLarge { size: usize, max: usize },

    #[error("Too many connections: {current} (max {max})")]
    TooManyConnections { current: usize, max: usize },

    #[error("Backpressure: pending bytes {current} exceeds limit {max}")]
    BackpressureExceeded { current: usize, max: usize },

    // ─── 副本错误 ─────────────────────────────────────────────────
    #[error("Not leader for partition {topic}-{partition}")]
    NotLeader { topic: String, partition: i32 },

    #[error("ISR shrink: replica {0} lagging")]
    IsrShrink(i32),

    // ─── 内部错误 ─────────────────────────────────────────────────
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Channel closed")]
    ChannelClosed,
}

/// 统一 Result 别名
pub type Result<T> = std::result::Result<T, RkError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let e = RkError::BufferUnderflow { need: 100, have: 50 };
        assert!(e.to_string().contains("100"));
        assert!(e.to_string().contains("50"));

        let e = RkError::RequestTooLarge { size: 200_000_000, max: 104_857_600 };
        assert!(e.to_string().contains("200000000"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let rk_err: RkError = io_err.into();
        assert!(matches!(rk_err, RkError::Io(_)));
    }
}
