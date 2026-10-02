//! Client Error — 客户端错误类型

use thiserror::Error;

/// 客户端错误
#[derive(Debug, Error)]
pub enum ClientError {
    /// IO 错误
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// 协议错误
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// 连接错误
    #[error("Connection error: {0}")]
    Connection(String),

    /// 超时
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Broker 返回错误码
    #[error("Kafka error: {0}")]
    Kafka(String),

    /// 序列化/反序列化
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// 配置错误
    #[error("Config error: {0}")]
    Config(String),

    /// 无 Leader
    #[error("No leader for partition {0}-{1}")]
    NoLeader(String, i32),

    /// 内部错误
    #[error("Internal error: {0}")]
    Internal(String),
}

/// 客户端结果
pub type ClientResult<T> = std::result::Result<T, ClientError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ClientError::Connection("refused".to_string());
        assert_eq!(err.to_string(), "Connection error: refused");

        let err = ClientError::NoLeader("test".to_string(), 0);
        assert!(err.to_string().contains("test"));
        assert!(err.to_string().contains("0"));
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let err: ClientError = io_err.into();
        assert!(err.to_string().contains("broken"));
    }
}
