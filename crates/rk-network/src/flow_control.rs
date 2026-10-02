//! 流控 (Backpressure)
//!
//! 防止 Producer 洪峰压垮 Broker。

use rk_core::BrokerConfig;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 流控制器
pub struct FlowController {
    max_connections: usize,
    max_request_size: usize,
    max_pending_bytes: usize,
    current_connections: AtomicUsize,
    current_pending_bytes: AtomicUsize,
}

impl FlowController {
    pub fn new(config: &BrokerConfig) -> Self {
        Self {
            max_connections: config.network.max_connections,
            max_request_size: config.network.max_request_size,
            max_pending_bytes: config.network.max_pending_bytes,
            current_connections: AtomicUsize::new(0),
            current_pending_bytes: AtomicUsize::new(0),
        }
    }

    /// 检查是否允许新连接
    pub fn try_accept_connection(&self) -> rk_core::Result<()> {
        let current = self.current_connections.load(Ordering::Relaxed);
        if current >= self.max_connections {
            return Err(rk_core::RkError::TooManyConnections {
                current,
                max: self.max_connections,
            });
        }
        self.current_connections.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 连接关闭时调用
    pub fn on_connection_close(&self) {
        self.current_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// 检查请求大小是否超限
    pub fn check_request_size(&self, size: usize) -> rk_core::Result<()> {
        if size > self.max_request_size {
            return Err(rk_core::RkError::RequestTooLarge {
                size,
                max: self.max_request_size,
            });
        }
        Ok(())
    }

    /// 增加待处理字节数
    pub fn add_pending_bytes(&self, bytes: usize) -> rk_core::Result<()> {
        let current = self
            .current_pending_bytes
            .fetch_add(bytes, Ordering::Relaxed)
            + bytes;
        if current > self.max_pending_bytes {
            return Err(rk_core::RkError::BackpressureExceeded {
                current,
                max: self.max_pending_bytes,
            });
        }
        Ok(())
    }

    /// 减少待处理字节数
    pub fn release_pending_bytes(&self, bytes: usize) {
        self.current_pending_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }

    /// 获取当前活跃连接数
    pub fn current_connections(&self) -> usize {
        self.current_connections.load(Ordering::Relaxed)
    }

    /// 获取当前待处理字节数
    pub fn current_pending_bytes(&self) -> usize {
        self.current_pending_bytes.load(Ordering::Relaxed)
    }

    /// 获取最大连接数限制
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// 获取流控统计快照
    pub fn snapshot(&self) -> FlowControlSnapshot {
        FlowControlSnapshot {
            current_connections: self.current_connections(),
            max_connections: self.max_connections,
            current_pending_bytes: self.current_pending_bytes(),
            max_pending_bytes: self.max_pending_bytes,
            max_request_size: self.max_request_size,
        }
    }
}

/// 流控统计快照
#[derive(Debug, Clone)]
pub struct FlowControlSnapshot {
    pub current_connections: usize,
    pub max_connections: usize,
    pub current_pending_bytes: usize,
    pub max_pending_bytes: usize,
    pub max_request_size: usize,
}
