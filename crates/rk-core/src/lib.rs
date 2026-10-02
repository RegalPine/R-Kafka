//! rk-core: R-Kafka 核心公共库
//!
//! 提供全局类型定义、统一错误模型和配置加载。

pub mod config;
pub mod error;
pub mod types;

pub use config::BrokerConfig;
pub use config::SaslUserConfig;
pub use error::{Result, RkError};
pub use types::*;
