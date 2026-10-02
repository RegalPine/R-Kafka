//! rk-core: R-Kafka 核心公共库
//!
//! 提供全局类型定义、统一错误模型和配置加载。

pub mod types;
pub mod error;
pub mod config;

pub use types::*;
pub use error::{RkError, Result};
pub use config::BrokerConfig;
pub use config::SaslUserConfig;
