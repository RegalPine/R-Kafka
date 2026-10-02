//! rk-security: R-Kafka 安全模块
//!
//! 提供传输加密、认证和授权能力:
//!
//! - **TLS/mTLS**: 基于 rustls 的传输层加密
//! - **SASL**: PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 认证
//! - **ACL**: Kafka 风格的访问控制列表
//! - **Audit**: 安全事件审计日志
//!
//! ```text
//! rk-security 架构:
//!
//! ┌──────────────────────────────────────────────┐
//! │              rk-security                      │
//! │                                               │
//! │  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │
//! │  │   TLS   │  │  SASL   │  │    ACL      │  │
//! │  │ (rustls)│  │ (SCRAM) │  │  (Engine)   │  │
//! │  └────┬────┘  └────┬────┘  └──────┬──────┘  │
//! │       │            │              │          │
//! │       └────────────┴──────────────┘          │
//! │                     │                         │
//! │              ┌──────┴──────┐                  │
//! │              │ AuditLogger │                  │
//! │              └─────────────┘                  │
//! └──────────────────────────────────────────────┘
//! ```

pub mod acl;
pub mod api_permissions;
pub mod audit;
pub mod auth_pipeline;
pub mod sasl;
pub mod tls;

// Re-exports
pub use acl::{
    group_read_acl, prefix_acl, super_user_acl, topic_read_acl, topic_write_acl, AclEngine,
    AclEntry, AclOperation, PermissionType, ResourcePattern, ResourceType,
};
pub use api_permissions::{
    api_permission, api_required_operation, api_required_resource_type, is_pre_auth_api,
    ApiPermission,
};
pub use audit::{AuditConfig, AuditEvent, AuditEventType, AuditLogger};
pub use auth_pipeline::{
    AuthzResult, SecurityContext, SecurityPipeline, SecurityPipelineConfig, SecurityPipelineSummary,
};
pub use sasl::{AuthState, SaslConfig, SaslMechanism, SaslSession, UserCredentials, UserDatabase};
pub use tls::{
    create_tls_acceptor, create_tls_connector, load_ca_certs, load_certs, load_private_key,
    parse_certificate_info, CertificateInfo, SecureStream, TlsClientConfig, TlsConfig,
    TlsConnectionState, TlsMode, TlsProtocolVersion,
};
