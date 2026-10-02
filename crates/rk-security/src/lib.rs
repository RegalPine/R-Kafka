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

pub mod tls;
pub mod sasl;
pub mod acl;
pub mod audit;
pub mod auth_pipeline;
pub mod api_permissions;

// Re-exports
pub use tls::{
    TlsConfig, TlsMode, TlsProtocolVersion, TlsClientConfig,
    TlsConnectionState, SecureStream, CertificateInfo,
    create_tls_acceptor, create_tls_connector,
    load_certs, load_private_key, load_ca_certs,
    parse_certificate_info,
};
pub use sasl::{
    SaslMechanism, SaslConfig, SaslSession, AuthState,
    UserCredentials, UserDatabase,
};
pub use acl::{
    ResourceType, ResourcePattern, AclOperation, PermissionType,
    AclEntry, AclEngine,
    super_user_acl, topic_read_acl, topic_write_acl,
    group_read_acl, prefix_acl,
};
pub use audit::{
    AuditEventType, AuditEvent, AuditConfig, AuditLogger,
};
pub use auth_pipeline::{
    SecurityContext, SecurityPipelineConfig, SecurityPipeline,
    AuthzResult, SecurityPipelineSummary,
};
pub use api_permissions::{
    ApiPermission, api_permission, is_pre_auth_api,
    api_required_operation, api_required_resource_type,
};
