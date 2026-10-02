//! ListTransactions Handler
//!
//! 处理 ListTransactions 请求 (API Key = 65)。
//! Phase 1: 从 ProducerStateManager 查询活跃事务。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::list_transactions::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::producer_state_manager::ProducerStateManager;

/// ListTransactions 请求处理器
pub struct ListTransactionsHandler {
    producer_state_manager: Arc<ProducerStateManager>,
}

impl ListTransactionsHandler {
    pub fn new(producer_state_manager: Arc<ProducerStateManager>) -> Self {
        Self { producer_state_manager }
    }

    /// 处理 ListTransactions 请求
    pub fn handle(
        &self,
        request: ListTransactionsRequest,
        _version: i16,
    ) -> Result<ListTransactionsResponse> {
        debug!(
            prefixes = request.transactional_id_prefixes.len(),
            state_filters = request.states.len(),
            "ListTransactions request"
        );

        // 从 ProducerStateManager 获取所有活跃事务
        let all_txns = self.producer_state_manager.list_active_transactions();

        let mut transaction_states = Vec::new();
        for txn_info in all_txns {
            // 按 transactional_id 前缀过滤
            if !request.transactional_id_prefixes.is_empty() {
                let matches = request.transactional_id_prefixes.iter()
                    .any(|prefix| txn_info.transactional_id.starts_with(prefix));
                if !matches {
                    continue;
                }
            }

            // 按状态过滤
            if !request.states.is_empty() {
                if !request.states.contains(&txn_info.state_str) {
                    continue;
                }
            }

            transaction_states.push(ListTransactionsResponseState {
                transactional_id: txn_info.transactional_id,
                producer_id: txn_info.producer_id,
                transaction_state: txn_info.state_str,
                transaction_timeout_ms: txn_info.timeout_ms,
                transaction_start_time_ms: txn_info.start_time_ms,
            });
        }

        Ok(ListTransactionsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            transaction_states,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_transactions_empty() {
        let psm = Arc::new(ProducerStateManager::new());
        let handler = ListTransactionsHandler::new(psm);
        let req = ListTransactionsRequest {
            transactional_id_prefixes: vec![],
            states: vec![],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert!(resp.transaction_states.is_empty());
    }
}
