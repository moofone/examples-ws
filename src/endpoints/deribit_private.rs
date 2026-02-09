//! Deribit JSON-RPC delegated-reply helpers.
//!
//! This module provides a minimal [`WsEndpointHandler`] implementation that enables
//! `ask(WsDelegatedRequest { confirm_mode: Confirmed, .. })` flows for Deribit-style JSON-RPC
//! over WebSocket.
//!
//! The handler intentionally:
//! - does not emit any messages downstream (it only matches request/response),
//! - treats any JSON-RPC `result` as success, and any `error` as rejection,
//! - additionally validates `result.order.order_state == "open"` when that field exists, so
//!   order placement replies can be sanity-checked in end-to-end tests.

use memchr::memmem;

use sonic_rs::JsonValueTrait;

use shared_ws::ws::{
    WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction, WsFrame,
    WsMessageAction, WsParseOutcome, WsRequestMatch, WsSubscriptionAction, WsSubscriptionManager,
    WsSubscriptionStatus,
};

/// No-op subscription manager for pure request/response websocket APIs.
#[derive(Debug, Default, Clone)]
pub struct NoSubscriptions;

impl WsSubscriptionManager for NoSubscriptions {
    type SubscriptionMessage = ();

    fn initial_subscriptions(&mut self) -> Vec<Self::SubscriptionMessage> {
        Vec::new()
    }

    fn update_subscriptions(
        &mut self,
        _action: WsSubscriptionAction<Self::SubscriptionMessage>,
    ) -> Result<Vec<Self::SubscriptionMessage>, String> {
        Ok(Vec::new())
    }

    fn serialize_subscription(&mut self, _msg: &Self::SubscriptionMessage) -> Vec<u8> {
        Vec::new()
    }

    fn handle_subscription_response(&mut self, _data: &[u8]) -> WsSubscriptionStatus {
        WsSubscriptionStatus::NotSubscriptionResponse
    }
}

/// A lightweight Deribit JSON-RPC matcher.
#[derive(Debug, Default, Clone)]
pub struct DeribitJsonRpcMatcher {
    subs: NoSubscriptions,
}

impl DeribitJsonRpcMatcher {
    pub fn new() -> Self {
        Self {
            subs: NoSubscriptions,
        }
    }
}

fn parse_jsonrpc_id(data: &[u8]) -> Option<u64> {
    let v = sonic_rs::get(data, &["id"]).ok()?;
    v.as_u64().or_else(|| {
        v.as_i64()
            .and_then(|i| if i >= 0 { Some(i as u64) } else { None })
    })
}

impl WsEndpointHandler for DeribitJsonRpcMatcher {
    type Message = ();
    type Error = std::io::Error;
    type Subscription = NoSubscriptions;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        &mut self.subs
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        None
    }

    fn parse(&mut self, _data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        Ok(WsParseOutcome::Message(WsMessageAction::Continue))
    }

    fn parse_frame(
        &mut self,
        _frame: &WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        Ok(WsParseOutcome::Message(WsMessageAction::Continue))
    }

    fn maybe_request_response(&self, data: &[u8]) -> bool {
        memmem::find(data, b"\"id\"").is_some()
    }

    fn match_request_response(&mut self, data: &[u8]) -> Option<WsRequestMatch> {
        let request_id = parse_jsonrpc_id(data)?;

        if sonic_rs::get(data, &["error"]).is_ok() {
            let code = sonic_rs::get(data, &["error", "code"])
                .ok()
                .and_then(|v| v.as_i64());
            let message = sonic_rs::get(data, &["error", "message"])
                .ok()
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "deribit jsonrpc error".to_string());

            let mut msg = message;
            if let Some(code) = code {
                msg.push_str(&format!(" (code {code})"));
            }

            return Some(WsRequestMatch {
                request_id,
                result: Err(msg),
                rate_limit_feedback: None,
            });
        }

        if sonic_rs::get(data, &["result"]).is_ok() {
            // If the response looks like an order placement response, validate the order state.
            if let Ok(state) = sonic_rs::get(data, &["result", "order", "order_state"]) {
                if let Some(state) = state.as_str() {
                    if state != "open" {
                        return Some(WsRequestMatch {
                            request_id,
                            result: Err(format!("deribit order_state is not open: {state}")),
                            rate_limit_feedback: None,
                        });
                    }
                }
            }

            return Some(WsRequestMatch {
                request_id,
                result: Ok(()),
                rate_limit_feedback: None,
            });
        }

        None
    }

    fn handle_message(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }

    fn handle_server_error(
        &mut self,
        _code: Option<i32>,
        _message: &str,
        _data: Option<sonic_rs::Value>,
    ) -> WsErrorAction {
        WsErrorAction::Continue
    }

    fn reset_state(&mut self) {}

    fn classify_disconnect(&self, _cause: &WsDisconnectCause) -> WsDisconnectAction {
        WsDisconnectAction::BackoffReconnect
    }
}
