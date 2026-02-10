//! Bybit websocket protocol helpers.
//!
//! This module integrates Bybit's websocket flow into `shared-ws` by providing:
//! - A subscription manager that emits Bybit `subscribe` / `unsubscribe` requests.
//! - A minimal endpoint handler that extracts topic notifications.
//!
//! Bybit uses JSON-level ping/pong (`{"op":"ping"}` / `{"op":"pong"}`), which is handled
//! by `crate::bybit::ping::BybitJsonPingPong` (or the generic `WsApplicationPingPong`).

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use sonic_rs::JsonValueTrait;
use thiserror::Error;

use shared_ws::ws::{
    WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction, WsFrame,
    WsMessageAction, WsParseOutcome, WsSubscriptionAction, WsSubscriptionManager,
    WsSubscriptionStatus,
};

/// Bybit topic identifier, e.g. `"publicTrade.BTCUSDT"` or `"order"`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BybitTopic(pub String);

impl From<&str> for BybitTopic {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Subscription operation to apply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BybitSubOp {
    Subscribe,
    Unsubscribe,
}

/// Subscription request produced by the subscription manager and serialized onto the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BybitSubscriptionRequest {
    pub op: BybitSubOp,
    pub topics: Vec<BybitTopic>,
}

impl BybitSubscriptionRequest {
    pub fn subscribe(topics: Vec<BybitTopic>) -> Self {
        Self {
            op: BybitSubOp::Subscribe,
            topics,
        }
    }

    pub fn unsubscribe(topics: Vec<BybitTopic>) -> Self {
        Self {
            op: BybitSubOp::Unsubscribe,
            topics,
        }
    }
}

#[derive(Debug, Error)]
pub enum BybitProtocolError {
    #[error("invalid json")]
    InvalidJson,
}

/// Parsed Bybit events surfaced by [`BybitTopicHandler`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BybitEvent {
    /// Bybit topic notification (`topic` field present).
    Topic { topic: BybitTopic, payload: Bytes },
}

/// Subscription manager for Bybit v5 topic subscriptions.
///
/// Maintains a desired topic set and emits `subscribe` / `unsubscribe` JSON requests.
#[derive(Clone, Debug)]
pub struct BybitSubscriptionManager {
    desired: BTreeSet<BybitTopic>,
}

impl BybitSubscriptionManager {
    pub fn new() -> Self {
        Self {
            desired: BTreeSet::new(),
        }
    }

    pub fn with_initial_topics(topics: impl IntoIterator<Item = BybitTopic>) -> Self {
        let mut mgr = Self::new();
        for t in topics {
            mgr.desired.insert(t);
        }
        mgr
    }

    pub fn desired_len(&self) -> usize {
        self.desired.len()
    }

    fn serialize_op(op: BybitSubOp, topics: &[BybitTopic]) -> Vec<u8> {
        let method = match op {
            BybitSubOp::Subscribe => "subscribe",
            BybitSubOp::Unsubscribe => "unsubscribe",
        };

        // Subscription changes are low frequency; keep serialization correct and dependency-free.
        let mut out = String::with_capacity(32 + topics.len() * 48);
        out.push_str("{\"op\":\"");
        out.push_str(method);
        out.push_str("\",\"args\":[");
        for (i, t) in topics.iter().enumerate() {
            if i != 0 {
                out.push(',');
            }
            out.push('"');
            for c in t.0.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    _ => out.push(c),
                }
            }
            out.push('"');
        }
        out.push_str("]}");
        out.into_bytes()
    }
}

impl Default for BybitSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WsSubscriptionManager for BybitSubscriptionManager {
    type SubscriptionMessage = BybitSubscriptionRequest;

    fn maybe_subscription_response(&self, data: &[u8]) -> bool {
        // Bybit subscription ACK frames contain `"op":"subscribe"` / `"op":"unsubscribe"`.
        // Most high-volume topic notifications do not include `"op"`.
        memchr::memmem::find(data, b"\"op\"").is_some()
    }

    fn initial_subscriptions(&mut self) -> Vec<Self::SubscriptionMessage> {
        if self.desired.is_empty() {
            return Vec::new();
        }
        vec![BybitSubscriptionRequest::subscribe(
            self.desired.iter().cloned().collect(),
        )]
    }

    fn update_subscriptions(
        &mut self,
        action: WsSubscriptionAction<Self::SubscriptionMessage>,
    ) -> Result<Vec<Self::SubscriptionMessage>, String> {
        match action {
            WsSubscriptionAction::Add(reqs) => {
                for req in &reqs {
                    if matches!(req.op, BybitSubOp::Subscribe) {
                        for t in &req.topics {
                            self.desired.insert(t.clone());
                        }
                    }
                }
                Ok(reqs)
            }
            WsSubscriptionAction::Remove(reqs) => {
                for req in &reqs {
                    if matches!(req.op, BybitSubOp::Unsubscribe) {
                        for t in &req.topics {
                            self.desired.remove(t);
                        }
                    }
                }
                Ok(reqs)
            }
            WsSubscriptionAction::Replace(reqs) => {
                self.desired.clear();
                for req in &reqs {
                    if matches!(req.op, BybitSubOp::Subscribe) {
                        for t in &req.topics {
                            self.desired.insert(t.clone());
                        }
                    }
                }
                Ok(reqs)
            }
            WsSubscriptionAction::Clear => {
                if self.desired.is_empty() {
                    return Ok(Vec::new());
                }
                let topics: Vec<_> = self.desired.iter().cloned().collect();
                self.desired.clear();
                Ok(vec![BybitSubscriptionRequest::unsubscribe(topics)])
            }
        }
    }

    fn serialize_subscription(&mut self, msg: &Self::SubscriptionMessage) -> Vec<u8> {
        match msg.op {
            BybitSubOp::Subscribe => Self::serialize_op(BybitSubOp::Subscribe, &msg.topics),
            BybitSubOp::Unsubscribe => Self::serialize_op(BybitSubOp::Unsubscribe, &msg.topics),
        }
    }

    fn handle_subscription_response(&mut self, data: &[u8]) -> WsSubscriptionStatus {
        // Fast-path: skip parsing if it doesn't look like an ACK.
        if memchr::memmem::find(data, b"\"op\"").is_none() {
            return WsSubscriptionStatus::NotSubscriptionResponse;
        }

        // Parse only ACK frames (low-frequency) into a Value for ergonomic field access.
        let Ok(value) = sonic_rs::from_slice::<sonic_rs::Value>(data) else {
            return WsSubscriptionStatus::NotSubscriptionResponse;
        };

        let Some(op) = value.get("op").and_then(|v| v.as_str()) else {
            return WsSubscriptionStatus::NotSubscriptionResponse;
        };

        if op != "subscribe" && op != "unsubscribe" {
            return WsSubscriptionStatus::NotSubscriptionResponse;
        }

        let success = value
            .get("success")
            .and_then(|v| v.as_bool())
            .or_else(|| {
                value
                    .get("retCode")
                    .and_then(|v| v.as_i64())
                    .map(|code| code == 0)
            })
            .unwrap_or(false);

        let message = value
            .get("retMsg")
            .or_else(|| value.get("ret_msg"))
            .and_then(|v| v.as_str())
            .map(|msg| {
                let code = value
                    .get("retCode")
                    .and_then(|v| v.as_i64())
                    .filter(|code| *code != 0);
                match (success, code) {
                    (false, Some(code)) => format!("{msg} (code {code})"),
                    _ => msg.to_string(),
                }
            });

        WsSubscriptionStatus::Acknowledged { success, message }
    }
}

/// Minimal endpoint handler that extracts `topic` notifications.
#[derive(Clone)]
pub struct BybitTopicHandler {
    subs: BybitSubscriptionManager,
    auth_provider: Option<Arc<dyn Fn() -> Vec<u8> + Send + Sync>>,
}

impl BybitTopicHandler {
    pub fn new(subs: BybitSubscriptionManager) -> Self {
        Self {
            subs,
            auth_provider: None,
        }
    }

    /// Configure an auth payload generator. If present, the websocket actor will send an auth
    /// frame immediately after connecting (before initial subscriptions).
    ///
    /// This is intentionally a generic provider closure so `shared-ws` does not need to depend on
    /// any specific HMAC/signature crates.
    pub fn with_auth_provider(
        mut self,
        auth_provider: Arc<dyn Fn() -> Vec<u8> + Send + Sync>,
    ) -> Self {
        self.auth_provider = Some(auth_provider);
        self
    }
}

impl WsEndpointHandler for BybitTopicHandler {
    type Message = BybitEvent;
    type Error = BybitProtocolError;
    type Subscription = BybitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        &mut self.subs
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        self.auth_provider.as_ref().map(|f| (f)())
    }

    fn parse(&mut self, _data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        // Prefer `parse_frame` (zero-copy).
        Err(BybitProtocolError::InvalidJson)
    }

    fn parse_frame(
        &mut self,
        frame: &WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        let payload: &Bytes = match frame {
            WsFrame::Text(b) => b.as_bytes(),
            WsFrame::Binary(b) => b,
            WsFrame::Ping(_) | WsFrame::Pong(_) | WsFrame::Close(_) => {
                return Ok(WsParseOutcome::Message(WsMessageAction::Continue));
            }
        };

        // Ignore JSON-level pong frames.
        let is_pong = match sonic_rs::get(payload, &["op"]) {
            Ok(v) => v
                .as_str()
                .map(|s| s.eq_ignore_ascii_case("pong"))
                .unwrap_or(false),
            Err(_) => false,
        };
        if is_pong {
            return Ok(WsParseOutcome::Message(WsMessageAction::Continue));
        }

        // Surface Bybit error frames (`retCode != 0`) so callers can decide fatal vs reconnect.
        if let Ok(ret_code) = sonic_rs::get(payload, &["retCode"])
            && let Some(code) = ret_code.as_i64()
            && code != 0
        {
            let message = sonic_rs::get(payload, &["retMsg"])
                .ok()
                .and_then(|v| v.as_str().map(ToOwned::to_owned))
                .or_else(|| {
                    sonic_rs::get(payload, &["ret_msg"])
                        .ok()
                        .and_then(|v| v.as_str().map(ToOwned::to_owned))
                })
                .unwrap_or_else(|| "server error".to_string());

            // Best-effort: include the parsed JSON so endpoint handlers can inspect it.
            let data = sonic_rs::from_slice::<sonic_rs::Value>(payload.as_ref()).ok();
            return Ok(WsParseOutcome::ServerError {
                code: Some(code as i32),
                message,
                data,
            });
        }

        // Fast-path: extract `topic` for stream notifications.
        let topic = sonic_rs::get(payload, &["topic"])
            .ok()
            .and_then(|v| v.as_str().map(|s| BybitTopic(s.to_string())));

        if let Some(topic) = topic {
            return Ok(WsParseOutcome::Message(WsMessageAction::Process(
                BybitEvent::Topic {
                    topic,
                    payload: payload.clone(),
                },
            )));
        }

        Ok(WsParseOutcome::Message(WsMessageAction::Continue))
    }

    fn handle_message(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }

    fn handle_server_error(
        &mut self,
        code: Option<i32>,
        _message: &str,
        _data: Option<sonic_rs::Value>,
    ) -> WsErrorAction {
        match code {
            // Auth errors observed in `trading-backend-poc`.
            Some(10003) | Some(10004) => WsErrorAction::Fatal,
            // Rate limit or throttling; keep the connection and let upstream retry.
            Some(10006) | Some(10018) => WsErrorAction::Continue,
            _ => WsErrorAction::Reconnect,
        }
    }

    fn reset_state(&mut self) {}

    fn classify_disconnect(&self, cause: &WsDisconnectCause) -> WsDisconnectAction {
        match cause {
            WsDisconnectCause::EndpointRequested { reason } if reason == "disconnect requested" => {
                WsDisconnectAction::Abort
            }
            _ => WsDisconnectAction::BackoffReconnect,
        }
    }
}
