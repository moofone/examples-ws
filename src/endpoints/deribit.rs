//! Deribit JSON-RPC websocket protocol helpers.
//!
//! This module integrates Deribit's websocket flow into `shared-ws` by providing:
//! - A subscription manager that emits Deribit `public/subscribe` requests.
//! - A baseline endpoint handler that parses Deribit subscription notifications.
//!
//! The handler is intentionally minimal and allocation-conscious:
//! - It overrides `parse_frame` to clone the underlying `Bytes` (refcount bump, zero-copy).
//! - It does not allocate full JSON objects for high-volume messages.

use std::collections::BTreeSet;

use bytes::Bytes;
use sonic_rs::JsonValueTrait;
use thiserror::Error;

use shared_ws::core::{
    WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction, WsFrame,
    WsMessageAction, WsParseOutcome, WsSubscriptionAction, WsSubscriptionManager,
    WsSubscriptionStatus,
};

/// Deribit channel identifier, e.g. `"ticker.BTC-PERPETUAL.raw"` or `"book.BTC-PERPETUAL.10.100ms"`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeribitChannel(pub String);

impl From<&str> for DeribitChannel {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Subscription operation to apply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeribitSubOp {
    Subscribe,
    Unsubscribe,
}

/// Subscription request produced by the subscription manager and serialized onto the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeribitSubscriptionRequest {
    pub op: DeribitSubOp,
    pub channels: Vec<DeribitChannel>,
}

impl DeribitSubscriptionRequest {
    pub fn subscribe(channels: Vec<DeribitChannel>) -> Self {
        Self {
            op: DeribitSubOp::Subscribe,
            channels,
        }
    }

    pub fn unsubscribe(channels: Vec<DeribitChannel>) -> Self {
        Self {
            op: DeribitSubOp::Unsubscribe,
            channels,
        }
    }
}

#[derive(Debug, Error)]
pub enum DeribitProtocolError {
    #[error("invalid json")]
    InvalidJson,
}

/// Parsed Deribit events surfaced by [`DeribitPublicHandler`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeribitEvent {
    /// Deribit subscription notification (`method == "subscription"`).
    Subscription {
        /// Channel identifier from `params.channel`.
        channel: DeribitChannel,
        /// Full raw payload (zero-copy via `Bytes` refcounting).
        payload: Bytes,
    },
}

/// Subscription manager for Deribit public subscriptions.
///
/// Maintains desired channels and emits `public/subscribe` / `public/unsubscribe` JSON-RPC requests.
#[derive(Clone, Debug)]
pub struct DeribitSubscriptionManager {
    desired: BTreeSet<DeribitChannel>,
    next_id: u64,
}

impl DeribitSubscriptionManager {
    pub fn new() -> Self {
        Self {
            desired: BTreeSet::new(),
            next_id: 1,
        }
    }

    pub fn with_initial_channels(channels: impl IntoIterator<Item = DeribitChannel>) -> Self {
        let mut mgr = Self::new();
        for ch in channels {
            mgr.desired.insert(ch);
        }
        mgr
    }

    pub fn desired_len(&self) -> usize {
        self.desired.len()
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
    }

    fn serialize_jsonrpc(&mut self, method: &str, channels: &[DeribitChannel]) -> Vec<u8> {
        // Subscription changes are low-frequency; keep serialization simple and correct.
        // Use a stable JSON-RPC envelope that Deribit accepts.
        let id = self.alloc_id();
        let mut out = String::with_capacity(128 + channels.len() * 48);
        out.push_str("{\"jsonrpc\":\"2.0\",\"id\":");
        out.push_str(&id.to_string());
        out.push_str(",\"method\":\"");
        out.push_str(method);
        out.push_str("\",\"params\":{\"channels\":[");
        for (i, ch) in channels.iter().enumerate() {
            if i != 0 {
                out.push(',');
            }
            // Channel strings are ASCII-ish; escape conservatively by replacing quotes/backslashes.
            out.push('"');
            for c in ch.0.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    _ => out.push(c),
                }
            }
            out.push('"');
        }
        out.push_str("]}}\n");
        out.into_bytes()
    }
}

impl Default for DeribitSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WsSubscriptionManager for DeribitSubscriptionManager {
    type SubscriptionMessage = DeribitSubscriptionRequest;

    fn initial_subscriptions(&mut self) -> Vec<Self::SubscriptionMessage> {
        if self.desired.is_empty() {
            return Vec::new();
        }
        vec![DeribitSubscriptionRequest::subscribe(
            self.desired.iter().cloned().collect(),
        )]
    }

    fn update_subscriptions(
        &mut self,
        action: WsSubscriptionAction<Self::SubscriptionMessage>,
    ) -> Result<Vec<Self::SubscriptionMessage>, String> {
        // Keep semantics simple:
        // - `Add`: apply subscribe requests and emit them.
        // - `Remove`: apply unsubscribe requests and emit them.
        // - `Replace`: clear then apply requests, emitting them.
        // - `Clear`: emit an unsubscribe of all desired channels (if any).
        match action {
            WsSubscriptionAction::Add(reqs) => {
                for req in &reqs {
                    match req.op {
                        DeribitSubOp::Subscribe => {
                            for ch in &req.channels {
                                self.desired.insert(ch.clone());
                            }
                        }
                        DeribitSubOp::Unsubscribe => {}
                    }
                }
                Ok(reqs)
            }
            WsSubscriptionAction::Remove(reqs) => {
                for req in &reqs {
                    match req.op {
                        DeribitSubOp::Unsubscribe => {
                            for ch in &req.channels {
                                self.desired.remove(ch);
                            }
                        }
                        DeribitSubOp::Subscribe => {}
                    }
                }
                Ok(reqs)
            }
            WsSubscriptionAction::Replace(reqs) => {
                self.desired.clear();
                for req in &reqs {
                    if matches!(req.op, DeribitSubOp::Subscribe) {
                        for ch in &req.channels {
                            self.desired.insert(ch.clone());
                        }
                    }
                }
                Ok(reqs)
            }
            WsSubscriptionAction::Clear => {
                if self.desired.is_empty() {
                    return Ok(Vec::new());
                }
                let channels: Vec<_> = self.desired.iter().cloned().collect();
                self.desired.clear();
                Ok(vec![DeribitSubscriptionRequest::unsubscribe(channels)])
            }
        }
    }

    fn serialize_subscription(&mut self, msg: &Self::SubscriptionMessage) -> Vec<u8> {
        match msg.op {
            DeribitSubOp::Subscribe => {
                self.serialize_jsonrpc("public/subscribe", msg.channels.as_slice())
            }
            DeribitSubOp::Unsubscribe => {
                self.serialize_jsonrpc("public/unsubscribe", msg.channels.as_slice())
            }
        }
    }

    fn handle_subscription_response(&mut self, _data: &[u8]) -> WsSubscriptionStatus {
        // Deribit subscription ACKs are regular JSON-RPC responses (id/result/error).
        // Keep this lightweight: detect success/failure and surface a message for failures.
        let data = _data;

        // Most frames are subscription notifications without `id`.
        let id = match sonic_rs::get(data, &["id"]) {
            Ok(v) => v.as_u64(),
            Err(_) => None,
        };
        let Some(_id) = id else {
            return WsSubscriptionStatus::NotSubscriptionResponse;
        };

        // If it has an `error` object, treat as an ACK failure.
        let error_message = sonic_rs::get(data, &["error", "message"])
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()));

        if error_message.is_some() || sonic_rs::get(data, &["error"]).is_ok() {
            let code = sonic_rs::get(data, &["error", "code"])
                .ok()
                .and_then(|v| v.as_i64());
            let mut msg = String::from("deribit jsonrpc error");
            if let Some(code) = code {
                msg.push_str(&format!(" code={code}"));
            }
            if let Some(m) = error_message {
                msg.push_str(&format!(" message={m}"));
            }
            return WsSubscriptionStatus::Acknowledged {
                success: false,
                message: Some(msg),
            };
        }

        // If it has a `result`, treat as an ACK success. Otherwise ignore; some Deribit frames
        // include `id` for other calls that the adapter doesn't care about.
        if sonic_rs::get(data, &["result"]).is_ok() {
            return WsSubscriptionStatus::Acknowledged {
                success: true,
                message: None,
            };
        }

        WsSubscriptionStatus::NotSubscriptionResponse
    }
}

/// Minimal Deribit public endpoint handler.
///
/// Produces [`DeribitEvent::Subscription`] messages for subscription notifications.
#[derive(Clone, Debug)]
pub struct DeribitPublicHandler {
    subs: DeribitSubscriptionManager,
}

impl DeribitPublicHandler {
    pub fn new(subs: DeribitSubscriptionManager) -> Self {
        Self { subs }
    }
}

impl WsEndpointHandler for DeribitPublicHandler {
    type Message = DeribitEvent;
    type Error = DeribitProtocolError;
    type Subscription = DeribitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        &mut self.subs
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        None
    }

    fn parse(&mut self, _data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        // The actor calls `parse_frame` for this handler (zero-copy).
        Err(DeribitProtocolError::InvalidJson)
    }

    fn parse_frame(
        &mut self,
        frame: &WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        let payload: &Bytes = match frame {
            WsFrame::Text(b) => b,
            WsFrame::Binary(b) => b,
            WsFrame::Ping(_) | WsFrame::Pong(_) | WsFrame::Close(_) => {
                return Ok(WsParseOutcome::Message(WsMessageAction::Continue));
            }
        };

        // Fast-path: extract "method" and check for subscription notifications.
        // Use sonic_rs lazy field access to avoid full deserialization.
        let is_subscription = match sonic_rs::get(payload, &["method"]) {
            Ok(lv) => matches!(lv.as_str(), Some("subscription")),
            Err(_) => false,
        };

        if is_subscription {
            let channel = sonic_rs::get(payload, &["params", "channel"])
                .ok()
                .and_then(|v| v.as_str().map(|s| DeribitChannel(s.to_string())));

            if let Some(channel) = channel {
                return Ok(WsParseOutcome::Message(WsMessageAction::Process(
                    DeribitEvent::Subscription {
                        channel,
                        payload: payload.clone(),
                    },
                )));
            }
        }

        // Treat all other messages as control/ack frames for now.
        Ok(WsParseOutcome::Message(WsMessageAction::Continue))
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

    fn classify_disconnect(&self, cause: &WsDisconnectCause) -> WsDisconnectAction {
        // Default to backoff reconnects (Deribit legacy behavior).
        //
        // Allow callers to perform an explicit "stay disconnected" by using a sentinel reason.
        match cause {
            WsDisconnectCause::EndpointRequested { reason }
                if reason == "disconnect requested" =>
            {
                WsDisconnectAction::Abort
            }
            _ => WsDisconnectAction::BackoffReconnect,
        }
    }
}
