use bytes::Bytes;
use tokio::time::Duration;

use sonic_rs::JsonValueTrait;

use shared_ws::core::{WsApplicationPingPong, WsPingPongStrategy};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BybitReqIdField {
    /// `reqId` (camelCase), commonly seen in Bybit v5 public channels.
    ReqId,
    /// `req_id` (snake_case), commonly used in private channels.
    ReqIdSnake,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BybitPingKeyMode {
    /// Use a fixed key and treat any `{"op":"pong"}` as a pong for that key.
    ///
    /// This matches Bybit public channel behavior observed in `trading-backend-poc`, where the
    /// server may not reliably echo request ids.
    Fixed(&'static str),
    /// Use a generated key and require the pong to contain a matching `reqId`/`req_id`.
    Generated,
}

fn now_epoch_ms_string() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    ms.to_string()
}

fn build_ping_json(req_id_field: Option<BybitReqIdField>, req_id: Option<&str>) -> Bytes {
    // Low-frequency; string formatting is fine and keeps dependencies minimal.
    let ts = now_epoch_ms_string();
    match (req_id_field, req_id) {
        (Some(BybitReqIdField::ReqId), Some(id)) => {
            Bytes::from(format!(r#"{{"op":"ping","reqId":"{id}","ts":"{ts}"}}"#))
        }
        (Some(BybitReqIdField::ReqIdSnake), Some(id)) => {
            Bytes::from(format!(r#"{{"op":"ping","req_id":"{id}","ts":"{ts}"}}"#))
        }
        _ => Bytes::from(format!(r#"{{"op":"ping","ts":"{ts}"}}"#)),
    }
}

fn parse_bybit_pong_key(payload: &Bytes) -> Option<(bool, Option<String>)> {
    let Ok(value) = sonic_rs::from_slice::<sonic_rs::Value>(payload.as_ref()) else {
        return None;
    };

    let op = value.get("op").and_then(|v| v.as_str())?;
    if !op.eq_ignore_ascii_case("pong") {
        return None;
    }

    let req = value
        .get("reqId")
        .or_else(|| value.get("req_id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Some((true, req))
}

/// Bybit JSON-level ping/pong strategy.
///
/// This is a convenience wrapper around [`WsApplicationPingPong`] with Bybit-specific defaults.
pub struct BybitJsonPingPong {
    inner: WsApplicationPingPong<
        Box<dyn Fn() -> (Bytes, String) + Send + Sync>,
        Box<dyn Fn(&Bytes) -> Option<String> + Send + Sync>,
    >,
}

impl BybitJsonPingPong {
    /// Public-channel default: fixed key (`"public_pong"`) and `reqId` field.
    pub fn public(interval: Duration, timeout: Duration) -> Self {
        Self::fixed(interval, timeout, BybitReqIdField::ReqId, "public_pong")
    }

    /// Private-channel default: generated `reqId` and require a matching `reqId` in the pong.
    ///
    /// This mirrors `trading-backend-poc`'s private ping strategy, which uses Bybit's `reqId`
    /// field (camelCase) for request correlation.
    pub fn private(interval: Duration, timeout: Duration) -> Self {
        use std::sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        };

        let counter = Arc::new(AtomicU64::new(1));
        let counter_clone = counter.clone();

        let create: Box<dyn Fn() -> (Bytes, String) + Send + Sync> = Box::new(move || {
            let id = counter_clone.fetch_add(1, Ordering::Relaxed);
            let key = format!("ping_{id}");
            let payload = build_ping_json(Some(BybitReqIdField::ReqId), Some(&key));
            (payload, key)
        });

        let parse: Box<dyn Fn(&Bytes) -> Option<String> + Send + Sync> = Box::new(move |payload: &Bytes| {
            let Some((_is_pong, req)) = parse_bybit_pong_key(payload) else {
                return None;
            };
            req
        });

        Self {
            inner: WsApplicationPingPong::new(interval, timeout, create, parse).with_max_pending(8),
        }
    }

    /// Fixed-key mode: treat any `{"op":"pong"}` as a pong for `key`.
    pub fn fixed(
        interval: Duration,
        timeout: Duration,
        req_id_field: BybitReqIdField,
        key: &'static str,
    ) -> Self {
        let create: Box<dyn Fn() -> (Bytes, String) + Send + Sync> = Box::new(move || {
            let payload = build_ping_json(Some(req_id_field), Some(key));
            (payload, key.to_string())
        });
        let parse: Box<dyn Fn(&Bytes) -> Option<String> + Send + Sync> =
            Box::new(move |payload: &Bytes| {
            let Some((is_pong, req)) = parse_bybit_pong_key(payload) else {
                return None;
            };
            if !is_pong {
                return None;
            }
            Some(req.unwrap_or_else(|| key.to_string()))
        });
        Self {
            inner: WsApplicationPingPong::new(interval, timeout, create, parse).with_max_pending(8),
        }
    }

    /// Generated-key mode: embed a unique `req_id` and require it to match in the pong.
    pub fn generated(interval: Duration, timeout: Duration) -> Self {
        let create: Box<dyn Fn() -> (Bytes, String) + Send + Sync> = Box::new(move || {
            let key = format!("ping_{}", now_epoch_ms_string());
            let payload = build_ping_json(Some(BybitReqIdField::ReqIdSnake), Some(&key));
            (payload, key)
        });
        let parse: Box<dyn Fn(&Bytes) -> Option<String> + Send + Sync> =
            Box::new(move |payload: &Bytes| {
            let Some((is_pong, req)) = parse_bybit_pong_key(payload) else {
                return None;
            };
            if !is_pong {
                return None;
            }
            req
        });
        Self {
            inner: WsApplicationPingPong::new(interval, timeout, create, parse).with_max_pending(8),
        }
    }
}

impl WsPingPongStrategy for BybitJsonPingPong {
    fn create_ping(&mut self) -> Option<shared_ws::core::WsFrame> {
        self.inner.create_ping()
    }

    fn handle_inbound(&mut self, message: &shared_ws::core::WsFrame) -> shared_ws::core::WsPongResult {
        self.inner.handle_inbound(message)
    }

    fn is_stale(&self) -> bool {
        self.inner.is_stale()
    }

    fn reset(&mut self) {
        self.inner.reset()
    }

    fn interval(&self) -> Duration {
        self.inner.interval()
    }

    fn timeout(&self) -> Duration {
        self.inner.timeout()
    }
}

