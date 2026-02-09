//! Deribit private websocket actor (authenticated JSON-RPC).
//!
//! This actor is intentionally minimal: it exists primarily to exercise the `shared-ws`
//! delegated reply API (`ask(WsDelegatedRequest { confirm_mode: Confirmed, .. })`) end-to-end.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use kameo::error::SendError;
use kameo::prelude::{Actor, ActorRef, Context, Message as KameoMessage, WeakActorRef};

use shared_rate_limiter::{Config, Cost, Outcome, RateLimiter, Scope};
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    ForwardAllIngress, GetConnectionStatus, ProtocolPingPong, WebSocketActor, WebSocketActorArgs,
    WebSocketBufferConfig, WebSocketEvent, WsConfirmMode, WsConnectionStatus, WsDelegatedError,
    WsDelegatedRequest, WsTlsConfig, into_ws_message,
};

use crate::endpoints::deribit_private::DeribitJsonRpcMatcher;

use super::reconnect::DeribitReconnect;

#[derive(Clone)]
pub struct DeribitAuthConfig {
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Clone)]
pub struct DeribitPrivateActorArgs {
    pub url: String,
    pub tls: WsTlsConfig,
    pub stale_threshold: Duration,
    pub enable_ping: bool,
    pub ping_interval: Duration,
    pub ping_timeout: Duration,
    pub ws_buffers: WebSocketBufferConfig,
    pub outbound_capacity: usize,
    pub reconnect: DeribitReconnect,
    pub auth: DeribitAuthConfig,
    /// Max time to wait for delegated confirmation.
    pub request_timeout: Duration,

    /// Local, in-process outbound limiter configuration.
    pub rate_limiter_cfg: Config,
    pub rate_limiter_key: shared_rate_limiter::Key,
    pub auth_cost: u32,
    pub order_cost: u32,
}

impl DeribitPrivateActorArgs {
    pub fn test_defaults(url: String) -> Self {
        Self {
            url,
            tls: WsTlsConfig::default(),
            stale_threshold: Duration::from_secs(30),
            enable_ping: false,
            ping_interval: Duration::from_secs(60),
            ping_timeout: Duration::from_secs(60),
            ws_buffers: WebSocketBufferConfig::default(),
            outbound_capacity: 128,
            reconnect: DeribitReconnect::default(),
            auth: DeribitAuthConfig {
                client_id: "test_client_id".to_string(),
                client_secret: "test_client_secret".to_string(),
            },
            request_timeout: Duration::from_secs(2),
            // Deribit uses a credit pool model; token bucket is a reasonable local approximation.
            rate_limiter_cfg: Config::token_bucket(50_000, 10_000),
            rate_limiter_key: 1,
            auth_cost: 500,
            order_cost: 500,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateOpenOrder {
    pub instrument: String,
    pub amount: f64,
    pub price: f64,
}

#[derive(Debug, Clone, kameo::Reply)]
pub struct CreateOpenOrderResponse {
    pub request_id: u64,
}

pub struct DeribitPrivateActor {
    args: DeribitPrivateActorArgs,
    ws: ActorRef<
        WebSocketActor<
            DeribitJsonRpcMatcher,
            DeribitReconnect,
            ProtocolPingPong,
            ForwardAllIngress<()>,
            TungsteniteTransport,
        >,
    >,
    limiter: RateLimiter,
    limiter_started_at: Instant,
    authenticated: bool,
    next_request_id: u64,
}

impl DeribitPrivateActor {
    fn alloc_request_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        id
    }

    fn limiter_now(&self) -> Duration {
        self.limiter_started_at.elapsed()
    }

    fn fingerprint(bytes: &[u8]) -> u64 {
        let mut h = DefaultHasher::new();
        bytes.hash(&mut h);
        h.finish()
    }

    fn push_escaped(out: &mut String, s: &str) {
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                _ => out.push(c),
            }
        }
    }

    fn build_auth_request(&self, request_id: u64) -> Vec<u8> {
        let mut out = String::with_capacity(256);
        out.push_str("{\"jsonrpc\":\"2.0\",\"id\":");
        out.push_str(&request_id.to_string());
        out.push_str(",\"method\":\"public/auth\",\"params\":{");
        out.push_str("\"grant_type\":\"client_credentials\",\"client_id\":\"");
        Self::push_escaped(&mut out, &self.args.auth.client_id);
        out.push_str("\",\"client_secret\":\"");
        Self::push_escaped(&mut out, &self.args.auth.client_secret);
        out.push_str("\"}}\n");
        out.into_bytes()
    }

    fn build_buy_request(
        &self,
        request_id: u64,
        instrument: &str,
        amount: f64,
        price: f64,
    ) -> Vec<u8> {
        let mut out = String::with_capacity(256);
        out.push_str("{\"jsonrpc\":\"2.0\",\"id\":");
        out.push_str(&request_id.to_string());
        out.push_str(",\"method\":\"private/buy\",\"params\":{");
        out.push_str("\"instrument_name\":\"");
        Self::push_escaped(&mut out, instrument);
        out.push_str("\",\"amount\":");
        out.push_str(&amount.to_string());
        out.push_str(",\"type\":\"limit\",\"price\":");
        out.push_str(&price.to_string());
        out.push_str("}}\n");
        out.into_bytes()
    }

    fn outcome_from_delegated_error(err: &WsDelegatedError) -> Outcome {
        match err {
            WsDelegatedError::NotDelivered { .. } => Outcome::NotSent,
            WsDelegatedError::Unconfirmed { .. } => Outcome::SentNoConfirm,
            WsDelegatedError::EndpointRejected {
                rate_limit_feedback,
                ..
            } => {
                let retry_after = rate_limit_feedback.as_ref().and_then(|f| f.retry_after);
                if let Some(retry_after) = retry_after {
                    Outcome::RateLimitedFeedback {
                        retry_after,
                        scope: Scope::Key,
                        bucket_hint: None,
                    }
                } else {
                    // The server replied; treat as spent capacity.
                    Outcome::Confirmed
                }
            }
            // Internal client-side errors (no send).
            WsDelegatedError::PayloadMismatch { .. } | WsDelegatedError::TooManyPending { .. } => {
                Outcome::NotSent
            }
        }
    }

    async fn send_jsonrpc_confirmed(
        &mut self,
        request_id: u64,
        payload: Vec<u8>,
        cost: u32,
    ) -> Result<(), String> {
        let Some(cost) = Cost::new(cost) else {
            return Err("invalid rate limiter cost (must be > 0)".to_string());
        };

        let now = self.limiter_now();
        let permit = self
            .limiter
            .try_acquire(self.args.rate_limiter_key, cost, now)
            .map_err(|deny| format!("locally rate limited: retry_after={:?}", deny.retry_after))?;

        let fingerprint = Self::fingerprint(payload.as_slice());
        let req = WsDelegatedRequest {
            request_id,
            fingerprint,
            frame: into_ws_message(payload),
            confirm_deadline: Instant::now() + self.args.request_timeout,
            confirm_mode: WsConfirmMode::Confirmed,
        };

        let res = self.ws.ask(req).await;

        let outcome = match &res {
            Ok(ok) => {
                if ok.confirmed {
                    Outcome::Confirmed
                } else {
                    Outcome::SentNoConfirm
                }
            }
            Err(SendError::HandlerError(err)) => Self::outcome_from_delegated_error(err),
            Err(_) => Outcome::NotSent,
        };

        // `commit` treats Outcome::NotSent as a defensive refund for fixed-window and token bucket.
        self.limiter.commit(permit, outcome, self.limiter_now());

        // Surface the delegated result as our API error.
        match res {
            Ok(ok) if ok.confirmed => Ok(()),
            Ok(_) => Err("request sent but not confirmed".to_string()),
            Err(err) => Err(format!("{err:?}")),
        }
    }

    async fn ensure_connected(&self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self
                .ws
                .ask(GetConnectionStatus)
                .await
                .map_err(|e| format!("{e:?}"))?;
            if matches!(status, WsConnectionStatus::Connected) {
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for ws connect within {timeout:?} (last={status:?})"
                ));
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Actor for DeribitPrivateActor {
    type Args = DeribitPrivateActorArgs;
    type Error = std::io::Error;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let limiter = RateLimiter::new(args.rate_limiter_cfg.clone());
        let limiter_started_at = Instant::now();

        let ping = ProtocolPingPong::new(args.ping_interval, args.ping_timeout);
        let ws = WebSocketActor::spawn(WebSocketActorArgs {
            url: args.url.clone(),
            tls: args.tls,
            transport: TungsteniteTransport::default(),
            reconnect_strategy: args.reconnect.clone(),
            handler: DeribitJsonRpcMatcher::new(),
            ingress: ForwardAllIngress::default(),
            ping_strategy: ping,
            enable_ping: args.enable_ping,
            stale_threshold: args.stale_threshold,
            ws_buffers: args.ws_buffers,
            outbound_capacity: args.outbound_capacity,
            circuit_breaker: None,
            latency_policy: None,
            payload_latency_sampling: None,
            registration: None,
            metrics: None,
        });

        let _ = ws.tell(WebSocketEvent::Connect).send().await;

        Ok(Self {
            args,
            ws,
            limiter,
            limiter_started_at,
            authenticated: false,
            next_request_id: 1,
        })
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: kameo::error::ActorStopReason,
    ) -> Result<(), Self::Error> {
        let _ = self.ws.stop_gracefully().await;
        Ok(())
    }
}

impl KameoMessage<CreateOpenOrder> for DeribitPrivateActor {
    type Reply = Result<CreateOpenOrderResponse, String>;

    async fn handle(
        &mut self,
        msg: CreateOpenOrder,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.ensure_connected(self.args.request_timeout).await?;

        if !self.authenticated {
            let auth_id = self.alloc_request_id();
            let payload = self.build_auth_request(auth_id);
            self.send_jsonrpc_confirmed(auth_id, payload, self.args.auth_cost)
                .await?;
            self.authenticated = true;
        }

        let order_id = self.alloc_request_id();
        let payload = self.build_buy_request(order_id, &msg.instrument, msg.amount, msg.price);
        self.send_jsonrpc_confirmed(order_id, payload, self.args.order_cost)
            .await?;

        Ok(CreateOpenOrderResponse {
            request_id: order_id,
        })
    }
}
