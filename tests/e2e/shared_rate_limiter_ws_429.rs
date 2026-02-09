use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::support::ws_mock::{WsMockCmd, WsMockEvent, WsMockFrameKind, WsMockServer};
use kameo::prelude::{Actor, ActorRef, Context, Message as KameoMessage};
use memchr::memmem;
use shared_rate_limiter::{Config, Cost, Deny, Feedback, FeedbackScope, Outcome, RateLimiter};
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    ForwardAllIngress, GetConnectionStatus, ProtocolPingPong, WebSocketActor, WebSocketActorArgs,
    WebSocketBufferConfig, WebSocketEvent, WsConfirmMode, WsConnectionStatus, WsDelegatedError,
    WsDelegatedRequest, WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction,
    WsFrame, WsMessageAction, WsParseOutcome, WsRateLimitFeedback, WsReconnectStrategy,
    WsRequestMatch, WsSubscriptionAction, WsSubscriptionManager, WsSubscriptionStatus,
    into_ws_message,
};
use sonic_rs::JsonValueTrait;

#[derive(Clone)]
struct NoReconnect;

impl WsReconnectStrategy for NoReconnect {
    fn next_delay(&mut self) -> Duration {
        Duration::from_secs(365 * 24 * 60 * 60)
    }
    fn reset(&mut self) {}
    fn should_retry(&self) -> bool {
        false
    }
}

#[derive(Debug, Default, Clone)]
struct NoSubscriptions;

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

#[derive(Clone, Debug, Default)]
struct DeribitRateLimitMatcher {
    subs: NoSubscriptions,
}

fn parse_jsonrpc_id(data: &[u8]) -> Option<u64> {
    let v = sonic_rs::get(data, &["id"]).ok()?;
    v.as_u64().or_else(|| {
        v.as_i64()
            .and_then(|i| if i >= 0 { Some(i as u64) } else { None })
    })
}

impl WsEndpointHandler for DeribitRateLimitMatcher {
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
        // Cheap precheck: only attempt full match if the payload contains `"id"` and either
        // `"result"` or `"error"` (Deribit JSON-RPC shape).
        if memmem::find(data, b"\"id\"").is_none() {
            return false;
        }
        memmem::find(data, b"\"result\"").is_some() || memmem::find(data, b"\"error\"").is_some()
    }

    fn match_request_response(&mut self, data: &[u8]) -> Option<WsRequestMatch> {
        let request_id = parse_jsonrpc_id(data)?;

        if sonic_rs::get(data, &["result"]).is_ok() {
            return Some(WsRequestMatch {
                request_id,
                result: Ok(()),
                rate_limit_feedback: None,
            });
        }

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

            // Deribit rate limit errors are JSON-RPC errors with `message="too_many_requests"`
            // and (commonly) `code=10028`. We surface this as feedback with a conservative
            // retry_after to drive the shared-rate_limiter feedback path.
            let feedback = match code {
                Some(10028) => Some(WsRateLimitFeedback {
                    retry_after: Some(Duration::from_millis(250)),
                    hint: Some("deribit_too_many_requests".to_string()),
                }),
                _ => None,
            };

            return Some(WsRequestMatch {
                request_id,
                result: Err(msg),
                rate_limit_feedback: feedback,
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
        WsDisconnectAction::Abort
    }
}

async fn spawn_rate_limit_jsonrpc_server(ok_before_limit: usize) -> SocketAddr {
    crate::support::init_tracing();

    let mut server = WsMockServer::spawn().await;
    let addr = server.addr;
    tracing::debug!(addr = %addr, "rate limit mock ws: listening");
    let cmds = server.cmds.clone();

    tokio::spawn(async move {
        let mut active_conn: Option<usize> = None;
        let mut seen = 0usize;

        while let Some(evt) = server.recv().await {
            match evt {
                WsMockEvent::Connected { conn_id } => {
                    active_conn = Some(conn_id);
                    tracing::debug!(conn_id, "rate limit mock ws: connected");
                }
                WsMockEvent::Frame { conn_id, frame } => {
                    if active_conn != Some(conn_id) {
                        continue;
                    }
                    if !matches!(frame.kind, WsMockFrameKind::Text | WsMockFrameKind::Binary) {
                        continue;
                    }

                    if let Ok(text) = std::str::from_utf8(frame.bytes.as_ref()) {
                        tracing::debug!(
                            direction = "client->server",
                            conn_id,
                            text = %text,
                            "rate limit mock ws: text"
                        );
                    } else {
                        tracing::debug!(
                            direction = "client->server",
                            conn_id,
                            bytes_len = frame.bytes.len(),
                            "rate limit mock ws: binary"
                        );
                    }

                    let request_id = parse_jsonrpc_id(frame.bytes.as_ref()).unwrap_or(0);
                    seen = seen.saturating_add(1);

                    // Deribit subscription ACK: `result` is an array of channel strings.
                    //
                    // This test always subscribes to exactly one channel. Keep the mock strict
                    // and predictable (no JSON parsing needed here).
                    let channels = ["ticker.BTC-PERPETUAL.raw"];

                    let resp = if seen <= ok_before_limit {
                        let mut out = String::with_capacity(64 + channels.len() * 48);
                        out.push_str("{\"jsonrpc\":\"2.0\",\"id\":");
                        out.push_str(&request_id.to_string());
                        out.push_str(",\"result\":[");
                        for (i, ch) in channels.iter().enumerate() {
                            if i != 0 {
                                out.push(',');
                            }
                            out.push('"');
                            for c in ch.chars() {
                                match c {
                                    '"' => out.push_str("\\\""),
                                    '\\' => out.push_str("\\\\"),
                                    _ => out.push(c),
                                }
                            }
                            out.push('"');
                        }
                        out.push_str("]}");
                        out
                    } else {
                        // Deribit rate-limit error is JSON-RPC: `code=10028`, `message=too_many_requests`.
                        format!(
                            "{{\"jsonrpc\":\"2.0\",\"id\":{request_id},\"error\":{{\"code\":10028,\"message\":\"too_many_requests\"}}}}"
                        )
                    };

                    tracing::debug!(
                        direction = "server->client",
                        conn_id,
                        text = %resp,
                        "rate limit mock ws: text"
                    );
                    let _ = cmds.send(WsMockCmd::SendText {
                        conn_id,
                        text: resp,
                    });
                }
                WsMockEvent::Disconnected { .. } => break,
            }
        }
    });

    addr
}

async fn wait_connected<E, R, P, I, T>(
    actor: &ActorRef<WebSocketActor<E, R, P, I, T>>,
    timeout: Duration,
) where
    E: WsEndpointHandler,
    R: WsReconnectStrategy,
    P: shared_ws::ws::WsPingPongStrategy,
    I: shared_ws::ws::WsIngress<Message = E::Message>,
    T: shared_ws::transport::WsTransport,
{
    let deadline = Instant::now() + timeout;
    loop {
        let status = actor.ask(GetConnectionStatus).await.unwrap();
        if matches!(status, WsConnectionStatus::Connected) {
            return;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for Connected status (last={status:?})");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

struct LimiterActor {
    rl: RateLimiter,
}

#[derive(Debug, Clone)]
struct ApplyLimiterFeedback {
    route_key: u64,
    retry_after: Duration,
    now: Duration,
}

#[derive(Debug, Clone)]
struct ProbeAcquire {
    key: u64,
    permits: u32,
    now: Duration,
}

impl Actor for LimiterActor {
    type Args = RateLimiter;
    type Error = std::io::Error;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { rl: args })
    }
}

impl KameoMessage<ApplyLimiterFeedback> for LimiterActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ApplyLimiterFeedback,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let fb = Feedback::new(msg.retry_after, FeedbackScope::Key);
        self.rl.apply_feedback(msg.route_key, &fb, msg.now);
    }
}

impl KameoMessage<ProbeAcquire> for LimiterActor {
    type Reply = Result<(), Deny>;

    async fn handle(
        &mut self,
        msg: ProbeAcquire,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(cost) = Cost::new(msg.permits) else {
            return Err(Deny {
                retry_after: Duration::ZERO,
            });
        };
        let p = self.rl.try_acquire(msg.key, cost, msg.now)?;
        // Keep accounting consistent; for this probe we treat it as "not sent".
        self.rl.commit(p, Outcome::NotSent, msg.now);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_429_endpoint_rejection_can_be_applied_to_shared_rate_limiter_via_actor_message() {
    crate::support::init_tracing();
    let addr = spawn_rate_limit_jsonrpc_server(3).await;

    // Websocket actor: delegated requests w/ an endpoint handler that surfaces 429 as feedback.
    let ws = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{addr}"),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: NoReconnect,
        handler: DeribitRateLimitMatcher::default(),
        ingress: ForwardAllIngress::default(),
        ping_strategy: ProtocolPingPong::new(Duration::from_secs(60), Duration::from_secs(60)),
        enable_ping: false,
        stale_threshold: Duration::from_secs(60),
        ws_buffers: WebSocketBufferConfig::default(),
        outbound_capacity: 32,
        circuit_breaker: None,
        latency_policy: None,
        payload_latency_sampling: None,
        registration: None,
        metrics: None,
    });
    ws.tell(WebSocketEvent::Connect).send().await.unwrap();
    wait_connected(&ws, Duration::from_secs(2)).await;

    // Limiter actor: use a large token bucket so only server feedback blocks.
    let limiter = RateLimiter::new(Config::token_bucket(1_000_000, 1_000_000));
    let limiter_actor = LimiterActor::spawn(limiter);

    // Send a few "subscribe" delegated requests; server confirms first N.
    for request_id in 1u64..=3u64 {
        let outbound = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{request_id},\"method\":\"public/subscribe\",\"params\":{{\"channels\":[\"ticker.BTC-PERPETUAL.raw\"]}}}}"
        );
        tracing::debug!(direction="client->server", request_id, text=%outbound, "rate limit test: sending subscribe");
        let ok = ws
            .ask(WsDelegatedRequest {
                request_id,
                fingerprint: request_id,
                frame: into_ws_message(outbound),
                confirm_deadline: Instant::now() + Duration::from_secs(2),
                confirm_mode: WsConfirmMode::Confirmed,
            })
            .await
            .unwrap();
        assert!(ok.confirmed);
        assert!(ok.rate_limit_feedback.is_none());
    }

    // Next request triggers 429.
    let request_id = 4u64;
    let outbound = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{request_id},\"method\":\"public/subscribe\",\"params\":{{\"channels\":[\"ticker.BTC-PERPETUAL.raw\"]}}}}"
    );
    tracing::debug!(direction="client->server", request_id, text=%outbound, "rate limit test: sending subscribe (expect rate limit)");

    let err = ws
        .ask(WsDelegatedRequest {
            request_id,
            fingerprint: request_id,
            frame: into_ws_message(outbound),
            confirm_deadline: Instant::now() + Duration::from_secs(2),
            confirm_mode: WsConfirmMode::Confirmed,
        })
        .await
        .expect_err("expected endpoint rejection");

    let retry_after = match err {
        kameo::error::SendError::HandlerError(WsDelegatedError::EndpointRejected {
            request_id: got,
            rate_limit_feedback: Some(fb),
            ..
        }) => {
            assert_eq!(got, request_id);
            fb.retry_after.expect("expected retry_after in feedback")
        }
        other => panic!("expected EndpointRejected w/ feedback, got {other:?}"),
    };

    // Apply the feedback to the shared-rate_limiter core via an actor message, and prove the key
    // is blocked for (about) retry_after.
    let now0 = Duration::from_secs(0);
    limiter_actor
        .ask(ApplyLimiterFeedback {
            route_key: 1,
            retry_after,
            now: now0,
        })
        .await
        .unwrap();

    let denied = limiter_actor
        .ask(ProbeAcquire {
            key: 1,
            permits: 1,
            now: now0,
        })
        .await
        .expect_err("expected limiter deny after applying 429 feedback");
    match denied {
        kameo::error::SendError::HandlerError(d) => assert_eq!(d.retry_after, retry_after),
        other => panic!("expected HandlerError(Deny), got {other:?}"),
    }

    // Still denied just before expiry.
    let almost = now0 + retry_after - Duration::from_millis(1);
    let denied2 = limiter_actor
        .ask(ProbeAcquire {
            key: 1,
            permits: 1,
            now: almost,
        })
        .await
        .expect_err("expected limiter deny just before feedback expiry");
    match denied2 {
        kameo::error::SendError::HandlerError(d) => {
            assert_eq!(d.retry_after, Duration::from_millis(1))
        }
        other => panic!("expected HandlerError(Deny), got {other:?}"),
    }

    // Allowed at expiry.
    let at = now0 + retry_after;
    limiter_actor
        .ask(ProbeAcquire {
            key: 1,
            permits: 1,
            now: at,
        })
        .await
        .unwrap();
}
