use std::time::{Duration, Instant};

use bytes::Bytes;
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    GetConnectionStats, GetConnectionStatus, ProtocolPingPong, WebSocketActor, WebSocketActorArgs,
    WebSocketBufferConfig, WebSocketEvent, WsConnectionStatus, WsEndpointHandler, WsErrorAction,
    WsMessageAction, WsParseOutcome, WsReconnectStrategy, WsRequestMatch, WsSubscriptionAction,
    WsSubscriptionManager, WsSubscriptionStatus,
};

use crate::support::ws_mock::{WsMockCmd, WsMockConfig, WsMockFrameKind, WsMockServer};

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
struct NoopHandler {
    subs: NoSubscriptions,
}

impl WsEndpointHandler for NoopHandler {
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

    fn maybe_request_response(&self, _data: &[u8]) -> bool {
        false
    }

    fn match_request_response(&mut self, _data: &[u8]) -> Option<WsRequestMatch> {
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

    fn classify_disconnect(
        &self,
        _cause: &shared_ws::ws::WsDisconnectCause,
    ) -> shared_ws::ws::WsDisconnectAction {
        shared_ws::ws::WsDisconnectAction::Abort
    }
}

async fn wait_connected<E, R, P, I, T>(
    actor: &kameo::prelude::ActorRef<WebSocketActor<E, R, P, I, T>>,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_from_server_is_replied_with_pong_and_health_stats_are_strict() {
    let mut server = WsMockServer::spawn_with(WsMockConfig { auto_pong: false }).await;
    let addr = server.addr;

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{addr}"),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: NoReconnect,
        handler: NoopHandler::default(),
        ingress: shared_ws::ws::ForwardAllIngress::default(),
        ping_strategy: ProtocolPingPong::new(Duration::from_secs(60), Duration::from_secs(60)),
        enable_ping: false,
        stale_threshold: Duration::from_secs(60),
        ws_buffers: WebSocketBufferConfig::default(),
        outbound_capacity: 8,
        circuit_breaker: None,
        latency_policy: None,
        payload_latency_sampling: None,
        registration: None,
        metrics: None,
    });

    actor.tell(WebSocketEvent::Connect).send().await.unwrap();
    wait_connected(&actor, Duration::from_secs(2)).await;

    let conn_id = server.wait_connected(Duration::from_secs(2)).await;

    let stats0 = actor.ask(GetConnectionStats).await.unwrap();
    assert_eq!(stats0.connect_attempts, 1);
    assert_eq!(stats0.connect_successes, 1);
    assert_eq!(stats0.reconnects, 0);
    assert_eq!(stats0.errors, 0);
    assert_eq!(stats0.recent_internal_errors, 0);
    assert_eq!(stats0.recent_server_errors, 0);
    assert_eq!(stats0.messages, 0);
    assert_eq!(stats0.latency_samples, 0);
    assert_eq!(stats0.p50_latency_us, 0);
    assert_eq!(stats0.p99_latency_us, 0);

    let payload = Bytes::from_static(b"ping-payload");
    server
        .cmds
        .send(WsMockCmd::SendPing {
            conn_id,
            bytes: payload.clone(),
        })
        .unwrap();

    let pong = server
        .wait_kind(Duration::from_secs(2), conn_id, WsMockFrameKind::Pong)
        .await;
    assert_eq!(pong.bytes, payload, "pong must echo ping payload");

    let stats1 = actor.ask(GetConnectionStats).await.unwrap();
    // Only one inbound frame (the server ping) should have been counted.
    assert_eq!(stats1.messages, 1);
    assert_eq!(stats1.errors, 0);
    assert_eq!(stats1.recent_internal_errors, 0);
    assert_eq!(stats1.recent_server_errors, 0);
    // Replying to a server ping is not a round-trip measurement.
    assert_eq!(stats1.latency_samples, 0);
    assert_eq!(stats1.p50_latency_us, 0);
    assert_eq!(stats1.p99_latency_us, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_sends_unsolicited_ping_records_pong_rtt_histogram_and_updates_health() {
    let mut server = WsMockServer::spawn_with(WsMockConfig { auto_pong: false }).await;
    let addr = server.addr;

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{addr}"),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: NoReconnect,
        handler: NoopHandler::default(),
        ingress: shared_ws::ws::ForwardAllIngress::default(),
        ping_strategy: ProtocolPingPong::new(Duration::from_secs(60), Duration::from_secs(1)),
        enable_ping: false,
        stale_threshold: Duration::from_secs(60),
        ws_buffers: WebSocketBufferConfig::default(),
        outbound_capacity: 8,
        circuit_breaker: None,
        latency_policy: None,
        payload_latency_sampling: None,
        registration: None,
        metrics: None,
    });

    actor.tell(WebSocketEvent::Connect).send().await.unwrap();
    wait_connected(&actor, Duration::from_secs(2)).await;

    let conn_id = server.wait_connected(Duration::from_secs(2)).await;

    let stats0 = actor.ask(GetConnectionStats).await.unwrap();
    assert_eq!(stats0.connect_attempts, 1);
    assert_eq!(stats0.connect_successes, 1);
    assert_eq!(stats0.reconnects, 0);
    assert_eq!(stats0.errors, 0);
    assert_eq!(stats0.recent_internal_errors, 0);
    assert_eq!(stats0.recent_server_errors, 0);
    assert_eq!(stats0.latency_samples, 0);

    // Trigger an unsolicited ping from the actor, then reply with a pong after a small delay to
    // ensure RTT is > 0us and is recorded in the histogram.
    actor.tell(WebSocketEvent::SendPing).send().await.unwrap();
    let ping = server
        .wait_kind(Duration::from_secs(2), conn_id, WsMockFrameKind::Ping)
        .await;

    tokio::time::sleep(Duration::from_millis(2)).await;
    server
        .cmds
        .send(WsMockCmd::SendPong {
            conn_id,
            bytes: ping.bytes.clone(),
        })
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let stats = actor.ask(GetConnectionStats).await.unwrap();
        if stats.latency_samples >= 1 {
            assert_eq!(stats.errors, 0);
            assert_eq!(stats.recent_internal_errors, 0);
            assert_eq!(stats.recent_server_errors, 0);

            assert!(stats.p50_latency_us > 0, "p50 must be recorded");
            assert!(stats.p99_latency_us >= stats.p50_latency_us);
            assert!(stats.messages >= 1, "pong should count as an inbound frame");
            break;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for latency histogram sample (stats={stats:?})");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
