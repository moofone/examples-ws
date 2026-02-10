use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use examples_ws::endpoints::deribit::{
    DeribitChannel, DeribitEvent, DeribitPublicHandler, DeribitSubOp, DeribitSubscriptionManager,
    DeribitSubscriptionRequest,
};
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    ProtocolPingPong, WebSocketActor, WebSocketActorArgs, WebSocketBufferConfig, WebSocketEvent,
    WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction, WsParseOutcome,
    WsReconnectStrategy, WsSubscriptionAction,
};
use tokio::sync::mpsc;

use crate::support::deribit_messages;
use crate::support::ws_mock::{WsMockFrameKind, WsMockServer};

#[derive(Clone)]
struct FastReconnect;

impl WsReconnectStrategy for FastReconnect {
    fn next_delay(&mut self) -> Duration {
        Duration::from_millis(10)
    }
    fn reset(&mut self) {}
    fn should_retry(&self) -> bool {
        true
    }
}

async fn spawn_deribit_server() -> WsMockServer {
    WsMockServer::spawn().await
}

#[derive(Clone)]
struct CapturingHandler {
    inner: DeribitPublicHandler,
    opened: Arc<AtomicUsize>,
    tx: mpsc::UnboundedSender<DeribitEvent>,
}

impl CapturingHandler {
    fn new(inner: DeribitPublicHandler, tx: mpsc::UnboundedSender<DeribitEvent>) -> Self {
        Self {
            inner,
            opened: Arc::new(AtomicUsize::new(0)),
            tx,
        }
    }
}

impl WsEndpointHandler for CapturingHandler {
    type Message = DeribitEvent;
    type Error = examples_ws::endpoints::deribit::DeribitProtocolError;
    type Subscription = DeribitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        self.inner.subscription_manager()
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        self.inner.generate_auth()
    }

    fn parse(&mut self, data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        self.inner.parse(data)
    }

    fn parse_frame(
        &mut self,
        frame: &shared_ws::ws::WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        self.inner.parse_frame(frame)
    }

    fn handle_message(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        let _ = self.tx.send(msg);
        Ok(())
    }

    fn handle_server_error(
        &mut self,
        code: Option<i32>,
        message: &str,
        data: Option<sonic_rs::Value>,
    ) -> WsErrorAction {
        self.inner.handle_server_error(code, message, data)
    }

    fn reset_state(&mut self) {
        self.inner.reset_state()
    }

    fn classify_disconnect(&self, cause: &WsDisconnectCause) -> WsDisconnectAction {
        self.inner.classify_disconnect(cause)
    }

    fn on_open(&mut self) {
        self.opened.fetch_add(1, Ordering::Relaxed);
        self.inner.on_open()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_sends_subscribe_on_connect_and_parses_subscription_notification() {
    let mut server = spawn_deribit_server().await;
    let addr = server.addr;

    let subs = DeribitSubscriptionManager::with_initial_channels([DeribitChannel::from(
        "ticker.BTC-PERPETUAL.raw",
    )]);
    let handler = DeribitPublicHandler::new(subs);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let handler = CapturingHandler::new(handler, event_tx);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: FastReconnect,
        handler,
        ingress: shared_ws::ws::ForwardAllIngress::default(),
        ping_strategy: ProtocolPingPong::new(Duration::from_secs(60), Duration::from_secs(60)),
        enable_ping: false,
        stale_threshold: Duration::from_secs(30),
        ws_buffers: WebSocketBufferConfig::default(),
        outbound_capacity: 32,
        circuit_breaker: None,
        latency_policy: None,
        payload_latency_sampling: None,
        registration: None,
        metrics: None,
    });

    actor.tell(WebSocketEvent::Connect).send().await.unwrap();

    // First server message should be the subscribe request emitted during connect.
    let conn_id = server.wait_connected(Duration::from_secs(2)).await;
    let subscribe = server
        .wait_frame(Duration::from_secs(2), Some(conn_id))
        .await;
    assert!(
        matches!(
            subscribe.kind,
            WsMockFrameKind::Text | WsMockFrameKind::Binary
        ),
        "unexpected subscribe frame kind: {:?}",
        subscribe.kind
    );

    let subscribe_str = std::str::from_utf8(subscribe.bytes.as_ref()).unwrap();
    assert!(subscribe_str.contains("\"method\":\"public/subscribe\""));
    assert!(subscribe_str.contains("ticker.BTC-PERPETUAL.raw"));

    // Inject a Deribit subscription notification.
    let notif = deribit_messages::subscription_notification(
        "ticker.BTC-PERPETUAL.raw",
        deribit_messages::ticker_data_example_json(),
    );
    actor
        .ask(WebSocketEvent::Inbound(shared_ws::ws::into_ws_message(
            notif.clone(),
        )))
        .await
        .unwrap();

    let evt = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match evt {
        DeribitEvent::Subscription { channel, payload } => {
            assert_eq!(channel.0, "ticker.BTC-PERPETUAL.raw");
            assert_eq!(payload, notif);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_parses_platform_state_subscription_notification() {
    let mut server = spawn_deribit_server().await;
    let addr = server.addr;

    let subs =
        DeribitSubscriptionManager::with_initial_channels([DeribitChannel::from("platform_state")]);
    let handler = DeribitPublicHandler::new(subs);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let handler = CapturingHandler::new(handler, event_tx);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: FastReconnect,
        handler,
        ingress: shared_ws::ws::ForwardAllIngress::default(),
        ping_strategy: ProtocolPingPong::new(Duration::from_secs(60), Duration::from_secs(60)),
        enable_ping: false,
        stale_threshold: Duration::from_secs(30),
        ws_buffers: WebSocketBufferConfig::default(),
        outbound_capacity: 32,
        circuit_breaker: None,
        latency_policy: None,
        payload_latency_sampling: None,
        registration: None,
        metrics: None,
    });

    actor.tell(WebSocketEvent::Connect).send().await.unwrap();

    // Ensure server observed connect + the initial subscribe request.
    let conn_id = server.wait_connected(Duration::from_secs(2)).await;
    let subscribe = server
        .wait_frame(Duration::from_secs(2), Some(conn_id))
        .await;
    let subscribe_str = std::str::from_utf8(subscribe.bytes.as_ref()).unwrap();
    assert!(subscribe_str.contains("\"method\":\"public/subscribe\""));
    assert!(subscribe_str.contains("platform_state"));

    let notif = deribit_messages::subscription_notification(
        "platform_state",
        deribit_messages::platform_state_data_example_json(),
    );
    actor
        .ask(WebSocketEvent::Inbound(shared_ws::ws::into_ws_message(
            notif.clone(),
        )))
        .await
        .unwrap();

    let evt = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match evt {
        DeribitEvent::Subscription { channel, payload } => {
            assert_eq!(channel.0, "platform_state");
            assert_eq!(payload, notif);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_dynamic_subscription_update_emits_subscribe_request() {
    let mut server = spawn_deribit_server().await;
    let addr = server.addr;

    let subs = DeribitSubscriptionManager::new();
    let handler = DeribitPublicHandler::new(subs);
    let (event_tx, _event_rx) = mpsc::unbounded_channel();
    let handler = CapturingHandler::new(handler, event_tx);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: FastReconnect,
        handler,
        ingress: shared_ws::ws::ForwardAllIngress::default(),
        ping_strategy: ProtocolPingPong::new(Duration::from_secs(60), Duration::from_secs(60)),
        enable_ping: false,
        stale_threshold: Duration::from_secs(30),
        ws_buffers: WebSocketBufferConfig::default(),
        outbound_capacity: 32,
        circuit_breaker: None,
        latency_policy: None,
        payload_latency_sampling: None,
        registration: None,
        metrics: None,
    });

    actor.tell(WebSocketEvent::Connect).send().await.unwrap();

    let conn_id = server.wait_connected(Duration::from_secs(2)).await;

    // Dynamically subscribe.
    actor
        .tell(shared_ws::ws::WsSubscriptionUpdate {
            action: WsSubscriptionAction::Add(vec![DeribitSubscriptionRequest {
                op: DeribitSubOp::Subscribe,
                channels: vec![DeribitChannel::from("book.BTC-PERPETUAL.10.100ms")],
            }]),
        })
        .send()
        .await
        .unwrap();

    let subscribe = server
        .wait_frame(Duration::from_secs(2), Some(conn_id))
        .await
        .bytes;

    let subscribe_str = std::str::from_utf8(subscribe.as_ref()).unwrap();
    assert!(subscribe_str.contains("\"method\":\"public/subscribe\""));
    assert!(subscribe_str.contains("book.BTC-PERPETUAL.10.100ms"));
}
