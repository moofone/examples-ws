use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use examples_ws::endpoints::deribit::{
    DeribitChannel, DeribitEvent, DeribitPublicHandler, DeribitSubscriptionManager,
};
use kameo::Actor;
use shared_ws::ws::{
    ProtocolPingPong, WebSocketActor, WebSocketActorArgs, WebSocketBufferConfig, WebSocketEvent,
    WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction, WsParseOutcome,
    WsReconnectStrategy,
};
use tokio::sync::mpsc;
use tracing::debug;

use super::mock_deribit_wss::{
    DeribitServerEvent, deribit_subscribe_ack, deribit_ticker_notification,
    parse_jsonrpc_method_and_id, spawn_deribit_mock_wss,
};

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
async fn deribit_wss_connects_subscribes_and_receives_stream_data() {
    crate::support::init_tracing();

    let (server, mut server_rx) = spawn_deribit_mock_wss().await;

    let channel = "ticker.BTC-PERPETUAL.raw";
    let subs = DeribitSubscriptionManager::with_initial_channels([DeribitChannel::from(channel)]);
    let handler = DeribitPublicHandler::new(subs);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let handler = CapturingHandler::new(handler, event_tx);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: server.url(),
        transport: server.client_transport(),
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

    let subscribe_id = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(DeribitServerEvent::Connected { .. })) => {}
            Ok(Some(DeribitServerEvent::InboundText { text })) => {
                let (method, id) = parse_jsonrpc_method_and_id(&text);
                if method.as_deref() == Some("public/subscribe") {
                    let id = id.expect("subscribe request should include id");
                    assert!(text.contains(channel), "subscribe should include channel");
                    debug!(
                        subscribe_id = id,
                        "sending deribit subscribe ack + notifications"
                    );
                    break id;
                }
            }
            Ok(Some(DeribitServerEvent::Disconnected)) => {
                panic!("server disconnected before subscribe")
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for subscribe request"),
        }
    };

    server.send_text(deribit_subscribe_ack(subscribe_id, &[channel]));
    server.send_text(deribit_ticker_notification(channel, 1700000000000, 100.0));
    server.send_text(deribit_ticker_notification(channel, 1700000000100, 100.5));
    server.send_text(deribit_ticker_notification(channel, 1700000000200, 101.0));

    for _ in 0..3 {
        let evt = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .unwrap()
            .expect("event channel closed");
        match evt {
            DeribitEvent::Subscription {
                channel: ch,
                payload,
            } => {
                assert_eq!(ch.0, channel);
                let s = std::str::from_utf8(payload.as_ref()).unwrap();
                assert!(s.contains("\"method\":\"subscription\""));
                assert!(s.contains(channel));
            }
        }
    }
}
