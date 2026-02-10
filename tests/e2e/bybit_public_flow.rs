use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use examples_ws::endpoints::bybit::{
    BybitEvent, BybitSubOp, BybitSubscriptionManager, BybitSubscriptionRequest, BybitTopic,
    BybitTopicHandler,
};
use shared_ws::client::accept_async;
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    WebSocketActor, WebSocketActorArgs, WebSocketBufferConfig, WebSocketEvent, WsDisconnectAction,
    WsDisconnectCause, WsEndpointHandler, WsErrorAction, WsMessage, WsParseOutcome,
    WsReconnectStrategy, WsSubscriptionAction,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

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

#[derive(Debug)]
enum ServerEvent {
    Connected { conn_id: usize },
    Data { conn_id: usize, bytes: Bytes },
}

async fn spawn_bybit_server() -> (SocketAddr, mpsc::UnboundedReceiver<ServerEvent>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut conn_id = 0usize;
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            conn_id = conn_id.saturating_add(1);
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut ws = accept_async(stream).await.unwrap();
                let _ = tx.send(ServerEvent::Connected { conn_id });

                while let Some(msg) = ws.next().await {
                    match msg {
                        Ok(WsMessage::Text(text)) => {
                            let _ = tx.send(ServerEvent::Data {
                                conn_id,
                                bytes: text.into_bytes(),
                            });
                        }
                        Ok(WsMessage::Binary(bytes)) => {
                            let _ = tx.send(ServerEvent::Data { conn_id, bytes });
                        }
                        Ok(WsMessage::Close(_)) => break,
                        Err(_) => break,
                        _ => {}
                    }
                }
            });
        }
    });

    (addr, rx)
}

#[derive(Clone)]
struct CapturingHandler {
    inner: BybitTopicHandler,
    opened: Arc<AtomicUsize>,
    tx: mpsc::UnboundedSender<BybitEvent>,
}

impl CapturingHandler {
    fn new(inner: BybitTopicHandler, tx: mpsc::UnboundedSender<BybitEvent>) -> Self {
        Self {
            inner,
            opened: Arc::new(AtomicUsize::new(0)),
            tx,
        }
    }
}

impl WsEndpointHandler for CapturingHandler {
    type Message = BybitEvent;
    type Error = examples_ws::endpoints::bybit::BybitProtocolError;
    type Subscription = BybitSubscriptionManager;

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
async fn bybit_sends_subscribe_on_connect_and_parses_topic_notification() {
    let (addr, mut server_rx) = spawn_bybit_server().await;

    let subs =
        BybitSubscriptionManager::with_initial_topics([BybitTopic::from("publicTrade.BTCUSDT")]);
    let handler = BybitTopicHandler::new(subs);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let handler = CapturingHandler::new(handler, event_tx);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: FastReconnect,
        handler,
        ingress: shared_ws::ws::ForwardAllIngress::default(),
        ping_strategy: shared_ws::ws::ProtocolPingPong::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
        ),
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

    let conn_id = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Connected { conn_id })) => break conn_id,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for server connect"),
        }
    };

    let subscribe = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id: c, bytes })) if c == conn_id => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for subscribe payload"),
        }
    };

    let subscribe_str = std::str::from_utf8(subscribe.as_ref()).unwrap();
    assert!(subscribe_str.contains("\"op\":\"subscribe\""));
    assert!(subscribe_str.contains("publicTrade.BTCUSDT"));

    let notif = Bytes::from_static(
        br#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"symbol":"BTCUSDT"}]}"#,
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
        BybitEvent::Topic { topic, payload } => {
            assert_eq!(topic.0, "publicTrade.BTCUSDT");
            assert_eq!(payload, notif);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bybit_dynamic_subscription_update_emits_subscribe_request() {
    let (addr, mut server_rx) = spawn_bybit_server().await;

    let subs = BybitSubscriptionManager::new();
    let handler = BybitTopicHandler::new(subs);
    let (event_tx, _event_rx) = mpsc::unbounded_channel();
    let handler = CapturingHandler::new(handler, event_tx);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: FastReconnect,
        handler,
        ingress: shared_ws::ws::ForwardAllIngress::default(),
        ping_strategy: shared_ws::ws::ProtocolPingPong::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
        ),
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

    let conn_id = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Connected { conn_id })) => break conn_id,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for server connect"),
        }
    };

    // No initial subscriptions, so first payload should be absent. Trigger an update.
    actor
        .ask(shared_ws::ws::WsSubscriptionUpdate {
            action: WsSubscriptionAction::Add(vec![BybitSubscriptionRequest {
                op: BybitSubOp::Subscribe,
                topics: vec![BybitTopic::from("publicTrade.ETHUSDT")],
            }]),
        })
        .await
        .unwrap();

    let subscribe = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id: c, bytes })) if c == conn_id => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for subscribe payload"),
        }
    };

    let s = std::str::from_utf8(subscribe.as_ref()).unwrap();
    assert!(s.contains("\"op\":\"subscribe\""));
    assert!(s.contains("publicTrade.ETHUSDT"));
}
