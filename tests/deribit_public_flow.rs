use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use kameo::Actor;
use shared_ws::client::accept_async;
use examples_ws::endpoints::deribit::{
    DeribitChannel, DeribitEvent, DeribitPublicHandler, DeribitSubOp, DeribitSubscriptionManager,
    DeribitSubscriptionRequest,
};
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    ProtocolPingPong, WebSocketActor, WebSocketActorArgs, WebSocketBufferConfig, WebSocketEvent,
    WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction, WsMessage,
    WsParseOutcome, WsReconnectStrategy, WsSubscriptionAction, WsTlsConfig,
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

async fn spawn_deribit_server() -> (SocketAddr, mpsc::UnboundedReceiver<ServerEvent>) {
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
                        Ok(WsMessage::Text(bytes)) | Ok(WsMessage::Binary(bytes)) => {
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
        frame: &shared_ws::core::WsFrame,
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
    let (addr, mut server_rx) = spawn_deribit_server().await;

    let subs = DeribitSubscriptionManager::with_initial_channels([DeribitChannel::from(
        "ticker.BTC-PERPETUAL.raw",
    )]);
    let handler = DeribitPublicHandler::new(subs);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let handler = CapturingHandler::new(handler, event_tx);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        tls: WsTlsConfig::default(),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: FastReconnect,
        handler,
        ingress: shared_ws::core::ForwardAllIngress::default(),
        ping_strategy: ProtocolPingPong::new(Duration::from_secs(60), Duration::from_secs(60)),
        enable_ping: false,
        stale_threshold: Duration::from_secs(30),
        ws_buffers: WebSocketBufferConfig::default(),
        global_rate_limit: None,
        outbound_capacity: 32,
        rate_limiter: None,
        circuit_breaker: None,
        latency_policy: None,
        payload_latency_sampling: None,
        registration: None,
        metrics: None,
    });

    actor.tell(WebSocketEvent::Connect).send().await.unwrap();

    // First server message should be the subscribe request emitted during connect.
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
    assert!(subscribe_str.contains("\"method\":\"public/subscribe\""));
    assert!(subscribe_str.contains("ticker.BTC-PERPETUAL.raw"));

    // Inject a Deribit subscription notification.
    let notif = Bytes::from_static(
        br#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"ticker.BTC-PERPETUAL.raw","data":{"timestamp":1700000000000}}}"#,
    );
    actor
        .ask(WebSocketEvent::Inbound(WsMessage::Text(notif.clone())))
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
async fn deribit_dynamic_subscription_update_emits_subscribe_request() {
    let (addr, mut server_rx) = spawn_deribit_server().await;

    let subs = DeribitSubscriptionManager::new();
    let handler = DeribitPublicHandler::new(subs);
    let (event_tx, _event_rx) = mpsc::unbounded_channel();
    let handler = CapturingHandler::new(handler, event_tx);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        tls: WsTlsConfig::default(),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: FastReconnect,
        handler,
        ingress: shared_ws::core::ForwardAllIngress::default(),
        ping_strategy: ProtocolPingPong::new(Duration::from_secs(60), Duration::from_secs(60)),
        enable_ping: false,
        stale_threshold: Duration::from_secs(30),
        ws_buffers: WebSocketBufferConfig::default(),
        global_rate_limit: None,
        outbound_capacity: 32,
        rate_limiter: None,
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

    let subscribe = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id: c, bytes })) if c == conn_id => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for subscribe payload"),
        }
    };

    let subscribe_str = std::str::from_utf8(subscribe.as_ref()).unwrap();
    assert!(subscribe_str.contains("\"method\":\"public/subscribe\""));
    assert!(subscribe_str.contains("book.BTC-PERPETUAL.10.100ms"));
}
