use std::time::{Duration, Instant};

use examples_ws::endpoints::deribit::{
    DeribitChannel, DeribitEvent, DeribitProtocolError, DeribitPublicHandler,
    DeribitSubscriptionManager,
};
use examples_ws::endpoints::deribit_private::DeribitJsonRpcMatcher;
use kameo::Actor;
use shared_ws::ws::{
    ProtocolPingPong, WebSocketActor, WebSocketActorArgs, WebSocketBufferConfig, WebSocketEvent,
    WsConfirmMode, WsDelegatedRequest, WsDisconnectAction, WsDisconnectCause, WsEndpointHandler,
    WsErrorAction, WsParseOutcome, WsReconnectStrategy, WsRequestMatch,
    into_ws_message,
};
use tokio::sync::mpsc;
use tracing::debug;

use super::mock_deribit_wss::{
    DeribitServerEvent, deribit_auth_ok, deribit_buy_ok, deribit_subscribe_ack,
    deribit_ticker_notification, parse_jsonrpc_method_and_id, spawn_deribit_mock_wss,
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
struct DeribitPubAndRpcHandler {
    pub_handler: DeribitPublicHandler,
    matcher: DeribitJsonRpcMatcher,
    auth_payload: Vec<u8>,
    tx: mpsc::UnboundedSender<DeribitEvent>,
}

impl WsEndpointHandler for DeribitPubAndRpcHandler {
    type Message = DeribitEvent;
    type Error = DeribitProtocolError;
    type Subscription = DeribitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        self.pub_handler.subscription_manager()
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        Some(self.auth_payload.clone())
    }

    fn parse(&mut self, _data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        Err(DeribitProtocolError::InvalidJson)
    }

    fn parse_frame(
        &mut self,
        frame: &shared_ws::ws::WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        self.pub_handler.parse_frame(frame)
    }

    fn maybe_request_response(&self, data: &[u8]) -> bool {
        <DeribitJsonRpcMatcher as WsEndpointHandler>::maybe_request_response(&self.matcher, data)
    }

    fn match_request_response(&mut self, data: &[u8]) -> Option<WsRequestMatch> {
        <DeribitJsonRpcMatcher as WsEndpointHandler>::match_request_response(
            &mut self.matcher,
            data,
        )
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
        self.pub_handler.handle_server_error(code, message, data)
    }

    fn reset_state(&mut self) {
        self.pub_handler.reset_state()
    }

    fn classify_disconnect(&self, cause: &WsDisconnectCause) -> WsDisconnectAction {
        self.pub_handler.classify_disconnect(cause)
    }
}

fn build_auth_request(id: u64) -> Vec<u8> {
    format!(
        concat!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":\"public/auth\",\"params\":{{",
            "\"grant_type\":\"client_credentials\",\"client_id\":\"test_client_id\",\"client_secret\":\"test_client_secret\"",
            "}}}}"
        ),
        id = id,
    )
    .into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_wss_subscribe_then_place_order_confirmed() {
    crate::support::init_tracing();

    let (server, mut server_rx) = spawn_deribit_mock_wss().await;

    let channel = "ticker.BTC-PERPETUAL.raw";
    let subs = DeribitSubscriptionManager::with_initial_channels([DeribitChannel::from(channel)]);

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let handler = DeribitPubAndRpcHandler {
        pub_handler: DeribitPublicHandler::new(subs),
        matcher: DeribitJsonRpcMatcher::new(),
        auth_payload: build_auth_request(9001),
        tx: event_tx,
    };

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

    // Drive the mock server: respond to auth + subscribe.
    let mut saw_auth = false;
    let mut saw_subscribe = false;
    while !(saw_auth && saw_subscribe) {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(DeribitServerEvent::Connected { .. })) => {}
            Ok(Some(DeribitServerEvent::InboundText { text })) => {
                let (method, id) = parse_jsonrpc_method_and_id(&text);
                match method.as_deref() {
                    Some("public/auth") => {
                        let id = id.expect("auth request should include id");
                        debug!(auth_id = id, "replying to deribit auth");
                        server.send_text(deribit_auth_ok(id));
                        saw_auth = true;
                    }
                    Some("public/subscribe") => {
                        let id = id.expect("subscribe request should include id");
                        assert!(text.contains(channel), "subscribe should include channel");
                        debug!(
                            subscribe_id = id,
                            "acking deribit subscribe + sending one notification"
                        );
                        server.send_text(deribit_subscribe_ack(id, &[channel]));
                        server.send_text(deribit_ticker_notification(
                            channel,
                            1700000000000,
                            100.0,
                        ));
                        saw_subscribe = true;
                    }
                    _ => {}
                }
            }
            Ok(Some(DeribitServerEvent::Disconnected)) => {
                panic!("server disconnected before auth+subscribe")
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for auth+subscribe"),
        }
    }

    // Ensure we received some subscription data before placing an order.
    let evt = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
        .await
        .unwrap()
        .expect("event channel closed");
    match evt {
        DeribitEvent::Subscription { channel: ch, .. } => assert_eq!(ch.0, channel),
    }

    // Send a mock order via delegated Confirmed flow.
    let request_id = 9101u64;
    let outbound = format!(
        concat!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{request_id},\"method\":\"private/buy\",\"params\":{{",
            "\"instrument_name\":\"BTC-PERPETUAL\",\"amount\":10.0,\"type\":\"limit\",\"price\":1.0",
            "}}}}"
        ),
        request_id = request_id,
    );

    let req = WsDelegatedRequest {
        request_id,
        fingerprint: request_id,
        frame: into_ws_message(outbound),
        confirm_deadline: Instant::now() + Duration::from_secs(2),
        confirm_mode: WsConfirmMode::Confirmed,
    };

    let actor2 = actor.clone();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = done_tx.send(actor2.ask(req).await);
    });

    // Wait for the server to observe the order request, then reply.
    loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(DeribitServerEvent::InboundText { text })) => {
                let (method, id) = parse_jsonrpc_method_and_id(&text);
                if method.as_deref() == Some("private/buy") {
                    let id = id.expect("buy request should include id");
                    assert_eq!(id, request_id);
                    debug!(order_id = id, "replying to deribit private/buy");
                    server.send_text(deribit_buy_ok(id, 1700000000001));
                    break;
                }
            }
            Ok(Some(DeribitServerEvent::Disconnected)) => panic!("server disconnected before buy"),
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for buy request"),
        }
    }

    let ok = tokio::time::timeout(Duration::from_secs(2), done_rx)
        .await
        .unwrap()
        .expect("order task dropped")
        .unwrap();
    assert!(ok.confirmed);
    assert_eq!(ok.request_id, request_id);
    assert!(ok.rate_limit_feedback.is_none());
}
