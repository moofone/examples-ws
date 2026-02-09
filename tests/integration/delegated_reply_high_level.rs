use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kameo::Actor;
use memchr::memmem;
use shared_ws::client::accept_async;
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    ForwardAllIngress, GetConnectionStatus, ProtocolPingPong, WebSocketActor, WebSocketActorArgs,
    WebSocketBufferConfig, WebSocketEvent, WsConfirmMode, WsDelegatedError, WsDelegatedOk,
    WsDelegatedRequest, WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction,
    WsFrame, WsMessageAction, WsParseOutcome, WsReconnectStrategy, WsRequestMatch,
    WsSubscriptionAction, WsSubscriptionManager, WsSubscriptionStatus, WsTlsConfig,
    into_ws_message,
};
use sonic_rs::JsonValueTrait;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

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
struct JsonRpcMatcherHandler {
    subs: NoSubscriptions,
}

impl JsonRpcMatcherHandler {
    fn new() -> Self {
        Self {
            subs: NoSubscriptions,
        }
    }
}

fn parse_jsonrpc_id(data: &[u8]) -> Option<u64> {
    let v = sonic_rs::get(data, &["id"]).ok()?;
    v.as_u64().or_else(|| {
        v.as_i64()
            .and_then(|i| if i >= 0 { Some(i as u64) } else { None })
    })
}

impl WsEndpointHandler for JsonRpcMatcherHandler {
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

    fn maybe_request_response(&self, data: &[u8]) -> bool {
        // Cheap precheck: only attempt full match if the payload contains `"id"`.
        memmem::find(data, b"\"id\"").is_some()
    }

    fn match_request_response(&mut self, data: &[u8]) -> Option<WsRequestMatch> {
        let request_id = parse_jsonrpc_id(data)?;
        if memmem::find(data, b"\"result\"").is_some() {
            return Some(WsRequestMatch {
                request_id,
                result: Ok(()),
                rate_limit_feedback: None,
            });
        }
        if memmem::find(data, b"\"error\"").is_some() {
            return Some(WsRequestMatch {
                request_id,
                result: Err("error".to_string()),
                rate_limit_feedback: None,
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

#[derive(Clone, Copy, Debug)]
enum ServerMode {
    ConfirmOk { delay: Duration },
    ConfirmErr { delay: Duration },
    NoReply,
}

async fn spawn_jsonrpc_server(mode: ServerMode) -> (SocketAddr, mpsc::UnboundedReceiver<Bytes>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::unbounded_channel::<Bytes>();

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut ws = accept_async(stream).await.unwrap();
                let Some(Ok(frame)) = ws.next().await else {
                    return;
                };
                let bytes: Bytes = match frame {
                    WsFrame::Text(text) => text.into_bytes(),
                    WsFrame::Binary(bytes) => bytes,
                    _ => Bytes::new(),
                };
                let _ = tx.send(bytes.clone());

                let id = parse_jsonrpc_id(bytes.as_ref()).unwrap_or(0);
                match mode {
                    ServerMode::NoReply => {}
                    ServerMode::ConfirmOk { delay } => {
                        tokio::time::sleep(delay).await;
                        let resp = format!(
                            "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"ok\":true}}}}",
                            id
                        );
                        let _ = ws.send(into_ws_message(resp)).await;
                    }
                    ServerMode::ConfirmErr { delay } => {
                        tokio::time::sleep(delay).await;
                        let resp = format!(
                            "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"message\":\"no\"}}}}",
                            id
                        );
                        let _ = ws.send(into_ws_message(resp)).await;
                    }
                }
            });
        }
    });

    (addr, rx)
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
        if matches!(status, shared_ws::ws::WsConnectionStatus::Connected) {
            return;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for Connected status (last={status:?})");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn assert_confirmed_ok(ok: WsDelegatedOk, request_id: u64) {
    assert_eq!(ok.request_id, request_id);
    assert!(ok.confirmed);
    assert!(ok.rate_limit_feedback.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegated_confirmed_happy_path_waits_for_confirmation() {
    let (addr, _rx) = spawn_jsonrpc_server(ServerMode::ConfirmOk {
        delay: Duration::from_millis(200),
    })
    .await;

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        tls: WsTlsConfig::default(),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: NoReconnect,
        handler: JsonRpcMatcherHandler::new(),
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

    actor.tell(WebSocketEvent::Connect).send().await.unwrap();
    wait_connected(&actor, Duration::from_secs(2)).await;

    let request_id = 42u64;
    let outbound = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"method\":\"do\"}}",
        request_id
    );
    let deadline = Instant::now() + Duration::from_secs(2);

    // Prove this is a Confirmed delegated request by ensuring it does not complete before the server replies.
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    let a2 = actor.clone();
    tokio::spawn(async move {
        let res = a2
            .ask(WsDelegatedRequest {
                request_id,
                fingerprint: 999,
                frame: into_ws_message(outbound),
                confirm_deadline: deadline,
                confirm_mode: WsConfirmMode::Confirmed,
            })
            .await;
        let _ = done_tx.send(res);
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut done_rx)
            .await
            .is_err(),
        "delegated Confirmed request completed before confirmation"
    );

    let ok = tokio::time::timeout(Duration::from_secs(2), &mut done_rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_confirmed_ok(ok, request_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegated_endpoint_rejected_is_surfaced() {
    let (addr, _rx) = spawn_jsonrpc_server(ServerMode::ConfirmErr {
        delay: Duration::from_millis(0),
    })
    .await;

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        tls: WsTlsConfig::default(),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: NoReconnect,
        handler: JsonRpcMatcherHandler::new(),
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

    actor.tell(WebSocketEvent::Connect).send().await.unwrap();
    wait_connected(&actor, Duration::from_secs(2)).await;

    let request_id = 7u64;
    let outbound = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"method\":\"do\"}}",
        request_id
    );
    let deadline = Instant::now() + Duration::from_secs(2);

    let err = actor
        .ask(WsDelegatedRequest {
            request_id,
            fingerprint: 999,
            frame: into_ws_message(outbound),
            confirm_deadline: deadline,
            confirm_mode: WsConfirmMode::Confirmed,
        })
        .await
        .expect_err("expected endpoint rejection");

    match err {
        kameo::error::SendError::HandlerError(WsDelegatedError::EndpointRejected {
            request_id: got,
            ..
        }) => assert_eq!(got, request_id),
        other => panic!("expected EndpointRejected handler error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegated_unconfirmed_when_server_accepts_but_never_replies() {
    let (addr, mut server_rx) = spawn_jsonrpc_server(ServerMode::NoReply).await;

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        tls: WsTlsConfig::default(),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: NoReconnect,
        handler: JsonRpcMatcherHandler::new(),
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

    actor.tell(WebSocketEvent::Connect).send().await.unwrap();
    wait_connected(&actor, Duration::from_secs(2)).await;

    let request_id = 123u64;
    let outbound = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"method\":\"do\"}}",
        request_id
    );
    let deadline = Instant::now() + Duration::from_millis(150);

    let err = actor
        .ask(WsDelegatedRequest {
            request_id,
            fingerprint: 999,
            frame: into_ws_message(outbound),
            confirm_deadline: deadline,
            confirm_mode: WsConfirmMode::Confirmed,
        })
        .await
        .expect_err("expected Unconfirmed");

    match err {
        kameo::error::SendError::HandlerError(WsDelegatedError::Unconfirmed {
            request_id: got,
            ..
        }) => assert_eq!(got, request_id),
        other => panic!("expected Unconfirmed handler error, got {other:?}"),
    }

    // Prove the frame was accepted/sent (server observed the outbound payload).
    let got = tokio::time::timeout(Duration::from_secs(2), server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(memmem::find(got.as_ref(), b"\"method\":\"do\"").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegated_not_delivered_when_writer_not_ready() {
    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: "ws://127.0.0.1:0".to_string(),
        tls: WsTlsConfig::default(),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: NoReconnect,
        handler: JsonRpcMatcherHandler::new(),
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

    // Intentionally do not connect. Writer is not ready, so delegated request must fail fast.
    let request_id = 1u64;
    let err = actor
        .ask(WsDelegatedRequest {
            request_id,
            fingerprint: 999,
            frame: into_ws_message("{\"jsonrpc\":\"2.0\",\"id\":1}"),
            confirm_deadline: Instant::now() + Duration::from_secs(1),
            confirm_mode: WsConfirmMode::Confirmed,
        })
        .await
        .expect_err("expected NotDelivered");

    match err {
        kameo::error::SendError::HandlerError(WsDelegatedError::NotDelivered {
            request_id: got,
            ..
        }) => assert_eq!(got, request_id),
        other => panic!("expected NotDelivered handler error, got {other:?}"),
    }
}
