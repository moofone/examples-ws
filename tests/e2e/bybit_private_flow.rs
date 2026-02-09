use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use examples_ws::endpoints::bybit::{BybitSubscriptionManager, BybitTopic, BybitTopicHandler};
use kameo::Actor;
use shared_ws::client::accept_async;
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    WebSocketActor, WebSocketActorArgs, WebSocketBufferConfig, WebSocketEvent, WsMessage,
    WsReconnectStrategy,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bybit_auth_is_sent_before_initial_subscribe() {
    let (addr, mut server_rx) = spawn_bybit_server().await;

    let subs = BybitSubscriptionManager::with_initial_topics([BybitTopic::from("order")]);
    let handler = BybitTopicHandler::new(subs).with_auth_provider(Arc::new(|| {
        br#"{"op":"auth","args":["key","1","sig"],"reqId":"auth_1"}"#.to_vec()
    }));

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url: format!("ws://{}", addr),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: FastReconnect,
        handler,
        ingress: shared_ws::ws::ForwardAllIngress::default(),
        ping_strategy: examples_ws::bybit::ping::BybitJsonPingPong::private(
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

    let first = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id: c, bytes })) if c == conn_id => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for first payload"),
        }
    };

    let second = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id: c, bytes })) if c == conn_id => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server event stream ended"),
            Err(_) => panic!("timed out waiting for second payload"),
        }
    };

    let first_str = std::str::from_utf8(first.as_ref()).unwrap();
    assert!(first_str.contains("\"op\":\"auth\""));

    let second_str = std::str::from_utf8(second.as_ref()).unwrap();
    assert!(second_str.contains("\"op\":\"subscribe\""));
    assert!(second_str.contains("order"));
}
