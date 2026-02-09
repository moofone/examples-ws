use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use examples_ws::bybit::public_actor::{BybitPublicActor, BybitPublicActorArgs, GetStats};
use kameo::Actor;
use shared_ws::client::accept_async;
use shared_ws::ws::ExponentialBackoffReconnect;
use shared_ws::ws::{WsMessage, WsTlsConfig};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[derive(Debug)]
enum ServerEvent {
    Connected { conn_id: usize },
    Data { conn_id: usize, bytes: Bytes },
}

async fn spawn_silent_ws_server() -> (SocketAddr, mpsc::UnboundedReceiver<ServerEvent>) {
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
            let my_conn_id = conn_id;
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut ws = accept_async(stream).await.unwrap();
                let _ = tx.send(ServerEvent::Connected {
                    conn_id: my_conn_id,
                });

                // Intentionally never send frames to the client. We only observe what the client sends.
                while let Some(msg) = ws.next().await {
                    match msg {
                        Ok(WsMessage::Text(text)) => {
                            let _ = tx.send(ServerEvent::Data {
                                conn_id: my_conn_id,
                                bytes: text.into_bytes(),
                            });
                        }
                        Ok(WsMessage::Binary(bytes)) => {
                            let _ = tx.send(ServerEvent::Data {
                                conn_id: my_conn_id,
                                bytes,
                            });
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
async fn bybit_public_actor_reconnects_when_stale_threshold_exceeded() {
    let (addr, mut server_rx) = spawn_silent_ws_server().await;

    // Important: stale checking is driven by the ping loop (it ticks `CheckStale`).
    // We make ping timeouts long so stale is triggered by the health monitor (no inbound frames),
    // not by ping/pong aging.
    let args = BybitPublicActorArgs {
        url: format!("ws://{addr}"),
        tls: WsTlsConfig::default(),
        initial_topics: vec!["publicTrade.BTCUSDT".to_string()],
        stale_threshold: Duration::from_millis(250),
        ws_buffers: shared_ws::ws::WebSocketBufferConfig::default(),
        outbound_capacity: 128,
        event_channel_capacity: 32,
        buffer_capacity: 1024,
        reconnect: ExponentialBackoffReconnect::new(
            Duration::from_millis(10),
            Duration::from_millis(50),
            1.5,
        ),
        enable_ping: true,
        ping_interval: Duration::from_millis(50),
        ping_timeout: Duration::from_secs(10),
    };

    let actor = BybitPublicActor::spawn(args);

    // Conn #1, observe initial subscribe (client -> server).
    let conn1 = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Connected { conn_id })) => break conn_id,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for conn1"),
        }
    };
    assert_eq!(conn1, 1);

    let subscribe1 = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id: 1, bytes })) => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for subscribe1"),
        }
    };
    let s1 = std::str::from_utf8(subscribe1.as_ref()).unwrap();
    assert!(s1.contains("\"op\":\"subscribe\""), "payload={s1}");
    assert!(s1.contains("publicTrade.BTCUSDT"), "payload={s1}");

    // The server stays silent. Once `stale_threshold` is exceeded, the actor should disconnect and reconnect.
    let conn2 = loop {
        match tokio::time::timeout(Duration::from_secs(4), server_rx.recv()).await {
            Ok(Some(ServerEvent::Connected { conn_id })) if conn_id >= 2 => break conn_id,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for reconnect (conn2)"),
        }
    };

    // On reconnect, initial subscribe should still be sent.
    let subscribe2 = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id, bytes })) if conn_id == conn2 => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for subscribe2"),
        }
    };
    let s2 = std::str::from_utf8(subscribe2.as_ref()).unwrap();
    assert!(s2.contains("\"op\":\"subscribe\""), "payload={s2}");
    assert!(s2.contains("publicTrade.BTCUSDT"), "payload={s2}");

    let stats = actor.ask(GetStats).await.unwrap();
    assert_eq!(stats.subscribed_topics, 1);

    let _ = actor.stop_gracefully().await;
}
