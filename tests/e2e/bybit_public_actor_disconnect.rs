use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use examples_ws::bybit::public_actor::{
    BybitPublicActor, BybitPublicActorArgs, GetStats, IsConnected, SubscribeTopics,
};
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

// Test server that drops connection #1 after it has observed `drop_after_msgs` data frames.
async fn spawn_bybit_server_drop_conn1_after(
    drop_after_msgs: usize,
) -> (SocketAddr, mpsc::UnboundedReceiver<ServerEvent>) {
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

                let mut seen_data = 0usize;
                while let Some(msg) = ws.next().await {
                    match msg {
                        Ok(WsMessage::Text(text)) => {
                            seen_data = seen_data.saturating_add(1);
                            let _ = tx.send(ServerEvent::Data {
                                conn_id: my_conn_id,
                                bytes: text.into_bytes(),
                            });
                            if my_conn_id == 1 && seen_data >= drop_after_msgs {
                                // Drop the stream without a close handshake to simulate an abrupt server kill.
                                break;
                            }
                        }
                        Ok(WsMessage::Binary(bytes)) => {
                            seen_data = seen_data.saturating_add(1);
                            let _ = tx.send(ServerEvent::Data {
                                conn_id: my_conn_id,
                                bytes,
                            });
                            if my_conn_id == 1 && seen_data >= drop_after_msgs {
                                // Drop the stream without a close handshake to simulate an abrupt server kill.
                                break;
                            }
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
async fn bybit_public_actor_resubscribes_after_server_drop() {
    // We expect: initial subscribe, then dynamic subscribe update, then server drops, then reconnect
    // and initial subscribe includes both topics.
    let (addr, mut server_rx) = spawn_bybit_server_drop_conn1_after(2).await;

    let args = BybitPublicActorArgs {
        url: format!("ws://{addr}"),
        tls: WsTlsConfig::default(),
        initial_topics: vec!["publicTrade.BTCUSDT".to_string()],
        stale_threshold: Duration::from_secs(30),
        ws_buffers: shared_ws::ws::WebSocketBufferConfig::default(),
        outbound_capacity: 128,
        event_channel_capacity: 32,
        buffer_capacity: 1024,
        reconnect: ExponentialBackoffReconnect::new(
            Duration::from_millis(10),
            Duration::from_millis(50),
            1.5,
        ),
        enable_ping: false,
        ping_interval: Duration::from_secs(60),
        ping_timeout: Duration::from_secs(60),
    };

    let actor = BybitPublicActor::spawn(args);

    // Wait for connect #1.
    let conn1 = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Connected { conn_id })) => break conn_id,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for server connect"),
        }
    };
    assert_eq!(conn1, 1);

    // Initial subscribe on conn #1.
    let first = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id, bytes })) if conn_id == 1 => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for initial subscribe"),
        }
    };
    let first_s = std::str::from_utf8(first.as_ref()).unwrap();
    assert!(
        first_s.contains("\"op\":\"subscribe\""),
        "payload={first_s}"
    );
    assert!(first_s.contains("publicTrade.BTCUSDT"), "payload={first_s}");

    // Dynamic subscribe update on conn #1.
    actor
        .ask(SubscribeTopics {
            topics: vec!["publicTrade.ETHUSDT".to_string()],
        })
        .await
        .unwrap();

    let second = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id, bytes })) if conn_id == 1 => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for dynamic subscribe"),
        }
    };
    let second_s = std::str::from_utf8(second.as_ref()).unwrap();
    assert!(
        second_s.contains("\"op\":\"subscribe\""),
        "payload={second_s}"
    );
    assert!(
        second_s.contains("publicTrade.ETHUSDT"),
        "payload={second_s}"
    );

    // After observing the second message, the server drops conn #1; actor should reconnect.
    let conn2 = loop {
        match tokio::time::timeout(Duration::from_secs(4), server_rx.recv()).await {
            Ok(Some(ServerEvent::Connected { conn_id })) if conn_id == 2 => break conn_id,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for reconnect"),
        }
    };
    assert_eq!(conn2, 2);

    // Initial subscribe on conn #2 should include both topics (BTC + dynamically added ETH).
    let third = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id, bytes })) if conn_id == 2 => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for resubscribe on reconnect"),
        }
    };
    let third_s = std::str::from_utf8(third.as_ref()).unwrap();
    assert!(
        third_s.contains("\"op\":\"subscribe\""),
        "payload={third_s}"
    );
    assert!(third_s.contains("publicTrade.BTCUSDT"), "payload={third_s}");
    assert!(third_s.contains("publicTrade.ETHUSDT"), "payload={third_s}");

    // From the examples' perspective, our wrapper-level state should still match expectations.
    let stats = actor.ask(GetStats).await.unwrap();
    assert_eq!(stats.subscribed_topics, 2);

    let connected = loop {
        let ok = actor.ask(IsConnected).await.unwrap();
        if ok {
            break true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(connected);

    let _ = actor.stop_gracefully().await;
}
