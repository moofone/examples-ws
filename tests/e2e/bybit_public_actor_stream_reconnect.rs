use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use examples_ws::bybit::public_actor::{
    BybitPublicActor, BybitPublicActorArgs, GetStats, SetEventSink, SubscribeTopics,
};
use examples_ws::endpoints::bybit::BybitEvent;
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

async fn spawn_ws_server_send_then_drop() -> (SocketAddr, mpsc::UnboundedReceiver<ServerEvent>) {
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

                let mut outbound_seen = 0usize;
                while let Some(msg) = ws.next().await {
                    match msg {
                        Ok(WsMessage::Text(text)) => {
                            outbound_seen = outbound_seen.saturating_add(1);
                            let _ = tx.send(ServerEvent::Data {
                                conn_id: my_conn_id,
                                bytes: text.into_bytes(),
                            });

                            // After we've observed the client subscribe(s), push a few topic notifications
                            // to ensure the kameo wrapper continues forwarding across reconnect.
                            if my_conn_id == 1 && outbound_seen == 2 {
                                let notif1 = Bytes::from_static(br#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"symbol":"BTCUSDT","ts":1700000000000}]}"#);
                                let notif2 = Bytes::from_static(br#"{"topic":"publicTrade.ETHUSDT","type":"snapshot","data":[{"symbol":"ETHUSDT","ts":1700000000001}]}"#);
                                let _ = ws.send(shared_ws::ws::into_ws_message(notif1)).await;
                                let _ = ws.send(shared_ws::ws::into_ws_message(notif2)).await;

                                // Abruptly drop the socket (no close handshake).
                                break;
                            }

                            if my_conn_id == 2 && outbound_seen == 1 {
                                let notif3 = Bytes::from_static(br#"{"topic":"publicTrade.ETHUSDT","type":"snapshot","data":[{"symbol":"ETHUSDT","ts":1700000000002}]}"#);
                                let _ = ws.send(shared_ws::ws::into_ws_message(notif3)).await;
                            }
                        }
                        Ok(WsMessage::Binary(bytes)) => {
                            outbound_seen = outbound_seen.saturating_add(1);
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
async fn bybit_public_actor_forwards_events_and_recovers_after_socket_drop() {
    let (addr, mut server_rx) = spawn_ws_server_send_then_drop().await;

    let args = BybitPublicActorArgs {
        url: format!("ws://{addr}"),
        tls: WsTlsConfig::default(),
        initial_topics: vec!["publicTrade.BTCUSDT".to_string()],
        stale_threshold: Duration::from_secs(30),
        ws_buffers: shared_ws::ws::WebSocketBufferConfig::default(),
        outbound_capacity: 128,
        event_channel_capacity: 64,
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

    let (sink_tx, mut sink_rx) = mpsc::channel::<BybitEvent>(64);
    actor.ask(SetEventSink { sink: sink_tx }).await.unwrap();

    // Wait for conn #1 and initial subscribe.
    let conn1 = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Connected { conn_id })) => break conn_id,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for conn1"),
        }
    };
    assert_eq!(conn1, 1);

    let sub1 = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id: 1, bytes })) => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for subscribe1"),
        }
    };
    let sub1_s = std::str::from_utf8(sub1.as_ref()).unwrap();
    assert!(sub1_s.contains("\"op\":\"subscribe\""), "payload={sub1_s}");
    assert!(sub1_s.contains("publicTrade.BTCUSDT"), "payload={sub1_s}");

    // Add ETH subscription; server will observe it as the second outbound message on conn #1,
    // then send 2 notifications and drop the socket.
    actor
        .ask(SubscribeTopics {
            topics: vec!["publicTrade.ETHUSDT".to_string()],
        })
        .await
        .unwrap();

    let sub2 = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id: 1, bytes })) => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for subscribe2"),
        }
    };
    let sub2_s = std::str::from_utf8(sub2.as_ref()).unwrap();
    assert!(sub2_s.contains("\"op\":\"subscribe\""), "payload={sub2_s}");
    assert!(sub2_s.contains("publicTrade.ETHUSDT"), "payload={sub2_s}");

    // We should receive the 2 notifications forwarded via the kameo wrapper before reconnect completes.
    let evt1 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let evt2 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let topics = match (evt1, evt2) {
        (BybitEvent::Topic { topic: t1, .. }, BybitEvent::Topic { topic: t2, .. }) => {
            vec![t1.0, t2.0]
        }
    };
    assert!(topics.contains(&"publicTrade.BTCUSDT".to_string()));
    assert!(topics.contains(&"publicTrade.ETHUSDT".to_string()));

    // After the server drops conn #1, the actor should reconnect and resubscribe to both topics.
    let conn2 = loop {
        match tokio::time::timeout(Duration::from_secs(4), server_rx.recv()).await {
            Ok(Some(ServerEvent::Connected { conn_id })) if conn_id == 2 => break conn_id,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for conn2"),
        }
    };
    assert_eq!(conn2, 2);

    let sub3 = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data { conn_id: 2, bytes })) => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for resubscribe"),
        }
    };
    let sub3_s = std::str::from_utf8(sub3.as_ref()).unwrap();
    assert!(sub3_s.contains("\"op\":\"subscribe\""), "payload={sub3_s}");
    assert!(sub3_s.contains("publicTrade.BTCUSDT"), "payload={sub3_s}");
    assert!(sub3_s.contains("publicTrade.ETHUSDT"), "payload={sub3_s}");

    // Server sends one more ETH notification on conn #2; it should be forwarded as well.
    let evt3 = tokio::time::timeout(Duration::from_secs(2), sink_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match evt3 {
        BybitEvent::Topic { topic, .. } => assert_eq!(topic.0, "publicTrade.ETHUSDT"),
    }

    let stats = actor.ask(GetStats).await.unwrap();
    assert_eq!(stats.subscribed_topics, 2);

    let _ = actor.stop_gracefully().await;
}
