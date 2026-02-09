use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use examples_ws::deribit::private_actor::{
    CreateOpenOrder, DeribitPrivateActor, DeribitPrivateActorArgs,
};
use kameo::Actor;
use shared_ws::client::accept_async;
use shared_ws::ws::{WsMessage, into_ws_message};
use sonic_rs::JsonValueTrait;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

async fn spawn_deribit_private_server(
    order_reply_delay: Duration,
) -> (SocketAddr, mpsc::UnboundedReceiver<Bytes>) {
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
                let mut authed = false;

                while let Some(msg) = ws.next().await {
                    let frame = match msg {
                        Ok(f) => f,
                        Err(_) => break,
                    };

                    let bytes: Bytes = match frame {
                        WsMessage::Text(text) => text.into_bytes(),
                        WsMessage::Binary(bytes) => bytes,
                        WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
                        WsMessage::Close(_) => break,
                    };

                    let _ = tx.send(bytes.clone());

                    let method = sonic_rs::get(bytes.as_ref(), &["method"])
                        .ok()
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_default();
                    let id = sonic_rs::get(bytes.as_ref(), &["id"])
                        .ok()
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0u64);

                    match method.as_str() {
                        "public/auth" => {
                            authed = true;
                            let resp = format!(
                                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"access_token\":\"t\"}}}}",
                                id
                            );
                            let _ = ws.send(into_ws_message(resp)).await;
                        }
                        "private/buy" => {
                            if !authed {
                                let resp = format!(
                                    "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"code\":13009,\"message\":\"unauthorized\"}}}}",
                                    id
                                );
                                let _ = ws.send(into_ws_message(resp)).await;
                            } else {
                                tokio::time::sleep(order_reply_delay).await;
                                let resp = format!(
                                    "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"order\":{{\"order_state\":\"open\"}}}}}}",
                                    id
                                );
                                let _ = ws.send(into_ws_message(resp)).await;
                            }
                        }
                        _ => {
                            let resp = format!(
                                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"ok\":true}}}}",
                                id
                            );
                            let _ = ws.send(into_ws_message(resp)).await;
                        }
                    }
                }
            });
        }
    });

    (addr, rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_create_open_order_over_ws_uses_delegated_confirmed_flow() {
    let (addr, mut rx) = spawn_deribit_private_server(Duration::from_millis(250)).await;

    let mut args = DeribitPrivateActorArgs::test_defaults(format!("ws://{addr}"));
    args.enable_ping = false;
    args.stale_threshold = Duration::from_secs(60);

    let actor = DeribitPrivateActor::spawn(args);

    // Kick off the order request without dropping it on timeout.
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    let actor2 = actor.clone();
    tokio::spawn(async move {
        let res = actor2
            .ask(CreateOpenOrder {
                instrument: "BTC-PERPETUAL".to_string(),
                amount: 10.0,
                price: 1.0,
            })
            .await;
        let _ = done_tx.send(res);
    });

    // If the actor incorrectly uses `Sent` instead of `Confirmed`, this would complete before
    // the server sends the delayed reply.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut done_rx)
            .await
            .is_err(),
        "order call completed before confirmation; expected Confirmed delegated reply flow"
    );

    let res = tokio::time::timeout(Duration::from_secs(3), &mut done_rx)
        .await
        .expect("timed out waiting for order result")
        .expect("order task dropped sender");

    let ack = res.expect("create order failed");
    assert!(ack.request_id > 0);

    // Server should observe auth then order.
    let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();

    let m1 = sonic_rs::get(first.as_ref(), &["method"])
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default();
    let m2 = sonic_rs::get(second.as_ref(), &["method"])
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default();

    assert_eq!(m1, "public/auth");
    assert_eq!(m2, "private/buy");
}
