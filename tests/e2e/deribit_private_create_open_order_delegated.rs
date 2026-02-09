use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use examples_ws::deribit::private_actor::{
    CreateOpenOrder, DeribitPrivateActor, DeribitPrivateActorArgs,
};
use kameo::Actor;
use shared_rate_limiter::Config;
use shared_ws::client::accept_async;
use shared_ws::ws::{WsMessage, into_ws_message};
use sonic_rs::JsonValueTrait;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::debug;

use crate::mock_deribit_wss::{
    DeribitServerEvent, deribit_auth_ok, deribit_buy_ok, parse_jsonrpc_method_and_id,
    spawn_deribit_mock_wss,
};

#[derive(Clone, Copy, Debug)]
enum BuyReplyMode {
    Ok,
    Error,
    NoReply,
}

async fn spawn_deribit_private_server(
    order_reply_delay: Duration,
) -> (SocketAddr, mpsc::UnboundedReceiver<Bytes>) {
    spawn_deribit_private_server_mode(order_reply_delay, BuyReplyMode::Ok).await
}

async fn spawn_deribit_private_server_mode(
    order_reply_delay: Duration,
    buy_mode: BuyReplyMode,
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

                    if let Ok(text) = std::str::from_utf8(bytes.as_ref()) {
                        debug!(direction="client->server", method=%method, id=%id, text=%text, "deribit private mock: request");
                    } else {
                        debug!(direction="client->server", method=%method, id=%id, bytes=bytes.len(), "deribit private mock: request (binary)");
                    }

                    match method.as_str() {
                        "public/auth" => {
                            authed = true;
                            let resp = format!(
                                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"access_token\":\"t\"}}}}",
                                id
                            );
                            debug!(direction="server->client", method="public/auth", id=%id, text=%resp, "deribit private mock: response");
                            let _ = ws.send(into_ws_message(resp)).await;
                        }
                        "private/buy" => {
                            if !authed {
                                let resp = format!(
                                    "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"code\":13009,\"message\":\"unauthorized\"}}}}",
                                    id
                                );
                                debug!(direction="server->client", method="private/buy", id=%id, text=%resp, "deribit private mock: response");
                                let _ = ws.send(into_ws_message(resp)).await;
                            } else {
                                match buy_mode {
                                    BuyReplyMode::Ok => {
                                        tokio::time::sleep(order_reply_delay).await;
                                        let resp = format!(
                                            "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"order\":{{\"order_state\":\"open\"}}}}}}",
                                            id
                                        );
                                        debug!(direction="server->client", method="private/buy", id=%id, text=%resp, "deribit private mock: response");
                                        let _ = ws.send(into_ws_message(resp)).await;
                                    }
                                    BuyReplyMode::Error => {
                                        tokio::time::sleep(order_reply_delay).await;
                                        let resp = format!(
                                            "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"code\":10000,\"message\":\"order rejected\"}}}}",
                                            id
                                        );
                                        debug!(direction="server->client", method="private/buy", id=%id, text=%resp, "deribit private mock: response");
                                        let _ = ws.send(into_ws_message(resp)).await;
                                    }
                                    BuyReplyMode::NoReply => {
                                        // Intentionally do nothing: exercise Unconfirmed path.
                                    }
                                }
                            }
                        }
                        _ => {
                            let resp = format!(
                                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"ok\":true}}}}",
                                id
                            );
                            debug!(direction="server->client", method=%method, id=%id, text=%resp, "deribit private mock: response");
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
    crate::support::init_tracing();

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_create_open_order_is_denied_by_local_rate_limiter_before_sending() {
    crate::support::init_tracing();

    let (addr, mut rx) = spawn_deribit_private_server(Duration::from_millis(0)).await;

    let mut args = DeribitPrivateActorArgs::test_defaults(format!("ws://{addr}"));
    args.enable_ping = false;
    args.stale_threshold = Duration::from_secs(60);

    // Deny immediately: default auth/order costs are 500, but capacity is only 10.
    args.rate_limiter_cfg = Config::token_bucket(10, 0);

    let actor = DeribitPrivateActor::spawn(args);

    let res = actor
        .ask(CreateOpenOrder {
            instrument: "BTC-PERPETUAL".to_string(),
            amount: 10.0,
            price: 1.0,
        })
        .await;

    let err = res.expect_err("expected local rate limiter deny");
    match err {
        kameo::error::SendError::HandlerError(msg) => {
            assert!(
                msg.contains("locally rate limited"),
                "unexpected error: {msg}"
            );
        }
        other => panic!("expected HandlerError(String), got {other:?}"),
    }

    // Ensure we did not send any JSON-RPC frames to the server.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .is_err(),
        "expected no frames to be sent when rate limited"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_create_open_order_surfaces_endpoint_rejection() {
    crate::support::init_tracing();

    let (addr, mut rx) =
        spawn_deribit_private_server_mode(Duration::from_millis(0), BuyReplyMode::Error).await;

    let mut args = DeribitPrivateActorArgs::test_defaults(format!("ws://{addr}"));
    args.enable_ping = false;
    args.stale_threshold = Duration::from_secs(60);

    let actor = DeribitPrivateActor::spawn(args);
    let res = actor
        .ask(CreateOpenOrder {
            instrument: "BTC-PERPETUAL".to_string(),
            amount: 10.0,
            price: 1.0,
        })
        .await;

    let err = res.expect_err("expected endpoint rejection");
    match err {
        kameo::error::SendError::HandlerError(msg) => {
            assert!(
                msg.contains("EndpointRejected") || msg.contains("order rejected"),
                "unexpected error: {msg}"
            );
        }
        other => panic!("expected HandlerError(String), got {other:?}"),
    }

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_create_open_order_times_out_unconfirmed_when_server_never_replies() {
    crate::support::init_tracing();

    let (addr, mut rx) =
        spawn_deribit_private_server_mode(Duration::from_millis(0), BuyReplyMode::NoReply).await;

    let mut args = DeribitPrivateActorArgs::test_defaults(format!("ws://{addr}"));
    args.enable_ping = false;
    args.stale_threshold = Duration::from_secs(60);
    args.request_timeout = Duration::from_millis(150);

    let actor = DeribitPrivateActor::spawn(args);
    let res = actor
        .ask(CreateOpenOrder {
            instrument: "BTC-PERPETUAL".to_string(),
            amount: 10.0,
            price: 1.0,
        })
        .await;

    let err = res.expect_err("expected unconfirmed timeout");
    match err {
        kameo::error::SendError::HandlerError(msg) => {
            assert!(msg.contains("Unconfirmed"), "unexpected error: {msg}");
        }
        other => panic!("expected HandlerError(String), got {other:?}"),
    }

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_create_open_order_times_out_unconfirmed_when_auth_never_replies_over_wss() {
    crate::support::init_tracing();

    let (handle, mut events) = spawn_deribit_mock_wss().await;

    // Driver: observe auth request but never reply.
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            match ev {
                DeribitServerEvent::InboundText { text } => {
                    let (method, _id) = parse_jsonrpc_method_and_id(&text);
                    if let Some(method) = method {
                        let _ = seen_tx.send(method);
                    }
                }
                _ => {}
            }
        }
    });

    let mut args = DeribitPrivateActorArgs::test_defaults(handle.url());
    args.transport = handle.client_transport();
    args.enable_ping = false;
    args.stale_threshold = Duration::from_secs(60);
    args.request_timeout = Duration::from_millis(150);

    let actor = DeribitPrivateActor::spawn(args);

    let res = actor
        .ask(CreateOpenOrder {
            instrument: "BTC-PERPETUAL".to_string(),
            amount: 10.0,
            price: 1.0,
        })
        .await;

    let err = res.expect_err("expected unconfirmed timeout on auth");
    match err {
        kameo::error::SendError::HandlerError(msg) => {
            assert!(msg.contains("Unconfirmed"), "unexpected error: {msg}");
        }
        other => panic!("expected HandlerError(String), got {other:?}"),
    }

    // We should have seen only an auth request, and never a buy request.
    let first = tokio::time::timeout(Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, "public/auth");

    assert!(
        tokio::time::timeout(Duration::from_millis(250), seen_rx.recv())
            .await
            .is_err(),
        "expected no further requests (private/buy) after auth timeout"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deribit_create_open_order_times_out_unconfirmed_when_buy_reply_is_delayed_past_deadline_over_wss()
 {
    crate::support::init_tracing();

    let (handle, mut events) = spawn_deribit_mock_wss().await;
    let deadline = Duration::from_millis(150);

    // Driver: reply to auth immediately, but delay buy reply beyond the client's confirm deadline.
    let h = handle.clone();
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            match ev {
                DeribitServerEvent::InboundText { text } => {
                    let (method, id) = parse_jsonrpc_method_and_id(&text);
                    if let Some(method) = method {
                        let _ = seen_tx.send(method.clone());
                        match method.as_str() {
                            "public/auth" => {
                                if let Some(id) = id {
                                    h.send_text(deribit_auth_ok(id));
                                }
                            }
                            "private/buy" => {
                                if let Some(id) = id {
                                    tokio::time::sleep(deadline * 2).await;
                                    h.send_text(deribit_buy_ok(id, 1_700_000_000_000));
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    });

    let mut args = DeribitPrivateActorArgs::test_defaults(handle.url());
    args.transport = handle.client_transport();
    args.enable_ping = false;
    args.stale_threshold = Duration::from_secs(60);
    args.request_timeout = deadline;

    let actor = DeribitPrivateActor::spawn(args);

    let res = actor
        .ask(CreateOpenOrder {
            instrument: "BTC-PERPETUAL".to_string(),
            amount: 10.0,
            price: 1.0,
        })
        .await;

    let err = res.expect_err("expected unconfirmed timeout on delayed buy reply");
    match err {
        kameo::error::SendError::HandlerError(msg) => {
            assert!(msg.contains("Unconfirmed"), "unexpected error: {msg}");
        }
        other => panic!("expected HandlerError(String), got {other:?}"),
    }

    // Server should have observed auth then buy.
    let m1 = tokio::time::timeout(Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(2), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m1, "public/auth");
    assert_eq!(m2, "private/buy");
}
