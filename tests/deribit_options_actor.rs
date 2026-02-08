use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use kameo::Actor;
use shared_ws::client::accept_async;
use examples_ws::deribit::instruments::{DeribitInstrument, DeribitInstrumentsProvider};
use examples_ws::deribit::options_actor::{Connect, DeribitOptionsActor, DeribitOptionsActorArgs, SubscribeCurrencies};
use shared_ws::ws::WsTlsConfig;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[derive(Debug)]
enum ServerEvent {
    Connected,
    Data(Bytes),
}

async fn spawn_ws_server() -> (SocketAddr, mpsc::UnboundedReceiver<ServerEvent>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut ws = accept_async(stream).await.unwrap();
                let _ = tx.send(ServerEvent::Connected);

                while let Some(msg) = ws.next().await {
                    match msg {
                        Ok(shared_ws::core::WsFrame::Text(bytes))
                        | Ok(shared_ws::core::WsFrame::Binary(bytes)) => {
                            let _ = tx.send(ServerEvent::Data(bytes));
                        }
                        Ok(shared_ws::core::WsFrame::Close(_)) => break,
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
struct MockInstruments;

impl DeribitInstrumentsProvider for MockInstruments {
    fn get_instruments(&self, currency: &str) -> BoxFuture<'static, Result<Vec<DeribitInstrument>, String>> {
        let currency = currency.to_string();
        Box::pin(async move {
            if currency != "BTC" {
                return Ok(Vec::new());
            }
            Ok(vec![
                DeribitInstrument {
                    instrument_name: "BTC-27DEC24-80000-P".to_string(),
                    kind: "option".to_string(),
                    is_active: true,
                    expiration_timestamp_ms: Some(1_735_257_600_000), // 2024-12-27T00:00:00Z
                },
                DeribitInstrument {
                    instrument_name: "BTC-01JAN20-10000-C".to_string(),
                    kind: "option".to_string(),
                    is_active: false,
                    expiration_timestamp_ms: Some(1_577_836_800_000),
                },
                DeribitInstrument {
                    instrument_name: "BTC-PERPETUAL".to_string(),
                    kind: "future".to_string(),
                    is_active: true,
                    expiration_timestamp_ms: None,
                },
            ])
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn options_actor_subscribe_currencies_emits_expected_channels() {
    let (addr, mut server_rx) = spawn_ws_server().await;

    let provider: Arc<dyn DeribitInstrumentsProvider> = Arc::new(MockInstruments);
    let args = DeribitOptionsActorArgs {
        url: format!("ws://{addr}"),
        tls: WsTlsConfig { validate_certs: false },
        ticker_interval: "raw".to_string(),
        stale_threshold: Duration::from_secs(30),
        enable_ping: false,
        ping_interval: Duration::from_secs(60),
        ping_timeout: Duration::from_secs(60),
        ws_buffers: shared_ws::core::WebSocketBufferConfig::default(),
        outbound_capacity: 256,
        event_channel_capacity: 128,
        buffer_capacity: 1024,
        instruments_provider: provider,
        reconnect: examples_ws::deribit::reconnect::DeribitReconnect::default(),
    };

    let actor = DeribitOptionsActor::spawn(args);
    actor.ask(Connect).await.unwrap();

    // Wait for connect.
    loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Connected)) => break,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for server connect"),
        }
    }

    actor
        .ask(SubscribeCurrencies {
            currencies: vec!["BTC".to_string()],
        })
        .await
        .unwrap();

    // The next message should include the generated public/subscribe request.
    let bytes = loop {
        match tokio::time::timeout(Duration::from_secs(2), server_rx.recv()).await {
            Ok(Some(ServerEvent::Data(bytes))) => break bytes,
            Ok(Some(_)) => {}
            Ok(None) => panic!("server channel closed"),
            Err(_) => panic!("timeout waiting for subscribe payload"),
        }
    };

    let s = std::str::from_utf8(bytes.as_ref()).unwrap();
    assert!(s.contains("\"method\":\"public/subscribe\""), "payload={s}");
    assert!(s.contains("ticker.BTC-27DEC24-80000-P.raw"), "payload={s}");
    assert!(s.contains("ticker.BTC-PERPETUAL.raw"), "payload={s}");
    assert!(s.contains("deribit_volatility_index.btc_usd"), "payload={s}");
}
