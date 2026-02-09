use std::sync::Arc;
use std::time::Duration;

use examples_ws::deribit::instruments::{DeribitInstrument, DeribitInstrumentsProvider};
use examples_ws::deribit::options_actor::{
    Connect, DeribitOptionsActor, DeribitOptionsActorArgs, SubscribeCurrencies,
};
use futures_util::future::BoxFuture;
use kameo::Actor;
use shared_ws::ws::WsTlsConfig;

use crate::support::ws_mock::{WsMockFrameKind, WsMockServer};

async fn spawn_ws_server() -> WsMockServer {
    WsMockServer::spawn().await
}

#[derive(Clone)]
struct MockInstruments;

impl DeribitInstrumentsProvider for MockInstruments {
    fn get_instruments(
        &self,
        currency: &str,
    ) -> BoxFuture<'static, Result<Vec<DeribitInstrument>, String>> {
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
    let mut server = spawn_ws_server().await;
    let addr = server.addr;

    let provider: Arc<dyn DeribitInstrumentsProvider> = Arc::new(MockInstruments);
    let args = DeribitOptionsActorArgs {
        url: format!("ws://{addr}"),
        tls: WsTlsConfig {
            validate_certs: false,
        },
        ticker_interval: "raw".to_string(),
        stale_threshold: Duration::from_secs(30),
        enable_ping: false,
        ping_interval: Duration::from_secs(60),
        ping_timeout: Duration::from_secs(60),
        ws_buffers: shared_ws::ws::WebSocketBufferConfig::default(),
        outbound_capacity: 256,
        event_channel_capacity: 128,
        buffer_capacity: 1024,
        instruments_provider: provider,
        reconnect: examples_ws::deribit::reconnect::DeribitReconnect::default(),
    };

    let actor = DeribitOptionsActor::spawn(args);
    actor.ask(Connect).await.unwrap();

    // Wait for connect.
    let _conn_id = server.wait_connected(Duration::from_secs(2)).await;

    actor
        .ask(SubscribeCurrencies {
            currencies: vec!["BTC".to_string()],
        })
        .await
        .unwrap();

    // The next message should include the generated public/subscribe request.
    let frame = server.wait_frame(Duration::from_secs(2), None).await;
    assert!(
        matches!(frame.kind, WsMockFrameKind::Text | WsMockFrameKind::Binary),
        "unexpected frame kind: {:?}",
        frame.kind
    );
    let bytes = frame.bytes;

    let s = std::str::from_utf8(bytes.as_ref()).unwrap();
    assert!(s.contains("\"method\":\"public/subscribe\""), "payload={s}");
    assert!(s.contains("ticker.BTC-27DEC24-80000-P.raw"), "payload={s}");
    assert!(s.contains("ticker.BTC-PERPETUAL.raw"), "payload={s}");
    assert!(
        s.contains("deribit_volatility_index.btc_usd"),
        "payload={s}"
    );
}
