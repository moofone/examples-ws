use std::time::Duration;

use bytes::Bytes;
use kameo::Actor;
use sonic_rs::JsonContainerTrait;
use sonic_rs::JsonValueTrait;

use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    ForwardAllIngress, WebSocketBufferConfig, WsDisconnectAction, WsDisconnectCause,
    WsEndpointHandler, WsErrorAction, WsParseOutcome,
};
use shared_ws::ws::{
    ProtocolPingPong, WebSocketActor, WebSocketActorArgs, WebSocketEvent,
};

use examples_ws::deribit::reconnect::DeribitReconnect;
use examples_ws::endpoints::deribit::{
    DeribitChannel, DeribitEvent, DeribitProtocolError, DeribitPublicHandler,
    DeribitSubscriptionManager,
};

#[derive(Clone)]
struct PrintingHandler {
    inner: DeribitPublicHandler,
}

impl PrintingHandler {
    fn new(inner: DeribitPublicHandler) -> Self {
        Self { inner }
    }
}

impl WsEndpointHandler for PrintingHandler {
    type Message = DeribitEvent;
    type Error = DeribitProtocolError;
    type Subscription = DeribitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        self.inner.subscription_manager()
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        self.inner.generate_auth()
    }

    fn parse(&mut self, _data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        // Prefer `parse_frame` (zero-copy).
        Err(DeribitProtocolError::InvalidJson)
    }

    fn parse_frame(
        &mut self,
        frame: &shared_ws::ws::WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        self.inner.parse_frame(frame)
    }

    fn handle_message(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            DeribitEvent::Subscription { channel, payload } => {
                if channel.0.starts_with("trades.") {
                    for t in parse_public_trade_payload(&payload) {
                        // Print as a Rust struct (Debug) for easy log scraping / replay.
                        println!("{t:?}");
                    }
                } else {
                    println!(
                        "channel {} {}",
                        channel.0,
                        String::from_utf8_lossy(payload.as_ref())
                    );
                }
            }
        }
        Ok(())
    }

    fn handle_server_error(
        &mut self,
        code: Option<i32>,
        message: &str,
        data: Option<sonic_rs::Value>,
    ) -> WsErrorAction {
        self.inner.handle_server_error(code, message, data)
    }

    fn reset_state(&mut self) {
        self.inner.reset_state()
    }

    fn classify_disconnect(&self, cause: &WsDisconnectCause) -> WsDisconnectAction {
        self.inner.classify_disconnect(cause)
    }
}

#[derive(Debug, Clone)]
struct Trade {
    instrument: String,
    direction: String,
    price: String,
    amount: String,
    ts_ms: i64,
    trade_id: String,
}

fn v_str<'a>(v: &'a sonic_rs::Value) -> Option<&'a str> {
    v.as_str()
}

fn v_to_string(v: &sonic_rs::Value) -> Option<String> {
    v.as_str()
        .map(|s| s.to_string())
        .or_else(|| v.as_i64().map(|n| n.to_string()))
        .or_else(|| v.as_u64().map(|n| n.to_string()))
        .or_else(|| v.as_f64().map(|f| f.to_string()))
}

fn parse_public_trade_payload(payload: &Bytes) -> Vec<Trade> {
    let Ok(value) = sonic_rs::from_slice::<sonic_rs::Value>(payload.as_ref()) else {
        return Vec::new();
    };

    let Some(data) = value
        .get("params")
        .and_then(|v| v.get("data"))
        .and_then(|v| v.as_array())
    else {
        return Vec::new();
    };

    data.iter()
        .filter_map(|row| {
            let instrument = row
                .get("instrument_name")
                .and_then(v_str)
                .unwrap_or("?")
                .to_string();

            let direction = row
                .get("direction")
                .and_then(v_str)
                .unwrap_or("?")
                .to_string();

            let price = row
                .get("price")
                .and_then(v_to_string)
                .unwrap_or_else(|| "?".to_string());

            let amount = row
                .get("amount")
                .and_then(v_to_string)
                .unwrap_or_else(|| "?".to_string());

            let ts_ms = row.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);

            let trade_id = row
                .get("trade_id")
                .and_then(v_str)
                .unwrap_or("")
                .to_string();

            Some(Trade {
                instrument,
                direction,
                price,
                amount,
                ts_ms,
                trade_id,
            })
        })
        .collect()
}

fn arg_value(args: &[String], name: &str) -> Option<String> {
    let mut i = 0usize;
    while i < args.len() {
        if args[i] == name {
            return args.get(i + 1).cloned();
        }
        i += 1;
    }
    None
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if has_flag(&args, "--help") || has_flag(&args, "-h") {
        eprintln!(
            "Usage: deribit_public_trades [--instrument BTC-PERPETUAL] [--testnet] [--url WS_URL]"
        );
        return;
    }

    let instrument =
        arg_value(&args, "--instrument").unwrap_or_else(|| "BTC-PERPETUAL".to_string());
    let channel = format!("trades.{instrument}.raw");

    let url = if let Some(u) = arg_value(&args, "--url") {
        u
    } else if has_flag(&args, "--testnet") {
        "wss://test.deribit.com/ws/api/v2".to_string()
    } else {
        "wss://www.deribit.com/ws/api/v2".to_string()
    };

    println!("connecting url={url} channel={channel}");

    let subs = DeribitSubscriptionManager::with_initial_channels([DeribitChannel(channel)]);
    let handler = PrintingHandler::new(DeribitPublicHandler::new(subs));

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url,
        transport: TungsteniteTransport::default(),
        reconnect_strategy: DeribitReconnect::default(),
        handler,
        ingress: ForwardAllIngress::default(),
        ping_strategy: ProtocolPingPong::new(Duration::from_secs(20), Duration::from_secs(30)),
        enable_ping: true,
        stale_threshold: Duration::from_secs(60),
        ws_buffers: WebSocketBufferConfig::default(),
        outbound_capacity: 128,
        circuit_breaker: None,
        latency_policy: None,
        payload_latency_sampling: None,
        registration: None,
        metrics: None,
    });

    let _ = actor.tell(WebSocketEvent::Connect).send().await;

    // Run until Ctrl+C.
    let _ = tokio::signal::ctrl_c().await;
}
