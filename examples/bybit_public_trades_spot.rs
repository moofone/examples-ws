use std::time::Duration;

use bytes::Bytes;
use sonic_rs::JsonContainerTrait;
use sonic_rs::JsonValueTrait;

use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    ForwardAllIngress, WebSocketBufferConfig, WsDisconnectAction, WsDisconnectCause,
    WsEndpointHandler, WsErrorAction, WsParseOutcome,
};
use shared_ws::ws::{WebSocketActor, WebSocketActorArgs, WebSocketEvent, WsReconnectStrategy};

use examples_ws::bybit::ping::BybitJsonPingPong;
use examples_ws::endpoints::bybit::{
    BybitEvent, BybitSubscriptionManager, BybitTopic, BybitTopicHandler,
};

#[derive(Clone, Debug)]
struct ExponentialReconnect {
    base: Duration,
    max: Duration,
    cur: Duration,
}

impl ExponentialReconnect {
    fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            cur: base,
        }
    }
}

impl WsReconnectStrategy for ExponentialReconnect {
    fn next_delay(&mut self) -> Duration {
        let delay = self.cur;
        let next = (self.cur.as_secs_f64() * 1.5).min(self.max.as_secs_f64());
        self.cur = Duration::from_secs_f64(next.max(self.base.as_secs_f64()));
        delay
    }

    fn reset(&mut self) {
        self.cur = self.base;
    }

    fn should_retry(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct PrintingHandler {
    inner: BybitTopicHandler,
}

impl PrintingHandler {
    fn new(inner: BybitTopicHandler) -> Self {
        Self { inner }
    }
}

impl WsEndpointHandler for PrintingHandler {
    type Message = BybitEvent;
    type Error = examples_ws::endpoints::bybit::BybitProtocolError;
    type Subscription = BybitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        self.inner.subscription_manager()
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        self.inner.generate_auth()
    }

    fn parse(&mut self, _data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        // Prefer `parse_frame` (zero-copy).
        Err(examples_ws::endpoints::bybit::BybitProtocolError::InvalidJson)
    }

    fn parse_frame(
        &mut self,
        frame: &shared_ws::ws::WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        self.inner.parse_frame(frame)
    }

    fn handle_message(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            BybitEvent::Topic { topic, payload } => {
                if topic.0.starts_with("publicTrade.") {
                    for t in parse_public_trade_payload(&payload) {
                        // Print as a Rust struct (Debug) for easy log scraping / replay.
                        println!("{t:?}");
                    }
                } else {
                    println!(
                        "topic {} {}",
                        topic.0,
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

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Trade {
    symbol: String,
    side: String,
    price: String,
    qty: String,
    ts_ms: i64,
}

fn v_str<'a>(v: &'a sonic_rs::Value) -> Option<&'a str> {
    v.as_str()
}

fn parse_public_trade_payload(payload: &Bytes) -> Vec<Trade> {
    let Ok(value) = sonic_rs::from_slice::<sonic_rs::Value>(payload.as_ref()) else {
        return Vec::new();
    };

    let Some(data) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };

    data.iter()
        .filter_map(|row| {
            let symbol = row
                .get("symbol")
                .and_then(v_str)
                .or_else(|| row.get("s").and_then(v_str))
                .unwrap_or("?")
                .to_string();

            let side = row
                .get("side")
                .and_then(v_str)
                .or_else(|| row.get("S").and_then(v_str))
                .unwrap_or("?")
                .to_string();

            let price = row
                .get("price")
                .and_then(v_str)
                .or_else(|| row.get("p").and_then(v_str))
                .map(|s| s.to_string())
                .or_else(|| {
                    row.get("price")
                        .and_then(|v| v.as_f64())
                        .map(|f| f.to_string())
                })
                .or_else(|| row.get("p").and_then(|v| v.as_f64()).map(|f| f.to_string()))
                .unwrap_or_else(|| "?".to_string());

            let qty = row
                .get("size")
                .and_then(v_str)
                .or_else(|| row.get("v").and_then(v_str))
                .or_else(|| row.get("q").and_then(v_str))
                .map(|s| s.to_string())
                .or_else(|| {
                    row.get("size")
                        .and_then(|v| v.as_f64())
                        .map(|f| f.to_string())
                })
                .or_else(|| row.get("v").and_then(|v| v.as_f64()).map(|f| f.to_string()))
                .or_else(|| row.get("q").and_then(|v| v.as_f64()).map(|f| f.to_string()))
                .unwrap_or_else(|| "?".to_string());

            let ts_ms = row
                .get("tradeTime")
                .and_then(|v| v.as_i64())
                .or_else(|| row.get("T").and_then(|v| v.as_i64()))
                .or_else(|| row.get("t").and_then(|v| v.as_i64()))
                .unwrap_or(0);

            Some(Trade {
                symbol,
                side,
                price,
                qty,
                ts_ms,
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
        eprintln!("Usage: bybit_public_trades_spot [--symbol SOLUSDT] [--testnet] [--url WS_URL]");
        return;
    }

    let symbol = arg_value(&args, "--symbol").unwrap_or_else(|| "SOLUSDT".to_string());
    let topic = format!("publicTrade.{symbol}");

    let url = if let Some(u) = arg_value(&args, "--url") {
        u
    } else if has_flag(&args, "--testnet") {
        "wss://stream-testnet.bybit.com/v5/public/spot".to_string()
    } else {
        "wss://stream.bybit.com/v5/public/spot".to_string()
    };

    println!("connecting url={url} topic={topic}");

    let subs = BybitSubscriptionManager::with_initial_topics([BybitTopic(topic)]);
    let handler = PrintingHandler::new(BybitTopicHandler::new(subs));

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url,
        transport: TungsteniteTransport::default(),
        reconnect_strategy: ExponentialReconnect::new(
            Duration::from_secs(1),
            Duration::from_secs(30),
        ),
        handler,
        ingress: ForwardAllIngress::default(),
        ping_strategy: BybitJsonPingPong::public(Duration::from_secs(20), Duration::from_secs(30)),
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
