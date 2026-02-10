//! Bybit public trades (spot) example that emits every decoded trade into kameo.
//!
//! Key points:
//! - Uses `WsIngress` to decode trades in the reader loop (outside the actor runtime).
//! - Emits every trade as a kameo message (`WsIngressAction::EmitBatch`).
//! - Forwards trades to a dedicated sink actor via `tell(...).try_send()` (no await per trade).

use std::time::Duration;

use kameo::prelude::{Actor, ActorRef, Context, Message as KameoMessage};
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{WebSocketActor, WebSocketActorArgs, WebSocketEvent};
use shared_ws::ws::{
    WebSocketBufferConfig, WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction,
    WsFrame, WsIngress, WsIngressAction, WsMessageAction, WsParseOutcome,
};

use examples_ws::bybit::ping::BybitJsonPingPong;
use examples_ws::endpoints::bybit::{BybitSubscriptionManager, BybitTopic, BybitTopicHandler};

#[derive(Debug, Clone, Copy)]
enum Side {
    Buy,
    Sell,
    Unknown,
}

fn de_side<'de, D>(d: D) -> Result<Side, D::Error>
where
    D: Deserializer<'de>,
{
    struct SideVisitor;
    impl<'de> Visitor<'de> for SideVisitor {
        type Value = Side;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a side string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(match v {
                "Buy" | "buy" => Side::Buy,
                "Sell" | "sell" => Side::Sell,
                _ => Side::Unknown,
            })
        }
    }

    d.deserialize_any(SideVisitor)
}

fn de_f64<'de, D>(d: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    struct F64Visitor;
    impl<'de> Visitor<'de> for F64Visitor {
        type Value = f64;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a number or numeric string")
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(v)
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(v as f64)
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(v as f64)
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            lexical_core::parse::<f64>(v.as_bytes()).map_err(|_| E::custom("bad f64"))
        }
    }

    d.deserialize_any(F64Visitor)
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Deserialize)]
struct Trade {
    #[serde(rename = "T", alias = "tradeTime")]
    ts_ms: i64,
    #[serde(rename = "p", alias = "price", deserialize_with = "de_f64")]
    price: f64,
    #[serde(rename = "v", alias = "size", alias = "q", deserialize_with = "de_f64")]
    qty: f64,
    #[serde(rename = "S", alias = "side", deserialize_with = "de_side")]
    side: Side,
}

#[derive(Debug, Deserialize)]
struct BybitTradeFrame<'a> {
    #[serde(borrow)]
    topic: &'a str,
    data: Vec<Trade>,
}

#[derive(Default)]
struct TradePrinter;

impl Actor for TradePrinter {
    type Args = ();
    type Error = std::convert::Infallible;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}

impl KameoMessage<Trade> for TradePrinter {
    type Reply = ();

    async fn handle(&mut self, msg: Trade, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        println!("{msg:?}");
    }
}

#[derive(Clone)]
struct SinkHandler {
    inner: BybitTopicHandler,
    sink: ActorRef<TradePrinter>,
    dropped: u64,
}

impl WsEndpointHandler for SinkHandler {
    type Message = Trade;
    type Error = examples_ws::endpoints::bybit::BybitProtocolError;
    type Subscription = BybitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        self.inner.subscription_manager()
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        self.inner.generate_auth()
    }

    fn parse(&mut self, _data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        Ok(WsParseOutcome::Message(WsMessageAction::Continue))
    }

    fn parse_frame(
        &mut self,
        frame: &WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        // We primarily rely on ingress for trade frames; still parse forwarded frames to surface
        // server errors and reuse disconnect classification behavior.
        match self.inner.parse_frame(frame)? {
            WsParseOutcome::ServerError {
                code,
                message,
                data,
            } => Ok(WsParseOutcome::ServerError {
                code,
                message,
                data,
            }),
            WsParseOutcome::Message(WsMessageAction::Reconnect(r)) => {
                Ok(WsParseOutcome::Message(WsMessageAction::Reconnect(r)))
            }
            WsParseOutcome::Message(WsMessageAction::Shutdown) => {
                Ok(WsParseOutcome::Message(WsMessageAction::Shutdown))
            }
            WsParseOutcome::Message(_) => Ok(WsParseOutcome::Message(WsMessageAction::Continue)),
        }
    }

    fn handle_message(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        if self.sink.tell(msg).try_send().is_err() {
            self.dropped = self.dropped.saturating_add(1);
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

    fn on_open(&mut self) {
        self.inner.on_open();
    }
}

#[derive(Clone)]
struct BybitPublicTradeIngress {
    expected_topic: String,
}

impl BybitPublicTradeIngress {
    fn new(expected_topic: String) -> Self {
        Self { expected_topic }
    }
}

impl WsIngress for BybitPublicTradeIngress {
    type Message = Trade;

    fn on_frame(&mut self, frame: WsFrame) -> WsIngressAction<Self::Message> {
        let payload: &[u8] = match &frame {
            WsFrame::Text(b) => b.as_bytes().as_ref(),
            WsFrame::Binary(b) => b.as_ref(),
            WsFrame::Ping(_) | WsFrame::Pong(_) | WsFrame::Close(_) => {
                return WsIngressAction::Forward(frame);
            }
        };

        // Fast path for the hot stream: typed deserialization into just the fields we care about.
        // This is typically faster than building a full `sonic_rs::Value` DOM.
        let Ok(frame_v) = sonic_rs::from_slice::<BybitTradeFrame<'_>>(payload) else {
            return WsIngressAction::Forward(frame);
        };
        if frame_v.topic != self.expected_topic {
            return WsIngressAction::Forward(frame);
        }
        if frame_v.data.is_empty() {
            WsIngressAction::Ignore
        } else {
            WsIngressAction::EmitBatch(frame_v.data)
        }
    }
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
            "Usage: bybit_trades_emit_all_kameo [--symbol SOLUSDT] [--testnet] [--url WS_URL]"
        );
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

    let trade_printer = TradePrinter::spawn(());

    let subs = BybitSubscriptionManager::with_initial_topics([BybitTopic(topic.clone())]);
    let inner = BybitTopicHandler::new(subs);
    let handler = SinkHandler {
        inner,
        sink: trade_printer,
        dropped: 0,
    };

    let ingress = BybitPublicTradeIngress::new(topic);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url,
        transport: TungsteniteTransport::default(),
        reconnect_strategy: shared_ws::ws::ExponentialBackoffReconnect::default(),
        handler,
        ingress,
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
