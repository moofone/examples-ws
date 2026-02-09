//! Bybit public trades (spot) example that aggregates trades into candles and only emits on 1% moves.
//!
//! Key points:
//! - Uses `WsIngress` to decode + aggregate in the reader loop (outside the actor runtime).
//! - Only emits candle updates when the close moves by >= 1% (epsilon gating).
//! - Uses fixed-capacity buffers (`Updates<N>`) to avoid heap allocation in the hot path.

use std::time::Duration;

use kameo::prelude::{Actor, ActorRef, Context, Message as KameoMessage};
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{WebSocketActor, WebSocketActorArgs, WebSocketEvent, WsTlsConfig};
use shared_ws::ws::{
    WebSocketBufferConfig, WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction,
    WsFrame, WsIngress, WsIngressAction, WsMessageAction, WsParseOutcome,
};

use examples_ws::bybit::ping::BybitJsonPingPong;
use examples_ws::candles::{CandleAggregator, CandleEmitReason, CandleUpdate, Trade, Updates};
use examples_ws::endpoints::bybit::{BybitSubscriptionManager, BybitTopic, BybitTopicHandler};

type CandleUpdates = Updates<32>;

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

#[derive(Debug, Clone, Copy, Deserialize)]
struct BybitTradeLite {
    #[serde(rename = "T", alias = "tradeTime")]
    ts_ms: i64,
    #[serde(rename = "p", alias = "price", deserialize_with = "de_f64")]
    price: f64,
    #[serde(rename = "v", alias = "size", alias = "q", deserialize_with = "de_f64")]
    qty: f64,
}

#[derive(Debug, Deserialize)]
struct BybitTradeFrame<'a> {
    #[serde(borrow)]
    topic: &'a str,
    data: Vec<BybitTradeLite>,
}

#[derive(Default)]
struct CandlePrinter;

impl Actor for CandlePrinter {
    type Args = ();
    type Error = std::convert::Infallible;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}

impl KameoMessage<CandleUpdate> for CandlePrinter {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: CandleUpdate,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("{msg:?}");
    }
}

#[derive(Clone)]
struct SinkHandler {
    inner: BybitTopicHandler,
    sink: ActorRef<CandlePrinter>,
    dropped: u64,
}

impl WsEndpointHandler for SinkHandler {
    type Message = CandleUpdates;
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
        // Still parse forwarded frames to surface server errors and reuse disconnect classification behavior.
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
        for u in msg.items.iter().take(msg.len) {
            if self.sink.tell(*u).try_send().is_err() {
                self.dropped = self.dropped.saturating_add(1);
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

    fn on_open(&mut self) {
        self.inner.on_open();
    }
}

struct BybitTradesToCandlesIngress {
    expected_topic: String,
    agg: CandleAggregator<1>,
}

impl BybitTradesToCandlesIngress {
    fn new(expected_topic: String, eps_frac: f64) -> Self {
        // 1m candles, epsilon configurable via `--eps` (fractional, e.g. 0.01 == 1%).
        Self {
            expected_topic,
            agg: CandleAggregator::<1>::new([60_000], eps_frac),
        }
    }
}

impl WsIngress for BybitTradesToCandlesIngress {
    type Message = CandleUpdates;

    fn on_frame(&mut self, frame: WsFrame) -> WsIngressAction<Self::Message> {
        let payload: &[u8] = match &frame {
            WsFrame::Text(b) => b.as_bytes().as_ref(),
            WsFrame::Binary(b) => b.as_ref(),
            WsFrame::Ping(_) | WsFrame::Pong(_) | WsFrame::Close(_) => {
                return WsIngressAction::Forward(frame);
            }
        };

        let Ok(frame_v) = sonic_rs::from_slice::<BybitTradeFrame<'_>>(payload) else {
            return WsIngressAction::Forward(frame);
        };
        if frame_v.topic != self.expected_topic {
            return WsIngressAction::Forward(frame);
        }

        let fallback = CandleUpdate {
            candle: examples_ws::candles::Candle {
                start_ms: 0,
                tf_ms: 0,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                volume: 0.0,
            },
            reason: CandleEmitReason::Finalized,
        };

        let mut out = CandleUpdates::new(fallback);

        for row in frame_v.data {
            let updates = self.agg.on_trade::<8>(Trade {
                ts_ms: row.ts_ms,
                price: row.price,
                qty: row.qty,
            });
            for u in updates.items.iter().take(updates.len) {
                // "Simple" gating: only emit close-epsilon updates (>= 1% move in close).
                if matches!(u.reason, CandleEmitReason::CloseEpsilon) {
                    out.push(*u);
                }
            }
        }

        if out.len == 0 {
            WsIngressAction::Ignore
        } else {
            WsIngressAction::Emit(out)
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
            "Usage: bybit_trades_to_candles_eps1 [--symbol SOLUSDT] [--eps 0.01] [--testnet] [--url WS_URL]"
        );
        return;
    }

    let symbol = arg_value(&args, "--symbol").unwrap_or_else(|| "SOLUSDT".to_string());
    let eps_frac = arg_value(&args, "--eps")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.01);
    let topic = format!("publicTrade.{symbol}");

    let url = if let Some(u) = arg_value(&args, "--url") {
        u
    } else if has_flag(&args, "--testnet") {
        "wss://stream-testnet.bybit.com/v5/public/spot".to_string()
    } else {
        "wss://stream.bybit.com/v5/public/spot".to_string()
    };

    println!("connecting url={url} topic={topic}");

    let candle_printer = CandlePrinter::spawn(());

    let subs = BybitSubscriptionManager::with_initial_topics([BybitTopic(topic.clone())]);
    let inner = BybitTopicHandler::new(subs);
    let handler = SinkHandler {
        inner,
        sink: candle_printer,
        dropped: 0,
    };

    let ingress = BybitTradesToCandlesIngress::new(topic, eps_frac);

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url,
        tls: WsTlsConfig::default(),
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
