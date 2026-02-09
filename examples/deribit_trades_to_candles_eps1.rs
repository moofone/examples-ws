//! Deribit public trades example that aggregates trades into candles and only emits on 1% moves.
//!
//! Key points:
//! - Uses `WsIngress` to decode + aggregate in the reader loop (outside the actor runtime).
//! - Only emits candle updates when the close moves by >= 1% (epsilon gating).
//! - Uses fixed-capacity buffers (`Updates<N>`) to avoid heap allocation in the hot path.

use std::time::Duration;

use kameo::prelude::{Actor, ActorRef, Context, Message as KameoMessage};
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};
use tracing_subscriber::EnvFilter;

use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{ProtocolPingPong, WebSocketActor, WebSocketActorArgs, WebSocketEvent};
use shared_ws::ws::{
    WebSocketBufferConfig, WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction,
    WsFrame, WsIngress, WsIngressAction, WsMessageAction, WsParseOutcome,
};

use examples_ws::candles::{CandleAggregator, CandleEmitReason, CandleUpdate, Trade, Updates};
use examples_ws::deribit::reconnect::DeribitReconnect;
use examples_ws::endpoints::deribit::{DeribitChannel, DeribitSubscriptionManager};

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

#[derive(Debug, Deserialize)]
struct DeribitSubFrame<'a> {
    #[serde(borrow)]
    method: &'a str,
    params: DeribitSubParams<'a>,
}

#[derive(Debug, Deserialize)]
struct DeribitSubParams<'a> {
    #[serde(borrow)]
    channel: &'a str,
    data: Vec<DeribitTradeLite>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct DeribitTradeLite {
    #[serde(rename = "timestamp")]
    ts_ms: i64,
    #[serde(deserialize_with = "de_f64")]
    price: f64,
    #[serde(rename = "amount", deserialize_with = "de_f64")]
    qty: f64,
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
    subs: DeribitSubscriptionManager,
    sink: ActorRef<CandlePrinter>,
    dropped: u64,
}

impl WsEndpointHandler for SinkHandler {
    type Message = CandleUpdates;
    type Error = std::convert::Infallible;
    type Subscription = DeribitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        &mut self.subs
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        None
    }

    fn parse(&mut self, _data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        Ok(WsParseOutcome::Message(WsMessageAction::Continue))
    }

    fn parse_frame(
        &mut self,
        _frame: &WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        Ok(WsParseOutcome::Message(WsMessageAction::Continue))
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
        _code: Option<i32>,
        _message: &str,
        _data: Option<sonic_rs::Value>,
    ) -> WsErrorAction {
        WsErrorAction::Continue
    }

    fn reset_state(&mut self) {}

    fn classify_disconnect(&self, cause: &WsDisconnectCause) -> WsDisconnectAction {
        // Mirror `DeribitPublicHandler` default behavior.
        match cause {
            WsDisconnectCause::EndpointRequested { reason } if reason == "disconnect requested" => {
                WsDisconnectAction::Abort
            }
            _ => WsDisconnectAction::BackoffReconnect,
        }
    }
}

struct DeribitTradesToCandlesIngress {
    expected_channel: String,
    agg: CandleAggregator<1>,
    trade_seen: u64,
    trade_print_first: u64,
    trade_sample_every: u64,
}

impl DeribitTradesToCandlesIngress {
    fn new(
        expected_channel: String,
        eps_frac: f64,
        trade_print_first: u64,
        trade_sample_every: u64,
    ) -> Self {
        // 1m candles, epsilon configurable via `--eps` (fractional, e.g. 0.01 == 1%).
        Self {
            expected_channel,
            agg: CandleAggregator::<1>::new([60_000], eps_frac),
            trade_seen: 0,
            trade_print_first,
            trade_sample_every,
        }
    }
}

impl WsIngress for DeribitTradesToCandlesIngress {
    type Message = CandleUpdates;

    fn on_frame(&mut self, frame: WsFrame) -> WsIngressAction<Self::Message> {
        let payload: &[u8] = match &frame {
            WsFrame::Text(b) => b.as_bytes().as_ref(),
            WsFrame::Binary(b) => b.as_ref(),
            WsFrame::Ping(_) | WsFrame::Pong(_) | WsFrame::Close(_) => {
                return WsIngressAction::Forward(frame);
            }
        };

        let Ok(v) = sonic_rs::from_slice::<DeribitSubFrame<'_>>(payload) else {
            return WsIngressAction::Forward(frame);
        };
        if v.method != "subscription" {
            return WsIngressAction::Forward(frame);
        }
        if v.params.channel != self.expected_channel {
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

        for row in v.params.data {
            let ts_ms = row.ts_ms;
            let price = row.price;
            let qty = row.qty;

            self.trade_seen = self.trade_seen.saturating_add(1);
            if self.trade_seen <= self.trade_print_first
                || (self.trade_sample_every > 0 && (self.trade_seen % self.trade_sample_every == 0))
            {
                // Debug-only visibility into the raw trade stream.
                println!("trade ts_ms={ts_ms} price={price} qty={qty}");
            }

            let updates = self.agg.on_trade::<8>(Trade { ts_ms, price, qty });
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

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,shared_ws=info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    init_tracing();

    let args: Vec<String> = std::env::args().collect();
    if has_flag(&args, "--help") || has_flag(&args, "-h") {
        eprintln!(
            "Usage: deribit_trades_to_candles_eps1 [--instrument BTC-PERPETUAL] [--interval 100ms] [--eps 0.01] [--trade-first 10] [--trade-sample 100] [--testnet] [--url WS_URL]"
        );
        return;
    }

    let instrument =
        arg_value(&args, "--instrument").unwrap_or_else(|| "BTC-PERPETUAL".to_string());
    // Note: Deribit requires authorization for `.raw` subscriptions; default to a public interval.
    let interval = arg_value(&args, "--interval").unwrap_or_else(|| "100ms".to_string());
    let eps_frac = arg_value(&args, "--eps")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.01);
    let trade_print_first = arg_value(&args, "--trade-first")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);
    let trade_sample_every = arg_value(&args, "--trade-sample")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(100);
    let channel = format!("trades.{instrument}.{interval}");

    let url = if let Some(u) = arg_value(&args, "--url") {
        u
    } else if has_flag(&args, "--testnet") {
        "wss://test.deribit.com/ws/api/v2".to_string()
    } else {
        "wss://www.deribit.com/ws/api/v2".to_string()
    };

    println!("connecting url={url} channel={channel}");

    let candle_printer = CandlePrinter::spawn(());

    let subs = DeribitSubscriptionManager::with_initial_channels([DeribitChannel(channel.clone())]);
    let handler = SinkHandler {
        subs,
        sink: candle_printer,
        dropped: 0,
    };
    let ingress = DeribitTradesToCandlesIngress::new(
        channel,
        eps_frac,
        trade_print_first,
        trade_sample_every,
    );

    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url,
        transport: TungsteniteTransport::default(),
        reconnect_strategy: DeribitReconnect::default(),
        handler,
        ingress,
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
