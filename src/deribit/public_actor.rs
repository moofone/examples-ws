//! Deribit public websocket actor (shared instance).
//!
//! This is a thin orchestrator around `WebSocketActor` that provides:
//! - ergonomic subscribe messages (ticker / orderbook),
//! - bounded event forwarding with buffering + drop/forward counters,
//! - cached available expiries via an injected instrument provider.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use kameo::prelude::{Actor, ActorRef, Context, Message as KameoMessage, WeakActorRef};
use tokio::sync::{Notify, mpsc, watch};

use crate::endpoints::deribit::{
    DeribitChannel, DeribitEvent, DeribitProtocolError, DeribitPublicHandler, DeribitSubOp,
    DeribitSubscriptionManager, DeribitSubscriptionRequest,
};
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    ForwardAllIngress, WebSocketBufferConfig, WsConnectionStats, WsConnectionStatus,
    WsDisconnectAction, WsDisconnectCause, WsEndpointHandler, WsErrorAction, WsParseOutcome,
    WsSubscriptionAction,
};
use shared_ws::ws::{
    GetConnectionStats, GetConnectionStatus, ProtocolPingPong, WebSocketActor, WebSocketActorArgs,
    WebSocketEvent, WsSubscriptionUpdate,
};

use super::date::{UtcDate, utc_date_from_unix_ms, utc_today};
use super::instruments::{DeribitInstrument, DeribitInstrumentsProvider};
use super::reconnect::DeribitReconnect;

#[derive(Debug, Default)]
struct ForwardMetrics {
    forwarded: AtomicU64,
    dropped: AtomicU64,
}

impl ForwardMetrics {
    fn inc_forwarded(&self) {
        self.forwarded.fetch_add(1, Ordering::Relaxed);
    }
    fn inc_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
    fn forwarded(&self) -> u64 {
        self.forwarded.load(Ordering::Relaxed)
    }
    fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

fn ticker_channel(instrument: &str, interval: &str) -> DeribitChannel {
    DeribitChannel(format!("ticker.{instrument}.{interval}"))
}

fn orderbook_channel(instrument: &str, depth: u8, interval: &str) -> DeribitChannel {
    DeribitChannel(format!("book.{instrument}.{depth}.{interval}"))
}

fn trades_channel(instrument: &str, interval: &str) -> DeribitChannel {
    DeribitChannel(format!("trades.{instrument}.{interval}"))
}

/// Message: subscribe to an instrument's orderbook (dynamic).
#[derive(Debug, Clone)]
pub struct SubscribeOrderBook {
    pub instrument: String,
    pub depth: u8,
}

/// Message: subscribe to an instrument's ticker (dynamic).
#[derive(Debug, Clone)]
pub struct SubscribeTicker {
    pub instrument: String,
}

/// Message: subscribe to an instrument's trade stream (dynamic).
#[derive(Debug, Clone)]
pub struct SubscribeTrades {
    pub instrument: String,
}

#[derive(Debug, Clone)]
pub struct SetEventSink {
    /// Where to forward `DeribitEvent`s after internal metrics/bookkeeping.
    ///
    /// If the sink is full, events are buffered up to `buffer_capacity` and then dropped.
    pub sink: mpsc::Sender<DeribitEvent>,
}

#[derive(Debug, Clone)]
pub struct GetStats;

#[derive(Debug, Clone)]
pub struct GetConnection;

#[derive(Debug, Clone)]
pub struct GetAvailableExpiries {
    pub currency: String,
}

#[derive(Debug, Clone, kameo::Reply)]
pub struct GetAvailableExpiriesResponse {
    pub expiries: Vec<UtcDate>,
}

#[derive(Debug, Clone)]
pub struct DeribitPublicStats {
    pub forwarded: u64,
    pub dropped: u64,
    pub subscribed_instruments: usize,
    pub connection: WsConnectionStatus,
    pub stats: WsConnectionStats,
}

#[derive(Debug)]
struct CacheExpiriesUpdate {
    currency: String,
    expiries: Vec<UtcDate>,
}

#[derive(Clone)]
struct ForwardingHandler {
    inner: DeribitPublicHandler,
    tx: mpsc::Sender<DeribitEvent>,
    metrics: Arc<ForwardMetrics>,
    on_open: Arc<Notify>,
}

impl WsEndpointHandler for ForwardingHandler {
    type Message = DeribitEvent;
    type Error = DeribitProtocolError;
    type Subscription = DeribitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        self.inner.subscription_manager()
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        self.inner.generate_auth()
    }

    fn parse(&mut self, data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        self.inner.parse(data)
    }

    fn parse_frame(
        &mut self,
        frame: &shared_ws::ws::WsFrame,
    ) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        self.inner.parse_frame(frame)
    }

    fn handle_message(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match self.tx.try_send(msg) {
            Ok(()) => {}
            // Internal backpressure: we could not enqueue for buffering/forwarding.
            Err(_) => self.metrics.inc_dropped(),
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
        self.on_open.notify_waiters();
        self.inner.on_open()
    }
}

/// Arguments for constructing the Deribit public actor.
#[derive(Clone)]
pub struct DeribitPublicActorArgs {
    pub url: String,
    pub transport: TungsteniteTransport,
    pub currencies: Vec<String>,
    pub ticker_interval: String,
    pub stale_threshold: Duration,
    pub enable_ping: bool,
    pub ping_interval: Duration,
    pub ping_timeout: Duration,
    pub ws_buffers: WebSocketBufferConfig,
    pub outbound_capacity: usize,
    pub event_channel_capacity: usize,
    pub buffer_capacity: usize,
    pub skip_perp_ticker: bool,
    pub instruments_provider: Option<Arc<dyn DeribitInstrumentsProvider>>,
    pub reconnect: DeribitReconnect,
}

impl DeribitPublicActorArgs {
    pub fn test_defaults(url: String) -> Self {
        Self {
            url,
            transport: TungsteniteTransport::default(),
            currencies: vec!["BTC".to_string(), "ETH".to_string()],
            ticker_interval: "raw".to_string(),
            stale_threshold: Duration::from_secs(30),
            enable_ping: false,
            ping_interval: Duration::from_secs(60),
            ping_timeout: Duration::from_secs(60),
            ws_buffers: WebSocketBufferConfig::default(),
            outbound_capacity: 128,
            event_channel_capacity: 1024,
            buffer_capacity: 10_000,
            skip_perp_ticker: false,
            instruments_provider: None,
            reconnect: DeribitReconnect::default(),
        }
    }
}

pub struct DeribitPublicActor {
    args: DeribitPublicActorArgs,
    ws: Option<
        ActorRef<
            WebSocketActor<
                ForwardingHandler,
                DeribitReconnect,
                ProtocolPingPong,
                ForwardAllIngress<DeribitEvent>,
                TungsteniteTransport,
            >,
        >,
    >,
    on_open: Arc<Notify>,
    metrics: Arc<ForwardMetrics>,
    sink_tx: watch::Sender<Option<mpsc::Sender<DeribitEvent>>>,
    subscribed_orderbooks: HashSet<String>,
    subscribed_tickers: HashSet<String>,
    subscribed_trades: HashSet<String>,
    cached_expiries: HashMap<String, Vec<UtcDate>>,
    expiry_task: Option<tokio::task::JoinHandle<()>>,
    forward_task: Option<tokio::task::JoinHandle<()>>,
}

impl DeribitPublicActor {
    pub fn new(args: DeribitPublicActorArgs) -> Self {
        let (sink_tx, _sink_rx) = watch::channel(None);
        Self {
            args,
            ws: None,
            on_open: Arc::new(Notify::new()),
            metrics: Arc::new(ForwardMetrics::default()),
            sink_tx,
            subscribed_orderbooks: HashSet::new(),
            subscribed_tickers: HashSet::new(),
            subscribed_trades: HashSet::new(),
            cached_expiries: HashMap::new(),
            expiry_task: None,
            forward_task: None,
        }
    }

    fn spawn_expiry_refresh_loop(&mut self, self_ref: ActorRef<Self>) {
        let Some(provider) = self.args.instruments_provider.clone() else {
            return;
        };

        let on_open = self.on_open.clone();
        let currencies = self.args.currencies.clone();

        self.expiry_task = Some(tokio::spawn(async move {
            // Refresh once on startup, then every (re)connect.
            loop {
                // Initial run happens immediately.
                for c in &currencies {
                    refresh_currency_expiries(self_ref.clone(), provider.clone(), c.clone()).await;
                }

                // Then wait for next connect signal.
                on_open.notified().await;
            }
        }));
    }

    fn spawn_forward_loop(
        &mut self,
        mut rx: mpsc::Receiver<DeribitEvent>,
        mut sink_rx: watch::Receiver<Option<mpsc::Sender<DeribitEvent>>>,
    ) {
        let metrics = self.metrics.clone();
        let buffer_capacity = self.args.buffer_capacity;
        let mut buffer: VecDeque<DeribitEvent> = VecDeque::with_capacity(buffer_capacity.min(1024));

        self.forward_task = Some(tokio::spawn(async move {
            let mut sink = sink_rx.borrow().clone();
            loop {
                tokio::select! {
                    biased;

                    changed = sink_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        sink = sink_rx.borrow().clone();
                        if let Some(ref tx) = sink {
                            for _ in 0..256 {
                                let Some(mut front) = buffer.pop_front() else { break; };
                                match tx.try_send(front) {
                                    Ok(()) => metrics.inc_forwarded(),
                                    Err(err) => {
                                        front = err.into_inner();
                                        buffer.push_front(front);
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    evt = rx.recv() => {
                        let Some(mut evt) = evt else { break; };

                        // If a sink exists, attempt immediate forward; on backpressure buffer.
                        if let Some(ref tx) = sink {
                            match tx.try_send(evt) {
                                Ok(()) => {
                                    metrics.inc_forwarded();
                                    continue;
                                }
                                Err(err) => evt = err.into_inner(),
                            }
                        }

                        if buffer.len() < buffer_capacity {
                            buffer.push_back(evt);
                        } else {
                            metrics.inc_dropped();
                        }

                        // If we have a sink, drain a little on each inbound to amortize.
                        if let Some(ref tx) = sink {
                            for _ in 0..64 {
                                let Some(mut front) = buffer.pop_front() else { break; };
                                match tx.try_send(front) {
                                    Ok(()) => {
                                        metrics.inc_forwarded();
                                    }
                                    Err(err) => {
                                    // Put it back and stop.
                                    front = err.into_inner();
                                    buffer.push_front(front);
                                    break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }));
    }
}

async fn refresh_currency_expiries(
    actor: ActorRef<DeribitPublicActor>,
    provider: Arc<dyn DeribitInstrumentsProvider>,
    currency: String,
) {
    let today = utc_today();
    let instruments: Vec<DeribitInstrument> = match provider.get_instruments(&currency).await {
        Ok(v) => v,
        Err(_) => return,
    };

    let mut expiries = std::collections::HashSet::new();
    for inst in instruments {
        if !inst.is_active || !inst.is_option() {
            continue;
        }
        let Some(ts) = inst.expiration_timestamp_ms else {
            continue;
        };
        let date = utc_date_from_unix_ms(ts);
        if let Some(today) = today {
            if date <= today {
                continue;
            }
        }
        expiries.insert(date);
    }

    let mut sorted: Vec<_> = expiries.into_iter().collect();
    sorted.sort();

    let _ = actor
        .tell(CacheExpiriesUpdate {
            currency,
            expiries: sorted,
        })
        .send()
        .await;
}

impl Actor for DeribitPublicActor {
    type Args = DeribitPublicActorArgs;
    type Error = DeribitProtocolError;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let mut this = Self::new(args);

        let (tx, rx) = mpsc::channel(this.args.event_channel_capacity.max(1));
        let metrics = this.metrics.clone();
        let on_open = this.on_open.clone();

        let subs = if this.args.skip_perp_ticker {
            DeribitSubscriptionManager::new()
        } else {
            let mut initial = Vec::new();
            for c in &this.args.currencies {
                let perp = format!("{c}-PERPETUAL");
                initial.push(ticker_channel(&perp, &this.args.ticker_interval));
            }
            DeribitSubscriptionManager::with_initial_channels(initial)
        };

        let handler = ForwardingHandler {
            inner: DeribitPublicHandler::new(subs),
            tx,
            metrics,
            on_open,
        };

        let ping = ProtocolPingPong::new(this.args.ping_interval, this.args.ping_timeout);
        let ws = WebSocketActor::spawn(WebSocketActorArgs {
            url: this.args.url.clone(),
            transport: this.args.transport.clone(),
            reconnect_strategy: this.args.reconnect.clone(),
            handler,
            ingress: ForwardAllIngress::default(),
            ping_strategy: ping,
            enable_ping: this.args.enable_ping,
            stale_threshold: this.args.stale_threshold,
            ws_buffers: this.args.ws_buffers,
            outbound_capacity: this.args.outbound_capacity,
            circuit_breaker: None,
            latency_policy: None,
            payload_latency_sampling: None,
            registration: None,
            metrics: None,
        });

        // Track initial subscribed tickers in our stats surface.
        if !this.args.skip_perp_ticker {
            for c in &this.args.currencies {
                this.subscribed_tickers.insert(format!("{c}-PERPETUAL"));
            }
        }

        this.ws = Some(ws.clone());

        // Start connect immediately (legacy behavior for public actor).
        let _ = ws.tell(WebSocketEvent::Connect).send().await;

        // Background loops.
        this.spawn_expiry_refresh_loop(actor_ref.clone());
        let sink_rx = this.sink_tx.subscribe();
        this.spawn_forward_loop(rx, sink_rx);

        Ok(this)
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: kameo::error::ActorStopReason,
    ) -> Result<(), Self::Error> {
        if let Some(handle) = self.expiry_task.take() {
            handle.abort();
        }
        if let Some(handle) = self.forward_task.take() {
            handle.abort();
        }
        if let Some(ws) = self.ws.take() {
            let _ = ws.stop_gracefully().await;
        }
        Ok(())
    }
}

impl KameoMessage<SetEventSink> for DeribitPublicActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SetEventSink,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let _ = self.sink_tx.send(Some(msg.sink));
    }
}

impl KameoMessage<SubscribeOrderBook> for DeribitPublicActor {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: SubscribeOrderBook,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.subscribed_orderbooks.contains(&msg.instrument) {
            return Ok(());
        }
        let Some(ws) = self.ws.as_ref() else {
            return Err("ws not started".to_string());
        };

        let ch = orderbook_channel(&msg.instrument, msg.depth, &self.args.ticker_interval);
        let req = DeribitSubscriptionRequest {
            op: DeribitSubOp::Subscribe,
            channels: vec![ch],
        };
        ws.ask(WsSubscriptionUpdate {
            action: WsSubscriptionAction::Add(vec![req]),
        })
        .await
        .map_err(|e| e.to_string())?;

        self.subscribed_orderbooks.insert(msg.instrument);
        Ok(())
    }
}

impl KameoMessage<SubscribeTicker> for DeribitPublicActor {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: SubscribeTicker,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.subscribed_tickers.contains(&msg.instrument) {
            return Ok(());
        }
        let Some(ws) = self.ws.as_ref() else {
            return Err("ws not started".to_string());
        };

        let ch = ticker_channel(&msg.instrument, &self.args.ticker_interval);
        let req = DeribitSubscriptionRequest {
            op: DeribitSubOp::Subscribe,
            channels: vec![ch],
        };
        ws.ask(WsSubscriptionUpdate {
            action: WsSubscriptionAction::Add(vec![req]),
        })
        .await
        .map_err(|e| e.to_string())?;

        self.subscribed_tickers.insert(msg.instrument);
        Ok(())
    }
}

impl KameoMessage<SubscribeTrades> for DeribitPublicActor {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: SubscribeTrades,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.subscribed_trades.contains(&msg.instrument) {
            return Ok(());
        }
        let Some(ws) = self.ws.as_ref() else {
            return Err("ws not started".to_string());
        };

        let ch = trades_channel(&msg.instrument, &self.args.ticker_interval);
        let req = DeribitSubscriptionRequest {
            op: DeribitSubOp::Subscribe,
            channels: vec![ch],
        };
        ws.ask(WsSubscriptionUpdate {
            action: WsSubscriptionAction::Add(vec![req]),
        })
        .await
        .map_err(|e| e.to_string())?;

        self.subscribed_trades.insert(msg.instrument);
        Ok(())
    }
}

impl KameoMessage<GetStats> for DeribitPublicActor {
    type Reply = Result<DeribitPublicStats, String>;

    async fn handle(
        &mut self,
        _msg: GetStats,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(ws) = self.ws.as_ref() else {
            return Err("ws not started".to_string());
        };

        let stats = ws
            .ask(GetConnectionStats)
            .await
            .map_err(|e| e.to_string())?;
        let status = ws
            .ask(GetConnectionStatus)
            .await
            .map_err(|e| e.to_string())?;

        Ok(DeribitPublicStats {
            forwarded: self.metrics.forwarded(),
            dropped: self.metrics.dropped(),
            subscribed_instruments: self.subscribed_orderbooks.len()
                + self.subscribed_tickers.len()
                + self.subscribed_trades.len(),
            connection: status,
            stats,
        })
    }
}

impl KameoMessage<GetConnection> for DeribitPublicActor {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        _msg: GetConnection,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(ws) = self.ws.as_ref() else {
            return Ok(false);
        };
        let status = ws
            .ask(GetConnectionStatus)
            .await
            .map_err(|e| e.to_string())?;
        Ok(matches!(status, WsConnectionStatus::Connected))
    }
}

impl KameoMessage<GetAvailableExpiries> for DeribitPublicActor {
    type Reply = GetAvailableExpiriesResponse;

    async fn handle(
        &mut self,
        msg: GetAvailableExpiries,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let expiries = self
            .cached_expiries
            .get(&msg.currency)
            .cloned()
            .unwrap_or_default();
        GetAvailableExpiriesResponse { expiries }
    }
}

impl KameoMessage<CacheExpiriesUpdate> for DeribitPublicActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: CacheExpiriesUpdate,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.cached_expiries.insert(msg.currency, msg.expiries);
    }
}
