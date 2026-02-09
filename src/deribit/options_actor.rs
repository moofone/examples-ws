//! Deribit options websocket actor (per-process instance).
//!
//! This actor is the migration target for the legacy Deribit "options" websocket actor:
//! - explicit `Connect` message,
//! - `SubscribeCurrencies` triggers instrument discovery (via injected provider) and subscribes to:
//!   - all active option ticker channels,
//!   - the perpetual ticker channel per currency,
//!   - the volatility index channel per currency (best-effort).
//! - bounded event forwarding with buffering + drop counters (like legacy ticker buffering).

use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use kameo::prelude::{Actor, ActorRef, Context, Message as KameoMessage, WeakActorRef};
use tokio::sync::{mpsc, watch};

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

use super::instruments::{DeribitInstrument, DeribitInstrumentsProvider};
use super::reconnect::DeribitReconnect;

#[derive(Debug, Default)]
struct ForwardMetrics {
    forwarded: AtomicU64,
    dropped: AtomicU64,
    subscriptions_processed: AtomicU64,
}

impl ForwardMetrics {
    fn inc_forwarded(&self) {
        self.forwarded.fetch_add(1, Ordering::Relaxed);
    }
    fn inc_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
    fn inc_subscriptions_processed(&self) {
        self.subscriptions_processed.fetch_add(1, Ordering::Relaxed);
    }
    fn forwarded(&self) -> u64 {
        self.forwarded.load(Ordering::Relaxed)
    }
    fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    fn subscriptions_processed(&self) -> u64 {
        self.subscriptions_processed.load(Ordering::Relaxed)
    }
}

fn ticker_channel(instrument: &str, interval: &str) -> DeribitChannel {
    DeribitChannel(format!("ticker.{instrument}.{interval}"))
}

fn dvol_channel(currency: &str) -> DeribitChannel {
    // Legacy Deribit code parsed `index_name` like "btc_usd". The Deribit channel name for the
    // volatility index is `deribit_volatility_index.<index_name>`.
    DeribitChannel(format!(
        "deribit_volatility_index.{}_usd",
        currency.to_lowercase()
    ))
}

#[derive(Debug, Clone)]
pub struct Connect;

#[derive(Debug, Clone)]
pub struct Disconnect;

#[derive(Debug, Clone)]
pub struct SubscribeCurrencies {
    pub currencies: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SetEventSink {
    pub sink: mpsc::Sender<DeribitEvent>,
}

#[derive(Debug, Clone)]
pub struct GetStats;

#[derive(Debug, Clone)]
pub struct IsConnected;

#[derive(Debug, Clone)]
pub struct DeribitOptionsStats {
    pub forwarded: u64,
    pub dropped: u64,
    pub subscriptions_processed: u64,
    pub connection: WsConnectionStatus,
    pub stats: WsConnectionStats,
}

#[derive(Clone)]
struct ForwardingHandler {
    inner: DeribitPublicHandler,
    tx: mpsc::Sender<DeribitEvent>,
    metrics: Arc<ForwardMetrics>,
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
}

#[derive(Clone)]
pub struct DeribitOptionsActorArgs {
    pub url: String,
    pub transport: TungsteniteTransport,
    pub ticker_interval: String,
    pub stale_threshold: Duration,
    pub enable_ping: bool,
    pub ping_interval: Duration,
    pub ping_timeout: Duration,
    pub ws_buffers: WebSocketBufferConfig,
    pub outbound_capacity: usize,
    pub event_channel_capacity: usize,
    pub buffer_capacity: usize,
    pub instruments_provider: Arc<dyn DeribitInstrumentsProvider>,
    pub reconnect: DeribitReconnect,
}

impl DeribitOptionsActorArgs {
    pub fn test_defaults(
        url: String,
        instruments_provider: Arc<dyn DeribitInstrumentsProvider>,
    ) -> Self {
        Self {
            url,
            transport: TungsteniteTransport::default(),
            ticker_interval: "raw".to_string(),
            stale_threshold: Duration::from_secs(30),
            enable_ping: false,
            ping_interval: Duration::from_secs(60),
            ping_timeout: Duration::from_secs(60),
            ws_buffers: WebSocketBufferConfig::default(),
            outbound_capacity: 256,
            event_channel_capacity: 4096,
            buffer_capacity: 50_000,
            instruments_provider,
            reconnect: DeribitReconnect::default(),
        }
    }
}

pub struct DeribitOptionsActor {
    args: DeribitOptionsActorArgs,
    ws: ActorRef<
        WebSocketActor<
            ForwardingHandler,
            DeribitReconnect,
            ProtocolPingPong,
            ForwardAllIngress<DeribitEvent>,
            TungsteniteTransport,
        >,
    >,
    metrics: Arc<ForwardMetrics>,
    sink_tx: watch::Sender<Option<mpsc::Sender<DeribitEvent>>>,
    forward_task: Option<tokio::task::JoinHandle<()>>,
}

impl DeribitOptionsActor {
    pub fn new(args: DeribitOptionsActorArgs) -> Self {
        let (sink_tx, _sink_rx) = watch::channel(None);
        let metrics = Arc::new(ForwardMetrics::default());
        let (tx, rx) = mpsc::channel(args.event_channel_capacity.max(1));

        let handler = ForwardingHandler {
            inner: DeribitPublicHandler::new(DeribitSubscriptionManager::new()),
            tx,
            metrics: metrics.clone(),
        };

        let ping = ProtocolPingPong::new(args.ping_interval, args.ping_timeout);
        let ws = WebSocketActor::spawn(WebSocketActorArgs {
            url: args.url.clone(),
            transport: args.transport.clone(),
            reconnect_strategy: args.reconnect.clone(),
            handler,
            ingress: ForwardAllIngress::default(),
            ping_strategy: ping,
            enable_ping: args.enable_ping,
            stale_threshold: args.stale_threshold,
            ws_buffers: args.ws_buffers,
            outbound_capacity: args.outbound_capacity,
            circuit_breaker: None,
            latency_policy: None,
            payload_latency_sampling: None,
            registration: None,
            metrics: None,
        });

        let mut this = Self {
            args,
            ws,
            metrics,
            sink_tx,
            forward_task: None,
        };
        let sink_rx = this.sink_tx.subscribe();
        this.spawn_forward_loop(rx, sink_rx);
        this
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

                        if let Some(ref tx) = sink {
                            for _ in 0..64 {
                                let Some(mut front) = buffer.pop_front() else { break; };
                                match tx.try_send(front) {
                                    Ok(()) => {
                                        metrics.inc_forwarded();
                                    }
                                    Err(err) => {
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

    async fn subscribe_currency(&mut self, currency: &str) -> Result<(), String> {
        let instruments: Vec<DeribitInstrument> = self
            .args
            .instruments_provider
            .get_instruments(currency)
            .await
            .map_err(|e| format!("get_instruments failed: {e}"))?;

        let mut channels = Vec::new();

        for inst in instruments {
            if !inst.is_active || !inst.is_option() {
                continue;
            }
            channels.push(ticker_channel(
                &inst.instrument_name,
                &self.args.ticker_interval,
            ));
        }

        // Always include perpetual ticker (funding rate) for this currency.
        let perp = format!("{currency}-PERPETUAL");
        channels.push(ticker_channel(&perp, &self.args.ticker_interval));

        // Best-effort: DVOL subscription per currency.
        channels.push(dvol_channel(currency));

        if channels.is_empty() {
            return Ok(());
        }

        let req = DeribitSubscriptionRequest {
            op: DeribitSubOp::Subscribe,
            channels,
        };

        self.ws
            .ask(WsSubscriptionUpdate {
                action: WsSubscriptionAction::Add(vec![req]),
            })
            .await
            .map_err(|e| e.to_string())?;

        self.metrics.inc_subscriptions_processed();
        Ok(())
    }
}

impl Actor for DeribitOptionsActor {
    type Args = DeribitOptionsActorArgs;
    type Error = DeribitProtocolError;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self::new(args))
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: kameo::error::ActorStopReason,
    ) -> Result<(), Self::Error> {
        if let Some(handle) = self.forward_task.take() {
            handle.abort();
        }
        let _ = self.ws.stop_gracefully().await;
        Ok(())
    }
}

impl KameoMessage<Connect> for DeribitOptionsActor {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        _msg: Connect,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.ws
            .tell(WebSocketEvent::Connect)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl KameoMessage<Disconnect> for DeribitOptionsActor {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        _msg: Disconnect,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.ws
            .tell(WebSocketEvent::Disconnect {
                reason: "disconnect requested".to_string(),
                cause: WsDisconnectCause::EndpointRequested {
                    reason: "disconnect requested".to_string(),
                },
            })
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl KameoMessage<SetEventSink> for DeribitOptionsActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SetEventSink,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let _ = self.sink_tx.send(Some(msg.sink));
    }
}

impl KameoMessage<SubscribeCurrencies> for DeribitOptionsActor {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: SubscribeCurrencies,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        for currency in msg.currencies {
            self.subscribe_currency(&currency).await?;
        }
        Ok(())
    }
}

impl KameoMessage<GetStats> for DeribitOptionsActor {
    type Reply = Result<DeribitOptionsStats, String>;

    async fn handle(
        &mut self,
        _msg: GetStats,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let stats = self
            .ws
            .ask(GetConnectionStats)
            .await
            .map_err(|e| e.to_string())?;
        let status = self
            .ws
            .ask(GetConnectionStatus)
            .await
            .map_err(|e| e.to_string())?;
        Ok(DeribitOptionsStats {
            forwarded: self.metrics.forwarded(),
            dropped: self.metrics.dropped(),
            subscriptions_processed: self.metrics.subscriptions_processed(),
            connection: status,
            stats,
        })
    }
}

impl KameoMessage<IsConnected> for DeribitOptionsActor {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        _msg: IsConnected,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let status = self
            .ws
            .ask(GetConnectionStatus)
            .await
            .map_err(|e| e.to_string())?;
        Ok(matches!(status, WsConnectionStatus::Connected))
    }
}
