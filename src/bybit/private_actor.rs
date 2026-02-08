//! Bybit private websocket actor (authenticated, shared instance).
//!
//! This mirrors the public actor wrapper but supports:
//! - sending an auth payload on connect (via an injected provider),
//! - dynamic topic subscriptions (e.g. `"order"`, `"execution"`, `"wallet"`),
//! - JSON-level ping/pong (private-mode correlation by `reqId`).

use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use kameo::prelude::{Actor, ActorRef, Context, Message as KameoMessage, WeakActorRef};
use tokio::sync::{mpsc, watch};

use shared_ws::core::{
    ForwardAllIngress, WebSocketBufferConfig, WsConnectionStats, WsConnectionStatus, WsTlsConfig,
};
use crate::endpoints::bybit::{
    BybitEvent, BybitSubOp, BybitSubscriptionManager, BybitSubscriptionRequest, BybitTopic,
    BybitTopicHandler,
};
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    GetConnectionStats, GetConnectionStatus, WebSocketActor, WebSocketActorArgs, WebSocketEvent,
    WsSubscriptionUpdate,
};

use super::ping::BybitJsonPingPong;

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

#[derive(Debug, Clone)]
pub struct SubscribeTopics {
    pub topics: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UnsubscribeTopics {
    pub topics: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SetEventSink {
    pub sink: mpsc::Sender<BybitEvent>,
}

/// Set or clear the auth payload generator.
///
/// If set, the underlying websocket actor will send the payload immediately after connecting.
#[derive(Clone)]
pub struct SetAuthProvider {
    pub provider: Option<Arc<dyn Fn() -> Vec<u8> + Send + Sync>>,
}

#[derive(Debug, Clone)]
pub struct GetStats;

#[derive(Debug, Clone)]
pub struct IsConnected;

#[derive(Debug, Clone)]
pub struct BybitPrivateStats {
    pub forwarded: u64,
    pub dropped: u64,
    pub subscribed_topics: usize,
    pub connection: WsConnectionStatus,
    pub stats: WsConnectionStats,
}

#[derive(Clone)]
struct ForwardingHandler {
    inner: BybitTopicHandler,
    tx: mpsc::Sender<BybitEvent>,
    metrics: Arc<ForwardMetrics>,
}

impl shared_ws::core::WsEndpointHandler for ForwardingHandler {
    type Message = BybitEvent;
    type Error = crate::endpoints::bybit::BybitProtocolError;
    type Subscription = BybitSubscriptionManager;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        self.inner.subscription_manager()
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        self.inner.generate_auth()
    }

    fn parse(
        &mut self,
        data: &[u8],
    ) -> Result<shared_ws::core::WsParseOutcome<Self::Message>, Self::Error> {
        self.inner.parse(data)
    }

    fn parse_frame(
        &mut self,
        frame: &shared_ws::core::WsFrame,
    ) -> Result<shared_ws::core::WsParseOutcome<Self::Message>, Self::Error> {
        self.inner.parse_frame(frame)
    }

    fn handle_message(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match self.tx.try_send(msg) {
            Ok(()) => {}
            Err(_) => self.metrics.inc_dropped(),
        }
        Ok(())
    }

    fn handle_server_error(
        &mut self,
        code: Option<i32>,
        message: &str,
        data: Option<sonic_rs::Value>,
    ) -> shared_ws::core::WsErrorAction {
        self.inner.handle_server_error(code, message, data)
    }

    fn reset_state(&mut self) {
        self.inner.reset_state()
    }

    fn classify_disconnect(
        &self,
        cause: &shared_ws::core::WsDisconnectCause,
    ) -> shared_ws::core::WsDisconnectAction {
        self.inner.classify_disconnect(cause)
    }

    fn on_open(&mut self) {
        self.inner.on_open()
    }
}

#[derive(Clone)]
pub struct BybitPrivateActorArgs {
    pub url: String,
    pub tls: WsTlsConfig,
    pub initial_topics: Vec<String>,
    pub stale_threshold: Duration,
    pub ws_buffers: WebSocketBufferConfig,
    pub outbound_capacity: usize,
    pub event_channel_capacity: usize,
    pub buffer_capacity: usize,
    pub enable_ping: bool,
    pub ping_interval: Duration,
    pub ping_timeout: Duration,
    pub auth_provider: Option<Arc<dyn Fn() -> Vec<u8> + Send + Sync>>,
}

impl BybitPrivateActorArgs {
    pub fn test_defaults(url: String) -> Self {
        Self {
            url,
            tls: WsTlsConfig::default(),
            initial_topics: vec!["order".to_string(), "execution".to_string()],
            stale_threshold: Duration::from_secs(30),
            ws_buffers: WebSocketBufferConfig::default(),
            outbound_capacity: 128,
            event_channel_capacity: 1024,
            buffer_capacity: 10_000,
            enable_ping: true,
            ping_interval: Duration::from_secs(20),
            ping_timeout: Duration::from_secs(30),
            auth_provider: None,
        }
    }
}

pub struct BybitPrivateActor {
    args: BybitPrivateActorArgs,
    ws: Option<
        ActorRef<
            WebSocketActor<
                ForwardingHandler,
                shared_ws::core::ExponentialBackoffReconnect,
                BybitJsonPingPong,
                ForwardAllIngress<BybitEvent>,
                TungsteniteTransport,
            >,
        >,
    >,
    metrics: Arc<ForwardMetrics>,
    sink_tx: watch::Sender<Option<mpsc::Sender<BybitEvent>>>,
    forward_task: Option<tokio::task::JoinHandle<()>>,
    subscribed: HashSet<String>,
}

impl BybitPrivateActor {
    pub fn new(args: BybitPrivateActorArgs) -> Self {
        let (sink_tx, _sink_rx) = watch::channel(None);
        Self {
            subscribed: args.initial_topics.iter().cloned().collect(),
            args,
            ws: None,
            metrics: Arc::new(ForwardMetrics::default()),
            sink_tx,
            forward_task: None,
        }
    }

    fn spawn_forward_loop(
        &mut self,
        mut rx: mpsc::Receiver<BybitEvent>,
        mut sink_rx: watch::Receiver<Option<mpsc::Sender<BybitEvent>>>,
    ) {
        let metrics = self.metrics.clone();
        let buffer_capacity = self.args.buffer_capacity;
        let mut buffer: VecDeque<BybitEvent> = VecDeque::with_capacity(buffer_capacity.min(1024));

        self.forward_task = Some(tokio::spawn(async move {
            let mut sink = sink_rx.borrow().clone();
            loop {
                tokio::select! {
                    biased;
                    changed = sink_rx.changed() => {
                        if changed.is_err() { break; }
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
                }
            }
        }));
    }
}

impl Actor for BybitPrivateActor {
    type Args = BybitPrivateActorArgs;
    type Error = crate::endpoints::bybit::BybitProtocolError;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let mut this = Self::new(args);

        let (tx, rx) = mpsc::channel(this.args.event_channel_capacity.max(1));
        let metrics = this.metrics.clone();

        let subs = BybitSubscriptionManager::with_initial_topics(
            this.args
                .initial_topics
                .iter()
                .map(|t| BybitTopic(t.clone())),
        );
        let handler = BybitTopicHandler::new(subs);
        let handler = if let Some(provider) = this.args.auth_provider.clone() {
            handler.with_auth_provider(provider)
        } else {
            handler
        };

        let handler = ForwardingHandler {
            inner: handler,
            tx,
            metrics,
        };

        let ping = BybitJsonPingPong::private(this.args.ping_interval, this.args.ping_timeout);
        let ws = WebSocketActor::spawn(WebSocketActorArgs {
            url: this.args.url.clone(),
            tls: this.args.tls,
            transport: TungsteniteTransport::default(),
            reconnect_strategy: shared_ws::core::ExponentialBackoffReconnect::default(),
            handler,
            ingress: ForwardAllIngress::default(),
            ping_strategy: ping,
            enable_ping: this.args.enable_ping,
            stale_threshold: this.args.stale_threshold,
            ws_buffers: this.args.ws_buffers,
            outbound_capacity: this.args.outbound_capacity,
            global_rate_limit: None,
            rate_limiter: None,
            circuit_breaker: None,
            latency_policy: None,
            payload_latency_sampling: None,
            registration: None,
            metrics: None,
        });

        this.ws = Some(ws.clone());
        let sink_rx = this.sink_tx.subscribe();
        this.spawn_forward_loop(rx, sink_rx);

        // Auto-connect on start (private singleton behavior, auth is enqueued via handler).
        let _ = ws.tell(WebSocketEvent::Connect).send().await;

        Ok(this)
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: kameo::error::ActorStopReason,
    ) -> Result<(), Self::Error> {
        if let Some(handle) = self.forward_task.take() {
            handle.abort();
        }
        if let Some(ws) = self.ws.take() {
            let _ = ws.stop_gracefully().await;
        }
        Ok(())
    }
}

impl KameoMessage<SetEventSink> for BybitPrivateActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SetEventSink,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let _ = self.sink_tx.send(Some(msg.sink));
    }
}

impl KameoMessage<SetAuthProvider> for BybitPrivateActor {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        _msg: SetAuthProvider,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Note: the auth provider is captured inside the handler at spawn time; changing it at
        // runtime would require a dedicated handler message. Keep this API placeholder so callers
        // have a stable surface; for now return an explicit error.
        Err("auth provider cannot be updated after start (recreate actor)".to_string())
    }
}

impl KameoMessage<SubscribeTopics> for BybitPrivateActor {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: SubscribeTopics,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(ws) = self.ws.as_ref() else {
            return Err("ws not started".to_string());
        };

        let mut topics = Vec::new();
        for t in msg.topics {
            if self.subscribed.insert(t.clone()) {
                topics.push(BybitTopic(t));
            }
        }
        if topics.is_empty() {
            return Ok(());
        }

        let req = BybitSubscriptionRequest {
            op: BybitSubOp::Subscribe,
            topics,
        };
        ws.ask(WsSubscriptionUpdate {
            action: shared_ws::core::WsSubscriptionAction::Add(vec![req]),
        })
        .await
        .map_err(|e| e.to_string())?;

        Ok(())
    }
}

impl KameoMessage<UnsubscribeTopics> for BybitPrivateActor {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: UnsubscribeTopics,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(ws) = self.ws.as_ref() else {
            return Err("ws not started".to_string());
        };

        let mut topics = Vec::new();
        for t in msg.topics {
            if self.subscribed.remove(&t) {
                topics.push(BybitTopic(t));
            }
        }
        if topics.is_empty() {
            return Ok(());
        }

        let req = BybitSubscriptionRequest {
            op: BybitSubOp::Unsubscribe,
            topics,
        };
        ws.ask(WsSubscriptionUpdate {
            action: shared_ws::core::WsSubscriptionAction::Remove(vec![req]),
        })
        .await
        .map_err(|e| e.to_string())?;

        Ok(())
    }
}

impl KameoMessage<GetStats> for BybitPrivateActor {
    type Reply = Result<BybitPrivateStats, String>;

    async fn handle(
        &mut self,
        _msg: GetStats,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(ws) = self.ws.as_ref() else {
            return Err("ws not started".to_string());
        };

        let stats = ws.ask(GetConnectionStats).await.map_err(|e| e.to_string())?;
        let status = ws.ask(GetConnectionStatus).await.map_err(|e| e.to_string())?;

        Ok(BybitPrivateStats {
            forwarded: self.metrics.forwarded(),
            dropped: self.metrics.dropped(),
            subscribed_topics: self.subscribed.len(),
            connection: status,
            stats,
        })
    }
}

impl KameoMessage<IsConnected> for BybitPrivateActor {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        _msg: IsConnected,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(ws) = self.ws.as_ref() else {
            return Ok(false);
        };
        let status = ws.ask(GetConnectionStatus).await.map_err(|e| e.to_string())?;
        Ok(matches!(status, WsConnectionStatus::Connected))
    }
}

