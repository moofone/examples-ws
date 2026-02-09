use std::time::Duration;

use kameo::Actor;
use shared_ws::transport::tungstenite::TungsteniteTransport;
use shared_ws::ws::{
    GetConnectionStats, ProtocolPingPong, WebSocketActor, WebSocketActorArgs,
    WebSocketBufferConfig, WebSocketEvent, WsDisconnectAction, WsDisconnectCause,
    WsEndpointHandler, WsErrorAction, WsMessageAction, WsParseOutcome, WsReconnectStrategy,
    WsSubscriptionAction, WsSubscriptionManager, WsSubscriptionStatus, WsTlsConfig,
};

#[derive(Clone, Debug)]
struct NoopSubscriptions;

impl WsSubscriptionManager for NoopSubscriptions {
    type SubscriptionMessage = Vec<u8>;

    fn initial_subscriptions(&mut self) -> Vec<Self::SubscriptionMessage> {
        Vec::new()
    }

    fn update_subscriptions(
        &mut self,
        _action: WsSubscriptionAction<Self::SubscriptionMessage>,
    ) -> Result<Vec<Self::SubscriptionMessage>, String> {
        Ok(Vec::new())
    }

    fn serialize_subscription(&mut self, msg: &Self::SubscriptionMessage) -> Vec<u8> {
        msg.clone()
    }

    fn handle_subscription_response(&mut self, _data: &[u8]) -> WsSubscriptionStatus {
        WsSubscriptionStatus::NotSubscriptionResponse
    }
}

#[derive(Clone, Debug)]
struct NoopReconnect;

impl WsReconnectStrategy for NoopReconnect {
    fn next_delay(&mut self) -> Duration {
        Duration::from_secs(0)
    }
    fn reset(&mut self) {}
    fn should_retry(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
struct NoopHandler {
    subs: NoopSubscriptions,
}

impl WsEndpointHandler for NoopHandler {
    type Message = ();
    type Error = std::io::Error;
    type Subscription = NoopSubscriptions;

    fn subscription_manager(&mut self) -> &mut Self::Subscription {
        &mut self.subs
    }

    fn generate_auth(&self) -> Option<Vec<u8>> {
        None
    }

    fn parse(&mut self, _data: &[u8]) -> Result<WsParseOutcome<Self::Message>, Self::Error> {
        Ok(WsParseOutcome::Message(WsMessageAction::Continue))
    }

    fn handle_message(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
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

    fn classify_disconnect(&self, _cause: &WsDisconnectCause) -> WsDisconnectAction {
        WsDisconnectAction::Abort
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let url = std::env::var("WS_URL").unwrap_or_else(|_| "wss://example.invalid".to_string());
    let actor = WebSocketActor::spawn(WebSocketActorArgs {
        url,
        tls: WsTlsConfig::default(),
        transport: TungsteniteTransport::default(),
        reconnect_strategy: NoopReconnect,
        handler: NoopHandler {
            subs: NoopSubscriptions,
        },
        ingress: shared_ws::ws::ForwardAllIngress::default(),
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
    tokio::time::sleep(Duration::from_secs(1)).await;

    match actor.ask(GetConnectionStats).await {
        Ok(stats) => println!("stats: {:?}", stats),
        Err(err) => eprintln!("stats error: {err:?}"),
    }
}
