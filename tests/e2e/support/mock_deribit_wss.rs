#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use rcgen::CertifiedKey;
use sonic_rs::JsonValueTrait;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::debug;

/// Inbound observations from the mock server (client -> server).
#[derive(Debug)]
pub enum DeribitServerEvent {
    Connected { peer: SocketAddr },
    InboundText { text: String },
    InboundBinary { bytes: Bytes },
    Disconnected,
}

#[derive(Debug)]
enum Outbound {
    Text(String),
}

/// Handle to a spawned mock Deribit-style WSS server.
///
/// Shared test component:
/// - wait on returned `DeribitServerEvent` receiver for inbound frames
/// - call `send_text(...)` to push Deribit-style JSON-RPC responses/notifications
#[derive(Clone)]
pub struct DeribitMockWssHandle {
    addr: SocketAddr,
    outbound: mpsc::UnboundedSender<Outbound>,
    server_cert_der: Vec<u8>,
}

impl DeribitMockWssHandle {
    pub fn url(&self) -> String {
        // Stable DNS name so the TLS server name matches the cert SAN.
        format!("wss://localhost:{}", self.addr.port())
    }

    /// Client-side transport configured to trust this server's self-signed certificate.
    ///
    /// This avoids relying on "disable validation" plumbing (which the shared-ws transport
    /// currently does not apply to tokio-tungstenite's TLS connector).
    pub fn client_transport(&self) -> shared_ws::transport::tungstenite::TungsteniteTransport {
        shared_ws::tls::install_rustls_crypto_provider();

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from(
                self.server_cert_der.clone(),
            ))
            .unwrap();

        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        shared_ws::transport::tungstenite::TungsteniteTransport::rustls(Arc::new(cfg))
    }

    pub fn send_text(&self, text: impl Into<String>) {
        let _ = self.outbound.send(Outbound::Text(text.into()));
    }
}

pub async fn spawn_deribit_mock_wss() -> (
    DeribitMockWssHandle,
    mpsc::UnboundedReceiver<DeribitServerEvent>,
) {
    shared_ws::tls::install_rustls_crypto_provider();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    let cert_der = cert.der().to_vec();
    let key_der = key_pair.serialize_der();

    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let key = rustls::pki_types::PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        key_der,
    ));

    let server_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));

    let (event_tx, event_rx) = mpsc::unbounded_channel::<DeribitServerEvent>();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Outbound>();

    tokio::spawn(async move {
        debug!(addr = %addr, "deribit mock wss: listening");

        // Keep accepting until we get a successful handshake.
        let (peer, ws_stream) = loop {
            let (stream, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    debug!("deribit mock wss: accept failed: {e}");
                    return;
                }
            };

            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    debug!("deribit mock wss: tls handshake failed: {e}");
                    continue;
                }
            };

            let ws_stream = match tokio_tungstenite::accept_async(tls_stream).await {
                Ok(s) => s,
                Err(e) => {
                    debug!("deribit mock wss: websocket handshake failed: {e}");
                    continue;
                }
            };

            break (peer, ws_stream);
        };

        debug!(peer = %peer, "deribit mock wss: connected");
        let _ = event_tx.send(DeribitServerEvent::Connected { peer });

        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        loop {
            tokio::select! {
                msg = ws_rx.next() => {
                    let Some(msg) = msg else { break; };
                    let Ok(msg) = msg else { break; };

                    match msg {
                        tokio_tungstenite::tungstenite::Message::Text(txt) => {
                            debug!(direction = "client->server", text = %txt, "deribit mock wss: text");
                            let _ = event_tx.send(DeribitServerEvent::InboundText { text: txt.to_string() });
                        }
                        tokio_tungstenite::tungstenite::Message::Binary(bin) => {
                            debug!(direction = "client->server", bytes = bin.len(), "deribit mock wss: binary");
                            let _ = event_tx.send(DeribitServerEvent::InboundBinary { bytes: Bytes::from(bin) });
                        }
                        tokio_tungstenite::tungstenite::Message::Ping(_) |
                        tokio_tungstenite::tungstenite::Message::Pong(_) => {}
                        tokio_tungstenite::tungstenite::Message::Close(_) => break,
                        tokio_tungstenite::tungstenite::Message::Frame(_) => {}
                    }
                }
                outbound = out_rx.recv() => {
                    let Some(outbound) = outbound else { break; };
                    match outbound {
                        Outbound::Text(text) => {
                            debug!(direction = "server->client", text = %text, "deribit mock wss: text");
                            let _ = ws_tx.send(tokio_tungstenite::tungstenite::Message::Text(text.into())).await;
                        }
                    }
                }
            }
        }

        debug!("deribit mock wss: disconnected");
        let _ = event_tx.send(DeribitServerEvent::Disconnected);
    });

    (
        DeribitMockWssHandle {
            addr,
            outbound: out_tx,
            server_cert_der: cert_der,
        },
        event_rx,
    )
}

pub fn parse_jsonrpc_method_and_id(text: &str) -> (Option<String>, Option<u64>) {
    let bytes = text.as_bytes();
    let method = sonic_rs::get(bytes, &["method"])
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()));
    let id = sonic_rs::get(bytes, &["id"]).ok().and_then(|v| v.as_u64());
    (method, id)
}

pub fn deribit_subscribe_ack(id: u64, channels: &[&str]) -> String {
    // Deribit examples: {"jsonrpc":"2.0","id":8106,"result":["ticker.BTC-PERPETUAL.raw"]}
    let mut out = String::with_capacity(64 + channels.len() * 48);
    out.push_str("{\"jsonrpc\":\"2.0\",\"id\":");
    out.push_str(&id.to_string());
    out.push_str(",\"result\":[");
    for (i, ch) in channels.iter().enumerate() {
        if i != 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(ch);
        out.push('"');
    }
    out.push_str("]}");
    out
}

pub fn deribit_auth_ok(id: u64) -> String {
    // Deribit-style auth result envelope (minimal but realistic).
    format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{{\"access_token\":\"test_access\",\"expires_in\":31536000,\"token_type\":\"bearer\",\"scope\":\"session:read_write\"}}}}"
    )
}

pub fn deribit_ticker_notification(channel: &str, timestamp_ms: i64, last_price: f64) -> String {
    // Deribit subscription notification envelope.
    format!(
        concat!(
            "{{\"jsonrpc\":\"2.0\",\"method\":\"subscription\",\"params\":{{",
            "\"channel\":\"{channel}\",",
            "\"data\":{{\"timestamp\":{timestamp_ms},\"instrument_name\":\"BTC-PERPETUAL\",\"last_price\":{last_price},\"best_bid_price\":{last_price},\"best_ask_price\":{last_price}}}",
            "}}}}"
        ),
        channel = channel,
        timestamp_ms = timestamp_ms,
        last_price = last_price,
    )
}

pub fn deribit_buy_ok(id: u64, creation_ts_ms: i64) -> String {
    // Deribit-style `private/buy` response. Key field for our matcher: result.order.order_state == "open".
    format!(
        concat!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{{",
            "\"order\":{{",
            "\"order_id\":\"test_order_1\",",
            "\"order_state\":\"open\",",
            "\"instrument_name\":\"BTC-PERPETUAL\",",
            "\"direction\":\"buy\",",
            "\"order_type\":\"limit\",",
            "\"price\":1.0,",
            "\"amount\":10.0,",
            "\"filled_amount\":0.0,",
            "\"creation_timestamp\":{creation_ts_ms}",
            "}}",
            "}}}}"
        ),
        id = id,
        creation_ts_ms = creation_ts_ms,
    )
}
