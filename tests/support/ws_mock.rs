#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use shared_ws::client::accept_async;
use shared_ws::ws::{WsCloseFrame, WsFrame, into_ws_message};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};

#[derive(Debug, Clone, Copy)]
pub struct WsMockConfig {
    /// If true, automatically reply to inbound `Ping` frames with a matching `Pong`.
    pub auto_pong: bool,
}

impl Default for WsMockConfig {
    fn default() -> Self {
        Self { auto_pong: true }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsMockFrameKind {
    Text,
    Binary,
    Ping,
    Pong,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsMockFrame {
    pub kind: WsMockFrameKind,
    pub bytes: Bytes,
}

#[derive(Debug)]
pub enum WsMockEvent {
    Connected { conn_id: usize },
    Frame { conn_id: usize, frame: WsMockFrame },
    Disconnected { conn_id: usize },
}

#[derive(Debug, Clone)]
pub enum WsMockCmd {
    SendText {
        conn_id: usize,
        text: String,
    },
    SendBinary {
        conn_id: usize,
        bytes: Bytes,
    },
    SendPing {
        conn_id: usize,
        bytes: Bytes,
    },
    SendPong {
        conn_id: usize,
        bytes: Bytes,
    },
    /// Websocket close handshake (optional code/reason).
    Close {
        conn_id: usize,
        code: Option<u16>,
        reason: Option<Bytes>,
    },
    /// Drop the connection without a close handshake.
    Drop {
        conn_id: usize,
    },
}

#[derive(Clone)]
struct ConnCmdTx {
    tx: mpsc::UnboundedSender<WsMockCmd>,
}

pub struct WsMockServer {
    pub addr: SocketAddr,
    pub cmds: mpsc::UnboundedSender<WsMockCmd>,
    events: mpsc::UnboundedReceiver<WsMockEvent>,
}

impl WsMockServer {
    pub async fn spawn() -> Self {
        Self::spawn_with(WsMockConfig::default()).await
    }

    pub async fn spawn_with(cfg: WsMockConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (evt_tx, evt_rx) = mpsc::unbounded_channel::<WsMockEvent>();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<WsMockCmd>();

        let conns: Arc<Mutex<HashMap<usize, ConnCmdTx>>> = Arc::new(Mutex::new(HashMap::new()));
        let next_id = Arc::new(AtomicUsize::new(0));

        // Router: global commands -> per-connection command channels.
        {
            let conns = conns.clone();
            tokio::spawn(async move {
                while let Some(cmd) = cmd_rx.recv().await {
                    let conn_id = match &cmd {
                        WsMockCmd::SendText { conn_id, .. } => *conn_id,
                        WsMockCmd::SendBinary { conn_id, .. } => *conn_id,
                        WsMockCmd::SendPing { conn_id, .. } => *conn_id,
                        WsMockCmd::SendPong { conn_id, .. } => *conn_id,
                        WsMockCmd::Close { conn_id, .. } => *conn_id,
                        WsMockCmd::Drop { conn_id, .. } => *conn_id,
                    };
                    let tx = { conns.lock().await.get(&conn_id).cloned() };
                    if let Some(tx) = tx {
                        let _ = tx.tx.send(cmd);
                    }
                }
            });
        }

        tokio::spawn({
            let conns = conns.clone();
            let next_id = next_id.clone();
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };

                    let conn_id = next_id.fetch_add(1, Ordering::Relaxed).saturating_add(1);

                    let (conn_cmd_tx, mut conn_cmd_rx) = mpsc::unbounded_channel::<WsMockCmd>();
                    conns
                        .lock()
                        .await
                        .insert(conn_id, ConnCmdTx { tx: conn_cmd_tx });

                    let evt_tx2 = evt_tx.clone();
                    let conns2 = conns.clone();
                    tokio::spawn(async move {
                        let mut ws = match accept_async(stream).await {
                            Ok(ws) => ws,
                            Err(_) => {
                                conns2.lock().await.remove(&conn_id);
                                let _ = evt_tx2.send(WsMockEvent::Disconnected { conn_id });
                                return;
                            }
                        };
                        let _ = evt_tx2.send(WsMockEvent::Connected { conn_id });

                        loop {
                            tokio::select! {
                                biased;

                                cmd = conn_cmd_rx.recv() => {
                                    let Some(cmd) = cmd else { break };
                                    match cmd {
                                        WsMockCmd::SendText { text, .. } => {
                                            let _ = ws.send(into_ws_message(text)).await;
                                        }
                                        WsMockCmd::SendBinary { bytes, .. } => {
                                            let _ = ws.send(into_ws_message(bytes)).await;
                                        }
                                        WsMockCmd::SendPing { bytes, .. } => {
                                            let _ = ws.send(WsFrame::Ping(bytes)).await;
                                        }
                                        WsMockCmd::SendPong { bytes, .. } => {
                                            let _ = ws.send(WsFrame::Pong(bytes)).await;
                                        }
                                        WsMockCmd::Close { code, reason, .. } => {
                                            let frame = match (code, reason) {
                                                (Some(code), Some(reason)) => WsFrame::Close(Some(WsCloseFrame { code, reason })),
                                                (Some(code), None) => WsFrame::Close(Some(WsCloseFrame { code, reason: Bytes::new() })),
                                                _ => WsFrame::Close(None),
                                            };
                                            let _ = ws.send(frame).await;
                                            break;
                                        }
                                        WsMockCmd::Drop { .. } => {
                                            break;
                                        }
                                    }
                                }

                                msg = ws.next() => {
                                    let Some(msg) = msg else { break };
                                    let Ok(frame) = msg else { break };

                                    let (kind, bytes) = match frame {
                                        WsFrame::Text(text) => (WsMockFrameKind::Text, text.into_bytes()),
                                        WsFrame::Binary(bytes) => (WsMockFrameKind::Binary, bytes),
                                        WsFrame::Ping(bytes) => {
                                            if cfg.auto_pong {
                                                let _ = ws.send(WsFrame::Pong(bytes.clone())).await;
                                            }
                                            (WsMockFrameKind::Ping, bytes)
                                        }
                                        WsFrame::Pong(bytes) => (WsMockFrameKind::Pong, bytes),
                                        WsFrame::Close(_) => (WsMockFrameKind::Close, Bytes::new()),
                                    };

                                    let _ = evt_tx2.send(WsMockEvent::Frame {
                                        conn_id,
                                        frame: WsMockFrame { kind, bytes },
                                    });

                                    if matches!(kind, WsMockFrameKind::Close) {
                                        break;
                                    }
                                }
                            }
                        }

                        conns2.lock().await.remove(&conn_id);
                        let _ = evt_tx2.send(WsMockEvent::Disconnected { conn_id });
                    });
                }
            }
        });

        Self {
            addr,
            cmds: cmd_tx,
            events: evt_rx,
        }
    }

    pub async fn recv(&mut self) -> Option<WsMockEvent> {
        self.events.recv().await
    }

    pub fn send_text(&self, conn_id: usize, text: impl Into<String>) {
        let _ = self.cmds.send(WsMockCmd::SendText {
            conn_id,
            text: text.into(),
        });
    }

    pub fn send_ping(&self, conn_id: usize, bytes: Bytes) {
        let _ = self.cmds.send(WsMockCmd::SendPing { conn_id, bytes });
    }

    pub fn send_pong(&self, conn_id: usize, bytes: Bytes) {
        let _ = self.cmds.send(WsMockCmd::SendPong { conn_id, bytes });
    }

    pub async fn wait_connected(&mut self, timeout: Duration) -> usize {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let evt = tokio::time::timeout(remaining, self.recv())
                .await
                .unwrap_or(None)
                .unwrap_or_else(|| panic!("timed out waiting for server connect"));
            if let WsMockEvent::Connected { conn_id } = evt {
                return conn_id;
            }
        }
    }

    pub async fn wait_frame(&mut self, timeout: Duration, conn_id: Option<usize>) -> WsMockFrame {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let evt = tokio::time::timeout(remaining, self.recv())
                .await
                .unwrap_or(None)
                .unwrap_or_else(|| panic!("timed out waiting for server frame"));
            match evt {
                WsMockEvent::Frame {
                    conn_id: got,
                    frame,
                } => {
                    if conn_id.map(|c| c == got).unwrap_or(true) {
                        return frame;
                    }
                }
                _ => {}
            }
        }
    }

    pub async fn wait_kind(
        &mut self,
        timeout: Duration,
        conn_id: usize,
        kind: WsMockFrameKind,
    ) -> WsMockFrame {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let evt = tokio::time::timeout(remaining, self.recv())
                .await
                .unwrap_or(None)
                .unwrap_or_else(|| panic!("timed out waiting for server frame kind={kind:?}"));
            match evt {
                WsMockEvent::Frame {
                    conn_id: got,
                    frame,
                } if got == conn_id && frame.kind == kind => return frame,
                _ => {}
            }
        }
    }
}
