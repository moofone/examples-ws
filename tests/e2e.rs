// Integration-test crate harness for "end-to-end" actor flows.
//
// Cargo only auto-discovers `tests/*.rs` as integration test crates.
// We keep the actual test modules in `tests/e2e/*.rs` for organization.

#[path = "e2e/bybit_private_flow.rs"]
mod bybit_private_flow;

#[path = "e2e/bybit_public_flow.rs"]
mod bybit_public_flow;

#[path = "e2e/bybit_public_actor_disconnect.rs"]
mod bybit_public_actor_disconnect;

#[path = "e2e/bybit_public_actor_stale.rs"]
mod bybit_public_actor_stale;

#[path = "e2e/bybit_public_actor_stream_reconnect.rs"]
mod bybit_public_actor_stream_reconnect;

#[path = "e2e/deribit_public_flow.rs"]
mod deribit_public_flow;

#[path = "e2e/deribit_options_actor.rs"]
mod deribit_options_actor;

#[path = "e2e/deribit_private_create_open_order_delegated.rs"]
mod deribit_private_create_open_order_delegated;
