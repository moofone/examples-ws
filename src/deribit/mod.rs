//! Deribit-specific helpers built on top of `shared-ws`.
//!
//! This module provides optional building blocks to migrate legacy Deribit websocket actors to
//! the `shared-ws` architecture:
//! - reconnect strategy with cooldown support,
//! - instrument provider abstraction (for REST-based instrument discovery in higher-level crates),
//! - thin actor wrappers for common public/options flows.

pub mod date;
pub mod instruments;
pub mod reconnect;

pub mod options_actor;
pub mod private_actor;
pub mod public_actor;
