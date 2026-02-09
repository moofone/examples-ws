//! Exchange-specific examples and adapters built on top of `shared-ws`.
//!
//! `shared-ws` is intended to stay protocol-agnostic.
//! This crate hosts concrete exchange integrations (Bybit, Deribit) that use the
//! generic `shared-ws` infrastructure.

pub mod bybit;
pub mod candles;
pub mod deribit;
pub mod endpoints;
