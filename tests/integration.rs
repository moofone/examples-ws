// Integration-test crate harness for protocol/library level tests.
//
// Cargo only auto-discovers `tests/*.rs` as integration test crates.
// We keep the actual test modules in `tests/integration/*.rs` for organization.

#[path = "integration/delegated_reply_high_level.rs"]
mod delegated_reply_high_level;
