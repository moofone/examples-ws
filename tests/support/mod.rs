#![allow(dead_code)]

pub mod ws_mock;
pub mod deribit_messages;

pub fn init_tracing() {
    use std::sync::OnceLock;

    use tracing_subscriber::EnvFilter;

    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .try_init();
    });
}
