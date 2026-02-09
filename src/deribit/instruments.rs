use futures_util::future::BoxFuture;

/// Minimal instrument surface used by the Deribit actor adapters.
///
/// Higher-level crates can implement `DeribitInstrumentsProvider` using Deribit's REST API
/// (`public/get_instruments`) and map into this struct.
#[derive(Clone, Debug)]
pub struct DeribitInstrument {
    pub instrument_name: String,
    /// Instrument kind (`"option"`, `"future"`, etc).
    pub kind: String,
    pub is_active: bool,
    /// Expiration timestamp in unix milliseconds (if applicable).
    pub expiration_timestamp_ms: Option<i64>,
}

impl DeribitInstrument {
    pub fn is_option(&self) -> bool {
        self.kind == "option"
    }
}

/// Instrument discovery boundary (typically backed by REST).
pub trait DeribitInstrumentsProvider: Send + Sync + 'static {
    fn get_instruments(
        &self,
        currency: &str,
    ) -> BoxFuture<'static, Result<Vec<DeribitInstrument>, String>>;
}
