#![allow(dead_code)]

use bytes::Bytes;

/// Deribit JSON-RPC notification envelope for subscriptions.
///
/// Shape (per docs):
/// `{"jsonrpc":"2.0","method":"subscription","params":{"channel":"...","data":{...}}}`
pub fn subscription_notification(channel: &str, data_json: &str) -> Bytes {
    let mut out = String::with_capacity(64 + channel.len() + data_json.len());
    out.push_str("{\"jsonrpc\":\"2.0\",\"method\":\"subscription\",\"params\":{\"channel\":\"");
    // Channels are ASCII-ish; escape conservatively.
    for c in channel.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out.push_str("\",\"data\":");
    out.push_str(data_json);
    out.push_str("}}");
    Bytes::from(out)
}

/// Spec-aligned example payload for `ticker.*` subscription notifications.
///
/// The *data* fields are taken from Deribit's subscription docs example for `ticker` and can be
/// used with any `ticker.<instrument>.<interval>` channel.
pub fn ticker_data_example_json() -> &'static str {
    // Keep this as a raw JSON object string so tests don't depend on serde_json.
    r#"{"timestamp":1664789279599,"instrument_name":"BTC-PERPETUAL","best_bid_price":19586.0,"best_bid_amount":12870.0,"best_ask_price":19586.5,"best_ask_amount":64030.0,"state":"open","last_price":19586.5,"min_price":19533.5,"max_price":19597.0,"mark_price":19586.55,"index_price":19580.83,"settlement_price":19594.55,"funding_8h":0.00001469,"current_funding":0.00004689,"estimated_delivery_price":19580.83,"delivery_price":null,"open_interest":135540030,"interest_rate":0.0,"bid_iv":0.0,"ask_iv":0.0,"underlying_price":null,"underlying_index":"BTC-USD","stats":{"volume":1408782060.0,"price_change":0.0112,"low":19397.0,"high":19597.0},"greeks":null}"#
}

/// Spec-aligned example payload for `platform_state` notifications.
///
/// Source: Deribit subscription docs for `platform_state`.
pub fn platform_state_data_example_json() -> &'static str {
    r#"{"price_index":"sol_usdc","locked":true}"#
}
