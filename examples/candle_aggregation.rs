//! Minimal candle aggregation example.
//!
//! This demonstrates the intended "ingress" pattern:
//! - decode/aggregate/filter in a tight loop (outside kameo)
//! - only emit compact snapshots when something is interesting

use examples_ws::candles::{CandleAggregator, Trade};

fn main() {
    // 1s, 5s, 1m candles, epsilon 1%.
    let mut agg = CandleAggregator::<3>::new([1_000, 5_000, 60_000], 0.01);

    // Pretend these trades came from decoded websocket frames.
    let trades = [
        Trade {
            ts_ms: 1_700_000_000_000,
            price: 100.0,
            qty: 1.0,
        },
        Trade {
            ts_ms: 1_700_000_000_200,
            price: 100.2,
            qty: 0.5,
        },
        Trade {
            ts_ms: 1_700_000_000_400,
            price: 101.5,
            qty: 0.25,
        },
        Trade {
            ts_ms: 1_700_000_001_050,
            price: 100.9,
            qty: 1.25,
        },
    ];

    for t in trades {
        let updates = agg.on_trade::<8>(t);
        for u in updates.items.iter().take(updates.len) {
            println!(
                "tf={} start={} o={} h={} l={} c={} v={} ({:?})",
                u.candle.tf_ms,
                u.candle.start_ms,
                u.candle.open,
                u.candle.high,
                u.candle.low,
                u.candle.close,
                u.candle.volume,
                u.reason
            );
        }
    }
}
