//! Minimal candle aggregation example.
//!
//! This demonstrates the intended "ingress" pattern:
//! - decode/aggregate/filter in a tight loop (outside kameo)
//! - only emit compact snapshots when something is interesting

#[derive(Copy, Clone, Debug)]
pub struct Trade {
    pub ts_ms: i64,
    pub price: f64,
    pub qty: f64,
}

#[derive(Copy, Clone, Debug)]
pub struct Candle {
    pub start_ms: i64,
    pub tf_ms: u32,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Copy, Clone, Debug)]
pub enum CandleEmitReason {
    HighEpsilon,
    CloseEpsilon,
    Finalized,
}

#[derive(Copy, Clone, Debug)]
pub struct CandleUpdate {
    pub candle: Candle,
    pub reason: CandleEmitReason,
}

/// Fixed-capacity update list without heap allocation.
#[derive(Copy, Clone, Debug)]
pub struct Updates<const N: usize> {
    pub len: usize,
    pub items: [CandleUpdate; N],
}

impl<const N: usize> Updates<N> {
    pub fn new(fallback: CandleUpdate) -> Self {
        Self {
            len: 0,
            items: [fallback; N],
        }
    }

    #[inline]
    pub fn push(&mut self, u: CandleUpdate) {
        if self.len < N {
            self.items[self.len] = u;
            self.len += 1;
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct CandleState {
    tf_ms: u32,
    cur: Option<Candle>,
    last_emit_high: f64,
    last_emit_close: f64,
}

impl CandleState {
    fn new(tf_ms: u32) -> Self {
        Self {
            tf_ms,
            cur: None,
            last_emit_high: f64::NAN,
            last_emit_close: f64::NAN,
        }
    }

    #[inline]
    fn candle_start(&self, ts_ms: i64) -> i64 {
        let tf = self.tf_ms as i64;
        (ts_ms / tf) * tf
    }

    fn on_trade(
        &mut self,
        t: Trade,
        eps_frac: f64,
    ) -> (Option<CandleUpdate>, Option<CandleUpdate>) {
        let start = self.candle_start(t.ts_ms);

        let mut finalized = None;
        if let Some(cur) = self.cur {
            if cur.start_ms != start {
                finalized = Some(CandleUpdate {
                    candle: cur,
                    reason: CandleEmitReason::Finalized,
                });
                self.cur = None;
            }
        }

        let mut c = self.cur.unwrap_or(Candle {
            start_ms: start,
            tf_ms: self.tf_ms,
            open: t.price,
            high: t.price,
            low: t.price,
            close: t.price,
            volume: 0.0,
        });

        c.high = c.high.max(t.price);
        c.low = c.low.min(t.price);
        c.close = t.price;
        c.volume += t.qty;
        self.cur = Some(c);

        let mut epsilon_emit = None;

        let hi_base = if self.last_emit_high.is_nan() {
            c.open
        } else {
            self.last_emit_high
        };
        if (c.high - hi_base).abs() / hi_base.max(1e-12) >= eps_frac {
            self.last_emit_high = c.high;
            epsilon_emit = Some(CandleUpdate {
                candle: c,
                reason: CandleEmitReason::HighEpsilon,
            });
        }

        let cl_base = if self.last_emit_close.is_nan() {
            c.open
        } else {
            self.last_emit_close
        };
        if epsilon_emit.is_none() && (c.close - cl_base).abs() / cl_base.max(1e-12) >= eps_frac {
            self.last_emit_close = c.close;
            epsilon_emit = Some(CandleUpdate {
                candle: c,
                reason: CandleEmitReason::CloseEpsilon,
            });
        }

        (finalized, epsilon_emit)
    }
}

pub struct CandleAggregator<const TFN: usize> {
    states: [CandleState; TFN],
    eps_frac: f64,
}

impl<const TFN: usize> CandleAggregator<TFN> {
    pub fn new(timeframes_ms: [u32; TFN], eps_frac: f64) -> Self {
        Self {
            states: timeframes_ms.map(CandleState::new),
            eps_frac,
        }
    }

    pub fn on_trade<const MAX_UPDATES: usize>(&mut self, t: Trade) -> Updates<MAX_UPDATES> {
        let fallback = CandleUpdate {
            candle: Candle {
                start_ms: 0,
                tf_ms: 0,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                volume: 0.0,
            },
            reason: CandleEmitReason::Finalized,
        };

        let mut out = Updates::<MAX_UPDATES>::new(fallback);
        for s in &mut self.states {
            let (finalized, eps_emit) = s.on_trade(t, self.eps_frac);
            if let Some(u) = finalized {
                out.push(u);
            }
            if let Some(u) = eps_emit {
                out.push(u);
            }
        }
        out
    }
}

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
