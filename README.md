# examples-ws

Concrete exchange adapters and runnable examples built on top of [`shared-ws`](https://github.com/moofone/shared-ws).

This repo intentionally hosts exchange/protocol specifics (Bybit, Deribit). The `shared-ws` crate stays generic.

## Running

- Run a basic connection skeleton:
  - `cargo run --bin basic_connect`

- Run the candle aggregation demo:
  - `cargo run --bin candle_aggregation`

## Tests

`cargo test`
