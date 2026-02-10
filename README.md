# examples-ws

High-level (actor) end-to-end examples using `shared-rate_limiter`, `examples-ws`, and `shared-ws`.

This repo intentionally hosts exchange/protocol specifics (Bybit, Deribit).

## Versioning

This crate follows SemVer. See `CHANGELOG.md` for notable changes per release.

## Moofone Dependency Versions

As of `examples-ws` `0.1.1`, this repo is pinned to the following versions of the moofone crates:

- `shared-ws` `0.1.0`
- `shared-rate_limiter` `0.1.0`
- `kameo` `0.19.2` (moofone fork)
- `kameo_remote` `0.1.0` (transitive via `kameo`'s `remote` feature)

The root `Cargo.toml` is intended to be a "golden" example of what an external consumer's dependency pins look like for this mini-system.

## License

Licensed under either of:
- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)

at your option.
