//! `mdc-inspect` — a full-screen terminal application for inspecting the
//! market-data providers of this workspace.
//!
//! The application runs one provider session at a time over a bounded,
//! loss-intolerant delivery path: provider events stream through
//! [`mdc_sinks::loss_intolerant_channel`] into an ordered reducer, and the
//! renderer draws coalesced display snapshots at a fixed cadence. It is
//! inspect-only: start, stop, focus, scroll, help, and quit.
//!
//! # Features
//!
//! - `binance` — Binance Spot inspection (enabled by default).
//! - `kraken` — Kraken inspection.
//! - `coinbase` — Coinbase Exchange inspection.
//! - `bitfinex` — Bitfinex inspection.
//! - `gateio` — Gate.io inspection.
//! - `bitstamp` — Bitstamp inspection.
//! - `huobi` — Huobi/HTX inspection.
//! - `coinbase-advanced` — Coinbase Advanced inspection.
//! - `lmax` — LMAX Digital inspection.
//! - `frankfurter` — Frankfurter/ECB dated reference rates.
//! - `vwd` — VWD/Infront synthetic indicative FX books; credentials come
//!   from the `MDC_INSPECT_VWD_*` environment variables.
//! - `all` — All eleven venue features.
//! - `native-tls` — TLS backend via native-tls (enabled by default).
//! - `rustls` — TLS backend via rustls.
//!
//! Venue features forward one-to-one to identically named `mdc-providers`
//! features. A binary without a venue feature starts, but offers no
//! selectable provider.

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(unexpected_cfgs))]

pub mod action;
pub mod app;
pub mod cli;
pub mod config;
pub mod diagnostics;
pub mod display;
pub mod error;
pub mod provider;
pub mod reducer;
pub mod session;
pub mod setup;
pub mod terminal;
pub mod view;
