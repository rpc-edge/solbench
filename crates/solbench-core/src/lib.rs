//! `solbench-core` — measurement primitives for Solana RPC/gRPC/relay benchmarking.
//!
//! This crate is deliberately network-free and dependency-light: it is the shared
//! foundation reused both by the `solbench` CLI/leaderboard and by downstream
//! latency harnesses (the CLOB market-maker instruments its quote/cancel hot path
//! with [`EventTimeline`], and reports with [`LatencyRecorder`]/[`LandingTracker`]).
//!
//! Everything here is pure and unit-tested so the numbers a benchmark or a bot
//! publishes rest on the same, verifiable measurement code.

pub mod landing;
pub mod stats;
pub mod timeline;

pub use landing::{LandingTracker, SlotDeltaSummary};
pub use stats::{LatencyRecorder, LatencySummary};
pub use timeline::EventTimeline;
