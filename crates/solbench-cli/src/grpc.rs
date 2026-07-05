//! `grpc` — Yellowstone gRPC slot first-seen race (feature `grpc`).
//!
//! Placeholder until the `grpc` feature is wired up (next commit). It will subscribe
//! to two Yellowstone endpoints concurrently and, per slot, report which endpoint
//! saw it first and by how much — the correct way to measure streaming latency
//! (a relative two-endpoint race), never `receive_time - blockTime`.

pub fn race(_slots: usize) -> Result<(), String> {
    Err("`solbench grpc` is not wired in yet (Yellowstone first-seen). Coming in a follow-up build.".into())
}
