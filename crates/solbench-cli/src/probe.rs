//! Network probes: blocking JSON-RPC latency sampling.
//!
//! Kept in the CLI (not in the network-free `solbench-core`). Each probe times the
//! round-trip of a `getSlot` call over N samples and folds them into a
//! [`solbench_core::LatencyRecorder`], so the numbers come from the shared,
//! tested measurement code.

use serde::Serialize;
use solbench_core::{LatencyRecorder, LatencySummary};
use std::thread::sleep;
use std::time::{Duration, Instant};

/// A named endpoint to probe. `url` may carry an API key in the query string; it
/// is never logged or rendered — only [`ProbeResult::host`] (key-stripped) is.
pub struct Endpoint {
    pub label: String,
    pub url: String,
}

#[derive(Serialize)]
pub struct ProbeResult {
    pub label: String,
    pub host: String,
    pub samples: usize,
    pub ok: usize,
    pub errors: usize,
    pub current_slot: Option<u64>,
    pub latency: Option<LatencySummary>,
}

/// Host portion of a URL with any credentials/query stripped
/// (`https://rpc.rpcedge.com/?api-key=...` -> `rpc.rpcedge.com`).
fn redact_host(url: &str) -> String {
    let no_scheme = url.split("://").nth(1).unwrap_or(url);
    no_scheme
        .split(['/', '?'])
        .next()
        .unwrap_or(no_scheme)
        .to_string()
}

/// Probe one endpoint `samples` times with `getSlot`, recording round-trip latency
/// for each successful call.
pub fn probe_rpc(ep: &Endpoint, samples: usize) -> ProbeResult {
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(6))
        .build();
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;

    let mut rec = LatencyRecorder::with_capacity(samples);
    let mut errors = 0usize;
    let mut current_slot = None;

    for i in 0..samples {
        let started = Instant::now();
        let outcome = agent
            .post(&ep.url)
            .set("content-type", "application/json")
            .send_string(body);
        match outcome {
            Ok(resp) => {
                let elapsed = started.elapsed();
                match resp
                    .into_string()
                    .ok()
                    .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                    .and_then(|v| v.get("result").and_then(|r| r.as_u64()))
                {
                    Some(slot) => {
                        current_slot = Some(slot);
                        rec.record(elapsed);
                    }
                    None => errors += 1,
                }
            }
            Err(_) => errors += 1,
        }
        // Be polite to public endpoints (and avoid trivial rate-limit skew).
        if i + 1 < samples {
            sleep(Duration::from_millis(40));
        }
    }

    ProbeResult {
        label: ep.label.clone(),
        host: redact_host(&ep.url),
        samples,
        ok: rec.len(),
        errors,
        current_slot,
        latency: rec.summary(),
    }
}

/// Endpoint set from the environment: a public mainnet baseline plus rpc edge when
/// `SOLBENCH_RPCEDGE_URL` is set (full URL incl. `?api-key=` — read at runtime,
/// never committed).
pub fn endpoints_from_env() -> Vec<Endpoint> {
    let mut endpoints = Vec::new();
    if let Ok(url) = std::env::var("SOLBENCH_RPCEDGE_URL") {
        if !url.trim().is_empty() {
            endpoints.push(Endpoint {
                label: "rpc edge".into(),
                url: url.trim().to_string(),
            });
        }
    }
    endpoints.push(Endpoint {
        label: "public mainnet".into(),
        url: "https://api.mainnet-beta.solana.com".into(),
    });
    endpoints
}
