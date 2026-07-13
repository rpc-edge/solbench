//! Network probes: open-loop, parallel JSON-RPC latency + slot-lag sampling.
//!
//! Kept in the CLI (not in the network-free `solbench-core`). Each endpoint is
//! sampled on its own thread against a *shared, fixed tick schedule*, so:
//!   - requests are **issued** on schedule (open-loop): a slow reply never delays
//!     the next send, which is the standard fix for coordinated omission of *samples*,
//!   - latency is **send → reply RTT** (wall time for that request), and
//!   - slot reads are tick-aligned across endpoints, so slot-lag is a fair
//!     same-moment comparison rather than a staggered snapshot.
//!
//! In-flight requests are capped so pathological `--samples` / low `--interval-ms`
//! cannot spawn unbounded threads.

use crate::util::redact_host;
use serde::Serialize;
use solbench_core::{LatencyRecorder, LatencySummary};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Cap concurrent request workers per endpoint (open-loop still issues on schedule
/// until this many are outstanding; then the scheduler joins the oldest worker).
const MAX_INFLIGHT: usize = 64;

/// A named endpoint to probe. `url` may carry an API key in the query string; it
/// is never logged or rendered — only [`ProbeResult::host`] (key-stripped) is.
#[derive(Clone)]
pub struct Endpoint {
    pub label: String,
    pub url: String,
}

/// Slot freshness relative to the leading endpoint at each shared tick.
#[derive(Serialize, Clone)]
pub struct SlotLag {
    /// Mean slots behind the fastest endpoint, across ticks (0.0 == always leading).
    pub avg: f64,
    /// Worst-case slots behind at any single tick.
    pub max: i64,
    pub ticks: usize,
}

#[derive(Serialize, Clone)]
pub struct ProbeResult {
    pub label: String,
    pub host: String,
    pub samples: usize,
    pub ok: usize,
    pub errors: usize,
    pub current_slot: Option<u64>,
    pub latency: Option<LatencySummary>,
    pub slot_lag: Option<SlotLag>,
}

/// Raw per-endpoint output of one sampling run.
struct Raw {
    label: String,
    host: String,
    rec: LatencyRecorder,
    errors: usize,
    last_slot: Option<u64>,
    /// (tick_index, slot) for each successful sample.
    slots: Vec<(usize, u64)>,
}

/// Sample one endpoint `samples` times on the shared tick schedule starting at `t0`.
///
/// True open-loop issue rate: each tick spawns its own request worker (subject to
/// [`MAX_INFLIGHT`]), so a slow reply never hides the next sample. Latency is the
/// actual send→reply round-trip; the shared `agent` pools warm connections.
fn run_endpoint(ep: &Endpoint, samples: usize, interval: Duration, t0: Instant) -> Raw {
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(6))
        .build();
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;

    let ok: Arc<Mutex<Vec<(usize, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(0usize));

    thread::scope(|scope| {
        let mut handles: Vec<thread::ScopedJoinHandle<'_, ()>> =
            Vec::with_capacity(MAX_INFLIGHT.min(samples.max(1)));
        for i in 0..samples {
            let intended = t0 + interval * i as u32;
            let now = Instant::now();
            if now < intended {
                thread::sleep(intended - now);
            }
            // Bound concurrency without rewriting the open-loop schedule semantics.
            if handles.len() >= MAX_INFLIGHT {
                let h = handles.remove(0);
                let _ = h.join();
            }
            let agent = agent.clone();
            let url = ep.url.clone();
            let ok = Arc::clone(&ok);
            let errors = Arc::clone(&errors);
            handles.push(scope.spawn(move || {
                let send = Instant::now();
                let outcome = agent
                    .post(&url)
                    .set("content-type", "application/json")
                    .send_string(body);
                match outcome {
                    Ok(resp) => {
                        let latency_ns = send.elapsed().as_nanos() as u64;
                        match resp
                            .into_string()
                            .ok()
                            .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                            .and_then(|v| v.get("result").and_then(|r| r.as_u64()))
                        {
                            Some(slot) => {
                                if let Ok(mut g) = ok.lock() {
                                    g.push((i, latency_ns, slot));
                                }
                            }
                            None => {
                                if let Ok(mut g) = errors.lock() {
                                    *g += 1;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        if let Ok(mut g) = errors.lock() {
                            *g += 1;
                        }
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.join();
        }
    });

    let mut data = match Arc::try_unwrap(ok) {
        Ok(m) => m.into_inner().unwrap_or_default(),
        Err(a) => a.lock().map(|g| g.clone()).unwrap_or_default(),
    };
    let errors = match Arc::try_unwrap(errors) {
        Ok(m) => m.into_inner().unwrap_or(0),
        Err(a) => a.lock().map(|g| *g).unwrap_or(0),
    };
    data.sort_by_key(|&(tick, _, _)| tick);

    let mut rec = LatencyRecorder::with_capacity(data.len());
    let mut slots = Vec::with_capacity(data.len());
    let mut last_slot = None;
    for &(tick, latency_ns, slot) in &data {
        rec.record_ns(latency_ns);
        slots.push((tick, slot));
        last_slot = Some(slot);
    }

    Raw {
        label: ep.label.clone(),
        host: redact_host(&ep.url),
        rec,
        errors,
        last_slot,
        slots,
    }
}

/// Probe every endpoint concurrently on a shared schedule and compute per-endpoint
/// latency + tick-aligned slot-lag.
pub fn probe_all(endpoints: &[Endpoint], samples: usize, interval_ms: u64) -> Vec<ProbeResult> {
    let samples = samples.max(1);
    let interval = Duration::from_millis(interval_ms.max(1));
    // Small startup offset so every thread shares the same tick-0 origin.
    let t0 = Instant::now() + Duration::from_millis(50);

    let raws: Vec<Raw> = thread::scope(|scope| {
        let handles: Vec<_> = endpoints
            .iter()
            .map(|ep| scope.spawn(move || run_endpoint(ep, samples, interval, t0)))
            .collect();
        handles.into_iter().filter_map(|h| h.join().ok()).collect()
    });

    // Leading (max) slot observed at each shared tick.
    let mut leader: HashMap<usize, u64> = HashMap::new();
    for raw in &raws {
        for &(tick, slot) in &raw.slots {
            leader
                .entry(tick)
                .and_modify(|m| *m = (*m).max(slot))
                .or_insert(slot);
        }
    }

    raws.into_iter()
        .map(|raw| {
            let ok = raw.rec.len();
            let latency = raw.rec.summary();
            let slot_lag = if raw.slots.is_empty() {
                None
            } else {
                let mut sum = 0i64;
                let mut max = 0i64;
                for &(tick, slot) in &raw.slots {
                    let lag = leader.get(&tick).copied().unwrap_or(slot) as i64 - slot as i64;
                    sum += lag;
                    max = max.max(lag);
                }
                Some(SlotLag {
                    avg: sum as f64 / raw.slots.len() as f64,
                    max,
                    ticks: raw.slots.len(),
                })
            };
            ProbeResult {
                label: raw.label,
                host: raw.host,
                samples,
                ok,
                errors: raw.errors,
                current_slot: raw.last_slot,
                latency,
                slot_lag,
            }
        })
        .collect()
}

/// Endpoint set from the environment: a public mainnet baseline plus an optional
/// primary provider when `SOLBENCH_RPCEDGE_URL` is set (full URL incl. `?api-key=` —
/// read at runtime, never committed).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::redact_host;

    #[test]
    fn redact_used_for_probe_hosts() {
        assert_eq!(
            redact_host("https://u:p@rpc.example.com/?api-key=x"),
            "rpc.example.com"
        );
    }

    #[test]
    fn empty_endpoints_returns_empty() {
        assert!(probe_all(&[], 5, 50).is_empty());
    }
}
