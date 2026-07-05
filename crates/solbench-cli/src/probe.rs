//! Network probes: open-loop, parallel JSON-RPC latency + slot-lag sampling.
//!
//! Kept in the CLI (not in the network-free `solbench-core`). Each endpoint is
//! sampled on its own thread against a *shared, fixed tick schedule*, so:
//!   - latency is measured from each sample's INTENDED start time, not its actual
//!     issue time — this corrects coordinated omission (a slow reply that delays
//!     the next send inflates the measured latency instead of being hidden), and
//!   - slot reads are tick-aligned across endpoints, so slot-lag is a fair
//!     same-moment comparison rather than a snapshot.

use serde::Serialize;
use solbench_core::{LatencyRecorder, LatencySummary};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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
/// True open-loop: each tick spawns its own request worker, so a slow reply never
/// delays the next send (no coordinated omission). Latency is the actual send->reply
/// round-trip; the shared `agent` pools warm connections across workers.
fn run_endpoint(ep: &Endpoint, samples: usize, interval: Duration, t0: Instant) -> Raw {
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(6))
        .build();
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;

    // (tick, latency_ns, slot) for successful samples; separate error counter.
    let ok: Arc<Mutex<Vec<(usize, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(0usize));

    thread::scope(|scope| {
        for i in 0..samples {
            let intended = t0 + interval * i as u32;
            let now = Instant::now();
            if now < intended {
                thread::sleep(intended - now);
            }
            let agent = agent.clone();
            let url = ep.url.clone();
            let ok = Arc::clone(&ok);
            let errors = Arc::clone(&errors);
            scope.spawn(move || {
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
                            Some(slot) => ok.lock().unwrap().push((i, latency_ns, slot)),
                            None => *errors.lock().unwrap() += 1,
                        }
                    }
                    Err(_) => *errors.lock().unwrap() += 1,
                }
            });
        }
    });

    let mut data = Arc::try_unwrap(ok).unwrap().into_inner().unwrap();
    let errors = Arc::try_unwrap(errors).unwrap().into_inner().unwrap();
    data.sort_by_key(|&(tick, _, _)| tick);

    let mut rec = LatencyRecorder::with_capacity(data.len());
    let mut slots = Vec::with_capacity(data.len());
    let mut last_slot = None;
    for &(tick, latency_ns, slot) in &data {
        rec.record_ns(latency_ns);
        slots.push((tick, slot));
        last_slot = Some(slot); // data is tick-sorted, so this ends on the latest
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
    let interval = Duration::from_millis(interval_ms.max(1));
    // Small startup offset so every thread shares the same tick-0 origin.
    let t0 = Instant::now() + Duration::from_millis(50);

    let raws: Vec<Raw> = thread::scope(|scope| {
        let handles: Vec<_> = endpoints
            .iter()
            .map(|ep| scope.spawn(move || run_endpoint(ep, samples, interval, t0)))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("probe thread"))
            .collect()
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
                    let lag = leader[&tick] as i64 - slot as i64;
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
