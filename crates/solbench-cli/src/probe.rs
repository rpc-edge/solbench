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
pub(crate) fn redact_host(url: &str) -> String {
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

/// Output of one open-loop sampling run over a single endpoint.
pub(crate) struct SampleOutput {
    pub rec: LatencyRecorder,
    pub errors: usize,
    /// (tick, extracted value) for each successful sample, tick-sorted.
    pub extracted: Vec<(usize, u64)>,
}

/// Sample one endpoint `samples` times on the shared tick schedule starting at `t0`.
///
/// True open-loop: each tick spawns its own request worker, so a slow reply never
/// delays the next send (no coordinated omission). Latency is the actual send->reply
/// round-trip; the shared `agent` pools warm connections across workers. `extract`
/// maps a parsed JSON-RPC response to `Some(value)` on success (value is an optional
/// numeric payload such as a slot; use 0 when unused) or `None` to count an error.
pub(crate) fn sample_endpoint<F>(
    url: &str,
    body: &str,
    samples: usize,
    interval: Duration,
    t0: Instant,
    extract: F,
) -> SampleOutput
where
    F: Fn(&serde_json::Value) -> Option<u64> + Sync,
{
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(6))
        .build();

    // (tick, latency_ns, extracted value) for successful samples; separate error counter.
    let ok: Arc<Mutex<Vec<(usize, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(0usize));
    let extract = &extract;

    thread::scope(|scope| {
        for i in 0..samples {
            let intended = t0 + interval * i as u32;
            let now = Instant::now();
            if now < intended {
                thread::sleep(intended - now);
            }
            let agent = agent.clone();
            let url = url.to_string();
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
                        {
                            Some(v) => match extract(&v) {
                                Some(val) => ok.lock().unwrap().push((i, latency_ns, val)),
                                None => *errors.lock().unwrap() += 1,
                            },
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
    let mut extracted = Vec::with_capacity(data.len());
    for &(tick, latency_ns, val) in &data {
        rec.record_ns(latency_ns);
        extracted.push((tick, val));
    }

    SampleOutput {
        rec,
        errors,
        extracted,
    }
}

/// Sample one endpoint's `getSlot` read latency and capture per-tick slots for slot-lag.
fn run_endpoint(ep: &Endpoint, samples: usize, interval: Duration, t0: Instant) -> Raw {
    const GET_SLOT: &str = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;
    let out = sample_endpoint(&ep.url, GET_SLOT, samples, interval, t0, |v| {
        v.get("result").and_then(|r| r.as_u64())
    });
    // `extracted` is tick-sorted, so the last entry is the latest slot.
    let last_slot = out.extracted.last().map(|&(_, slot)| slot);
    Raw {
        label: ep.label.clone(),
        host: redact_host(&ep.url),
        rec: out.rec,
        errors: out.errors,
        last_slot,
        slots: out.extracted,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tiny_http::{Response, Server};

    /// Serve exactly `count` requests with a fixed body, then stop. Returns the URL.
    fn serve_canned(body: &'static str, count: usize) -> (String, thread::JoinHandle<()>) {
        let server = Server::http("127.0.0.1:0").unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/");
        let handle = thread::spawn(move || {
            for _ in 0..count {
                match server.recv() {
                    Ok(req) => {
                        let _ = req.respond(Response::from_string(body));
                    }
                    Err(_) => break,
                }
            }
        });
        (url, handle)
    }

    #[test]
    fn sample_endpoint_records_latency_and_extracts_value() {
        let samples = 5;
        let (url, handle) = serve_canned(r#"{"jsonrpc":"2.0","id":1,"result":123}"#, samples);
        let t0 = Instant::now() + Duration::from_millis(50);
        let out = sample_endpoint(
            &url,
            r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#,
            samples,
            Duration::from_millis(10),
            t0,
            |v| v.get("result").and_then(|r| r.as_u64()),
        );
        assert_eq!(out.rec.len(), samples);
        assert_eq!(out.errors, 0);
        assert!(out.extracted.iter().all(|&(_, v)| v == 123));
        handle.join().unwrap();
    }

    #[test]
    fn sample_endpoint_counts_malformed_as_errors() {
        let samples = 4;
        let (url, handle) = serve_canned("not json", samples);
        let t0 = Instant::now() + Duration::from_millis(50);
        let out = sample_endpoint(&url, "{}", samples, Duration::from_millis(10), t0, |v| {
            v.get("result").and_then(|r| r.as_u64())
        });
        assert_eq!(out.errors, samples);
        assert_eq!(out.rec.len(), 0);
        handle.join().unwrap();
    }

    #[test]
    fn redact_host_strips_credentials() {
        assert_eq!(
            redact_host("https://rpc.rpcedge.com/?api-key=secret"),
            "rpc.rpcedge.com"
        );
    }
}
