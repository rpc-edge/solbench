//! Network probes: open-loop, parallel JSON-RPC latency + slot-lag sampling.
//!
//! Kept in the CLI (not in the network-free `solbench-core`). Each endpoint is
//! sampled on its own thread against a *shared, fixed tick schedule*, so:
//!   - every tick fires its own request worker, so a slow reply never delays the
//!     next send and can never hide a sample — the fix for coordinated omission.
//!     Latency is then the true send->reply round-trip of each request, and
//!   - slot readings are compared at the moment each *server* is estimated to have
//!     read the chain (`send + rtt/2`), not merely at the moment the request left
//!     this host. Requests sent on the same tick do NOT reach their servers at the
//!     same time: a distant endpoint reads the chain later and would otherwise
//!     report a higher slot — and win a freshness comparison — purely for being
//!     far away. See `solbench_core::slotlag`.

use serde::Serialize;
use solbench_core::{observed_at_ns, LatencyRecorder, LatencySummary, SlotLagEstimator};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub use solbench_core::SlotLagSummary as SlotLag;

/// A named endpoint to probe. `url` may carry an API key in the query string; it
/// is never logged or rendered — only [`ProbeResult::host`] (key-stripped) is.
#[derive(Clone)]
pub struct Endpoint {
    pub label: String,
    pub url: String,
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

/// One successful sample: when the request left this host (offset from the shared
/// run origin `t0`), how long the round-trip took, and the slot that came back.
#[derive(Debug, Clone, Copy)]
struct Sample {
    tick: usize,
    send_offset_ns: u64,
    rtt_ns: u64,
    slot: u64,
}

/// Raw per-endpoint output of one sampling run.
struct Raw {
    label: String,
    host: String,
    rec: LatencyRecorder,
    errors: usize,
    last_slot: Option<u64>,
    reads: Vec<Sample>,
}

/// Saturating nanoseconds; a `Duration` can outrange `u64` where a wrapping `as`
/// cast would silently report a huge latency as a tiny one.
fn as_ns(d: Duration) -> u64 {
    d.as_nanos().min(u64::MAX as u128) as u64
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

    let ok: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::new()));
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
                        let rtt_ns = as_ns(send.elapsed());
                        match resp
                            .into_string()
                            .ok()
                            .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                            .and_then(|v| v.get("result").and_then(|r| r.as_u64()))
                        {
                            // `send` is stamped per-request against the run-wide origin
                            // `t0`, so offsets are comparable across endpoints.
                            Some(slot) => ok.lock().unwrap().push(Sample {
                                tick: i,
                                send_offset_ns: as_ns(send.saturating_duration_since(t0)),
                                rtt_ns,
                                slot,
                            }),
                            None => *errors.lock().unwrap() += 1,
                        }
                    }
                    Err(_) => *errors.lock().unwrap() += 1,
                }
            });
        }
    });

    let mut reads = Arc::try_unwrap(ok).unwrap().into_inner().unwrap();
    let errors = Arc::try_unwrap(errors).unwrap().into_inner().unwrap();
    reads.sort_by_key(|r| r.tick);

    let mut rec = LatencyRecorder::with_capacity(reads.len());
    for r in &reads {
        rec.record_ns(r.rtt_ns);
    }
    // reads is tick-sorted, so the last one is the most recent.
    let last_slot = reads.last().map(|r| r.slot);

    Raw {
        label: ep.label.clone(),
        host: redact_host(&ep.url),
        rec,
        errors,
        last_slot,
        reads,
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

    // Score every reading at the moment its SERVER is estimated to have read the
    // chain (send + rtt/2) — not at the moment we sent the request. Two requests
    // sent on the same tick arrive at their servers at different times, so the
    // send instant is the wrong thing to align on.
    let mut est = SlotLagEstimator::with_capacity(raws.iter().map(|r| r.reads.len()).sum());
    for (ep, raw) in raws.iter().enumerate() {
        for r in &raw.reads {
            est.observe(ep, observed_at_ns(r.send_offset_ns, r.rtt_ns), r.slot);
        }
    }
    let mut lags = est.summaries(raws.len());

    raws.into_iter()
        .zip(lags.drain(..))
        .map(|(raw, slot_lag)| ProbeResult {
            label: raw.label,
            host: raw.host,
            samples,
            ok: raw.rec.len(),
            errors: raw.errors,
            current_slot: raw.last_slot,
            latency: raw.rec.summary(),
            slot_lag,
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

    #[test]
    fn redact_host_strips_credentials_and_path() {
        assert_eq!(
            redact_host("https://rpc.rpcedge.com/?api-key=secret"),
            "rpc.rpcedge.com"
        );
        assert_eq!(
            redact_host("https://api.mainnet-beta.solana.com"),
            "api.mainnet-beta.solana.com"
        );
    }

    /// A mock RPC serving a chain that advances one slot every 400ms from a SHARED
    /// epoch. `owd_ms` simulates one-way network delay (round-trip = 2 * owd_ms).
    ///
    /// It reads the slot at request-processing time — `send + owd` — exactly as a
    /// real RPC does. So two of these, at different distances, are by construction
    /// EQUALLY FRESH: neither node is behind the other, only further away.
    fn spawn_mock_rpc(epoch: Instant, base_slot: u64, owd_ms: u64) -> String {
        let server = tiny_http::Server::http("127.0.0.1:0").expect("bind mock rpc");
        let url = format!("http://{}", server.server_addr());
        thread::spawn(move || {
            for mut req in server.incoming_requests() {
                // One thread per request: the probe is open-loop, so a serialised
                // mock would itself become the bottleneck being measured.
                thread::spawn(move || {
                    let mut sink = String::new();
                    let _ = req.as_reader().read_to_string(&mut sink);
                    thread::sleep(Duration::from_millis(owd_ms)); // request in flight
                    let slot = base_slot + (epoch.elapsed().as_millis() as u64 / 400);
                    thread::sleep(Duration::from_millis(owd_ms)); // reply in flight
                    let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{slot}}}"#);
                    let header = tiny_http::Header::from_bytes(
                        &b"content-type"[..],
                        &b"application/json"[..],
                    )
                    .expect("valid header");
                    let _ = req.respond(tiny_http::Response::from_string(body).with_header(header));
                });
            }
        });
        url
    }

    /// Regression: distance is not staleness.
    ///
    /// Both endpoints serve the same chain and are equally fresh; only the network
    /// distance differs. Aligning slots at SEND time reported the co-located
    /// endpoint ~0.75 slots behind and crowned the 600ms-away one the leader —
    /// exactly backwards. Both must now measure ~zero lag.
    #[test]
    fn distance_is_not_reported_as_slot_lag() {
        let epoch = Instant::now();
        let base_slot = 400_000_000u64;

        let near = spawn_mock_rpc(epoch, base_slot, 2); // ~4ms round-trip
        let far = spawn_mock_rpc(epoch, base_slot, 300); // ~600ms round-trip

        let endpoints = vec![
            Endpoint {
                label: "near".into(),
                url: near,
            },
            Endpoint {
                label: "far".into(),
                url: far,
            },
        ];
        let results = probe_all(&endpoints, 8, 200);

        for r in &results {
            assert!(r.ok > 0, "{} returned no samples", r.label);
        }
        let near_lag = results[0].slot_lag.expect("near observed");
        let far_lag = results[1].slot_lag.expect("far observed");

        // The far endpoint really is ~600ms away — the latency metric must still say so.
        let far_p50 = results[1].latency.as_ref().expect("far latency").p50_ns;
        assert!(
            far_p50 > 400_000_000,
            "the distant endpoint should still measure as slow: {far_p50}ns"
        );

        // ...but it is not FRESHER, and the co-located one is not BEHIND.
        assert_eq!(
            near_lag.avg, 0.0,
            "co-located endpoint penalised for being close: {near_lag:?}"
        );
        assert_eq!(
            far_lag.avg, 0.0,
            "distant endpoint credited with freshness it did not earn: {far_lag:?}"
        );
    }
}
