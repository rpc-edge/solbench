//! Per-method read-latency matrix: measure a set of read RPC methods across all
//! endpoints, reusing the fair open-loop sampler from `probe`.
//!
//! Latency here is a network-inclusive round-trip from THIS host. Methods differ
//! in server-side cost, so compare endpoints *within* a method and read
//! cross-method gaps as relative method cost, not infrastructure.

use crate::probe::{redact_host, sample_endpoint, Endpoint};
use serde::Serialize;
use serde_json::json;
use solbench_core::LatencySummary;
use std::thread;
use std::time::{Duration, Instant};

/// Canonical Solana sysvar accounts — present on any cluster, so the account-reading
/// methods stay provider-neutral and reproducible.
pub const CLOCK: &str = "SysvarC1ock11111111111111111111111111111111";
pub const RENT: &str = "SysvarRent111111111111111111111111111111111";
pub const RECENT_BLOCKHASHES: &str = "SysvarRecentB1ockHashes11111111111111111111";

/// A read method to probe: a display name and the exact JSON-RPC request body.
pub struct MethodSpec {
    pub name: &'static str,
    pub body: String,
}

/// The default matrix. `account` is the `getAccountInfo` target (default: Clock sysvar).
pub fn method_specs(account: &str) -> Vec<MethodSpec> {
    let mk = |name: &'static str, v: serde_json::Value| MethodSpec {
        name,
        body: v.to_string(),
    };
    vec![
        mk(
            "getSlot",
            json!({"jsonrpc":"2.0","id":1,"method":"getSlot"}),
        ),
        mk(
            "getVersion",
            json!({"jsonrpc":"2.0","id":1,"method":"getVersion"}),
        ),
        mk(
            "getLatestBlockhash",
            json!({"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash"}),
        ),
        mk(
            "getAccountInfo",
            json!({"jsonrpc":"2.0","id":1,"method":"getAccountInfo",
                   "params":[account, {"encoding":"base64"}]}),
        ),
        mk(
            "getMultipleAccounts",
            json!({"jsonrpc":"2.0","id":1,"method":"getMultipleAccounts",
                   "params":[[CLOCK, RENT, RECENT_BLOCKHASHES], {"encoding":"base64"}]}),
        ),
    ]
}

/// Success = a JSON-RPC `result` is present and there is no `error`. The numeric
/// payload is unused for these methods, so return 0 on success.
fn methods_extract(v: &serde_json::Value) -> Option<u64> {
    if v.get("error").is_some() {
        None
    } else {
        v.get("result").is_some().then_some(0u64)
    }
}

#[derive(Serialize)]
pub struct MethodEndpointResult {
    pub label: String,
    pub host: String,
    pub ok: usize,
    pub errors: usize,
    pub latency: Option<LatencySummary>,
}

#[derive(Serialize)]
pub struct MethodReport {
    pub method: String,
    pub results: Vec<MethodEndpointResult>,
}

/// Probe each method across every endpoint. Methods run in sequence; within a
/// method, all endpoints share one fixed tick schedule (fair same-moment compare).
pub fn probe_methods(
    endpoints: &[Endpoint],
    specs: &[MethodSpec],
    samples: usize,
    interval_ms: u64,
) -> Vec<MethodReport> {
    let interval = Duration::from_millis(interval_ms.max(1));
    specs
        .iter()
        .map(|spec| {
            let t0 = Instant::now() + Duration::from_millis(50);
            let results: Vec<MethodEndpointResult> = thread::scope(|scope| {
                let handles: Vec<_> = endpoints
                    .iter()
                    .map(|ep| {
                        scope.spawn(move || {
                            let out = sample_endpoint(
                                &ep.url,
                                &spec.body,
                                samples,
                                interval,
                                t0,
                                methods_extract,
                            );
                            MethodEndpointResult {
                                label: ep.label.clone(),
                                host: redact_host(&ep.url),
                                ok: out.rec.len(),
                                errors: out.errors,
                                latency: out.rec.summary(),
                            }
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("methods probe thread"))
                    .collect()
            });
            MethodReport {
                method: spec.name.to_string(),
                results,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiny_http::{Response, Server};

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
    fn method_specs_are_well_formed() {
        let specs = method_specs(CLOCK);
        let names: Vec<_> = specs.iter().map(|s| s.name).collect();
        assert_eq!(
            names,
            [
                "getSlot",
                "getVersion",
                "getLatestBlockhash",
                "getAccountInfo",
                "getMultipleAccounts"
            ]
        );
        for s in &specs {
            let v: serde_json::Value = serde_json::from_str(&s.body).unwrap();
            assert_eq!(v["method"], s.name);
        }
        let gai: serde_json::Value = serde_json::from_str(
            &method_specs("ACCT")
                .into_iter()
                .find(|s| s.name == "getAccountInfo")
                .unwrap()
                .body,
        )
        .unwrap();
        assert_eq!(gai["params"][0], "ACCT");
        let gma: serde_json::Value = serde_json::from_str(
            &specs
                .iter()
                .find(|s| s.name == "getMultipleAccounts")
                .unwrap()
                .body,
        )
        .unwrap();
        assert_eq!(gma["params"][0].as_array().unwrap().len(), 3);
    }

    #[test]
    fn methods_extract_distinguishes_success_and_error() {
        assert_eq!(methods_extract(&json!({"result":{"x":1}})), Some(0));
        assert_eq!(methods_extract(&json!({"result":123})), Some(0));
        assert_eq!(
            methods_extract(&json!({"error":{"code":-1,"message":"x"}})),
            None
        );
        assert_eq!(methods_extract(&json!({})), None);
    }

    #[test]
    fn probe_methods_reports_latency_per_method() {
        let samples = 3;
        let (url, handle) =
            serve_canned(r#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#, samples);
        let endpoints = vec![Endpoint {
            label: "local".into(),
            url,
        }];
        let specs = vec![MethodSpec {
            name: "getSlot",
            body: r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#.to_string(),
        }];
        let reports = probe_methods(&endpoints, &specs, samples, 10);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].method, "getSlot");
        assert_eq!(reports[0].results.len(), 1);
        let r = &reports[0].results[0];
        assert_eq!(r.ok, samples);
        assert_eq!(r.errors, 0);
        assert!(r.latency.is_some());
        handle.join().unwrap();
    }
}
