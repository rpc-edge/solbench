//! `report` — emit a leaderboard-shaped JSON run.
//!
//! Output matches the `LeaderboardRun` schema that rpcedge.com/benchmarks/live
//! renders: paste it in (or wire a co-located cron to publish it) and the board
//! fills in. `report` measures what `probe` measures — per-provider read latency
//! (p50) and slot-lag; `firstSeenP50` (from `solbench grpc`) and `landingRate`
//! (from `solbench send`) stay `null` until filled from those runs.

use crate::probe::{endpoints_from_env, probe_all, Endpoint};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
struct Entry {
    provider: String,
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    is_self: Option<bool>,
    #[serde(rename = "firstSeenP50")]
    first_seen_p50: Option<f64>,
    #[serde(rename = "slotLag")]
    slot_lag: Option<f64>,
    #[serde(rename = "landingRate")]
    landing_rate: Option<f64>,
    #[serde(rename = "readLatencyP50")]
    read_latency_p50: Option<f64>,
}

#[derive(Serialize)]
struct Run {
    status: &'static str,
    region: String,
    network: String,
    window: String,
    #[serde(rename = "capturedAt")]
    captured_at: String,
    #[serde(rename = "toolRepo")]
    tool_repo: &'static str,
    entries: Vec<Entry>,
}

pub fn run(
    region: Option<String>,
    network: String,
    window: String,
    providers: Vec<String>,
    samples: usize,
    interval_ms: u64,
) -> Result<(), String> {
    // rpc edge (from env) + public baseline, plus any `--provider "name=url"`.
    let mut endpoints = endpoints_from_env();
    for p in &providers {
        let (name, url) = p
            .split_once('=')
            .ok_or_else(|| format!("--provider must be \"name=url\", got: {p}"))?;
        endpoints.push(Endpoint {
            label: name.trim().to_string(),
            url: url.trim().to_string(),
        });
    }

    let results = probe_all(&endpoints, samples, interval_ms);
    let entries = results
        .into_iter()
        .map(|r| Entry {
            is_self: if r.label == "rpc edge" {
                Some(true)
            } else {
                None
            },
            provider: r.label,
            first_seen_p50: None, // from `solbench grpc`
            slot_lag: r.slot_lag.as_ref().map(|s| round1(s.avg)),
            landing_rate: None, // from `solbench send`
            read_latency_p50: r.latency.as_ref().map(|l| round1(l.p50_ns as f64 / 1e6)),
        })
        .collect();

    let region = region
        .or_else(|| std::env::var("SOLBENCH_REGION").ok())
        .unwrap_or_else(|| "unknown".into());

    let run = Run {
        status: "published",
        region,
        network,
        window,
        captured_at: utc_now(),
        tool_repo: "https://github.com/rpc-edge/solbench",
        entries,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&run).map_err(|e| e.to_string())?
    );
    Ok(())
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

/// UTC "YYYY-MM-DD · HH:MM UTC" from the system clock (no date-lib dependency).
fn utc_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as i64;
    let (y, m, d) = civil_from_days(secs.div_euclid(86400));
    let rem = secs.rem_euclid(86400);
    format!(
        "{y:04}-{m:02}-{d:02} · {:02}:{:02} UTC",
        rem / 3600,
        (rem % 3600) / 60
    )
}

/// Howard Hinnant's days-from-civil inverse: days-since-epoch -> (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::civil_from_days;

    #[test]
    fn epoch_and_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(18_993), (2022, 1, 1)); // 2022-01-01
        assert_eq!(civil_from_days(20_454), (2026, 1, 1)); // 2026-01-01
    }
}
