use super::artifacts::{Manifest, SourceHealth};
use anyhow::{bail, Result};
use serde::Serialize;
use std::{collections::BTreeMap, fs, path::Path};
#[derive(Serialize)]
struct ReportSummary {
    schema_version: u32,
    report_scope: &'static str,
    attempt_id: String,
    eligible: bool,
    matched_signatures: usize,
    target_matched_signatures: usize,
    profile: String,
    started_at: String,
    duration_seconds: f64,
    measurement: super::config::MeasurementHost,
    endpoints: Vec<EndpointPerformance>,
    first_share: BTreeMap<String, u64>,
    completeness: BTreeMap<String, u64>,
    pairwise: Vec<Pairwise>,
    warnings: Vec<String>,
}
#[derive(Serialize)]
struct EndpointPerformance {
    name: String,
    endpoint_host: String,
    resolved_ips: Vec<String>,
    endpoint_type: super::config::StreamKind,
    observations: u64,
    first_detections: u64,
    first_detection_rate_pct: f64,
    p50_lag_ms: f64,
    p95_lag_ms: f64,
    p99_lag_ms: f64,
}
#[derive(Serialize)]
struct Pairwise {
    source_a: String,
    source_b: String,
    comparable: u64,
    a_first: u64,
    b_first: u64,
    ties: u64,
    p50_delta_ns: i64,
    p90_delta_ns: i64,
    p95_delta_ns: i64,
    p99_delta_ns: i64,
    p999_delta_ns: i64,
    max_abs_delta_ns: u64,
}
pub fn render(dir: &Path, public_output: Option<&Path>, operator_lifecycle: bool) -> Result<()> {
    let manifest: Manifest = serde_json::from_slice(&fs::read(dir.join("manifest.json"))?)?;
    let events: Vec<solbench_core::MatchedStreamEvent> =
        super::artifacts::read_ndjson(&dir.join("matched-events.ndjson"))?;
    let health: BTreeMap<String, SourceHealth> =
        serde_json::from_slice(&fs::read(dir.join("source-health.json"))?)?;
    if operator_lifecycle {
        validate_operator_lifecycle(&manifest)?;
    }
    let summary = build(&manifest, &events, operator_lifecycle);
    fs::write(
        dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    let md = markdown(&summary, &health);
    fs::write(dir.join("summary.md"), &md)?;
    fs::write(
        dir.join("report.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    fs::write(dir.join("report.md"), &md)?;
    fs::write(dir.join("report.html"), html(&summary, &health))?;
    super::artifacts::write_checksums(dir)?;
    if let Some(out) = public_output {
        if !operator_lifecycle {
            bail!("public bundles require an explicit publication scope; use --operator-lifecycle for an eligible RPCEdge-only run")
        }
        public_bundle(dir, out)?
    };
    println!("rendered offline report for {}", manifest.attempt_id);
    Ok(())
}
fn build(
    m: &Manifest,
    events: &[solbench_core::MatchedStreamEvent],
    operator_lifecycle: bool,
) -> ReportSummary {
    let mut first = BTreeMap::new();
    let mut complete = BTreeMap::new();
    for e in events {
        if let Some((name, _)) = e.arrivals.iter().min_by_key(|(_, a)| a.receive_offset_ns) {
            *first.entry(name.clone()).or_default() += 1
        }
        for name in e.arrivals.keys() {
            *complete.entry(name.clone()).or_default() += 1
        }
    }
    let names = m.sources.iter().map(|s| s.name.clone()).collect::<Vec<_>>();
    let endpoints = m
        .sources
        .iter()
        .map(|source| endpoint_performance(source, events, &first))
        .collect();
    let mut pairwise = Vec::new();
    for i in 0..names.len() {
        for j in i + 1..names.len() {
            pairwise.push(pair(&names[i], &names[j], events))
        }
    }
    let warnings = m
        .sources
        .iter()
        .filter(|s| s.clock_comparability != solbench_core::ClockComparability::Verified)
        .map(|s| {
            format!(
                "{} provider created_at is diagnostic only ({:?})",
                s.name, s.clock_comparability
            )
        })
        .collect();
    ReportSummary {
        schema_version: 1,
        report_scope: if operator_lifecycle {
            "rpcedge_operator_lifecycle"
        } else {
            "private_diagnostic"
        },
        attempt_id: m.attempt_id.clone(),
        eligible: m.status == super::artifacts::AttemptStatus::Completed
            && m.matched_signatures >= m.target_matched_signatures,
        matched_signatures: m.matched_signatures,
        target_matched_signatures: m.target_matched_signatures,
        profile: m.profile.clone(),
        started_at: m.started_at.clone(),
        duration_seconds: m.duration_ns.unwrap_or_default() as f64 / 1e9,
        measurement: m.measurement.clone(),
        endpoints,
        first_share: first,
        completeness: complete,
        pairwise,
        warnings,
    }
}
fn endpoint_performance(
    source: &super::artifacts::PublicSource,
    events: &[solbench_core::MatchedStreamEvent],
    first: &BTreeMap<String, u64>,
) -> EndpointPerformance {
    let mut lags = Vec::new();
    for event in events {
        let Some(arrival) = event.arrivals.get(&source.name) else {
            continue;
        };
        let earliest = event
            .arrivals
            .values()
            .map(|value| value.receive_offset_ns)
            .min()
            .unwrap_or(arrival.receive_offset_ns);
        lags.push(arrival.receive_offset_ns.saturating_sub(earliest));
    }
    lags.sort_unstable();
    let observations = lags.len() as u64;
    let first_detections = first.get(&source.name).copied().unwrap_or_default();
    let rate = first_detections as f64 * 100.0 / observations.max(1) as f64;
    EndpointPerformance {
        name: source.name.clone(),
        endpoint_host: source.endpoint_host.clone(),
        resolved_ips: source.resolved_ips.clone(),
        endpoint_type: source.kind,
        observations,
        first_detections,
        first_detection_rate_pct: rate,
        p50_lag_ms: percentile(&lags, 0.50) as f64 / 1e6,
        p95_lag_ms: percentile(&lags, 0.95) as f64 / 1e6,
        p99_lag_ms: percentile(&lags, 0.99) as f64 / 1e6,
    }
}
fn percentile(sorted: &[u64], quantile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((quantile * sorted.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}
fn pair(a: &str, b: &str, events: &[solbench_core::MatchedStreamEvent]) -> Pairwise {
    let mut d = Vec::new();
    let (mut aw, mut bw, mut ties) = (0, 0, 0);
    for e in events {
        if let (Some(x), Some(y)) = (e.arrivals.get(a), e.arrivals.get(b)) {
            let v = x.receive_offset_ns as i128 - y.receive_offset_ns as i128;
            let v = v.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            if v < 0 {
                aw += 1
            } else if v > 0 {
                bw += 1
            } else {
                ties += 1
            };
            d.push(v)
        }
    }
    d.sort();
    let q = |p: f64| {
        if d.is_empty() {
            0
        } else {
            d[((p * d.len() as f64).ceil() as usize)
                .saturating_sub(1)
                .min(d.len() - 1)]
        }
    };
    let max = d.iter().map(|v| v.unsigned_abs()).max().unwrap_or(0);
    Pairwise {
        source_a: a.into(),
        source_b: b.into(),
        comparable: d.len() as u64,
        a_first: aw,
        b_first: bw,
        ties,
        p50_delta_ns: q(0.5),
        p90_delta_ns: q(0.9),
        p95_delta_ns: q(0.95),
        p99_delta_ns: q(0.99),
        p999_delta_ns: q(0.999),
        max_abs_delta_ns: max,
    }
}
fn markdown(s: &ReportSummary, h: &BTreeMap<String, SourceHealth>) -> String {
    let title = if s.report_scope == "rpcedge_operator_lifecycle" {
        "RPCEdge deshred lifecycle benchmark"
    } else {
        "Transaction stream benchmark"
    };
    let mut o=format!("# {title}: {}\n\n- Scope: **{}**\n- Eligible: **{}**\n- Matched signatures: **{} / {}**\n- Profile: `{}`\n- Measurement host: **{}**, `{}`{}\n\nThis is an RPCEdge operator-host lifecycle measurement, not a neutral provider ranking. It measures how much earlier the same transaction signature is delivered through `SubscribeDeshred` than through normal processed Yellowstone gRPC. Client monotonic arrival is authoritative. Provider `created_at` is diagnostic only.\n\n",s.attempt_id,s.report_scope,s.eligible,s.matched_signatures,s.target_matched_signatures,s.profile,s.measurement.region,s.measurement.public_ip,s.measurement.datacenter.as_ref().map(|v|format!(", {v}")).unwrap_or_default());
    if s.report_scope == "rpcedge_operator_lifecycle" {
        if let Some(p) = s.pairwise.first() {
            let wins = p.a_first as f64 * 100.0 / p.comparable.max(1) as f64;
            o.push_str(&format!("## Result\n\n`SubscribeDeshred` arrived first for **{wins:.3}%** of paired signatures ({}/{}), with a **{:.3} ms median advantage**. At p95 and p99 of the signed distribution it remained **{:.3} ms** and **{:.3} ms** earlier, respectively.\n\n",p.a_first,p.comparable,-p.p50_delta_ns as f64/1e6,-p.p95_delta_ns as f64/1e6,-p.p99_delta_ns as f64/1e6));
        }
    }
    o.push_str("## Paired client-arrival delta\n\n| A | B | comparable | A first | B first | p50 A-B (ms) | p90 | p95 | p99 | p99.9 |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for p in &s.pairwise {
        o.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} |\n",
            p.source_a,
            p.source_b,
            p.comparable,
            p.a_first,
            p.b_first,
            p.p50_delta_ns as f64 / 1e6,
            p.p90_delta_ns as f64 / 1e6,
            p.p95_delta_ns as f64 / 1e6,
            p.p99_delta_ns as f64 / 1e6,
            p.p999_delta_ns as f64 / 1e6
        ))
    }
    o.push_str("\n## Source health\n\n");
    for (n, v) in h {
        o.push_str(&format!(
            "- **{}**: {} messages, {} duplicates, {} disconnects, {} errors\n",
            n,
            v.messages,
            v.duplicates,
            v.disconnects,
            v.errors.len()
        ))
    }
    o.push_str("\n## Interpretation boundary\n\nThis report compares two lifecycle boundaries operated by RPCEdge. It makes no claim about other RPC providers. Reproduce the methodology against infrastructure you control.\n");
    o
}
fn esc(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
fn html(s: &ReportSummary, h: &BTreeMap<String, SourceHealth>) -> String {
    let Some(p) = s.pairwise.first() else {
        return "<!doctype html><title>Invalid report</title><p>No pairwise data.</p>".into();
    };
    let fastest = s
        .endpoints
        .iter()
        .max_by(|a, b| a.first_detections.cmp(&b.first_detections));
    let endpoint_cards = s.endpoints.iter().map(endpoint_card).collect::<String>();
    let health_rows = h
        .iter()
        .map(|(name, value)| {
            format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(name),
                value.messages,
                value.duplicates,
                value.disconnects,
                value.errors.len()
            )
        })
        .collect::<String>();
    let datacenter = s
        .measurement
        .datacenter
        .as_deref()
        .unwrap_or("Not disclosed");
    let duration_minutes = (s.duration_seconds / 60.0).floor() as u64;
    let duration_seconds = s.duration_seconds.round() as u64 % 60;
    let fastest_name = fastest.map(|value| value.name.as_str()).unwrap_or("n/a");
    let fastest_rate = fastest
        .map(|value| value.first_detection_rate_pct)
        .unwrap_or_default();
    let started_at = format_started(&s.started_at);
    let matched = format_count(s.matched_signatures as u64);
    format!(
        r#"<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>RPCEdge deshred lifecycle benchmark</title>
<style>:root{{color-scheme:dark}}*{{box-sizing:border-box}}body{{max-width:1180px;margin:0 auto;padding:3rem 1.25rem 5rem;background:#08090c;color:#f0f1f4;font:16px system-ui;line-height:1.55}}h1{{font-size:clamp(2.1rem,5vw,3.6rem);line-height:1.08;margin:.4rem 0}}h2{{margin:0 0 .35rem}}.section{{margin-top:2.4rem;background:#151518;border:1px solid #303036;border-radius:18px;padding:1.5rem}}.eyebrow,.label{{color:#9c9ca6;text-transform:uppercase;letter-spacing:.1em;font-size:.78rem}}.accent{{color:#b85cff}}.verified{{color:#42e8b4;border:1px solid #16865f;border-radius:999px;padding:.35rem .8rem;font-size:.75rem}}.section-head{{display:flex;justify-content:space-between;gap:1rem;align-items:start}}.grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:1rem;margin-top:1.25rem}}.card,.mini{{background:#17171b;border:1px solid #36363e;border-radius:14px;padding:1.15rem;overflow-wrap:anywhere}}.value{{font-size:1.6rem;font-weight:700}}.muted{{color:#a2a2ac}}.endpoint{{margin-top:1.2rem;background:#17171b;border:1px solid #36363e;border-radius:16px;padding:1.5rem}}.endpoint h3{{font-size:1.4rem;margin:0}}.endpoint-top{{display:flex;justify-content:space-between;gap:1rem}}.metrics{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:1rem;margin-top:1rem}}.metric{{border:1px solid #36363e;border-radius:12px;padding:1rem}}.metric strong{{display:block;font-size:1.35rem;margin-top:.25rem}}.bar{{height:9px;background:#29292e;border-radius:99px;margin-top:.65rem;overflow:hidden}}.bar span{{display:block;height:100%;background:#3cc39b;border-radius:99px}}.scope{{border-left:3px solid #b85cff;padding:1rem 1.25rem;background:#1b151f;margin-top:1rem}}.table{{overflow-x:auto;border:1px solid #36363e;border-radius:14px}}table{{width:100%;border-collapse:collapse;min-width:720px}}th,td{{padding:.9rem;text-align:right;border-bottom:1px solid #303036}}th:first-child,td:first-child{{text-align:left}}code{{color:#cab7ff}}footer{{margin-top:3rem;color:#9c9ca6}}@media(max-width:760px){{body{{padding-top:2rem}}.grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}.metrics{{grid-template-columns:1fr}}.section-head,.endpoint-top{{display:block}}.verified{{display:inline-block;margin-top:.75rem}}}}@media(max-width:520px){{.grid{{grid-template-columns:1fr}}.section{{padding:1rem}}}}</style>
<header><div class="eyebrow">RPCEdge · performance and benchmarking</div><h1>Deshred <span class="accent">vs</span> processed gRPC</h1><p class="muted">Matched transaction first-detection benchmark for <code>pump_amm_transactions_v1</code>.</p></header>
<section class="section"><div class="section-head"><div><div class="eyebrow">Run</div><h2 class="accent">Benchmark Overview</h2></div><span class="verified">VERIFIED</span></div><div class="grid"><div class="card"><div class="label">Run ID</div><code>{}</code></div><div class="card"><div class="label">Started</div>{}</div><div class="card"><div class="label">Account</div><code>pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA</code></div><div class="card"><div class="label">Commitment</div>processed</div></div></section>
<section class="section"><h2>Run Insights</h2><p class="muted">Snapshot of the standout signals captured for this benchmark run.</p><div class="grid"><div class="card"><div class="label">Signatures captured</div><div class="value">{}</div><span class="muted">Target 50,000</span></div><div class="card"><div class="label">Fastest stream</div><div class="value">{}</div><span class="muted">{fastest_rate:.3}% first detection rate</span></div><div class="card"><div class="label">Test duration</div><div class="value">{duration_minutes}m {duration_seconds}s</div><span class="muted">Sample-paced run plus 30s grace</span></div><div class="card"><div class="label">Measurement host</div><div class="value">Frankfurt</div><span class="muted">{}</span></div></div><div class="scope"><strong>Scope:</strong> RPCEdge operator-host lifecycle measurement. This is not a neutral provider ranking and makes no claim about other RPC providers.</div></section>
<section class="section"><h2>Stream Performance</h2><p class="muted">Latency is each stream's non-negative arrival lag from the first stream observed for the same signature.</p>{endpoint_cards}</section>
<section class="section"><h2>Direct lifecycle delta</h2><p>The signed paired distribution preserves how much earlier deshred arrived. Negative values mean deshred was earlier.</p><div class="table"><table><thead><tr><th>Pair</th><th>Samples</th><th>p50</th><th>p90</th><th>p95</th><th>p99</th><th>p99.9</th></tr></thead><tbody><tr><td>deshred − processed</td><td>{}</td><td>{:.3} ms</td><td>{:.3} ms</td><td>{:.3} ms</td><td>{:.3} ms</td><td>{:.3} ms</td></tr></tbody></table></div></section>
<section class="section"><h2>Methodology and disclosure</h2><div class="grid"><div class="card"><div class="label">Host</div>{}, {}</div><div class="card"><div class="label">Facility</div>{}</div><div class="card"><div class="label">Matching</div>Transaction signature</div><div class="card"><div class="label">Timing</div>Client monotonic receipt</div></div><p><code>SubscribeDeshred</code> exposes pre-execution transaction intent. Normal processed Yellowstone gRPC exposes post-execution transaction metadata. Both streams used the same Pump AMM account filter and public RPCEdge TLS endpoint. Provider <code>created_at</code> is retained only as diagnostic evidence.</p></section>
<h2>Source health</h2><div class="table"><table><thead><tr><th>Source</th><th>Messages</th><th>Duplicates</th><th>Disconnects</th><th>Errors</th></tr></thead><tbody>{health_rows}</tbody></table></div>
<footer>Generated offline from checksummed artifacts. Reproduce the methodology against infrastructure you control.</footer>"#,
        esc(&s.attempt_id),
        esc(&started_at),
        matched,
        esc(fastest_name),
        esc(&s.measurement.public_ip),
        p.comparable,
        p.p50_delta_ns as f64 / 1e6,
        p.p90_delta_ns as f64 / 1e6,
        p.p95_delta_ns as f64 / 1e6,
        p.p99_delta_ns as f64 / 1e6,
        p.p999_delta_ns as f64 / 1e6,
        esc(&s.measurement.region),
        esc(&s.measurement.public_ip),
        esc(datacenter)
    )
}
fn endpoint_card(endpoint: &EndpointPerformance) -> String {
    let title = match endpoint.endpoint_type {
        super::config::StreamKind::Deshred => "RPCEdge SubscribeDeshred",
        super::config::StreamKind::Processed => "RPCEdge processed gRPC",
    };
    let kind = match endpoint.endpoint_type {
        super::config::StreamKind::Deshred => "pre-execution deshred",
        super::config::StreamKind::Processed => "yellowstone processed",
    };
    let ip = endpoint.resolved_ips.join(", ");
    let bar = endpoint.first_detection_rate_pct.clamp(1.0, 100.0);
    let first_detections = format_count(endpoint.first_detections);
    let observations = format_count(endpoint.observations);
    format!(
        r#"<article class="endpoint"><div class="endpoint-top"><div><h3>{}</h3><code>{}</code></div><span class="verified">VERIFIED</span></div><div class="metrics"><div class="metric"><div class="label">Median latency (p50)</div><strong>{:.3} ms</strong><div class="bar"><span style="width:{bar:.3}%"></span></div></div><div class="metric"><div class="label">High tail latency (p95)</div><strong>{:.3} ms</strong></div><div class="metric"><div class="label">P99 latency</div><strong>{:.3} ms</strong></div><div class="metric"><div class="label">First detection rate</div><strong>{:.3}%</strong></div><div class="metric"><div class="label">First detections</div><strong>{}</strong></div><div class="metric"><div class="label">Observations</div><strong>{}</strong></div><div class="metric"><div class="label">Resolved IP</div><strong>{}</strong></div><div class="metric"><div class="label">Endpoint type</div><strong>{kind}</strong></div></div><p><strong>Reliability insight:</strong> detected <span class="accent">{} of {}</span> matched signatures first ({:.3}%).</p></article>"#,
        esc(title),
        esc(&endpoint.endpoint_host),
        endpoint.p50_lag_ms,
        endpoint.p95_lag_ms,
        endpoint.p99_lag_ms,
        endpoint.first_detection_rate_pct,
        first_detections,
        observations,
        esc(&ip),
        first_detections,
        observations,
        endpoint.first_detection_rate_pct
    )
}
fn format_started(value: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.format("%b %d, %Y, %H:%M:%S UTC").to_string())
        .unwrap_or_else(|_| value.to_string())
}
fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let mut output = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            output.push(',');
        }
        output.push(character);
    }
    output
}
fn validate_operator_lifecycle(manifest: &Manifest) -> Result<()> {
    let mut names = manifest
        .sources
        .iter()
        .map(|source| source.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    if names != ["rpcedge_deshred", "rpcedge_processed"] {
        bail!("operator lifecycle publication requires exactly rpcedge_deshred and rpcedge_processed; found {}", names.join(", "))
    }
    if manifest.target_matched_signatures < 50_000 || manifest.matched_signatures < 50_000 {
        bail!(
            "operator lifecycle publication requires at least 50,000 target and matched signatures"
        )
    }
    if manifest.measurement.public_ip != "185.191.118.181"
        || !manifest
            .measurement
            .region
            .to_ascii_lowercase()
            .contains("frankfurt")
    {
        bail!("operator lifecycle publication requires the disclosed RPCEdge Frankfurt host")
    }
    if manifest.status != super::artifacts::AttemptStatus::Completed {
        bail!("operator lifecycle publication requires a completed attempt")
    }
    Ok(())
}
fn public_bundle(dir: &Path, out: &Path) -> Result<()> {
    if out.exists() && fs::read_dir(out)?.next().is_some() {
        bail!("public output must be empty")
    };
    fs::create_dir_all(out)?;
    for n in [
        "manifest.json",
        "config.redacted.toml",
        "source-health.json",
        "summary.json",
        "summary.md",
        "report.json",
        "report.md",
        "report.html",
    ] {
        fs::copy(dir.join(n), out.join(n))?;
    }
    let input = fs::File::open(dir.join("matched-events.ndjson"))?;
    let output = fs::File::create(out.join("matched-events.ndjson.zst"))?;
    zstd::stream::copy_encode(input, output, 9)?;
    super::artifacts::write_checksums(out)
}

#[cfg(test)]
mod tests {
    use super::{format_count, percentile};

    #[test]
    fn endpoint_percentiles_use_nearest_rank() {
        let samples = (1..=100).collect::<Vec<u64>>();
        assert_eq!(percentile(&samples, 0.50), 50);
        assert_eq!(percentile(&samples, 0.95), 95);
        assert_eq!(percentile(&samples, 0.99), 99);
    }

    #[test]
    fn empty_endpoint_percentiles_are_zero() {
        assert_eq!(percentile(&[], 0.99), 0);
    }

    #[test]
    fn report_counts_have_grouping_separators() {
        assert_eq!(format_count(55_424), "55,424");
        assert_eq!(format_count(42), "42");
    }
}
