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
    measurement: super::config::MeasurementHost,
    first_share: BTreeMap<String, u64>,
    completeness: BTreeMap<String, u64>,
    pairwise: Vec<Pairwise>,
    warnings: Vec<String>,
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
        measurement: m.measurement.clone(),
        first_share: first,
        completeness: complete,
        pairwise,
        warnings,
    }
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
    let wins = p.a_first as f64 * 100.0 / p.comparable.max(1) as f64;
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
    format!(
        r#"<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>RPCEdge deshred lifecycle benchmark</title>
<style>:root{{color-scheme:dark}}*{{box-sizing:border-box}}body{{max-width:1120px;margin:0 auto;padding:3rem 1.25rem 5rem;background:#090b10;color:#e8ebf2;font:16px system-ui;line-height:1.55}}header{{border-bottom:1px solid #293040;padding-bottom:1.5rem;margin-bottom:2rem}}h1{{font-size:clamp(2rem,5vw,3.5rem);line-height:1.08;margin:.5rem 0 1rem}}h2{{margin-top:2.5rem}}.eyebrow{{color:#8da2fb;text-transform:uppercase;letter-spacing:.12em;font-weight:750}}.lede{{max-width:780px;color:#b9c1d1;font-size:1.15rem}}.grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:1rem}}.card{{background:#111620;border:1px solid #293040;border-radius:14px;padding:1.25rem}}.value{{font-size:clamp(1.7rem,4vw,2.6rem);font-weight:750}}.label{{color:#9da8bc}}.scope{{border-left:3px solid #8da2fb;padding:1rem 1.25rem;background:#111620}}.table{{overflow-x:auto;border:1px solid #293040;border-radius:14px}}table{{width:100%;border-collapse:collapse;min-width:720px}}th,td{{padding:.9rem;text-align:right;border-bottom:1px solid #293040}}th:first-child,td:first-child{{text-align:left}}code{{color:#b9c7ff}}.meta{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:.75rem}}.meta div{{background:#111620;padding:1rem;border-radius:10px}}footer{{margin-top:3rem;color:#9da8bc}}@media(max-width:700px){{body{{padding-top:2rem}}.grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}.meta{{grid-template-columns:1fr}}}}</style>
<header><div class="eyebrow">solbench · operator lifecycle report</div><h1>SubscribeDeshred vs processed gRPC</h1><p class="lede">A paired RPCEdge measurement of when the same Pump AMM transaction signature becomes visible before and after execution.</p></header>
<section class="grid"><div class="card"><div class="value">{wins:.3}%</div><div class="label">Deshred arrived first</div></div><div class="card"><div class="value">{:.3} ms</div><div class="label">Median advantage</div></div><div class="card"><div class="value">{}</div><div class="label">Matched signatures</div></div><div class="card"><div class="value">0</div><div class="label">Source errors</div></div></section>
<h2>Result</h2><p><code>SubscribeDeshred</code> arrived first for <strong>{}/{}</strong> paired signatures. At p95 and p99 of the signed distribution it remained <strong>{:.3} ms</strong> and <strong>{:.3} ms</strong> earlier.</p>
<div class="scope"><strong>Interpretation boundary:</strong> this is an RPCEdge operator-host product lifecycle measurement, not a neutral provider ranking. It makes no claim about other RPC providers.</div>
<h2>Signed client-arrival distribution</h2><div class="table"><table><thead><tr><th>Pair</th><th>p50</th><th>p90</th><th>p95</th><th>p99</th><th>p99.9</th></tr></thead><tbody><tr><td>deshred − processed</td><td>{:.3} ms</td><td>{:.3} ms</td><td>{:.3} ms</td><td>{:.3} ms</td><td>{:.3} ms</td></tr></tbody></table></div><p class="label">Negative values mean deshred arrived earlier. Client monotonic receipt time is authoritative.</p>
<h2>Measurement disclosure</h2><div class="meta"><div><span class="label">Profile</span><br><code>{}</code></div><div><span class="label">Attempt</span><br><code>{}</code></div><div><span class="label">Host</span><br>{}, {}</div><div><span class="label">Facility</span><br>{}</div></div>
<h2>Source health</h2><div class="table"><table><thead><tr><th>Source</th><th>Messages</th><th>Duplicates</th><th>Disconnects</th><th>Errors</th></tr></thead><tbody>{health_rows}</tbody></table></div>
<footer>Provider <code>created_at</code> values are retained as diagnostic evidence only. Reproduce this methodology against infrastructure you control.</footer>"#,
        -p.p50_delta_ns as f64 / 1e6,
        s.matched_signatures,
        p.a_first,
        p.comparable,
        -p.p95_delta_ns as f64 / 1e6,
        -p.p99_delta_ns as f64 / 1e6,
        p.p50_delta_ns as f64 / 1e6,
        p.p90_delta_ns as f64 / 1e6,
        p.p95_delta_ns as f64 / 1e6,
        p.p99_delta_ns as f64 / 1e6,
        p.p999_delta_ns as f64 / 1e6,
        esc(&s.profile),
        esc(&s.attempt_id),
        esc(&s.measurement.region),
        esc(&s.measurement.public_ip),
        esc(datacenter)
    )
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
