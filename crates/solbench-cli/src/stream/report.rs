use super::artifacts::{Manifest, SourceHealth};
use anyhow::{bail, Result};
use serde::Serialize;
use std::{collections::BTreeMap, fs, path::Path};
#[derive(Serialize)]
struct ReportSummary {
    schema_version: u32,
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
pub fn render(dir: &Path, public_output: Option<&Path>) -> Result<()> {
    let manifest: Manifest = serde_json::from_slice(&fs::read(dir.join("manifest.json"))?)?;
    let events: Vec<solbench_core::MatchedStreamEvent> =
        super::artifacts::read_ndjson(&dir.join("matched-events.ndjson"))?;
    let health: BTreeMap<String, SourceHealth> =
        serde_json::from_slice(&fs::read(dir.join("source-health.json"))?)?;
    let summary = build(&manifest, &events);
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
        public_bundle(dir, out)?
    };
    println!("rendered offline report for {}", manifest.attempt_id);
    Ok(())
}
fn build(m: &Manifest, events: &[solbench_core::MatchedStreamEvent]) -> ReportSummary {
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
    let mut o=format!("# Transaction stream benchmark: {}\n\n- Eligible: **{}**\n- Matched signatures: **{} / {}**\n- Profile: `{}`\n- Measurement host: **{}**, `{}`{}\n\nClient monotonic arrival is authoritative. Provider `created_at` is diagnostic only unless clocks and semantics are verified comparable.\n\n## Pairwise client-arrival deltas\n\n| A | B | comparable | A first | B first | p50 A-B (ms) | p95 | p99 |\n|---|---|---:|---:|---:|---:|---:|---:|\n",s.attempt_id,s.eligible,s.matched_signatures,s.target_matched_signatures,s.profile,s.measurement.region,s.measurement.public_ip,s.measurement.datacenter.as_ref().map(|v|format!(", {v}")).unwrap_or_default());
    for p in &s.pairwise {
        o.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.3} | {:.3} | {:.3} |\n",
            p.source_a,
            p.source_b,
            p.comparable,
            p.a_first,
            p.b_first,
            p.p50_delta_ns as f64 / 1e6,
            p.p95_delta_ns as f64 / 1e6,
            p.p99_delta_ns as f64 / 1e6
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
    o.push_str("\n## Independent validation\n\nThorofare and GeyserBench runs are attached separately and may corroborate, diverge, or be inconclusive. No validation status is inferred automatically.\n");
    o
}
fn esc(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
fn html(s: &ReportSummary, h: &BTreeMap<String, SourceHealth>) -> String {
    let body = esc(&markdown(s, h));
    format!("<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width'><title>solbench {}</title><style>body{{max-width:1100px;margin:3rem auto;padding:0 1rem;font:16px system-ui;line-height:1.5}}pre{{white-space:pre-wrap;background:#f5f5f5;padding:1.5rem;border-radius:12px}}</style><h1>solbench transaction stream report</h1><pre>{body}</pre>",esc(&s.attempt_id))
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
