//! Minimal self-hosted dashboard: serves a live probe comparison over HTTP.
//!
//! Single-threaded and blocking by design — each page load runs a fresh probe of
//! every endpoint and renders the result. No framework, no build step; just the
//! `solbench` binary.

use crate::probe::{probe_all, Endpoint, ProbeResult};
use tiny_http::{Header, Response, Server};

pub fn serve(
    endpoints: Vec<Endpoint>,
    samples: usize,
    interval_ms: u64,
    port: u16,
) -> std::io::Result<()> {
    let server =
        Server::http(("127.0.0.1", port)).map_err(|e| std::io::Error::other(e.to_string()))?;
    println!("solbench dashboard: http://127.0.0.1:{port}  (Ctrl-C to stop)");

    for request in server.incoming_requests() {
        if request.url() == "/favicon.ico" {
            let _ = request.respond(Response::empty(404));
            continue;
        }
        let results = probe_all(&endpoints, samples, interval_ms);
        let html = render(&results, samples);
        let header = Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..])
            .expect("valid header");
        let _ = request.respond(Response::from_string(html).with_header(header));
    }
    Ok(())
}

fn ms(ns: u64) -> String {
    format!("{:.2}", ns as f64 / 1_000_000.0)
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn render(results: &[ProbeResult], samples: usize) -> String {
    let best = results
        .iter()
        .filter_map(|r| r.latency.as_ref().map(|l| (r.host.clone(), l.p50_ns)))
        .min_by_key(|(_, p)| *p)
        .map(|(h, _)| h);

    let mut rows = String::new();
    for r in results {
        let is_best = best.as_deref() == Some(r.host.as_str());
        let (p50, p99, jitter) = match &r.latency {
            Some(l) => (ms(l.p50_ns), ms(l.p99_ns), ms(l.stddev_ns)),
            None => ("-".into(), "-".into(), "-".into()),
        };
        let slot = r
            .current_slot
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".into());
        let lag = r
            .slot_lag
            .as_ref()
            .map(|s| format!("{:.1}", s.avg))
            .unwrap_or_else(|| "-".into());
        let tag = if is_best {
            " <span class=\"tag\">fastest p50</span>"
        } else {
            ""
        };
        rows.push_str(&format!(
            "<tr class=\"{cls}\"><td class=\"lbl\">{label}{tag}</td><td class=\"mono\">{host}</td>\
             <td class=\"num\">{p50}</td><td class=\"num\">{p99}</td><td class=\"num\">{jitter}</td>\
             <td class=\"num\">{ok}/{samples}</td><td class=\"num\">{slot}</td><td class=\"num\">{lag}</td></tr>",
            cls = if is_best { "best" } else { "" },
            label = html_escape(&r.label),
            tag = tag,
            host = html_escape(&r.host),
            p50 = p50,
            p99 = p99,
            jitter = jitter,
            ok = r.ok,
            samples = samples,
            slot = slot,
            lag = lag,
        ));
    }

    TEMPLATE
        .replace("__ROWS__", &rows)
        .replace("__SAMPLES__", &samples.to_string())
}

const TEMPLATE: &str = r#"<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="10">
<title>solbench</title>
<style>
:root{--bg:#08090A;--fg:#E9EEEC;--dim:#8A918E;--accent:#C5F23F;--line:#1c1f1e;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;padding:40px}
.wrap{max-width:920px;margin:0 auto}
h1{font-size:22px;margin:0 0 4px;font-weight:600}
h1 b{color:var(--accent)}
.sub{color:var(--dim);margin:0 0 28px;font-size:13px}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:10px 12px;border-bottom:1px solid var(--line)}
th{color:var(--dim);font-weight:500;font-size:12px;text-transform:uppercase;letter-spacing:.06em}
td.num{text-align:right;font-variant-numeric:tabular-nums}
tr.best td{background:rgba(197,242,63,.06)}
.lbl{font-weight:600}
.tag{color:var(--accent);font-size:11px;border:1px solid var(--accent);border-radius:4px;padding:1px 6px;margin-left:8px}
.mono{color:var(--dim)}
.foot{color:var(--dim);font-size:12px;margin-top:24px}
</style></head>
<body><div class="wrap">
<h1>solbench <b>&raquo;</b> live RPC latency</h1>
<p class="sub">getSlot round-trip, __SAMPLES__ samples/endpoint &middot; auto-refreshes every 10s &middot; times in ms</p>
<table>
<thead><tr><th>endpoint</th><th>host</th><th>p50</th><th>p99</th><th>jitter</th><th>ok</th><th>slot</th><th>lag</th></tr></thead>
<tbody>__ROWS__</tbody>
</table>
<p class="foot"><b>Read latency from THIS host.</b> getSlot round-trip is dominated by network distance to the client &mdash; it is <b>not</b> a proxy for transaction-landing or shred/first-seen latency, where co-located infra wins. Run solbench from your trading edge for a meaningful comparison. &middot; jitter = stddev (consistency) &middot; lag = slots behind the leading endpoint &middot; lower is better</p>
</div></body></html>"#;
