//! `solbench` CLI.
//!
//! `probe` measures live RPC latency; `serve` renders it as a local dashboard;
//! `demo` exercises the measurement core on synthetic data.

mod probe;
mod server;

use clap::{Parser, Subcommand};
use probe::{endpoints_from_env, probe_rpc};
use solbench_core::LatencyRecorder;

#[derive(Parser)]
#[command(
    name = "solbench",
    version,
    about = "Continuous Solana RPC/gRPC/relay benchmark."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Probe endpoints once and print a latency comparison.
    Probe {
        /// Samples per endpoint.
        #[arg(long, default_value_t = 20)]
        samples: usize,
        /// Emit raw per-endpoint results as JSON (for CI / reproducible runs).
        #[arg(long)]
        json: bool,
    },
    /// Serve a live latency dashboard on localhost.
    Serve {
        #[arg(long, default_value_t = 8787)]
        port: u16,
        /// Samples per endpoint, per page load.
        #[arg(long, default_value_t = 12)]
        samples: usize,
    },
    /// Run the measurement pipeline over synthetic samples (demonstrates solbench-core).
    Demo,
}

fn main() {
    match Cli::parse().command {
        Command::Probe { samples, json } => {
            let endpoints = endpoints_from_env();
            if !json && endpoints.iter().all(|e| e.label != "rpc edge") {
                eprintln!("note: set SOLBENCH_RPCEDGE_URL to include rpc edge in the comparison.");
            }
            let results: Vec<_> = endpoints.iter().map(|ep| probe_rpc(ep, samples)).collect();

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&results).expect("results serialize")
                );
                return;
            }

            let ms = |ns: u64| format!("{:.2}", ns as f64 / 1e6);
            println!(
                "{:<16} {:<26} {:>8} {:>8} {:>8} {:>8} {:>7} {:>12}",
                "endpoint", "host", "p50ms", "p99ms", "jitter", "max", "ok", "slot"
            );
            for r in &results {
                let (p50, p99, jitter, max) = match &r.latency {
                    Some(l) => (ms(l.p50_ns), ms(l.p99_ns), ms(l.stddev_ns), ms(l.max_ns)),
                    None => ("-".into(), "-".into(), "-".into(), "-".into()),
                };
                println!(
                    "{:<16} {:<26} {:>8} {:>8} {:>8} {:>8} {:>7} {:>12}",
                    r.label,
                    r.host,
                    p50,
                    p99,
                    jitter,
                    max,
                    format!("{}/{}", r.ok, r.samples),
                    r.current_slot
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "-".into()),
                );
            }
            eprintln!(
                "\njitter = stddev (consistency); lower is steadier. getSlot round-trip from THIS\n\
                 host (read latency) - dominated by network distance to the client, NOT a proxy for\n\
                 tx-landing or shred first-seen latency. Run from your co-located edge for a\n\
                 comparison that reflects the infra."
            );
        }
        Command::Serve { port, samples } => {
            let endpoints = endpoints_from_env();
            if let Err(e) = server::serve(endpoints, samples, port) {
                eprintln!("solbench serve failed: {e}");
                std::process::exit(1);
            }
        }
        Command::Demo => {
            let mut rec = LatencyRecorder::new();
            for ns in [
                820_000u64, 910_000, 1_050_000, 1_200_000, 3_400_000, 1_100_000, 950_000,
            ] {
                rec.record_ns(ns);
            }
            match rec.summary() {
                Some(summary) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&summary).expect("serializes")
                    );
                }
                None => println!("no samples"),
            }
        }
    }
}
