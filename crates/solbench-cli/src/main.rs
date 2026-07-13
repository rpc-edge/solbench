//! `solbench` CLI.
//!
//! `probe` measures live RPC read-latency + slot-lag (open-loop, tick-aligned);
//! `serve` renders it as a local dashboard; `demo` exercises the measurement core.
//! `grpc` (feature `grpc`) races Yellowstone first-seen and `send` (feature `send`)
//! measures transaction landing — the metrics that reflect co-located infra.

mod grpc;
mod probe;
mod report;
mod send;
mod server;
mod stream;
mod util;

use clap::{Parser, Subcommand};
use probe::{endpoints_from_env, probe_all};
use solbench_core::LatencyRecorder;

#[derive(Parser)]
#[command(
    name = "solbench",
    version,
    about = "Provider-neutral Solana RPC/gRPC latency benchmark."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run, verify, and report bounded transaction-stream races.
    Stream {
        #[command(subcommand)]
        command: stream::StreamCommand,
    },
    /// Probe endpoints (read latency + slot-lag) and print a comparison.
    Probe {
        /// Samples per endpoint.
        #[arg(long, default_value_t = 20)]
        samples: usize,
        /// Milliseconds between sample ticks (open-loop schedule).
        #[arg(long, default_value_t = 100)]
        interval_ms: u64,
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
        /// Milliseconds between sample ticks.
        #[arg(long, default_value_t = 120)]
        interval_ms: u64,
    },
    /// Race two Yellowstone gRPC endpoints on slot first-seen (feature `grpc`).
    ///
    /// Endpoints come from SOLBENCH_GRPC_A / SOLBENCH_GRPC_B (`host:443` or URL);
    /// per-endpoint x-token from SOLBENCH_GRPC_A_TOKEN / _B_TOKEN.
    Grpc {
        /// Slot updates to observe before reporting.
        #[arg(long, default_value_t = 200)]
        slots: usize,
    },
    /// Measure transaction landing (submit -> on-chain inclusion) (feature `send`).
    ///
    /// DEVNET-first. rpc edge is mainnet-only, and mainnet keypairs should live on
    /// your own secure host, not in CI. Never commit a keypair.
    Send {
        /// Path to a keypair JSON file (else SOLBENCH_KEYPAIR).
        #[arg(long)]
        keypair: Option<String>,
        /// RPC URL to submit through (else SOLBENCH_SEND_URL; defaults to devnet).
        #[arg(long)]
        url: Option<String>,
        /// Number of transactions to send.
        #[arg(long, default_value_t = 10)]
        count: usize,
    },
    /// Emit a leaderboard-shaped JSON run (feeds rpcedge.com/benchmarks/live).
    Report {
        /// Measurement region label (else SOLBENCH_REGION).
        #[arg(long)]
        region: Option<String>,
        #[arg(long, default_value = "mainnet")]
        network: String,
        #[arg(long, default_value = "single run")]
        window: String,
        /// Extra provider to measure, "name=url" (repeatable).
        #[arg(long = "provider")]
        providers: Vec<String>,
        #[arg(long, default_value_t = 20)]
        samples: usize,
        #[arg(long, default_value_t = 100)]
        interval_ms: u64,
    },
    /// Run the measurement pipeline over synthetic samples (demonstrates solbench-core).
    Demo,
}

fn main() {
    match Cli::parse().command {
        Command::Stream { command } => {
            if let Err(error) = stream::execute(command) {
                eprintln!("solbench stream: {error:#}");
                std::process::exit(1);
            }
        }
        Command::Probe {
            samples,
            interval_ms,
            json,
        } => {
            let endpoints = endpoints_from_env();
            if !json && endpoints.iter().all(|e| e.label != "rpc edge") {
                eprintln!("note: set SOLBENCH_RPCEDGE_URL to include rpc edge in the comparison.");
            }
            let results = probe_all(&endpoints, samples, interval_ms);

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&results).expect("results serialize")
                );
                return;
            }

            let ms = |ns: u64| format!("{:.2}", ns as f64 / 1e6);
            println!(
                "{:<16} {:<26} {:>8} {:>8} {:>8} {:>7} {:>7} {:>12}",
                "endpoint", "host", "p50ms", "p99ms", "jitter", "lag", "ok", "slot"
            );
            for r in &results {
                let (p50, p99, jitter) = match &r.latency {
                    Some(l) => (ms(l.p50_ns), ms(l.p99_ns), ms(l.stddev_ns)),
                    None => ("-".into(), "-".into(), "-".into()),
                };
                let lag = r
                    .slot_lag
                    .as_ref()
                    .map(|s| format!("{:.1}", s.avg))
                    .unwrap_or_else(|| "-".into());
                println!(
                    "{:<16} {:<26} {:>8} {:>8} {:>8} {:>7} {:>7} {:>12}",
                    r.label,
                    r.host,
                    p50,
                    p99,
                    jitter,
                    lag,
                    format!("{}/{}", r.ok, r.samples),
                    r.current_slot
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "-".into()),
                );
            }
            eprintln!(
                "\njitter = stddev (consistency); lag = mean slots behind the leading endpoint.\n\
                 getSlot round-trip is read latency from THIS host - dominated by network distance\n\
                 to the client, NOT a proxy for tx-landing or shred first-seen latency. Use\n\
                 `solbench grpc` / `solbench send` (or run co-located) for the infra story."
            );
        }
        Command::Serve {
            port,
            samples,
            interval_ms,
        } => {
            let endpoints = endpoints_from_env();
            if let Err(e) = server::serve(endpoints, samples, interval_ms, port) {
                eprintln!("solbench serve failed: {e}");
                std::process::exit(1);
            }
        }
        Command::Grpc { slots } => {
            if let Err(e) = grpc::race(slots) {
                eprintln!("solbench grpc: {e}");
                std::process::exit(1);
            }
        }
        Command::Send {
            keypair,
            url,
            count,
        } => {
            if let Err(e) = send::run(keypair, url, count) {
                eprintln!("solbench send: {e}");
                std::process::exit(1);
            }
        }
        Command::Report {
            region,
            network,
            window,
            providers,
            samples,
            interval_ms,
        } => {
            if let Err(e) = report::run(region, network, window, providers, samples, interval_ms) {
                eprintln!("solbench report: {e}");
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
