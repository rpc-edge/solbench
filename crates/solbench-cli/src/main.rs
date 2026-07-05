//! `solbench` CLI.
//!
//! Early scaffold: the measurement foundation (`solbench-core`) is in place and
//! unit-tested; live endpoint probing is not wired up yet. The `demo` subcommand
//! exercises the core so the pipeline is runnable end to end today.

use clap::{Parser, Subcommand};
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
    /// Probe a set of endpoints (not implemented yet).
    Probe,
    /// Run the measurement pipeline over synthetic samples (demonstrates solbench-core).
    Demo,
}

fn main() {
    match Cli::parse().command {
        Command::Probe => {
            eprintln!("solbench probe: live endpoint probing is not implemented yet.");
            eprintln!("The measurement core (solbench-core) is ready; network probes are next.");
            std::process::exit(2);
        }
        Command::Demo => {
            // Synthetic first-seen latencies (ns) to show the summary pipeline.
            let mut rec = LatencyRecorder::new();
            for ns in [
                820_000u64, 910_000, 1_050_000, 1_200_000, 3_400_000, 1_100_000, 950_000,
            ] {
                rec.record_ns(ns);
            }
            match rec.summary() {
                Some(summary) => {
                    let json = serde_json::to_string_pretty(&summary).expect("summary serializes");
                    println!("{json}");
                }
                None => println!("no samples"),
            }
        }
    }
}
