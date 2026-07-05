# solbench

[![CI](https://github.com/rpc-edge/solbench/actions/workflows/ci.yml/badge.svg)](https://github.com/rpc-edge/solbench/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org)

**Measure what your Solana RPC/gRPC actually costs you — from your region, reproducibly.**
Provider-neutral latency benchmarking as a single self-hostable Rust binary, plus a tested
measurement library you can build on.

Most "which Solana RPC is fastest?" answers are marketing or one-off scripts that report
averages and hide the tail. solbench measures the distribution — including **jitter**
(consistency), which for trading matters as much as the median — and is honest about what a
number does and doesn't mean.

## Example output

```text
$ solbench probe
endpoint         host                          p50ms    p99ms   jitter      max      ok         slot
your-edge        rpc.example.com                11.20    18.44     2.10    19.01   20/20    431009810
public mainnet   api.mainnet-beta.solana.com    38.30   246.25    41.70   251.10   20/20    431009812

jitter = stddev (consistency); lower is steadier.
```

Or run `solbench serve` for a live localhost dashboard, or `solbench probe --json` for raw
per-sample data (CI regression / reproducible runs).

## What it measures — and what it does *not* (yet)

| Metric | Status | Note |
|---|---|---|
| Read latency (`getSlot` round-trip): p50/p90/p99/**p99.9**, jitter, max | ✅ now | **network-inclusive** — dominated by distance from *your host* to the endpoint |
| Slot freshness / lag (slots behind the leading endpoint) | ✅ now | relative, from the same host |
| Transaction landing rate + time-to-land | ⏳ roadmap | the metric traders actually optimize |
| Yellowstone gRPC first-seen delta (concurrent two-endpoint race) | ⏳ roadmap | rpc edge's real differentiator |
| Per-method matrix (`getAccountInfo`, `getMultipleAccounts`, …) | ⏳ roadmap | |

**Honesty first (it's the whole point):** `getSlot` round-trip is *read latency from the host
running solbench*, dominated by network distance to the client. A globally-CDN'd public RPC will
look fast from a random machine. It is **not** a proxy for transaction-landing or shred/first-seen
latency, where co-located infrastructure wins. **Run solbench from your trading edge** for a
comparison that reflects the infrastructure, not your laptop's geography.

## Install

Not yet on crates.io. Install the binary from source (Rust stable):

```sh
cargo install --git https://github.com/rpc-edge/solbench solbench
```

Or clone and build:

```sh
git clone https://github.com/rpc-edge/solbench && cd solbench
cargo build --release   # ./target/release/solbench
```

## Usage

```sh
solbench probe                 # probe once, print a comparison table
solbench probe --samples 50    # more samples for tighter percentiles
solbench probe --json          # raw per-endpoint results as JSON
solbench serve                 # live dashboard at http://127.0.0.1:8787
solbench demo                  # measurement pipeline over synthetic data
```

By default solbench probes a public mainnet baseline. Add any authenticated endpoint via the
environment — the full URL (including `?api-key=`) is read at runtime and **never committed or
logged** (only the host is shown):

```sh
SOLBENCH_RPCEDGE_URL="https://rpc.rpcedge.com/?api-key=…" solbench probe
```

## Methodology & limitations

- **Distributions, not averages.** p50/p90/p99/p99.9 + stddev (jitter); the tail is what trading
  cares about.
- **Monotonic clocks** for every duration (no NTP skew).
- **Host-relative, same-vantage comparison.** All endpoints are probed from the same host at the
  same time, so shared network conditions are common to every row — but absolute numbers still
  include the RTT from *that host*. Report your measurement region when you publish a run.
- **Operator disclosure.** solbench is maintained by [rpc edge](https://rpcedge.com), a Solana
  infra provider that may appear in results. Endpoints are configured identically; the harness and
  raw JSON are open so anyone can reproduce. A non-reproducible score is a self-reported claim —
  run your own.
- **Known limits today:** read-latency only (no landing/first-seen yet); no open-loop load model or
  HDR histograms yet (see roadmap); public endpoints may rate-limit under high `--samples`.

## Roadmap

Ordered by how much they close the gap to "what traders actually trade on":

1. **Jitter/consistency + p99.9** as first-class output — *done*.
2. **Slot-lag / freshness** tracker per endpoint.
3. **Yellowstone gRPC first-seen** mode (concurrent two-endpoint race; never `blockTime`).
4. **Landing-rate `send`** command (on-chain inclusion, not `sendTransaction`-returned-success).
5. **Per-method latency matrix.**
6. Open-loop sampling + HDR histograms; crates.io publish + prebuilt binaries (cargo-dist).

## How it's built

A Cargo workspace. `solbench-core` is a standalone, **network-free, unit-tested** measurement
library (percentile stats + jitter, per-operation event timelines, landing-rate tracking) — the
same primitives are reused by downstream latency harnesses, so a benchmark and a bot publish
numbers from the same verifiable code.

```
solbench/
  crates/
    solbench-core/   # reusable measurement library (stats, timeline, landing)
    solbench-cli/    # the `solbench` binary (probe, serve, demo)
```

## Contributing

Issues and PRs welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md). CI enforces
`cargo fmt`, `cargo clippy -D warnings`, and `cargo test`.

## License

MIT — see [LICENSE](./LICENSE). Provider-neutral by design; the hosted leaderboard is operated by
[rpc edge](https://rpcedge.com).
