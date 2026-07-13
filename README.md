# solbench

[![CI](https://github.com/rpc-edge/solbench/actions/workflows/ci.yml/badge.svg)](https://github.com/rpc-edge/solbench/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org)

**Measure what your Solana RPC/gRPC actually costs you — from your region, reproducibly.**
Provider-neutral latency benchmarking as a single self-hostable Rust binary, plus a tested
measurement library you can build on.

Maintained by [rpc edge](https://rpcedge.com) - co-located Solana RPC, Yellowstone gRPC, decoded
shreds, and a transaction sender. The harness stays open and fair; the product is optional.
[Self-serve access →](https://app.rpcedge.com/signup) · [Published reports →](https://rpcedge.com/benchmarks)

## Bounded transaction-stream races

Compare normal Yellowstone processed transactions and `SubscribeDeshred` concurrently across an arbitrary N-way source list:

```bash
# Validate config (no network, any feature set)
cargo run --release -- stream check-config --config examples/stream-pump-amm-rpcedge-triton.toml

cargo run --release --features grpc -- stream run --config examples/stream-pump-amm-rpcedge-triton.toml
cargo run --release --features grpc -- stream verify --artifact-dir artifacts/<attempt-id>
# Public bundles use matched-events.ndjson.zst — verify accepts plain or .zst
env -u SOLBENCH_RPCEDGE_GRPC_TOKEN -u SOLBENCH_TRITON_GRPC_TOKEN \
  cargo run --release --features grpc -- stream report --artifact-dir artifacts/<attempt-id>
```

The publication profile is `pump_amm_transactions_v1`, with 50,000 matched observations, a 30-second grace, and no automatic retry. See [methodology](docs/stream-methodology.md), [artifacts](docs/artifacts.md), and [publishing](docs/publishing.md). Independent Thorofare and GeyserBench attempts are validation evidence, not hidden inputs to the primary result.

Most "which Solana RPC is fastest?" answers are marketing or one-off scripts that report
averages and hide the tail. solbench measures the distribution - including **jitter**
(consistency), which for trading matters as much as the median - and is honest about what a
number does and doesn't mean.

Default builds stay lean (`probe` / `serve` / `report` / offline stream report). Full streaming
and send metrics need features:

```sh
cargo build --release --features "grpc,send"
# GitHub Release binaries already include grpc + send
```

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
| Open-loop sampling (no coordinated omission) | ✅ now | each tick sends independently; a slow reply never hides the next sample |
| Slot freshness / lag (slots behind the leading endpoint) | ✅ now | tick-aligned, fair same-moment comparison |
| Transaction landing rate + slots-to-land | ✅ `--features send` | actual on-chain inclusion (not `sendTransaction`-returned-success) |
| Yellowstone gRPC first-seen delta (concurrent two-endpoint race) | ✅ `--features grpc` | the metric that reflects co-located infra; never `blockTime` |
| Per-method matrix (`getAccountInfo`, `getMultipleAccounts`, …) | ⏳ roadmap | |

**Honesty first (it's the whole point):** `getSlot` round-trip is *read latency from the host
running solbench*, dominated by network distance to the client. A globally-CDN'd public RPC will
look fast from a random machine. It is **not** a proxy for transaction-landing or shred/first-seen
latency, where co-located infrastructure wins. **Run solbench from your trading edge** for a
comparison that reflects the infrastructure, not your laptop's geography.

## Install

Not yet on crates.io. Prefer a **GitHub Release** binary (when tagged `v*`), or build from source (Rust stable):

```sh
# From source (always works)
cargo install --git https://github.com/rpc-edge/solbench solbench

# Or clone and build
git clone https://github.com/rpc-edge/solbench && cd solbench
cargo build --release   # ./target/release/solbench
```

Tagged releases attach linux/macOS tarballs via `.github/workflows/release.yml`.
Linux and Apple Silicon ship with full `grpc`+`send`. Intel macOS ships the lean default
binary (probe/report); build with `--features "grpc,send"` from source for stream races.

### Which repo for which benchmark?

| Measurement | Tool |
|---|---|
| Read latency, slot-lag, gRPC first-seen, stream races (deshred vs processed) | **this repo (`solbench`)** |
| Transaction submit → observe, leader-paced route A/B, QUIC relay | [`solana-tx-bench`](https://github.com/rpc-edge/solana-tx-bench) |

When you publish a report, set `repositoryUrl` to the harness that produced it. Do not substitute.

### Hosted leaderboard feed

`solbench report` emits stable JSON a host can render (schema used by
[rpcedge.com/benchmarks/live](https://rpcedge.com/benchmarks/live) among others). How you publish
that file is up to you - this repo only produces the measurement.

```sh
# Co-located host preferred — geography dominates absolute read latency
# Auth-bearing URLs stay in the environment; never commit them
SOLBENCH_RPCEDGE_URL="https://YOUR_RPC_HOST/?api-key=…" \
  solbench report --region "your-region · your-facility" --window "rolling 24h" \
  > run.json

# Add any rival the same way
solbench report --region "your-region" \
  --provider "other=https://…" \
  > run.json
```

Label the region honestly. Laptop geography is not co-located infrastructure.

## Usage

```sh
solbench probe                   # read latency + jitter + slot-lag, one table
solbench probe --samples 50      # more samples for tighter percentiles
solbench probe --interval-ms 150 # open-loop tick cadence (>= typical RTT)
solbench probe --json            # raw per-endpoint results as JSON
solbench serve                   # live dashboard at http://127.0.0.1:8787
solbench report --region "…"     # leaderboard-shaped JSON for a hosted board
solbench demo                    # measurement pipeline over synthetic data
```

`report` fills read latency + slot-lag per provider; `firstSeenP50` / `landingRate` come from
separate `grpc` / `send` runs. Add rivals with `--provider "Name=https://…"`.

**Transaction landing** (opt-in, pulls `solana-sdk`) — measures actual on-chain inclusion.
DEVNET-first; point it at mainnet only with your own funded keypair on your own host (never CI):

```sh
cargo build --release --features send
SOLBENCH_KEYPAIR=~/devnet.json \
  solbench send --url https://api.devnet.solana.com --count 10
```

**gRPC first-seen** (opt-in, pulls `yellowstone-grpc`) — races two Yellowstone endpoints on which
sees each `(slot, status)` event first, and by how much. This is the streaming metric that reflects
co-located infra (a relative two-endpoint race — never `receive_time - blockTime`):

```sh
cargo build --release --features grpc
SOLBENCH_GRPC_A=https://grpc.example.com:443 SOLBENCH_GRPC_A_TOKEN=… \
SOLBENCH_GRPC_B=https://other.example.com:443 SOLBENCH_GRPC_B_TOKEN=… \
  solbench grpc --slots 200
```

By default solbench probes a public mainnet baseline. Add any authenticated endpoint via the
environment — the full URL (including `?api-key=`) is read at runtime and **never committed or
logged** (only the host is shown):

```sh
SOLBENCH_RPCEDGE_URL="https://YOUR_RPC_HOST/?api-key=…" solbench probe
```
## Methodology & limitations

- **Distributions, not averages.** p50/p90/p99/p99.9 + stddev (jitter); the tail is what trading
  cares about.
- **Open-loop issue rate.** Each sample tick fires its own request on a fixed schedule, so a slow
  reply never delays (and never hides) the next sample. Latency is **send→reply RTT** for that
  request (not a synthetic intended-start clock).
- **Monotonic clocks** for every duration (no NTP skew).
- **Host-relative, same-vantage comparison.** All endpoints are probed from the same host on the
  same tick schedule, so shared network conditions are common to every row — but absolute numbers
  still include the RTT from *that host*. Report your measurement region when you publish a run.
- **On-chain inclusion for landing.** `send` waits for `getSignatureStatuses` to confirm, never
  treating a `sendTransaction` success as "landed."
- **Operator disclosure.** solbench is maintained by [rpc edge](https://rpcedge.com), a Solana
  infra provider that may appear in results. Endpoints are configured identically; the harness and
  raw JSON are open so anyone can reproduce. A non-reproducible score is a self-reported claim -
  run your own.
- **Known limits today:** `probe` read-latency is network-inclusive (run co-located, or use `grpc`/
  `send`, to reflect infra); exact percentiles (no HDR histogram yet); public endpoints may
  rate-limit under high `--samples`.

## Soft commercial note

This tool is free and MIT-licensed. You do not need an rpc edge account to use it.

If you are shopping for co-located Solana infrastructure and want the same desk that maintains
this harness:

| | |
|---|---|
| Product | [rpcedge.com](https://rpcedge.com) |
| Self-serve (API keys, plans) | [app.rpcedge.com/signup](https://app.rpcedge.com/signup) |
| Docs | [docs.rpcedge.com](https://docs.rpcedge.com) |
| Measure us like anyone else | `SOLBENCH_RPCEDGE_URL="https://rpc.rpcedge.com/?api-key=…" solbench probe` |
| Evidence board | [rpcedge.com/benchmarks](https://rpcedge.com/benchmarks) |

No preferred treatment in the code path. Your endpoints, your host, your numbers.

## Roadmap

Ordered by how much they close the gap to "what traders actually trade on":

1. **Jitter/consistency + p99.9** as first-class output — ✅ done.
2. **Open-loop sampling** (no coordinated omission) — ✅ done.
3. **Slot-lag / freshness** tracker per endpoint — ✅ done.
4. **Landing-rate `send`** (on-chain inclusion) — ✅ done (`--features send`).
5. **Yellowstone gRPC first-seen** (concurrent two-endpoint race; never `blockTime`) — ✅ done
   (`--features grpc`).
6. **Per-method latency matrix**; HDR histograms; crates.io publish + prebuilt binaries (cargo-dist).

## How it's built

A Cargo workspace. `solbench-core` is a standalone, **network-free, unit-tested** measurement
library (percentile stats + jitter, per-operation event timelines, landing-rate tracking) — the
same primitives are reused by downstream latency harnesses, so a benchmark and a bot publish
numbers from the same verifiable code.

```
solbench/
  crates/
    solbench-core/   # network-free measurement library (stats, stream match, landing)
    solbench-cli/    # binary: probe, serve, report, stream, grpc, send, demo
```

## Contributing

Issues and PRs welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md). CI enforces
`cargo fmt`, `cargo clippy -D warnings`, and `cargo test`.

## License

MIT - see [LICENSE](./LICENSE). Provider-neutral by design.

Built by [rpc edge](https://rpcedge.com). Prefer numbers over claims - then
[try the stack](https://app.rpcedge.com/signup) if the measurements fit your desk.
