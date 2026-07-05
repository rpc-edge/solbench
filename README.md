# solbench

Continuous, provider-neutral benchmarking for Solana infrastructure: transaction
landing rate under congestion, p50/p99 first-seen delta, slot lag, and Yellowstone
gRPC stream freshness. Single Rust binary, self-hostable, with a public leaderboard.

Most "which Solana RPC is fastest?" answers are marketing or one-off scripts. solbench
is the neutral tool that measures it continuously and publishes the numbers, so the
claim rests on data anyone can reproduce.

## Status

Early but runnable. The measurement **foundation** and a **read-latency probe + local
dashboard** work today; the metrics that actually reflect co-located infra (landing,
shred first-seen) are the next milestone.

- [x] `solbench-core` — latency/percentile stats, per-operation event timelines, and
      landing-rate tracking. Network-free, pure, tested.
- [x] RPC read-latency probe (`probe`) + live local dashboard (`serve`).
- [ ] Transaction-landing latency/rate (submit → slots-to-land).
- [ ] Yellowstone gRPC first-seen / stream-freshness probe.
- [ ] Continuous scheduler + report output; hosted leaderboard.

## What it measures (and what it doesn't, yet)

The `probe`/`serve` commands currently measure **`getSlot` read-latency from the host
running solbench**. That number is dominated by network distance from *that host* to the
endpoint — so a globally-CDN'd public RPC will look fast from a random machine. It is
**not** a proxy for transaction-landing or shred/first-seen latency, which is where
co-located infrastructure wins. Run solbench **from your trading edge** for a comparison
that reflects the infrastructure, not your laptop's geography. Landing + first-seen probes
(the metrics that make the real case) are the next milestone.

## Why a workspace

`solbench-core` is a standalone library, not CLI-internal, on purpose: the same
measurement primitives are reused by downstream latency harnesses (e.g. an on-chain
market-maker that reports quote-to-ack and cancel latency). Whatever a benchmark or a
bot publishes rests on the same verifiable measurement code.

```
solbench/
  crates/
    solbench-core/   # reusable measurement library (stats, timeline, landing)
    solbench-cli/    # the `solbench` binary
```

## Build

```sh
cargo test                              # run the core test suite
cargo run -p solbench -- demo           # measurement pipeline on synthetic data
cargo run -p solbench -- probe          # probe endpoints once, print a comparison
cargo run -p solbench -- serve          # live dashboard at http://127.0.0.1:8787
```

Set `SOLBENCH_RPCEDGE_URL` (full URL incl. `?api-key=`) to include an rpc edge endpoint
in the comparison — read from the environment at runtime, never committed:

```sh
SOLBENCH_RPCEDGE_URL="https://rpc.rpcedge.com/?api-key=…" cargo run -p solbench -- probe
```

## License

MIT. Provider-neutral by design; the hosted leaderboard is operated by
[rpc edge](https://rpcedge.com).
