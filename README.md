# solbench

Continuous, provider-neutral benchmarking for Solana infrastructure: transaction
landing rate under congestion, p50/p99 first-seen delta, slot lag, and Yellowstone
gRPC stream freshness. Single Rust binary, self-hostable, with a public leaderboard.

Most "which Solana RPC is fastest?" answers are marketing or one-off scripts. solbench
is the neutral tool that measures it continuously and publishes the numbers, so the
claim rests on data anyone can reproduce.

## Status

Early scaffold. The measurement **foundation** is done and unit-tested; live endpoint
probing is the next milestone.

- [x] `solbench-core` — latency/percentile stats, per-operation event timelines, and
      landing-rate tracking. Network-free, pure, tested.
- [ ] Endpoint probing (RPC / Yellowstone gRPC / relay).
- [ ] Continuous scheduler + report output.
- [ ] Self-hostable dashboard + hosted leaderboard.

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
cargo test          # run the core test suite
cargo run -p solbench -- demo    # exercise the measurement pipeline on synthetic data
cargo run -p solbench -- probe   # (not implemented yet)
```

## License

MIT. Provider-neutral by design; the hosted leaderboard is operated by
[rpc edge](https://rpcedge.com).
