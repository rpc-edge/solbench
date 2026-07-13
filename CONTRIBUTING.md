# Contributing to solbench

Thanks for your interest. solbench aims to be a *credible, provider-neutral* benchmark — so
contributions that improve measurement fairness or add honestly-labeled metrics are especially
welcome.

## Development

```sh
cargo test --all                                    # unit tests
cargo fmt --all                                     # format
cargo clippy --all-targets --all-features -- -D warnings   # lint (warnings are errors)
cargo run -p solbench -- probe                      # try it
```

CI runs all of the above on Linux and macOS; please make sure they pass locally before opening a PR.

## Guidelines

- **Keep `solbench-core` network-free.** It's a pure, tested measurement library reused by other
  tools — network I/O belongs in the CLI (or a future dedicated crate), not in core.
- **Label every metric honestly.** State whether a number is network-inclusive or
  network-isolated, and what it does *not* mean. Read our stance in the README's *Methodology &
  limitations* section. We would rather ship a caveated metric than a misleading one.
- **Prefer distributions to averages** — percentiles + jitter, not means.
- Add tests for any new measurement logic in `solbench-core`.

## Reporting issues

Bugs, methodology critiques, and endpoint/metric requests are all welcome as issues. Security
reports: see [SECURITY.md](./SECURITY.md).

## Maintainer

solbench is maintained by [rpc edge](https://rpcedge.com). Product access is optional and separate
from contributing: [app.rpcedge.com/signup](https://app.rpcedge.com/signup).
