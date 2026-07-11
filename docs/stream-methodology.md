# Transaction stream benchmark methodology

`solbench stream` performs a bounded, concurrent transaction-signature race. It is separate from the lightweight always-on internal `stream-latency-compare` canary and from the existing slot/status `solbench grpc` race.

The only v1 profile is `pump_amm_transactions_v1`: `vote=false` and account include `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`. A publication attempt targets 50,000 unique signatures observed by at least two sources, then retains a 30-second completion grace, with a 900-second wall limit. A source failure aborts the fresh attempt; there is no automatic retry or silent fallback.

The initial matrix is RPCEdge and Triton, each through customer-facing TLS endpoints, each using normal processed Yellowstone `Subscribe` and `SubscribeDeshred`. All four subscriptions run together on one measurement host. Local monotonic arrival establishes first-seen order. Provider `created_at` is retained as diagnostic evidence; source-to-client age is not exact geographical latency, and creation order is not claimed unless timestamp semantics and clock comparability are verified.

Reports disclose the measurement host public IP, region, and configured datacenter/facility. They show per-source first share and completeness plus signed pairwise distributions. There is deliberately no blended provider score.
