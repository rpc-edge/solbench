# RPCEdge deshred lifecycle benchmark: stream-20260711T075608Z

- Scope: **rpcedge_operator_lifecycle**
- Eligible: **true**
- Matched signatures: **55424 / 50000**
- Profile: `pump_amm_transactions_v1`
- Measurement host: **Frankfurt, Germany**, `185.191.118.181`, Cherry Servers Frankfurt (RPCEdge operator host)

This is an RPCEdge operator-host lifecycle measurement, not a neutral provider ranking. It measures how much earlier the same transaction signature is delivered through `SubscribeDeshred` than through normal processed Yellowstone gRPC. Client monotonic arrival is authoritative. Provider `created_at` is diagnostic only.

## Result

`SubscribeDeshred` arrived first for **99.924%** of paired signatures (55382/55424), with a **4.640 ms median advantage**. At p95 and p99 of the signed distribution it remained **1.615 ms** and **1.092 ms** earlier, respectively.

## Paired client-arrival delta

| A | B | comparable | A first | B first | p50 A-B (ms) | p90 | p95 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| rpcedge_deshred | rpcedge_processed | 55424 | 55382 | 42 | -4.640 | -1.935 | -1.615 | -1.092 | -0.124 |

## Source health

- **rpcedge_deshred**: 55476 messages, 47 duplicates, 0 disconnects, 0 errors
- **rpcedge_processed**: 102927 messages, 64 duplicates, 0 disconnects, 0 errors

## Interpretation boundary

This report compares two lifecycle boundaries operated by RPCEdge. It makes no claim about other RPC providers. Reproduce the methodology against infrastructure you control.
