# Security

## Handling of credentials

Transaction-stream configs contain environment-variable names only. Never commit resolved endpoints with userinfo/query credentials, tokens, `.env` files, full environment dumps, raw transaction payloads, or unreviewed artifact directories. Public bundles must pass checksum verification and a credential scan before review.

solbench probes RPC endpoints that may carry an API key in the URL (e.g.
`https://rpc.rpcedge.com/?api-key=…`). By design:

- The endpoint URL is **read from the environment at runtime** (`SOLBENCH_RPCEDGE_URL`) — it is
  never read from, or written to, a committed file.
- Only the **host** is ever printed or rendered (in the CLI table, the dashboard, and JSON output).
  The API key / query string is stripped and never logged, displayed, or transmitted anywhere except
  to the endpoint you point it at.
- solbench makes **no outbound requests** other than to the endpoints you configure.

When you self-host the `serve` dashboard, it binds to `127.0.0.1` (localhost) only.

## Dependency audit

CI runs `cargo audit` on every PR and weekly (`audit.yml`). Known **transitive**
advisories from optional `solana-sdk` (`send` feature) are documented in
[`.cargo/audit.toml`](./.cargo/audit.toml) with upgrade notes. Default and `grpc`
builds do not pull those crates.

## Reporting a vulnerability

Please report suspected vulnerabilities privately via GitHub Security Advisories
("Report a vulnerability" on the repo's Security tab), or to `hello@rpcedge.com`. Please do not open
a public issue for security reports. We aim to acknowledge within a few business days.
