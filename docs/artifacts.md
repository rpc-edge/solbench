# Transaction stream artifacts

Every attempt creates an immutable directory immediately. Completed and aborted attempts retain:

- `manifest.json` — status, failure reason, target, profile, measurement host, redacted endpoint hosts/IPs;
- `config.redacted.toml` — environment-variable names, never resolved values;
- `observations.ndjson` and `matched-events.ndjson`;
- `source-health.json` and `checksums.sha256`;
- offline-generated `summary.*` and `report.*` files.

`stream verify` validates checksums, schemas, NDJSON and completion eligibility. `stream report` uses only local artifacts. `--public-output` omits raw observations and compresses matched evidence.

Never store credentials, auth-bearing URLs, full environment dumps, transaction payloads, or private operational notes in artifacts.
