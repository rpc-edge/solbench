# Publishing benchmark reports

Execution does not publish. A successful 50,000-signature attempt is only a candidate:

1. verify and render it offline with credentials unset;
2. run Thorofare and GeyserBench separately on the same host and closest supported filter;
3. attach human-authored `corroborates`, `diverges`, or `inconclusive` evidence;
4. review claims, source health, missingness, clock caveats, IP/datacenter disclosure, and secret scans;
5. copy only the reviewed public bundle into `docs/reports/<slug>/` and open a PR.

RPCEdge’s curated page is a second reviewed publication after the raw GitHub Pages URL is stable. Readers are encouraged to reproduce the benchmark against their own endpoints.
