# Troubleshooting

Use this when runs fail before or during NCC checks.

## TLS / certificate errors

- **Symptom:** `tls: failed to verify certificate` or `x509: ... certificate is not standards compliant`.
- **Lab / self-signed:** Set `insecure-skip-verify: true` in config or pass `--insecure-skip-verify` on the CLI.
- **Production:** Fix Prism certificates (SANs, chain) and keep `insecure-skip-verify: false`.

## Prism Central and multiple clusters

- **Symptom:** `NCC-40023` or “invalid node IP(s)” for the cluster you expect.
- **Cause:** The v4 API lists **all** registered clusters (e.g. AOS + Prism Central). The orchestrator must match your `--clusters` address to the **correct** entity and send **CVM IPs** for that cluster.
- **Fix:** Use an address that matches the target cluster in Prism (PC VM IP for PC, external IP or CVM IP for AOS). Use `ncc-orchestrator discover-clusters` to list valid addresses.

## `--insecure-skip-verify` ignored

- Ensure you are not only setting it in a config file that is not loaded; or upgrade to **v1.0.0+**, which fixed a viper binding bug with `discover-clusters` subcommand flags overwriting root flags.

## Exit codes

| Code | Meaning |
|------|----------------|
| 0 | Success (all clusters completed without runner-level error) |
| 1 | Failure (e.g. all clusters failed, or other error) |
| 2 | Configuration / validation error |
| 3 | Partial success (some clusters OK, some failed) |

See **`outputfiles/run-summary.json`** for `exit_code` and per-cluster `clusters[]`.

## GitHub `--update` rate limits

- Set **`GITHUB_TOKEN`** in the environment for higher API rate limits when calling `GET /repos/.../releases/latest`.
