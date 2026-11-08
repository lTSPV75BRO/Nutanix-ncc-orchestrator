# Connect your clusters

NCC Orchestrator runs NCC on Prism Element clusters. You either list the
clusters yourself or let it find them through Prism Central. Everything here
is on **Settings → Clusters & config** (admin), in the **Clusters** and
**Sign-in & connection** sections. **Find a setting** at the top jumps to any
field.

## 1. Choose the clusters

**Find clusters from:**

- **A list of clusters:** type each Prism Element IP address or host name in
  **Clusters** and press Enter. For many clusters, use a **Clusters file** with
  one per line (`cluster`, `cluster,user` or `cluster,user,password`):

  ```text
  # cluster[,username[,password]]
  10.38.66.37
  10.38.66.7,admin
  pe-site2.example.local,svc-ncc,secret://site2_password
  ```

- **Prism Central discovery:** enter your **Prism Centrals** (or one **Prism
  Central URL**, such as `https://pc.example.com:9440`). Every cluster
  registered there is checked, and new clusters are picked up on the next run.
  Leave **Discovery API** on `v4` unless your Prism Central is too old for it.

To see what discovery finds before a run:

```bash
ncc-orchestrator discover-clusters --prism-central-url https://pc.example.com:9440 \
  --username admin --password "$NCC_PASSWORD" --format table
```

## 2. Sign in to Prism

- **Username:** a Prism account that can run NCC on the clusters. Use a
  dedicated service account so its activity is easy to recognise in Prism's
  audit log. Different credentials per cluster go in the clusters file.
- **Password:** don't type the password itself. Use a reference such as
  `secret://NCC_PASSWORD` and set `NCC_PASSWORD` as an environment variable of
  the service. The page offers to convert a plain password for you. See
  [Secrets](INTEGRATIONS.md#secrets-from-the-environment-or-a-file).

## 3. Certificates

Prism uses a self-signed certificate unless you replaced it. Pick one:

| Setting | When |
| --- | --- |
| **Trusted CA file** | Prism certificates come from your internal CA. Point it at the CA's PEM file. Best option. |
| **Pinned certificates** | Self-signed Prism certificates you want to accept exactly. Paste each SHA-256 fingerprint. |
| **Skip certificate checks** | Lab only. Accepts any certificate. |

Get a cluster's fingerprint with:

```bash
openssl s_client -connect 10.38.66.37:9440 </dev/null 2>/dev/null | openssl x509 -noout -fingerprint -sha256
```

## 4. Save and try it

1. **Save** (Ctrl+S / ⌘S). The config is checked before it is written.
2. Go to **Runs → New run**. The drawer runs a **preflight check** first: it
   tries to reach and sign in to every cluster and shows which ones fail and
   why.
3. Start the run. Results appear on **Overview** and **Findings** as each
   cluster finishes.

## Prism Central alerts

With Prism Centrals configured, **Findings** can also show Prism Central's
own alerts next to NCC results. Turn it on or off in **Settings → Features**
(Prism Central alerts).

## Environments (prod, lab, …)

If groups of clusters need different settings (for example production with a
longer timeout and a lab with certificate checks off), create an environment
per group: **Settings → Clusters & config → Environments**. An environment
holds only the run settings that differ; everything else comes from the main
config. Pick the environment in **New run**, and give each its own timetable
under **Settings → Schedule → Environment schedules**. Results from all
environments share one history and can be filtered by environment.

Details: [`FEATURES_AND_CONFIG_FLAGS.md` §2.13a](../FEATURES_AND_CONFIG_FLAGS.md#213a-environments-dev--staging--prod).

## Troubleshooting

| Problem | What to check |
| --- | --- |
| `x509: certificate signed by unknown authority` | Set **Trusted CA file** or **Pinned certificates** (above). |
| `401 Unauthorized` | Wrong username or password, or the `secret://` variable isn't set for the service. **Runs → New run** preflight names the cluster. |
| `secret "…" not found` | The referenced environment variable or secrets-file entry is missing. |
| Discovery finds no clusters | Check the Prism Central URL includes `:9440` and the account can see the clusters in Prism Central. |
| `429 Too Many Requests` | Prism is rate-limiting; the runner backs off automatically. Lower **Clusters at once** under **Run behavior** if it keeps happening. |

More: [`TROUBLESHOOTING.md`](../TROUBLESHOOTING.md).
