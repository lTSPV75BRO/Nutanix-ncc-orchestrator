type AnyRecord = Record<string, unknown>;

export function asRecord(v: unknown): AnyRecord {
  if (!v || typeof v !== "object" || Array.isArray(v)) return {};
  return v as AnyRecord;
}

export function asArray(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

export function toNumber(v: unknown, fallback = 0): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const parsed = Number(v);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
}

export function displayClusterName(row: AnyRecord): string {
  const candidates = [row.clusterName, row.cluster_name, row.cluster, row.cluster_ip, row.ip, row.address];
  for (const c of candidates) {
    if (typeof c === "string" && c.trim() && c.trim() !== "-") return c.trim();
  }
  return "unknown-cluster";
}

function normalizeClusterKey(value: unknown): string {
  let raw = String(value || "").trim().toLowerCase();
  if (!raw || raw === "-") return "";
  raw = raw.replace(/^https?:\/\//, "");
  raw = raw.replace(/:\d+$/, "");
  raw = raw.replace(/\/+$/, "");
  return raw;
}

function isIPv4Like(value: string): boolean {
  return /^\d{1,3}(?:\.\d{1,3}){3}$/.test(value);
}

function pickPreferredClusterName(values: unknown[]): string {
  const options = values.map((v) => String(v || "").trim()).filter((v) => v && v !== "-");
  if (options.length === 0) return "";
  const preferred = options.find((v) => !isIPv4Like(normalizeClusterKey(v)));
  return preferred || options[0];
}

function addClusterPair(map: Map<string, string>, values: unknown[]) {
  const preferred = pickPreferredClusterName(values);
  if (!preferred) return;
  values.forEach((v) => {
    const key = normalizeClusterKey(v);
    if (!key) return;
    const existing = map.get(key);
    if (!existing || isIPv4Like(normalizeClusterKey(existing))) {
      map.set(key, preferred);
    }
  });
}

type ClusterMapInput = {
  runSummary?: unknown;
  checksSnapshot?: unknown;
  aggRows?: unknown[];
  drilldownDiff?: unknown;
  flakyChecks?: unknown;
  sloDashboard?: unknown;
  regressionSummary?: unknown;
};

export function buildClusterNameMap(input: ClusterMapInput): Record<string, string> {
  const map = new Map<string, string>();
  const addFromRow = (row: AnyRecord) =>
    addClusterPair(map, [row.clusterName, row.cluster_name, row.name, row.cluster, row.cluster_ip, row.ip, row.address, displayClusterName(row)]);

  asArray(asRecord(input.runSummary).clusters)
    .map((c) => asRecord(c))
    .forEach(addFromRow);

  (input.aggRows || []).map((r) => asRecord(r)).forEach(addFromRow);

  const snapshot = asRecord(input.checksSnapshot);
  asArray(snapshot.clusters)
    .map((c) => asRecord(c))
    .forEach((c) => {
      addFromRow(c);
      asArray(c.checks)
        .map((chk) => asRecord(chk))
        .forEach((chk) => addFromRow({ ...chk, cluster: c.cluster || c.address, clusterName: c.clusterName || c.cluster_name }));
    });
  asArray(input.checksSnapshot).map((r) => asRecord(r)).forEach(addFromRow);

  asArray(asRecord(input.drilldownDiff).clusters)
    .map((c) => asRecord(c))
    .forEach(addFromRow);

  asArray(asRecord(input.flakyChecks).checks)
    .map((c) => asRecord(c))
    .forEach(addFromRow);

  asArray(asRecord(input.sloDashboard).clusters)
    .map((c) => asRecord(c))
    .forEach(addFromRow);

  const reg = asRecord(input.regressionSummary);
  ["increased_clusters", "decreased_clusters", "unchanged_clusters"].forEach((k) => {
    asArray(reg[k]).forEach((v) => addClusterPair(map, [v]));
  });

  return Object.fromEntries(map.entries());
}

export function resolveClusterName(value: unknown, clusterNameMap: Record<string, string>): string {
  const raw = String(value || "").trim();
  if (!raw || raw === "-") return "-";
  const direct = clusterNameMap[normalizeClusterKey(raw)];
  if (direct) return direct;
  if (!isIPv4Like(normalizeClusterKey(raw))) return raw;
  return raw;
}
