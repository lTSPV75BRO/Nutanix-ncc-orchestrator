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
  const candidates = [row.clusterName, row.cluster_name, row.cluster, row.cluster_ip, row.ip, row.address, row.cluster_uuid, row.clusterUUID];
  return pickPreferredClusterName(candidates) || "unknown-cluster";
}

function normalizeClusterKey(value: unknown): string {
  let raw = String(value || "").trim().toLowerCase();
  if (!raw || raw === "-" || raw === "<nil>") return "";
  raw = raw.replace(/^urn:uuid:/, "").replace(/^\{|\}$/g, "");
  raw = raw.replace(/^https?:\/\//, "");
  raw = raw.replace(/:\d+$/, "");
  raw = raw.replace(/\/+$/, "");
  return raw;
}

function isIPv4Like(value: string): boolean {
  return /^\d{1,3}(?:\.\d{1,3}){3}$/.test(value);
}

function isUUIDLike(value: string): boolean {
  const raw = value.trim().replace(/^urn:uuid:/i, "").replace(/[{}]/g, "");
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(raw) || /^[0-9a-f]{32}$/i.test(raw);
}

function pickPreferredClusterName(values: unknown[]): string {
  const options = values.map((v) => String(v || "").trim()).filter((v) => v && v !== "-" && v !== "<nil>");
  if (options.length === 0) return "";
  const named = options.find((v) => !isUUIDLike(v) && !isIPv4Like(normalizeClusterKey(v)));
  if (named) return named;
  const host = options.find((v) => !isUUIDLike(v));
  return host || options[0];
}

function addClusterPair(map: Map<string, string>, values: unknown[]) {
  const preferred = pickPreferredClusterName(values);
  if (!preferred) return;
  values.forEach((v) => {
    const key = normalizeClusterKey(v);
    if (!key) return;
    const existing = map.get(key);
    const existingWeak = !existing || isIPv4Like(normalizeClusterKey(existing)) || isUUIDLike(existing);
    const nextBetter = existingWeak && !isUUIDLike(preferred);
    if (!existing || nextBetter || (!isUUIDLike(preferred) && isUUIDLike(existing))) {
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
    addClusterPair(map, [
      row.clusterName,
      row.cluster_name,
      row.name,
      row.cluster,
      row.cluster_ip,
      row.ip,
      row.address,
      row.cluster_uuid,
      row.clusterUUID,
      displayClusterName(row),
    ]);

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

export function mergePCClusterIdentityMap(
  base: Record<string, string>,
  clusterMap?: Record<string, { name?: string; address?: string; ext_id?: string }>,
): Record<string, string> {
  if (!clusterMap) return base;
  const out = { ...base };
  for (const ident of Object.values(clusterMap)) {
    const named = String(ident.name || "").trim();
    const address = String(ident.address || "").trim();
    const name = (named && !isUUIDLike(named) ? named : "") || (address && !isUUIDLike(address) ? address : "") || named || address;
    if (!name) continue;
    for (const key of [ident.ext_id, ident.address, ident.name]) {
      const normalized = normalizeClusterKey(key);
      if (normalized) out[normalized] = name;
    }
  }
  return out;
}

export function nccDispositionKey(cluster: string, check: string): string {
  return `${cluster.trim().toLowerCase()}|${check.trim().toLowerCase()}`;
}

export function resolveClusterName(value: unknown, clusterNameMap: Record<string, string>): string {
  const raw = String(value || "").trim();
  if (!raw || raw === "-") return "-";
  const direct = clusterNameMap[normalizeClusterKey(raw)];
  if (direct && !isUUIDLike(direct)) return direct;
  if (direct && isUUIDLike(raw)) return direct;
  if (isUUIDLike(raw)) return raw;
  if (!isIPv4Like(normalizeClusterKey(raw))) return raw;
  return raw;
}
