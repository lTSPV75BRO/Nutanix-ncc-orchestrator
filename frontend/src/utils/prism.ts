// Where a cluster's Prism lives. A cluster is identified by its address: an IPv4
// address or a DNS name. Names without a dot ("dev-lab-01") and UUIDs are display
// names or ids, not addresses, so they get no link.

const IPV4 = /^\d{1,3}(?:\.\d{1,3}){3}$/;
const FQDN = /^(?=.{4,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z][a-z0-9-]{0,61}[a-z0-9]$/i;

/** The host part of a cluster address: no scheme, path or port. */
export function cleanClusterHost(value: string): string {
  return value
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/\/.*$/, "")
    .replace(/:\d+$/, "");
}

/** True for an IPv4 address or a fully qualified DNS name. */
export function isClusterAddress(value: string): boolean {
  const host = cleanClusterHost(value);
  return IPV4.test(host) || FQDN.test(host);
}

/** https://<host>:9440 for an address, or "" when value is not one. */
export function prismURLFor(value: string): string {
  const host = cleanClusterHost(value);
  return IPV4.test(host) || FQDN.test(host) ? `https://${host}:9440` : "";
}
