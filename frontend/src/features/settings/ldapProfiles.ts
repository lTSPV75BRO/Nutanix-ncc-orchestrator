// Form values and API payloads for Settings → Access → Sign-in → LDAP
// (the default directory plus named directory profiles).
import type { LDAPConfig, LDAPProfile, LDAPProfileInput } from "../../api/types";

// The form keeps each stored profile's name as previous_name so a rename
// carries the profile's write-only secrets over on the server.
export type LDAPProfileFormValues = Partial<LDAPProfile> & {
  previous_name?: string;
  bind_password?: string;
  ca_cert_pem?: string;
};

export const NEW_LDAP_PROFILE: LDAPProfileFormValues = {
  name: "",
  upn_suffixes: [],
  netbios_names: [],
  url: "",
  base_dn: "",
  start_tls: false,
  insecure_skip_verify: false,
  default_role: "viewer",
};

export const textList = (v: unknown): string[] =>
  Array.isArray(v) ? v.map((x) => String(x).trim()).filter(Boolean) : [];

// Form values for everything except the default directory's write-only
// secrets, which the caller seeds once (a refetch must not wipe what the
// admin is typing).
export function ldapFormValues(cfg?: LDAPConfig) {
  return {
    enabled: cfg?.enabled ?? false,
    url: cfg?.url ?? "",
    start_tls: cfg?.start_tls ?? false,
    insecure_skip_verify: cfg?.insecure_skip_verify ?? false,
    bind_dn: cfg?.bind_dn ?? "",
    base_dn: cfg?.base_dn ?? "",
    user_filter: cfg?.user_filter ?? "",
    username_attribute: cfg?.username_attribute ?? "",
    group_attribute: cfg?.group_attribute ?? "",
    role_map: cfg?.role_map ?? "",
    default_role: cfg?.default_role ?? "viewer",
    upn_suffixes: cfg?.upn_suffixes ?? [],
    netbios_names: cfg?.netbios_names ?? [],
    profiles: (cfg?.profiles ?? []).map(
      (p): LDAPProfileFormValues => ({ ...p, previous_name: p.name, bind_password: "", ca_cert_pem: "" }),
    ),
  };
}

// profilePayload turns one profile's form values into the API shape. Blank
// secrets are omitted so the server keeps the stored ones.
export function ldapProfilePayload(p: LDAPProfileFormValues): LDAPProfileInput {
  const name = String(p.name ?? "").trim();
  const prev = String(p.previous_name ?? "").trim();
  const bind = String(p.bind_password ?? "");
  const ca = String(p.ca_cert_pem ?? "").trim();
  return {
    name,
    ...(prev && prev !== name ? { previous_name: prev } : {}),
    upn_suffixes: textList(p.upn_suffixes),
    netbios_names: textList(p.netbios_names),
    url: String(p.url ?? "").trim(),
    start_tls: Boolean(p.start_tls),
    insecure_skip_verify: Boolean(p.insecure_skip_verify),
    bind_dn: String(p.bind_dn ?? "").trim(),
    base_dn: String(p.base_dn ?? "").trim(),
    user_filter: String(p.user_filter ?? "").trim(),
    group_attribute: String(p.group_attribute ?? "").trim(),
    role_map: String(p.role_map ?? "").trim(),
    default_role: String(p.default_role ?? "viewer"),
    ...(bind ? { bind_password: bind } : {}),
    ...(ca ? { ca_cert_pem: ca } : {}),
  };
}
