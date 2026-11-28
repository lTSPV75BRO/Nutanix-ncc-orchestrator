export const ROLE_OPTIONS = [
  { value: "admin", label: "admin — full access" },
  { value: "operator", label: "operator — trigger/cancel runs" },
  { value: "viewer", label: "viewer — read-only" },
];

// The built-in "admin" account's role is fixed and it cannot be deleted; the UI
// disables those controls to match the server-side enforcement.
export function isReservedAdmin(username?: string): boolean {
  return (username ?? "").trim().toLowerCase() === "admin";
}

export const RESERVED_ADMIN_HINT = "The built-in admin account's role is fixed and it cannot be deleted.";
