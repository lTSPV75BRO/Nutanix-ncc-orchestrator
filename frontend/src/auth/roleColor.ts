/** antd Tag colour for a role: admin gold, operator blue, viewer plain. */
export function roleColor(role?: string): string {
  return role === "admin" ? "gold" : role === "operator" ? "blue" : "default";
}
