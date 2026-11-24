import { Space, Tabs } from "antd";
import { useSearchParams } from "react-router";
import { ClusterOutlined, KeyOutlined, LoginOutlined, SafetyCertificateOutlined, UserOutlined } from "@ant-design/icons";
import { UsersCard, PasswordResetRequestsCard } from "./access/UsersCards";
import { ExternalAuthCard, SessionCard } from "./access/SignInCards";
import { ClusterGroupsCard } from "./access/ClusterGroupsCard";
import { TokensCard } from "./access/TokensCard";
import { TLSCard } from "./access/TLSCard";

type AccessView = "users" | "signin" | "groups" | "tokens" | "https";
const ACCESS_VIEWS: AccessView[] = ["users", "signin", "groups", "tokens", "https"];
/** Which tab holds the card a `?focus=` link (search, health fixes) points at. */
const FOCUS_VIEW: Record<string, AccessView> = { users: "users", ldap: "signin", sso: "signin", tls: "https" };

/**
 * Access, one concern per tab: who can sign in and with what role, how they
 * sign in, which clusters they see, API tokens, and HTTPS. The tab is kept in
 * the URL (?view=); old ?focus= links open the tab that holds that card.
 */
export function AccessSection({ isKubernetes = false }: { isKubernetes?: boolean }) {
  const [params, setParams] = useSearchParams();
  const requested = params.get("view") as AccessView | null;
  const view: AccessView =
    requested && ACCESS_VIEWS.includes(requested) ? requested : FOCUS_VIEW[params.get("focus") ?? ""] ?? "users";
  const stack = (...children: React.ReactNode[]) => (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      {children}
    </Space>
  );
  return (
    <Tabs
      className="access-tabs"
      activeKey={view}
      onChange={(k) => {
        const next = new URLSearchParams(params);
        next.set("view", k);
        next.delete("focus");
        setParams(next, { replace: true });
      }}
      items={[
        { key: "users", icon: <UserOutlined aria-hidden />, label: "Users & roles", children: stack(<UsersCard key="u" />, <PasswordResetRequestsCard key="p" />) },
        { key: "signin", icon: <LoginOutlined aria-hidden />, label: "Sign-in", children: stack(<ExternalAuthCard key="e" />, <SessionCard key="s" />) },
        { key: "groups", icon: <ClusterOutlined aria-hidden />, label: "Cluster groups", children: <ClusterGroupsCard /> },
        { key: "tokens", icon: <KeyOutlined aria-hidden />, label: "API tokens", children: <TokensCard /> },
        { key: "https", icon: <SafetyCertificateOutlined aria-hidden />, label: "HTTPS", children: <TLSCard isKubernetes={isKubernetes} /> },
      ]}
    />
  );
}

// UpdatesCard, BackupRestoreCard and ScheduledBackupCard are the Updates and
// Backups tabs of Updates & backups (features/updates/UpdatesBackupsPage.tsx).
export { BackupRestoreCard } from "./access/BackupRestoreCard";
export { ScheduledBackupCard } from "./access/ScheduledBackupCard";
export { UpdatesCard } from "./access/UpdatesCard";
