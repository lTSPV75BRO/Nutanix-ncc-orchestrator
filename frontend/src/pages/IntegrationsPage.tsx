import { useMemo, useState, type ReactNode } from "react";
import { Link, useParams } from "react-router";
import { Button, Checkbox, Input, Progress, Segmented, Tooltip } from "antd";
import {
  ArrowLeftOutlined,
  CheckCircleFilled,
  CheckOutlined,
  ClockCircleOutlined,
  CopyOutlined,
  ExportOutlined,
  KeyOutlined,
  LinkOutlined,
  QuestionCircleOutlined,
  RightOutlined,
  SearchOutlined,
  SwapOutlined,
  UserOutlined,
} from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { useAuth } from "../auth/AuthContext";
import { PersonalTokensModal } from "../features/tokens/PersonalTokensModal";
import {
  CATEGORIES,
  INTEGRATIONS,
  REPO_DOCS,
  TERMS,
  defaultApiAddress,
  fillSnippet,
  integrationById,
  parseApiAddress,
  type ApiAddress,
  type Integration,
  type IntegrationCategory,
  type Snippet,
  type Step,
} from "../features/integrations/catalog";
import { useLocalStorageState } from "../hooks/useLocalStorageState";
import { sectionHref } from "../navigation";
import { EmptyState, PageHeader, Panel, StatusBadge } from "../ui";

const ROLE_TEXT: Record<Integration["role"], string> = {
  viewer: "Viewer account",
  operator: "Operator account",
  admin: "Admin account",
  none: "No NCC account",
};

/**
 * Third-party integrations: a catalog of the tools NCC works with and, per
 * tool, a step-by-step guide written for someone new to that tool. Snippets
 * are filled in with the API server's address, which the reader confirms once.
 */
export function IntegrationsPage() {
  const { id } = useParams();
  const [rawAddress, setRawAddress] = useLocalStorageState("integrations.apiAddress", defaultApiAddress(window.location));
  const address = useMemo(() => parseApiAddress(rawAddress), [rawAddress]);
  if (id) {
    const integration = integrationById(id);
    if (!integration) {
      return (
        <div className="integrations-page">
          <EmptyState
            title="No such integration"
            description="It may have been renamed."
            actions={
              <Link to="/integrations">
                <Button>All integrations</Button>
              </Link>
            }
          />
        </div>
      );
    }
    return <IntegrationGuide integration={integration} address={address} rawAddress={rawAddress} onAddress={setRawAddress} />;
  }
  return <IntegrationCatalog rawAddress={rawAddress} onAddress={setRawAddress} address={address} />;
}

function IntegrationCatalog({ rawAddress, onAddress, address }: { rawAddress: string; onAddress: (v: string) => void; address: ApiAddress }) {
  const [category, setCategory] = useState<IntegrationCategory | "All">("All");
  const [search, setSearch] = useState("");
  const q = search.trim().toLowerCase();
  const shown = INTEGRATIONS.filter(
    (i) =>
      (category === "All" || i.category === category) &&
      (!q || [i.name, i.alsoFor ?? "", i.summary, i.category].some((t) => t.toLowerCase().includes(q))),
  );
  return (
    <div className="integrations-page">
      <PageHeader
        title="Integrations"
        description="Connect NCC Orchestrator to the tools your team already uses. Each guide walks you through every step, in order, with copy-ready settings."
      />

      <section className="integrations-how" aria-label="How integrations work">
        <HowStep n={1} icon={<UserOutlined />} title="Give the tool its own account" text="An NCC account just for that tool, with the least access it needs." />
        <HowStep n={2} icon={<KeyOutlined />} title="Create an access token" text="A long password for software. The tool sends it with every request." />
        <HowStep n={3} icon={<SwapOutlined />} title="Connect the tool" text="Paste NCC's API address and the token into the tool, then test." />
      </section>

      <div className="integrations-toolbar">
        <Segmented
          aria-label="Category"
          value={category}
          onChange={(v) => setCategory(v as IntegrationCategory | "All")}
          options={["All", ...CATEGORIES]}
        />
        <Input
          allowClear
          prefix={<SearchOutlined aria-hidden />}
          placeholder="Search, e.g. Splunk"
          aria-label="Search integrations"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="integrations-search"
        />
      </div>

      {shown.length ? (
        <div className="integrations-grid">
          {shown.map((i) => (
            <IntegrationCard key={i.id} integration={i} />
          ))}
        </div>
      ) : (
        <EmptyState
          title="No integration matches"
          description="Any tool that can call a REST API can still connect: see “Your own scripts”."
          actions={<Button onClick={() => { setSearch(""); setCategory("All"); }}>Show all</Button>}
        />
      )}

      <ApiAddressPanel rawAddress={rawAddress} onAddress={onAddress} address={address} />
    </div>
  );
}

function HowStep({ n, icon, title, text }: { n: number; icon: ReactNode; title: string; text: string }) {
  return (
    <div className="integrations-how-step">
      <span className="integrations-how-icon" aria-hidden>
        {icon}
      </span>
      <div>
        <div className="integrations-how-title">
          <span className="integrations-how-n">{n}.</span> {title}
        </div>
        <div className="integrations-how-text">{text}</div>
      </div>
    </div>
  );
}

function IntegrationCard({ integration: i }: { integration: Integration }) {
  const [done] = useStepProgress(i.id);
  const total = i.steps.length;
  const count = i.steps.filter((s) => done.includes(s.id)).length;
  return (
    <Link to={`/integrations/${i.id}`} className="integration-card">
      <div className="integration-card-head">
        <span className="integration-card-icon" aria-hidden>
          {i.icon}
        </span>
        <div className="integration-card-names">
          <span className="integration-card-name">{i.name}</span>
          <span className="integration-card-category">{i.category}</span>
        </div>
        <RightOutlined className="integration-card-trail" aria-hidden />
      </div>
      <p className="integration-card-summary">{i.summary}</p>
      {i.alsoFor ? <p className="integration-card-also">{i.alsoFor}</p> : null}
      <div className="integration-card-meta">
        <LevelBadge level={i.level} />
        <span>
          <ClockCircleOutlined aria-hidden /> {i.time.replace("About ", "~")}
        </span>
        {count > 0 ? (
          <span className={count === total ? "integration-card-done" : undefined}>
            {count === total ? <CheckCircleFilled aria-hidden /> : null} {count}/{total} steps done
          </span>
        ) : null}
      </div>
    </Link>
  );
}

function LevelBadge({ level }: { level: Integration["level"] }) {
  const tone = level === "Easy" ? "ok" : level === "Moderate" ? "info" : "warn";
  return <StatusBadge tone={tone}>{level}</StatusBadge>;
}

/** Which steps of a guide this browser has ticked. */
function useStepProgress(id: string): [string[], (step: string, on: boolean) => void, () => void] {
  const [done, setDone] = useLocalStorageState<string[]>(`integrations.progress.${id}`, []);
  const list = Array.isArray(done) ? done : [];
  const toggle = (step: string, on: boolean) => setDone(on ? Array.from(new Set([...list, step])) : list.filter((s) => s !== step));
  return [list, toggle, () => setDone([])];
}

function IntegrationGuide({
  integration: i,
  address,
  rawAddress,
  onAddress,
}: {
  integration: Integration;
  address: ApiAddress;
  rawAddress: string;
  onAddress: (v: string) => void;
}) {
  const [done, toggle, reset] = useStepProgress(i.id);
  const [tokensOpen, setTokensOpen] = useState(false);
  const { role } = useAuth();
  const count = i.steps.filter((s) => done.includes(s.id)).length;
  const required = i.steps.filter((s) => !s.optional);
  const requiredDone = required.every((s) => done.includes(s.id));
  const usesApi = i.terms.includes("api");

  return (
    <div className="integrations-page">
      <Link to="/integrations" className="integration-back">
        <ArrowLeftOutlined aria-hidden /> All integrations
      </Link>
      <header className="integration-hero">
        <span className="integration-hero-icon" aria-hidden>
          {i.icon}
        </span>
        <div className="integration-hero-text">
          <h2>{i.name}</h2>
          <p>{i.summary}</p>
          <div className="integration-card-meta">
            <LevelBadge level={i.level} />
            <span>
              <ClockCircleOutlined aria-hidden /> {i.time}
            </span>
            <span>
              <UserOutlined aria-hidden /> {ROLE_TEXT[i.role]}
            </span>
          </div>
        </div>
      </header>

      <div className="integration-layout">
        <div className="integration-main">
          <div className="integration-progress" aria-live="polite">
            <Progress percent={Math.round((count / i.steps.length) * 100)} showInfo={false} size="small" aria-label={`${count} of ${i.steps.length} setup steps done`} />
            <span>
              {requiredDone ? (
                <>
                  <CheckCircleFilled className="integration-progress-done" aria-hidden /> All required steps are done
                </>
              ) : (
                `${count} of ${i.steps.length} steps done`
              )}
            </span>
            {count ? (
              <Button type="link" size="small" onClick={reset}>
                Start over
              </Button>
            ) : null}
          </div>

          <ol className="integration-steps">
            {i.steps.map((step, idx) => (
              <StepCard
                key={step.id}
                n={idx + 1}
                step={step}
                address={address}
                done={done.includes(step.id)}
                onDone={(on) => toggle(step.id, on)}
                onTokens={() => setTokensOpen(true)}
              />
            ))}
          </ol>

          {i.id === "notifications" ? <NotificationChannelStatus /> : null}

          <p className="integration-docs">
            Full reference, edge cases and troubleshooting:{" "}
            <a href={`${REPO_DOCS}${i.doc}`} target="_blank" rel="noreferrer">
              {i.doc.split("#")[0]} <ExportOutlined aria-hidden />
            </a>
          </p>
        </div>

        <aside className="integration-side" aria-label="About this integration" tabIndex={0}>
          <Panel title="What you get">
            <ul className="integration-list is-check">
              {i.youGet.map((t) => (
                <li key={t}>
                  <CheckOutlined aria-hidden /> {t}
                </li>
              ))}
            </ul>
          </Panel>
          <Panel title="Before you start">
            <ul className="integration-list">
              {i.youNeed.map((t) => (
                <li key={t}>{t}</li>
              ))}
            </ul>
            <p className="integration-direction">
              <SwapOutlined aria-hidden /> {i.direction}
            </p>
          </Panel>
          {usesApi ? <ApiAddressPanel rawAddress={rawAddress} onAddress={onAddress} address={address} compact /> : null}
          {i.terms.length ? (
            <Panel title="Words you'll see">
              <dl className="integration-terms">
                {i.terms.map((k) =>
                  TERMS[k] ? (
                    <div key={k}>
                      <dt>{TERMS[k].term}</dt>
                      <dd>{TERMS[k].meaning}</dd>
                    </div>
                  ) : null,
                )}
              </dl>
            </Panel>
          ) : null}
        </aside>
      </div>
      <PersonalTokensModal open={tokensOpen} onClose={() => setTokensOpen(false)} role={role} />
    </div>
  );
}

function StepCard({
  n,
  step,
  address,
  done,
  onDone,
  onTokens,
}: {
  n: number;
  step: Step;
  address: ApiAddress;
  done: boolean;
  onDone: (on: boolean) => void;
  onTokens: () => void;
}) {
  return (
    <li className={`integration-step${done ? " is-done" : ""}`}>
      <span className="integration-step-n" aria-hidden>
        {done ? <CheckOutlined /> : n}
      </span>
      <div className="integration-step-body">
        <div className="integration-step-head">
          <h3>
            {step.title}
            {step.optional ? <span className="integration-step-optional">Optional</span> : null}
          </h3>
          <span className="integration-step-where">{step.where}</span>
        </div>
        <div className="integration-step-text">{step.body}</div>
        {step.snippets?.map((s) => (
          <CodeSnippet key={s.label} snippet={s} address={address} />
        ))}
        <div className="integration-step-foot">
          <StepAction action={step.action} onTokens={onTokens} />
          <Checkbox checked={done} onChange={(e) => onDone(e.target.checked)}>
            Done
          </Checkbox>
        </div>
      </div>
    </li>
  );
}

function StepAction({ action, onTokens }: { action: Step["action"]; onTokens: () => void }) {
  const { isAdmin, loginEnabled } = useAuth();
  if (action === "token") {
    if (!loginEnabled) {
      return <span className="integration-hint">Sign-in is off on this server, so use the static API token (NCC_API_VIEWER_TOKEN) instead.</span>;
    }
    return (
      <Button icon={<KeyOutlined />} onClick={onTokens}>
        Open my access tokens
      </Button>
    );
  }
  const link = (key: string, label: string, params?: Record<string, string>) =>
    isAdmin ? (
      <Link to={sectionHref(key, params)}>
        <Button icon={<LinkOutlined />}>{label}</Button>
      </Link>
    ) : (
      <span className="integration-hint">An admin does this step.</span>
    );
  if (action === "access") return link("access", "Open Access settings");
  if (action === "notifications") return link("notifications", "Open Notifications");
  if (action === "developer") return link("developer", "Open the API explorer");
  return <span />;
}

function CodeSnippet({ snippet, address }: { snippet: Snippet; address: ApiAddress }) {
  const code = fillSnippet(snippet.code, address);
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard blocked (http, permissions): the text stays selectable.
    }
  };
  return (
    <figure className="integration-code">
      <figcaption>
        <span>{snippet.label}</span>
        <Button size="small" type="text" icon={copied ? <CheckOutlined /> : <CopyOutlined />} onClick={() => void copy()} aria-label={`Copy ${snippet.label}`}>
          {copied ? "Copied" : "Copy"}
        </Button>
      </figcaption>
      <pre tabIndex={0}>
        <code>{code}</code>
      </pre>
      {snippet.note ? <p className="integration-hint">{snippet.note}</p> : null}
    </figure>
  );
}

/**
 * The API server address used in every snippet, and a check that NCC is
 * answering. The check runs from this browser (through this web page), so it
 * proves NCC works, not that the other tool's server can reach it; each guide
 * has a command for that.
 */
function ApiAddressPanel({
  rawAddress,
  onAddress,
  address,
  compact = false,
}: {
  rawAddress: string;
  onAddress: (v: string) => void;
  address: ApiAddress;
  compact?: boolean;
}) {
  const [checking, setChecking] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(null);
  const check = async () => {
    setChecking(true);
    setResult(null);
    try {
      const list = await api.nccAlerts({ limit: 1 });
      const open = list.count;
      setResult({
        ok: true,
        text: `NCC is answering: ${open === 1 ? "1 open finding" : `${open} open findings`}${list.last_run ? `, last run ${new Date(list.last_run).toLocaleString()}` : ""}.`,
      });
    } catch (err) {
      setResult({ ok: false, text: `NCC did not answer: ${err instanceof Error ? err.message : String(err)}` });
    } finally {
      setChecking(false);
    }
  };
  const looksLikeUi = typeof window !== "undefined" && address.host === window.location.host;
  return (
    <Panel
      title="NCC's API address"
      description={compact ? undefined : "Tools connect to the API server, not to this web page. Every guide fills this address into its settings."}
      className="integration-address"
    >
      <label className="integration-address-label" htmlFor="integration-api-address">
        API server address{" "}
        <Tooltip title={TERMS.api.meaning}>
          <QuestionCircleOutlined aria-label="What is this?" />
        </Tooltip>
      </label>
      <Input
        id="integration-api-address"
        value={rawAddress}
        onChange={(e) => onAddress(e.target.value)}
        placeholder="https://ncc-api.example.com:8081"
        spellCheck={false}
      />
      {looksLikeUi ? (
        <p className="integration-warn">This is the web page's own address. Tools usually need the API server (port 8081 by default) because the web page strips tokens.</p>
      ) : null}
      <p className="integration-hint">
        Default port 8081. On Kubernetes, in-cluster tools use <code>http://ncc-v2-api.&lt;namespace&gt;.svc:8081</code>; tools outside need an
        Ingress for the API.
      </p>
      <div className="integration-address-check">
        <Button onClick={() => void check()} loading={checking}>
          Check NCC is answering
        </Button>
        {result ? (
          <span className={result.ok ? "integration-ok" : "integration-warn"} role="status">
            {result.text}
          </span>
        ) : null}
      </div>
    </Panel>
  );
}

/** Live delivery state of the notification channels (admins). */
function NotificationChannelStatus() {
  const { isAdmin } = useAuth();
  const statusQuery = useQuery({ queryKey: ["notification-status"], queryFn: api.getNotificationStatus, enabled: isAdmin });
  if (!isAdmin) return null;
  const channels = statusQuery.data?.channels ?? [];
  const name = (c: string) => (c === "email" ? "Email" : c === "slack" ? "Slack" : "Webhook");
  return (
    <Panel title="Your channels now" description="From Settings → Notifications.">
      {statusQuery.isLoading ? (
        <p className="integration-hint">Loading…</p>
      ) : (
        <ul className="integration-channels">
          {channels.map((c) => (
            <li key={c.channel}>
              <span>{name(c.channel)}</span>
              {!c.enabled ? (
                <StatusBadge tone="neutral">Off</StatusBadge>
              ) : c.state === "ok" ? (
                <StatusBadge tone="ok">Delivering</StatusBadge>
              ) : c.state === "failing" ? (
                <StatusBadge tone="fail">Failing</StatusBadge>
              ) : (
                <StatusBadge tone="info">On, nothing sent yet</StatusBadge>
              )}
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
