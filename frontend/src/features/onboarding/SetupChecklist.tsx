import { Alert, Button, Card, Drawer, Popconfirm, Progress, Skeleton, Space, Tag, Tooltip, Typography } from "antd";
import {
  CheckCircleFilled,
  ClockCircleOutlined,
  EyeInvisibleOutlined,
  EyeOutlined,
  FlagOutlined,
  MinusCircleOutlined,
  RocketOutlined,
  UndoOutlined,
} from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import { sectionHref } from "../../navigation";
import { api } from "../../api/client";
import type { OnboardingStatus, OnboardingStep, OnboardingUpdate } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { notify, notifyError } from "../../notify";

// The setup checklist replaces the old browser-local "first-time setup"
// modal. Its state comes from GET /api/v1/onboarding, which derives each step
// from the install's real state (clusters configured, a successful run, a
// schedule, backups, …), so it is identical in every browser, ticks itself
// off as work gets done, and never blocks the app.

export const ONBOARDING_QUERY_KEY = ["onboarding"] as const;

/** Window event that opens the checklist drawer (the header menu sends it). */
export const OPEN_SETUP_EVENT = "ncc:open-setup";

export function openSetupChecklist() {
  window.dispatchEvent(new Event(OPEN_SETUP_EVENT));
}

/** Setup status for operators/admins; disabled (no request) for viewers. */
export function useOnboarding(enabled: boolean) {
  return useQuery({
    queryKey: ONBOARDING_QUERY_KEY,
    queryFn: api.onboarding,
    enabled,
    staleTime: 30_000,
    refetchInterval: 60_000,
    refetchOnWindowFocus: true,
    retry: 1,
  });
}

function useUpdateOnboarding() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (update: OnboardingUpdate) => api.updateOnboarding(update),
    onSuccess: (data) => {
      queryClient.setQueryData(ONBOARDING_QUERY_KEY, data);
    },
    onError: (e) => notifyError(e, "Couldn't update the setup checklist"),
  });
}

function useSetDismissed() {
  const update = useUpdateOnboarding();
  return { ...update, mutate: (dismissed: boolean, opts?: { onSuccess?: () => void }) => update.mutate({ dismissed }, opts) };
}

/** The next step to work on: the first required one that is neither done nor skipped. */
function nextStep(status: OnboardingStatus): OnboardingStep | undefined {
  return status.steps.find((s) => !s.done && !s.skipped && !s.optional);
}

/** The ordered step list shared by the dashboard card and the drawer. */
export function SetupStepList({ status, onNavigate }: { status: OnboardingStatus; onNavigate?: () => void }) {
  const navigate = useNavigate();
  const { isAdmin, loginEnabled } = useAuth();
  const update = useUpdateOnboarding();
  const firstPending = nextStep(status)?.id;

  const actionFor = (step: OnboardingStep) => {
    if (step.skipped) {
      return isAdmin ? (
        <Button
          size="small"
          type="text"
          icon={<UndoOutlined />}
          loading={update.isPending && update.variables?.unskip_step === step.id}
          onClick={() => update.mutate({ unskip_step: step.id })}
        >
          Undo skip
        </Button>
      ) : null;
    }
    // The Access tab only exists when sign-in is enabled.
    const canOpen = !(step.tab === "access" && !loginEnabled);
    if (step.admin_only && !isAdmin) {
      return <Typography.Text type="secondary">Ask an administrator</Typography.Text>;
    }
    return (
      <Space size={4} wrap>
        {canOpen ? (
          <Button
            size="small"
            type={step.id === firstPending ? "primary" : "default"}
            onClick={() => {
              onNavigate?.();
              navigate(sectionHref(step.tab));
            }}
          >
            {step.done ? "Review" : step.action_label}
          </Button>
        ) : null}
        {!step.done && isAdmin ? (
          <Tooltip title="Not needed here? Skipping counts the step as done for everyone. You can undo it.">
            <Button
              size="small"
              type="text"
              aria-label={`Skip: ${step.title}`}
              loading={update.isPending && update.variables?.skip_step === step.id}
              onClick={() => update.mutate({ skip_step: step.id })}
            >
              Skip
            </Button>
          </Tooltip>
        ) : null}
      </Space>
    );
  };

  return (
    <ol className="setup-steps" aria-label="Setup steps">
      {status.steps.map((step, i) => (
        <li
          key={step.id}
          className={`setup-step${step.done ? " is-done" : ""}${step.skipped ? " is-skipped" : ""}${step.id === firstPending ? " is-next" : ""}`}
        >
          <span className="setup-step-marker" aria-hidden="true">
            {step.done ? (
              <CheckCircleFilled />
            ) : step.skipped ? (
              <MinusCircleOutlined className="setup-step-skipped-icon" />
            ) : (
              <span className="setup-step-number">{i + 1}</span>
            )}
          </span>
          <div className="setup-step-body">
            <div className="setup-step-head">
              <Typography.Text strong className="setup-step-title">
                {step.title}
              </Typography.Text>
              <span className="visually-hidden">{step.done ? " (done)" : step.skipped ? " (skipped)" : " (not done)"}</span>
              {step.optional ? <Tag>Optional</Tag> : null}
              {step.skipped ? <Tag>Skipped</Tag> : null}
            </div>
            <Typography.Text type="secondary" className="setup-step-desc">
              {step.description}
            </Typography.Text>
            {step.detail ? (
              <Typography.Text className={`setup-step-detail${step.done ? "" : " is-pending"}`}>{step.detail}</Typography.Text>
            ) : null}
          </div>
          <div className="setup-step-action">{actionFor(step)}</div>
        </li>
      ))}
    </ol>
  );
}

function progressPercent(status: OnboardingStatus): number {
  return Math.round((status.required_done / Math.max(1, status.required_total)) * 100);
}

/**
 * Full checklist card (kept for embedding; the Overview uses SetupBanner).
 * Shown to operators and admins until the required steps are
 * done or an admin hides it for everyone.
 */
export function SetupChecklistCard() {
  const { isAdmin, canOperate } = useAuth();
  const query = useOnboarding(isAdmin || canOperate);
  const setDismissed = useSetDismissed();
  const status = query.data;
  if (!status || status.completed || status.dismissed) return null;

  return (
    <Card
      className="page-card setup-card"
      title={
        <Space size={10}>
          <RocketOutlined />
          <span>Finish setting up NCC Orchestrator</span>
        </Space>
      }
      extra={
        isAdmin ? (
          <Tooltip title="Hide this checklist for everyone. It stays available from the Setup button in the header.">
            <Button
              size="small"
              type="text"
              icon={<EyeInvisibleOutlined />}
              loading={setDismissed.isPending}
              onClick={() =>
                setDismissed.mutate(true, {
                  onSuccess: () => notify.info("Setup checklist hidden. Reopen it from Setup in the header."),
                })
              }
            >
              Hide
            </Button>
          </Tooltip>
        ) : null
      }
    >
      <div className="setup-card-progress">
        <Progress
          percent={progressPercent(status)}
          size="small"
          showInfo={false}
          aria-label={`${status.required_done} of ${status.required_total} required steps done`}
        />
        <Typography.Text type="secondary">
          {status.required_done} of {status.required_total} required steps done
        </Typography.Text>
      </div>
      <SetupStepList status={status} />
    </Card>
  );
}

/** Admin control to complete setup now, or reopen it after finishing. */
function FinishSetupButton({ status, onDone }: { status: OnboardingStatus; onDone?: () => void }) {
  const update = useUpdateOnboarding();
  const left = status.required_total - status.required_done;
  if (status.finished) {
    return (
      <Button
        size="small"
        icon={<UndoOutlined />}
        loading={update.isPending}
        onClick={() => update.mutate({ finished: false }, { onSuccess: () => notify.info("Setup reopened.") })}
      >
        Reopen setup
      </Button>
    );
  }
  if (status.completed) return null;
  return (
    <Popconfirm
      title="Finish setup now?"
      description={`${left} required step${left === 1 ? " is" : "s are"} not done. The checklist and Setup button go away for everyone; you can reopen it from the user menu.`}
      okText="Finish setup"
      onConfirm={() =>
        update.mutate(
          { finished: true },
          {
            onSuccess: () => {
              notify.success("Setup marked as complete.");
              onDone?.();
            },
          },
        )
      }
    >
      <Button size="small" icon={<FlagOutlined />} loading={update.isPending}>
        Finish setup
      </Button>
    </Popconfirm>
  );
}

/** Every step, in a drawer; shared by the top-bar button and the Overview banner. */
function SetupDrawer({ open, onClose, status, loading }: { open: boolean; onClose: () => void; status: OnboardingStatus; loading?: boolean }) {
  const { isAdmin } = useAuth();
  const setDismissed = useSetDismissed();
  return (
    <Drawer
      title="Setup checklist"
      open={open}
      onClose={onClose}
      size={560}
      footer={
        isAdmin ? (
          <Space style={{ width: "100%", justifyContent: "space-between" }} wrap>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              Skip steps that do not apply, or finish setup to hide the checklist.
            </Typography.Text>
            <FinishSetupButton status={status} onDone={onClose} />
          </Space>
        ) : null
      }
      extra={
        isAdmin && !status.completed ? (
          <Button
            size="small"
            icon={status.dismissed ? <EyeOutlined /> : <EyeInvisibleOutlined />}
            loading={setDismissed.isPending}
            onClick={() => setDismissed.mutate(!status.dismissed)}
          >
            {status.dismissed ? "Show on Overview" : "Hide from Overview"}
          </Button>
        ) : null
      }
    >
      {loading ? (
        <Skeleton active />
      ) : (
        <Space orientation="vertical" size={16} style={{ width: "100%" }}>
          {status.finished ? (
            <Alert
              type="success"
              showIcon
              title="Setup is marked as complete"
              description={`Finished${status.finished_by ? ` by ${status.finished_by}` : ""}${status.finished_at ? ` on ${new Date(status.finished_at).toLocaleDateString()}` : ""}. The steps below still show what is left.`}
            />
          ) : status.completed ? (
            <Alert type="success" showIcon title="All required steps are done" />
          ) : null}
          <Progress percent={progressPercent(status)} size="small" />
          <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
            Steps tick themselves off as the work is done — there is nothing to mark manually.
          </Typography.Paragraph>
          <SetupStepList status={status} onNavigate={onClose} />
        </Space>
      )}
    </Drawer>
  );
}

/**
 * Slim Overview banner: progress, the next required step and its button, and
 * the full list in a drawer. Shown until setup is done or an admin hides it.
 */
export function SetupBanner() {
  const { isAdmin, canOperate, loginEnabled } = useAuth();
  const navigate = useNavigate();
  const query = useOnboarding(isAdmin || canOperate);
  const setDismissed = useSetDismissed();
  const update = useUpdateOnboarding();
  const [open, setOpen] = useState(false);
  const status = query.data;
  if (!status || status.completed || status.dismissed) return null;
  const next = nextStep(status);
  // Same rules as the step list: admin-only steps and Access without sign-in have no button.
  const canDoNext = next && !(next.admin_only && !isAdmin) && !(next.tab === "access" && !loginEnabled);

  return (
    <section className="setup-banner" aria-label="Setup progress">
      <RocketOutlined className="setup-banner-icon" aria-hidden="true" />
      <div className="setup-banner-text">
        <span className="setup-banner-title">
          Finish setting up · {status.required_done} of {status.required_total} required steps done
        </span>
        {next ? <span className="setup-banner-next">Next: {next.title}</span> : null}
      </div>
      <Progress
        className="setup-banner-progress"
        percent={progressPercent(status)}
        size="small"
        showInfo={false}
        aria-label={`${status.required_done} of ${status.required_total} required steps done`}
      />
      <Space size={8} wrap>
        {next && canDoNext ? (
          <Button type="primary" size="small" onClick={() => navigate(sectionHref(next.tab))}>
            {next.action_label}
          </Button>
        ) : null}
        {next && isAdmin ? (
          <Tooltip title="Not needed here? Skipping counts the step as done. You can undo it from All steps.">
            <Button
              size="small"
              type="text"
              loading={update.isPending && update.variables?.skip_step === next.id}
              onClick={() => update.mutate({ skip_step: next.id })}
            >
              Skip step
            </Button>
          </Tooltip>
        ) : null}
        <Button size="small" onClick={() => setOpen(true)}>
          All steps
        </Button>
        {isAdmin ? <FinishSetupButton status={status} /> : null}
        {isAdmin ? (
          <Tooltip title="Hide this for everyone. It stays available from Setup in the top bar.">
            <Button
              size="small"
              type="text"
              icon={<EyeInvisibleOutlined />}
              aria-label="Hide setup progress"
              loading={setDismissed.isPending}
              onClick={() =>
                setDismissed.mutate(true, {
                  onSuccess: () => notify.info("Setup progress hidden. Reopen it from Setup in the top bar."),
                })
              }
            />
          </Tooltip>
        ) : null}
      </Space>
      <SetupDrawer open={open} onClose={() => setOpen(false)} status={status} loading={query.isLoading} />
    </section>
  );
}

/**
 * Header entry point: visible to operators/admins while setup is incomplete.
 * Its drawer also opens on OPEN_SETUP_EVENT (user menu), including after
 * setup is finished, so an admin can reopen it.
 */
export function SetupHeaderButton() {
  const { isAdmin, canOperate } = useAuth();
  const query = useOnboarding(isAdmin || canOperate);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    const onOpen = () => setOpen(true);
    window.addEventListener(OPEN_SETUP_EVENT, onOpen);
    return () => window.removeEventListener(OPEN_SETUP_EVENT, onOpen);
  }, []);
  const status = query.data;
  if (!status) return null;
  if (status.completed) {
    return <SetupDrawer open={open} onClose={() => setOpen(false)} status={status} loading={query.isLoading} />;
  }

  return (
    <>
      <Tooltip title={`Setup: ${status.required_done} of ${status.required_total} required steps done`}>
        <Button
          className="header-setup-btn"
          icon={<ClockCircleOutlined />}
          aria-label={`Setup checklist: ${status.required_done} of ${status.required_total} required steps done`}
          onClick={() => setOpen(true)}
        >
          <span className="header-setup-label">Setup</span>
          <span className="header-setup-count">
            {status.required_done}/{status.required_total}
          </span>
        </Button>
      </Tooltip>
      <SetupDrawer open={open} onClose={() => setOpen(false)} status={status} loading={query.isLoading} />
    </>
  );
}
