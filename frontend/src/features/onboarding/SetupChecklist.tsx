import { Button, Card, Drawer, Progress, Skeleton, Space, Tag, Tooltip, Typography } from "antd";
import { CheckCircleFilled, ClockCircleOutlined, EyeInvisibleOutlined, EyeOutlined, RocketOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router";
import { api } from "../../api/client";
import type { OnboardingStatus, OnboardingStep } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { notify, notifyError } from "../../notify";

// The setup checklist replaces the old browser-local "first-time setup"
// modal. Its state comes from GET /api/v1/onboarding, which derives each step
// from the install's real state (clusters configured, a successful run, a
// schedule, backups, …), so it is identical in every browser, ticks itself
// off as work gets done, and never blocks the app.

export const ONBOARDING_QUERY_KEY = ["onboarding"] as const;

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

function useSetDismissed() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (dismissed: boolean) => api.setOnboardingDismissed(dismissed),
    onSuccess: (data) => {
      queryClient.setQueryData(ONBOARDING_QUERY_KEY, data);
    },
    onError: (e) => notifyError(e, "Could not update the setup checklist"),
  });
}

/** The ordered step list shared by the dashboard card and the drawer. */
export function SetupStepList({ status, onNavigate }: { status: OnboardingStatus; onNavigate?: () => void }) {
  const navigate = useNavigate();
  const { isAdmin, loginEnabled } = useAuth();
  const firstPending = status.steps.find((s) => !s.done && !s.optional)?.id;

  const actionFor = (step: OnboardingStep) => {
    // The Access tab only exists when sign-in is enabled.
    if (step.tab === "access" && !loginEnabled) return null;
    if (step.admin_only && !isAdmin) {
      return <Typography.Text type="secondary">Ask an administrator</Typography.Text>;
    }
    return (
      <Button
        size="small"
        type={step.id === firstPending ? "primary" : "default"}
        onClick={() => {
          onNavigate?.();
          navigate(`/settings?tab=${encodeURIComponent(step.tab)}`);
        }}
      >
        {step.done ? "Review" : step.action_label}
      </Button>
    );
  };

  return (
    <ol className="setup-steps" aria-label="Setup steps">
      {status.steps.map((step, i) => (
        <li key={step.id} className={`setup-step${step.done ? " is-done" : ""}${step.id === firstPending ? " is-next" : ""}`}>
          <span className="setup-step-marker" aria-hidden="true">
            {step.done ? <CheckCircleFilled /> : <span className="setup-step-number">{i + 1}</span>}
          </span>
          <div className="setup-step-body">
            <div className="setup-step-head">
              <Typography.Text strong className="setup-step-title">
                {step.title}
              </Typography.Text>
              <span className="visually-hidden">{step.done ? " (done)" : " (not done)"}</span>
              {step.optional ? <Tag>Optional</Tag> : null}
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
 * Dashboard card. Shown to operators and admins until the required steps are
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

/** Header entry point: visible to operators/admins while setup is incomplete. */
export function SetupHeaderButton() {
  const { isAdmin, canOperate } = useAuth();
  const query = useOnboarding(isAdmin || canOperate);
  const setDismissed = useSetDismissed();
  const [open, setOpen] = useState(false);
  const status = query.data;
  if (!status || status.completed) return null;

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
      <Drawer
        title="Setup checklist"
        open={open}
        onClose={() => setOpen(false)}
        width={560}
        extra={
          isAdmin ? (
            <Button
              size="small"
              icon={status.dismissed ? <EyeOutlined /> : <EyeInvisibleOutlined />}
              loading={setDismissed.isPending}
              onClick={() => setDismissed.mutate(!status.dismissed)}
            >
              {status.dismissed ? "Show on dashboard" : "Hide from dashboard"}
            </Button>
          ) : null
        }
      >
        {query.isLoading ? (
          <Skeleton active />
        ) : (
          <Space orientation="vertical" size={16} style={{ width: "100%" }}>
            <Progress percent={progressPercent(status)} size="small" />
            <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
              Steps tick themselves off as the work is done — there is nothing to mark manually.
            </Typography.Paragraph>
            <SetupStepList status={status} onNavigate={() => setOpen(false)} />
          </Space>
        )}
      </Drawer>
    </>
  );
}
