import { Alert, Button, Card, Modal, Progress, Space, Steps, Tag, Typography } from "antd";
import { useMemo } from "react";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";

type Props = {
  open: boolean;
  onComplete: () => void;
  onSkip: () => void;
  onPause: (forMs?: number) => void;
};

const SETUP_STEPS: Array<{
  title: string;
  description: string;
  tab: string;
  eta: string;
  why: string;
  checklist: string[];
  success: string;
}> = [
  {
    title: "Connection",
    description: "Confirm API health, paths, and basic runtime status.",
    tab: "connection",
    eta: "2-3 min",
    why: "A healthy control plane prevents false troubleshooting later.",
    checklist: [
      "API health shows ok and the service is reachable.",
      "Install/config/log paths point to expected locations.",
      "System Health has no critical failures.",
    ],
    success: "Connection checks are green and you can safely proceed to configuration.",
  },
  {
    title: "Configuration",
    description: "Validate config inputs and cluster/source settings.",
    tab: "config",
    eta: "4-6 min",
    why: "Good input validation avoids failed runs and noisy alerts.",
    checklist: [
      "Cluster source and credentials are configured.",
      "Config validation succeeds without blocking errors.",
      "Any policy gates or filters match your environment intent.",
    ],
    success: "A valid runtime config is saved and ready for first run.",
  },
  {
    title: "Schedule + Notifications",
    description: "Set run cadence and test at least one notification channel.",
    tab: "schedule",
    eta: "3-5 min",
    why: "Automation + delivery guarantees operational visibility.",
    checklist: [
      "A recurring schedule is created or confirmed.",
      "At least one channel (email/slack/webhook) is tested.",
      "Quiet hours / maintenance windows are set if required.",
    ],
    success: "Runs and notifications are automated for day-2 operations.",
  },
  {
    title: "First Run + Backup",
    description: "Trigger one run, verify results, then create a recovery snapshot.",
    tab: "runs",
    eta: "5-8 min",
    why: "A first successful cycle validates both detection and recovery posture.",
    checklist: [
      "A manual run completed and results are visible.",
      "No unexpected critical findings remain unresolved.",
      "A backup snapshot exists and restore path is documented.",
    ],
    success: "You have evidence that monitoring and recovery are both working.",
  },
];

export function FirstTimeSetupModal({ open, onComplete, onSkip, onPause }: Props) {
  const [doneTabs, setDoneTabs] = useLocalStorageState<string[]>("ux.firstSetup.doneTabs", []);
  const [currentStep, setCurrentStep] = useLocalStorageState<number>("ux.firstSetup.currentStep", 0);
  const normalizedStep = Math.max(0, Math.min(SETUP_STEPS.length - 1, currentStep));
  const step = SETUP_STEPS[normalizedStep];
  const completedCount = doneTabs.filter((t) => SETUP_STEPS.some((s) => s.tab === t)).length;
  const percent = Math.round((completedCount / Math.max(1, SETUP_STEPS.length)) * 100);
  const isCurrentDone = doneTabs.includes(step.tab);
  const allDone = completedCount >= SETUP_STEPS.length;

  const doneTabSet = useMemo(() => new Set(doneTabs), [doneTabs]);
  const nextPending = SETUP_STEPS.findIndex((s) => !doneTabSet.has(s.tab));
  const guidedHint =
    nextPending >= 0
      ? `Recommended next step: ${SETUP_STEPS[nextPending].title}`
      : "All setup steps completed. You can mark setup complete.";

  const openTab = (tab: string) => {
    // Let users work through setup without the modal immediately reappearing.
    onPause(30 * 60 * 1000);
    window.location.assign(`/settings?tab=${encodeURIComponent(tab)}`);
  };

  const markCurrentDone = () => {
    if (!doneTabSet.has(step.tab)) {
      setDoneTabs([...doneTabs, step.tab]);
    }
    if (normalizedStep < SETUP_STEPS.length - 1) {
      setCurrentStep(normalizedStep + 1);
    }
  };

  return (
    <Modal
      title="Guided First-Time Setup"
      open={open}
      onCancel={onSkip}
      width={760}
      footer={
        <Space>
          <Button onClick={() => onPause(30 * 60 * 1000)}>Continue later (30m)</Button>
          <Button onClick={() => onPause(24 * 60 * 60 * 1000)}>Remind me tomorrow</Button>
          <Button onClick={onSkip}>Skip for now</Button>
          <Button type="primary" onClick={onComplete} disabled={!allDone}>
            Mark setup complete
          </Button>
        </Space>
      }
      destroyOnClose
      maskClosable={false}
    >
      <Space direction="vertical" size={12} style={{ width: "100%" }}>
        <Alert
          type="info"
          showIcon
          message="New install detected"
          description="Work through this once to validate runtime, schedule, and recovery readiness. Progress is saved automatically."
        />
        <Typography.Text type="secondary">{guidedHint}</Typography.Text>
        <Progress percent={percent} size="small" />
        <Steps
          current={normalizedStep}
          size="default"
          onChange={(i) => setCurrentStep(i)}
          items={SETUP_STEPS.map((s) => ({
            key: s.tab,
            title: s.title,
            status: doneTabSet.has(s.tab) ? "finish" : "process",
          }))}
        />
        <div style={{ border: "1px solid var(--ant-color-border-secondary, #e5e7eb)", borderRadius: 8, padding: 12 }}>
          <Space direction="vertical" size={6} style={{ width: "100%" }}>
            <Typography.Text strong>
              Step {normalizedStep + 1}: {step.title}
            </Typography.Text>
            <Typography.Text type="secondary">{step.description}</Typography.Text>
            <Space size={8}>
              <Tag color={isCurrentDone ? "success" : "processing"}>{isCurrentDone ? "Completed" : "In progress"}</Tag>
              <Tag>{`ETA ${step.eta}`}</Tag>
            </Space>
            <Card size="small" bordered={false} style={{ background: "var(--ant-color-fill-quaternary, #fafafa)" }}>
              <Space direction="vertical" size={4} style={{ width: "100%" }}>
                <Typography.Text strong>Why this matters</Typography.Text>
                <Typography.Text type="secondary">{step.why}</Typography.Text>
              </Space>
            </Card>
            <Card size="small" title="Checklist">
              <Space direction="vertical" size={4} style={{ width: "100%" }}>
                {step.checklist.map((item) => (
                  <Typography.Text key={item}>- {item}</Typography.Text>
                ))}
              </Space>
            </Card>
            <Alert type="success" showIcon message="Success criteria" description={step.success} />
            <Space wrap>
              <Button type="primary" onClick={() => openTab(step.tab)}>
                Open this step
              </Button>
              <Button onClick={markCurrentDone} disabled={isCurrentDone}>
                {isCurrentDone ? "Marked complete" : "Mark as done"}
              </Button>
              <Button onClick={() => setCurrentStep(Math.max(0, normalizedStep - 1))} disabled={normalizedStep === 0}>
                Previous
              </Button>
              <Button
                onClick={() => setCurrentStep(Math.min(SETUP_STEPS.length - 1, normalizedStep + 1))}
                disabled={normalizedStep === SETUP_STEPS.length - 1}
              >
                Next
              </Button>
            </Space>
          </Space>
        </div>
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          If this environment was initialized from backup restore, this guide is skipped automatically.
        </Typography.Text>
      </Space>
    </Modal>
  );
}

