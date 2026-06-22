import { useMemo } from "react";
import { Button, Card, List, Progress, Space, Tag, Typography } from "antd";
import { CheckCircleOutlined, ClockCircleOutlined } from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api } from "../../api/client";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";

type Step = {
  id: string;
  title: string;
  description: string;
  done: boolean;
  link?: string;
};

export function OnboardingSection() {
  const [doneMap, setDoneMap] = useLocalStorageState<Record<string, boolean>>("ux.onboarding.done", {});
  const health = useQuery({ queryKey: ["health"], queryFn: api.health, staleTime: 20_000 });
  const schedule = useQuery({ queryKey: ["schedule-health"], queryFn: api.scheduleHealth, staleTime: 20_000 });
  const backupSchedule = useQuery({ queryKey: ["settings", "backup-schedule"], queryFn: api.getBackupSchedule, staleTime: 20_000 });
  const notifications = useQuery({ queryKey: ["settings", "notifications"], queryFn: api.getNotifications, staleTime: 20_000 });
  const runs = useQuery({ queryKey: ["runs", "onboarding"], queryFn: () => api.runs({ limit: 20 }), staleTime: 20_000 });

  const steps = useMemo<Step[]>(() => {
    const apiHealthy = (health.data?.status || "") === "ok";
    const hasSchedule = Boolean(schedule.data?.configured || schedule.data?.installed);
    const hasBackups = Boolean(backupSchedule.data?.schedule?.last_run_at);
    const hasConfigPath = Boolean((health.data?.config_path || "").trim());
    const loginEnabled = Boolean(health.data?.login_enabled);
    const notificationsConfigured = Boolean(
      notifications.data?.enabled &&
        (notifications.data?.slack?.enabled || notifications.data?.webhook?.enabled || notifications.data?.email?.enabled),
    );
    const hasSuccessfulRun = (runs.data ?? []).some((r) => r.success === true || r.clusters_ok || r.total_checks);
    return [
      {
        id: "connection",
        title: "Validate connection",
        description: "Confirm API health and resolved paths are correct.",
        done: Boolean(doneMap.connection || apiHealthy),
        link: "/settings?tab=connection",
      },
      {
        id: "config",
        title: "Finalize configuration",
        description: "Review config.yaml and ensure output/log paths and source targets are intentional.",
        done: Boolean(doneMap.config || (apiHealthy && hasConfigPath)),
        link: "/settings?tab=config",
      },
      {
        id: "auth-baseline",
        title: "Apply access baseline",
        description: "Validate auth mode, roles, and session policy before first release use.",
        done: Boolean(doneMap["auth-baseline"] || loginEnabled),
        link: "/settings?tab=access",
      },
      {
        id: "schedule",
        title: "Configure schedule",
        description: "Set cadence and verify schedule health is green.",
        done: Boolean(doneMap.schedule || hasSchedule),
        link: "/settings?tab=schedule",
      },
      {
        id: "notifications",
        title: "Test notifications",
        description: "Send at least one test notification channel.",
        done: Boolean(doneMap.notifications || notificationsConfigured),
        link: "/settings?tab=notifications",
      },
      {
        id: "run",
        title: "Trigger and validate first run",
        description: "Execute one run and confirm summary/check artifacts are produced.",
        done: Boolean(doneMap.run || hasSuccessfulRun),
        link: "/settings?tab=runs",
      },
      {
        id: "backup",
        title: "Create first snapshot",
        description: "Create at least one backup for rollback confidence.",
        done: Boolean(doneMap.backup || hasBackups),
        link: "/settings?tab=access",
      },
    ];
  }, [
    doneMap,
    health.data?.status,
    health.data?.config_path,
    health.data?.login_enabled,
    schedule.data?.configured,
    schedule.data?.installed,
    backupSchedule.data?.schedule?.last_run_at,
    notifications.data?.enabled,
    notifications.data?.slack?.enabled,
    notifications.data?.webhook?.enabled,
    notifications.data?.email?.enabled,
    runs.data,
  ]);

  const completed = steps.filter((s) => s.done).length;
  const pct = Math.round((completed / steps.length) * 100);

  const setDone = (id: string, done: boolean) => setDoneMap((prev) => ({ ...prev, [id]: done }));

  return (
    <Card className="page-card">
      <Space direction="vertical" style={{ width: "100%" }} size={14}>
        <Typography.Title level={4} className="section-title">
          Guided First-Run Onboarding
        </Typography.Title>
        <Typography.Text type="secondary">
          Follow these steps once per environment. Progress is saved locally and can be replayed.
        </Typography.Text>
        <Progress percent={pct} size="small" />
        <List
          itemLayout="horizontal"
          dataSource={steps}
          renderItem={(item) => (
            <List.Item
              actions={[
                item.link ? (
                  <Link key="open" to={item.link}>
                    <Button size="small">Open</Button>
                  </Link>
                ) : null,
                <Button
                  key="toggle"
                  size="small"
                  type={item.done ? "default" : "primary"}
                  icon={item.done ? <CheckCircleOutlined /> : <ClockCircleOutlined />}
                  onClick={() => setDone(item.id, !item.done)}
                >
                  {item.done ? "Mark pending" : "Mark done"}
                </Button>,
              ]}
            >
              <List.Item.Meta
                title={
                  <Space size={8}>
                    {item.title}
                    <Tag color={item.done ? "success" : "default"}>{item.done ? "done" : "pending"}</Tag>
                  </Space>
                }
                description={item.description}
              />
            </List.Item>
          )}
        />
      </Space>
    </Card>
  );
}


