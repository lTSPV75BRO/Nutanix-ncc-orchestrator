import { useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Divider,
  Form,
  InputNumber,
  Select,
  Space,
  Switch,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import { ClockCircleOutlined, DatabaseOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../../api/client";
import { notify, notifyError } from "../../../notify";
import { formatDateTime } from "../../../utils/datetime";
import { IconTitle } from "../../../ui";

const BACKUP_EVERY_OPTIONS = [
  { value: "1h", label: "Every hour" },
  { value: "6h", label: "Every 6 hours" },
  { value: "12h", label: "Every 12 hours" },
  { value: "24h", label: "Daily" },
  { value: "7d", label: "Weekly" },
  { value: "14d", label: "Every 2 weeks" },
  { value: "30d", label: "Every month" },
];

export function ScheduledBackupCard() {
  const qc = useQueryClient();
  const query = useQuery({
    queryKey: ["settings", "backup-schedule"],
    queryFn: api.getBackupSchedule,
  });
  const data = query.data;
  const sched = data?.schedule;
  const keyConfigured = Boolean(data?.key_configured);

  const [enabled, setEnabled] = useState(false);
  const [every, setEvery] = useState("24h");
  const [encrypt, setEncrypt] = useState(false);
  const [retain, setRetain] = useState<number>(7);

  useEffect(() => {
    if (sched) {
      setEnabled(Boolean(sched.enabled));
      setEvery(sched.every || "24h");
      setEncrypt(Boolean(sched.encrypt));
      setRetain(typeof sched.retain === "number" ? sched.retain : 7);
    }
  }, [sched]);

  const saveMut = useMutation({
    mutationFn: (runNow: boolean) =>
      api.updateBackupSchedule({ enabled, every, encrypt, retain, run_now: runNow }),
    onSuccess: (res, runNow) => {
      notify.success(runNow ? "Schedule saved and a snapshot was taken." : "Scheduled backups updated.");
      void qc.invalidateQueries({ queryKey: ["settings", "backup-schedule"] });
      void qc.invalidateQueries({ queryKey: ["settings", "backups"] });
      void res;
    },
    onError: (e) => notifyError(e, "Could not update scheduled backups"),
  });

  return (
    <Card
      className="page-card"
      title={<IconTitle icon={<ClockCircleOutlined />}>Scheduled backups</IconTitle>}
    >
      <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
        Automatically take server-side snapshots on a fixed cadence. Backups run in-process, so
        encryption uses the API server&apos;s configured key — a detached timer could not. Older
        scheduled snapshots are pruned to your retention count; manual and pre-update rollback
        points are never touched.
      </Typography.Paragraph>

      {!keyConfigured ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          title="No backup encryption key configured"
          description="Set NCC_BACKUP_KEY_FILE, NCC_BACKUP_KEY, or NCC_BACKUP_PASSPHRASE on the API server to enable encrypted scheduled backups"
        />
      ) : null}

      <Form layout="vertical" style={{ maxWidth: 480 }}>
        <Form.Item label="Enable scheduled backups" htmlFor="backup-schedule-enabled">
          <Switch id="backup-schedule-enabled" checked={enabled} onChange={setEnabled} />
        </Form.Item>
        <Form.Item label="Frequency" htmlFor="backup-schedule-every">
          <Select
            id="backup-schedule-every"
            disabled={!enabled}
            value={every}
            onChange={setEvery}
            options={BACKUP_EVERY_OPTIONS}
            style={{ width: 220 }}
          />
        </Form.Item>
        <Form.Item label="Keep newest (retention)" htmlFor="backup-schedule-retain" extra="0 keeps every scheduled snapshot.">
          <InputNumber
            id="backup-schedule-retain"
            disabled={!enabled}
            min={0}
            max={365}
            value={retain}
            onChange={(v) => setRetain(typeof v === "number" ? v : 0)}
          />
        </Form.Item>
        <Form.Item label="Encrypt at rest (AES-256-GCM)" htmlFor="backup-schedule-encrypt">
          <Tooltip title={keyConfigured ? "" : "Configure a backup key on the API server first."}>
            <Switch
              id="backup-schedule-encrypt"
              checked={encrypt && keyConfigured}
              disabled={!enabled || !keyConfigured}
              onChange={setEncrypt}
            />
          </Tooltip>
        </Form.Item>
        <Space>
          <Button
            type="primary"
            loading={saveMut.isPending}
            onClick={() => saveMut.mutate(false)}
          >
            Save
          </Button>
          <Button
            icon={<DatabaseOutlined />}
            loading={saveMut.isPending}
            disabled={!enabled}
            onClick={() => saveMut.mutate(true)}
          >
            Save and back up now
          </Button>
        </Space>
      </Form>

      {sched?.last_run_at ? (
        <>
          <Divider style={{ margin: "16px 0" }} />
          <Space orientation="vertical" size={2}>
            <Typography.Text type="secondary">
              Last run: {formatDateTime(sched.last_run_at)}{" "}
              {sched.last_status === "ok" ? (
                <Tag color="green">ok</Tag>
              ) : sched.last_status === "error" ? (
                <Tag color="red">failed</Tag>
              ) : null}
            </Typography.Text>
            {sched.last_status === "ok" && sched.last_file ? (
              <Typography.Text type="secondary" className="mono" style={{ fontSize: 12 }}>
                {sched.last_file}
              </Typography.Text>
            ) : null}
            {sched.last_status === "error" && sched.last_error ? (
              <Typography.Text type="danger" style={{ fontSize: 12 }}>
                {sched.last_error}
              </Typography.Text>
            ) : null}
            {data?.next_run ? (
              <Typography.Text type="secondary">
                Next run (approx): {formatDateTime(data.next_run)}
              </Typography.Text>
            ) : null}
          </Space>
        </>
      ) : null}
    </Card>
  );
}
