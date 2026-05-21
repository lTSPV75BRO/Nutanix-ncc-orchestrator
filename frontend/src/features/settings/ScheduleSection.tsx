import { useMemo, useState } from "react";
import { Button, Card, Col, Input, InputNumber, Radio, Row, Select, Space, Switch, Typography } from "antd";
import { api } from "../../api/client";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";

type Props = {
  backendConfigPath: string;
  onError: (e: unknown) => void;
};

type Mode = "simple" | "advanced";

export function ScheduleSection({ backendConfigPath, onError }: Props) {
  const [mode, setMode] = useLocalStorageState<Mode>("settings.schedule.mode", "simple");
  const [type, setType] = useLocalStorageState("settings.schedule.type", "auto");
  const [action, setAction] = useLocalStorageState("settings.schedule.action", "create");
  const [cron, setCron] = useLocalStorageState("settings.schedule.cron", "");
  const [everyValue, setEveryValue] = useLocalStorageState("settings.schedule.everyValue", 4);
  const [everyUnit, setEveryUnit] = useLocalStorageState("settings.schedule.everyUnit", "h");
  const [config, setConfig] = useLocalStorageState("settings.schedule.config", "config.yaml");
  const [taskName, setTaskName] = useLocalStorageState("settings.schedule.taskName", "ncc-orchestrator");
  const [logPath, setLogPath] = useLocalStorageState("settings.schedule.logPath", "logs/ncc-scheduler.log");
  const [withLock, setWithLock] = useLocalStorageState("settings.schedule.withLock", true);
  const [printOnly, setPrintOnly] = useLocalStorageState("settings.schedule.printOnly", true);
  const [apply, setApply] = useLocalStorageState("settings.schedule.apply", false);
  const [healthOut, setHealthOut] = useState("");
  const [out, setOut] = useState("");

  const payload = useMemo(() => {
    const every = everyValue > 0 ? `${everyValue}${everyUnit}` : "";
    return {
      type: mode === "advanced" ? type : "auto",
      action: mode === "advanced" ? action : "create",
      cron: mode === "advanced" ? cron.trim() : "",
      every,
      config: config.trim(),
      log_path: logPath.trim(),
      with_lock: withLock,
      task_name: taskName.trim(),
      print_only: printOnly,
      apply,
    };
  }, [mode, type, action, cron, everyValue, everyUnit, config, logPath, withLock, taskName, printOnly, apply]);

  const load = async () => {
    try {
      const s = await api.loadSchedule();
      setType(s.type || "auto");
      setAction(s.action || "create");
      setCron(s.cron || "");
      setConfig(s.config || backendConfigPath || "config.yaml");
      setLogPath(s.log_path || "logs/ncc-scheduler.log");
      setWithLock(Boolean(s.with_lock ?? true));
      setTaskName(s.task_name || "ncc-orchestrator");
      setPrintOnly(Boolean(s.print_only));
      const m = String(s.every || "").match(/^(\d+)([mhd])$/);
      if (m) {
        setEveryValue(Number(m[1]));
        setEveryUnit(m[2]);
      }
      setMode((s.type !== "auto" || s.action !== "create" || (s.cron || "").trim()) ? "advanced" : "simple");
      setOut(JSON.stringify(s, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const loadHealth = async () => {
    try {
      const h = await api.scheduleHealth();
      setHealthOut(JSON.stringify(h, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const save = async () => {
    if (!payload.config) {
      setOut("Config path is required.");
      return;
    }
    try {
      const r = await api.saveSchedule(payload);
      setOut(JSON.stringify(r, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  return (
    <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Schedule
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          Use Simple mode unless you need platform-specific behavior.
        </Typography.Text>
        <div style={{ marginTop: 12, marginBottom: 12 }}>
          <Radio.Group value={mode} onChange={(e) => setMode(e.target.value)}>
            <Radio.Button value="simple">Simple</Radio.Button>
            <Radio.Button value="advanced">Advanced</Radio.Button>
          </Radio.Group>
        </div>
        <Row gutter={8}>
          <Col xs={24} md={6}>
            <Typography.Text type="secondary">Run every</Typography.Text>
            <InputNumber min={1} value={everyValue} onChange={(v) => setEveryValue(Number(v || 1))} style={{ width: "100%" }} />
          </Col>
          <Col xs={24} md={6}>
            <Typography.Text type="secondary">Unit</Typography.Text>
            <Select
              value={everyUnit}
              onChange={setEveryUnit}
              options={[
                { value: "m", label: "minutes" },
                { value: "h", label: "hours" },
                { value: "d", label: "days" },
              ]}
            />
          </Col>
          <Col xs={24} md={6}>
            <Typography.Text type="secondary">Config Path</Typography.Text>
            <Input value={config} onChange={(e) => setConfig(e.target.value)} />
          </Col>
          <Col xs={24} md={6}>
            <Typography.Text type="secondary">Task Name</Typography.Text>
            <Input value={taskName} onChange={(e) => setTaskName(e.target.value)} />
          </Col>
          <Col xs={24} md={6}>
            <Typography.Text type="secondary">Scheduler Log Path</Typography.Text>
            <Input value={logPath} onChange={(e) => setLogPath(e.target.value)} />
          </Col>
        </Row>
        <Space size={8} wrap style={{ marginTop: 12, marginBottom: 12 }}>
          <Button onClick={() => { setEveryValue(15); setEveryUnit("m"); }}>Every 15m</Button>
          <Button onClick={() => { setEveryValue(1); setEveryUnit("h"); }}>Every 1h</Button>
          <Button onClick={() => { setEveryValue(4); setEveryUnit("h"); }}>Every 4h</Button>
          <Button onClick={() => { setEveryValue(24); setEveryUnit("h"); }}>Every 24h</Button>
        </Space>
        <div style={{ opacity: mode === "advanced" ? 1 : 0.5, pointerEvents: mode === "advanced" ? "auto" : "none" }}>
          <Row gutter={8}>
            <Col xs={24} md={8}>
              <Typography.Text type="secondary">Scheduler type</Typography.Text>
              <Select
                value={type}
                onChange={setType}
                options={[
                  { value: "auto", label: "auto" },
                  { value: "cron", label: "cron" },
                  { value: "windows", label: "windows" },
                ]}
              />
            </Col>
            <Col xs={24} md={8}>
              <Typography.Text type="secondary">Action</Typography.Text>
              <Select
                value={action}
                onChange={setAction}
                options={[
                  { value: "create", label: "create/update" },
                  { value: "list", label: "list" },
                  { value: "remove", label: "remove" },
                  { value: "run-now", label: "run-now" },
                ]}
              />
            </Col>
            <Col xs={24} md={8}>
              <Typography.Text type="secondary">Cron</Typography.Text>
              <Input value={cron} onChange={(e) => setCron(e.target.value)} />
            </Col>
          </Row>
        </div>
        <Space size={16} style={{ marginTop: 16, marginBottom: 8 }}>
          <Space size={6}>
            <Switch checked={printOnly} onChange={setPrintOnly} />
            <Typography.Text>print-only</Typography.Text>
          </Space>
          <Space size={6}>
            <Switch checked={apply} onChange={setApply} />
            <Typography.Text>apply now</Typography.Text>
          </Space>
          <Space size={6}>
            <Switch checked={withLock} onChange={setWithLock} />
            <Typography.Text>flock overlap lock</Typography.Text>
          </Space>
        </Space>
        <pre>{JSON.stringify(payload, null, 2)}</pre>
        <Space size={8} style={{ marginTop: 12 }}>
          <Button onClick={load}>Load Schedule</Button>
          <Button onClick={loadHealth}>Scheduler Health</Button>
          <Button type="primary" onClick={save}>
            Save Schedule
          </Button>
        </Space>
        <div style={{ marginTop: 12 }}>
          <Typography.Text type="secondary">Scheduler Health</Typography.Text>
          <pre>{healthOut}</pre>
        </div>
        <div style={{ marginTop: 12 }}>
          <pre>{out}</pre>
        </div>
    </Card>
  );
}
