import { useState } from "react";
import {
  Alert,
  Button,
  Card,
  Input,
  Modal,
  Popconfirm,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
  Upload,
} from "antd";
import { DatabaseOutlined, DeleteOutlined, DownloadOutlined, LockOutlined, PlusOutlined, ReloadOutlined, RollbackOutlined, SafetyCertificateOutlined, UploadOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../../api/client";
import { triggerBlobDownload } from "../../../utils/download";
import type {
  BackupEntry,
  BackupVerifyReport,
} from "../../../api/types";
import { notify, notifyError } from "../../../notify";
import { formatDateTime } from "../../../utils/datetime";
import { IconTitle } from "../../../ui";
import { BackupVerifyChecks, backupVerifyReportFrom } from "../BackupVerifyChecks";

// Human-readable byte size for the backups table.
function humanSize(bytes?: number): string {
  if (!bytes || bytes <= 0) return "—";
  const units = ["B", "KB", "MB", "GB"];
  let v = bytes;
  let u = 0;
  while (v >= 1024 && u < units.length - 1) {
    v /= 1024;
    u += 1;
  }
  return `${v.toFixed(v < 10 && u > 0 ? 1 : 0)} ${units[u]}`;
}

const BACKUP_KIND_META: Record<string, { color: string; label: string }> = {
  "pre-update": { color: "gold", label: "pre-update" },
  manual: { color: "blue", label: "manual" },
  other: { color: "default", label: "other" },
};

const MIN_BACKUP_PASSPHRASE_LEN = 8;
const SAFETY_TIER_TEXT = {
  stateChange: "This changes server state.",
  destructive: "This is destructive and may require manual recovery.",
  irreversible: "This action is irreversible.",
};

// passphraseIssue validates an encryption passphrase against its confirmation.
// Returns a human message when the backup should be blocked, or null when it's
// fine. When `required` is set (server policy NCC_BACKUP_ENCRYPTION=required and
// no server-side key covers this backup) an empty passphrase is blocked too.
function passphraseIssue(pass: string, confirm: string, required = false): string | null {
  const p = pass.trim();
  if (!p) return required ? "Encryption is required for backups on this server. Enter a passphrase." : null;
  if (p.length < MIN_BACKUP_PASSPHRASE_LEN) {
    return `Use at least ${MIN_BACKUP_PASSPHRASE_LEN} characters (a lost passphrase cannot be recovered).`;
  }
  if (p !== confirm.trim()) return "Passphrases do not match.";
  return null;
}

export function BackupRestoreCard() {
  const qc = useQueryClient();
  const backupsQuery = useQuery({ queryKey: ["settings", "backups"], queryFn: api.listBackups });
  const [downloading, setDownloading] = useState(false);
  const [restoring, setRestoring] = useState(false);
  const [busyName, setBusyName] = useState<string | null>(null);
  const [downloadModalOpen, setDownloadModalOpen] = useState(false);
  const [downloadPass, setDownloadPass] = useState("");
  const [downloadPassConfirm, setDownloadPassConfirm] = useState("");
  const [restoreFile, setRestoreFile] = useState<File | null>(null);
  const [restorePass, setRestorePass] = useState("");
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [createPass, setCreatePass] = useState("");
  const [createPassConfirm, setCreatePassConfirm] = useState("");
  const [restoreNamedEntry, setRestoreNamedEntry] = useState<BackupEntry | null>(null);
  const [restoreNamedPass, setRestoreNamedPass] = useState("");
  const [verifyEntry, setVerifyEntry] = useState<BackupEntry | null>(null);
  const [verifyPass, setVerifyPass] = useState("");
  const [verifyName, setVerifyName] = useState<string | null>(null);
  const [verifyResult, setVerifyResult] = useState<{ name: string; report: BackupVerifyReport } | null>(null);

  const backups = (backupsQuery.data?.backups ?? []) as BackupEntry[];
  const encPolicy = backupsQuery.data?.encryption_policy;
  // Downloads leave the server, so only a passphrase can protect them.
  const downloadPassRequired = Boolean(encPolicy?.required);
  // Server-side snapshots can be sealed with the server key instead.
  const createPassRequired = Boolean(encPolicy?.required && !encPolicy?.server_key_configured);
  const rollback = backups.find((b) => b.rollback_candidate);
  const refresh = () => qc.invalidateQueries({ queryKey: ["settings", "backups"] });

  const handleDownload = async () => {
    const pass = downloadPass.trim();
    setDownloading(true);
    try {
      const { blob, filename } = await api.downloadBackup(pass || undefined);
      triggerBlobDownload(blob, filename);
      notify.success(pass ? "Encrypted backup downloaded." : "Backup downloaded.");
      setDownloadModalOpen(false);
      setDownloadPass("");
      setDownloadPassConfirm("");
    } catch (e) {
      notifyError(e, "Could not create backup");
    } finally {
      setDownloading(false);
    }
  };

  const createMut = useMutation({
    mutationFn: (passphrase?: string) => api.createBackup(passphrase),
    onSuccess: (res) => {
      notify.success(res.message ?? "Snapshot created.");
      setCreateModalOpen(false);
      setCreatePass("");
      setCreatePassConfirm("");
      void refresh();
    },
    onError: (e) => notifyError(e, "Could not create snapshot"),
  });

  const deleteMut = useMutation({
    mutationFn: (name: string) => api.deleteBackup(name),
    onSuccess: () => {
      notify.success("Backup deleted.");
      void refresh();
    },
    onError: (e) => notifyError(e, "Could not delete backup"),
  });

  // Poll the backend health endpoint until the restarted stack answers, then
  // reload so the app re-bootstraps against the restored config/session.
  const waitForRestartAndReload = async () => {
    const deadline = Date.now() + 120_000;
    // The old api/ui servers are being torn down; give them a head start.
    await new Promise((r) => setTimeout(r, 4000));
    while (Date.now() < deadline) {
      try {
        const resp = await fetch("/api/v1/health", { cache: "no-store", credentials: "include" });
        if (resp.ok) break;
      } catch {
        // still down — keep waiting
      }
      await new Promise((r) => setTimeout(r, 2000));
    }
    window.location.reload();
  };

  const announceRestore = (restarting: boolean, message?: string) => {
    if (restarting) {
      Modal.success({
        title: "Backup restored — restarting",
        content:
          message ??
          "The stack is restarting to load the restored data. This page will reconnect automatically.",
        okText: "OK",
      });
      void waitForRestartAndReload();
    } else {
      Modal.success({
        title: "Backup restored",
        content:
          message ??
          "Restart the services (v2-stop then v2-start) for the restored data to take effect.",
      });
    }
  };

  const doRestoreUpload = async (file: File, passphrase?: string) => {
    setRestoring(true);
    try {
      const res = await api.restoreBackup(file, passphrase);
      const data = (res.data ?? {}) as { restarting?: boolean };
      setRestoreFile(null);
      setRestorePass("");
      announceRestore(Boolean(data.restarting), res.message);
    } catch (e) {
      notifyError(e, "Restore failed");
    } finally {
      setRestoring(false);
    }
  };

  const doRestoreNamed = async (name: string, passphrase?: string) => {
    setBusyName(name);
    try {
      const res = await api.restoreNamedBackup(name, passphrase);
      const data = (res.data ?? {}) as { restarting?: boolean };
      setRestoreNamedEntry(null);
      setRestoreNamedPass("");
      announceRestore(Boolean(data.restarting), res.message);
    } catch (e) {
      notifyError(e, "Restore failed");
    } finally {
      setBusyName(null);
    }
  };

  const downloadNamed = async (name: string) => {
    setBusyName(name);
    try {
      const { blob, filename } = await api.downloadNamedBackup(name);
      triggerBlobDownload(blob, filename);
    } catch (e) {
      notifyError(e, "Could not download backup");
    } finally {
      setBusyName(null);
    }
  };

  const restoreBody = (entry: Pick<BackupEntry, "name"> & Partial<BackupEntry>) => {
    const isRollback = Boolean(entry.rollback_candidate);
    const hasStack = Boolean(entry.includes_stack);
    return (
      <div>
        <Typography.Paragraph>
          This overwrites the install directory with the contents of <b>{entry.name}</b> —
          configuration and referenced files, local accounts and roles, the API token, and
          scheduler/notification state
          {hasStack
            ? ", plus the previous software version (orchestrator, API, UI, and frontend)."
            : "."}
        </Typography.Paragraph>
        {isRollback && !hasStack ? (
          <Typography.Paragraph>
            This snapshot was taken before versioned rollback existed, so the running software
            version stays as it is. Only configuration and accounts are restored.
          </Typography.Paragraph>
        ) : null}
        <Typography.Paragraph style={{ marginBottom: 0 }}>
          {isRollback ? "The stack rolls back and " : "The stack "}
          <b>restarts automatically</b> afterward to load the restored data — this page reconnects
          on its own, and you may be asked to sign in again if the API token or your account
          changed.
        </Typography.Paragraph>
      </div>
    );
  };

  // Opens the restore confirmation. Encrypted snapshots need a passphrase, so we
  // use a stateful modal (rendered below) rather than the static Modal.confirm.
  const confirmRestoreNamed = (entry: BackupEntry) => {
    setRestoreNamedPass("");
    setRestoreNamedEntry(entry);
  };

  // verify runs a non-destructive integrity check (v2-restore --verify-only).
  // Encrypted snapshots need the passphrase (verifying also confirms it), so we
  // collect it in a modal first; plaintext snapshots verify in one click.
  const doVerify = async (name: string, passphrase?: string) => {
    setVerifyName(name);
    try {
      const res = await api.verifyNamedBackup(name, passphrase);
      setVerifyEntry(null);
      setVerifyPass("");
      const report = backupVerifyReportFrom(res.data);
      if (report) {
        setVerifyResult({ name, report });
      } else {
        Modal.success({
          title: "Backup verified",
          content: res.message ?? "The archive is intact and restorable.",
        });
      }
    } catch (e) {
      // A failed check still returns the check list: show it instead of a toast.
      const report = backupVerifyReportFrom((e as { data?: unknown })?.data);
      if (report) {
        setVerifyEntry(null);
        setVerifyPass("");
        setVerifyResult({ name, report });
      } else {
        notifyError(e, "Verification failed");
      }
    } finally {
      setVerifyName(null);
    }
  };

  const confirmVerify = (entry: BackupEntry) => {
    if (entry.encrypted) {
      setVerifyPass("");
      setVerifyEntry(entry);
      return;
    }
    void doVerify(entry.name);
  };

  // beforeUpload intercepts the selected file and opens a destructive-action
  // confirmation (with an optional decryption passphrase field), returning false
  // so antd never auto-uploads it.
  const confirmRestoreUpload = (file: File): boolean => {
    setRestorePass("");
    setRestoreFile(file);
    return false;
  };

  const columns = [
    {
      title: "Name",
      dataIndex: "name",
      render: (n: string) => (
        <Typography.Text className="mono" style={{ fontSize: 12 }}>
          {n}
        </Typography.Text>
      ),
    },
    {
      title: "Type",
      dataIndex: "kind",
      render: (k: string, rec: BackupEntry) => {
        const meta = BACKUP_KIND_META[k] ?? BACKUP_KIND_META.other;
        return (
          <Space size={4}>
            <Tag color={meta.color}>{meta.label}</Tag>
            {rec.rollback_candidate ? <Tag color="volcano">latest rollback</Tag> : null}
            {rec.encrypted ? (
              <Tooltip title="Encrypted at rest (AES-256-GCM) — restore needs the passphrase">
                <Tag color="geekblue" icon={<LockOutlined />}>
                  encrypted
                </Tag>
              </Tooltip>
            ) : null}
          </Space>
        );
      },
    },
    { title: "Size", dataIndex: "size", render: (v: number) => humanSize(v) },
    {
      title: "Created",
      dataIndex: "mod_time",
      render: (v: string) => formatDateTime(v),
    },
    {
      title: "Actions",
      key: "actions",
      render: (_: unknown, rec: BackupEntry) => (
        <Space wrap>
          <Button
            size="small"
            danger
            icon={rec.rollback_candidate ? <RollbackOutlined /> : <UploadOutlined />}
            loading={busyName === rec.name}
            onClick={() => confirmRestoreNamed(rec)}
          >
            {rec.rollback_candidate ? "Roll back" : "Restore"}
          </Button>
          <Button
            size="small"
            icon={<SafetyCertificateOutlined />}
            loading={verifyName === rec.name}
            onClick={() => confirmVerify(rec)}
          >
            Verify
          </Button>
          <Button
            size="small"
            icon={<DownloadOutlined />}
            loading={busyName === rec.name}
            onClick={() => downloadNamed(rec.name)}
          >
            Download
          </Button>
          <Popconfirm
            title={`Delete ${rec.name}?`}
            description={`${SAFETY_TIER_TEXT.destructive} ${SAFETY_TIER_TEXT.irreversible}`}
            okText="Delete"
            okButtonProps={{ danger: true }}
            onConfirm={() => deleteMut.mutate(rec.name)}
          >
            <Button size="small" danger icon={<DeleteOutlined />}>
              Delete
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Card
      className="page-card"
      id="settings-backup"
      title={<IconTitle icon={<DatabaseOutlined />}>Backup &amp; restore</IconTitle>}
      extra={
        <Space>
          {rollback ? (
            <Tooltip title={`Roll back software version and configuration to ${rollback.name}`}>
              <Button
                icon={<RollbackOutlined />}
                danger
                loading={busyName === rollback.name}
                onClick={() => confirmRestoreNamed(rollback)}
              >
                Roll back last update
              </Button>
            </Tooltip>
          ) : null}
          <Button
            icon={<ReloadOutlined />}
            onClick={() => refresh()}
            loading={backupsQuery.isFetching}
          >
            Refresh
          </Button>
        </Space>
      }
    >
      <Typography.Paragraph type="secondary">
        A backup is a single <Typography.Text code>.tar.gz</Typography.Text> of this installation —
        configuration and referenced files, local accounts and roles, the API token,
        scheduler/notification state, the start settings (CORS, listen addresses, session TTL), the
        audit log, and the latest run&apos;s report. Backups are portable across operating systems
        and versions, so a Windows backup restores onto Linux or macOS. Restoring overwrites the
        current installation and then restarts the services automatically. Archives contain secrets —
        store them securely.
      </Typography.Paragraph>

      <Space wrap style={{ marginBottom: 16 }}>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => {
            setCreatePass("");
            setCreatePassConfirm("");
            setCreateModalOpen(true);
          }}
          loading={createMut.isPending}
        >
          Create snapshot
        </Button>
        <Button
          icon={<DownloadOutlined />}
          onClick={() => {
            setDownloadPass("");
            setDownloadPassConfirm("");
            setDownloadModalOpen(true);
          }}
          loading={downloading}
        >
          Download backup
        </Button>
        <Upload
          accept=".gz,.tgz,.tar.gz,.enc,application/gzip,application/octet-stream"
          showUploadList={false}
          beforeUpload={(file) => confirmRestoreUpload(file as unknown as File)}
        >
          <Button icon={<UploadOutlined />} danger loading={restoring}>
            Restore from file…
          </Button>
        </Upload>
      </Space>

      <Typography.Title level={2} className="section-subheading" style={{ marginTop: 0 }}>
        Server-side backups
      </Typography.Title>
      <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
        Snapshots saved on the server, including automatic <b>pre-update</b> backups taken before
        each in-app update (configuration plus the software version that was running). Restore any
        of them without re-uploading, or roll back the most recent update — including binaries and
        frontend — with one click.
      </Typography.Paragraph>
      <Table
        rowKey="name"
        size="small"
        loading={backupsQuery.isLoading}
        dataSource={backups}
        columns={columns}
        pagination={false}
        locale={{ emptyText: "No server-side backups yet. Create a snapshot or run an update." }}
      />

      <Modal
        title="Download backup"
        open={downloadModalOpen}
        onCancel={() => {
          setDownloadModalOpen(false);
          setDownloadPass("");
          setDownloadPassConfirm("");
        }}
        onOk={handleDownload}
        okText={downloadPass.trim() ? "Encrypt and download" : "Download"}
        okButtonProps={{
          loading: downloading,
          icon: <DownloadOutlined />,
          disabled: passphraseIssue(downloadPass, downloadPassConfirm, downloadPassRequired) !== null,
        }}
        cancelText="Cancel"
        destroyOnHidden
      >
        <Typography.Paragraph type="secondary" style={{ marginBottom: 12 }}>
          The archive contains secrets (API token, password hashes, SAML SP key, LDAP bind
          password). Optionally encrypt it at rest with <b>AES-256-GCM</b> by setting a passphrase —
          you&apos;ll need the same passphrase to restore. Leave blank to download an unencrypted{" "}
          <Typography.Text code>.tar.gz</Typography.Text>.
        </Typography.Paragraph>
        <Space orientation="vertical" size={8} style={{ width: "100%" }}>
          <Input.Password
            autoFocus
            placeholder={downloadPassRequired ? "Encryption passphrase (required)" : "Encryption passphrase (optional)"}
            value={downloadPass}
            onChange={(e) => setDownloadPass(e.target.value)}
            allowClear
          />
          {downloadPass.trim() ? (
            <Input.Password
              placeholder="Confirm passphrase"
              value={downloadPassConfirm}
              onChange={(e) => setDownloadPassConfirm(e.target.value)}
              allowClear
            />
          ) : null}
          {passphraseIssue(downloadPass, downloadPassConfirm, downloadPassRequired) ? (
            <Typography.Text type="danger" style={{ fontSize: 12 }}>
              {passphraseIssue(downloadPass, downloadPassConfirm, downloadPassRequired)}
            </Typography.Text>
          ) : null}
        </Space>
      </Modal>

      <Modal
        title="Restore from uploaded backup?"
        open={restoreFile !== null}
        onCancel={() => {
          setRestoreFile(null);
          setRestorePass("");
        }}
        onOk={() => {
          if (restoreFile) void doRestoreUpload(restoreFile, restorePass.trim() || undefined);
        }}
        okText="Restore and overwrite"
        okButtonProps={{ danger: true, loading: restoring }}
        cancelText="Cancel"
        width={540}
        destroyOnHidden
      >
        {restoreFile ? restoreBody({ name: restoreFile.name }) : null}
        <Typography.Paragraph type="secondary" style={{ marginTop: 4, marginBottom: 6 }}>
          If this archive was encrypted (<Typography.Text code>v2-backup --encrypt</Typography.Text>{" "}
          or a passphrase-protected download), enter its passphrase. Leave blank for an unencrypted
          archive.
        </Typography.Paragraph>
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 8 }}
          title={SAFETY_TIER_TEXT.destructive}
          description="Restoring overwrites current runtime config, users, and tokens, then restarts services"
        />
        <Input.Password
          placeholder="Decryption passphrase (only for encrypted archives)"
          value={restorePass}
          onChange={(e) => setRestorePass(e.target.value)}
          allowClear
        />
      </Modal>

      <Modal
        title="Create snapshot"
        open={createModalOpen}
        onCancel={() => {
          setCreateModalOpen(false);
          setCreatePass("");
          setCreatePassConfirm("");
        }}
        onOk={() => createMut.mutate(createPass.trim() || undefined)}
        okText={createPass.trim() ? "Create encrypted snapshot" : "Create snapshot"}
        okButtonProps={{
          loading: createMut.isPending,
          icon: <PlusOutlined />,
          disabled: passphraseIssue(createPass, createPassConfirm, createPassRequired) !== null,
        }}
        cancelText="Cancel"
        destroyOnHidden
      >
        <Typography.Paragraph type="secondary" style={{ marginBottom: 12 }}>
          Saves a snapshot on the server under <Typography.Text code>backups/</Typography.Text>. It
          contains secrets — optionally seal it at rest with <b>AES-256-GCM</b> by setting a
          passphrase (stored as <Typography.Text code>.tar.gz.enc</Typography.Text>; restoring it
          later needs the same passphrase). Leave blank for a plain snapshot.
        </Typography.Paragraph>
        <Space orientation="vertical" size={8} style={{ width: "100%" }}>
          <Input.Password
            autoFocus
            placeholder={
              createPassRequired
                ? "Encryption passphrase (required)"
                : encPolicy?.server_key_configured
                  ? "Passphrase (optional — the server backup key is used otherwise)"
                  : "Encryption passphrase (optional)"
            }
            value={createPass}
            onChange={(e) => setCreatePass(e.target.value)}
            allowClear
          />
          {createPass.trim() ? (
            <Input.Password
              placeholder="Confirm passphrase"
              value={createPassConfirm}
              onChange={(e) => setCreatePassConfirm(e.target.value)}
              allowClear
            />
          ) : null}
          {passphraseIssue(createPass, createPassConfirm, createPassRequired) ? (
            <Typography.Text type="danger" style={{ fontSize: 12 }}>
              {passphraseIssue(createPass, createPassConfirm, createPassRequired)}
            </Typography.Text>
          ) : null}
        </Space>
      </Modal>

      <Modal
        title={
          restoreNamedEntry?.rollback_candidate
            ? "Roll back the last update?"
            : "Restore this backup?"
        }
        open={restoreNamedEntry !== null}
        onCancel={() => {
          setRestoreNamedEntry(null);
          setRestoreNamedPass("");
        }}
        onOk={() => {
          if (restoreNamedEntry) {
            void doRestoreNamed(restoreNamedEntry.name, restoreNamedPass.trim() || undefined);
          }
        }}
        okText={
          restoreNamedEntry?.rollback_candidate ? "Roll back and restart" : "Restore and restart"
        }
        okButtonProps={{ danger: true, loading: busyName === restoreNamedEntry?.name }}
        cancelText="Cancel"
        width={540}
        destroyOnHidden
      >
        {restoreNamedEntry
          ? restoreBody(restoreNamedEntry)
          : null}
        {restoreNamedEntry?.encrypted ? (
          <>
            <Typography.Paragraph type="secondary" style={{ marginTop: 4, marginBottom: 6 }}>
              This snapshot is <b>encrypted</b>. Enter the passphrase used when it was created.
            </Typography.Paragraph>
            <Input.Password
              autoFocus
              placeholder="Decryption passphrase"
              value={restoreNamedPass}
              onChange={(e) => setRestoreNamedPass(e.target.value)}
              allowClear
            />
          </>
        ) : null}
      </Modal>

      <Modal
        title="Verify encrypted backup"
        open={verifyEntry !== null}
        onCancel={() => {
          setVerifyEntry(null);
          setVerifyPass("");
        }}
        onOk={() => {
          if (verifyEntry) void doVerify(verifyEntry.name, verifyPass.trim() || undefined);
        }}
        okText="Verify"
        okButtonProps={{
          icon: <SafetyCertificateOutlined />,
          loading: verifyName === verifyEntry?.name,
          disabled: !verifyPass.trim(),
        }}
        cancelText="Cancel"
        width={520}
        destroyOnHidden
      >
        <Typography.Paragraph type="secondary" style={{ marginBottom: 8 }}>
          Checks <b>{verifyEntry?.name}</b> without restoring it or touching the live stack:
          decryption, every file readable, safe paths, checksums, config, secrets, report paths,
          and version. It&apos;s encrypted, so enter the passphrase.
        </Typography.Paragraph>
        <Input.Password
          autoFocus
          placeholder="Decryption passphrase"
          value={verifyPass}
          onChange={(e) => setVerifyPass(e.target.value)}
          onPressEnter={() => {
            if (verifyEntry && verifyPass.trim()) void doVerify(verifyEntry.name, verifyPass.trim());
          }}
          allowClear
        />
      </Modal>

      <Modal
        title={verifyResult ? `Backup check: ${verifyResult.name}` : "Backup check"}
        open={verifyResult !== null}
        onCancel={() => setVerifyResult(null)}
        footer={null}
        width={680}
        destroyOnHidden
      >
        {verifyResult ? <BackupVerifyChecks report={verifyResult.report} /> : null}
      </Modal>
    </Card>
  );
}
