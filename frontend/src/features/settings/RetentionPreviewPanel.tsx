import { useState } from "react";
import { Alert, Button, Space, Tag, Typography } from "antd";
import { EyeOutlined } from "@ant-design/icons";
import { api } from "../../api/client";
import type { RetentionPreview, RetentionPreviewRule } from "../../api/types";
import { ItemList, middleEllipsis } from "../../ui";
import { formatBytes } from "../health/SupervisorPanel";
import { plural } from "../../utils/text";

/** Rows shown per rule; the total still counts every item. */
const MAX_ROWS = 20;

function ruleStatus(rule: RetentionPreviewRule): string {
  if (rule.error) return `Could not check: ${rule.error}`;
  if (!rule.enabled) return "Off: keeps everything.";
  if (rule.count === 0) return "Nothing past the limit.";
  return `${plural(rule.count, "item")}, ${formatBytes(rule.bytes)}`;
}

/**
 * Lists what the saved retention settings would remove now, without
 * removing anything (GET /api/v1/settings/retention/preview).
 */
export function RetentionPreviewPanel() {
  const [loading, setLoading] = useState(false);
  const [preview, setPreview] = useState<RetentionPreview | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      setPreview(await api.retentionPreview());
    } catch (e) {
      setPreview(null);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="retention-preview">
      <Space wrap size={8} align="center">
        <Button icon={<EyeOutlined />} loading={loading} onClick={() => void load()}>
          Preview what would be removed
        </Button>
        <Typography.Text type="secondary">Uses the saved settings. Nothing is deleted.</Typography.Text>
      </Space>
      {error ? <Alert style={{ marginTop: 10 }} type="error" showIcon title={error} /> : null}
      {preview ? (
        <div style={{ marginTop: 10 }}>
          <Typography.Text strong>
            {preview.total_count === 0
              ? "Nothing would be removed."
              : `${plural(preview.total_count, "item")} would be removed (${formatBytes(preview.total_bytes)}).`}
          </Typography.Text>
          {(preview.rules ?? []).map((rule) => (
            <div key={rule.id} style={{ marginTop: 8 }}>
              <Space size={8} wrap>
                <Typography.Text>{rule.title}</Typography.Text>
                <Tag>{ruleStatus(rule)}</Tag>
              </Space>
              <ItemList
                style={{ marginTop: 4 }}
                bordered
                items={rule.items.slice(0, MAX_ROWS)}
                itemKey={(it) => it.path}
                render={(it) => (
                  <>
                    <Typography.Text code title={it.path}>
                      {middleEllipsis(it.path, 72)}
                    </Typography.Text>
                    <Typography.Text type="secondary">
                      {formatBytes(it.bytes)}
                      {it.modified ? ` · ${it.modified.slice(0, 10)}` : ""}
                    </Typography.Text>
                  </>
                )}
              />
              {rule.items.length > MAX_ROWS ? (
                <Typography.Text type="secondary">and {plural(rule.items.length - MAX_ROWS, "more item")}</Typography.Text>
              ) : null}
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}
