import type { ReactNode } from "react";
import { Button } from "antd";
import { ExclamationCircleOutlined } from "@ant-design/icons";

/**
 * Sticky bar along the bottom of a form page while it has unsaved changes:
 * what is pending, Discard, and Save. Renders nothing when clean, so pages can
 * mount it unconditionally.
 */
export function SaveBar({
  dirty,
  saving = false,
  onSave,
  onDiscard,
  message = "You have unsaved changes.",
  saveLabel = "Save",
  saveDisabled = false,
}: {
  dirty: boolean;
  saving?: boolean;
  onSave: () => void;
  onDiscard?: () => void;
  message?: ReactNode;
  saveLabel?: string;
  /** e.g. while the form has validation errors. */
  saveDisabled?: boolean;
}) {
  if (!dirty) return null;
  return (
    <div className="ui-savebar" role="region" aria-label="Unsaved changes">
      <span className="ui-savebar-message">
        <ExclamationCircleOutlined aria-hidden="true" /> {message}
      </span>
      <span className="ui-savebar-actions">
        {onDiscard ? (
          <Button onClick={onDiscard} disabled={saving}>
            Discard
          </Button>
        ) : null}
        <Button type="primary" onClick={onSave} loading={saving} disabled={saveDisabled}>
          {saveLabel}
        </Button>
      </span>
    </div>
  );
}
