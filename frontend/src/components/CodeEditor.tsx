import { lazy, Suspense } from "react";
import { Skeleton } from "antd";

export type EditorLanguage = "json" | "yaml" | "html" | "csv" | "plaintext";

export type CodeEditorProps = {
  value: string;
  onChange?: (value: string) => void;
  language: EditorLanguage;
  height?: number;
  readOnly?: boolean;
  autoRevealLastLine?: boolean;
  jumpToLastSignal?: number;
};

export function inferEditorLanguage(name: string): EditorLanguage {
  const lower = name.toLowerCase();
  if (lower.endsWith(".json")) return "json";
  if (lower.endsWith(".yaml") || lower.endsWith(".yml")) return "yaml";
  if (lower.endsWith(".html") || lower.endsWith(".htm")) return "html";
  if (lower.endsWith(".csv")) return "csv";
  return "plaintext";
}

const LazyMonaco = lazy(() => import("./MonacoCodeEditor"));

/**
 * Public CodeEditor that lazy-loads Monaco only when actually rendered. This
 * keeps the initial bundle ~250kB lighter; pages that don't use the editor
 * (Dashboard, Insights) never download Monaco.
 */
export function CodeEditor(props: CodeEditorProps) {
  const height = props.height ?? 320;
  return (
    <Suspense
      fallback={
        <div style={{ height, borderRadius: 6, padding: 10 }}>
          <Skeleton active paragraph={{ rows: Math.max(2, Math.floor(height / 40)) }} title={false} />
        </div>
      }
    >
      <LazyMonaco {...props} />
    </Suspense>
  );
}
