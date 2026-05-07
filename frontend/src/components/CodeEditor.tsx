import MonacoEditor from "@monaco-editor/react";
import { useEffect, useMemo, useRef } from "react";
import { useAppTheme } from "../theme";
import type { editor as MonacoEditorApi } from "monaco-editor";

export type EditorLanguage = "json" | "yaml" | "html" | "csv" | "plaintext";

type Props = {
  value: string;
  onChange?: (value: string) => void;
  language: EditorLanguage;
  height?: number;
  readOnly?: boolean;
  autoRevealLastLine?: boolean;
};

export function inferEditorLanguage(name: string): EditorLanguage {
  const lower = name.toLowerCase();
  if (lower.endsWith(".json")) return "json";
  if (lower.endsWith(".yaml") || lower.endsWith(".yml")) return "yaml";
  if (lower.endsWith(".html") || lower.endsWith(".htm")) return "html";
  if (lower.endsWith(".csv")) return "csv";
  return "plaintext";
}

export function CodeEditor({
  value,
  onChange,
  language,
  height = 320,
  readOnly = false,
  autoRevealLastLine = false,
}: Props) {
  const { theme } = useAppTheme();
  const editorRef = useRef<MonacoEditorApi.IStandaloneCodeEditor | null>(null);
  const monacoTheme = useMemo(() => {
    if (theme === "light") return "light";
    return "vs-dark";
  }, [theme]);

  useEffect(() => {
    if (!autoRevealLastLine) return;
    const editor = editorRef.current;
    if (!editor) return;
    const lastLine = editor.getModel()?.getLineCount() ?? 1;
    editor.revealLine(lastLine);
    editor.setPosition({ lineNumber: lastLine, column: 1 });
  }, [autoRevealLastLine, value]);

  return (
    <MonacoEditor
      language={language}
      theme={monacoTheme}
      value={value}
      height={height}
      onChange={(next) => onChange?.(next ?? "")}
      onMount={(editor) => {
        editorRef.current = editor;
      }}
      options={{
        readOnly,
        minimap: { enabled: false },
        automaticLayout: true,
        fontSize: 13,
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
        scrollBeyondLastLine: false,
        wordWrap: "on",
        lineNumbers: "on",
      }}
    />
  );
}
