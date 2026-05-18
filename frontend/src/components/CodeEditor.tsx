import MonacoEditor from "@monaco-editor/react";
import { useEffect, useMemo, useRef, useState } from "react";
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
  const fallbackRef = useRef<HTMLTextAreaElement | null>(null);
  const [fallback, setFallback] = useState(false);
  const monacoTheme = useMemo(() => {
    if (theme === "light") return "light";
    return "vs-dark";
  }, [theme]);

  useEffect(() => {
    const t = window.setTimeout(() => {
      if (!editorRef.current) {
        setFallback(true);
      }
    }, 600);
    return () => window.clearTimeout(t);
  }, [language, readOnly]);

  useEffect(() => {
    if (!autoRevealLastLine) return;
    const editor = editorRef.current;
    if (editor) {
      const lastLine = editor.getModel()?.getLineCount() ?? 1;
      editor.revealLine(lastLine);
      editor.setPosition({ lineNumber: lastLine, column: 1 });
      return;
    }
    const fallback = fallbackRef.current;
    if (!fallback) return;
    fallback.scrollTop = fallback.scrollHeight;
  }, [autoRevealLastLine, value]);

  if (fallback) {
    const isLight = theme === "light";
    return (
      <textarea
        ref={fallbackRef}
        value={value}
        onChange={(e) => onChange?.(e.target.value)}
        readOnly={readOnly}
        style={{
          width: "100%",
          height,
          resize: "vertical",
          borderRadius: 6,
          border: isLight ? "1px solid #d9d9d9" : "1px solid #30363d",
          padding: 10,
          fontSize: 13,
          lineHeight: 1.5,
          fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
          background: isLight ? (readOnly ? "#fafafa" : "#fff") : "#0d1117",
          color: isLight ? "rgba(0,0,0,0.88)" : "rgba(255,255,255,0.92)",
        }}
      />
    );
  }

  return (
    <MonacoEditor
      language={language}
      theme={monacoTheme}
      value={value}
      height={height}
      onChange={(next) => onChange?.(next ?? "")}
      onMount={(editor) => {
        editorRef.current = editor;
        setFallback(false);
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
