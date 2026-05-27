import MonacoEditor, { loader } from "@monaco-editor/react";
import { useEffect, useMemo, useRef, useState } from "react";
import * as monaco from "monaco-editor";
import { useAppTheme } from "../theme";
import type { editor as MonacoEditorApi } from "monaco-editor";
import type { CodeEditorProps } from "./CodeEditor";

// Bundle Monaco's web workers as same-origin assets so the strict CSP
// (`script-src 'self'`, `worker-src` falls back to `'self'`) doesn't block
// them, and so the editor works in air-gapped Nutanix labs. Vite's `?worker`
// suffix produces a Worker constructor that loads a hashed JS file from
// /assets/. We need at minimum the editor worker (general tokenization) plus
// the JSON worker (used for our artifact JSON viewer); the YAML "language"
// is just basic-languages tokenization which runs on the main thread and
// doesn't need a worker.
import EditorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";
import JsonWorker from "monaco-editor/esm/vs/language/json/json.worker?worker";

// `@monaco-editor/react` defaults to fetching its loader script from
// https://cdn.jsdelivr.net, which is blocked by our strict CSP and wouldn't
// work offline. Pointing the loader at the already-bundled `monaco-editor`
// package keeps everything served from the UI server itself.
//
// Note: importing the umbrella `monaco-editor` package pulls every language
// contribution (~3.6 MB lazy chunk). That's acceptable here because Monaco is
// already lazy-loaded via React.lazy() in CodeEditor.tsx — initial page load
// is unaffected.
loader.config({ monaco });

// Wire web workers. Returning a real Worker (not null) is required: Monaco
// calls `.postMessage` on the result and crashes on null. Vite serves these
// from /assets/ as same-origin scripts, satisfying `worker-src 'self'`.
if (typeof self !== "undefined") {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (self as any).MonacoEnvironment = {
    getWorker(_workerId: string, label: string): Worker {
      if (label === "json") return new JsonWorker();
      return new EditorWorker();
    },
  };
}

// Custom Monaco themes that mirror each AppThemeKey ("light" / "dark" /
// "it-pro"). Surface and border colors are kept in lock-step with the Ant
// Design tokens defined in `theme.tsx` so the editor visually blends into
// the surrounding card and chrome instead of looking like a third-party
// embed. If you change the app palette, update the matching color block
// below as well.
//
// Token color set is intentionally minimal. We're not building VS Code; we
// just need readable, consistent YAML / JSON / log highlighting.
let monacoThemesRegistered = false;
function ensureAppThemes() {
  if (monacoThemesRegistered) return;
  monacoThemesRegistered = true;

  // Light: clean neutral zinc palette — matches `colorBgContainer: #ffffff`
  // and `colorBorder: #e4e4e7` from the app's light theme. Tokens use the
  // same blue / teal / purple accents that the light app uses for primary
  // actions / success / type tags so the editor "speaks the same language".
  monaco.editor.defineTheme("ncc-light", {
    base: "vs",
    inherit: true,
    rules: [
      { token: "comment", foreground: "71717a", fontStyle: "italic" },
      { token: "string", foreground: "0f766e" },
      { token: "number", foreground: "be185d" },
      { token: "keyword", foreground: "1d4ed8" },
      { token: "type", foreground: "7c3aed" },
      { token: "delimiter", foreground: "52525b" },
    ],
    colors: {
      "editor.background": "#ffffff",
      "editor.foreground": "#18181b",
      "editorLineNumber.foreground": "#a1a1aa",
      "editorLineNumber.activeForeground": "#18181b",
      "editor.selectionBackground": "#dbeafe",
      "editor.inactiveSelectionBackground": "#e4e4e7",
      "editorCursor.foreground": "#2563eb",
      "editor.lineHighlightBackground": "#fafafa",
      "editorIndentGuide.background": "#f4f4f5",
      "editorIndentGuide.activeBackground": "#e4e4e7",
      "editorWidget.background": "#fafafa",
      "editorWidget.border": "#e4e4e7",
    },
  });

  // Dark: near-black neutral charcoal — matches `colorBgContainer: #141414`
  // and `colorBorder: #262626`. Token colors are deliberately desaturated
  // (light purple / teal / amber) so they read clearly on near-black
  // without the orange-brown / saturated red that the default `vs-dark`
  // theme uses for strings and keywords.
  monaco.editor.defineTheme("ncc-dark", {
    base: "vs-dark",
    inherit: true,
    rules: [
      { token: "comment", foreground: "71717a", fontStyle: "italic" },
      { token: "string", foreground: "5eead4" },
      { token: "number", foreground: "fbbf24" },
      { token: "keyword", foreground: "c4b5fd" },
      { token: "type", foreground: "f0abfc" },
      { token: "delimiter", foreground: "a1a1aa" },
    ],
    colors: {
      "editor.background": "#141414",
      "editor.foreground": "#ededed",
      "editorLineNumber.foreground": "#52525b",
      "editorLineNumber.activeForeground": "#d4d4d8",
      "editor.selectionBackground": "#3b2f6b",
      "editor.inactiveSelectionBackground": "#262626",
      "editorCursor.foreground": "#a78bfa",
      "editor.lineHighlightBackground": "#1a1a1a",
      "editorIndentGuide.background": "#1f1f1f",
      "editorIndentGuide.activeBackground": "#3f3f46",
      "editorWidget.background": "#0f0f0f",
      "editorWidget.border": "#262626",
    },
  });

  // IT-Pro: deep navy / teal — unchanged because the it-pro app theme is
  // also unchanged. Kept here for completeness so the editor switches
  // cleanly between all three modes.
  monaco.editor.defineTheme("ncc-it-pro", {
    base: "vs-dark",
    inherit: true,
    rules: [
      { token: "comment", foreground: "64748b", fontStyle: "italic" },
      { token: "string", foreground: "5eead4" },
      { token: "number", foreground: "f59e0b" },
      { token: "keyword", foreground: "38bdf8" },
      { token: "type", foreground: "14b8a6" },
      { token: "delimiter", foreground: "94a3b8" },
    ],
    colors: {
      "editor.background": "#152238",
      "editor.foreground": "#dbeafe",
      "editorLineNumber.foreground": "#475569",
      "editorLineNumber.activeForeground": "#bae6fd",
      "editor.selectionBackground": "#1e3a5f",
      "editor.inactiveSelectionBackground": "#1f3b57",
      "editorCursor.foreground": "#14b8a6",
      "editor.lineHighlightBackground": "#1c2c47",
      "editorIndentGuide.background": "#1e293b",
      "editorIndentGuide.activeBackground": "#334155",
      "editorWidget.background": "#0b1220",
      "editorWidget.border": "#334155",
    },
  });
}

export default function MonacoCodeEditor({
  value,
  onChange,
  language,
  height = 320,
  readOnly = false,
  autoRevealLastLine = false,
  jumpToLastSignal = 0,
}: CodeEditorProps) {
  const { theme } = useAppTheme();
  const editorRef = useRef<MonacoEditorApi.IStandaloneCodeEditor | null>(null);
  const fallbackRef = useRef<HTMLTextAreaElement | null>(null);
  const [fallback, setFallback] = useState(false);
  // Map the app theme to a registered Monaco theme. Custom themes must be
  // defined before MonacoEditor mounts, so we register on first render.
  ensureAppThemes();
  const monacoTheme = useMemo(() => {
    if (theme === "light") return "ncc-light";
    if (theme === "it-pro") return "ncc-it-pro";
    return "ncc-dark";
  }, [theme]);

  // When the user toggles the app theme while the editor is open, re-apply
  // the matching Monaco theme so colors update without remounting.
  useEffect(() => {
    if (editorRef.current) {
      monaco.editor.setTheme(monacoTheme);
    }
  }, [monacoTheme]);

  useEffect(() => {
    const t = window.setTimeout(() => {
      if (!editorRef.current) {
        setFallback(true);
      }
    }, 600);
    return () => window.clearTimeout(t);
  }, [language, readOnly]);

  useEffect(() => {
    if (!autoRevealLastLine && jumpToLastSignal <= 0) return;
    const revealToBottom = () => {
      const editor = editorRef.current;
      if (editor) {
        const lastLine = editor.getModel()?.getLineCount() ?? 1;
        editor.revealLineInCenterIfOutsideViewport(lastLine);
        editor.setPosition({ lineNumber: lastLine, column: 1 });
        editor.setScrollTop(editor.getScrollHeight());
        return;
      }
      const fb = fallbackRef.current;
      if (!fb) return;
      fb.scrollTop = fb.scrollHeight;
    };
    const raf = window.requestAnimationFrame(() => {
      revealToBottom();
      window.requestAnimationFrame(revealToBottom);
    });
    return () => window.cancelAnimationFrame(raf);
  }, [autoRevealLastLine, jumpToLastSignal, value]);

  if (fallback) {
    // Mirror the active app theme so the fallback <textarea> doesn't visually
    // jump out as a "different surface" while Monaco is loading or when it
    // fails to mount. Surfaces match `theme.tsx` colorBgContainer values.
    const fallbackPalette =
      theme === "light"
        ? {
            border: "1px solid #e4e4e7",
            background: readOnly ? "#fafafa" : "#ffffff",
            color: "#18181b",
          }
        : theme === "it-pro"
          ? {
              border: "1px solid #334155",
              background: readOnly ? "#0b1220" : "#152238",
              color: "#dbeafe",
            }
          : {
              border: "1px solid #262626",
              background: readOnly ? "#0f0f0f" : "#141414",
              color: "#ededed",
            };
    return (
      <textarea
        ref={fallbackRef}
        value={value}
        onChange={(e) => onChange?.(e.target.value)}
        readOnly={readOnly}
        aria-label="Code editor (fallback)"
        spellCheck={false}
        autoComplete="off"
        autoCorrect="off"
        autoCapitalize="off"
        style={{
          width: "100%",
          height,
          resize: "vertical",
          borderRadius: 6,
          border: fallbackPalette.border,
          padding: 10,
          fontSize: 13,
          lineHeight: 1.5,
          fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
          background: fallbackPalette.background,
          color: fallbackPalette.color,
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
        if (autoRevealLastLine) {
          const lastLine = editor.getModel()?.getLineCount() ?? 1;
          editor.revealLineInCenterIfOutsideViewport(lastLine);
          editor.setPosition({ lineNumber: lastLine, column: 1 });
          editor.setScrollTop(editor.getScrollHeight());
        }
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
