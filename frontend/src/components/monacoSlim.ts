// Monaco with only what the app uses: every editor feature, JSON (with its
// worker) and the YAML / INI / HTML highlighters.
//
// `import "monaco-editor"` also registers ~80 other languages and bundles the
// TypeScript, CSS and HTML language services with their workers (~8.5 MB in
// the build, mostly ts.worker), none of which the config and file editors use.
// Add a language here if inferEditorLanguage (CodeEditor.tsx) gains one.
import "monaco-editor/features/register.all.js";
import "monaco-editor/languages/features/json/register.js";
import "monaco-editor/languages/definitions/yaml/register.js";
import "monaco-editor/languages/definitions/ini/register.js";
import "monaco-editor/languages/definitions/html/register.js";

export * from "monaco-editor/editor/editor.api.js";
