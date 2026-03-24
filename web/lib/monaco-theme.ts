import type { Monaco } from "@monaco-editor/react";

let themesRegistered = false;

export function defineBruinMonacoThemes(monaco: Monaco) {
  if (themesRegistered) {
    return;
  }

  monaco.editor.defineTheme("bruin-vs", {
    base: "vs",
    inherit: true,
    semanticHighlighting: true,
    rules: [],
    colors: {},
    semanticTokenColors: {
      schema: "#92400e",
      table: "#0f766e",
      column: "#1d4ed8",
    },
  });

  monaco.editor.defineTheme("bruin-vs-dark", {
    base: "vs-dark",
    inherit: true,
    semanticHighlighting: true,
    rules: [],
    colors: {},
    semanticTokenColors: {
      schema: "#fbbf24",
      table: "#5eead4",
      column: "#93c5fd",
    },
  });

  themesRegistered = true;
}
