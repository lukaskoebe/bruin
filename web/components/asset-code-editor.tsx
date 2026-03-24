"use client";

import type { Monaco } from "@monaco-editor/react";
import type * as MonacoNS from "monaco-editor";
import { lazy, Suspense } from "react";

import { loadMonacoEditorModule } from "@/lib/load-monaco-editor";
import { WebAsset } from "@/lib/types";

const MonacoEditor = lazy(async () => {
  const module = await loadMonacoEditorModule();
  return { default: module.default };
});

export function AssetCodeEditor({
  asset,
  editorModelPath,
  editorValue,
  editorHighlighted,
  helpMode,
  highlightStyle,
  mobile,
  monacoTheme,
  onChange,
  onMount,
}: {
  asset: WebAsset | null;
  editorModelPath: string;
  editorValue: string;
  editorHighlighted: boolean;
  helpMode: boolean;
  highlightStyle?: React.CSSProperties;
  mobile: boolean;
  monacoTheme: string;
  onChange: (value?: string) => void;
  onMount: (editor: MonacoNS.editor.IStandaloneCodeEditor, monaco: Monaco) => void;
}) {
  return (
    <div
      className={`${mobile ? "min-h-[240px]" : "h-[55%]"} border-b ${
        helpMode && editorHighlighted ? "ring-2 ring-primary/70 ring-inset" : ""
      }`}
      style={helpMode && editorHighlighted ? highlightStyle : undefined}
    >
      <Suspense
        fallback={<div className="flex h-full items-center justify-center text-sm text-muted-foreground">Loading editor...</div>}
      >
        <MonacoEditor
          language={asset ? editorLanguageForAssetPath(asset.path) : "sql"}
          path={editorModelPath}
          saveViewState
          keepCurrentModel
          value={editorValue}
          theme={monacoTheme}
          onChange={onChange}
          onMount={onMount}
          options={{
            minimap: { enabled: false },
            fontSize: 13,
            quickSuggestions: true,
            suggestOnTriggerCharacters: true,
          }}
        />
      </Suspense>
    </div>
  );
}

function editorLanguageForAssetPath(path: string): "sql" | "python" | "yaml" {
  const lowerPath = path.toLowerCase();
  if (lowerPath.endsWith(".py")) {
    return "python";
  }
  if (lowerPath.endsWith(".yml") || lowerPath.endsWith(".yaml")) {
    return "yaml";
  }
  return "sql";
}
