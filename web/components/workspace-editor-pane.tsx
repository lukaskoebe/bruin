"use client";

import Editor from "@monaco-editor/react";
import type { Monaco } from "@monaco-editor/react";
import type * as MonacoNS from "monaco-editor";
import { Database, Eye, Hammer, Trash2 } from "lucide-react";
import { CSSProperties, useCallback, useMemo, useState } from "react";
import { Controller, UseFormReturn } from "react-hook-form";
import { Panel } from "react-resizable-panels";

import { VisualizationSettingsEditor } from "@/components/visualization-settings-editor";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { useSQLIntellisense } from "@/hooks/use-sql-intellisense";
import { buildSchemaForAsset } from "@/lib/sql-schema";
import { WebAsset, WorkspaceState } from "@/lib/types";

export type AssetConfigForm = {
  type: string;
  materialization: string;
  custom_checks: string;
  columns: string;
};

type WorkspaceEditorPaneProps = {
  asset: WebAsset | null;
  pipelineId: string | null;
  selectedEnvironment?: string;
  workspace: WorkspaceState | null;
  helpMode: boolean;
  actionHighlighted: boolean;
  editorHighlighted: boolean;
  visualizationHighlighted: boolean;
  highlightStyle?: CSSProperties;
  materializeLoading: boolean;
  inspectLoading: boolean;
  deleteLoading: boolean;
  editorValue: string;
  monacoTheme: string;
  assetEditorTab: "configuration" | "checks" | "visualization";
  form: UseFormReturn<AssetConfigForm>;
  assetColumns: Array<{ name?: string }>;
  onEditorTabChange: (
    value: "configuration" | "checks" | "visualization"
  ) => void;
  onEditorChange: (value?: string) => void;
  onMaterializeSelectedAsset: () => void;
  onInspectSelectedAsset: () => void;
  onOpenDeleteDialog: () => void;
  onMaterializationTypeChange: (materializationType: string) => void;
  onSaveVisualizationSettings: (
    visualizationMeta: Record<string, string>
  ) => void;
  onGoToAsset?: (pipelineId: string, assetId: string) => void;
};

const MATERIALIZATION_NONE_VALUE = "__none__";

export function WorkspaceEditorPane({
  asset,
  pipelineId,
  selectedEnvironment,
  workspace,
  helpMode,
  actionHighlighted,
  editorHighlighted,
  visualizationHighlighted,
  highlightStyle,
  materializeLoading,
  inspectLoading,
  deleteLoading,
  editorValue,
  monacoTheme,
  assetEditorTab,
  form,
  assetColumns,
  onEditorTabChange,
  onEditorChange,
  onMaterializeSelectedAsset,
  onInspectSelectedAsset,
  onOpenDeleteDialog,
  onMaterializationTypeChange,
  onSaveVisualizationSettings,
  onGoToAsset,
}: WorkspaceEditorPaneProps) {
  const [monacoInstance, setMonacoInstance] = useState<Monaco | null>(null);
  const [editorInstance, setEditorInstance] =
    useState<MonacoNS.editor.IStandaloneCodeEditor | null>(null);

  const schemaTables = useMemo(() => {
    if (!workspace || !asset) {
      return [];
    }
    return buildSchemaForAsset(workspace, asset);
  }, [workspace, asset]);

  useSQLIntellisense(monacoInstance, editorInstance, schemaTables, onGoToAsset);

  const handleEditorMount = useCallback(
    (editor: MonacoNS.editor.IStandaloneCodeEditor, monaco: Monaco) => {
      setEditorInstance(editor);
      setMonacoInstance(monaco);
    },
    []
  );

  return (
    <Panel defaultSize={32} minSize={24}>
      <div className="flex h-full min-h-0 flex-col">
        <div className="border-b px-4 py-3">
          <div className="mb-2 text-sm font-semibold">Asset Editor</div>
          <div className="text-xs opacity-70">
            {asset?.path ?? "No asset selected"}
          </div>
          <div
            className={`mt-2 flex gap-2 ${
              helpMode && actionHighlighted
                ? "rounded-md ring-2 ring-primary/70 ring-offset-2"
                : ""
            }`}
            style={helpMode && actionHighlighted ? highlightStyle : undefined}
          >
            <Button
              size="sm"
              variant="outline"
              disabled={!asset || materializeLoading}
              onClick={onMaterializeSelectedAsset}
              type="button"
            >
              <Hammer className="mr-1 inline size-3" />
              {materializeLoading ? "Running..." : "Materialize"}
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={!asset || inspectLoading}
              onClick={onInspectSelectedAsset}
              type="button"
            >
              <Eye className="mr-1 inline size-3" />
              {inspectLoading ? "Loading..." : "Inspect Data"}
            </Button>
            <Button
              size="sm"
              variant="destructive"
              disabled={!asset || deleteLoading}
              onClick={onOpenDeleteDialog}
              type="button"
            >
              <Trash2 className="mr-1 inline size-3" />
              Delete Asset
            </Button>
          </div>
        </div>

        <div className="flex min-h-0 flex-1">
          <div className="flex min-h-0 flex-1 flex-col">
            <div
              className={`h-[55%] border-b ${
                helpMode && editorHighlighted
                  ? "ring-2 ring-primary/70 ring-inset"
                  : ""
              }`}
              style={helpMode && editorHighlighted ? highlightStyle : undefined}
            >
              <Editor
                language={
                  asset ? editorLanguageForAssetPath(asset.path) : "sql"
                }
                value={editorValue}
                theme={monacoTheme}
                onChange={onEditorChange}
                onMount={handleEditorMount}
                options={{
                  minimap: { enabled: false },
                  fontSize: 13,
                  quickSuggestions: true,
                  suggestOnTriggerCharacters: true,
                }}
              />
            </div>

            <div className="flex flex-1 flex-col overflow-y-auto p-4">
              <Tabs
                className="flex-1"
                onValueChange={(value) =>
                  onEditorTabChange(
                    value as "configuration" | "checks" | "visualization"
                  )
                }
                value={assetEditorTab}
              >
                <TabsList
                  className={`grid w-full grid-cols-2 ${
                    helpMode && visualizationHighlighted
                      ? "ring-2 ring-primary/70 ring-offset-2"
                      : ""
                  }`}
                  style={
                    helpMode && visualizationHighlighted
                      ? highlightStyle
                      : undefined
                  }
                >
                  <TabsTrigger value="configuration">Configuration</TabsTrigger>
                  {/* <TabsTrigger value="checks">Quality / Checks</TabsTrigger> */}
                  <TabsTrigger value="visualization">Visualization</TabsTrigger>
                </TabsList>

                <TabsContent className="mt-3 space-y-3" value="configuration">
                  <div className="grid gap-1">
                    <Label>Type</Label>
                    <Input {...form.register("type")} />
                  </div>
                  <div className="grid gap-1">
                    <Label>Materialization</Label>
                    <Controller
                      control={form.control}
                      name="materialization"
                      render={({ field }) => (
                        <Select
                          onValueChange={(value) => {
                            const materializationType =
                              value === MATERIALIZATION_NONE_VALUE ? "" : value;
                            field.onChange(materializationType);
                            onMaterializationTypeChange(materializationType);
                          }}
                          value={
                            field.value?.trim()
                              ? field.value
                              : MATERIALIZATION_NONE_VALUE
                          }
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="Select materialization" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value={MATERIALIZATION_NONE_VALUE}>
                              none
                            </SelectItem>
                            <SelectItem value="table">table</SelectItem>
                            <SelectItem value="view">view</SelectItem>
                            <SelectItem value="incremental">
                              incremental
                            </SelectItem>
                          </SelectContent>
                        </Select>
                      )}
                    />
                  </div>
                </TabsContent>

                <TabsContent className="mt-3 space-y-3" value="checks">
                  <div className="grid gap-1">
                    <Label>Custom checks</Label>
                    <Textarea
                      className="min-h-24"
                      {...form.register("custom_checks")}
                    />
                  </div>
                </TabsContent>

                <TabsContent className="mt-3 space-y-3" value="visualization">
                  {asset && (
                    <VisualizationSettingsEditor
                      columnNames={
                        (assetColumns ?? [])
                          .map((column) => column.name)
                          .filter(Boolean) as string[]
                      }
                      disabled={!asset || !pipelineId}
                      key={`${asset.id}:${JSON.stringify(asset.meta ?? {})}`}
                      meta={asset.meta}
                      monacoTheme={monacoTheme}
                      onSave={onSaveVisualizationSettings}
                    />
                  )}
                </TabsContent>
              </Tabs>

              <div className="mt-3 flex items-center gap-2 text-xs opacity-70">
                <Database className="size-3" />
                Environment: {selectedEnvironment || "default"}
              </div>
            </div>
          </div>
        </div>
      </div>
    </Panel>
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
