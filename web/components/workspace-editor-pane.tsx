"use client";

import { useAtomValue, useSetAtom } from "jotai";
import Editor from "@monaco-editor/react";
import type { Monaco } from "@monaco-editor/react";
import type * as MonacoNS from "monaco-editor";
import { Bug, ChevronDown, Database, Eye, Hammer, Trash2 } from "lucide-react";
import { CSSProperties, useCallback, useEffect, useMemo, useRef, useState } from "react";
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
import { useYAMLIntellisense } from "@/hooks/use-yaml-intellisense";
import { inferAssetColumns } from "@/lib/api";
import {
  registerAssetColumnsAtom,
  selectedAssetColumnEntriesAtom,
  selectedAssetInspectColumnsAtom,
  selectedEnvironmentAtom,
  selectedAssetSchemaSuggestionTablesAtom,
  selectedAssetSchemaTablesAtom,
} from "@/lib/atoms";
import { WebAsset } from "@/lib/types";

export type AssetConfigForm = {
  name: string;
  type: string;
  materialization: string;
  custom_checks: string;
  columns: string;
};

type WorkspaceEditorPaneProps = {
  asset: WebAsset | null;
  pipelineId: string | null;
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
  assetPreviewRows: Record<string, unknown>[];
  onEditorTabChange: (
    value: "configuration" | "checks" | "visualization"
  ) => void;
  onEditorChange: (value?: string) => void;
  onMaterializeSelectedAsset: () => void;
  onInspectSelectedAsset: () => void;
  onOpenDeleteDialog: () => void;
  onAssetNameChange: (assetName: string) => void;
  onMaterializationTypeChange: (materializationType: string) => void;
  onSaveVisualizationSettings: (
    visualizationMeta: Record<string, string>
  ) => void;
  onGoToAsset?: (pipelineId: string, assetId: string) => void;
  mobile?: boolean;
};

const MATERIALIZATION_NONE_VALUE = "__none__";

export function WorkspaceEditorPane({
  asset,
  pipelineId,
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
  assetPreviewRows,
  onEditorTabChange,
  onEditorChange,
  onMaterializeSelectedAsset,
  onInspectSelectedAsset,
  onOpenDeleteDialog,
  onAssetNameChange,
  onMaterializationTypeChange,
  onSaveVisualizationSettings,
  onGoToAsset,
  mobile = false,
}: WorkspaceEditorPaneProps) {
  const [monacoInstance, setMonacoInstance] = useState<Monaco | null>(null);
  const [editorInstance, setEditorInstance] =
    useState<MonacoNS.editor.IStandaloneCodeEditor | null>(null);
  const selectedEnvironment = useAtomValue(selectedEnvironmentAtom);
  const assetColumns = useAtomValue(selectedAssetColumnEntriesAtom);
  const assetInspectColumns = useAtomValue(selectedAssetInspectColumnsAtom);
  const schemaTables = useAtomValue(selectedAssetSchemaTablesAtom);
  const schemaSuggestionTables = useAtomValue(selectedAssetSchemaSuggestionTablesAtom);
  const registerAssetColumns = useSetAtom(registerAssetColumnsAtom);
  const requestedInferenceAssetIdsRef = useRef(new Set<string>());

  useEffect(() => {
    if (!asset) {
      return;
    }

    const upstreamNameSet = new Set((asset.upstreams ?? []).map((name) => name.toLowerCase()));
    const tablesNeedingInference = schemaSuggestionTables.filter(
      (table) =>
        table.assetId &&
        table.columns.length === 0 &&
        upstreamNameSet.has(table.name.toLowerCase())
    );

    for (const table of tablesNeedingInference) {
      const tableAssetId = table.assetId;
      if (!tableAssetId || requestedInferenceAssetIdsRef.current.has(tableAssetId)) {
        continue;
      }

      requestedInferenceAssetIdsRef.current.add(tableAssetId);
      void inferAssetColumns(tableAssetId)
        .then((response) => {
          registerAssetColumns({
            assetId: tableAssetId,
            method: "asset-column-inference",
            columns: (response.columns ?? []).map((column) => ({
              name: column.name,
              type: column.type,
              description: column.description,
              primaryKey: column.primary_key,
            })),
          });
        })
        .catch(() => {
          // noop: debug panel will still show missing columns
        });
    }
  }, [asset, registerAssetColumns, schemaSuggestionTables]);

  useSQLIntellisense(
    monacoInstance,
    editorInstance,
    asset,
    schemaTables,
    asset?.upstreams ?? [],
    selectedEnvironment,
    onGoToAsset
  );
  useYAMLIntellisense(monacoInstance, editorInstance, asset);

  const handleEditorMount = useCallback(
    (editor: MonacoNS.editor.IStandaloneCodeEditor, monaco: Monaco) => {
      setEditorInstance(editor);
      setMonacoInstance(monaco);
    },
    []
  );

  const editorModelPath = useMemo(() => {
    if (!asset) {
      return "inmemory://bruin/no-selection.sql";
    }

    const extension = asset.path.split(".").pop()?.toLowerCase() ?? "sql";
    return `inmemory://bruin/assets/${asset.id}.${extension}`;
  }, [asset]);

  const debugResolvedUpstreamTables = useMemo(() => {
    if (!asset) {
      return [];
    }

    return (asset.upstreams ?? [])
      .map((upstreamName) => {
        const table = schemaSuggestionTables.find(
          (candidate) =>
            candidate.name.toLowerCase() === upstreamName.toLowerCase() ||
            candidate.shortName.toLowerCase() === upstreamName.toLowerCase()
        );

        const hasWorkspaceColumns = table?.columns.some((column) =>
          column.sourceMethods.some(
            (method) => method === "workspace-load" || method === "workspace-event"
          )
        );
        const hasInferredColumns = table?.columns.some((column) =>
          column.sourceMethods.includes("asset-column-inference")
        );
        const source = !table
          ? "unresolved"
          : table.columns.length === 0
            ? "resolved-without-columns"
            : hasWorkspaceColumns && hasInferredColumns
              ? "declared+inferred"
              : hasInferredColumns
                ? "inferred"
                : "declared";

        return {
          upstreamName,
          table,
          source,
        };
      })
      .filter(
        (
          item
        ): item is {
          upstreamName: string;
          table: (typeof schemaSuggestionTables)[number] | undefined;
          source: string;
        } => Boolean(item.upstreamName)
      );
  }, [asset, schemaSuggestionTables]);

  const declaredColumnNames = useMemo(
    () =>
      (asset?.columns ?? [])
        .map((column) => column.name)
        .filter(Boolean) as string[],
    [asset]
  );

  const mergedColumnNames = useMemo(
    () =>
      (assetColumns ?? [])
        .map((column) => column.name)
        .filter(Boolean) as string[],
    [assetColumns]
  );

  const content = (
    <div className="flex h-full min-h-0 min-w-0 flex-col">
      <div className="border-b px-4 py-3">
        <div className="mb-2 text-sm font-semibold">Asset Editor</div>
        <div className="text-xs opacity-70">
          {asset?.path ?? "No asset selected"}
        </div>
        <div
          className={`mt-2 flex flex-wrap gap-2 ${
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

      <div className="flex min-h-0 min-w-0 flex-1 overflow-hidden">
        <div className="flex min-h-0 min-w-0 flex-1 flex-col">
          <div
            className={`${mobile ? "min-h-[240px]" : "h-[55%]"} border-b ${
              helpMode && editorHighlighted
                ? "ring-2 ring-primary/70 ring-inset"
                : ""
            }`}
            style={helpMode && editorHighlighted ? highlightStyle : undefined}
          >
            <Editor
              language={asset ? editorLanguageForAssetPath(asset.path) : "sql"}
              path={editorModelPath}
              saveViewState
              keepCurrentModel
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

          <div className="flex min-w-0 flex-1 flex-col overflow-x-hidden overflow-y-auto p-4">
            <Tabs
              className="flex min-h-0 min-w-0 flex-1 flex-col"
              onValueChange={(value) =>
                onEditorTabChange(
                  value as "configuration" | "checks" | "visualization"
                )
              }
              value={assetEditorTab}
            >
                <TabsList
                  className={`grid w-full min-w-0 grid-cols-2 ${
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
                    <Label>Name</Label>
                    <Controller
                      control={form.control}
                      name="name"
                      render={({ field }) => (
                        <Input
                          {...field}
                          onBlur={(event) => {
                            field.onBlur();
                            onAssetNameChange(event.target.value);
                          }}
                        />
                      )}
                    />
                  </div>
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
                      key={asset.id}
                      meta={asset.meta}
                      monacoTheme={monacoTheme}
                      previewRows={assetPreviewRows}
                      onSave={onSaveVisualizationSettings}
                    />
                  )}
                </TabsContent>
              </Tabs>

              <div className="mt-3 flex items-center gap-2 text-xs opacity-70">
                <Database className="size-3" />
                Environment: {selectedEnvironment || "default"}
              </div>

              {asset && (
                <details className="mt-3 rounded-md border bg-muted/20 p-2 text-[10px] leading-4">
                  <summary className="flex cursor-pointer list-none items-center gap-1 font-medium text-muted-foreground">
                    <Bug className="size-3" />
                    SQL column debug
                    <ChevronDown className="size-3" />
                  </summary>

                  <div className="mt-2 grid gap-2 text-[10px]">
                    <DebugList
                      items={asset.upstreams ?? []}
                      title={`Parsed upstreams (${asset.upstreams?.length ?? 0})`}
                    />

                    <DebugList
                      items={declaredColumnNames}
                      title={`Selected asset declared columns (${declaredColumnNames.length})`}
                    />

                    <DebugList
                      items={assetInspectColumns}
                      title={`Selected asset inspect columns (${assetInspectColumns.length})`}
                    />

                    <DebugList
                      items={mergedColumnNames}
                      title={`Merged visualization/editor columns (${mergedColumnNames.length})`}
                    />

                    <div className="grid gap-1">
                      <div className="font-medium text-muted-foreground">
                        Same-connection schema tables ({schemaTables.length})
                      </div>
                      <div className="max-h-36 overflow-auto rounded border bg-background/70 p-2 font-mono">
                        {schemaSuggestionTables.length > 0 ? (
                          schemaSuggestionTables.map((table) => (
                            <div className="mb-1 break-all last:mb-0" key={table.name}>
                              <div>
                                {table.name} · {table.columns.length} cols
                                {table.columns.some((column) =>
                                  column.sourceMethods.includes("asset-column-inference")
                                )
                                  ? " · inferred"
                                  : " · declared"}
                              </div>
                              <div className="text-muted-foreground">
                                {table.columns.map((column) => column.name).join(", ") ||
                                  "(no columns)"}
                              </div>
                            </div>
                          ))
                        ) : (
                          <div className="text-muted-foreground">No schema tables available.</div>
                        )}
                      </div>
                    </div>

                    <div className="grid gap-1">
                      <div className="font-medium text-muted-foreground">
                        Upstream tables resolved for completion ({debugResolvedUpstreamTables.length})
                      </div>
                      <div className="max-h-36 overflow-auto rounded border bg-background/70 p-2 font-mono">
                        {debugResolvedUpstreamTables.length > 0 ? (
                          debugResolvedUpstreamTables.map(({ upstreamName, table, source }) => (
                            <div className="mb-1 break-all last:mb-0" key={upstreamName}>
                              <div>
                                {upstreamName}
                                {table ? ` → ${table.name}` : " → unresolved"}
                                {source ? ` · ${source}` : ""}
                              </div>
                              <div className="text-muted-foreground">
                                {table
                                  ? table.columns.map((column) => column.name).join(", ") ||
                                    "(resolved, but no columns)"
                                  : "(not found in same-connection schema tables)"}
                              </div>
                            </div>
                          ))
                        ) : (
                          <div className="text-muted-foreground">
                            No parsed upstreams on the selected asset.
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                </details>
              )}
            </div>
          </div>
        </div>
    </div>
  );

  if (mobile) {
    return content;
  }

  return <Panel defaultSize={32} minSize={24}>{content}</Panel>;
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

function DebugList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="grid gap-1">
      <div className="font-medium text-muted-foreground">{title}</div>
      <div className="rounded border bg-background/70 p-2 font-mono break-all">
        {items.length > 0 ? items.join(", ") : "(empty)"}
      </div>
    </div>
  );
}
