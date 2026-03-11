"use client";

import { useAtom, useAtomValue } from "jotai";
import { CSSProperties, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useForm } from "react-hook-form";
import {
  NodeTypes,
  ReactFlowInstance,
  useEdgesState,
  useNodesState,
} from "reactflow";
import { Plus } from "lucide-react";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import "reactflow/dist/style.css";

import { AssetNode } from "@/components/asset-node";
import { NewAssetNode } from "@/components/new-asset-node";
import {
  AssetConfigForm,
  WorkspaceEditorPane,
} from "@/components/workspace-editor-pane";
import { WorkspaceCanvasPane } from "@/components/workspace-canvas-pane";
import { WorkspaceSidebar } from "@/components/workspace-sidebar";
import { WorkspaceOnboardingPanel } from "@/components/workspace-onboarding-panel";
import { WorkspaceDialogs } from "@/components/workspace-dialogs";
import { Button } from "@/components/ui/button";
import { SidebarProvider } from "@/components/ui/sidebar";
import {
  assetEditorTabAtom,
  editorValueAtom,
  enrichedSelectedAssetAtom,
  pipelineAtom,
} from "@/lib/atoms";
import { buildFlowFromPipeline } from "@/lib/graph";
import { buildCreateAssetInput } from "@/lib/workspace-shell-helpers";
import { useAssetActions } from "@/hooks/use-asset-actions";
import { useAssetCanvasInteractions } from "@/hooks/use-asset-canvas-interactions";
import { useAssetPreviews } from "@/hooks/use-asset-previews";
import { useAssetResults } from "@/hooks/use-asset-results";
import { useDebouncedAssetSave } from "@/hooks/use-debounced-asset-save";
import { useEditorActions } from "@/hooks/use-editor-actions";
import { useGraphViewportFocus } from "@/hooks/use-graph-viewport-focus";
import { useOnboardingActions } from "@/hooks/use-onboarding-actions";
import { useOnboardingState } from "@/hooks/use-onboarding-state";
import { usePipelineMaterializationState } from "@/hooks/use-pipeline-materialization-state";
import { usePersistedNodePositions } from "@/hooks/use-persisted-node-positions";
import { useWorkspaceDerivedState } from "@/hooks/use-workspace-derived-state";
import { useWorkspaceSelection } from "@/hooks/use-workspace-selection";
import { useWorkspaceSync } from "@/hooks/use-workspace-sync";
import { useWorkspaceTheme } from "@/hooks/use-workspace-theme";

export function WorkspaceShell() {
  const workspace = useWorkspaceSync();
  const [assetEditorTab, setAssetEditorTab] = useAtom(assetEditorTabAtom);
  const editorValue = useAtomValue(editorValueAtom);
  const asset = useAtomValue(enrichedSelectedAssetAtom);
  const pipeline = useAtomValue(pipelineAtom);
  const { activePipeline, selectedAsset, navigateSelection } =
    useWorkspaceSelection();

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [reactFlowInstance, setReactFlowInstance] =
    useState<ReactFlowInstance | null>(null);
  const [storedNodePositions, setStoredNodePositions] =
    usePersistedNodePositions();
  const [showOnboarding, setShowOnboarding] = useState(true);
  const [helpMode, setHelpMode] = useState(false);
  const [pendingPipelinePathSelection, setPendingPipelinePathSelection] =
    useState<string | null>(null);
  const canvasContainerRef = useRef<HTMLDivElement | null>(null);

  const { scheduleSave, flushAssetSave } = useDebouncedAssetSave(500);
  const { theme, setTheme, monacoTheme } = useWorkspaceTheme();
  const {
    uiMessage,
    createPipelineDialogOpen,
    setCreatePipelineDialogOpen,
    createPipelinePath,
    setCreatePipelinePath,
    createPipelineLoading,
    openCreatePipelineDialog,
    confirmCreatePipeline,
    runCreateAsset,
    runDeleteAsset,
    runUpdateAsset,
  } = useAssetActions();
  const {
    inspectResult,
    inspectLoading,
    materializeLoading,
    materializeStatus,
    materializeError,
    hasInspectData,
    hasMaterializeData,
    hasResultData,
    effectiveResultTab,
    materializeOutputHtml,
    setResultTab,
    runInspectForAsset,
    runMaterializeForAsset,
    setMaterializeBatchResult,
    clearResultsAfterDelete,
  } = useAssetResults();
  const nodeTypes = useMemo<NodeTypes>(
    () => ({ assetNode: AssetNode, newAssetNode: NewAssetNode }),
    []
  );

  const { enrichedPipeline, refreshPipelineMaterialization } =
    usePipelineMaterializationState();
  const { existingAssetNames, defaultAssetNamesByKind, visualAssets } =
    useWorkspaceDerivedState({
      workspace,
      enrichedPipeline,
    });

  const { inspectByAssetId, inspectLoadingByAssetId, clearPreviewForAsset } =
    useAssetPreviews(visualAssets);

  // Derive available columns from both asset metadata and inspect results.
  const assetColumns = useMemo(() => {
    if (!asset) return [] as Array<{ name?: string }>;
    const assetColumnNames =
      asset.columns?.map((c: { name?: string }) => c.name ?? "") ?? [];
    const inspectColumnNames: string[] =
      inspectByAssetId[asset.id]?.columns ?? [];
    const names: string[] = [
      ...new Set([...assetColumnNames, ...inspectColumnNames]),
    ];
    return names.map((name) => ({ name }));
  }, [asset, inspectByAssetId]);

  const graph = useMemo(
    () =>
      buildFlowFromPipeline(
        enrichedPipeline,
        inspectByAssetId,
        inspectLoadingByAssetId
      ),
    [enrichedPipeline, inspectByAssetId, inspectLoadingByAssetId]
  );

  const connectedNodeIDs = useMemo(() => {
    const ids = new Set<string>();
    for (const edge of graph.edges) {
      ids.add(edge.source);
      ids.add(edge.target);
    }
    return ids;
  }, [graph.edges]);

  const form = useForm<AssetConfigForm>({
    defaultValues: {
      type: "",
      materialization: "",
      custom_checks: "",
      columns: "",
    },
  });

  useGraphViewportFocus({
    reactFlowInstance,
    activePipelineId: pipeline?.id ?? null,
    graphNodes: graph.nodes,
    graphEdges: graph.edges,
    selectedAssetId: selectedAsset,
    storedNodePositions,
    canvasContainerRef,
  });

  useEffect(() => {
    form.reset({
      type: asset?.type ?? "",
      materialization: asset?.materialization_type ?? "",
      custom_checks: "",
      columns: "",
    });
  }, [asset?.materialization_type, asset?.type, form]);

  const previousSelectedAssetRef = useRef<string | null>(null);

  useEffect(() => {
    const previousSelectedAsset = previousSelectedAssetRef.current;
    if (previousSelectedAsset && previousSelectedAsset !== selectedAsset) {
      flushAssetSave(previousSelectedAsset);
    }
    previousSelectedAssetRef.current = selectedAsset;
  }, [flushAssetSave, selectedAsset]);

  const { onboarding, onboardingHelp } = useOnboardingState(enrichedPipeline);
  const {
    onboardingMaterializeLoading,
    handleCreateOnboardingAsset,
    handleApplyPythonStarter,
    handleApplySQLStarter,
    handleMaterializeOnboardingAssets,
    handleApplyVisualizationStarter,
  } = useOnboardingActions({
    editorValue,
    onboarding,
    existingAssetNames,
    navigateSelection,
    runCreateAsset,
    runUpdateAsset,
    refreshPipelineMaterialization,
    setMaterializeBatchResult,
  });

  const {
    handlePaneClick,
    handlePaneContextMenu,
    handleNodeDragStop,
    handleNodeClick,
  } = useAssetCanvasInteractions({
    reactFlowInstance,
    canvasContainerRef,
    graphNodes: graph.nodes,
    graphEdges: graph.edges,
    connectedNodeIDs,
    storedNodePositions,
    setStoredNodePositions,
    defaultAssetNamesByKind,
    setNodes,
    setEdges,
    runCreateAsset,
    navigateSelection,
    buildCreateAssetInput,
  });

  const {
    deleteDialogOpen,
    deleteLoading,
    setDeleteDialogOpen,
    handleEditorChange,
    handleSaveVisualizationSettings,
    handleConfirmDeleteAsset,
    handleMaterializeSelectedAsset,
    handleInspectSelectedAsset,
    handleMaterializationTypeChange,
  } = useEditorActions({
    editorValue,
    scheduleSave,
    runUpdateAsset,
    runDeleteAsset,
    runInspectForAsset,
    runMaterializeForAsset,
    refreshPipelineMaterialization,
    navigateSelection,
    clearResultsAfterDelete,
    clearPreviewForAsset,
  });

  const handleCreatePipeline = openCreatePipelineDialog;

  const handleConfirmCreatePipeline = useCallback(async () => {
    const createdPath = createPipelinePath.trim();
    const created = await confirmCreatePipeline();
    if (created && createdPath) {
      setPendingPipelinePathSelection(createdPath);
    }
    return created;
  }, [confirmCreatePipeline, createPipelinePath]);

  useEffect(() => {
    if (!pendingPipelinePathSelection || !workspace) {
      return;
    }

    const normalizedPendingPath = pendingPipelinePathSelection
      .replaceAll("\\", "/")
      .trim();

    const matchingPipeline = workspace.pipelines.find((currentPipeline) => {
      const pipelinePath = currentPipeline.path.replaceAll("\\", "/").trim();
      return (
        pipelinePath === normalizedPendingPath ||
        pipelinePath.endsWith(`/${normalizedPendingPath}`)
      );
    });

    if (!matchingPipeline) {
      return;
    }

    navigateSelection(matchingPipeline.id, matchingPipeline.assets[0]?.id ?? null);
    setPendingPipelinePathSelection(null);
  }, [navigateSelection, pendingPipelinePathSelection, workspace]);

  const helpPulseStyle = useMemo<CSSProperties>(
    () => ({
      animation: "bruin-help-scale 520ms ease-in-out infinite alternate",
    }),
    []
  );

  const onboardingContent = (
    <WorkspaceOnboardingPanel
      helpMode={helpMode}
      onboarding={onboarding}
      onboardingHelp={onboardingHelp}
      onboardingMaterializeLoading={onboardingMaterializeLoading}
      pipelineExists={Boolean(pipeline)}
      showOnboarding={showOnboarding}
      onApplyPythonStarter={handleApplyPythonStarter}
      onApplySQLStarter={handleApplySQLStarter}
      onApplyVisualizationStarter={handleApplyVisualizationStarter}
      onCreateOnboardingAsset={handleCreateOnboardingAsset}
      onCreatePipeline={handleCreatePipeline}
      onHide={() => setShowOnboarding(false)}
      onMaterializeOnboardingAssets={handleMaterializeOnboardingAssets}
      onShow={() => setShowOnboarding(true)}
      onToggleHelp={() => setHelpMode((previous) => !previous)}
    />
  );

  const hasPipelines = (workspace?.pipelines.length ?? 0) > 0;

  return (
    <div className="h-screen w-screen overflow-hidden bg-background text-foreground">
      {uiMessage && (
        <div className="pointer-events-none fixed right-4 top-4 z-50">
          <div
            className={`rounded-md border px-3 py-2 text-sm shadow ${
              uiMessage.type === "error"
                ? "border-destructive/50 bg-destructive/10 text-destructive"
                : "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
            }`}
          >
            {uiMessage.text}
          </div>
        </div>
      )}
      <SidebarProvider defaultOpen className="h-full min-h-0 overflow-hidden">
        <PanelGroup
          direction="horizontal"
          className="h-full min-h-0 overflow-hidden"
        >
          <Panel defaultSize={18} minSize={14}>
            <WorkspaceSidebar
              workspace={workspace}
              activePipeline={activePipeline}
              selectedAsset={selectedAsset}
              highlighted={helpMode && onboardingHelp.target === "sidebar"}
              highlightStyle={
                helpMode && onboardingHelp.target === "sidebar"
                  ? helpPulseStyle
                  : undefined
              }
              onboardingContent={onboardingContent}
              theme={theme}
              onToggleTheme={() =>
                setTheme((current) => (current === "dark" ? "light" : "dark"))
              }
              onCreatePipeline={handleCreatePipeline}
              onNavigateSelection={navigateSelection}
            />
          </Panel>

          <PanelResizeHandle className="w-px bg-border" />

          {hasPipelines ? (
            <>
              <WorkspaceCanvasPane
                highlighted={helpMode && onboardingHelp.target === "canvas"}
                highlightStyle={helpPulseStyle}
                hasResultData={hasResultData}
                canvasContainerRef={canvasContainerRef}
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes}
                inspectResult={inspectResult}
                inspectLoading={inspectLoading}
                materializeLoading={materializeLoading}
                hasInspectData={hasInspectData}
                hasMaterializeData={hasMaterializeData}
                effectiveResultTab={effectiveResultTab}
                materializeStatus={materializeStatus}
                materializeError={materializeError}
                materializeOutputHtml={materializeOutputHtml}
                onResultTabChange={setResultTab}
                onInit={setReactFlowInstance}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onNodeDragStop={handleNodeDragStop}
                onPaneClick={handlePaneClick}
                onPaneContextMenu={handlePaneContextMenu}
                onNodeClick={handleNodeClick}
              />

              <PanelResizeHandle className="w-px bg-border" />

              <WorkspaceEditorPane
                asset={asset}
                pipelineId={pipeline?.id ?? null}
                selectedEnvironment={workspace?.selected_environment}
                workspace={workspace}
                helpMode={helpMode}
                actionHighlighted={onboardingHelp.target === "actions"}
                editorHighlighted={onboardingHelp.target === "editor"}
                visualizationHighlighted={onboardingHelp.target === "visualization"}
                highlightStyle={helpPulseStyle}
                materializeLoading={materializeLoading}
                inspectLoading={inspectLoading}
                deleteLoading={deleteLoading}
                editorValue={editorValue}
                monacoTheme={monacoTheme}
                assetEditorTab={assetEditorTab}
                form={form}
                assetColumns={assetColumns}
                onEditorTabChange={setAssetEditorTab}
                onEditorChange={handleEditorChange}
                onMaterializeSelectedAsset={handleMaterializeSelectedAsset}
                onInspectSelectedAsset={handleInspectSelectedAsset}
                onOpenDeleteDialog={() => setDeleteDialogOpen(true)}
                onMaterializationTypeChange={handleMaterializationTypeChange}
                onSaveVisualizationSettings={handleSaveVisualizationSettings}
                onGoToAsset={navigateSelection}
              />
            </>
          ) : (
            <Panel defaultSize={82} minSize={30}>
              <div className="flex h-full items-center justify-center bg-muted/10 p-8">
                <div className="max-w-md rounded-lg border bg-card p-6 text-center shadow-sm">
                  <h2 className="text-lg font-semibold">No pipelines yet</h2>
                  <p className="mt-2 text-sm text-muted-foreground">
                    Create your first pipeline to start editing assets and building your workflow.
                  </p>
                  <Button className="mt-4" onClick={handleCreatePipeline} type="button">
                    <Plus className="mr-2 size-4" />
                    Create pipeline
                  </Button>
                </div>
              </div>
            </Panel>
          )}
        </PanelGroup>

        <WorkspaceDialogs
          deleteDialogOpen={deleteDialogOpen}
          deleteLoading={deleteLoading}
          selectedAssetName={asset?.name}
          canDeleteAsset={Boolean(asset && pipeline)}
          onDeleteDialogOpenChange={(open) => {
            if (!deleteLoading) {
              setDeleteDialogOpen(open);
            }
          }}
          onConfirmDeleteAsset={handleConfirmDeleteAsset}
          onCancelDeleteAsset={() => setDeleteDialogOpen(false)}
          createPipelineDialogOpen={createPipelineDialogOpen}
          createPipelineLoading={createPipelineLoading}
          createPipelinePath={createPipelinePath}
          onCreatePipelineDialogOpenChange={(open) => {
            if (!createPipelineLoading) {
              setCreatePipelineDialogOpen(open);
            }
          }}
          onCreatePipelinePathChange={setCreatePipelinePath}
          onConfirmCreatePipeline={handleConfirmCreatePipeline}
        />
      </SidebarProvider>
      <style jsx global={true}>{`
        @keyframes bruin-help-scale {
          from {
            transform: scale(1);
          }
          to {
            transform: scale(1.05);
          }
        }
      `}</style>
    </div>
  );
}
