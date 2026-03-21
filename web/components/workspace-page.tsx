"use client";

import { useAtom, useAtomValue } from "jotai";
import {
  CSSProperties,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { useForm } from "react-hook-form";
import {
  NodeTypes,
  ReactFlowInstance,
  useEdgesState,
  useNodesState,
} from "reactflow";
import { Plus } from "lucide-react";
import { PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import "reactflow/dist/style.css";

import { AssetNode } from "@/components/asset-node";
import { NewAssetNode } from "@/components/new-asset-node";
import { WorkspaceAssetDialogs } from "@/components/workspace-asset-dialogs";
import {
  AssetConfigForm,
  WorkspaceEditorPane,
} from "@/components/workspace-editor-pane";
import { WorkspaceCanvasPane } from "@/components/workspace-canvas-pane";
import { WorkspaceOnboardingPanel } from "@/components/workspace-onboarding-panel";
import { Button } from "@/components/ui/button";
import {
  assetEditorTabAtom,
  editorValueAtom,
  enrichedSelectedAssetAtom,
} from "@/lib/atoms";
import {
  buildFlowFromPipeline,
  computeGraphLayoutPositions,
} from "@/lib/graph";
import { buildCreateAssetInput } from "@/lib/workspace-shell-helpers";
import { useAssetCanvasInteractions } from "@/hooks/use-asset-canvas-interactions";
import { useAssetPreviews } from "@/hooks/use-asset-previews";
import { getTablePreviewLimit } from "@/lib/asset-visualization";
import { useDebouncedAssetSave } from "@/hooks/use-debounced-asset-save";
import { useEditorActions } from "@/hooks/use-editor-actions";
import { useGraphViewportFocus } from "@/hooks/use-graph-viewport-focus";
import { useOnboardingActions } from "@/hooks/use-onboarding-actions";
import { useOnboardingState } from "@/hooks/use-onboarding-state";
import { usePersistedNodePositions } from "@/hooks/use-persisted-node-positions";
import { useWorkspaceDerivedState } from "@/hooks/use-workspace-derived-state";

import { useWorkspaceLayout } from "./workspace-layout";

export function WorkspacePage() {
  const {
    assetActions,
    assetResults,
    monacoTheme,
    navigateSelection,
    pipeline,
    pipelineMaterialization,
    sidebarOnboardingMount,
    selectedAsset,
    setSidebarState,
    workspace,
  } = useWorkspaceLayout();
  const [assetEditorTab, setAssetEditorTab] = useAtom(assetEditorTabAtom);
  const editorValue = useAtomValue(editorValueAtom);
  const asset = useAtomValue(enrichedSelectedAssetAtom);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [reactFlowInstance, setReactFlowInstance] =
    useState<ReactFlowInstance | null>(null);
  const [storedNodePositions, setStoredNodePositions] =
    usePersistedNodePositions();
  const [showOnboarding, setShowOnboarding] = useState(true);
  const [helpMode, setHelpMode] = useState(false);
  const [recomputeVersion, setRecomputeVersion] = useState(0);
  const canvasContainerRef = useRef<HTMLDivElement | null>(null);

  const { scheduleSave, flushAssetSave } = useDebouncedAssetSave(500);
  const nodeTypes = useMemo<NodeTypes>(
    () => ({ assetNode: AssetNode, newAssetNode: NewAssetNode }),
    []
  );

  const { enrichedPipeline, refreshPipelineMaterialization } =
    pipelineMaterialization;
  const { existingAssetNames, defaultAssetNamesByKind, visualAssets } =
    useWorkspaceDerivedState({
      workspace,
      enrichedPipeline,
    });

  const {
    inspectByAssetId,
    inspectLoadingByAssetId,
    canLoadMoreByAssetId,
    loadMorePreviewRows,
    clearPreviewForAsset,
    getRowsForAsset,
  } = useAssetPreviews(visualAssets);

  const areVisualPreviewsReady = useMemo(() => {
    if (visualAssets.length === 0) {
      return true;
    }

    return visualAssets.every((visualAsset) =>
      Boolean(inspectByAssetId[visualAsset.id])
    );
  }, [inspectByAssetId, visualAssets]);

  const assetPreviewRows = useMemo(() => {
    if (!asset) {
      return [] as Record<string, unknown>[];
    }

    const limit = getTablePreviewLimit(asset.meta, 25);
    return getRowsForAsset(asset.id, limit) ?? assetResults.inspectResult?.rows ?? [];
  }, [asset, assetResults.inspectResult, getRowsForAsset]);

  const graph = useMemo(
    () =>
      buildFlowFromPipeline(
        enrichedPipeline,
        inspectByAssetId,
        inspectLoadingByAssetId,
        storedNodePositions,
        canLoadMoreByAssetId,
        loadMorePreviewRows
      ),
    [
      canLoadMoreByAssetId,
      enrichedPipeline,
      inspectByAssetId,
      inspectLoadingByAssetId,
      loadMorePreviewRows,
      storedNodePositions,
    ]
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
      name: "",
      type: "",
      materialization: "",
      custom_checks: "",
      columns: "",
    },
  });

  useGraphViewportFocus({
    reactFlowInstance,
    activePipelineId: pipeline?.id ?? null,
    recomputeVersion,
    graphNodes: graph.nodes,
    graphEdges: graph.edges,
    selectedAssetId: selectedAsset,
    storedNodePositions,
    canvasContainerRef,
  });

  useEffect(() => {
    if (!enrichedPipeline || enrichedPipeline.assets.length === 0) {
      return;
    }

    if (!areVisualPreviewsReady) {
      return;
    }

    const assetIds = enrichedPipeline.assets.map(
      (currentAsset) => currentAsset.id
    );
    const hasStoredPositionsForPipeline = assetIds.some(
      (assetId) => storedNodePositions[assetId]
    );

    if (hasStoredPositionsForPipeline) {
      return;
    }

    const initialPositions = computeGraphLayoutPositions(
      enrichedPipeline,
      inspectByAssetId,
      inspectLoadingByAssetId
    );

    setStoredNodePositions((previous) => ({
      ...previous,
      ...initialPositions,
    }));
  }, [
    areVisualPreviewsReady,
    enrichedPipeline,
    inspectByAssetId,
    inspectLoadingByAssetId,
    setStoredNodePositions,
    storedNodePositions,
  ]);

  useEffect(() => {
    form.reset({
      name: asset?.name ?? "",
      type: asset?.type ?? "",
      materialization: asset?.materialization_type ?? "",
      custom_checks: "",
      columns: "",
    });
  }, [asset?.materialization_type, asset?.name, asset?.type, form]);

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
    runCreateAsset: assetActions.runCreateAsset,
    runUpdateAsset: assetActions.runUpdateAsset,
    refreshPipelineMaterialization,
    setMaterializeBatchResult: assetResults.setMaterializeBatchResult,
  });

  const helpPulseStyle = useMemo<CSSProperties>(
    () => ({
      animation: "bruin-help-scale 520ms ease-in-out infinite alternate",
    }),
    []
  );

  const onboardingContent = useMemo(
    () => (
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
        onCreatePipeline={assetActions.openCreatePipelineDialog}
        onHide={() => setShowOnboarding(false)}
        onMaterializeOnboardingAssets={handleMaterializeOnboardingAssets}
        onShow={() => setShowOnboarding(true)}
        onToggleHelp={() => setHelpMode((previous) => !previous)}
      />
    ),
    [
      assetActions.openCreatePipelineDialog,
      handleApplyPythonStarter,
      handleApplySQLStarter,
      handleApplyVisualizationStarter,
      handleCreateOnboardingAsset,
      handleMaterializeOnboardingAssets,
      helpMode,
      onboarding,
      onboardingHelp,
      onboardingMaterializeLoading,
      pipeline,
      showOnboarding,
    ]
  );

  useEffect(() => {
    setSidebarState({
      highlighted: helpMode && onboardingHelp.target === "sidebar",
      highlightStyle:
        helpMode && onboardingHelp.target === "sidebar"
          ? helpPulseStyle
          : undefined,
    });

    return () => setSidebarState({});
  }, [helpMode, helpPulseStyle, onboardingHelp.target, setSidebarState]);

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
    runCreateAsset: assetActions.runCreateAsset,
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
    handleAssetNameChange,
    handleMaterializationTypeChange,
  } = useEditorActions({
    editorValue,
    scheduleSave,
    runUpdateAsset: assetActions.runUpdateAsset,
    runDeleteAsset: assetActions.runDeleteAsset,
    runInspectForAsset: assetResults.runInspectForAsset,
    runMaterializeForAsset: assetResults.runMaterializeForAsset,
    refreshPipelineMaterialization,
    navigateSelection,
    clearResultsAfterDelete: assetResults.clearResultsAfterDelete,
    clearPreviewForAsset,
  });

  const handleRecomputeGraph = () => {
    if (!enrichedPipeline) {
      return;
    }

    const recomputedPositions = computeGraphLayoutPositions(
      enrichedPipeline,
      inspectByAssetId,
      inspectLoadingByAssetId
    );

    setStoredNodePositions((previous) => ({
      ...previous,
      ...recomputedPositions,
    }));
    setRecomputeVersion((previous) => previous + 1);
  };

  const hasPipelines = workspace.pipelines.length > 0;

  return (
    <>
      {sidebarOnboardingMount
        ? createPortal(onboardingContent, sidebarOnboardingMount)
        : null}

      {hasPipelines ? (
        <PanelGroup direction="horizontal" className="h-full min-h-0 overflow-hidden">
          <WorkspaceCanvasPane
            highlighted={helpMode && onboardingHelp.target === "canvas"}
            highlightStyle={helpPulseStyle}
            hasResultData={assetResults.hasResultData}
            canvasContainerRef={canvasContainerRef}
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            inspectResult={assetResults.inspectResult}
            inspectLoading={assetResults.inspectLoading}
            materializeLoading={assetResults.materializeLoading}
            pipelineMaterializeLoading={assetResults.pipelineMaterializeLoading}
            hasInspectData={assetResults.hasInspectData}
            hasMaterializeData={assetResults.hasMaterializeData}
            effectiveResultTab={assetResults.effectiveResultTab}
            materializeStatus={assetResults.materializeStatus}
            materializeError={assetResults.materializeError}
            materializeOutputHtml={assetResults.materializeOutputHtml}
            onResultTabChange={assetResults.setResultTab}
            onInit={setReactFlowInstance}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeDragStop={handleNodeDragStop}
            onPaneClick={handlePaneClick}
            onPaneContextMenu={handlePaneContextMenu}
            onNodeClick={handleNodeClick}
            onRecomputeGraph={handleRecomputeGraph}
          />

          <PanelResizeHandle className="w-px bg-border" />

          <WorkspaceEditorPane
            asset={asset}
            pipelineId={pipeline?.id ?? null}
            helpMode={helpMode}
            actionHighlighted={onboardingHelp.target === "actions"}
            editorHighlighted={onboardingHelp.target === "editor"}
            visualizationHighlighted={onboardingHelp.target === "visualization"}
            highlightStyle={helpPulseStyle}
            materializeLoading={assetResults.materializeLoading}
            inspectLoading={assetResults.inspectLoading}
            deleteLoading={deleteLoading}
            editorValue={editorValue}
            monacoTheme={monacoTheme}
            assetEditorTab={assetEditorTab}
            form={form}
            assetPreviewRows={assetPreviewRows}
            onEditorTabChange={setAssetEditorTab}
            onEditorChange={handleEditorChange}
            onMaterializeSelectedAsset={handleMaterializeSelectedAsset}
            onInspectSelectedAsset={handleInspectSelectedAsset}
            onOpenDeleteDialog={() => setDeleteDialogOpen(true)}
            onAssetNameChange={handleAssetNameChange}
            onMaterializationTypeChange={handleMaterializationTypeChange}
            onSaveVisualizationSettings={handleSaveVisualizationSettings}
            onGoToAsset={navigateSelection}
          />
        </PanelGroup>
      ) : (
        <div className="flex h-full items-center justify-center bg-muted/10 p-8">
          <div className="max-w-md rounded-lg border bg-card p-6 text-center shadow-sm">
            <h2 className="text-lg font-semibold">No pipelines yet</h2>
            <p className="mt-2 text-sm text-muted-foreground">
              Create your first pipeline to start editing assets and building your workflow.
            </p>
            <Button
              className="mt-4"
              onClick={assetActions.openCreatePipelineDialog}
              type="button"
            >
              <Plus className="mr-2 size-4" />
              Create pipeline
            </Button>
          </div>
        </div>
      )}

      <WorkspaceAssetDialogs
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
      />
    </>
  );
}