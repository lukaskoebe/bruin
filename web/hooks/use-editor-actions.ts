"use client";

import { useAtomValue, useSetAtom } from "jotai";
import { useCallback, useEffect, useRef, useState } from "react";

import { fillAssetColumnsFromDB } from "@/lib/api";
import { VISUALIZATION_META_KEYS } from "@/components/visualization-settings-editor";
import {
  editorDraftAtom,
  enrichedSelectedAssetAtom,
  pipelineAtom,
  workspaceAtom,
} from "@/lib/atoms";

type UseEditorActionsInput = {
  editorValue: string;
  scheduleSave: (pipelineId: string, assetId: string, content: string) => void;
  runUpdateAsset: (
    pipelineId: string,
    assetId: string,
    input: {
      name?: string;
      type?: string;
      content?: string;
      materialization_type?: string;
      meta?: Record<string, string>;
    }
  ) => Promise<boolean>;
  runDeleteAsset: (pipelineId: string, assetId: string) => Promise<boolean>;
  runInspectForAsset: (assetId: string) => Promise<unknown>;
  runMaterializeForAsset: (
    assetId: string,
    refresh?: () => Promise<void> | void
  ) => Promise<unknown>;
  refreshPipelineMaterialization: (pipelineId: string) => Promise<void>;
  navigateSelection: (pipelineId: string, assetId: string | null) => void;
  clearResultsAfterDelete: () => void;
  clearPreviewForAsset: (assetId: string) => void;
};

export function useEditorActions({
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
}: UseEditorActionsInput) {
  const asset = useAtomValue(enrichedSelectedAssetAtom);
  const pipeline = useAtomValue(pipelineAtom);
  const pipelineId = pipeline?.id ?? null;
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const fillColumnsTimerRef = useRef<ReturnType<typeof setTimeout> | null>(
    null
  );
  const setEditorDraft = useSetAtom(editorDraftAtom);
  const setWorkspace = useSetAtom(workspaceAtom);

  const handleEditorChange = useCallback(
    (value?: string) => {
      const nextValue = value ?? "";
      if (!asset || !pipelineId) {
        return;
      }

      setEditorDraft({
        assetId: asset.id,
        content: nextValue,
      });
      scheduleSave(pipelineId, asset.id, nextValue);

      const isSQLAsset =
        asset.path.toLowerCase().endsWith(".sql") ||
        asset.type.toLowerCase().includes("sql");
      if (!isSQLAsset) {
        return;
      }

      if (fillColumnsTimerRef.current) {
        clearTimeout(fillColumnsTimerRef.current);
      }

      const currentAssetID = asset.id;
      fillColumnsTimerRef.current = setTimeout(() => {
        void fillAssetColumnsFromDB(currentAssetID).catch(() => {
          // noop: best-effort post-edit sync
        });
      }, 1200);
    },
    [asset, pipelineId, scheduleSave, setEditorDraft]
  );

  useEffect(() => {
    return () => {
      if (fillColumnsTimerRef.current) {
        clearTimeout(fillColumnsTimerRef.current);
      }
    };
  }, []);

  const handleSaveVisualizationSettings = useCallback(
    (visualizationMeta: Record<string, string>) => {
      if (!asset || !pipelineId) {
        return;
      }

      const mergedMeta: Record<string, string> = {
        ...(asset.meta ?? {}),
      };

      for (const key of VISUALIZATION_META_KEYS) {
        delete mergedMeta[key];
      }

      for (const [key, value] of Object.entries(visualizationMeta)) {
        mergedMeta[key] = value;
      }

      void runUpdateAsset(pipelineId, asset.id, {
        content: editorValue,
        meta: mergedMeta,
      });
    },
    [asset, editorValue, pipelineId, runUpdateAsset]
  );

  const handleConfirmDeleteAsset = useCallback(() => {
    if (!asset || !pipelineId || deleteLoading) {
      return;
    }

    setDeleteLoading(true);
    void runDeleteAsset(pipelineId, asset.id)
      .then((deleted) => {
        if (!deleted) {
          return;
        }

        setDeleteDialogOpen(false);
        clearResultsAfterDelete();
        clearPreviewForAsset(asset.id);
        navigateSelection(pipelineId, null);
      })
      .finally(() => setDeleteLoading(false));
  }, [
    asset,
    clearPreviewForAsset,
    clearResultsAfterDelete,
    deleteLoading,
    navigateSelection,
    pipelineId,
    runDeleteAsset,
  ]);

  const handleMaterializeSelectedAsset = useCallback(() => {
    if (!asset) {
      return;
    }

    void runMaterializeForAsset(asset.id, async () => {
      if (pipelineId) {
        await refreshPipelineMaterialization(pipelineId).catch(() => undefined);
      }
    });
  }, [
    asset,
    pipelineId,
    refreshPipelineMaterialization,
    runMaterializeForAsset,
  ]);

  const handleInspectSelectedAsset = useCallback(() => {
    if (!asset) {
      return;
    }

    void runInspectForAsset(asset.id);
  }, [asset, runInspectForAsset]);

  const handleMaterializationTypeChange = useCallback(
    (materializationType: string) => {
      if (!asset || !pipelineId) {
        return;
      }

      void runUpdateAsset(pipelineId, asset.id, {
        content: editorValue,
        materialization_type: materializationType,
      });
    },
    [asset, editorValue, pipelineId, runUpdateAsset]
  );

  const handleAssetTypeChange = useCallback(
    (assetType: string) => {
      if (!asset || !pipelineId) {
        return;
      }

      const trimmedType = assetType.trim();
      if (!trimmedType || trimmedType === asset.type) {
        return;
      }

      void runUpdateAsset(pipelineId, asset.id, {
        type: trimmedType,
        content: editorValue,
      }).then((updated) => {
        if (!updated) {
          return;
        }

        setWorkspace((current) => {
          if (!current) {
            return current;
          }

          return {
            ...current,
            pipelines: current.pipelines.map((currentPipeline) => {
              if (currentPipeline.id !== pipelineId) {
                return currentPipeline;
              }

              return {
                ...currentPipeline,
                assets: currentPipeline.assets.map((currentAsset) =>
                  currentAsset.id === asset.id
                    ? { ...currentAsset, type: trimmedType }
                    : currentAsset
                ),
              };
            }),
          };
        });
      });
    },
    [asset, editorValue, pipelineId, runUpdateAsset, setWorkspace]
  );

  const handleAssetNameChange = useCallback(
    (assetName: string) => {
      if (!asset || !pipelineId) {
        return;
      }

      const trimmedName = assetName.trim();
      if (!trimmedName || trimmedName === asset.name) {
        return;
      }

      void runUpdateAsset(pipelineId, asset.id, {
        name: trimmedName,
        content: editorValue,
      });
    },
    [asset, editorValue, pipelineId, runUpdateAsset]
  );

  return {
    deleteDialogOpen,
    deleteLoading,
    setDeleteDialogOpen,
    handleEditorChange,
    handleSaveVisualizationSettings,
    handleConfirmDeleteAsset,
    handleMaterializeSelectedAsset,
    handleInspectSelectedAsset,
    handleAssetNameChange,
    handleAssetTypeChange,
    handleMaterializationTypeChange,
  };
}
