"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { createAsset, createPipeline, deleteAsset, updateAsset } from "@/lib/api";

export type UIMessage = {
  type: "success" | "error";
  text: string;
};

type CreateAssetInput = {
  name?: string;
  type?: string;
  path?: string;
  content?: string;
};

type UpdateAssetInput = {
  content?: string;
  materialization_type?: string;
  meta?: Record<string, string>;
};

export function useAssetActions(defaultPipelinePath = "my-pipeline") {
  const [createPipelineDialogOpen, setCreatePipelineDialogOpen] = useState(false);
  const [createPipelinePath, setCreatePipelinePath] = useState(defaultPipelinePath);
  const [createPipelineLoading, setCreatePipelineLoading] = useState(false);
  const [uiMessage, setUIMessage] = useState<UIMessage | null>(null);
  const uiMessageTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const pushUIMessage = useCallback((type: UIMessage["type"], text: string) => {
    if (uiMessageTimerRef.current) {
      clearTimeout(uiMessageTimerRef.current);
    }

    setUIMessage({ type, text });
    uiMessageTimerRef.current = setTimeout(() => {
      setUIMessage(null);
      uiMessageTimerRef.current = null;
    }, 4500);
  }, []);

  useEffect(() => {
    return () => {
      if (uiMessageTimerRef.current) {
        clearTimeout(uiMessageTimerRef.current);
      }
    };
  }, []);

  const openCreatePipelineDialog = useCallback(() => {
    if (createPipelineLoading) {
      return;
    }

    setCreatePipelinePath((current) => current || defaultPipelinePath);
    setCreatePipelineDialogOpen(true);
  }, [createPipelineLoading, defaultPipelinePath]);

  const confirmCreatePipeline = useCallback(async () => {
    const path = createPipelinePath.trim();
    if (!path || createPipelineLoading) {
      if (!path) {
        pushUIMessage("error", "Pipeline path is required.");
      }
      return false;
    }

    setCreatePipelineLoading(true);
    try {
      await createPipeline({ path, name: path });
      setCreatePipelineDialogOpen(false);
      pushUIMessage("success", `Pipeline "${path}" created.`);
      return true;
    } catch (error) {
      pushUIMessage("error", `Failed to create pipeline: ${String(error)}`);
      return false;
    } finally {
      setCreatePipelineLoading(false);
    }
  }, [createPipelineLoading, createPipelinePath, pushUIMessage]);

  const runCreateAsset = useCallback(
    async (pipelineId: string, input: CreateAssetInput) => {
      try {
        return await createAsset(pipelineId, input);
      } catch (error) {
        pushUIMessage("error", `Failed to create asset: ${String(error)}`);
        return null;
      }
    },
    [pushUIMessage]
  );

  const runDeleteAsset = useCallback(
    async (pipelineId: string, assetId: string) => {
      try {
        await deleteAsset(pipelineId, assetId);
        return true;
      } catch (error) {
        pushUIMessage("error", `Failed to delete asset: ${String(error)}`);
        return false;
      }
    },
    [pushUIMessage]
  );

  const runUpdateAsset = useCallback(
    async (pipelineId: string, assetId: string, input: UpdateAssetInput) => {
      try {
        await updateAsset(pipelineId, assetId, input);
        return true;
      } catch (error) {
        pushUIMessage("error", `Failed to update asset: ${String(error)}`);
        return false;
      }
    },
    [pushUIMessage]
  );

  return {
    uiMessage,
    pushUIMessage,
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
  };
}
