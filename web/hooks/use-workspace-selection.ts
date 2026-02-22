"use client";

import { useAtom, useAtomValue } from "jotai";
import { useCallback, useEffect } from "react";

import {
  activePipelineAtom,
  resolvedActivePipelineAtom,
  resolvedSelectedAssetAtom,
  selectedAssetAtom,
  workspaceAtom,
} from "@/lib/atoms";

export function useWorkspaceSelection(): {
  activePipeline: string | null;
  selectedAsset: string | null;
  navigateSelection: (pipelineId: string, assetId: string | null) => void;
} {
  const workspace = useAtomValue(workspaceAtom);
  const [requestedPipeline, setRequestedPipeline] = useAtom(activePipelineAtom);
  const [requestedAsset, setRequestedAsset] = useAtom(selectedAssetAtom);
  const activePipeline = useAtomValue(resolvedActivePipelineAtom);
  const selectedAsset = useAtomValue(resolvedSelectedAssetAtom);

  useEffect(() => {
    const readSelectionFromLocation = () => {
      const params = new URLSearchParams(window.location.search);
      setRequestedPipeline(params.get("pipeline"));
      setRequestedAsset(params.get("asset"));
    };

    readSelectionFromLocation();
    window.addEventListener("popstate", readSelectionFromLocation);
    return () => {
      window.removeEventListener("popstate", readSelectionFromLocation);
    };
  }, [setRequestedAsset, setRequestedPipeline]);

  const navigateSelection = useCallback(
    (pipelineId: string, assetId: string | null) => {
      const params = new URLSearchParams(window.location.search);
      params.set("pipeline", pipelineId);
      if (assetId) {
        params.set("asset", assetId);
      } else {
        params.delete("asset");
      }

      const query = params.toString();
      const nextURL = query
        ? `${window.location.pathname}?${query}`
        : window.location.pathname;
      window.history.replaceState(window.history.state, "", nextURL);

      setRequestedPipeline(params.get("pipeline"));
      setRequestedAsset(params.get("asset"));
    },
    [setRequestedAsset, setRequestedPipeline]
  );

  useEffect(() => {
    if (!workspace || workspace.pipelines.length === 0) {
      return;
    }

    if (requestedPipeline !== activePipeline || requestedAsset !== selectedAsset) {
      const params = new URLSearchParams(window.location.search);
      if (activePipeline) {
        params.set("pipeline", activePipeline);
      } else {
        params.delete("pipeline");
      }

      if (selectedAsset) {
        params.set("asset", selectedAsset);
      } else {
        params.delete("asset");
      }

      const query = params.toString();
      const nextURL = query
        ? `${window.location.pathname}?${query}`
        : window.location.pathname;
      window.history.replaceState(window.history.state, "", nextURL);

      setRequestedPipeline(activePipeline);
      setRequestedAsset(selectedAsset);
    }
  }, [
    activePipeline,
    requestedAsset,
    requestedPipeline,
    selectedAsset,
    setRequestedAsset,
    setRequestedPipeline,
    workspace,
  ]);

  return {
    activePipeline,
    selectedAsset,
    navigateSelection,
  };
}
