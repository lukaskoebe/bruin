"use client";

import { useNavigate, useRouterState } from "@tanstack/react-router";
import { useAtomValue, useSetAtom } from "jotai";
import { useCallback, useEffect } from "react";

import {
  activePipelineAtom,
  resolvedActivePipelineAtom,
  resolvedSelectedAssetAtom,
  selectedAssetAtom,
} from "@/lib/atoms";

export function useWorkspaceSelection(): {
  activePipeline: string | null;
  selectedAsset: string | null;
  navigateSelection: (pipelineId: string, assetId: string | null) => void;
} {
  const navigate = useNavigate();
  const setRequestedPipeline = useSetAtom(activePipelineAtom);
  const setRequestedAsset = useSetAtom(selectedAssetAtom);
  const requestedPipeline = useAtomValue(activePipelineAtom);
  const requestedAsset = useAtomValue(selectedAssetAtom);
  const activePipeline = useAtomValue(resolvedActivePipelineAtom);
  const selectedAsset = useAtomValue(resolvedSelectedAssetAtom);
  const locationState = useRouterState({
    select: (state) => ({
      pathname: state.location.pathname,
      search: state.location.search as {
        pipeline?: string;
        asset?: string;
      },
    }),
  });

  useEffect(() => {
    if (locationState.pathname !== "/") {
      return;
    }

    const nextPipeline = locationState.search.pipeline ?? null;
    const nextAsset = locationState.search.asset ?? null;

    if (requestedPipeline !== nextPipeline) {
      setRequestedPipeline(nextPipeline);
    }

    if (requestedAsset !== nextAsset) {
      setRequestedAsset(nextAsset);
    }
  }, [
    locationState.pathname,
    locationState.search.asset,
    locationState.search.pipeline,
    requestedAsset,
    requestedPipeline,
    setRequestedAsset,
    setRequestedPipeline,
  ]);

  const navigateSelection = useCallback(
    (pipelineId: string, assetId: string | null) => {
      const nextAsset = assetId ?? undefined;

      if (
        locationState.pathname === "/" &&
        locationState.search.pipeline === pipelineId &&
        locationState.search.asset === nextAsset
      ) {
        return;
      }

      void navigate({
        to: "/",
        search: {
          pipeline: pipelineId,
          asset: nextAsset,
        },
        replace: true,
      });
    },
    [
      locationState.pathname,
      locationState.search.asset,
      locationState.search.pipeline,
      navigate,
    ]
  );

  return {
    activePipeline,
    selectedAsset,
    navigateSelection,
  };
}
