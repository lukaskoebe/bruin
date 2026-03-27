"use client";

import { useNavigate, useRouterState } from "@tanstack/react-router";
import { useAtomValue, useSetAtom } from "jotai";
import { useCallback, useEffect } from "react";

import {
  resolvedActivePipelineAtom,
  resolvedSelectedAssetAtom,
  routeSelectionAtom,
} from "@/lib/atoms/domains/workspace";

export function useWorkspaceSelection(): {
  activePipeline: string | null;
  selectedAsset: string | null;
  navigateSelection: (pipelineId: string, assetId: string | null) => void;
} {
  const navigate = useNavigate();
  const setRouteSelection = useSetAtom(routeSelectionAtom);
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

    setRouteSelection({
      pipeline: locationState.search.pipeline ?? null,
      asset: locationState.search.asset ?? null,
    });
  }, [
    locationState.pathname,
    locationState.search.asset,
    locationState.search.pipeline,
    setRouteSelection,
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
