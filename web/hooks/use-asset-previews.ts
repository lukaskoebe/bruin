"use client";

import { useAtom } from "jotai";
import { useCallback, useEffect, useMemo, useRef } from "react";
import useSWR from "swr";

import { inspectAsset } from "@/lib/api";
import { changedAssetIdsAtom } from "@/lib/atoms";
import { AssetInspectResponse, WebAsset } from "@/lib/types";

/** Stable empty objects — avoids new references on every render while loading. */
const EMPTY_INSPECT_MAP: Record<string, AssetInspectResponse> = {};
const EMPTY_LOADING_MAP: Record<string, boolean> = {};

/**
 * Fetch inspect results for a list of asset IDs.
 * Returns a map of assetId → AssetInspectResponse.
 */
async function fetchInspectBatch(
  ids: string[],
): Promise<Record<string, AssetInspectResponse>> {
  const results: Record<string, AssetInspectResponse> = {};
  await Promise.all(
    ids.map(async (id) => {
      try {
        results[id] = await inspectAsset(id, { limit: 200 });
      } catch (error) {
        results[id] = {
          status: "error",
          columns: [],
          rows: [],
          raw_output: "",
          error: String(error),
        };
      }
    }),
  );
  return results;
}

/**
 * Smart asset preview hook powered by SWR.
 *
 * Uses SWR for caching, deduplication, and stale-while-revalidate.
 * When changed asset IDs arrive via SSE, only those assets are re-fetched
 * and merged into the cached data (avoids refetching all visual assets).
 *
 * IMPORTANT: The effect depends ONLY on primitive string keys to avoid
 * infinite loops. Function refs (mutate, setChangedIds) are read via
 * useRef — never placed in the dependency array.
 */
export function useAssetPreviews(visualAssets: WebAsset[]) {
  const [changedIds, setChangedIds] = useAtom(changedAssetIdsAtom);

  const assetIds = useMemo(
    () => visualAssets.map((a) => a.id).sort(),
    [visualAssets],
  );

  const idsKey = assetIds.join(",");
  const swrKey = idsKey ? `inspect:${idsKey}` : null;

  const {
    data,
    isValidating,
    mutate,
  } = useSWR<Record<string, AssetInspectResponse>>(
    swrKey,
    () => fetchInspectBatch(assetIds),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      dedupingInterval: 2000,
    },
  );

  // Stable reference: module-level constant when data is not yet loaded
  // to avoid creating a new {} on every render (which causes cascading
  // useMemo recomputations and infinite loops).
  const inspectByAssetId = data ?? EMPTY_INSPECT_MAP;

  // --- Refs for values used inside effects that must NOT be deps. ---
  const mutateRef = useRef(mutate);
  useEffect(() => { mutateRef.current = mutate; });

  const setChangedIdsRef = useRef(setChangedIds);
  useEffect(() => { setChangedIdsRef.current = setChangedIds; });

  // Primitive string key derived from the changedIds atom — safe as an
  // effect dep because it only changes when new IDs arrive or are drained.
  const relevantChangedKey = useMemo(
    () =>
      Array.from(changedIds)
        .filter((id) => assetIds.includes(id))
        .sort()
        .join(","),
    [changedIds, assetIds],
  );

  useEffect(() => {
    if (!relevantChangedKey || !swrKey) return;

    const idsToRefresh = relevantChangedKey.split(",").filter(Boolean);

    // Drain the processed IDs from the atom.
    setChangedIdsRef.current((prev: Set<string>) => {
      const next = new Set(prev);
      let removed = false;
      for (const id of idsToRefresh) {
        if (next.delete(id)) removed = true;
      }
      return removed ? next : prev;
    });

    // Selectively re-fetch only the changed assets and merge with cache.
    void mutateRef.current(
      async (prev) => {
        const refreshed = await fetchInspectBatch(idsToRefresh);
        return { ...(prev ?? {}), ...refreshed };
      },
      { revalidate: false },
    );
    // ONLY primitive string keys here — no objects, no function refs.
  }, [relevantChangedKey, swrKey]);

  // Build per-asset loading map — an asset is loading when SWR is validating
  // and we don't yet have data for it.
  // Stable empty ref when not loading to prevent cascading re-renders.
  const inspectLoadingByAssetId = useMemo<Record<string, boolean>>(() => {
    if (!isValidating) return EMPTY_LOADING_MAP;
    const loading: Record<string, boolean> = {};
    for (const id of assetIds) {
      if (!(id in inspectByAssetId)) {
        loading[id] = true;
      }
    }
    return Object.keys(loading).length > 0 ? loading : EMPTY_LOADING_MAP;
  }, [isValidating, assetIds, inspectByAssetId]);

  const clearPreviewForAsset = useCallback(
    (assetId: string) => {
      void mutateRef.current(
        (prev) => {
          if (!prev) return prev;
          const next = { ...prev };
          delete next[assetId];
          return next;
        },
        { revalidate: false },
      );
    },
    [],
  );

  return {
    inspectByAssetId,
    inspectLoadingByAssetId,
    clearPreviewForAsset,
  };
}
