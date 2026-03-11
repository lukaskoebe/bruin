"use client";

import { useAtom } from "jotai";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { inspectAsset } from "@/lib/api";
import { changedAssetIdsAtom } from "@/lib/atoms";
import { AssetInspectResponse, WebAsset } from "@/lib/types";

/** Stable empty objects — avoids new references on every render while idle. */
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
 * Persistent asset preview cache.
 *
 * Keeps inspect results by asset ID across changes to the current set of visual
 * assets, so adding a visualization to one asset does not wipe or re-fetch the
 * previews for every other visual asset.
 */
export function useAssetPreviews(visualAssets: WebAsset[]) {
  const [changedIds, setChangedIds] = useAtom(changedAssetIdsAtom);
  const [inspectCache, setInspectCache] = useState<Record<string, AssetInspectResponse>>({});
  const [loadingByAssetId, setLoadingByAssetId] = useState<Record<string, boolean>>({});

  const assetIds = useMemo(
    () => visualAssets.map((a) => a.id).sort(),
    [visualAssets],
  );

  const setChangedIdsRef = useRef(setChangedIds);
  useEffect(() => { setChangedIdsRef.current = setChangedIds; });

  const mergeResults = useCallback((results: Record<string, AssetInspectResponse>) => {
    if (Object.keys(results).length === 0) {
      return;
    }

    setInspectCache((prev) => ({ ...prev, ...results }));
  }, []);

  const markLoading = useCallback((ids: string[], isLoading: boolean) => {
    if (ids.length === 0) {
      return;
    }

    setLoadingByAssetId((prev) => {
      const next = { ...prev };
      for (const id of ids) {
        if (isLoading) {
          next[id] = true;
        } else {
          delete next[id];
        }
      }
      return next;
    });
  }, []);

  const fetchAndMerge = useCallback(
    async (ids: string[]) => {
      if (ids.length === 0) {
        return;
      }

      markLoading(ids, true);
      try {
        const results = await fetchInspectBatch(ids);
        mergeResults(results);
      } finally {
        markLoading(ids, false);
      }
    },
    [markLoading, mergeResults],
  );

  useEffect(() => {
    const missingIds = assetIds.filter((id) => !(id in inspectCache));
    if (missingIds.length === 0) {
      return;
    }

    void fetchAndMerge(missingIds);
  }, [assetIds, fetchAndMerge, inspectCache]);

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
    if (!relevantChangedKey) return;

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

    void fetchAndMerge(idsToRefresh);
  }, [fetchAndMerge, relevantChangedKey]);

  const inspectByAssetId = useMemo<Record<string, AssetInspectResponse>>(() => {
    if (assetIds.length === 0) {
      return EMPTY_INSPECT_MAP;
    }

    const next: Record<string, AssetInspectResponse> = {};
    for (const id of assetIds) {
      const inspect = inspectCache[id];
      if (inspect) {
        next[id] = inspect;
      }
    }

    return Object.keys(next).length > 0 ? next : EMPTY_INSPECT_MAP;
  }, [assetIds, inspectCache]);

  const inspectLoadingByAssetId = useMemo<Record<string, boolean>>(() => {
    if (assetIds.length === 0) return EMPTY_LOADING_MAP;
    const loading: Record<string, boolean> = {};
    for (const id of assetIds) {
      if (loadingByAssetId[id]) {
        loading[id] = true;
      }
    }
    return Object.keys(loading).length > 0 ? loading : EMPTY_LOADING_MAP;
  }, [assetIds, loadingByAssetId]);

  const clearPreviewForAsset = useCallback(
    (assetId: string) => {
      setInspectCache((prev) => {
        if (!(assetId in prev)) {
          return prev;
        }

        const next = { ...prev };
        delete next[assetId];
        return next;
      });

      setLoadingByAssetId((prev) => {
        if (!(assetId in prev)) {
          return prev;
        }

        const next = { ...prev };
        delete next[assetId];
        return next;
      });
    },
    [],
  );

  return {
    inspectByAssetId,
    inspectLoadingByAssetId,
    clearPreviewForAsset,
  };
}
