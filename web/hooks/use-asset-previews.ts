"use client";

import { useAtom, useSetAtom } from "jotai";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { inspectAsset } from "@/lib/api";
import {
  getTablePreviewLimit,
  getAssetViewMode,
} from "@/lib/asset-visualization";
import { changedAssetIdsAtom, registerAssetColumnsAtom as registerAssetColumnsSuggestionAtom } from "@/lib/atoms";
import { AssetInspectResponse, WebAsset } from "@/lib/types";

/** Stable empty objects — avoids new references on every render while idle. */
const EMPTY_INSPECT_MAP: Record<string, AssetInspectResponse> = {};
const EMPTY_LOADING_MAP: Record<string, boolean> = {};

/**
 * Fetch inspect results for a list of asset IDs.
 * Returns a map of assetId → AssetInspectResponse.
 */
async function fetchInspectBatch(
  requests: Array<{ id: string; limit: number }>
): Promise<Record<string, AssetInspectResponse>> {
  const results: Record<string, AssetInspectResponse> = {};
  await Promise.all(
    requests.map(async ({ id, limit }) => {
      try {
        results[id] = await inspectAsset(id, { limit });
      } catch (error) {
        results[id] = {
          status: "error",
          columns: [],
          rows: [],
          raw_output: "",
          error: String(error),
        };
      }
    })
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
  const registerAssetColumns = useSetAtom(registerAssetColumnsSuggestionAtom);
  const [inspectCache, setInspectCache] = useState<
    Record<string, AssetInspectResponse>
  >({});
  const [loadingByAssetId, setLoadingByAssetId] = useState<
    Record<string, boolean>
  >({});
  const [requestedLimitByAssetId, setRequestedLimitByAssetId] = useState<
    Record<string, number>
  >({});
  const [fetchedLimitByAssetId, setFetchedLimitByAssetId] = useState<
    Record<string, number>
  >({});

  const assetIds = useMemo(
    () => visualAssets.map((a) => a.id).sort(),
    [visualAssets]
  );
  const assetById = useMemo(
    () => Object.fromEntries(visualAssets.map((asset) => [asset.id, asset])),
    [visualAssets]
  );
  const baseLimitByAssetId = useMemo(() => {
    const limits: Record<string, number> = {};
    for (const asset of visualAssets) {
      limits[asset.id] =
        getAssetViewMode(asset.meta) === "table"
          ? getTablePreviewLimit(asset.meta, 25)
          : 200;
    }
    return limits;
  }, [visualAssets]);

  const setChangedIdsRef = useRef(setChangedIds);
  useEffect(() => {
    setChangedIdsRef.current = setChangedIds;
  });

  useEffect(() => {
    setRequestedLimitByAssetId((prev) => {
      let changed = false;
      const next = { ...prev };

      for (const [assetId, baseLimit] of Object.entries(baseLimitByAssetId)) {
        if (next[assetId] === undefined) {
          next[assetId] = baseLimit;
          changed = true;
          continue;
        }

        if (next[assetId] < baseLimit) {
          next[assetId] = baseLimit;
          changed = true;
        }
      }

      return changed ? next : prev;
    });
  }, [baseLimitByAssetId]);

  const mergeResults = useCallback(
    (results: Record<string, AssetInspectResponse>) => {
      if (Object.keys(results).length === 0) {
        return;
      }

      setInspectCache((prev) => ({ ...prev, ...results }));

      for (const [assetId, result] of Object.entries(results)) {
        registerAssetColumns({
          assetId,
          method: "asset-inspect",
          columns: (result.columns ?? []).map((name) => ({ name })),
        });
      }
    },
    [registerAssetColumns]
  );

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
    async (requests: Array<{ id: string; limit: number }>) => {
      if (requests.length === 0) {
        return;
      }

      const ids = requests.map((request) => request.id);
      markLoading(ids, true);
      try {
        const results = await fetchInspectBatch(requests);
        mergeResults(results);
        setFetchedLimitByAssetId((prev) => {
          const next = { ...prev };
          for (const request of requests) {
            next[request.id] = request.limit;
          }
          return next;
        });
      } finally {
        markLoading(ids, false);
      }
    },
    [markLoading, mergeResults]
  );

  useEffect(() => {
    const missingRequests = assetIds
      .filter((id) => !(id in inspectCache))
      .map((id) => ({
        id,
        limit: requestedLimitByAssetId[id] ?? baseLimitByAssetId[id] ?? 200,
      }));

    if (missingRequests.length === 0) {
      return;
    }

    void fetchAndMerge(missingRequests);
  }, [
    assetIds,
    baseLimitByAssetId,
    fetchAndMerge,
    inspectCache,
    requestedLimitByAssetId,
  ]);

  useEffect(() => {
    const expandedRequests = assetIds
      .filter(
        (id) =>
          (requestedLimitByAssetId[id] ?? 0) > (fetchedLimitByAssetId[id] ?? 0)
      )
      .map((id) => ({ id, limit: requestedLimitByAssetId[id] }));

    if (expandedRequests.length === 0) {
      return;
    }

    void fetchAndMerge(expandedRequests);
  }, [assetIds, fetchAndMerge, fetchedLimitByAssetId, requestedLimitByAssetId]);

  // Primitive string key derived from the changedIds atom — safe as an
  // effect dep because it only changes when new IDs arrive or are drained.
  const relevantChangedKey = useMemo(
    () =>
      Array.from(changedIds)
        .filter((id) => assetIds.includes(id))
        .sort()
        .join(","),
    [changedIds, assetIds]
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

    void fetchAndMerge(
      idsToRefresh.map((id) => ({
        id,
        limit: requestedLimitByAssetId[id] ?? baseLimitByAssetId[id] ?? 200,
      }))
    );
  }, [
    baseLimitByAssetId,
    fetchAndMerge,
    relevantChangedKey,
    requestedLimitByAssetId,
  ]);

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

  const clearPreviewForAsset = useCallback((assetId: string) => {
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

    setFetchedLimitByAssetId((prev) => {
      if (!(assetId in prev)) {
        return prev;
      }

      const next = { ...prev };
      delete next[assetId];
      return next;
    });
  }, []);

  const canLoadMoreByAssetId = useMemo<Record<string, boolean>>(() => {
    const next: Record<string, boolean> = {};
    for (const assetId of assetIds) {
      const asset = assetById[assetId];
      const inspect = inspectCache[assetId];
      if (!asset || !inspect || getAssetViewMode(asset.meta) !== "table") {
        continue;
      }

      const requestedLimit =
        requestedLimitByAssetId[assetId] ?? baseLimitByAssetId[assetId] ?? 25;
      if (inspect.rows.length >= requestedLimit) {
        next[assetId] = true;
      }
    }
    return next;
  }, [
    assetById,
    assetIds,
    baseLimitByAssetId,
    inspectCache,
    requestedLimitByAssetId,
  ]);

  const loadMorePreviewRows = useCallback(
    (assetId: string) => {
      const baseLimit = baseLimitByAssetId[assetId] ?? 25;
      setRequestedLimitByAssetId((prev) => ({
        ...prev,
        [assetId]: (prev[assetId] ?? baseLimit) + baseLimit,
      }));
    },
    [baseLimitByAssetId]
  );

  return {
    inspectByAssetId,
    inspectLoadingByAssetId,
    canLoadMoreByAssetId,
    loadMorePreviewRows,
    clearPreviewForAsset,
  };
}
