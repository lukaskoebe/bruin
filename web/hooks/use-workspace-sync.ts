"use client";

import { useAtomValue, useSetAtom } from "jotai";
import { useEffect } from "react";

import { changedAssetIdsAtom, workspaceAtom } from "@/lib/atoms";
import { getWorkspace } from "@/lib/api";
import { WorkspaceEvent, WorkspaceState } from "@/lib/types";

function mergeWorkspaceWithPreservedContent(
  current: WorkspaceState | null,
  incoming: WorkspaceState,
): WorkspaceState {
  if (!current) {
    return incoming;
  }

  const contentByAssetId = new Map<string, string>();
  for (const pipeline of current.pipelines ?? []) {
    for (const asset of pipeline.assets ?? []) {
      if (asset.id && asset.content) {
        contentByAssetId.set(asset.id, asset.content);
      }
    }
  }

  const nextPipelines = (incoming.pipelines ?? []).map((pipeline) => ({
    ...pipeline,
    assets: (pipeline.assets ?? []).map((asset) => {
      if (asset.content) {
        return asset;
      }

      const preserved = contentByAssetId.get(asset.id);
      if (!preserved) {
        return asset;
      }

      return {
        ...asset,
        content: preserved,
      };
    }),
  }));

  return {
    ...incoming,
    pipelines: nextPipelines,
  };
}

export function useWorkspaceSync() {
  const workspace = useAtomValue(workspaceAtom);
  const setWorkspace = useSetAtom(workspaceAtom);
  const setChangedAssetIds = useSetAtom(changedAssetIdsAtom);

  useEffect(() => {
    let mounted = true;

    getWorkspace()
      .then((data) => {
        if (mounted) {
          setWorkspace(data);
        }
      })
      .catch(() => undefined);

    const source = new EventSource("/api/events");
    source.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data) as WorkspaceEvent;

        // Accumulate changed asset IDs for downstream consumers (e.g. preview refresh).
        const incomingChanged = payload.changed_asset_ids;
        if (incomingChanged && incomingChanged.length > 0) {
          setChangedAssetIds((prev: Set<string>) => {
            let changed = false;
            const next = new Set(prev);
            for (const id of incomingChanged) {
              if (!next.has(id)) {
                next.add(id);
                changed = true;
              }
            }
            return changed ? next : prev;
          });
        }

        setWorkspace((current) => {
          const currentRevision = current?.revision ?? -1;
          const incomingRevision = payload.workspace?.revision ?? currentRevision + 1;

          if (incomingRevision <= currentRevision) {
            return current;
          }

          if (payload.lite) {
            return mergeWorkspaceWithPreservedContent(current, payload.workspace);
          }

          return payload.workspace;
        });
      } catch {
        return;
      }
    };

    return () => {
      mounted = false;
      source.close();
    };
  }, [setWorkspace, setChangedAssetIds]);

  return workspace as WorkspaceState | null;
}