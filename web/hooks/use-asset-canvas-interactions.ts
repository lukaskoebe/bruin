"use client";

import { useAtomValue } from "jotai";
import { Dispatch, MutableRefObject, SetStateAction, useCallback, useEffect, useState } from "react";
import { Edge, Node, ReactFlowInstance } from "reactflow";

import { NewAssetKind, NewAssetNodeData } from "@/components/new-asset-node";
import { StoredNodePositions } from "@/hooks/use-persisted-node-positions";
import { pipelineAtom, resolvedSelectedAssetAtom } from "@/lib/atoms";

type NewAssetDraftState = {
  flowX: number;
  flowY: number;
  name: string;
  kind: NewAssetKind;
};

type UseAssetCanvasInteractionsInput = {
  reactFlowInstance: ReactFlowInstance | null;
  canvasContainerRef: MutableRefObject<HTMLDivElement | null>;
  graphNodes: Node[];
  graphEdges: Edge[];
  connectedNodeIDs: Set<string>;
  storedNodePositions: StoredNodePositions;
  setStoredNodePositions: Dispatch<SetStateAction<StoredNodePositions>>;
  defaultAssetNamesByKind: Record<NewAssetKind, string>;
  setNodes: Dispatch<SetStateAction<Node[]>>;
  setEdges: Dispatch<SetStateAction<Edge[]>>;
  runCreateAsset: (
    pipelineId: string,
    input: { name?: string; type?: string; path?: string; content?: string }
  ) => Promise<{ asset_id?: string } | null>;
  navigateSelection: (pipelineId: string, assetId: string | null) => void;
  buildCreateAssetInput: (
    name: string,
    kind: NewAssetKind
  ) => { name: string; type: string; path?: string; content?: string };
};

const NEW_ASSET_NODE_ID = "__new_asset__";

export function useAssetCanvasInteractions({
  reactFlowInstance,
  canvasContainerRef,
  graphNodes,
  graphEdges,
  connectedNodeIDs,
  storedNodePositions,
  setStoredNodePositions,
  defaultAssetNamesByKind,
  setNodes,
  setEdges,
  runCreateAsset,
  navigateSelection,
  buildCreateAssetInput,
}: UseAssetCanvasInteractionsInput) {
  const pipeline = useAtomValue(pipelineAtom);
  const selectedAssetId = useAtomValue(resolvedSelectedAssetAtom);
  const pipelineId = pipeline?.id ?? null;
  const [newAssetDraft, setNewAssetDraft] = useState<NewAssetDraftState | null>(
    null
  );

  const openNewAssetInput = useCallback(
    (clientX: number, clientY: number) => {
      const container = canvasContainerRef.current;
      if (!container) {
        return;
      }

      const rect = container.getBoundingClientRect();
      const x = Math.max(12, Math.min(clientX - rect.left, rect.width - 260));
      const y = Math.max(12, Math.min(clientY - rect.top, rect.height - 130));
      const flowPosition = reactFlowInstance?.screenToFlowPosition({
        x: clientX,
        y: clientY,
      });

      setNewAssetDraft({
        flowX: flowPosition?.x ?? x,
        flowY: flowPosition?.y ?? y,
        name: defaultAssetNamesByKind.sql,
        kind: "sql",
      });
    },
    [canvasContainerRef, defaultAssetNamesByKind.sql, reactFlowInstance]
  );

  const handlePaneClick = useCallback(
    (event: React.MouseEvent) => {
      if (newAssetDraft) {
        setNewAssetDraft(null);
        return;
      }

      if (!pipelineId) {
        return;
      }

      openNewAssetInput(event.clientX, event.clientY);
    },
    [newAssetDraft, openNewAssetInput, pipelineId]
  );

  const handlePaneContextMenu = useCallback(
    (event: React.MouseEvent) => {
      event.preventDefault();
      if (newAssetDraft) {
        setNewAssetDraft(null);
        return;
      }

      if (!pipelineId) {
        return;
      }

      openNewAssetInput(event.clientX, event.clientY);
    },
    [newAssetDraft, openNewAssetInput, pipelineId]
  );

  const submitNewAsset = useCallback(
    (nameValue?: string) => {
      if (!pipelineId || !newAssetDraft) {
        return;
      }

      const name = (nameValue ?? newAssetDraft.name).trim();
      if (!name) {
        setNewAssetDraft(null);
        return;
      }

      const draftPosition = { x: newAssetDraft.flowX, y: newAssetDraft.flowY };
      const createInput = buildCreateAssetInput(name, newAssetDraft.kind);
      void runCreateAsset(pipelineId, createInput).then((response) => {
        if (response?.asset_id) {
          setStoredNodePositions((previous) => ({
            ...previous,
            [response.asset_id as string]: draftPosition,
          }));
          navigateSelection(pipelineId, response.asset_id);
        }
      });
      setNewAssetDraft(null);
    },
    [
      buildCreateAssetInput,
      navigateSelection,
      newAssetDraft,
      pipelineId,
      runCreateAsset,
      setStoredNodePositions,
    ]
  );

  useEffect(() => {
    const mappedNodes = graphNodes.map((node) => ({
      ...node,
      position:
        !connectedNodeIDs.has(node.id) && storedNodePositions[node.id]
          ? storedNodePositions[node.id]
          : node.position,
      selected: selectedAssetId ? node.id === selectedAssetId : false,
    }));

    if (newAssetDraft) {
      const draftData: NewAssetNodeData = {
        name: newAssetDraft.name,
        kind: newAssetDraft.kind,
        onKindChange: (kind) => {
          const nextName = defaultAssetNamesByKind[kind];
          setNewAssetDraft((previous) =>
            previous ? { ...previous, kind, name: nextName } : previous
          );
          return nextName;
        },
        onCreate: (name) => submitNewAsset(name),
        onCancel: () => setNewAssetDraft(null),
      };

      mappedNodes.push({
        id: NEW_ASSET_NODE_ID,
        type: "newAssetNode",
        data: draftData,
        position: { x: newAssetDraft.flowX, y: newAssetDraft.flowY },
        selected: false,
        draggable: true,
        selectable: false,
      });
    }

    setNodes(mappedNodes);
    setEdges(graphEdges);
  }, [
    connectedNodeIDs,
    defaultAssetNamesByKind,
    graphEdges,
    graphNodes,
    newAssetDraft,
    selectedAssetId,
    setEdges,
    setNodes,
    storedNodePositions,
    submitNewAsset,
  ]);

  const handleNodeDragStop = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      if (node.id === NEW_ASSET_NODE_ID) {
        setNewAssetDraft((previous) =>
          previous
            ? { ...previous, flowX: node.position.x, flowY: node.position.y }
            : previous
        );
        return;
      }

      if (connectedNodeIDs.has(node.id)) {
        return;
      }

      setStoredNodePositions((previous) => ({
        ...previous,
        [node.id]: node.position,
      }));
    },
    [connectedNodeIDs, setStoredNodePositions]
  );

  const handleNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      if (node.id === NEW_ASSET_NODE_ID) {
        return;
      }
      if (pipelineId) {
        navigateSelection(pipelineId, node.id);
      }
    },
    [navigateSelection, pipelineId]
  );

  return {
    handlePaneClick,
    handlePaneContextMenu,
    handleNodeDragStop,
    handleNodeClick,
  };
}
