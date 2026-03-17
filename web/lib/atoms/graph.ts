import { atom } from "jotai";
import { Edge, Node } from "reactflow";

export const nodesAtom = atom<Node[]>([]);
export const edgesAtom = atom<Edge[]>([]);

// Tracks which asset IDs have changed (content edit or materialization).
// The SSE handler accumulates IDs here; useAssetPreviews drains them after processing.
export const changedAssetIdsAtom = atom<Set<string>>(new Set<string>());