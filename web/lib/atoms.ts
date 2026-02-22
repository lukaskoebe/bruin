import { atom } from "jotai";
import { Edge, Node } from "reactflow";

import { AssetInspectResponse, WebAsset, WebPipeline, WorkspaceState } from "@/lib/types";

export const workspaceAtom = atom<WorkspaceState | null>(null);
export const activePipelineAtom = atom<string | null>(null);
export const selectedAssetAtom = atom<string | null>(null);

export const resolvedActivePipelineAtom = atom<string | null>((get) => {
	const workspace = get(workspaceAtom);
	const activePipeline = get(activePipelineAtom);

	if (!workspace || workspace.pipelines.length === 0) {
		return activePipeline;
	}

	const selectedPipeline =
		workspace.pipelines.find((pipeline) => pipeline.id === activePipeline) ??
		workspace.pipelines[0];

	return selectedPipeline.id;
});

export const pipelineAtom = atom<WebPipeline | null>((get) => {
	const workspace = get(workspaceAtom);
	const activePipeline = get(resolvedActivePipelineAtom);

	if (!workspace || !activePipeline) {
		return null;
	}

	return workspace.pipelines.find((pipeline) => pipeline.id === activePipeline) ?? null;
});

export const resolvedSelectedAssetAtom = atom<string | null>((get) => {
	const pipeline = get(pipelineAtom);
	const selectedAsset = get(selectedAssetAtom);

	if (!pipeline) {
		return selectedAsset;
	}

	const pipelineAsset =
		pipeline.assets.find((asset) => asset.id === selectedAsset) ??
		pipeline.assets[0] ??
		null;

	return pipelineAsset?.id ?? null;
});

export const selectedAssetDataAtom = atom<WebAsset | null>((get) => {
	const pipeline = get(pipelineAtom);
	const selectedAsset = get(resolvedSelectedAssetAtom);

	if (!pipeline || !selectedAsset) {
		return null;
	}

	return pipeline.assets.find((asset) => asset.id === selectedAsset) ?? null;
});

export type MaterializationState = {
	is_materialized: boolean;
	freshness_status?: "fresh" | "stale";
	materialized_as?: string;
	row_count?: number;
	connection?: string;
	materialization_type?: string;
};

export type MaterializationByAssetId = Record<string, MaterializationState>;

export const materializationByAssetIdAtom = atom<MaterializationByAssetId>({});

export const enrichedPipelineAtom = atom<WebPipeline | null>((get) => {
	const pipeline = get(pipelineAtom);
	const materializationByAssetId = get(materializationByAssetIdAtom);

	if (!pipeline) {
		return null;
	}

	return {
		...pipeline,
		assets: pipeline.assets.map((pipelineAsset) => {
			const lazy = materializationByAssetId[pipelineAsset.id];
			if (!lazy) {
				return pipelineAsset;
			}

			return {
				...pipelineAsset,
				is_materialized: lazy.is_materialized,
				freshness_status: lazy.freshness_status ?? pipelineAsset.freshness_status,
				materialized_as: lazy.materialized_as ?? pipelineAsset.materialized_as,
				row_count:
					lazy.row_count === undefined ? pipelineAsset.row_count : lazy.row_count,
				connection: lazy.connection ?? pipelineAsset.connection,
				materialization_type:
					lazy.materialization_type ?? pipelineAsset.materialization_type,
			};
		}),
	};
});

export const enrichedSelectedAssetAtom = atom<WebAsset | null>((get) => {
	const pipeline = get(enrichedPipelineAtom);
	const selectedAsset = get(resolvedSelectedAssetAtom);

	if (!pipeline || !selectedAsset) {
		return null;
	}

	return pipeline.assets.find((asset) => asset.id === selectedAsset) ?? null;
});

export type EditorDraftState = {
	assetId: string | null;
	content: string;
};

export const editorDraftAtom = atom<EditorDraftState>({
	assetId: null,
	content: "",
});

export const editorValueAtom = atom<string>((get) => {
	const asset = get(selectedAssetDataAtom);
	const editorDraft = get(editorDraftAtom);

	if (asset && editorDraft.assetId === asset.id) {
		return editorDraft.content;
	}

	return asset?.content ?? "";
});

export type AssetResultTab = "inspect" | "materialize";

export type AssetResultsState = {
	inspectResult: AssetInspectResponse | null;
	inspectLoading: boolean;
	materializeOutput: string;
	materializeStatus: "ok" | "error" | null;
	materializeError: string;
	materializeLoading: boolean;
	resultTab: AssetResultTab;
};

export const assetResultsAtom = atom<AssetResultsState>({
	inspectResult: null,
	inspectLoading: false,
	materializeOutput: "",
	materializeStatus: null,
	materializeError: "",
	materializeLoading: false,
	resultTab: "inspect",
});

export type AssetEditorTab = "configuration" | "checks" | "visualization";

export const assetEditorTabAtom = atom<AssetEditorTab>("configuration");

export const nodesAtom = atom<Node[]>([]);
export const edgesAtom = atom<Edge[]>([]);

// Tracks which asset IDs have changed (content edit or materialization).
// The SSE handler accumulates IDs here; useAssetPreviews drains them after processing.
export const changedAssetIdsAtom = atom<Set<string>>(new Set<string>());