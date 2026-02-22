import dagre from "dagre";
import { Edge, Node } from "reactflow";

import { AssetViewMode, getAssetViewMode } from "@/lib/asset-visualization";
import { AssetInspectResponse, WebPipeline } from "@/lib/types";

export type AssetNodeData = {
  name: string;
  assetType: string;
  connection?: string;
  meta?: Record<string, string>;
  isMaterialized: boolean;
  freshnessStatus?: "fresh" | "stale";
  materializedAs?: string;
  rowCount?: number;
  previewLoading?: boolean;
  preview?: {
    mode: "table" | "chart" | "markdown";
    columns: string[];
    rows: Record<string, unknown>[];
    error?: string;
  };
};

export function buildFlowFromPipeline(
  pipeline: WebPipeline | null,
  inspectByAssetID?: Record<string, AssetInspectResponse | undefined>,
  inspectLoadingByAssetID?: Record<string, boolean>,
): {
  nodes: Node[];
  edges: Edge[];
} {
  if (!pipeline) {
    return { nodes: [], edges: [] };
  }

  const byName = new Map(pipeline.assets.map((asset) => [asset.name, asset.id]));
  const edges: Edge[] = [];

  for (const asset of pipeline.assets) {
    for (const upstream of asset.upstreams ?? []) {
      const sourceId = byName.get(upstream);
      if (!sourceId) {
        continue;
      }

      edges.push({
        id: `${sourceId}->${asset.id}`,
        source: sourceId,
        target: asset.id,
      });
    }
  }

  const graph = new dagre.graphlib.Graph();
  graph.setDefaultEdgeLabel(() => ({}));
  graph.setGraph({
    rankdir: "TB",
    nodesep: 64,
    ranksep: 120,
    marginx: 24,
    marginy: 24,
    ranker: "network-simplex",
  });

  for (const asset of pipeline.assets) {
    const inspect = inspectByAssetID?.[asset.id];
    const previewMode = getAssetViewMode(asset.meta);
    const size = estimateNodeSize(previewMode, inspect, inspectLoadingByAssetID?.[asset.id] === true);
    graph.setNode(asset.id, { width: size.width, height: size.height });
  }

  for (const edge of edges) {
    graph.setEdge(edge.source, edge.target);
  }

  dagre.layout(graph);

  const nodes: Node[] = pipeline.assets.map((asset) => {
    const inspect = inspectByAssetID?.[asset.id];
    const previewMode = getAssetViewMode(asset.meta);
    const isPreviewLoading = inspectLoadingByAssetID?.[asset.id] === true;
    const size = estimateNodeSize(previewMode, inspect, isPreviewLoading);
    const layoutNode = graph.node(asset.id) as { x: number; y: number } | undefined;
    const x = layoutNode?.x ?? 0;
    const y = layoutNode?.y ?? 0;

    return {
      id: asset.id,
      type: "assetNode",
      position: {
        x: x - size.width / 2,
        y: y - size.height / 2,
      },
      data: {
        name: asset.name,
        assetType: asset.type,
        connection: asset.connection,
        meta: asset.meta,
        isMaterialized: asset.is_materialized,
        freshnessStatus: asset.freshness_status,
        materializedAs: asset.materialized_as || asset.materialization_type,
        rowCount: asset.row_count,
        previewLoading: isPreviewLoading,
        preview:
          previewMode && inspect && (inspect.rows.length > 0 || inspect.error)
            ? {
                mode: previewMode,
                columns: inspect.columns,
                rows: inspect.rows,
                error: inspect.error,
              }
            : undefined,
      },
    };
  });

  return { nodes, edges };
}

function estimateNodeSize(
  mode: AssetViewMode | null,
  inspect: AssetInspectResponse | undefined,
  isLoading: boolean,
) {
  if (!mode) {
    return { width: 260, height: 126 };
  }

  if (mode === "chart") {
    return { width: 380, height: 280 };
  }

  if (isLoading) {
    if (mode === "table") {
      return { width: 420, height: 230 };
    }
    return { width: 420, height: 240 };
  }

  if (!inspect) {
    return { width: 260, height: 126 };
  }

  if (mode === "table") {
    const cols = Math.max(1, inspect.columns.length);
    const rows = Math.min(8, inspect.rows.length);
    return {
      width: Math.min(760, Math.max(300, cols * 130)),
      height: Math.min(420, Math.max(170, 96 + rows * 28)),
    };
  }

  const textLength = JSON.stringify(inspect.rows[0] ?? {}).length;
  const estimatedLines = Math.max(6, Math.min(22, Math.ceil(textLength / 56)));
  return {
    width: 420,
    height: Math.min(500, Math.max(190, 90 + estimatedLines * 18)),
  };
}