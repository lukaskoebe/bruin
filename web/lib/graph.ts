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
  canLoadMorePreviewRows?: boolean;
  onLoadMorePreviewRows?: () => void;
  onCreateDownstreamAsset?: () => void;
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
  positionsByAssetId?: Record<string, { x: number; y: number }>,
  canLoadMoreByAssetId?: Record<string, boolean>,
  onLoadMorePreviewRows?: (assetId: string) => void
): {
  nodes: Node[];
  edges: Edge[];
} {
  if (!pipeline) {
    return { nodes: [], edges: [] };
  }

  const byName = new Map(
    pipeline.assets.map((asset) => [asset.name, asset.id])
  );
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

  const nodes: Node[] = pipeline.assets.map((asset, index) => {
    const inspect = inspectByAssetID?.[asset.id];
    const previewMode = getAssetViewMode(asset.meta);
    const isPreviewLoading = inspectLoadingByAssetID?.[asset.id] === true;
    const size = estimateNodeSize(previewMode, inspect, isPreviewLoading);
    const storedPosition = positionsByAssetId?.[asset.id];
    const fallbackPosition = defaultNodePosition(index);
    const x = storedPosition?.x ?? fallbackPosition.x;
    const y = storedPosition?.y ?? fallbackPosition.y;

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
        canLoadMorePreviewRows: canLoadMoreByAssetId?.[asset.id] === true,
        onLoadMorePreviewRows: onLoadMorePreviewRows
          ? () => onLoadMorePreviewRows(asset.id)
          : undefined,
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

export function computeGraphLayoutPositions(
  pipeline: WebPipeline | null,
  inspectByAssetID?: Record<string, AssetInspectResponse | undefined>,
  inspectLoadingByAssetID?: Record<string, boolean>
): Record<string, { x: number; y: number }> {
  if (!pipeline) {
    return {};
  }

  const byName = new Map(
    pipeline.assets.map((asset) => [asset.name, asset.id])
  );
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
    const size = estimateNodeSize(
      previewMode,
      inspect,
      inspectLoadingByAssetID?.[asset.id] === true
    );
    graph.setNode(asset.id, { width: size.width, height: size.height });
  }

  for (const asset of pipeline.assets) {
    for (const upstream of asset.upstreams ?? []) {
      const sourceId = byName.get(upstream);
      if (!sourceId) {
        continue;
      }

      graph.setEdge(sourceId, asset.id);
    }
  }

  dagre.layout(graph);

  const positions: Record<string, { x: number; y: number }> = {};
  for (const asset of pipeline.assets) {
    const inspect = inspectByAssetID?.[asset.id];
    const previewMode = getAssetViewMode(asset.meta);
    const isPreviewLoading = inspectLoadingByAssetID?.[asset.id] === true;
    const size = estimateNodeSize(previewMode, inspect, isPreviewLoading);
    const layoutNode = graph.node(asset.id) as
      | { x: number; y: number }
      | undefined;
    const x = layoutNode?.x ?? 0;
    const y = layoutNode?.y ?? 0;

    positions[asset.id] = {
      x: x - size.width / 2,
      y: y - size.height / 2,
    };
  }

  return positions;
}

function defaultNodePosition(index: number) {
  const columns = 3;
  const column = index % columns;
  const row = Math.floor(index / columns);

  return {
    x: 32 + column * 320,
    y: 32 + row * 220,
  };
}

function estimateNodeSize(
  mode: AssetViewMode | null,
  inspect: AssetInspectResponse | undefined,
  isLoading: boolean
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
    const sampledRows = inspect.rows.slice(0, 8);
    const estimatedWidth = estimateTableWidth(inspect.columns, sampledRows);
    const rowCount = Math.min(8, inspect.rows.length);
    return {
      width: estimatedWidth,
      height: Math.min(420, Math.max(170, 96 + rowCount * 28)),
    };
  }

  const textLength = JSON.stringify(inspect.rows[0] ?? {}).length;
  const estimatedLines = Math.max(6, Math.min(22, Math.ceil(textLength / 56)));
  return {
    width: 420,
    height: Math.min(500, Math.max(190, 90 + estimatedLines * 18)),
  };
}

function estimateTableWidth(
  columns: string[],
  rows: Record<string, unknown>[]
): number {
  if (columns.length === 0) {
    return 300;
  }

  const totalWidth = columns.reduce((sum, column) => {
    const values = rows.map((row) => stringifyCellValue(row[column]));
    const widestText = [column, ...values].reduce((widest, current) => {
      const width = measureTextWidth(current || "");
      return Math.max(widest, width);
    }, 0);

    return sum + Math.max(96, widestText + 28);
  }, 0);

  return Math.min(760, Math.max(300, totalWidth + 4));
}

let measurementContext: CanvasRenderingContext2D | null | undefined;

function measureTextWidth(value: string): number {
  if (typeof document === "undefined") {
    return value.length * 7;
  }

  if (measurementContext === undefined) {
    const canvas = document.createElement("canvas");
    measurementContext = canvas.getContext("2d");
    if (measurementContext) {
      measurementContext.font = "500 11px ui-sans-serif, system-ui, sans-serif";
    }
  }

  if (!measurementContext) {
    return value.length * 7;
  }

  return measurementContext.measureText(value).width;
}

function stringifyCellValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }

  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return String(value);
  }

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
