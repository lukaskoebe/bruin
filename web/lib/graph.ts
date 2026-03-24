import dagre from "dagre";
import { Edge, Node } from "reactflow";

import { AssetViewMode, getAssetViewMode } from "@/lib/asset-visualization";
import { AssetInspectResponse, WebPipeline } from "@/lib/types";

const DOWNSTREAM_NODE_VERTICAL_GAP = 40;

export type AssetNodeData = {
  name: string;
  assetType: string;
  connection?: string;
  parameters?: Record<string, string>;
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

  const assetsById = new Map(pipeline.assets.map((asset) => [asset.id, asset]));
  const byName = new Map(
    pipeline.assets.map((asset) => [asset.name, asset.id])
  );
  const edges: Edge[] = [];
  const sizeByAssetId = new Map<string, { width: number; height: number }>();

  for (const asset of pipeline.assets) {
    const inspect = inspectByAssetID?.[asset.id];
    const previewMode = getAssetViewMode(asset.meta);
    const isPreviewLoading = inspectLoadingByAssetID?.[asset.id] === true;
    sizeByAssetId.set(
      asset.id,
      estimateNodeSize(asset.type, previewMode, inspect, isPreviewLoading)
    );
  }

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

  const resolvedPositionsByAssetId = new Map<
    string,
    { x: number; y: number }
  >();

  const resolveNodePosition = (
    assetId: string,
    index: number,
    visiting = new Set<string>()
  ): { x: number; y: number } => {
    const cachedPosition = resolvedPositionsByAssetId.get(assetId);
    if (cachedPosition) {
      return cachedPosition;
    }

    const storedPosition = positionsByAssetId?.[assetId];
    if (storedPosition) {
      resolvedPositionsByAssetId.set(assetId, storedPosition);
      return storedPosition;
    }

    if (visiting.has(assetId)) {
      const fallbackPosition = defaultNodePosition(index);
      resolvedPositionsByAssetId.set(assetId, fallbackPosition);
      return fallbackPosition;
    }

    visiting.add(assetId);

    const asset = assetsById.get(assetId);
    const upstreamName = asset?.upstreams?.[0];
    const hasSingleUpstream = (asset?.upstreams?.length ?? 0) === 1;
    const upstreamId = upstreamName ? byName.get(upstreamName) : undefined;

    if (hasSingleUpstream && upstreamId) {
      const upstreamIndex = pipeline.assets.findIndex(
        (candidate) => candidate.id === upstreamId
      );
      const upstreamPosition = resolveNodePosition(
        upstreamId,
        upstreamIndex >= 0 ? upstreamIndex : index,
        visiting
      );
      const upstreamSize = sizeByAssetId.get(upstreamId);

      if (upstreamSize) {
        const downstreamPosition: { x: number; y: number } = {
          x: upstreamPosition.x,
          y:
            upstreamPosition.y +
            upstreamSize.height +
            DOWNSTREAM_NODE_VERTICAL_GAP,
        };
        resolvedPositionsByAssetId.set(assetId, downstreamPosition);
        visiting.delete(assetId);
        return downstreamPosition;
      }
    }

    const fallbackPosition = defaultNodePosition(index);
    resolvedPositionsByAssetId.set(assetId, fallbackPosition);
    visiting.delete(assetId);
    return fallbackPosition;
  };

  const nodes: Node[] = pipeline.assets.map((asset, index) => {
    const inspect = inspectByAssetID?.[asset.id];
    const previewMode = getAssetViewMode(asset.meta);
    const isPreviewLoading = inspectLoadingByAssetID?.[asset.id] === true;
    const resolvedPosition = resolveNodePosition(asset.id, index);

    return {
      id: asset.id,
      type: "assetNode",
      position: resolvedPosition,
      data: {
        name: asset.name,
        assetType: asset.type,
        connection: asset.connection,
        parameters: asset.parameters,
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
          previewMode && inspect
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
      asset.type,
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
    const size = estimateNodeSize(
      asset.type,
      previewMode,
      inspect,
      isPreviewLoading
    );
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
  assetType: string,
  mode: AssetViewMode | null,
  inspect: AssetInspectResponse | undefined,
  isLoading: boolean
) {
  const isIngestrAsset = assetType.trim().toLowerCase() === "ingestr";

  if (!mode) {
    return isIngestrAsset
      ? { width: 320, height: 150 }
      : { width: 260, height: 126 };
  }

  if (mode === "chart") {
    return isIngestrAsset
      ? { width: 400, height: 304 }
      : { width: 380, height: 280 };
  }

  if (isLoading) {
    if (mode === "table") {
      return isIngestrAsset
        ? { width: 440, height: 260 }
        : { width: 420, height: 230 };
    }
    return isIngestrAsset
      ? { width: 440, height: 270 }
      : { width: 420, height: 240 };
  }

  if (!inspect) {
    return isIngestrAsset
      ? { width: 320, height: 150 }
      : { width: 260, height: 126 };
  }

  if (mode === "table") {
    const sampledRows = inspect.rows.slice(0, 8);
    const estimatedWidth = estimateTableWidth(inspect.columns, sampledRows);
    const rowCount = Math.min(8, inspect.rows.length);
    return {
      width: isIngestrAsset ? Math.max(340, estimatedWidth) : estimatedWidth,
      height: Math.min(
        420,
        Math.max(isIngestrAsset ? 210 : 170, 96 + rowCount * 28)
      ),
    };
  }

  const textLength = JSON.stringify(inspect.rows[0] ?? {}).length;
  const estimatedLines = Math.max(6, Math.min(22, Math.ceil(textLength / 56)));
  return {
    width: isIngestrAsset ? 440 : 420,
    height: Math.min(
      500,
      Math.max(isIngestrAsset ? 214 : 190, 90 + estimatedLines * 18)
    ),
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

    return sum + Math.max(128, widestText + 32);
  }, 0);

  return Math.min(760, Math.max(320, totalWidth + 8));
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
