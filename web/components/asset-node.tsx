"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { Handle, NodeProps, Position } from "reactflow";

import {
  AssetNodeMeasurement,
  AssetNodePreview,
} from "@/components/asset-node-preview";
import { AssetTypeIcon } from "@/components/asset-type-icon";
import { AssetNodeData } from "@/lib/graph";
import {
  buildLineChartSpec,
  buildMarkdown,
  getAssetViewMode,
} from "@/lib/asset-visualization";

export function AssetNode({ data, selected }: NodeProps<AssetNodeData>) {
  const materializationType = normalizeMaterialization(data.materializedAs);

  const previewMode = getAssetViewMode(data.meta);
  const chartType = (data.meta?.web_chart_type ?? "line").trim().toLowerCase();
  const previewRows = useMemo(
    () => data.preview?.rows ?? [],
    [data.preview?.rows]
  );
  const previewColumns = useMemo(
    () => data.preview?.columns ?? [],
    [data.preview?.columns]
  );
  const previewError = data.preview?.error;
  const isPreviewLoading = Boolean(previewMode && data.previewLoading);
  const chart = useMemo(
    () => buildLineChartSpec(previewRows, data.meta),
    [data.meta, previewRows]
  );
  const markdown = useMemo(
    () => buildMarkdown(data.meta, previewRows),
    [data.meta, previewRows]
  );
  const measurementRef = useRef<HTMLDivElement | null>(null);
  const [measuredSize, setMeasuredSize] = useState<{
    width: number;
    height: number;
  } | null>(null);
  const { prefix, leaf } = useMemo(
    () => splitAssetName(data.name),
    [data.name]
  );

  useEffect(() => {
    if (previewMode !== "table" && previewMode !== "markdown") {
      return;
    }

    const element = measurementRef.current;
    if (!element) {
      return;
    }

    const nextWidth = Math.ceil(element.scrollWidth);
    const nextHeight = Math.ceil(element.scrollHeight);
    if (nextWidth === 0 || nextHeight === 0) {
      return;
    }

    setMeasuredSize({
      width: Math.min(760, Math.max(260, nextWidth + 24)),
      height: Math.min(500, Math.max(126, nextHeight + 24)),
    });
  }, [chart, markdown, previewColumns, previewMode, previewRows]);

  const rowCountClass =
    data.rowCount === undefined || data.rowCount === null
      ? "text-muted-foreground"
      : data.rowCount === 0
        ? "text-destructive"
        : "text-chart-2";

  return (
    <>
      <Handle type="target" position={Position.Top} />
      <div
        className={`relative min-w-56 rounded-lg border-2 bg-card p-3 shadow-sm transition-colors ${
          selected
            ? "border-primary ring-2 ring-primary/30"
            : "border-border/90"
        }`}
        style={
          previewMode === "chart"
            ? {
                width: 380,
                minHeight: 280,
              }
            : (previewMode === "table" || previewMode === "markdown") &&
                measuredSize
              ? {
                  width: measuredSize.width,
                  minHeight: measuredSize.height,
                }
              : undefined
        }
      >
        {data.isMaterialized && (
          <span
            className={`absolute top-2 right-2 size-2.5 rounded-full ${
              data.freshnessStatus === "stale"
                ? "bg-muted-foreground"
                : "bg-emerald-500"
            }`}
          />
        )}

        <div className="flex items-center gap-1.5 pr-4">
          <AssetTypeIcon
            assetType={data.assetType}
            className="shrink-0 text-muted-foreground px-2"
            connection={data.connection}
            meta={data.meta}
          />
          <div className="min-w-0 flex-1 overflow-hidden">
            <div
              className="truncate text-sm font-semibold leading-tight"
              title={data.name}
            >
              {leaf}
            </div>
            {prefix && (
              <div
                className="truncate text-[10px] leading-tight text-muted-foreground/90"
                title={prefix}
              >
                {prefix}
              </div>
            )}
          </div>
        </div>

        <div className="mt-2 text-xs">
          Materialization:{" "}
          <span className="font-medium">{materializationType}</span>
        </div>

        <div className={`text-xs ${rowCountClass}`}>
          Rows: {formatRowCount(data.rowCount)}
        </div>

        <AssetNodePreview
          canLoadMorePreviewRows={data.canLoadMorePreviewRows}
          chart={chart}
          chartType={chartType}
          isPreviewLoading={isPreviewLoading}
          markdown={markdown}
          onLoadMorePreviewRows={data.onLoadMorePreviewRows}
          previewColumns={previewColumns}
          previewError={previewError}
          previewMode={previewMode}
          previewRows={previewRows}
        />

        <AssetNodeMeasurement
          markdown={markdown || ""}
          measurementRef={measurementRef}
          previewColumns={previewColumns}
          previewMode={previewMode}
          previewRows={previewRows}
        />
      </div>
      <Handle type="source" position={Position.Bottom} />
    </>
  );
}

function formatRowCount(value?: number) {
  if (value === undefined || value === null) {
    return "unknown";
  }

  return value.toLocaleString();
}

function normalizeMaterialization(value?: string) {
  const raw = (value ?? "").toLowerCase();
  if (raw === "table") {
    return "Table";
  }
  if (raw === "view") {
    return "View";
  }
  return "Unset";
}

function splitAssetName(name: string) {
  const parts = name.split(".").filter(Boolean);
  if (parts.length <= 1) {
    return {
      prefix: "",
      leaf: name,
    };
  }

  return {
    prefix: parts.slice(0, -1).join("."),
    leaf: parts[parts.length - 1],
  };
}
