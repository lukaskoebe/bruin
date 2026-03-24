"use client";

import { CSSProperties } from "react";
import { Eye, Hammer, Trash2 } from "lucide-react";

import { Button } from "@/components/ui/button";

type AssetEditorHeaderProps = {
  assetPath?: string | null;
  helpMode: boolean;
  actionHighlighted: boolean;
  highlightStyle?: CSSProperties;
  hasAsset: boolean;
  materializeLoading: boolean;
  inspectLoading: boolean;
  deleteLoading: boolean;
  onMaterialize: () => void;
  onInspect: () => void;
  onDelete: () => void;
};

export function AssetEditorHeader({
  assetPath,
  helpMode,
  actionHighlighted,
  highlightStyle,
  hasAsset,
  materializeLoading,
  inspectLoading,
  deleteLoading,
  onMaterialize,
  onInspect,
  onDelete,
}: AssetEditorHeaderProps) {
  return (
    <div className="border-b px-4 py-3">
      <div className="mb-2 text-sm font-semibold">Asset Editor</div>
      <div className="text-xs opacity-70">{assetPath ?? "No asset selected"}</div>
      <div
        className={`mt-2 flex flex-wrap gap-2 ${
          helpMode && actionHighlighted
            ? "rounded-md ring-2 ring-primary/70 ring-offset-2"
            : ""
        }`}
        style={helpMode && actionHighlighted ? highlightStyle : undefined}
      >
        <Button
          size="sm"
          variant="outline"
          disabled={!hasAsset || materializeLoading}
          onClick={onMaterialize}
          type="button"
        >
          <Hammer className="mr-1 inline size-3" />
          {materializeLoading ? "Running..." : "Materialize"}
        </Button>
        <Button
          size="sm"
          variant="outline"
          disabled={!hasAsset || inspectLoading}
          onClick={onInspect}
          type="button"
        >
          <Eye className="mr-1 inline size-3" />
          {inspectLoading ? "Loading..." : "Inspect Data"}
        </Button>
        <Button
          size="sm"
          variant="destructive"
          disabled={!hasAsset || deleteLoading}
          onClick={onDelete}
          type="button"
        >
          <Trash2 className="mr-1 inline size-3" />
          Delete Asset
        </Button>
      </div>
    </div>
  );
}
