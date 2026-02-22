"use client";

import { useVirtualizer } from "@tanstack/react-virtual";
import { useMemo, useRef } from "react";

type Props = {
  columns: string[];
  rows: Record<string, unknown>[];
  height?: number;
};

export function VirtualDataTable({ columns, rows, height = 260 }: Props) {
  const parentRef = useRef<HTMLDivElement>(null);

  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 36,
    overscan: 8,
  });

  const virtualRows = rowVirtualizer.getVirtualItems();

  const fallbackColumns = useMemo(() => {
    if (columns.length > 0) {
      return columns;
    }
    const firstRow = rows[0];
    return firstRow ? Object.keys(firstRow) : [];
  }, [columns, rows]);

  if (rows.length === 0) {
    return (
      <div className="rounded border bg-muted/20 p-3 text-xs opacity-70">
        No rows returned.
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded border bg-background">
      <div className="grid grid-flow-col auto-cols-fr border-b bg-muted/60 text-xs font-semibold">
        {fallbackColumns.map((column) => (
          <div className="truncate border-r px-2 py-2 last:border-r-0" key={column}>
            {column}
          </div>
        ))}
      </div>

      <div ref={parentRef} style={{ height }} className="overflow-auto">
        <div
          style={{ height: `${rowVirtualizer.getTotalSize()}px`, position: "relative" }}
        >
          {virtualRows.map((virtualRow) => {
            const row = rows[virtualRow.index] ?? {};
            return (
              <div
                key={virtualRow.key}
                className={`grid grid-flow-col auto-cols-fr border-b text-xs ${
                  virtualRow.index % 2 === 0 ? "bg-background" : "bg-muted/20"
                }`}
                style={{
                  position: "absolute",
                  top: 0,
                  left: 0,
                  width: "100%",
                  transform: `translateY(${virtualRow.start}px)`,
                }}
              >
                {fallbackColumns.map((column) => (
                  (() => {
                    const cell = formatCell(row[column]);
                    return (
                      <div
                        key={`${virtualRow.key}-${column}`}
                        className={`truncate border-r px-2 py-2 last:border-r-0 ${cell.className}`}
                        title={cell.value}
                      >
                        {cell.value}
                      </div>
                    );
                  })()
                ))}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function formatCell(value: unknown): { value: string; className: string } {
  if (value === null || value === undefined) {
    return { value: "null", className: "text-muted-foreground italic" };
  }

  if (typeof value === "number") {
    return { value: String(value), className: "text-primary font-medium" };
  }

  if (typeof value === "boolean") {
    return {
      value: value ? "true" : "false",
      className: value ? "text-primary" : "text-destructive",
    };
  }

  if (value instanceof Date) {
    return { value: value.toISOString(), className: "text-foreground" };
  }

  if (typeof value === "object") {
    return {
      value: JSON.stringify(value),
      className: "text-muted-foreground",
    };
  }

  return { value: String(value), className: "text-foreground" };
}