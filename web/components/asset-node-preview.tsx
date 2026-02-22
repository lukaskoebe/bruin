import ReactMarkdown from "react-markdown";
import { Loader2 } from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  XAxis,
  YAxis,
} from "recharts";

import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";
import { AssetViewMode, LineChartSpec } from "@/lib/asset-visualization";

interface AssetNodePreviewProps {
  previewMode: AssetViewMode | null;
  chartType: string;
  chart: LineChartSpec | null;
  markdown: string;
  previewColumns: string[];
  previewRows: Record<string, unknown>[];
  previewError?: string;
  isPreviewLoading: boolean;
}

export function AssetNodePreview({
  previewMode,
  chartType,
  chart,
  markdown,
  previewColumns,
  previewRows,
  previewError,
  isPreviewLoading,
}: AssetNodePreviewProps) {
  if (previewError) {
    return (
      <div className="mt-2 rounded border border-destructive/40 bg-destructive/10 p-2 text-[11px] text-destructive">
        {previewError}
      </div>
    );
  }

  if (isPreviewLoading) {
    return <PreviewLoading />;
  }

  return (
    <div className="mt-2">
      {previewMode === "chart" && chart && (
        <ChartPreview chart={chart} chartType={chartType} />
      )}
      {previewMode === "table" && previewRows.length > 0 && (
        <TablePreview columns={previewColumns} rows={previewRows} />
      )}
      {previewMode === "markdown" && markdown && (
        <MarkdownPreview markdown={markdown} />
      )}
    </div>
  );
}

interface AssetNodeMeasurementProps {
  previewMode: AssetViewMode | null;
  previewColumns: string[];
  previewRows: Record<string, unknown>[];
  markdown: string;
  measurementRef: React.RefObject<HTMLDivElement | null>;
}

export function AssetNodeMeasurement({
  previewMode,
  previewColumns,
  previewRows,
  markdown,
  measurementRef,
}: AssetNodeMeasurementProps) {
  if (previewMode !== "table" && previewMode !== "markdown") {
    return null;
  }

  return (
    <div
      className="pointer-events-none fixed left-0 top-0 -z-10 opacity-0"
      ref={measurementRef}
    >
      {previewMode === "table" ? (
        <div className="rounded border bg-background">
          <table className="w-full border-collapse text-[11px]">
            <thead>
              <tr>
                {previewColumns.map((column) => (
                  <th
                    className="border-b px-2 py-1.5 text-left font-medium"
                    key={column}
                  >
                    {column}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {previewRows.slice(0, 8).map((row, rowIndex) => (
                <tr key={rowIndex}>
                  {previewColumns.map((column) => (
                    <td
                      className="border-b px-2 py-1 align-top"
                      key={`${rowIndex}-${column}`}
                    >
                      {stringifyCellValue(row[column])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="rounded border bg-background p-2">
          <article className="max-w-none text-xs leading-5 text-foreground">
            <ReactMarkdown>{markdown}</ReactMarkdown>
          </article>
        </div>
      )}
    </div>
  );
}

function PreviewLoading() {
  return (
    <div className="mt-2 rounded border bg-background p-2">
      <div className="mb-2 flex items-center gap-2 text-xs text-muted-foreground">
        <Loader2 className="size-3.5 animate-spin" />
        Loading preview…
      </div>
      <div className="space-y-2 animate-pulse">
        <div className="h-3 w-2/3 rounded bg-muted" />
        <div className="h-3 w-full rounded bg-muted" />
        <div className="h-3 w-5/6 rounded bg-muted" />
        <div className="h-16 w-full rounded bg-muted/80" />
      </div>
    </div>
  );
}

function ChartPreview({
  chart,
  chartType,
}: {
  chart: LineChartSpec;
  chartType: string;
}) {
  return (
    <div className="h-54 rounded border bg-background p-2">
      <ChartContainer className="h-full min-h-50 w-full" config={chart.config}>
        {chartType === "bar" ? (
          <BarChart accessibilityLayer data={chart.data}>
            <CartesianGrid vertical={false} />
            <XAxis
              axisLine={false}
              dataKey={chart.xKey}
              tickLine={false}
              tickMargin={8}
            />
            <YAxis axisLine={false} tickLine={false} tickMargin={8} />
            <ChartTooltip
              content={<ChartTooltipContent hideLabel />}
              cursor={false}
            />
            <ChartLegend content={<ChartLegendContent />} />
            {chart.series.map((series) => (
              <Bar
                key={series}
                dataKey={series}
                fill={`var(--color-${series})`}
                radius={6}
              />
            ))}
          </BarChart>
        ) : (
          <LineChart accessibilityLayer data={chart.data}>
            <CartesianGrid vertical={false} />
            <XAxis
              axisLine={false}
              dataKey={chart.xKey}
              tickLine={false}
              tickMargin={8}
            />
            <YAxis axisLine={false} tickLine={false} tickMargin={8} />
            <ChartTooltip content={<ChartTooltipContent />} />
            <ChartLegend content={<ChartLegendContent />} />
            {chart.series.map((series) => (
              <Line
                key={series}
                dataKey={series}
                dot={false}
                stroke={`var(--color-${series})`}
                strokeWidth={2}
                type="monotone"
              />
            ))}
          </LineChart>
        )}
      </ChartContainer>
    </div>
  );
}

function TablePreview({
  columns,
  rows,
}: {
  columns: string[];
  rows: Record<string, unknown>[];
}) {
  return (
    <div className="max-h-56 overflow-auto rounded border bg-background">
      <table className="w-full border-collapse text-[11px]">
        <thead className="sticky top-0 bg-muted/70">
          <tr>
            {columns.map((column) => (
              <th
                className="border-b px-2 py-1.5 text-left font-medium"
                key={column}
              >
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 8).map((row, rowIndex) => (
            <tr className="odd:bg-muted/20" key={rowIndex}>
              {columns.map((column) => (
                <td
                  className="border-b px-2 py-1 align-top"
                  key={`${rowIndex}-${column}`}
                >
                  {stringifyCellValue(row[column])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MarkdownPreview({ markdown }: { markdown: string }) {
  return (
    <div className="max-h-56 overflow-auto rounded border bg-background p-2">
      <article className="max-w-none text-xs leading-5 text-foreground">
        <ReactMarkdown
          components={{
            h1: ({ children }) => (
              <h1 className="mb-2 mt-1 text-lg font-bold">{children}</h1>
            ),
            h2: ({ children }) => (
              <h2 className="mb-2 mt-2 text-base font-semibold">{children}</h2>
            ),
            p: ({ children }) => <p className="mb-1.5">{children}</p>,
            ul: ({ children }) => (
              <ul className="mb-1.5 list-disc pl-4">{children}</ul>
            ),
            li: ({ children }) => <li className="mb-0.5">{children}</li>,
          }}
        >
          {markdown}
        </ReactMarkdown>
      </article>
    </div>
  );
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
