import { ChartConfig } from "@/components/ui/chart";

export type AssetViewMode = "table" | "chart" | "markdown";

export type LineChartSpec = {
  xKey: string;
  series: string[];
  data: Record<string, unknown>[];
  config: ChartConfig;
};

export function getAssetViewMode(
  meta?: Record<string, string>
): AssetViewMode | null {
  const value = (meta?.web_view ?? "").trim().toLowerCase();
  if (value === "chart" || value === "markdown" || value === "table") {
    return value;
  }
  return null;
}

export function buildMarkdown(
  meta: Record<string, string> | undefined,
  rows: Record<string, unknown>[]
): string {
  const template = (meta?.web_markdown_template ?? "").trim();
  if (template.length > 0) {
    return interpolateMarkdownTemplate(template, rows);
  }

  return getMarkdownValue(rows, meta?.web_markdown_column);
}

export function buildLineChartSpec(
  rows: Record<string, unknown>[],
  meta?: Record<string, string>
): LineChartSpec | null {
  if (rows.length === 0) {
    return null;
  }

  const firstRow = rows[0];
  const availableKeys = Object.keys(firstRow);
  if (availableKeys.length < 2) {
    return null;
  }

  const xKey = pickXKey(meta, availableKeys);
  const series = pickSeriesKeys(meta, availableKeys, xKey);
  if (series.length === 0) {
    return null;
  }

  const config = series.reduce<ChartConfig>((acc, key, index) => {
    acc[key] = {
      label: key,
      color: `var(--chart-${(index % 5) + 1})`,
    };
    return acc;
  }, {});

  return {
    xKey,
    series,
    data: rows,
    config,
  };
}

function interpolateMarkdownTemplate(
  template: string,
  rows: Record<string, unknown>[]
): string {
  const firstRow = rows[0] ?? {};

  return template.replace(/\{\{\s*([^}]+)\s*\}\}/g, (_match, rawExpression) => {
    const expression = String(rawExpression).trim();

    if (expression === "rows.length") {
      return String(rows.length);
    }

    const rowIndexed = expression.match(/^row\[(\d+)\]\.(.+)$/);
    if (rowIndexed) {
      const rowIndex = Number(rowIndexed[1]);
      const key = rowIndexed[2];
      return stringifyTemplateValue(rows[rowIndex]?.[key]);
    }

    const rowPrefixed = expression.match(/^row(\d+)\.(.+)$/);
    if (rowPrefixed) {
      const rowIndex = Number(rowPrefixed[1]);
      const key = rowPrefixed[2];
      return stringifyTemplateValue(rows[rowIndex]?.[key]);
    }

    if (expression.startsWith("first.")) {
      const key = expression.slice("first.".length);
      return stringifyTemplateValue(firstRow[key]);
    }

    return stringifyTemplateValue(firstRow[expression]);
  });
}

function stringifyTemplateValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value === "string") {
    let normalized = value
      .replace(/\\r\\n/g, "\n")
      .replace(/\\n/g, "\n")
      .replace(/\\t/g, "\t");

    const trimmed = normalized.trim();
    if (
      trimmed.includes("\n- ") &&
      !trimmed.startsWith("- ") &&
      !trimmed.startsWith("* ")
    ) {
      normalized = `- ${trimmed}`;
    }

    return normalized;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function getMarkdownValue(
  rows: Record<string, unknown>[],
  markdownColumn?: string
): string {
  if (rows.length === 0) {
    return "";
  }

  const preferred = (markdownColumn ?? "").trim();
  const firstRow = rows[0];

  if (preferred && typeof firstRow[preferred] === "string") {
    return firstRow[preferred] as string;
  }

  for (const key of ["markdown", "md", "content", "text"]) {
    const value = firstRow[key];
    if (typeof value === "string") {
      return value;
    }
  }

  for (const value of Object.values(firstRow)) {
    if (typeof value === "string") {
      return value;
    }
  }

  return "";
}

function pickXKey(
  meta: Record<string, string> | undefined,
  keys: string[]
): string {
  const configured = (meta?.web_chart_x ?? "").trim();
  if (configured && keys.includes(configured)) {
    return configured;
  }

  const candidate = keys.find((key) => {
    const lower = key.toLowerCase();
    return (
      lower.includes("date") ||
      lower.includes("time") ||
      lower.includes("month") ||
      lower.includes("day") ||
      lower.includes("x")
    );
  });

  return candidate ?? keys[0];
}

function pickSeriesKeys(
  meta: Record<string, string> | undefined,
  keys: string[],
  xKey: string
): string[] {
  const configured = (meta?.web_chart_series ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .filter((key) => key !== xKey && keys.includes(key));

  if (configured.length > 0) {
    return configured;
  }

  return keys.filter((key) => key !== xKey).slice(0, 3);
}
