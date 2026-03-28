"use client";

import { useEffect, useMemo, useRef } from "react";
import type * as MonacoNS from "monaco-editor";

import {
  getSQLDatabases,
  getSQLPathSuggestions,
  getSQLTableColumns,
  getSQLTables,
} from "@/lib/api";
import {
  registerSQLProviders,
  resolveTableAtPosition,
} from "@/lib/monaco-sql-providers";
import { resolveConnection, SchemaTable } from "@/lib/sql-schema";
import { WebAsset } from "@/lib/types";
import { useAtomValue } from "jotai";

import { workspaceAtom } from "@/lib/atoms/domains/workspace";
import { useSQLParseContext } from "@/hooks/use-sql-parse-context";
import { useSQLSemanticDecorations } from "@/hooks/use-sql-semantic-decorations";
import { fetchJSON } from "@/lib/api-core";

function buildValueSuggestionQuery(
  assetType: string | undefined,
  quotedTable: string,
  quotedColumn: string,
  trimmedPrefix: string,
) {
  const escapedPrefix = trimmedPrefix.replaceAll("'", "''");
  const normalizedAssetType = assetType?.toLowerCase() ?? "";

  switch (normalizedAssetType) {
    case "duckdb.sql":
    case "pg.sql":
    case "rs.sql":
    case "bq.sql":
    case "sf.sql":
    case "athena.sql":
    case "databricks.sql":
    case "ms.sql":
    case "synapse.sql":
    case "my.sql":
    default:
      return trimmedPrefix
        ? `select distinct ${quotedColumn} as value from ${quotedTable} where lower(cast(${quotedColumn} as varchar)) like lower('%${escapedPrefix}%') order by 1 limit 10`
        : `select distinct ${quotedColumn} as value from ${quotedTable} order by 1 limit 10`;
  }
}

function quoteSQLIdentifier(identifier: string) {
  return identifier
    .split(".")
    .map(
      (part) =>
        `"${part
          .trim()
          .replace(/^[\[\]"'`]+|[\[\]"'`]+$/g, "")
          .replaceAll('"', '""')}"`,
    )
    .join(".");
}

/**
 * React hook that registers Monaco SQL completion / definition / hover
 * providers scoped to the given schema tables.
 *
 * Providers are re-registered whenever the `tables` reference changes.
 * Call this once from the component that owns the Monaco editor.
 */
export function useSQLIntellisense(
  monaco: typeof MonacoNS | null,
  editor: MonacoNS.editor.IStandaloneCodeEditor | null,
  asset: WebAsset | null,
  sqlContent: string,
  tables: SchemaTable[],
  upstreamNames: string[],
  environment?: string,
  onGoToAsset?: (pipelineId: string, assetId: string) => void,
) {
  const workspace = useAtomValue(workspaceAtom);
  const parseContext = useSQLParseContext(asset, sqlContent, tables);
  const parseContextKey = useMemo(() => JSON.stringify(parseContext ?? null), [parseContext]);
  useSQLSemanticDecorations(editor, parseContext);
  const lastGoodParseContextRef = useRef<typeof parseContext>(null);
  if (parseContext && (!parseContext.errors || parseContext.errors.length === 0)) {
    lastGoodParseContextRef.current = parseContext;
  }
  // Keep a stable ref to the latest callback so we don't re-register on
  // every render when the parent re-creates the function.
  const goToAssetRef = useRef(onGoToAsset);
  goToAssetRef.current = onGoToAsset;
  const activeParseContext =
    parseContext && (!parseContext.errors || parseContext.errors.length === 0)
      ? parseContext
      : lastGoodParseContextRef.current;

  useEffect(() => {
    if (!monaco) {
      return;
    }

    const connectionName =
      asset && workspace ? resolveConnection(asset, workspace.connections ?? {}) : null;

    const disposable = registerSQLProviders(monaco, tables, upstreamNames, {
      getParseContext: () => {
        if (parseContext && (!parseContext.errors || parseContext.errors.length === 0)) {
          return parseContext;
        }

        return lastGoodParseContextRef.current;
      },
      async provideTableContextSuggestions({ monaco: monacoInstance, prefix, range }) {
        if (!connectionName) {
          return [];
        }

        let databasesResponse;
        try {
          databasesResponse = await getSQLDatabases({
            connection: connectionName,
            environment,
          });
        } catch {
          return [];
        }

        const databaseNames = databasesResponse.databases ?? [];
        const tableResponses = await Promise.all(
          databaseNames.map(async (databaseName) => {
            try {
              return await getSQLTables({
                connection: connectionName,
                database: databaseName,
                environment,
              });
            } catch {
              return null;
            }
          })
        );

        const normalizedPrefix = prefix.trim().toLowerCase();

        return tableResponses
          .flatMap((response) => response?.tables ?? [])
          .filter((table) => {
            if (!normalizedPrefix) {
              return true;
            }

            return (
              table.name.toLowerCase().includes(normalizedPrefix) ||
              table.short_name.toLowerCase().includes(normalizedPrefix)
            );
          })
          .map((table) => ({
            label: {
              label: table.name,
              description: "Remote table",
            },
            kind: monacoInstance.languages.CompletionItemKind.Struct,
            detail: `Remote table: ${table.name}`,
            insertText: table.name,
            range,
            sortText: "3",
          }));
      },
      async provideColumnSuggestions({
        monaco: monacoInstance,
        tableIdentifier,
        columnPrefix,
        range,
      }) {
        if (!connectionName) {
          return [];
        }

        const normalizedIdentifier = tableIdentifier.trim().toLowerCase();
        const localTable = tables.find(
          (table) =>
            table.name.toLowerCase() === normalizedIdentifier ||
            table.shortName.toLowerCase() === normalizedIdentifier
        );

        const remoteTableName = localTable?.name ?? tableIdentifier;

        let response;
        try {
          response = await getSQLTableColumns({
            connection: connectionName,
            table: remoteTableName,
            environment,
          });
        } catch {
          return [];
        }

        const normalizedPrefix = columnPrefix.trim().toLowerCase();

        return (response.columns ?? [])
          .filter((column) => {
            if (!normalizedPrefix) {
              return true;
            }

            return column.name.toLowerCase().includes(normalizedPrefix);
          })
          .map((column) => ({
            label: column.name,
            kind: monacoInstance.languages.CompletionItemKind.Field,
            detail: column.type
              ? `${remoteTableName}.${column.name} (${column.type})`
              : `${remoteTableName}.${column.name}`,
            documentation: column.description || undefined,
            insertText: column.name,
            range,
            sortText: column.primary_key ? "0" : "1",
          }));
      },
      async provideColumnValueSuggestions({
        monaco: monacoInstance,
        tableIdentifier,
        columnName,
        prefix,
        range,
        insideQuotes,
      }) {
        if (!connectionName || !activeParseContext) {
          return [];
        }

        const matchingColumn = (activeParseContext.columns ?? []).find((column) => {
          const columnPart = column.parts.findLast((part) => part.kind === "column");
          return (
            column.qualifier?.toLowerCase() === tableIdentifier.toLowerCase() &&
            columnPart?.name.toLowerCase() === columnName.toLowerCase() &&
            column.resolved_table
          );
        });

        const resolvedTable = matchingColumn?.resolved_table;
        if (!resolvedTable) {
          return [];
        }

        const trimmedPrefix = prefix.trim();
        const quotedTable = quoteSQLIdentifier(resolvedTable);
        const quotedColumn = quoteSQLIdentifier(columnName);
        const valueQuery = buildValueSuggestionQuery(
          asset?.type,
          quotedTable,
          quotedColumn,
          trimmedPrefix,
        );

        try {
          const payload = await fetchJSON<{
            values?: Array<string | number | boolean | null>;
          }>(`/api/sql/column-values`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            cache: "no-store",
            body: JSON.stringify({
              connection: connectionName,
              environment: environment ?? "",
              query: valueQuery,
            }),
          });

          return (payload.values ?? []).map((value, index) => ({
            label: String(value ?? "NULL"),
            kind: monacoInstance.languages.CompletionItemKind.Value,
            detail: `${resolvedTable}.${columnName}`,
            insertText:
              typeof value === "string"
                ? insideQuotes
                  ? String(value).replaceAll("'", "''")
                  : `'${String(value).replaceAll("'", "''")}'`
                : String(value ?? "NULL"),
            range,
            sortText: `0${index}`,
          }));
        } catch {
          return [];
        }
      },
      async providePathSuggestions({ monaco: monacoInstance, prefix, range }) {
        if (!asset?.id) {
          return [];
        }

        let response;
        try {
          response = await getSQLPathSuggestions({
            assetId: asset.id,
            prefix,
            environment,
          });
        } catch {
          return [];
        }

        return response.suggestions.map((suggestion) => ({
          label: suggestion.value,
          kind: suggestion.kind === "directory"
            ? monacoInstance.languages.CompletionItemKind.Folder
            : monacoInstance.languages.CompletionItemKind.File,
          detail: suggestion.detail,
          insertText: suggestion.value,
          range,
          sortText: suggestion.kind === "directory" ? "0" : "1",
        }));
      },
    });

    return () => {
      disposable.dispose();
    };
  }, [activeParseContext, asset, editor, environment, monaco, parseContextKey, tables, upstreamNames, workspace]);

  useEffect(() => {
    if (!editor || tables.length === 0) {
      return;
    }

    const disposable = editor.onMouseDown((event) => {
      if (!event.event.leftButton) {
        return;
      }

      if (!event.event.ctrlKey && !event.event.metaKey) {
        return;
      }

      const position = event.target.position;
      if (!position) {
        return;
      }

      const model = editor.getModel();
      if (!model) {
        return;
      }

      const table = resolveTableAtPosition(model, position, tables);
      if (!table?.assetId || !table.pipelineId) {
        return;
      }

      event.event.preventDefault();
      event.event.stopPropagation();
      goToAssetRef.current?.(table.pipelineId, table.assetId);
    });

    return () => {
      disposable.dispose();
    };
  }, [editor, tables]);

  useEffect(() => {
    if (!editor || !monaco) {
      return;
    }

    const model = editor.getModel();
    if (!model) {
      return;
    }

    const diagnostics = (parseContext?.diagnostics ?? [])
      .filter((diagnostic) => diagnostic.range)
      .map((diagnostic) => ({
        severity:
          diagnostic.severity === "warning"
            ? monaco.MarkerSeverity.Warning
            : diagnostic.severity === "info"
              ? monaco.MarkerSeverity.Info
              : monaco.MarkerSeverity.Error,
        message: diagnostic.message,
        startLineNumber: diagnostic.range!.line,
        startColumn: diagnostic.range!.col,
        endLineNumber: diagnostic.range!.end_line,
        endColumn: diagnostic.range!.end_col,
      }));

    monaco.editor.setModelMarkers(model, "bruin-sql-parse-context", diagnostics);

    return () => {
      monaco.editor.setModelMarkers(model, "bruin-sql-parse-context", []);
    };
  }, [editor, monaco, parseContextKey]);
}
