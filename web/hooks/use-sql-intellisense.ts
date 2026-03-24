"use client";

import { useEffect, useRef } from "react";
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
  tables: SchemaTable[],
  upstreamNames: string[],
  environment?: string,
  onGoToAsset?: (pipelineId: string, assetId: string) => void,
) {
  const workspace = useAtomValue(workspaceAtom);
  // Keep a stable ref to the latest callback so we don't re-register on
  // every render when the parent re-creates the function.
  const goToAssetRef = useRef(onGoToAsset);
  goToAssetRef.current = onGoToAsset;

  useEffect(() => {
    if (!monaco) {
      return;
    }

    const connectionName =
      asset && workspace ? resolveConnection(asset, workspace.connections ?? {}) : null;

    const disposable = registerSQLProviders(monaco, tables, upstreamNames, {
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
  }, [asset, environment, monaco, tables, upstreamNames, workspace]);

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
}
