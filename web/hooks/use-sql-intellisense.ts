"use client";

import { useAtomValue, useSetAtom } from "jotai";
import { useEffect, useRef } from "react";
import type * as MonacoNS from "monaco-editor";

import {
  getSQLDatabases,
  getSQLTableColumns,
  getSQLTables,
} from "@/lib/api";
import {
  registerSQLProviders,
  resolveTableAtPosition,
} from "@/lib/monaco-sql-providers";
import {
  getDatabaseSuggestions,
  registerConnectionDatabasesAtom,
  registerConnectionTablesAtom,
  registerRemoteTableColumnsAtom,
  selectedEnvironmentAtom,
  suggestionCatalogAtom,
  workspaceAtom,
} from "@/lib/atoms";
import {
  parseQualifiedTableName,
  resolveConnection,
  SchemaTable,
} from "@/lib/sql-schema";
import { WebAsset } from "@/lib/types";

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
  onGoToAsset?: (pipelineId: string, assetId: string) => void,
) {
  const workspace = useAtomValue(workspaceAtom);
  const catalog = useAtomValue(suggestionCatalogAtom);
  const selectedEnvironment = useAtomValue(selectedEnvironmentAtom);
  const registerConnectionDatabases = useSetAtom(registerConnectionDatabasesAtom);
  const registerConnectionTables = useSetAtom(registerConnectionTablesAtom);
  const registerRemoteTableColumns = useSetAtom(registerRemoteTableColumnsAtom);

  // Keep a stable ref to the latest callback so we don't re-register on
  // every render when the parent re-creates the function.
  const goToAssetRef = useRef(onGoToAsset);
  goToAssetRef.current = onGoToAsset;

  const databaseRequestCacheRef = useRef(new Map<string, Promise<string[]>>());
  const tableRequestCacheRef = useRef(new Map<string, Promise<SchemaTable[]>>());
  const columnRequestCacheRef = useRef(new Map<string, Promise<SchemaTable | null>>());

  useEffect(() => {
    if (!monaco || tables.length === 0) {
      return;
    }

    const currentConnection =
      asset && workspace ? resolveConnection(asset, workspace.connections ?? {}) : null;
    const currentConnectionType = currentConnection
      ? workspace?.connections?.[currentConnection]
      : undefined;

    const getCachedRemoteTables = (databaseName: string) =>
      catalog.tables
        .filter(
          (table) =>
            !table.isBruinAsset &&
            table.connectionName === currentConnection &&
            (table.databaseName ?? "") === databaseName &&
            table.sources.some(
              (source) =>
                source.method === "connection-table-discovery" &&
                (!selectedEnvironment || source.environment === selectedEnvironment)
            )
        )
        .map((table) => ({
          name: table.name,
          shortName: table.shortName,
          columns: table.columns.map((column) => ({
            name: column.name,
            type: column.type,
            description: column.description,
            primaryKey: column.primaryKey,
          })),
          isBruinAsset: false,
          connectionName: table.connectionName ?? undefined,
          connectionType: table.connectionType ?? undefined,
          databaseName: table.databaseName ?? undefined,
          sourceMethods: table.sourceMethods,
        }));

    const getAllCachedRemoteTables = () =>
      catalog.tables
        .filter(
          (table) =>
            !table.isBruinAsset &&
            table.connectionName === currentConnection &&
            table.sources.some(
              (source) =>
                source.method === "connection-table-discovery" &&
                (!selectedEnvironment || source.environment === selectedEnvironment)
            )
        )
        .map((table) => ({
          name: table.name,
          shortName: table.shortName,
          columns: table.columns.map((column) => ({
            name: column.name,
            type: column.type,
            description: column.description,
            primaryKey: column.primaryKey,
          })),
          isBruinAsset: false,
          connectionName: table.connectionName ?? undefined,
          connectionType: table.connectionType ?? undefined,
          databaseName: table.databaseName ?? undefined,
          sourceMethods: table.sourceMethods,
        }));

    const loadRemoteDatabases = async () => {
      const databaseCacheKey = [
        currentConnection,
        selectedEnvironment ?? "",
      ].join("::");
      const existing = databaseRequestCacheRef.current.get(databaseCacheKey);
      const cached = getDatabaseSuggestions(catalog, {
        connectionName: currentConnection ?? "",
        environment: selectedEnvironment,
      });

      let pending = existing;
      if (!pending && cached.length === 0 && currentConnection) {
        pending = getSQLDatabases({
          connection: currentConnection,
          environment: selectedEnvironment,
        })
          .then((response) => {
            registerConnectionDatabases({
              connectionName: currentConnection,
              connectionType: response.connection_type ?? currentConnectionType,
              environment: selectedEnvironment,
              databases: response.databases,
            });

            return response.databases;
          })
          .catch(() => []);
        databaseRequestCacheRef.current.set(databaseCacheKey, pending);
      }

      return cached.length > 0 ? cached : await (pending ?? Promise.resolve([]));
    };

    const loadRemoteTablesForDatabase = async (databaseName: string) => {
      const cachedTables = getCachedRemoteTables(databaseName);
      const tableCacheKey = [
        currentConnection,
        databaseName,
        selectedEnvironment ?? "",
      ].join("::");
      const existing = tableRequestCacheRef.current.get(tableCacheKey);

      let pending = existing;
      if (!pending && cachedTables.length === 0 && currentConnection) {
        pending = getSQLTables({
          connection: currentConnection,
          database: databaseName,
          environment: selectedEnvironment,
        })
          .then((response) => {
            const nextTables = response.tables.map((table) => ({
              name: table.name,
              shortName: table.short_name,
              columns: [],
              isBruinAsset: false,
              connectionName: currentConnection,
              connectionType: response.connection_type ?? currentConnectionType,
              databaseName: table.database_name,
              sourceMethods: ["connection-table-discovery"],
            } satisfies SchemaTable));

            registerConnectionTables({
              method: "connection-table-discovery",
              connectionName: currentConnection,
              connectionType: response.connection_type ?? currentConnectionType,
              databaseName,
              environment: selectedEnvironment,
              tables: response.tables.map((table) => ({
                name: table.name,
                schemaName: table.schema_name,
                databaseName: table.database_name,
              })),
            });

            return nextTables;
          })
          .catch(() => []);
        tableRequestCacheRef.current.set(tableCacheKey, pending);
      }

      return cachedTables.length > 0 ? cachedTables : await (pending ?? Promise.resolve([]));
    };

    const toTableCompletionItems = (
      range: MonacoNS.IRange,
      prefix: string,
      remoteTables: SchemaTable[]
    ): MonacoNS.languages.CompletionItem[] => {
      const normalizedPrefix = prefix.trim().toLowerCase();
      const schemaSuggestions = new Map<string, MonacoNS.languages.CompletionItem>();
      const tableSuggestions: MonacoNS.languages.CompletionItem[] = [];

      for (const table of remoteTables) {
        if (
          normalizedPrefix &&
          !table.name.toLowerCase().includes(normalizedPrefix)
        ) {
          const schemaLabel = table.databaseName && table.shortName !== table.name
            ? `${table.databaseName}.${parseQualifiedTableName(table.name).schemaName ?? ""}`
            : "";
          if (!schemaLabel || !schemaLabel.toLowerCase().includes(normalizedPrefix)) {
            continue;
          }
        }

        const parts = parseQualifiedTableName(table.name);
        if (table.databaseName && parts.schemaName) {
          const schemaLabel = `${table.databaseName}.${parts.schemaName}`;
          if (!schemaSuggestions.has(schemaLabel)) {
            schemaSuggestions.set(schemaLabel, {
              label: {
                label: schemaLabel,
                description: "Schema",
              },
              kind: monaco.languages.CompletionItemKind.Module,
              detail: `Schema on ${currentConnection}`,
              insertText: `${schemaLabel}.`,
              range,
              sortText: "2",
            });
          }
        }

        tableSuggestions.push({
          label: {
            label: table.name,
            description: "Remote Table",
          },
          kind: monaco.languages.CompletionItemKind.Struct,
          detail: `Remote table on ${currentConnection}`,
          documentation:
            table.columns.length > 0
              ? `Columns: ${table.columns.map((column) => column.name).join(", ")}`
              : undefined,
          insertText: table.name,
          range,
          sortText: "3",
        });
      }

      return [...schemaSuggestions.values(), ...tableSuggestions];
    };

    const disposable = registerSQLProviders(monaco, tables, upstreamNames, {
      provideTableContextSuggestions: async ({ range, prefix }) => {
        if (!currentConnection) {
          return [];
        }

        const normalizedPrefix = prefix.trim().replace(/"/g, "");
        const parts = normalizedPrefix.split(".").filter(Boolean);

        if (parts.length <= 1) {
          const databases = await loadRemoteDatabases();
          const databaseSuggestions = databases
            .filter((databaseName) =>
              !normalizedPrefix
                ? true
                : databaseName.toLowerCase().includes(normalizedPrefix.toLowerCase())
            )
            .map((databaseName) => ({
              label: {
                label: databaseName,
                description: "Database",
              },
              kind: monaco.languages.CompletionItemKind.Module,
              detail: `Database on ${currentConnection}`,
              insertText: `${databaseName}.`,
              range,
              sortText: "1",
            }));

          if (!normalizedPrefix) {
            return databaseSuggestions;
          }

          let remoteTables = getAllCachedRemoteTables().filter(
            (table) =>
              table.name.toLowerCase().includes(normalizedPrefix.toLowerCase()) ||
              table.shortName.toLowerCase().includes(normalizedPrefix.toLowerCase())
          );

          if (remoteTables.length === 0 && databases.length > 0) {
            const loadedTables = await Promise.all(
              databases.map((databaseName) => loadRemoteTablesForDatabase(databaseName))
            );
            remoteTables = loadedTables
              .flat()
              .filter(
                (table) =>
                  table.name.toLowerCase().includes(normalizedPrefix.toLowerCase()) ||
                  table.shortName.toLowerCase().includes(normalizedPrefix.toLowerCase())
              );
          }

          return [
            ...databaseSuggestions,
            ...toTableCompletionItems(range, normalizedPrefix, remoteTables),
          ];
        }

        const databaseName = parts[0];
        if (!databaseName) {
          return [];
        }

        const remoteTables = await loadRemoteTablesForDatabase(databaseName);

        return toTableCompletionItems(range, normalizedPrefix, remoteTables);
      },
      provideColumnSuggestions: async ({ tableIdentifier, columnPrefix, range }) => {
        if (!currentConnection) {
          return [];
        }

        const normalizedIdentifier = tableIdentifier.trim().replace(/"/g, "");
        if (!normalizedIdentifier || !normalizedIdentifier.includes(".")) {
          return [];
        }

        const existingTable = catalog.tables.find(
          (table) =>
            table.connectionName === currentConnection &&
            (table.name.toLowerCase() === normalizedIdentifier.toLowerCase() ||
              table.shortName.toLowerCase() === normalizedIdentifier.toLowerCase())
        );
        const qualifiedTableName = existingTable?.name ?? normalizedIdentifier;
        const tableParts = parseQualifiedTableName(qualifiedTableName);
        const cacheKey = [
          currentConnection,
          qualifiedTableName.toLowerCase(),
          selectedEnvironment ?? "",
        ].join("::");
        const existing = columnRequestCacheRef.current.get(cacheKey);

        let pending = existing;
        if (!pending && !(existingTable && existingTable.columns.length > 0)) {
          pending = getSQLTableColumns({
            connection: currentConnection,
            table: qualifiedTableName,
            environment: selectedEnvironment,
          })
            .then((response) => {
              registerConnectionTables({
                method: "connection-table-discovery",
                connectionName: currentConnection,
                connectionType: currentConnectionType,
                databaseName: tableParts.databaseName ?? existingTable?.databaseName,
                environment: selectedEnvironment,
                tables: [
                  {
                    name: qualifiedTableName,
                    schemaName: tableParts.schemaName,
                    databaseName:
                      tableParts.databaseName ?? existingTable?.databaseName,
                  },
                ],
              });
              registerRemoteTableColumns({
                connectionName: currentConnection,
                connectionType: currentConnectionType,
                databaseName: tableParts.databaseName ?? existingTable?.databaseName,
                tableName: qualifiedTableName,
                environment: selectedEnvironment,
                columns: (response.columns ?? []).map((column) => ({
                  name: column.name,
                  type: column.type,
                  description: column.description,
                  primaryKey: column.primary_key,
                })),
              });

              return {
                name: qualifiedTableName,
                shortName: tableParts.shortName,
                columns: (response.columns ?? []).map((column) => ({
                  name: column.name,
                  type: column.type,
                  description: column.description,
                  primaryKey: column.primary_key,
                })),
                isBruinAsset: false,
              };
            })
            .catch(() => null);
          columnRequestCacheRef.current.set(cacheKey, pending);
        }

        const resolvedTable =
          existingTable && existingTable.columns.length > 0
            ? {
                name: existingTable.name,
                shortName: existingTable.shortName,
                columns: existingTable.columns.map((column) => ({
                  name: column.name,
                  type: column.type,
                  description: column.description,
                  primaryKey: column.primaryKey,
                })),
                isBruinAsset: false,
              }
            : await (pending ?? Promise.resolve(null));

        return (resolvedTable?.columns ?? [])
          .filter((column) =>
            !columnPrefix
              ? true
              : column.name.toLowerCase().startsWith(columnPrefix.toLowerCase())
          )
          .map((column) => ({
            label: column.name,
            kind: monaco.languages.CompletionItemKind.Field,
            detail: `${qualifiedTableName}.${column.name}${
              column.type ? ` (${column.type})` : ""
            }`,
            documentation: column.description || undefined,
            insertText: column.name,
            range,
            sortText: column.primaryKey ? "0" : "1",
          }));
      },
    });

    return () => {
      disposable.dispose();
    };
  }, [
    asset,
    catalog,
    monaco,
    registerConnectionDatabases,
    registerConnectionTables,
    registerRemoteTableColumns,
    selectedEnvironment,
    tables,
    upstreamNames,
    workspace,
  ]);

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
