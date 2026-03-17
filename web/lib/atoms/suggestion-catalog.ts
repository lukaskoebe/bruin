import {
  parseQualifiedTableName,
  resolveConnection,
  SchemaColumn,
  SchemaTable,
} from "@/lib/sql-schema";
import { IngestrSuggestion, WebAsset, WebColumn, WorkspaceState } from "@/lib/types";

import {
  ConnectionSuggestionEntry,
  DynamicSuggestionState,
  SuggestionCatalogState,
  SuggestionColumnState,
  SuggestionConnectionState,
  SuggestionObservation,
  SuggestionObservationMethod,
  SuggestionTableState,
  SuggestionWorkspaceSyncSource,
} from "./suggestion-types";

const SOURCE_PRIORITY: Record<string, number> = {
  postgres: 0,
  duckdb: 1,
  s3: 2,
};

function normalizeTableName(value: string): string {
  return value.trim().replace(/^['"`]+|['"`]+$/g, "");
}

function toSchemaColumns(columns?: WebColumn[]): SchemaColumn[] {
  if (!columns || columns.length === 0) {
    return [];
  }

  return columns.map((column) => ({
    name: column.name,
    type: column.type,
    description: column.description,
    primaryKey: column.primary_key,
  }));
}

function connectionKey(name: string, databaseName?: string | null): string {
  return `${name.toLowerCase()}::${(databaseName ?? "").toLowerCase()}`;
}

function assetTableKey(assetId: string, pipelineId?: string): string {
  return `asset::${pipelineId ?? ""}::${assetId}`;
}

function externalTableKey(
  connectionName: string,
  tableName: string,
  databaseName?: string | null
): string {
  return `external::${connectionKey(
    connectionName,
    databaseName
  )}::${normalizeTableName(tableName).toLowerCase()}`;
}

function columnKey(tableKey: string, name: string): string {
  return `${tableKey}::${name.toLowerCase()}`;
}

function sourceSignature(source: SuggestionObservation): string {
  return [
    source.method,
    source.pipelineId ?? "",
    source.assetId ?? "",
    source.connectionName ?? "",
    source.connectionType ?? "",
    source.databaseName ?? "",
    source.environment ?? "",
    String(source.workspaceRevision ?? ""),
    source.workspaceEventType ?? "",
    source.workspaceEventPath ?? "",
    source.workspaceLite ? "1" : "0",
  ].join("|");
}

function addSource(
  existing: SuggestionObservation[],
  source: SuggestionObservation
): SuggestionObservation[] {
  const signature = sourceSignature(source);
  if (existing.some((item) => sourceSignature(item) === signature)) {
    return existing;
  }

  return [...existing, source];
}

function addSourceMethod(
  existing: SuggestionObservationMethod[],
  method: SuggestionObservationMethod
): SuggestionObservationMethod[] {
  return existing.includes(method) ? existing : [...existing, method];
}

function mergeColumnValue<T>(
  current: T | undefined,
  next: T | undefined
): T | undefined {
  return current === undefined || current === null || current === ("" as T)
    ? next
    : current;
}

function sortColumns(columns: SuggestionColumnState[]): SuggestionColumnState[] {
  return [...columns].sort((left, right) => left.name.localeCompare(right.name));
}

function sortTables(tables: SuggestionTableState[]): SuggestionTableState[] {
  return [...tables].sort((left, right) => left.name.localeCompare(right.name));
}

function sortConnections(
  connections: SuggestionConnectionState[]
): SuggestionConnectionState[] {
  return [...connections].sort((left, right) => {
    const leftPriority = SOURCE_PRIORITY[left.type] ?? 99;
    const rightPriority = SOURCE_PRIORITY[right.type] ?? 99;

    if (leftPriority !== rightPriority) {
      return leftPriority - rightPriority;
    }

    return left.name.localeCompare(right.name);
  });
}

function buildWorkspaceObservation(
  recordedAt: string,
  options: {
    method: SuggestionObservationMethod;
    pipelineId?: string;
    assetId?: string;
    assetPath?: string;
    connectionName?: string | null;
    connectionType?: string | null;
    databaseName?: string | null;
    workspaceRevision?: number;
    workspaceEventType?: string;
    workspaceEventPath?: string;
    workspaceLite?: boolean;
  }
): SuggestionObservation {
  return {
    method: options.method,
    recordedAt,
    pipelineId: options.pipelineId,
    assetId: options.assetId,
    assetPath: options.assetPath,
    connectionName: options.connectionName,
    connectionType: options.connectionType,
    databaseName: options.databaseName,
    workspaceRevision: options.workspaceRevision,
    workspaceEventType: options.workspaceEventType,
    workspaceEventPath: options.workspaceEventPath,
    workspaceLite: options.workspaceLite,
  };
}

function upsertConnection(
  connections: Map<string, SuggestionConnectionState>,
  input: ConnectionSuggestionEntry,
  source: SuggestionObservation
) {
  const key = connectionKey(input.name, input.databaseName);
  const current = connections.get(key);

  if (!current) {
    connections.set(key, {
      key,
      name: input.name,
      type: input.type,
      databaseName: input.databaseName,
      sourceMethods: [source.method],
      sources: [source],
    });
    return;
  }

  connections.set(key, {
    ...current,
    type: current.type || input.type,
    databaseName: current.databaseName ?? input.databaseName,
    sourceMethods: addSourceMethod(current.sourceMethods, source.method),
    sources: addSource(current.sources, source),
  });
}

function upsertTable(
  tables: Map<string, SuggestionTableState>,
  input: Omit<SuggestionTableState, "sourceMethods" | "sources" | "columns">,
  source: SuggestionObservation
): SuggestionTableState {
  const current = tables.get(input.key);

  if (!current) {
    const created: SuggestionTableState = {
      ...input,
      sourceMethods: [source.method],
      sources: [source],
      columns: [],
    };
    tables.set(input.key, created);
    return created;
  }

  const next: SuggestionTableState = {
    ...current,
    shortName: current.shortName || input.shortName,
    schemaName: current.schemaName ?? input.schemaName,
    databaseName: current.databaseName ?? input.databaseName,
    connectionKey: current.connectionKey ?? input.connectionKey,
    connectionName: current.connectionName ?? input.connectionName,
    connectionType: current.connectionType ?? input.connectionType,
    pipelineId: current.pipelineId ?? input.pipelineId,
    assetId: current.assetId ?? input.assetId,
    assetPath: current.assetPath ?? input.assetPath,
    isBruinAsset: current.isBruinAsset || input.isBruinAsset,
    remoteSuggestionKind:
      current.remoteSuggestionKind ?? input.remoteSuggestionKind,
    remoteSuggestionDetail:
      current.remoteSuggestionDetail ?? input.remoteSuggestionDetail,
    sourceMethods: addSourceMethod(current.sourceMethods, source.method),
    sources: addSource(current.sources, source),
    columns: current.columns,
  };

  tables.set(input.key, next);
  return next;
}

function upsertColumns(
  tables: Map<string, SuggestionTableState>,
  tableKey: string,
  columns: SchemaColumn[],
  source: SuggestionObservation
) {
  const table = tables.get(tableKey);
  if (!table || columns.length === 0) {
    return;
  }

  const columnsByKey = new Map(
    table.columns.map((column) => [column.key, column] as const)
  );

  for (const column of columns) {
    const key = columnKey(tableKey, column.name);
    const current = columnsByKey.get(key);
    if (!current) {
      columnsByKey.set(key, {
        key,
        name: column.name,
        type: column.type,
        description: column.description,
        primaryKey: column.primaryKey,
        tableKey,
        sourceMethods: [source.method],
        sources: [source],
      });
      continue;
    }

    columnsByKey.set(key, {
      ...current,
      type: mergeColumnValue(current.type, column.type),
      description: mergeColumnValue(current.description, column.description),
      primaryKey: current.primaryKey ?? column.primaryKey,
      sourceMethods: addSourceMethod(current.sourceMethods, source.method),
      sources: addSource(current.sources, source),
    });
  }

  tables.set(tableKey, {
    ...table,
    columns: sortColumns(Array.from(columnsByKey.values())),
  });
}

function emptyCatalog(): SuggestionCatalogState {
  return {
    connections: [],
    connectionsByKey: {},
    tables: [],
    tablesByKey: {},
  };
}

function buildCatalogFromWorkspace(
  workspace: WorkspaceState | null,
  syncSource: SuggestionWorkspaceSyncSource | null
): SuggestionCatalogState {
  if (!workspace) {
    return emptyCatalog();
  }

  const connections = new Map<string, SuggestionConnectionState>();
  const tables = new Map<string, SuggestionTableState>();
  const recordedAt = syncSource?.recordedAt ?? new Date(0).toISOString();
  const method = syncSource?.method ?? "workspace-load";
  const workspaceRevision = workspace.revision;

  for (const [name, type] of Object.entries(workspace.connections ?? {})) {
    upsertConnection(
      connections,
      { name, type },
      buildWorkspaceObservation(recordedAt, {
        method,
        connectionName: name,
        connectionType: type,
        workspaceRevision,
        workspaceEventType: syncSource?.eventType,
        workspaceEventPath: syncSource?.eventPath,
        workspaceLite: syncSource?.lite,
      })
    );
  }

  for (const pipeline of workspace.pipelines ?? []) {
    for (const asset of pipeline.assets ?? []) {
      const connectionName = resolveConnection(asset, workspace.connections ?? {});
      const connectionType = connectionName
        ? workspace.connections?.[connectionName] ?? null
        : null;
      const tableParts = parseQualifiedTableName(asset.name);
      const tableKey = assetTableKey(asset.id, pipeline.id);
      const tableSource = buildWorkspaceObservation(recordedAt, {
        method,
        pipelineId: pipeline.id,
        assetId: asset.id,
        assetPath: asset.path,
        connectionName,
        connectionType,
        databaseName: tableParts.databaseName,
        workspaceRevision,
        workspaceEventType: syncSource?.eventType,
        workspaceEventPath: syncSource?.eventPath,
        workspaceLite: syncSource?.lite,
      });

      if (connectionName && connectionType) {
        upsertConnection(
          connections,
          {
            name: connectionName,
            type: connectionType,
            databaseName: tableParts.databaseName,
          },
          tableSource
        );
      }

      upsertTable(
        tables,
        {
          key: tableKey,
          name: asset.name,
          shortName: tableParts.shortName,
          schemaName: tableParts.schemaName,
          databaseName: tableParts.databaseName,
          connectionKey: connectionName
            ? connectionKey(connectionName, tableParts.databaseName)
            : null,
          connectionName,
          connectionType,
          pipelineId: pipeline.id,
          assetId: asset.id,
          assetPath: asset.path,
          isBruinAsset: true,
        },
        tableSource
      );

      upsertColumns(tables, tableKey, toSchemaColumns(asset.columns), tableSource);
    }
  }

  return toCatalogResult(connections, tables);
}

function findWorkspaceAsset(
  workspace: WorkspaceState,
  assetId: string
): { asset: WebAsset; pipelineId: string } | null {
  for (const pipeline of workspace.pipelines ?? []) {
    for (const asset of pipeline.assets ?? []) {
      if (asset.id === assetId) {
        return { asset, pipelineId: pipeline.id };
      }
    }
  }

  return null;
}

function mergeDynamicSuggestions(
  baseCatalog: SuggestionCatalogState,
  workspace: WorkspaceState | null,
  dynamicState: DynamicSuggestionState
): SuggestionCatalogState {
  const connections = new Map(
    Object.values(baseCatalog.connectionsByKey).map((connection) => [
      connection.key,
      connection,
    ])
  );
  const tables = new Map(
    Object.values(baseCatalog.tablesByKey).map((table) => [table.key, table])
  );

  for (const [assetId, observations] of Object.entries(
    dynamicState.assetColumnsByAssetId
  )) {
    const workspaceAsset = workspace ? findWorkspaceAsset(workspace, assetId) : null;
    const tableKey = workspaceAsset
      ? assetTableKey(assetId, workspaceAsset.pipelineId)
      : assetTableKey(assetId);
    const connectionName = workspaceAsset
      ? resolveConnection(workspaceAsset.asset, workspace?.connections ?? {})
      : null;
    const connectionType = connectionName
      ? workspace?.connections?.[connectionName] ?? null
      : null;
    const qualifiedName = normalizeTableName(workspaceAsset?.asset.name ?? assetId);
    const tableParts = parseQualifiedTableName(qualifiedName);

    upsertTable(
      tables,
      {
        key: tableKey,
        name: qualifiedName,
        shortName: tableParts.shortName,
        schemaName: tableParts.schemaName,
        databaseName: tableParts.databaseName,
        connectionKey: connectionName
          ? connectionKey(connectionName, tableParts.databaseName)
          : null,
        connectionName,
        connectionType,
        pipelineId: workspaceAsset?.pipelineId,
        assetId,
        assetPath: workspaceAsset?.asset.path,
        isBruinAsset: true,
      },
      {
        method: observations[0]?.method ?? "asset-column-inference",
        recordedAt: observations[0]?.recordedAt ?? new Date().toISOString(),
        assetId,
        pipelineId: workspaceAsset?.pipelineId,
        assetPath: workspaceAsset?.asset.path,
        connectionName,
        connectionType,
        databaseName: tableParts.databaseName,
      }
    );

    for (const observation of observations) {
      upsertColumns(tables, tableKey, observation.columns, {
        method: observation.method,
        recordedAt: observation.recordedAt,
        assetId,
        pipelineId: workspaceAsset?.pipelineId,
        assetPath: workspaceAsset?.asset.path,
        connectionName,
        connectionType,
        databaseName: tableParts.databaseName,
        environment: observation.environment,
      });
    }
  }

  for (const observations of Object.values(dynamicState.remoteTablesByConnectionKey)) {
    for (const observation of observations) {
      const key = connectionKey(observation.connectionName, observation.databaseName);
      const source: SuggestionObservation = {
        method: observation.method,
        recordedAt: observation.recordedAt,
        connectionName: observation.connectionName,
        connectionType: observation.connectionType,
        databaseName: observation.databaseName,
        environment: observation.environment,
      };

      if (observation.connectionType) {
        upsertConnection(
          connections,
          {
            name: observation.connectionName,
            type: observation.connectionType,
            databaseName: observation.databaseName,
          },
          source
        );
      }

      for (const suggestion of observation.suggestions) {
        const normalizedName = normalizeTableName(suggestion.value);
        if (!normalizedName) {
          continue;
        }

        const tableParts = parseQualifiedTableName(normalizedName);
        upsertTable(
          tables,
          {
            key: externalTableKey(
              observation.connectionName,
              normalizedName,
              observation.databaseName ?? tableParts.databaseName
            ),
            name: normalizedName,
            shortName: tableParts.shortName,
            schemaName: tableParts.schemaName,
            databaseName:
              observation.databaseName ?? tableParts.databaseName ?? null,
            connectionKey: key,
            connectionName: observation.connectionName,
            connectionType: observation.connectionType ?? null,
            isBruinAsset: false,
            remoteSuggestionKind: suggestion.kind,
            remoteSuggestionDetail: suggestion.detail,
          },
          source
        );
      }
    }
  }

  return toCatalogResult(connections, tables);
}

function toCatalogResult(
  connections: Map<string, SuggestionConnectionState>,
  tables: Map<string, SuggestionTableState>
): SuggestionCatalogState {
  const sortedConnections = sortConnections(Array.from(connections.values()));
  const sortedTables = sortTables(Array.from(tables.values()));

  return {
    connections: sortedConnections,
    connectionsByKey: Object.fromEntries(
      sortedConnections.map((connection) => [connection.key, connection])
    ),
    tables: sortedTables,
    tablesByKey: Object.fromEntries(
      sortedTables.map((table) => [table.key, table])
    ),
  };
}

export function buildSuggestionCatalog(args: {
  workspace: WorkspaceState | null;
  syncSource: SuggestionWorkspaceSyncSource | null;
  dynamicState: DynamicSuggestionState;
}): SuggestionCatalogState {
  const baseCatalog = buildCatalogFromWorkspace(args.workspace, args.syncSource);
  return mergeDynamicSuggestions(baseCatalog, args.workspace, args.dynamicState);
}

export function mergeRemoteSuggestions(
  left: IngestrSuggestion[],
  right: IngestrSuggestion[]
): IngestrSuggestion[] {
  const merged = new Map<string, IngestrSuggestion>();

  for (const item of [...left, ...right]) {
    const key = normalizeTableName(item.value).toLowerCase();
    const current = merged.get(key);

    if (!current) {
      merged.set(key, item);
      continue;
    }

    merged.set(key, {
      value: current.value || item.value,
      kind: current.kind ?? item.kind,
      detail: current.detail ?? item.detail,
    });
  }

  return Array.from(merged.values()).sort((a, b) => a.value.localeCompare(b.value));
}

export function getConnectionSuggestions(
  catalog: SuggestionCatalogState
): ConnectionSuggestionEntry[] {
  return catalog.connections.map((connection) => ({
    name: connection.name,
    type: connection.type,
    databaseName: connection.databaseName,
  }));
}

export function getSelectedAssetSuggestionTable(
  catalog: SuggestionCatalogState,
  assetId: string | null | undefined
): SuggestionTableState | null {
  if (!assetId) {
    return null;
  }

  return catalog.tables.find((table) => table.assetId === assetId) ?? null;
}

export function getSelectedAssetColumnEntries(
  table: SuggestionTableState | null
): Array<{ name?: string }> {
  return (table?.columns ?? []).map((column) => ({ name: column.name }));
}

export function getSelectedAssetInspectColumns(
  table: SuggestionTableState | null
): string[] {
  if (!table) {
    return [];
  }

  return table.columns
    .filter((column) => column.sourceMethods.includes("asset-inspect"))
    .map((column) => column.name);
}

export function getSchemaSuggestionTablesForAsset(
  workspace: WorkspaceState | null,
  catalog: SuggestionCatalogState,
  asset: WebAsset | null
): SuggestionTableState[] {
  if (!workspace || !asset) {
    return [];
  }

  const currentConnection = resolveConnection(asset, workspace.connections ?? {});
  if (!currentConnection) {
    return [];
  }

  return catalog.tables.filter(
    (table) => table.isBruinAsset && table.connectionName === currentConnection
  );
}

export function toSchemaTables(tables: SuggestionTableState[]): SchemaTable[] {
  return tables.map((table) => ({
    name: table.name,
    shortName: table.shortName,
    columns: table.columns.map((column) => ({
      name: column.name,
      type: column.type,
      description: column.description,
      primaryKey: column.primaryKey,
    })),
    isBruinAsset: table.isBruinAsset,
    assetId: table.assetId,
    pipelineId: table.pipelineId,
    assetPath: table.assetPath,
    connectionName: table.connectionName ?? undefined,
    connectionType: table.connectionType ?? undefined,
    databaseName: table.databaseName ?? undefined,
    sourceMethods: table.sourceMethods,
  }));
}

export function getIngestrTableSuggestionsFromCatalog(
  catalog: SuggestionCatalogState,
  options: {
    connectionName: string;
    environment?: string;
    prefix?: string;
  }
): IngestrSuggestion[] {
  const prefix = options.prefix?.trim().toLowerCase() ?? "";

  return catalog.tables
    .filter((table) => {
      if (table.isBruinAsset || table.connectionName !== options.connectionName) {
        return false;
      }

      const matchingSource = table.sources.some(
        (source) =>
          source.method === "ingestr-suggestions" &&
          (!options.environment || source.environment === options.environment)
      );

      if (!matchingSource) {
        return false;
      }

      if (!prefix) {
        return true;
      }

      return table.name.toLowerCase().includes(prefix);
    })
    .map((table) => ({
      value: table.name,
      kind: table.remoteSuggestionKind,
      detail: table.remoteSuggestionDetail,
    }))
    .sort((left, right) => left.value.localeCompare(right.value));
}