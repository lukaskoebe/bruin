import { atom } from "jotai";

import { SchemaColumn, SchemaTable } from "@/lib/sql-schema";

import { selectedAssetDataAtom } from "./selection";
import {
  buildSuggestionCatalog,
  getConnectionSuggestions,
  getSchemaSuggestionTablesForAsset,
  getSelectedAssetColumnEntries,
  getSelectedAssetInspectColumns,
  getSelectedAssetSuggestionTable,
  toSchemaTables,
} from "./suggestion-catalog";
import {
  ConnectionSuggestionEntry,
  DynamicAssetColumnObservation,
  DynamicRemoteDatabaseObservation,
  DynamicRemoteTableObservation,
  DynamicRemoteTableColumnObservation,
  DynamicSuggestionState,
  emptyDynamicSuggestionState,
  RegisterAssetColumnsPayload,
  RegisterConnectionDatabasesPayload,
  RegisterConnectionTablesPayload,
  RegisterRemoteTableColumnsPayload,
  RemoteTableSuggestionEntry,
  SuggestionCatalogState,
  SuggestionDatabaseState,
  SuggestionTableState,
} from "./suggestion-types";
import { workspaceAtom, workspaceSyncSourceAtom } from "./workspace";

export type {
  ConnectionSuggestionEntry,
  RegisterAssetColumnsPayload,
  RegisterConnectionDatabasesPayload,
  RegisterConnectionTablesPayload,
  RegisterRemoteTableColumnsPayload,
  SuggestionCatalogState,
  SuggestionColumnState,
  SuggestionConnectionState,
  SuggestionDatabaseState,
  SuggestionObservation,
  SuggestionObservationMethod,
  SuggestionTableState,
} from "./suggestion-types";
export {
  getDatabaseSuggestions,
  getIngestrTableSuggestionsFromCatalog,
} from "./suggestion-catalog";

const dynamicSuggestionStateAtom = atom<DynamicSuggestionState>(
  emptyDynamicSuggestionState
);

function normalizeRegisteredColumns(
  columns: RegisterAssetColumnsPayload["columns"]
): SchemaColumn[] {
  return columns
    .map((column) => ({
      name: column.name,
      type: column.type,
      description: column.description,
      primaryKey: column.primaryKey,
    }))
    .filter((column) => column.name.trim().length > 0);
}

function replaceAssetColumnObservation(
  observations: DynamicAssetColumnObservation[],
  nextObservation: DynamicAssetColumnObservation
): DynamicAssetColumnObservation[] {
  const signature = `${nextObservation.method}::${nextObservation.environment ?? ""}`;

  return [
    ...observations.filter(
      (observation) =>
        `${observation.method}::${observation.environment ?? ""}` !== signature
    ),
    nextObservation,
  ];
}

function replaceConnectionTableObservation(
  observations: DynamicSuggestionState["remoteTablesByConnectionKey"][string],
  nextObservation: DynamicSuggestionState["remoteTablesByConnectionKey"][string][number]
): DynamicSuggestionState["remoteTablesByConnectionKey"][string] {
  const signature = `${nextObservation.method}::${
    nextObservation.environment ?? ""
  }::${nextObservation.databaseName ?? ""}::${nextObservation.prefix ?? ""}`;

  return [
    ...observations.filter(
      (observation) =>
        `${observation.method}::${observation.environment ?? ""}::${
          observation.databaseName ?? ""
        }::${observation.prefix ?? ""}` !== signature
    ),
    nextObservation,
  ];
}

function replaceConnectionDatabaseObservation(
  observations: DynamicSuggestionState["remoteDatabasesByConnectionKey"][string],
  nextObservation: DynamicRemoteDatabaseObservation
): DynamicSuggestionState["remoteDatabasesByConnectionKey"][string] {
  const signature = `${nextObservation.environment ?? ""}`;

  return [
    ...observations.filter(
      (observation) => `${observation.environment ?? ""}` !== signature
    ),
    nextObservation,
  ];
}

function replaceRemoteTableColumnObservation(
  observations: DynamicSuggestionState["remoteTableColumnsByTableKey"][string],
  nextObservation: DynamicRemoteTableColumnObservation
): DynamicSuggestionState["remoteTableColumnsByTableKey"][string] {
  const signature = `${nextObservation.environment ?? ""}`;

  return [
    ...observations.filter(
      (observation) => `${observation.environment ?? ""}` !== signature
    ),
    nextObservation,
  ];
}

function normalizeRegisteredRemoteTables(
  tables: RegisterConnectionTablesPayload["tables"]
): RemoteTableSuggestionEntry[] {
  return tables
    .map((table) => ({
      name: table.name.trim(),
      schemaName: table.schemaName,
      databaseName: table.databaseName,
      kind: table.kind,
      detail: table.detail,
    }))
    .filter((table) => table.name.length > 0);
}

function mergeRemoteTableEntries(
  left: RemoteTableSuggestionEntry[],
  right: RemoteTableSuggestionEntry[]
): RemoteTableSuggestionEntry[] {
  const merged = new Map<string, RemoteTableSuggestionEntry>();

  for (const item of [...left, ...right]) {
    const key = item.name.toLowerCase();
    const current = merged.get(key);

    if (!current) {
      merged.set(key, item);
      continue;
    }

    merged.set(key, {
      name: current.name || item.name,
      schemaName: current.schemaName ?? item.schemaName,
      databaseName: current.databaseName ?? item.databaseName,
      kind: current.kind ?? item.kind,
      detail: current.detail ?? item.detail,
    });
  }

  return Array.from(merged.values()).sort((left, right) =>
    left.name.localeCompare(right.name)
  );
}

export const registerAssetColumnsAtom = atom(
  null,
  (get, set, payload: RegisterAssetColumnsPayload) => {
    const current = get(dynamicSuggestionStateAtom);
    const nextObservation: DynamicAssetColumnObservation = {
      method: payload.method,
      recordedAt: new Date().toISOString(),
      environment: payload.environment,
      columns: normalizeRegisteredColumns(payload.columns),
    };
    const currentObservations =
      current.assetColumnsByAssetId[payload.assetId] ?? [];

    set(dynamicSuggestionStateAtom, {
      ...current,
      assetColumnsByAssetId: {
        ...current.assetColumnsByAssetId,
        [payload.assetId]: replaceAssetColumnObservation(
          currentObservations,
          nextObservation
        ),
      },
    });
  }
);

export const registerConnectionTablesAtom = atom(
  null,
  (get, set, payload: RegisterConnectionTablesPayload) => {
    const current = get(dynamicSuggestionStateAtom);
    const key = `${payload.connectionName.toLowerCase()}::${(
      payload.databaseName ?? ""
    ).toLowerCase()}`;
    const currentObservations = current.remoteTablesByConnectionKey[key] ?? [];
    const existing = currentObservations.find(
      (observation) =>
        `${observation.method}::${observation.environment ?? ""}::${
          observation.databaseName ?? ""
        }::${observation.prefix ?? ""}` ===
        `${payload.method ?? "ingestr-suggestions"}::${payload.environment ?? ""}::${
          payload.databaseName ?? ""
        }::${payload.prefix ?? ""}`
    );

    const mergedTables = existing
      ? mergeRemoteTableEntries(existing.tables, payload.tables)
      : normalizeRegisteredRemoteTables(payload.tables);
    const nextObservation: DynamicRemoteTableObservation = {
      method: payload.method ?? "ingestr-suggestions",
      recordedAt: new Date().toISOString(),
      environment: payload.environment,
      prefix: payload.prefix,
      connectionName: payload.connectionName,
      connectionType: payload.connectionType,
      databaseName: payload.databaseName,
      tables: mergedTables,
    };

    set(dynamicSuggestionStateAtom, {
      ...current,
      remoteTablesByConnectionKey: {
        ...current.remoteTablesByConnectionKey,
        [key]: replaceConnectionTableObservation(
          currentObservations,
          nextObservation
        ),
      },
    });
  }
);

export const registerConnectionDatabasesAtom = atom(
  null,
  (get, set, payload: RegisterConnectionDatabasesPayload) => {
    const current = get(dynamicSuggestionStateAtom);
    const key = `${payload.connectionName.toLowerCase()}::`;
    const currentObservations = current.remoteDatabasesByConnectionKey[key] ?? [];
    const nextObservation: DynamicRemoteDatabaseObservation = {
      method: "connection-database-discovery",
      recordedAt: new Date().toISOString(),
      environment: payload.environment,
      connectionName: payload.connectionName,
      connectionType: payload.connectionType,
      databases: payload.databases
        .map((databaseName) => databaseName.trim())
        .filter((databaseName) => databaseName.length > 0)
        .sort((left, right) => left.localeCompare(right)),
    };

    set(dynamicSuggestionStateAtom, {
      ...current,
      remoteDatabasesByConnectionKey: {
        ...current.remoteDatabasesByConnectionKey,
        [key]: replaceConnectionDatabaseObservation(
          currentObservations,
          nextObservation
        ),
      },
    });
  }
);

export const registerRemoteTableColumnsAtom = atom(
  null,
  (get, set, payload: RegisterRemoteTableColumnsPayload) => {
    const current = get(dynamicSuggestionStateAtom);
    const key = [
      payload.connectionName.toLowerCase(),
      (payload.databaseName ?? "").toLowerCase(),
      payload.tableName.toLowerCase(),
    ].join("::");
    const currentObservations = current.remoteTableColumnsByTableKey[key] ?? [];
    const nextObservation: DynamicRemoteTableColumnObservation = {
      method: "connection-column-discovery",
      recordedAt: new Date().toISOString(),
      environment: payload.environment,
      connectionName: payload.connectionName,
      connectionType: payload.connectionType,
      databaseName: payload.databaseName,
      tableName: payload.tableName,
      columns: normalizeRegisteredColumns(payload.columns),
    };

    set(dynamicSuggestionStateAtom, {
      ...current,
      remoteTableColumnsByTableKey: {
        ...current.remoteTableColumnsByTableKey,
        [key]: replaceRemoteTableColumnObservation(
          currentObservations,
          nextObservation
        ),
      },
    });
  }
);

export const suggestionCatalogAtom = atom<SuggestionCatalogState>((get) => {
  return buildSuggestionCatalog({
    workspace: get(workspaceAtom),
    syncSource: get(workspaceSyncSourceAtom),
    dynamicState: get(dynamicSuggestionStateAtom),
  });
});

export const connectionSuggestionsAtom = atom<ConnectionSuggestionEntry[]>((get) =>
  getConnectionSuggestions(get(suggestionCatalogAtom))
);

export const databaseSuggestionsAtom = atom<SuggestionDatabaseState[]>((get) =>
  get(suggestionCatalogAtom).databases
);

export const selectedAssetSuggestionTableAtom = atom<SuggestionTableState | null>(
  (get) => {
    return getSelectedAssetSuggestionTable(
      get(suggestionCatalogAtom),
      get(selectedAssetDataAtom)?.id
    );
  }
);

export const selectedAssetColumnEntriesAtom = atom<Array<{ name?: string }>>(
  (get) => getSelectedAssetColumnEntries(get(selectedAssetSuggestionTableAtom))
);

export const selectedAssetInspectColumnsAtom = atom<string[]>((get) =>
  getSelectedAssetInspectColumns(get(selectedAssetSuggestionTableAtom))
);

export const selectedAssetSchemaSuggestionTablesAtom = atom<SuggestionTableState[]>(
  (get) =>
    getSchemaSuggestionTablesForAsset(
      get(workspaceAtom),
      get(suggestionCatalogAtom),
      get(selectedAssetDataAtom)
    )
);

export const selectedAssetSchemaTablesAtom = atom<SchemaTable[]>((get) =>
  toSchemaTables(get(selectedAssetSchemaSuggestionTablesAtom))
);