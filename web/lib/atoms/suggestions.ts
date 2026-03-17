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
  mergeRemoteSuggestions,
  toSchemaTables,
} from "./suggestion-catalog";
import {
  ConnectionSuggestionEntry,
  DynamicAssetColumnObservation,
  DynamicSuggestionState,
  emptyDynamicSuggestionState,
  RegisterAssetColumnsPayload,
  RegisterConnectionTablesPayload,
  SuggestionCatalogState,
  SuggestionTableState,
} from "./suggestion-types";
import { workspaceAtom, workspaceSyncSourceAtom } from "./workspace";

export type {
  ConnectionSuggestionEntry,
  RegisterAssetColumnsPayload,
  RegisterConnectionTablesPayload,
  SuggestionCatalogState,
  SuggestionColumnState,
  SuggestionConnectionState,
  SuggestionObservation,
  SuggestionObservationMethod,
  SuggestionTableState,
} from "./suggestion-types";
export { getIngestrTableSuggestionsFromCatalog } from "./suggestion-catalog";

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
  const signature = `${nextObservation.environment ?? ""}`;

  return [
    ...observations.filter(
      (observation) => `${observation.environment ?? ""}` !== signature
    ),
    nextObservation,
  ];
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
        `${observation.environment ?? ""}` === `${payload.environment ?? ""}`
    );

    const mergedSuggestions = existing
      ? mergeRemoteSuggestions(existing.suggestions, payload.suggestions)
      : payload.suggestions;
    const nextObservation: DynamicRemoteTableObservation = {
      method: "ingestr-suggestions",
      recordedAt: new Date().toISOString(),
      environment: payload.environment,
      prefix: payload.prefix,
      connectionName: payload.connectionName,
      connectionType: payload.connectionType,
      databaseName: payload.databaseName,
      suggestions: mergedSuggestions,
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