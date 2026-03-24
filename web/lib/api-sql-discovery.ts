import { buildQueryString, fetchJSON } from "@/lib/api-core";
import {
  SqlDiscoveryDatabasesResponse,
  SqlDiscoveryTableColumnsResponse,
  SqlDiscoveryTablesResponse,
  SqlPathSuggestionsResponse,
} from "@/lib/types";

export async function getSQLDatabases(options: {
  connection: string;
  environment?: string;
}) {
  return fetchJSON<SqlDiscoveryDatabasesResponse>(
    `/api/sql/databases${buildQueryString({
      connection: options.connection,
      environment: options.environment,
    })}`,
    { cache: "no-store" }
  );
}

export async function getSQLPathSuggestions(options: {
  assetId: string;
  prefix: string;
  environment?: string;
}) {
  return fetchJSON<SqlPathSuggestionsResponse>(
    `/api/assets/${options.assetId}/sql-path-suggestions${buildQueryString({
      prefix: options.prefix,
      environment: options.environment,
    })}`,
    { cache: "no-store" }
  );
}

export async function getSQLTables(options: {
  connection: string;
  database: string;
  environment?: string;
}) {
  return fetchJSON<SqlDiscoveryTablesResponse>(
    `/api/sql/tables${buildQueryString({
      connection: options.connection,
      database: options.database,
      environment: options.environment,
    })}`,
    { cache: "no-store" }
  );
}

export async function getSQLTableColumns(options: {
  connection: string;
  table: string;
  environment?: string;
}) {
  return fetchJSON<SqlDiscoveryTableColumnsResponse>(
    `/api/sql/table-columns${buildQueryString({
      connection: options.connection,
      table: options.table,
      environment: options.environment,
    })}`,
    { cache: "no-store" }
  );
}
