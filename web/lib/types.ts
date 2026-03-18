export type WebAsset = {
  id: string;
  name: string;
  type: string;
  path: string;
  content: string;
  upstreams: string[];
  meta?: Record<string, string>;
  columns?: WebColumn[];
  connection?: string;
  materialization_type?: string;
  is_materialized: boolean;
  freshness_status?: "fresh" | "stale";
  materialized_as?: string;
  row_count?: number;
};

export type WebColumnCheck = {
  name: string;
  value?: unknown;
  blocking?: boolean;
  description?: string;
};

export type WebColumn = {
  name: string;
  type?: string;
  description?: string;
  tags?: string[];
  primary_key?: boolean;
  update_on_merge?: boolean;
  merge_sql?: string;
  nullable?: boolean;
  owner?: string;
  domains?: string[];
  meta?: Record<string, string>;
  checks?: WebColumnCheck[];
};

export type WebPipeline = {
  id: string;
  name: string;
  path: string;
  assets: WebAsset[];
};

export type WorkspaceState = {
  pipelines: WebPipeline[];
  connections: Record<string, string>;
  selected_environment: string;
  errors: string[];
  updated_at: string;
  revision?: number;
};

export type WorkspaceEvent = {
  type: string;
  path?: string;
  workspace: WorkspaceState;
  lite?: boolean;
  changed_asset_ids?: string[];
};

export type AssetInspectResponse = {
  status: "ok" | "error";
  columns: string[];
  rows: Record<string, unknown>[];
  raw_output: string;
  command?: string[];
  error?: string;
};

export type MaterializeResponse = {
  status: "ok" | "error";
  command: string[];
  output: string;
  exit_code: number;
  error?: string;
  materialized_at?: string;
  changed_asset_ids?: string[];
};

export type PipelineMaterializationResponse = {
  pipeline_id: string;
  assets: Array<{
    asset_id: string;
    is_materialized: boolean;
    freshness_status?: "fresh" | "stale";
    materialized_as?: string;
    row_count?: number;
    connection?: string;
    materialization_type?: string;
  }>;
};

export type InferColumnsResponse = {
  status: "ok" | "error";
  columns: WebColumn[];
  raw_output: string;
  command?: string[];
  error?: string;
};

export type IngestrSuggestion = {
  value: string;
  kind?: string;
  detail?: string;
};

export type IngestrSuggestionsResponse = {
  status: "ok" | "error";
  connection_type?: string;
  suggestions: IngestrSuggestion[];
  error?: string;
};

export type SqlDiscoveryDatabasesResponse = {
  status: "ok" | "error";
  connection_name: string;
  connection_type?: string;
  databases: string[];
  error?: string;
};

export type SqlDiscoveryTable = {
  name: string;
  short_name: string;
  schema_name?: string;
  database_name?: string;
};

export type SqlDiscoveryTablesResponse = {
  status: "ok" | "error";
  connection_name: string;
  connection_type?: string;
  database: string;
  tables: SqlDiscoveryTable[];
  error?: string;
};

export type SqlDiscoveryTableColumnsResponse = {
  status: "ok" | "error";
  connection_name: string;
  table: string;
  columns: WebColumn[];
  raw_output: string;
  command?: string[];
  error?: string;
};

export type AssetFreshnessEntry = {
  asset_name: string;
  materialized_at?: string;
  materialized_status?: string;
  content_changed_at?: string;
};

export type AssetFreshnessResponse = {
  assets: AssetFreshnessEntry[];
};
