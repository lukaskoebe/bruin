import {
  AssetFreshnessResponse,
  AssetInspectResponse,
  InferColumnsResponse,
  IngestrSuggestionsResponse,
  PipelineMaterializationResponse,
  SqlDiscoveryDatabasesResponse,
  SqlDiscoveryTableColumnsResponse,
  SqlDiscoveryTablesResponse,
  SqlPathSuggestionsResponse,
  WebColumn,
  WorkspaceConfigResponse,
  WorkspaceState,
} from "@/lib/types";
import { extractInspectErrorText } from "@/lib/inspect-errors";

type JSONErrorPayload = {
  error?: { message?: string };
  message?: string;
};

type MaterializeStreamPayload = {
  status?: "ok" | "error";
  command?: string[];
  output?: string;
  error?: string;
  exit_code?: number;
  changed_asset_ids?: string[];
  materialized_at?: string;
  chunk?: string;
};

type FillColumnsFromDBResponse = {
  status: "ok" | "error";
  results?: Array<{
    command: string[];
    output: string;
    exit_code: number;
    error?: string;
  }>;
};

async function readJSON<T>(res: Response): Promise<T> {
  if (!res.ok) {
    throw new Error(await getResponseErrorMessage(res));
  }

  return (await res.json()) as T;
}

async function getResponseErrorMessage(res: Response): Promise<string> {
  const text = await res.text();
  const parsed = parseJSONSafely<JSONErrorPayload>(text);

  return (
    parsed?.error?.message ||
    parsed?.message ||
    text ||
    `Request failed: ${res.status}`
  );
}

function parseJSONSafely<T>(text: string): T | null {
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
}

function buildQueryString(params: Record<string, string | number | undefined>) {
  const searchParams = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === "") {
      continue;
    }

    searchParams.set(key, String(value));
  }

  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

async function fetchJSON<T>(input: RequestInfo | URL, init?: RequestInit) {
  const res = await fetch(input, init);
  return readJSON<T>(res);
}

async function fetchJSONWithBody<T>(
  input: RequestInfo | URL,
  method: "POST" | "PUT" | "DELETE",
  body?: unknown,
  init?: RequestInit
) {
  return fetchJSON<T>(input, {
    ...init,
    method,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

async function readTextOrThrow(res: Response) {
  if (!res.ok) {
    throw new Error(await getResponseErrorMessage(res));
  }

  return res.text();
}

async function readSSEStream(
  res: Response,
  handlers: {
    onChunk?: (chunk: string) => void;
    onDone?: (payload: MaterializeStreamPayload) => void;
  },
  endedMessage: string
) {
  if (!res.ok) {
    throw new Error(await getResponseErrorMessage(res));
  }

  if (!res.body) {
    throw new Error("Streaming response body is not available.");
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let donePayload: MaterializeStreamPayload | null = null;

  while (true) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });

    let delimiterIndex = buffer.indexOf("\n\n");
    while (delimiterIndex >= 0) {
      const rawEvent = buffer.slice(0, delimiterIndex);
      buffer = buffer.slice(delimiterIndex + 2);
      delimiterIndex = buffer.indexOf("\n\n");

      const parsed = parseSSEEvent(rawEvent);
      if (!parsed) {
        continue;
      }

      if (parsed.event === "output" && typeof parsed.data?.chunk === "string") {
        handlers.onChunk?.(parsed.data.chunk);
      }

      if (parsed.event === "done") {
        donePayload = parsed.data;
        handlers.onDone?.(parsed.data);
      }
    }

    if (done) {
      break;
    }
  }

  if (!donePayload) {
    throw new Error(endedMessage);
  }

  return donePayload;
}

function parseSSEEvent(rawEvent: string): {
  event: string;
  data: MaterializeStreamPayload;
} | null {
  const lines = rawEvent.split(/\r?\n/);
  let event = "message";
  const dataLines: string[] = [];

  for (const line of lines) {
    if (line.startsWith("event:")) {
      event = line.slice("event:".length).trim();
      continue;
    }

    if (line.startsWith("data:")) {
      dataLines.push(line.slice("data:".length).trim());
    }
  }

  if (dataLines.length === 0) {
    return null;
  }

  return {
    event,
    data: JSON.parse(dataLines.join("\n")) as MaterializeStreamPayload,
  };
}

function normalizeInspectResponse(
  response: AssetInspectResponse
): AssetInspectResponse {
  if (response.status !== "error") {
    return response;
  }

  const extracted = extractInspectErrorText(response.raw_output);

  if (!extracted) {
    return response;
  }

  return {
    ...response,
    error: extracted,
  };
}

export async function getWorkspace(): Promise<WorkspaceState> {
  return fetchJSON<WorkspaceState>("/api/workspace", { cache: "no-store" });
}

export async function getWorkspaceConfig(): Promise<WorkspaceConfigResponse> {
  return fetchJSON<WorkspaceConfigResponse>("/api/config", {
    cache: "no-store",
  });
}

export async function createWorkspaceEnvironment(input: {
  name: string;
  schema_prefix?: string;
  set_as_default?: boolean;
}): Promise<WorkspaceConfigResponse> {
  return fetchJSONWithBody<WorkspaceConfigResponse>(
    "/api/config/environments",
    "POST",
    input
  );
}

export async function updateWorkspaceEnvironment(input: {
  name: string;
  new_name?: string;
  schema_prefix?: string;
  set_as_default?: boolean;
}): Promise<WorkspaceConfigResponse> {
  return fetchJSONWithBody<WorkspaceConfigResponse>(
    "/api/config/environments",
    "PUT",
    input
  );
}

export async function cloneWorkspaceEnvironment(input: {
  source_name: string;
  target_name: string;
  schema_prefix?: string;
  set_as_default?: boolean;
}): Promise<WorkspaceConfigResponse> {
  return fetchJSONWithBody<WorkspaceConfigResponse>(
    "/api/config/environments/clone",
    "POST",
    input
  );
}

export async function deleteWorkspaceEnvironment(
  name: string
): Promise<WorkspaceConfigResponse> {
  return fetchJSONWithBody<WorkspaceConfigResponse>(
    "/api/config/environments",
    "DELETE",
    { name }
  );
}

export async function createWorkspaceConnection(input: {
  environment_name: string;
  name: string;
  type: string;
  values: Record<string, unknown>;
}): Promise<WorkspaceConfigResponse> {
  return fetchJSONWithBody<WorkspaceConfigResponse>(
    "/api/config/connections",
    "POST",
    input
  );
}

export async function updateWorkspaceConnection(input: {
  environment_name: string;
  current_name?: string;
  name: string;
  type: string;
  values: Record<string, unknown>;
}): Promise<WorkspaceConfigResponse> {
  return fetchJSONWithBody<WorkspaceConfigResponse>(
    "/api/config/connections",
    "PUT",
    input
  );
}

export async function deleteWorkspaceConnection(input: {
  environment_name: string;
  name: string;
}): Promise<WorkspaceConfigResponse> {
  return fetchJSONWithBody<WorkspaceConfigResponse>(
    "/api/config/connections",
    "DELETE",
    input
  );
}

export async function testWorkspaceConnection(input: {
  environment_name: string;
  name: string;
}): Promise<{ status: string; message?: string }> {
  return fetchJSONWithBody<{ status: string; message?: string }>(
    "/api/config/connections/test",
    "POST",
    input
  );
}

export async function createPipeline(input: {
  path: string;
  name?: string;
  content?: string;
}) {
  return fetchJSONWithBody<Record<string, string>>(
    "/api/pipelines",
    "POST",
    input
  );
}

export async function deletePipeline(pipelineId: string) {
  return fetchJSON<Record<string, string>>(`/api/pipelines/${pipelineId}`, {
    method: "DELETE",
  });
}

export async function createAsset(
  pipelineId: string,
  input: {
    name?: string;
    type?: string;
    path?: string;
    content?: string;
    source_asset_id?: string;
  }
) {
  return fetchJSONWithBody<{ status: string; asset_id?: string; asset_path?: string }>(
    `/api/pipelines/${pipelineId}/assets`,
    "POST",
    input
  );
}

export async function updateAsset(
  pipelineId: string,
  assetId: string,
  input: {
    name?: string;
    type?: string;
    content?: string;
    materialization_type?: string;
    meta?: Record<string, string>;
  }
) {
  return fetchJSONWithBody<Record<string, string>>(
    `/api/pipelines/${pipelineId}/assets/${assetId}`,
    "PUT",
    input
  );
}

export async function deleteAsset(pipelineId: string, assetId: string) {
  return fetchJSON<Record<string, string>>(
    `/api/pipelines/${pipelineId}/assets/${assetId}`,
    {
      method: "DELETE",
    }
  );
}

export async function inspectAsset(
  assetId: string,
  options?: { limit?: number; environment?: string }
) {
  const res = await fetch(
    `/api/assets/${assetId}/inspect${buildQueryString({
      limit: options?.limit,
      environment: options?.environment,
    })}`,
    { method: "GET" }
  );

  const text = await readTextOrThrow(res);
  const parsed = parseJSONSafely<AssetInspectResponse>(text);

  if (parsed) {
    return normalizeInspectResponse(parsed);
  }

  throw new Error(text || `Request failed: ${res.status}`);
}

export async function materializeAssetStream(
  assetId: string,
  handlers: {
    onChunk?: (chunk: string) => void;
    onDone?: (payload: MaterializeStreamPayload) => void;
  }
) {
  const res = await fetch(`/api/assets/${assetId}/materialize/stream`, {
    method: "POST",
    headers: {
      Accept: "text/event-stream",
    },
  });

  return readSSEStream(
    res,
    handlers,
    "Asset materialization stream ended unexpectedly."
  );
}

export async function materializePipelineStream(
  pipelineId: string,
  handlers: {
    onChunk?: (chunk: string) => void;
    onDone?: (payload: MaterializeStreamPayload) => void;
  }
) {
  const res = await fetch(`/api/pipelines/${pipelineId}/materialize/stream`, {
    method: "POST",
    headers: {
      Accept: "text/event-stream",
    },
  });

  return readSSEStream(
    res,
    handlers,
    "Pipeline materialization stream ended unexpectedly."
  );
}

export async function getIngestrSuggestions(options: {
  connection: string;
  prefix?: string;
  environment?: string;
}) {
  return fetchJSON<IngestrSuggestionsResponse>(
    `/api/ingestr/suggestions${buildQueryString({
      connection: options.connection,
      prefix: options.prefix,
      environment: options.environment,
    })}`,
    { cache: "no-store" }
  );
}

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

export async function getPipelineMaterialization(pipelineId: string) {
  return fetchJSON<PipelineMaterializationResponse>(
    `/api/pipelines/${pipelineId}/materialization`,
    {
      method: "GET",
      cache: "no-store",
    }
  );
}

export async function inferAssetColumns(assetId: string) {
  return fetchJSON<InferColumnsResponse>(`/api/assets/${assetId}/columns/infer`, {
    method: "GET",
  });
}

export async function updateAssetColumns(
  assetId: string,
  columns: WebColumn[]
) {
  return fetchJSONWithBody<Record<string, string>>(
    `/api/assets/${assetId}/columns`,
    "PUT",
    { columns }
  );
}

export async function fillAssetColumnsFromDB(assetId: string) {
  const res = await fetch(`/api/assets/${assetId}/fill-columns-from-db`, {
    method: "POST",
  });

  const text = await readTextOrThrow(res);
  if (!text) {
    return { status: res.ok ? "ok" : "error" };
  }

  const parsed = parseJSONSafely<FillColumnsFromDBResponse>(text);
  if (!parsed) {
    throw new Error(text || `Request failed: ${res.status}`);
  }

  return parsed;
}

export async function getAssetFreshness(): Promise<AssetFreshnessResponse> {
  return fetchJSON<AssetFreshnessResponse>("/api/assets/freshness", {
    cache: "no-store",
  });
}
