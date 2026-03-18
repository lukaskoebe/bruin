import {
  AssetFreshnessResponse,
  AssetInspectResponse,
  InferColumnsResponse,
  IngestrSuggestionsResponse,
  SqlPathSuggestionsResponse,
  SqlDiscoveryDatabasesResponse,
  SqlDiscoveryTableColumnsResponse,
  SqlDiscoveryTablesResponse,
  WebColumn,
  PipelineMaterializationResponse,
  WorkspaceConfigResponse,
  WorkspaceState,
} from "@/lib/types";

async function readJSON<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Request failed: ${res.status}`);
  }
  return (await res.json()) as T;
}

export async function getWorkspace(): Promise<WorkspaceState> {
  const res = await fetch("/api/workspace", { cache: "no-store" });
  return readJSON<WorkspaceState>(res);
}

export async function getWorkspaceConfig(): Promise<WorkspaceConfigResponse> {
  const res = await fetch("/api/config", { cache: "no-store" });
  return readJSON<WorkspaceConfigResponse>(res);
}

export async function createWorkspaceEnvironment(input: {
  name: string;
  schema_prefix?: string;
  set_as_default?: boolean;
}): Promise<WorkspaceConfigResponse> {
  const res = await fetch("/api/config/environments", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });

  return readJSON<WorkspaceConfigResponse>(res);
}

export async function updateWorkspaceEnvironment(input: {
  name: string;
  new_name?: string;
  schema_prefix?: string;
  set_as_default?: boolean;
}): Promise<WorkspaceConfigResponse> {
  const res = await fetch("/api/config/environments", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });

  return readJSON<WorkspaceConfigResponse>(res);
}

export async function cloneWorkspaceEnvironment(input: {
  source_name: string;
  target_name: string;
  schema_prefix?: string;
  set_as_default?: boolean;
}): Promise<WorkspaceConfigResponse> {
  const res = await fetch("/api/config/environments/clone", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });

  return readJSON<WorkspaceConfigResponse>(res);
}

export async function deleteWorkspaceEnvironment(name: string): Promise<WorkspaceConfigResponse> {
  const res = await fetch("/api/config/environments", {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });

  return readJSON<WorkspaceConfigResponse>(res);
}

export async function createWorkspaceConnection(input: {
  environment_name: string;
  name: string;
  type: string;
  values: Record<string, unknown>;
}): Promise<WorkspaceConfigResponse> {
  const res = await fetch("/api/config/connections", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });

  return readJSON<WorkspaceConfigResponse>(res);
}

export async function updateWorkspaceConnection(input: {
  environment_name: string;
  current_name?: string;
  name: string;
  type: string;
  values: Record<string, unknown>;
}): Promise<WorkspaceConfigResponse> {
  const res = await fetch("/api/config/connections", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });

  return readJSON<WorkspaceConfigResponse>(res);
}

export async function deleteWorkspaceConnection(input: {
  environment_name: string;
  name: string;
}): Promise<WorkspaceConfigResponse> {
  const res = await fetch("/api/config/connections", {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });

  return readJSON<WorkspaceConfigResponse>(res);
}

export async function createPipeline(input: {
  path: string;
  name?: string;
  content?: string;
}) {
  const res = await fetch("/api/pipelines", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  return readJSON<Record<string, string>>(res);
}

export async function deletePipeline(pipelineId: string) {
  const res = await fetch(`/api/pipelines/${pipelineId}`, {
    method: "DELETE",
  });
  return readJSON<Record<string, string>>(res);
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
  const res = await fetch(`/api/pipelines/${pipelineId}/assets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  return readJSON<{ status: string; asset_id?: string; asset_path?: string }>(
    res
  );
}

export async function updateAsset(
  pipelineId: string,
  assetId: string,
  input: {
    name?: string;
    content?: string;
    materialization_type?: string;
    meta?: Record<string, string>;
  }
) {
  const res = await fetch(`/api/pipelines/${pipelineId}/assets/${assetId}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  return readJSON<Record<string, string>>(res);
}

export async function deleteAsset(pipelineId: string, assetId: string) {
  const res = await fetch(`/api/pipelines/${pipelineId}/assets/${assetId}`, {
    method: "DELETE",
  });
  return readJSON<Record<string, string>>(res);
}

export async function inspectAsset(
  assetId: string,
  options?: { limit?: number; environment?: string }
) {
  const params = new URLSearchParams();
  if (options?.limit) {
    params.set("limit", String(options.limit));
  }
  if (options?.environment) {
    params.set("environment", options.environment);
  }

  const query = params.toString();
  const url = `/api/assets/${assetId}/inspect${query ? `?${query}` : ""}`;
  const res = await fetch(url, { method: "GET" });

  const text = await res.text();
  let parsed: AssetInspectResponse | null = null;

  try {
    parsed = JSON.parse(text) as AssetInspectResponse;
  } catch {
    parsed = null;
  }

  if (parsed) {
    return normalizeInspectResponse(parsed);
  }

  throw new Error(text || `Request failed: ${res.status}`);
}

function normalizeInspectResponse(
  response: AssetInspectResponse
): AssetInspectResponse {
  if (response.status !== "error") {
    return response;
  }

  const output = response.raw_output ?? "";
  const extracted = extractErrorFromRawInspectOutput(output);

  if (!extracted) {
    return response;
  }

  return {
    ...response,
    error: extracted,
  };
}

function extractErrorFromRawInspectOutput(rawOutput: string): string | null {
  const trimmed = rawOutput.trim();
  if (!trimmed) {
    return null;
  }

  try {
    const parsed = JSON.parse(trimmed) as {
      error?: unknown;
      message?: unknown;
    };
    if (typeof parsed.error === "string" && parsed.error.trim()) {
      return parsed.error.trim();
    }
    if (typeof parsed.message === "string" && parsed.message.trim()) {
      return parsed.message.trim();
    }
  } catch {
    return null;
  }

  return null;
}

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

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Request failed: ${res.status}`);
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
    throw new Error("Asset materialization stream ended unexpectedly.");
  }

  return donePayload;
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

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Request failed: ${res.status}`);
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
    throw new Error("Pipeline materialization stream ended unexpectedly.");
  }

  return donePayload;
}

export async function getIngestrSuggestions(options: {
  connection: string;
  prefix?: string;
  environment?: string;
}) {
  const params = new URLSearchParams();
  params.set("connection", options.connection);
  if (options.prefix) {
    params.set("prefix", options.prefix);
  }
  if (options.environment) {
    params.set("environment", options.environment);
  }

  const res = await fetch(`/api/ingestr/suggestions?${params.toString()}`, {
    cache: "no-store",
  });

  return readJSON<IngestrSuggestionsResponse>(res);
}

export async function getSQLDatabases(options: {
  connection: string;
  environment?: string;
}) {
  const params = new URLSearchParams();
  params.set("connection", options.connection);
  if (options.environment) {
    params.set("environment", options.environment);
  }

  const res = await fetch(`/api/sql/databases?${params.toString()}`, {
    cache: "no-store",
  });

  return readJSON<SqlDiscoveryDatabasesResponse>(res);
}

export async function getSQLPathSuggestions(options: {
  assetId: string;
  prefix: string;
  environment?: string;
}) {
  const params = new URLSearchParams();
  params.set("prefix", options.prefix);
  if (options.environment) {
    params.set("environment", options.environment);
  }

  const res = await fetch(
    `/api/assets/${options.assetId}/sql-path-suggestions?${params.toString()}`,
    {
      cache: "no-store",
    }
  );

  return readJSON<SqlPathSuggestionsResponse>(res);
}

export async function getSQLTables(options: {
  connection: string;
  database: string;
  environment?: string;
}) {
  const params = new URLSearchParams();
  params.set("connection", options.connection);
  params.set("database", options.database);
  if (options.environment) {
    params.set("environment", options.environment);
  }

  const res = await fetch(`/api/sql/tables?${params.toString()}`, {
    cache: "no-store",
  });

  return readJSON<SqlDiscoveryTablesResponse>(res);
}

export async function getSQLTableColumns(options: {
  connection: string;
  table: string;
  environment?: string;
}) {
  const params = new URLSearchParams();
  params.set("connection", options.connection);
  params.set("table", options.table);
  if (options.environment) {
    params.set("environment", options.environment);
  }

  const res = await fetch(`/api/sql/table-columns?${params.toString()}`, {
    cache: "no-store",
  });

  return readJSON<SqlDiscoveryTableColumnsResponse>(res);
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

export async function getPipelineMaterialization(pipelineId: string) {
  const res = await fetch(`/api/pipelines/${pipelineId}/materialization`, {
    method: "GET",
    cache: "no-store",
  });
  return readJSON<PipelineMaterializationResponse>(res);
}

export async function inferAssetColumns(assetId: string) {
  const res = await fetch(`/api/assets/${assetId}/columns/infer`, {
    method: "GET",
  });
  return readJSON<InferColumnsResponse>(res);
}

export async function updateAssetColumns(
  assetId: string,
  columns: WebColumn[]
) {
  const res = await fetch(`/api/assets/${assetId}/columns`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ columns }),
  });
  return readJSON<Record<string, string>>(res);
}

export async function fillAssetColumnsFromDB(assetId: string) {
  const res = await fetch(`/api/assets/${assetId}/fill-columns-from-db`, {
    method: "POST",
  });

  const text = await res.text();
  if (!text) {
    return { status: res.ok ? "ok" : "error" };
  }

  try {
    return JSON.parse(text) as {
      status: "ok" | "error";
      results?: Array<{
        command: string[];
        output: string;
        exit_code: number;
        error?: string;
      }>;
    };
  } catch {
    throw new Error(text || `Request failed: ${res.status}`);
  }
}

export async function getAssetFreshness(): Promise<AssetFreshnessResponse> {
  const res = await fetch("/api/assets/freshness", { cache: "no-store" });
  return readJSON<AssetFreshnessResponse>(res);
}
