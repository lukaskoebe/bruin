import {
  AssetFreshnessResponse,
  AssetInspectResponse,
  InferColumnsResponse,
  MaterializeResponse,
  WebColumn,
  PipelineMaterializationResponse,
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

export async function createAsset(
  pipelineId: string,
  input: { name?: string; type?: string; path?: string; content?: string },
) {
  const res = await fetch(`/api/pipelines/${pipelineId}/assets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  return readJSON<{ status: string; asset_id?: string; asset_path?: string }>(res);
}

export async function updateAsset(
  pipelineId: string,
  assetId: string,
  input: {
    content?: string;
    materialization_type?: string;
    meta?: Record<string, string>;
  },
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
  options?: { limit?: number; environment?: string },
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

function normalizeInspectResponse(response: AssetInspectResponse): AssetInspectResponse {
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
    const parsed = JSON.parse(trimmed) as { error?: unknown; message?: unknown };
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

export async function materializeAsset(assetId: string) {
  const res = await fetch(`/api/assets/${assetId}/materialize`, {
    method: "POST",
  });

  const text = await res.text();
  let parsed: MaterializeResponse | null = null;

  try {
    parsed = JSON.parse(text) as MaterializeResponse;
  } catch {
    parsed = null;
  }

  if (parsed) {
    return parsed;
  }

  throw new Error(text || `Request failed: ${res.status}`);
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

export async function updateAssetColumns(assetId: string, columns: WebColumn[]) {
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