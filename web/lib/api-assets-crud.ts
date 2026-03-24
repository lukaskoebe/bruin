import { fetchJSON, fetchJSONWithBody } from "@/lib/api-core";

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
