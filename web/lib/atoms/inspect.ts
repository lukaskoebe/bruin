import { atom } from "jotai";

import { AssetInspectResponse } from "@/lib/types";

export type AssetInspectEntry = {
  result: AssetInspectResponse;
  fetchedLimit: number;
};

export type AssetInspectState = {
  byAssetId: Record<string, AssetInspectEntry>;
  loadingByAssetId: Record<string, boolean>;
  requestedLimitsByAssetId: Record<string, number>;
};

export const assetInspectAtom = atom<AssetInspectState>({
  byAssetId: {},
  loadingByAssetId: {},
  requestedLimitsByAssetId: {},
});
