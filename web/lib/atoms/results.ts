import { atom } from "jotai";

import { AssetInspectResponse } from "@/lib/types";

export type AssetResultTab = "inspect" | "materialize";

export type AssetResultsState = {
  inspectResult: AssetInspectResponse | null;
  inspectLoading: boolean;
  materializeOutput: string;
  materializeStatus: "ok" | "error" | null;
  materializeError: string;
  materializeLoading: boolean;
  resultTab: AssetResultTab;
};

export const assetResultsAtom = atom<AssetResultsState>({
  inspectResult: null,
  inspectLoading: false,
  materializeOutput: "",
  materializeStatus: null,
  materializeError: "",
  materializeLoading: false,
  resultTab: "inspect",
});