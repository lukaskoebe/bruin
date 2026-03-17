import { atom } from "jotai";

import { selectedAssetDataAtom } from "./selection";

export type EditorDraftState = {
  assetId: string | null;
  content: string;
};

export const editorDraftAtom = atom<EditorDraftState>({
  assetId: null,
  content: "",
});

export const editorValueAtom = atom<string>((get) => {
  const asset = get(selectedAssetDataAtom);
  const editorDraft = get(editorDraftAtom);

  if (asset && editorDraft.assetId === asset.id) {
    return editorDraft.content;
  }

  return asset?.content ?? "";
});

export type AssetEditorTab = "configuration" | "checks" | "visualization";

export const assetEditorTabAtom = atom<AssetEditorTab>("configuration");