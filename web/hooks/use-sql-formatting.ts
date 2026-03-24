"use client";

import { useCallback, useEffect, useMemo } from "react";
import type * as MonacoNS from "monaco-editor";

import { isSqlAssetType } from "@/lib/asset-types";
import { WebAsset } from "@/lib/types";

export function useSQLFormatting(
  asset: WebAsset | null,
  editor: MonacoNS.editor.IStandaloneCodeEditor | null,
  monaco: typeof MonacoNS | null,
) {
  const isSqlAsset = useMemo(() => {
    if (!asset) {
      return false;
    }

    return isSqlAssetType(asset.type) || asset.path.toLowerCase().endsWith(".sql");
  }, [asset]);

  const shortcutLabel = useMemo(() => "⌘ + ⇧ + I", []);

  const formatSQL = useCallback(() => {
    if (!editor || !isSqlAsset) {
      return;
    }

    void editor.getAction("editor.action.formatDocument")?.run();
  }, [editor, isSqlAsset]);

  useEffect(() => {
    if (!editor || !monaco || !isSqlAsset) {
      return;
    }

    const subscription = editor.onKeyDown((event) => {
      const ctrlOrCmd = event.ctrlKey || event.metaKey;
      if (!ctrlOrCmd || !event.shiftKey) {
        return;
      }

      if (event.keyCode !== monaco.KeyCode.KeyI) {
        return;
      }

      event.preventDefault();
      event.stopPropagation();
      formatSQL();
    });

    return () => {
      subscription.dispose();
    };
  }, [editor, formatSQL, isSqlAsset, monaco]);

  return {
    isSqlAsset,
    formatSQL,
    shortcutLabel,
  };
}
