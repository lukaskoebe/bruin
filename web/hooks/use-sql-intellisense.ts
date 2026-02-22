"use client";

import { useEffect, useRef } from "react";
import type * as MonacoNS from "monaco-editor";

import {
  registerSQLProviders,
  resolveTableAtPosition,
} from "@/lib/monaco-sql-providers";
import { SchemaTable } from "@/lib/sql-schema";

/**
 * React hook that registers Monaco SQL completion / definition / hover
 * providers scoped to the given schema tables.
 *
 * Providers are re-registered whenever the `tables` reference changes.
 * Call this once from the component that owns the Monaco editor.
 */
export function useSQLIntellisense(
  monaco: typeof MonacoNS | null,
  editor: MonacoNS.editor.IStandaloneCodeEditor | null,
  tables: SchemaTable[],
  onGoToAsset?: (pipelineId: string, assetId: string) => void,
) {
  // Keep a stable ref to the latest callback so we don't re-register on
  // every render when the parent re-creates the function.
  const goToAssetRef = useRef(onGoToAsset);
  goToAssetRef.current = onGoToAsset;

  useEffect(() => {
    if (!monaco || tables.length === 0) {
      return;
    }

    const disposable = registerSQLProviders(monaco, tables);

    return () => {
      disposable.dispose();
    };
  }, [monaco, tables]);

  useEffect(() => {
    if (!editor || tables.length === 0) {
      return;
    }

    const disposable = editor.onMouseDown((event) => {
      if (!event.event.leftButton) {
        return;
      }

      if (!event.event.ctrlKey && !event.event.metaKey) {
        return;
      }

      const position = event.target.position;
      if (!position) {
        return;
      }

      const model = editor.getModel();
      if (!model) {
        return;
      }

      const table = resolveTableAtPosition(model, position, tables);
      if (!table?.assetId || !table.pipelineId) {
        return;
      }

      event.event.preventDefault();
      event.event.stopPropagation();
      goToAssetRef.current?.(table.pipelineId, table.assetId);
    });

    return () => {
      disposable.dispose();
    };
  }, [editor, tables]);
}
