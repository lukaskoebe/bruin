"use client";

import AnsiToHtml from "ansi-to-html";
import { useAtom, useAtomValue, useSetAtom } from "jotai";
import { useEffect, useMemo, useState } from "react";

import {
  assetResultsAtom,
  changedAssetIdsAtom,
  resolvedSelectedAssetAtom,
} from "@/lib/atoms";
import { useAssetInspect } from "@/hooks/use-asset-inspect";
import { materializeAssetStream, materializePipelineStream } from "@/lib/api";
import { AssetInspectResponse } from "@/lib/types";

export function useAssetResults() {
  const [results, setResults] = useAtom(assetResultsAtom);
  const setChangedAssetIds = useSetAtom(changedAssetIdsAtom);
  const [pipelineMaterializeLoading, setPipelineMaterializeLoading] =
    useState(false);
  const [assetMaterializeLoading, setAssetMaterializeLoading] = useState(false);
  const selectedAssetId = useAtomValue(resolvedSelectedAssetAtom);
  const { inspectAssetById, inspectByAssetId, inspectLoadingByAssetId } =
    useAssetInspect();
  const {
    materializeOutput,
    materializeStatus,
    materializeError,
    resultTab,
  } = results;

  const inspectResult = selectedAssetId
    ? inspectByAssetId[selectedAssetId] ?? null
    : null;
  const inspectLoading = selectedAssetId
    ? inspectLoadingByAssetId[selectedAssetId] ?? false
    : false;

  const effectiveMaterializeLoading =
    assetMaterializeLoading || pipelineMaterializeLoading;

  useEffect(() => {
    setResults((previous) =>
      previous.materializeLoading === effectiveMaterializeLoading
        ? previous
        : { ...previous, materializeLoading: effectiveMaterializeLoading }
    );
  }, [effectiveMaterializeLoading, setResults]);

  const setResultTab = (tab: "inspect" | "materialize") => {
    setResults((previous) => ({
      ...previous,
      resultTab: tab,
    }));
  };

  const hasInspectData = Boolean(
    inspectResult && (inspectResult.rows.length > 0 || inspectResult.error)
  );
  const hasMaterializeData =
    materializeOutput.trim().length > 0 ||
    materializeError.trim().length > 0 ||
    effectiveMaterializeLoading;
  const hasResultData =
    hasInspectData ||
    hasMaterializeData ||
    inspectLoading ||
    effectiveMaterializeLoading;

  const ansiConverter = useMemo(() => new AnsiToHtml({ escapeXML: true }), []);
  const materializeOutputHtml = useMemo(() => {
    const normalized = materializeOutput.replace(/\r\n/g, "\n");
    return ansiConverter.toHtml(normalized);
  }, [ansiConverter, materializeOutput]);

  const effectiveResultTab = useMemo(() => {
    if (resultTab === "inspect" && !hasInspectData && hasMaterializeData) {
      return "materialize" as const;
    }

    if (resultTab === "materialize" && !hasMaterializeData && hasInspectData) {
      return "inspect" as const;
    }

    return resultTab;
  }, [hasInspectData, hasMaterializeData, resultTab]);

  const runInspectForAsset = async (assetId: string) => {
    try {
      const result = await inspectAssetById(assetId, { force: true, limit: 200 });
      if (result.rows.length > 0 || result.error) {
        setResultTab("inspect");
      }
      return result;
    } catch (error) {
      const failure: AssetInspectResponse = {
        status: "error",
        columns: [],
        rows: [],
        raw_output: "",
        error: String(error),
      };
      setResultTab("inspect");
      return failure;
    }
  };

  const runMaterializeForAsset = async (
    assetId: string,
    refresh?: () => Promise<void> | void
  ) => {
    setAssetMaterializeLoading(true);
    setResults((previous) => ({
      ...previous,
      materializeOutput: "",
      materializeStatus: null,
      materializeError: "",
      resultTab: "materialize",
    }));

    try {
      const result = await materializeAssetStream(assetId, {
        onChunk: (chunk) => {
          setResults((previous) => ({
            ...previous,
            materializeOutput: previous.materializeOutput + chunk,
            resultTab: "materialize",
          }));
        },
      });
      setResults((previous) => ({
        ...previous,
        materializeOutput: result.output ?? previous.materializeOutput,
        materializeStatus: result.status ?? "error",
        materializeError: result.error ?? "",
      }));

      const affectedIds = result.changed_asset_ids;
      if (affectedIds && affectedIds.length > 0) {
        setChangedAssetIds((prev: Set<string>) => {
          const next = new Set(prev);
          for (const id of affectedIds) {
            next.add(id);
          }
          return next;
        });
      } else {
        setChangedAssetIds((prev: Set<string>) => new Set([...prev, assetId]));
      }

      if (
        (result.output ?? "").trim().length > 0 ||
        (result.error ?? "").trim().length > 0
      ) {
        setResultTab("materialize");
      }
      return result;
    } catch (error) {
      setResults((previous) => ({
        ...previous,
        materializeOutput:
          previous.materializeOutput +
          (previous.materializeOutput ? "\n" : "") +
          String(error),
        materializeStatus: "error",
        materializeError: String(error),
      }));
      setResultTab("materialize");
      return null;
    } finally {
      setAssetMaterializeLoading(false);
      if (refresh) {
        await refresh();
      }
    }
  };

  const runMaterializePipeline = async (
    pipelineId: string,
    refresh?: () => Promise<void> | void
  ) => {
    setPipelineMaterializeLoading(true);
    setResults((previous) => ({
      ...previous,
      materializeOutput: "",
      materializeStatus: null,
      materializeError: "",
      resultTab: "materialize",
    }));

    try {
      const result = await materializePipelineStream(pipelineId, {
        onChunk: (chunk) => {
          setResults((previous) => ({
            ...previous,
            materializeOutput: previous.materializeOutput + chunk,
            resultTab: "materialize",
          }));
        },
      });

      setResults((previous) => ({
        ...previous,
        materializeOutput: result.output ?? previous.materializeOutput,
        materializeStatus: result.status ?? "error",
        materializeError: result.error ?? "",
        resultTab: "materialize",
      }));

      const affectedIds = result.changed_asset_ids ?? [];
      if (affectedIds.length > 0) {
        setChangedAssetIds((prev: Set<string>) => {
          const next = new Set(prev);
          for (const id of affectedIds) {
            next.add(id);
          }
          return next;
        });
      }

      return result;
    } catch (error) {
      setResults((previous) => ({
        ...previous,
        materializeStatus: "error",
        materializeError: String(error),
        materializeOutput:
          previous.materializeOutput +
          (previous.materializeOutput ? "\n" : "") +
          String(error),
        resultTab: "materialize",
      }));
      return null;
    } finally {
      setPipelineMaterializeLoading(false);
      if (refresh) {
        await refresh();
      }
    }
  };

  const setMaterializeBatchResult = (
    output: string,
    status: "ok" | "error",
    errorMessage: string
  ) => {
    setResults((previous) => ({
      ...previous,
      materializeOutput: output,
      materializeStatus: status,
      materializeError: errorMessage,
      resultTab: "materialize",
    }));
  };

  const clearResultsAfterDelete = () => {
    setResults((previous) => ({
      ...previous,
      materializeOutput: "",
      materializeError: "",
    }));
  };

  return {
    inspectResult,
    inspectLoading,
    materializeLoading: effectiveMaterializeLoading,
    pipelineMaterializeLoading,
    materializeStatus,
    materializeError,
    hasInspectData,
    hasMaterializeData,
    hasResultData,
    effectiveResultTab,
    materializeOutputHtml,
    setResultTab,
    runInspectForAsset,
    runMaterializeForAsset,
    runMaterializePipeline,
    setMaterializeBatchResult,
    clearResultsAfterDelete,
  };
}
