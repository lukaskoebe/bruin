"use client";

import AnsiToHtml from "ansi-to-html";
import { useAtom, useSetAtom } from "jotai";
import { useEffect, useMemo, useState } from "react";
import useSWRMutation from "swr/mutation";

import { assetResultsAtom, changedAssetIdsAtom } from "@/lib/atoms";
import {
  inspectAsset,
  materializeAsset,
  materializePipelineStream,
} from "@/lib/api";
import { AssetInspectResponse } from "@/lib/types";

export function useAssetResults() {
  const [results, setResults] = useAtom(assetResultsAtom);
  const setChangedAssetIds = useSetAtom(changedAssetIdsAtom);
  const [pipelineMaterializeLoading, setPipelineMaterializeLoading] =
    useState(false);
  const {
    inspectResult,
    materializeOutput,
    materializeStatus,
    materializeError,
    resultTab,
  } = results;

  const { trigger: triggerInspect, isMutating: inspectLoading } =
    useSWRMutation(
      "asset.inspect",
      async (_key: string, { arg }: { arg: string }) =>
        inspectAsset(arg, { limit: 200 })
    );

  const { trigger: triggerMaterialize, isMutating: materializeLoading } =
    useSWRMutation(
      "asset.materialize",
      async (_key: string, { arg }: { arg: string }) => materializeAsset(arg)
    );

  useEffect(() => {
    setResults((previous) =>
      previous.inspectLoading === inspectLoading
        ? previous
        : { ...previous, inspectLoading }
    );
  }, [inspectLoading, setResults]);

  const effectiveMaterializeLoading =
    materializeLoading || pipelineMaterializeLoading;

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
      const result = await triggerInspect(assetId);
      setResults((previous) => ({
        ...previous,
        inspectResult: result,
      }));
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
      setResults((previous) => ({
        ...previous,
        inspectResult: failure,
      }));
      setResultTab("inspect");
      return failure;
    }
  };

  const runMaterializeForAsset = async (
    assetId: string,
    refresh?: () => Promise<void> | void
  ) => {
    try {
      const result = await triggerMaterialize(assetId);
      setResults((previous) => ({
        ...previous,
        materializeOutput: result.output ?? "",
        materializeStatus: result.status,
        materializeError: result.error ?? "",
      }));

      // Mark the materialized asset (and its downstreams) as changed
      // so preview inspections are refreshed.
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
        // Fallback: at minimum mark the materialized asset itself.
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
        materializeOutput: String(error),
        materializeStatus: "error",
        materializeError: String(error),
      }));
      setResultTab("materialize");
      return null;
    } finally {
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
      inspectResult: null,
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
