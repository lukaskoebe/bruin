"use client";

import { useEffect, useRef } from "react";
import { Play } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Spinner } from "@/components/ui/spinner";
import { VirtualDataTable } from "@/components/virtual-data-table";
import { AssetInspectResponse } from "@/lib/types";

type Props = {
  inspectResult: AssetInspectResponse | null;
  inspectLoading: boolean;
  materializeLoading: boolean;
  pipelineMaterializeLoading?: boolean;
  hasInspectData: boolean;
  hasMaterializeData: boolean;
  effectiveResultTab: "inspect" | "materialize";
  materializeStatus: "ok" | "error" | null;
  materializeError: string;
  materializeOutputHtml: string;
  onResultTabChange: (value: "inspect" | "materialize") => void;
};

export function WorkspaceResultsPanel({
  inspectResult,
  inspectLoading,
  materializeLoading,
  pipelineMaterializeLoading = false,
  hasInspectData,
  hasMaterializeData,
  effectiveResultTab,
  materializeStatus,
  materializeError,
  materializeOutputHtml,
  onResultTabChange,
}: Props) {
  const materializeOutputRef = useRef<HTMLPreElement | null>(null);
  const inspectErrorDetails = extractInspectErrorDetails(
    inspectResult?.raw_output
  );

  useEffect(() => {
    const element = materializeOutputRef.current;
    if (!element) {
      return;
    }

    element.scrollTop = element.scrollHeight;
  }, [materializeOutputHtml, materializeLoading, effectiveResultTab]);

  return (
    <div className="flex h-full min-h-0 flex-col border-t bg-muted/20">
      <Tabs
        className="flex min-h-0 flex-1 flex-col"
        onValueChange={(value) =>
          onResultTabChange(value as "inspect" | "materialize")
        }
        value={effectiveResultTab}
      >
        <div className="flex items-center justify-between border-b px-3 py-2">
          <TabsList>
            <TabsTrigger disabled={!hasInspectData} value="inspect">
              Inspect
            </TabsTrigger>
            <TabsTrigger disabled={!hasMaterializeData} value="materialize">
              Materialize
            </TabsTrigger>
          </TabsList>

          <div className="text-[11px] opacity-70">
            {effectiveResultTab === "inspect"
              ? `${inspectResult?.rows.length ?? 0} rows`
              : "CLI output"}
          </div>
        </div>

        <TabsContent className="min-h-0 flex-1 p-2" value="inspect">
          {inspectLoading ? (
            <LoadingState label="Inspecting asset..." />
          ) : inspectResult?.error ? (
            <div className="flex h-full min-h-0 flex-col gap-2">
              <div className="rounded border border-destructive/40 bg-destructive/10 p-2 text-xs text-destructive">
                {inspectResult.error}
              </div>
              {inspectErrorDetails && (
                <pre className="min-h-0 flex-1 overflow-auto whitespace-pre-wrap rounded border border-destructive/30 bg-destructive/5 p-2 font-mono text-[11px]">
                  {inspectErrorDetails}
                </pre>
              )}
            </div>
          ) : (
            <VirtualDataTable
              columns={inspectResult?.columns ?? []}
              rows={inspectResult?.rows ?? []}
              height={200}
            />
          )}
        </TabsContent>

        <TabsContent className="min-h-0 flex-1 p-2" value="materialize">
          <div className="flex h-full min-h-0 flex-col gap-2">
            {materializeLoading && pipelineMaterializeLoading && (
              <div className="flex items-center gap-2 rounded border border-primary/30 bg-primary/5 px-2 py-1 text-xs text-primary">
                <Play className="size-3.5 animate-pulse fill-current" />
                Running pipeline...
              </div>
            )}
            {materializeStatus === "error" && (
              <div className="rounded border border-destructive/40 bg-destructive/10 px-2 py-1 text-xs text-destructive">
                Materialization failed
                {materializeError ? `: ${materializeError}` : ""}
              </div>
            )}
            <div
              className={`min-h-0 flex flex-1 flex-col overflow-hidden rounded border ${
                materializeStatus === "error"
                  ? "border-destructive/40 bg-destructive/5"
                  : "bg-background"
              }`}
            >
              <pre
                ref={materializeOutputRef}
                className="min-h-0 flex-1 overflow-auto whitespace-pre-wrap p-2 font-mono text-[11px]"
                dangerouslySetInnerHTML={{ __html: materializeOutputHtml }}
              />
              {materializeLoading && (
                <div className="flex items-center gap-2 border-t bg-muted/40 px-2 py-1 font-mono text-[11px] text-muted-foreground">
                  <Spinner className="size-3.5" />
                  <span>
                    {pipelineMaterializeLoading
                      ? "Waiting for pipeline output..."
                      : "Waiting for asset output..."}
                  </span>
                </div>
              )}
            </div>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}

function LoadingState({ label }: { label: string }) {
  return (
    <div className="flex h-full min-h-0 items-center justify-center rounded border bg-background">
      <div className="flex items-center gap-2 text-xs opacity-80">
        <Spinner className="size-4" />
        <span>{label}</span>
      </div>
    </div>
  );
}

function extractInspectErrorDetails(rawOutput: string | undefined): string {
  const trimmed = (rawOutput ?? "").trim();
  if (!trimmed) {
    return "";
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
    return trimmed;
  }

  return trimmed;
}
