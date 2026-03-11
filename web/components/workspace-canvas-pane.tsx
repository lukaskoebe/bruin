"use client";

import { CSSProperties, MutableRefObject } from "react";
import { RefreshCcw } from "lucide-react";
import {
  Background,
  Controls,
  Edge,
  Node,
  NodeTypes,
  ReactFlow,
  ReactFlowInstance,
} from "reactflow";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";

import { WorkspaceResultsPanel } from "@/components/workspace-results-panel";
import { Button } from "@/components/ui/button";
import { AssetInspectResponse } from "@/lib/types";

type WorkspaceCanvasPaneProps = {
  highlighted: boolean;
  highlightStyle?: CSSProperties;
  hasResultData: boolean;
  canvasContainerRef: MutableRefObject<HTMLDivElement | null>;
  nodes: Node[];
  edges: Edge[];
  nodeTypes: NodeTypes;
  inspectResult: AssetInspectResponse | null;
  inspectLoading: boolean;
  materializeLoading: boolean;
  hasInspectData: boolean;
  hasMaterializeData: boolean;
  effectiveResultTab: "inspect" | "materialize";
  materializeStatus: "ok" | "error" | null;
  materializeError: string;
  materializeOutputHtml: string;
  onResultTabChange: (tab: "inspect" | "materialize") => void;
  onInit: (instance: ReactFlowInstance) => void;
  onNodesChange: Parameters<typeof ReactFlow>[0]["onNodesChange"];
  onEdgesChange: Parameters<typeof ReactFlow>[0]["onEdgesChange"];
  onNodeDragStop: Parameters<typeof ReactFlow>[0]["onNodeDragStop"];
  onPaneClick: Parameters<typeof ReactFlow>[0]["onPaneClick"];
  onPaneContextMenu: Parameters<typeof ReactFlow>[0]["onPaneContextMenu"];
  onNodeClick: Parameters<typeof ReactFlow>[0]["onNodeClick"];
  onRecomputeGraph: () => void;
};

export function WorkspaceCanvasPane({
  highlighted,
  highlightStyle,
  hasResultData,
  canvasContainerRef,
  nodes,
  edges,
  nodeTypes,
  inspectResult,
  inspectLoading,
  materializeLoading,
  hasInspectData,
  hasMaterializeData,
  effectiveResultTab,
  materializeStatus,
  materializeError,
  materializeOutputHtml,
  onResultTabChange,
  onInit,
  onNodesChange,
  onEdgesChange,
  onNodeDragStop,
  onPaneClick,
  onPaneContextMenu,
  onNodeClick,
  onRecomputeGraph,
}: WorkspaceCanvasPaneProps) {
  return (
    <Panel
      className={highlighted ? "ring-2 ring-primary/70 ring-inset" : ""}
      style={highlighted ? highlightStyle : undefined}
      defaultSize={50}
      minSize={30}
    >
      <PanelGroup direction="vertical">
        <Panel defaultSize={hasResultData ? 72 : 100} minSize={45}>
          <div className="relative h-full" ref={canvasContainerRef}>
            <div className="absolute right-3 top-3 z-10">
              <Button
                onClick={onRecomputeGraph}
                size="sm"
                type="button"
                variant="outline"
              >
                <RefreshCcw className="mr-2 size-3.5" />
                Recompute graph
              </Button>
            </div>
            <ReactFlow
              nodes={nodes}
              edges={edges}
              nodeTypes={nodeTypes}
              panActivationKeyCode={null}
              onInit={onInit}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onNodeDragStop={onNodeDragStop}
              onPaneClick={onPaneClick}
              onPaneContextMenu={onPaneContextMenu}
              onNodeClick={onNodeClick}
            >
              <Background />
              <Controls />
            </ReactFlow>
          </div>
        </Panel>

        {hasResultData && (
          <>
            <PanelResizeHandle className="h-px bg-border" />

            <Panel defaultSize={28} minSize={20}>
              <WorkspaceResultsPanel
                inspectResult={inspectResult}
                inspectLoading={inspectLoading}
                materializeLoading={materializeLoading}
                hasInspectData={hasInspectData}
                hasMaterializeData={hasMaterializeData}
                effectiveResultTab={effectiveResultTab}
                materializeStatus={materializeStatus}
                materializeError={materializeError}
                materializeOutputHtml={materializeOutputHtml}
                onResultTabChange={onResultTabChange}
              />
            </Panel>
          </>
        )}
      </PanelGroup>
    </Panel>
  );
}
