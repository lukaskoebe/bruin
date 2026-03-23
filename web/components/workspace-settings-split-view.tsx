"use client";

import { ReactNode } from "react";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";

import { useIsMobile } from "@/hooks/use-mobile";

export function WorkspaceSettingsSplitView({
  content,
  pane,
}: {
  content: ReactNode;
  pane: ReactNode;
}) {
  const isMobile = useIsMobile();

  return (
    <PanelGroup
      direction={isMobile ? "vertical" : "horizontal"}
      className="h-full min-h-0 overflow-hidden"
    >
      <Panel defaultSize={isMobile ? 52 : 50} minSize={isMobile ? 38 : 30}>
        {content}
      </Panel>

      <PanelResizeHandle className={isMobile ? "h-px bg-border" : "w-px bg-border"} />

      {pane}
    </PanelGroup>
  );
}
