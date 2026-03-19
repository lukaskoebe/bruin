"use client";

import { ReactNode } from "react";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";

export function WorkspaceSettingsSplitView({
  content,
  pane,
}: {
  content: ReactNode;
  pane: ReactNode;
}) {
  return (
    <PanelGroup
      direction="horizontal"
      className="h-full min-h-0 overflow-hidden"
    >
      <Panel defaultSize={50} minSize={30}>
        {content}
      </Panel>

      <PanelResizeHandle className="w-px bg-border" />

      {pane}
    </PanelGroup>
  );
}