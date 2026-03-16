"use client";

import { ChevronRight, Moon, Plus, Sun, Trash2, Workflow } from "lucide-react";
import { CSSProperties, ReactNode, useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { WorkspaceState } from "@/lib/types";

type Props = {
  workspace: WorkspaceState | null;
  activePipeline: string | null;
  selectedAsset: string | null;
  highlighted?: boolean;
  highlightStyle?: CSSProperties;
  onboardingContent?: ReactNode;
  theme: "light" | "dark";
  onToggleTheme: () => void;
  onCreatePipeline: () => void;
  onDeletePipeline: () => void;
  canDeletePipeline: boolean;
  deletePipelineLoading: boolean;
  onNavigateSelection: (pipelineId: string, assetId: string | null) => void;
};

export function WorkspaceSidebar({
  workspace,
  activePipeline,
  selectedAsset,
  highlighted = false,
  highlightStyle,
  onboardingContent,
  theme,
  onToggleTheme,
  onCreatePipeline,
  onDeletePipeline,
  canDeletePipeline,
  deletePipelineLoading,
  onNavigateSelection,
}: Props) {
  const [expandedPipelineIds, setExpandedPipelineIds] = useState<Set<string>>(
    () => new Set(activePipeline ? [activePipeline] : [])
  );

  useEffect(() => {
    if (!activePipeline) {
      return;
    }

    setExpandedPipelineIds((previous) => {
      if (previous.has(activePipeline)) {
        return previous;
      }

      const next = new Set(previous);
      next.add(activePipeline);
      return next;
    });
  }, [activePipeline]);

  return (
    <Sidebar
      className={`h-full w-full border-r transition-transform ${
        highlighted ? "ring-2 ring-primary/70 ring-inset" : ""
      }`}
      collapsible="none"
      style={highlightStyle}
    >
      <SidebarHeader className="border-b px-4 py-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm font-semibold">
            <Workflow className="size-4" />
            Pipelines
          </div>
          <div className="flex items-center gap-2">
            <Button
              size="icon-sm"
              type="button"
              variant="outline"
              onClick={onToggleTheme}
            >
              {theme === "dark" ? (
                <Sun className="size-3.5" />
              ) : (
                <Moon className="size-3.5" />
              )}
            </Button>
            <Button
              size="icon-sm"
              type="button"
              variant="outline"
              disabled={!canDeletePipeline || deletePipelineLoading}
              onClick={onDeletePipeline}
            >
              <Trash2 className="size-3.5" />
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={onCreatePipeline}
              type="button"
            >
              <Plus className="mr-1 inline size-3" />
              New
            </Button>
          </div>
        </div>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Workspace</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {workspace?.pipelines.map((item) => {
                const isActive = item.id === activePipeline;
                const isExpanded = expandedPipelineIds.has(item.id);

                return (
                  <SidebarMenuItem key={item.id}>
                    <SidebarMenuButton
                      isActive={isActive}
                      onClick={() => {
                        if (isActive) {
                          setExpandedPipelineIds((previous) => {
                            const next = new Set(previous);
                            if (next.has(item.id)) {
                              next.delete(item.id);
                            } else {
                              next.add(item.id);
                            }
                            return next;
                          });
                          return;
                        }

                        setExpandedPipelineIds((previous) => {
                          const next = new Set(previous);
                          next.add(item.id);
                          return next;
                        });
                        onNavigateSelection(
                          item.id,
                          item.assets[0]?.id ?? null
                        );
                      }}
                      type="button"
                    >
                      <ChevronRight
                        className={`size-3 transition-transform ${
                          isExpanded ? "rotate-90" : ""
                        }`}
                      />
                      <span>{item.name}</span>
                    </SidebarMenuButton>

                    {isExpanded && (
                      <SidebarMenu className="pl-3">
                        {item.assets.map((asset) => (
                          <SidebarMenuItem key={asset.id}>
                            <SidebarMenuButton
                              isActive={asset.id === selectedAsset}
                              onClick={() =>
                                onNavigateSelection(item.id, asset.id)
                              }
                              size="sm"
                              type="button"
                            >
                              <span>{asset.name}</span>
                            </SidebarMenuButton>
                          </SidebarMenuItem>
                        ))}
                      </SidebarMenu>
                    )}
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        {onboardingContent && (
          <SidebarGroup>
            <SidebarGroupLabel>Onboarding</SidebarGroupLabel>
            <SidebarGroupContent>{onboardingContent}</SidebarGroupContent>
          </SidebarGroup>
        )}
      </SidebarContent>
    </Sidebar>
  );
}
