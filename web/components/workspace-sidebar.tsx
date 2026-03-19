"use client";

import { Link } from "@tanstack/react-router";
import {
  Cable,
  ChevronRight,
  Moon,
  Play,
  Plus,
  Settings2,
  Sun,
  Trash2,
  Workflow,
} from "lucide-react";
import { CSSProperties, useEffect, useState } from "react";

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
  currentView: "workspace" | "environments" | "connections";
  connectionsEnvironment?: string | null;
  activePipeline: string | null;
  selectedAsset: string | null;
  highlighted?: boolean;
  highlightStyle?: CSSProperties;
  theme: "light" | "dark";
  onToggleTheme: () => void;
  onCreatePipeline: () => void;
  onDeletePipeline: () => void;
  canDeletePipeline: boolean;
  deletePipelineLoading: boolean;
  onRunPipeline: () => void;
  canRunPipeline: boolean;
  runPipelineLoading: boolean;
  onOnboardingMountChange?: (element: HTMLDivElement | null) => void;
};

export function WorkspaceSidebar({
  workspace,
  currentView,
  connectionsEnvironment,
  activePipeline,
  selectedAsset,
  highlighted = false,
  highlightStyle,
  theme,
  onToggleTheme,
  onCreatePipeline,
  onDeletePipeline,
  canDeletePipeline,
  deletePipelineLoading,
  onRunPipeline,
  canRunPipeline,
  runPipelineLoading,
  onOnboardingMountChange,
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
              size="sm"
              type="button"
              variant={runPipelineLoading ? "default" : "outline"}
              disabled={!canRunPipeline || runPipelineLoading}
              onClick={onRunPipeline}
            >
              <Play
                className={`mr-1 inline size-3.5 ${
                  runPipelineLoading ? "animate-pulse fill-current" : ""
                }`}
              />
              {runPipelineLoading ? "Running..." : "Run"}
            </Button>
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
          <SidebarGroupLabel>Views</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton
                  asChild
                  isActive={currentView === "workspace"}
                >
                  <Link
                    to="/"
                    search={{
                      pipeline: activePipeline ?? undefined,
                      asset: selectedAsset ?? undefined,
                    }}
                    activeOptions={{ exact: true, includeSearch: false }}
                  >
                    <Workflow className="size-4" />
                    <span>Workspace</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
              <SidebarMenuItem>
                <SidebarMenuButton
                  asChild
                  isActive={currentView === "environments"}
                >
                  <Link
                    to="/settings/environments"
                    activeOptions={{ exact: true, includeSearch: false }}
                  >
                    <Settings2 className="size-4" />
                    <span>Environments</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
              <SidebarMenuItem>
                <SidebarMenuButton
                  asChild
                  isActive={currentView === "connections"}
                >
                  <Link
                    to="/settings/connections"
                    search={{
                      environment: connectionsEnvironment ?? undefined,
                    }}
                    activeOptions={{ exact: true, includeSearch: false }}
                  >
                    <Cable className="size-4" />
                    <span>Connections</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel>Workspace</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {workspace?.pipelines.map((item) => {
                const isActive = item.id === activePipeline;
                const isExpanded = expandedPipelineIds.has(item.id);

                return (
                  <SidebarMenuItem key={item.id}>
                    <SidebarMenuButton asChild isActive={isActive}>
                      <Link
                        to="/"
                        search={{
                          pipeline: item.id,
                          asset: item.assets[0]?.id ?? undefined,
                        }}
                        activeOptions={{ exact: true, includeSearch: false }}
                        onClick={() => {
                          setExpandedPipelineIds((previous) => {
                            const next = new Set(previous);
                            if (next.has(item.id)) {
                              next.delete(item.id);
                            } else {
                              next.add(item.id);
                            }
                            return next;
                          });
                        }}
                      >
                        <ChevronRight
                          onClick={(e) => {
                            e.stopPropagation();
                            e.preventDefault();

                            setExpandedPipelineIds((previous) => {
                              const next = new Set(previous);
                              if (next.has(item.id)) {
                                next.delete(item.id);
                              } else {
                                next.add(item.id);
                              }
                              return next;
                            });
                          }}
                          className={`size-3 transition-transform ${
                            isExpanded ? "rotate-90" : ""
                          }`}
                        />
                        <span>{item.name}</span>
                      </Link>
                    </SidebarMenuButton>

                    {isExpanded && (
                      <SidebarMenu className="pl-3">
                        {item.assets.map((asset) => (
                          <SidebarMenuItem key={asset.id}>
                            <SidebarMenuButton
                              asChild
                              isActive={asset.id === selectedAsset}
                              size="sm"
                            >
                              <Link
                                to="/"
                                search={{ pipeline: item.id, asset: asset.id }}
                                activeOptions={{
                                  exact: true,
                                  includeSearch: false,
                                }}
                              >
                                <span>{asset.name}</span>
                              </Link>
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

        {onOnboardingMountChange && (
          <SidebarGroup>
            <SidebarGroupLabel>Onboarding</SidebarGroupLabel>
            <SidebarGroupContent>
              <div ref={onOnboardingMountChange} />
            </SidebarGroupContent>
          </SidebarGroup>
        )}
      </SidebarContent>
    </Sidebar>
  );
}
