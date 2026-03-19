import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useState } from "react";

import { WorkspaceConfigContent } from "@/components/workspace-config-content";
import { WorkspaceConnectionPane } from "@/components/workspace-connection-pane";
import { useWorkspaceSettingsLayout } from "@/components/workspace-settings-layout";
import { WorkspaceSettingsSplitView } from "@/components/workspace-settings-split-view";
import {
  useResolvedWorkspaceConnection,
  useResolvedWorkspaceEnvironment,
} from "@/hooks/use-workspace-config-selection";

export const Route = createFileRoute("/_workspace/settings/connections")({
  validateSearch: (search: Record<string, unknown>) => ({
    environment:
      typeof search.environment === "string" ? search.environment : undefined,
  }),
  component: WorkspaceSettingsConnectionsRouteComponent,
});

function WorkspaceSettingsConnectionsRouteComponent() {
  const search = Route.useSearch();
  const navigate = useNavigate();

  return (
    <WorkspaceConnectionsRoutePage
      selectedConfigEnvironment={search.environment}
      onSelectedConfigEnvironmentChange={(environment) =>
        navigate({
          to: "/settings/connections",
          search: { environment: environment ?? undefined },
          replace: true,
        })
      }
    />
  );
}

function WorkspaceConnectionsRoutePage({
  selectedConfigEnvironment,
  onSelectedConfigEnvironmentChange,
}: {
  selectedConfigEnvironment?: string;
  onSelectedConfigEnvironmentChange?: (environment: string | null) => void;
}) {
  const {
    fallbackConfigEnvironment,
    handleCreateWorkspaceConnection,
    handleDeleteWorkspaceConnection,
    handleUpdateWorkspaceConnection,
    loadWorkspaceConfig,
    normalizedConfigEnvironments,
    workspaceConfig,
    workspaceConfigBusy,
    workspaceConfigLoading,
    workspaceConfigStatusMessage,
    workspaceConfigStatusTone,
  } = useWorkspaceSettingsLayout();
  const [selectedConnectionName, setSelectedConnectionName] = useState<
    string | null
  >(null);
  const [connectionEditorMode, setConnectionEditorMode] = useState<
    "edit" | "create"
  >("edit");
  const { activeEnvironment, resolvedEnvironmentName } =
    useResolvedWorkspaceEnvironment({
      defaultEnvironment: fallbackConfigEnvironment ?? undefined,
      environments: normalizedConfigEnvironments,
      selectedEnvironmentName: selectedConfigEnvironment,
      onSelectedEnvironmentChange: (name) =>
        onSelectedConfigEnvironmentChange?.(name),
    });
  const { resolvedConnectionName } = useResolvedWorkspaceConnection({
    activeEnvironment,
    selectedConnectionName,
    onSelectedConnectionChange: setSelectedConnectionName,
  });

  return (
    <WorkspaceSettingsSplitView
      content={
        <WorkspaceConfigContent
          view="connections"
          configPath={workspaceConfig?.path ?? ".bruin.yml"}
          defaultEnvironment={workspaceConfig?.default_environment}
          selectedEnvironmentName={resolvedEnvironmentName}
          environments={normalizedConfigEnvironments}
          selectedConnectionName={resolvedConnectionName}
          loading={workspaceConfigLoading}
          onSelectEnvironment={(name) => {
            onSelectedConfigEnvironmentChange?.(name);
            setConnectionEditorMode("edit");
          }}
          onSelectConnection={(name) => {
            setSelectedConnectionName(name);
            setConnectionEditorMode("edit");
          }}
          onCreateEnvironment={() => undefined}
          onCloneEnvironment={() => undefined}
          onCreateConnection={() => setConnectionEditorMode("create")}
        />
      }
      pane={
        <WorkspaceConnectionPane
          configPath={workspaceConfig?.path ?? ".bruin.yml"}
          defaultEnvironment={workspaceConfig?.default_environment}
          selectedEnvironment={resolvedEnvironmentName}
          selectedConnectionName={resolvedConnectionName}
          environments={normalizedConfigEnvironments}
          connectionTypes={workspaceConfig?.connection_types ?? []}
          loading={workspaceConfigLoading}
          busy={workspaceConfigBusy}
          parseError={workspaceConfig?.parse_error}
          statusMessage={workspaceConfigStatusMessage}
          statusTone={workspaceConfigStatusTone}
          mode={connectionEditorMode}
          onModeChange={setConnectionEditorMode}
          onSelectedEnvironmentChange={(name) =>
            onSelectedConfigEnvironmentChange?.(name)
          }
          onSelectedConnectionChange={setSelectedConnectionName}
          onReload={() => void loadWorkspaceConfig()}
          onCreateConnection={handleCreateWorkspaceConnection}
          onUpdateConnection={handleUpdateWorkspaceConnection}
          onDeleteConnection={handleDeleteWorkspaceConnection}
        />
      }
    />
  );
}
