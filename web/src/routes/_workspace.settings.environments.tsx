import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";

import { WorkspaceConfigContent } from "@/components/workspace-config-content";
import { WorkspaceEnvironmentPane } from "@/components/workspace-environment-pane";
import { useWorkspaceSettingsLayout } from "@/components/workspace-settings-layout";
import { WorkspaceSettingsSplitView } from "@/components/workspace-settings-split-view";
import { useResolvedWorkspaceEnvironment } from "@/hooks/use-workspace-config-selection";

export const Route = createFileRoute("/_workspace/settings/environments")({
  component: WorkspaceSettingsEnvironmentsRouteComponent,
});

function WorkspaceSettingsEnvironmentsRouteComponent() {
  return <WorkspaceEnvironmentsRoutePage />;
}

export function WorkspaceEnvironmentsRoutePage() {
  const {
    fallbackConfigEnvironment,
    handleCloneWorkspaceEnvironment,
    handleCreateWorkspaceEnvironment,
    handleDeleteWorkspaceEnvironment,
    handleUpdateWorkspaceEnvironment,
    loadWorkspaceConfig,
    normalizedConfigEnvironments,
    workspaceConfig,
    workspaceConfigBusy,
    workspaceConfigLoading,
    workspaceConfigStatusMessage,
    workspaceConfigStatusTone,
  } = useWorkspaceSettingsLayout();
  const [selectedEnvironmentEditorName, setSelectedEnvironmentEditorName] =
    useState<string | null>(null);
  const [environmentEditorMode, setEnvironmentEditorMode] = useState<
    "edit" | "create" | "clone"
  >("edit");
  const { resolvedEnvironmentName } = useResolvedWorkspaceEnvironment({
    defaultEnvironment: fallbackConfigEnvironment ?? undefined,
    environments: normalizedConfigEnvironments,
    selectedEnvironmentName: selectedEnvironmentEditorName,
    onSelectedEnvironmentChange: setSelectedEnvironmentEditorName,
  });

  return (
    <WorkspaceSettingsSplitView
      content={
        <WorkspaceConfigContent
          view="environments"
          configPath={workspaceConfig?.path ?? ".bruin.yml"}
          defaultEnvironment={workspaceConfig?.default_environment}
          selectedEnvironmentName={resolvedEnvironmentName}
          environments={normalizedConfigEnvironments}
          selectedConnectionName={null}
          loading={workspaceConfigLoading}
          onSelectEnvironment={(name) => {
            setSelectedEnvironmentEditorName(name);
            setEnvironmentEditorMode("edit");
          }}
          onSelectConnection={() => undefined}
          onCreateEnvironment={() => setEnvironmentEditorMode("create")}
          onCloneEnvironment={() => setEnvironmentEditorMode("clone")}
          onCreateConnection={() => undefined}
        />
      }
      pane={
        <WorkspaceEnvironmentPane
          configPath={workspaceConfig?.path ?? ".bruin.yml"}
          defaultEnvironment={workspaceConfig?.default_environment}
          selectedEnvironment={resolvedEnvironmentName}
          environments={normalizedConfigEnvironments}
          loading={workspaceConfigLoading}
          busy={workspaceConfigBusy}
          parseError={workspaceConfig?.parse_error}
          statusMessage={workspaceConfigStatusMessage}
          statusTone={workspaceConfigStatusTone}
          mode={environmentEditorMode}
          onModeChange={setEnvironmentEditorMode}
          onSelectedEnvironmentChange={setSelectedEnvironmentEditorName}
          onReload={() => void loadWorkspaceConfig()}
          onCreateEnvironment={handleCreateWorkspaceEnvironment}
          onUpdateEnvironment={handleUpdateWorkspaceEnvironment}
          onCloneEnvironment={handleCloneWorkspaceEnvironment}
          onDeleteEnvironment={handleDeleteWorkspaceEnvironment}
        />
      }
    />
  );
}
