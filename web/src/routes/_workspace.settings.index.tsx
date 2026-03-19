import { createFileRoute } from "@tanstack/react-router";

import { WorkspaceEnvironmentsRoutePage } from "./_workspace.settings.environments";

export const Route = createFileRoute("/_workspace/settings/")({
  component: WorkspaceSettingsIndexRouteComponent,
});

function WorkspaceSettingsIndexRouteComponent() {
  return <WorkspaceEnvironmentsRoutePage />;
}
