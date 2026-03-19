import { createFileRoute } from "@tanstack/react-router";

import { WorkspaceShell } from "@/components/workspace-shell";

export const Route = createFileRoute("/settings/environments")({
  component: SettingsEnvironmentsComponent,
});

function SettingsEnvironmentsComponent() {
  return <WorkspaceShell view="environments" />;
}