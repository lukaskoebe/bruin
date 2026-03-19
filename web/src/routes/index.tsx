import { createFileRoute } from "@tanstack/react-router";

import { WorkspaceShell } from "@/components/workspace-shell";

export const Route = createFileRoute("/")({
  component: IndexComponent,
});

function IndexComponent() {
  return <WorkspaceShell view="workspace" />;
}