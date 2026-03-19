import { createFileRoute } from "@tanstack/react-router";

import { WorkspacePage } from "@/components/workspace-page";

export const Route = createFileRoute("/_workspace/")({
  validateSearch: (search: Record<string, unknown>) => ({
    pipeline:
      typeof search.pipeline === "string" ? search.pipeline : undefined,
    asset: typeof search.asset === "string" ? search.asset : undefined,
  }),
  component: WorkspaceIndexRouteComponent,
});

function WorkspaceIndexRouteComponent() {
  return <WorkspacePage />;
}