import { createFileRoute, useNavigate } from "@tanstack/react-router";

import { WorkspaceShell } from "@/components/workspace-shell";

export const Route = createFileRoute("/settings/connections")({
  validateSearch: (search: Record<string, unknown>) => ({
    environment:
      typeof search.environment === "string" ? search.environment : undefined,
  }),
  component: SettingsConnectionsComponent,
});

function SettingsConnectionsComponent() {
  const search = Route.useSearch();
  const navigate = useNavigate();

  return (
    <WorkspaceShell
      view="connections"
      selectedConfigEnvironment={search.environment}
      onSelectedConfigEnvironmentChange={(environment) =>
        navigate({
          to: "/settings/connections",
          search: environment ? { environment } : {},
          replace: true,
        })
      }
    />
  );
}