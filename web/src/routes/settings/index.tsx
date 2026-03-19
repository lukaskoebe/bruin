import { Navigate, createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/settings/")({
  component: SettingsIndexComponent,
});

function SettingsIndexComponent() {
  return <Navigate to="/settings/environments" />;
}