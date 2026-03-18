import {
  Navigate,
  Outlet,
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
  useNavigate,
} from "@tanstack/react-router";
import { Suspense } from "react";
import { WorkspaceShell } from "@/components/workspace-shell";
import { AppProviders } from "./providers";

function RootLayout() {
  return (
    <AppProviders>
      <Outlet />
    </AppProviders>
  );
}

const rootRoute = createRootRoute({
  component: RootLayout,
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: () => (
    <Suspense fallback={null}>
      <WorkspaceShell view="workspace" />
    </Suspense>
  ),
});

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings",
  component: () => <Navigate to="/settings/environments" />, 
});

const environmentsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings/environments",
  component: () => (
    <Suspense fallback={null}>
      <WorkspaceShell view="environments" />
    </Suspense>
  ),
});

const connectionsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings/connections",
  validateSearch: (search: Record<string, unknown>) => ({
    environment:
      typeof search.environment === "string" ? search.environment : undefined,
  }),
  component: ConnectionsRouteComponent,
});

function ConnectionsRouteComponent() {
  const search = connectionsRoute.useSearch();
  const navigate = useNavigate();

  return (
    <Suspense fallback={null}>
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
    </Suspense>
  );
}

const routeTree = rootRoute.addChildren([
  indexRoute,
  settingsRoute,
  environmentsRoute,
  connectionsRoute,
]);

export const router = createRouter({
  routeTree,
});

export function AppRouter() {
  return <RouterProvider router={router} />;
}
