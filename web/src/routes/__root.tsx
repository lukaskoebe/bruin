import { Outlet, createRootRoute } from "@tanstack/react-router";

import { AppProviders } from "@/src/providers";

export const Route = createRootRoute({
  component: RootComponent,
});

function RootComponent() {
  return (
    <AppProviders>
      <Outlet />
    </AppProviders>
  );
}