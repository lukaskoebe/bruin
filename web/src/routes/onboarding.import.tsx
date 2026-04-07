import { createFileRoute, redirect } from "@tanstack/react-router";

import { OnboardingRoutePage } from "@/components/onboarding-route-page";
import { getOnboardingState } from "@/lib/api";

export const Route = createFileRoute("/onboarding/import")({
  beforeLoad: async () => {
    const state = await getOnboardingState();
    if (!state.active) {
      throw redirect({ to: "/" });
    }
    return { onboardingState: state };
  },
  component: OnboardingImportRouteComponent,
});

function OnboardingImportRouteComponent() {
  const { onboardingState } = Route.useRouteContext();
  return <OnboardingRoutePage onboardingState={onboardingState} />;
}
