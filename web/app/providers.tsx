"use client";

import { Provider } from "jotai";
import { ReactNode } from "react";

export function AppProviders({ children }: { children: ReactNode }) {
  return <Provider>{children}</Provider>;
}