"use client";
import { Suspense } from "react";

import { WorkspaceShell } from "@/components/workspace-shell";

export default function Home() {
  return (
    <Suspense fallback={null}>
      <WorkspaceShell />
    </Suspense>
  );
}
