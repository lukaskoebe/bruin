"use client";

import { useState } from "react";

export type StoredNodePositions = Record<string, { x: number; y: number }>;

export function usePersistedNodePositions(): [
  StoredNodePositions,
  React.Dispatch<React.SetStateAction<StoredNodePositions>>,
] {
  const [storedNodePositions, setStoredNodePositions] =
    useState<StoredNodePositions>({});

  return [storedNodePositions, setStoredNodePositions];
}
