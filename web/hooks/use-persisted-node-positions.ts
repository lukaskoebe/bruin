"use client";

import { useEffect, useState } from "react";

export type StoredNodePositions = Record<string, { x: number; y: number }>;

export function usePersistedNodePositions(
  storageKey = "bruin-web-node-positions",
): [StoredNodePositions, React.Dispatch<React.SetStateAction<StoredNodePositions>>] {
  const [storedNodePositions, setStoredNodePositions] = useState<StoredNodePositions>({});

  useEffect(() => {
    const raw = window.localStorage.getItem(storageKey);
    if (!raw) {
      return;
    }

    try {
      const parsed = JSON.parse(raw) as StoredNodePositions;
      setStoredNodePositions(parsed);
    } catch {
      // ignore malformed persisted positions
    }
  }, [storageKey]);

  useEffect(() => {
    window.localStorage.setItem(storageKey, JSON.stringify(storedNodePositions));
  }, [storageKey, storedNodePositions]);

  return [storedNodePositions, setStoredNodePositions];
}
