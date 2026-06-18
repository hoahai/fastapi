import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  buildScopedPageStateStorageKey,
  readScopedPageState,
  writeScopedPageState,
  type ScopedPageState,
} from "@shared/cache";

export type CriteriaLoadTrigger = "load-button" | "cache-chip";

export type CriteriaLoadMode = "initial-load" | "reload" | "hard-refresh";

export type CriteriaLoadPlan = {
  trigger: CriteriaLoadTrigger;
  mode: CriteriaLoadMode;
  criteriaKey: string;
  loadedCriteriaKey: string | null;
  isFirstLoad: boolean;
  isSameCriteria: boolean;
  shouldUseCacheFirst: boolean;
  shouldIgnoreCache: boolean;
  shouldPromptBeforeReload: boolean;
  shouldResetToBaseline: boolean;
};

export type CriteriaBaselineStoreOptions<TSnapshot> = {
  cloneSnapshot?: (snapshot: TSnapshot) => TSnapshot;
  areSnapshotsEqual?: (left: TSnapshot | null, right: TSnapshot | null) => boolean;
  persistence?: CriteriaBaselinePersistenceOptions<TSnapshot>;
};

export type CriteriaBaselinePersistenceOptions<TSnapshot> = {
  scope: ScopedPageState | null;
  enabled?: boolean;
  validateSnapshot?: (value: unknown) => value is TSnapshot;
};

type CriteriaKeyInput = string | number | null | undefined;

function normalizeCriteriaKey(value: CriteriaKeyInput): string {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function cloneCriteriaSnapshot<TSnapshot>(snapshot: TSnapshot): TSnapshot {
  if (snapshot === null || typeof snapshot !== "object") {
    return snapshot;
  }

  if (typeof globalThis.structuredClone === "function") {
    try {
      return globalThis.structuredClone(snapshot);
    } catch {
      // Fall back to JSON cloning for serializable snapshots.
    }
  }

  try {
    return JSON.parse(JSON.stringify(snapshot)) as TSnapshot;
  } catch {
    return snapshot;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizePersistedBaselineMap<TSnapshot>(
  value: unknown,
  validateSnapshot?: (value: unknown) => value is TSnapshot,
): Record<string, TSnapshot> {
  if (!isRecord(value)) {
    return {};
  }

  const normalized: Record<string, TSnapshot> = {};
  for (const [criteriaKey, snapshot] of Object.entries(value)) {
    const normalizedCriteriaKey = normalizeCriteriaKey(criteriaKey);
    if (!normalizedCriteriaKey) {
      continue;
    }
    if (validateSnapshot && !validateSnapshot(snapshot)) {
      continue;
    }
    normalized[normalizedCriteriaKey] = snapshot as TSnapshot;
  }
  return normalized;
}

export function areCriteriaSnapshotsEqual<TSnapshot>(left: TSnapshot | null, right: TSnapshot | null): boolean {
  if (left === right) {
    return true;
  }
  if (left === null || right === null) {
    return false;
  }

  try {
    return JSON.stringify(left) === JSON.stringify(right);
  } catch {
    return false;
  }
}

export function resolveCriteriaLoadPlan(params: {
  trigger: CriteriaLoadTrigger;
  criteriaKey: CriteriaKeyInput;
  loadedCriteriaKey: CriteriaKeyInput;
  hasDirtyState?: boolean;
}): CriteriaLoadPlan {
  const currentCriteriaKey = normalizeCriteriaKey(params.criteriaKey);
  const loadedCriteriaKey = normalizeCriteriaKey(params.loadedCriteriaKey);
  const isFirstLoad = !loadedCriteriaKey;
  const isSameCriteria = Boolean(currentCriteriaKey && loadedCriteriaKey && currentCriteriaKey === loadedCriteriaKey);

  if (params.trigger === "cache-chip") {
    return {
      trigger: "cache-chip",
      mode: "hard-refresh",
      criteriaKey: currentCriteriaKey,
      loadedCriteriaKey: loadedCriteriaKey || null,
      isFirstLoad,
      isSameCriteria,
      shouldUseCacheFirst: false,
      shouldIgnoreCache: true,
      shouldPromptBeforeReload: false,
      shouldResetToBaseline: false,
    };
  }

  return {
    trigger: "load-button",
    mode: isSameCriteria ? "reload" : "initial-load",
    criteriaKey: currentCriteriaKey,
    loadedCriteriaKey: loadedCriteriaKey || null,
    isFirstLoad,
    isSameCriteria,
    shouldUseCacheFirst: !isSameCriteria,
    shouldIgnoreCache: isSameCriteria,
    shouldPromptBeforeReload: Boolean(isSameCriteria && params.hasDirtyState),
    shouldResetToBaseline: isSameCriteria,
  };
}

export function useCriteriaBaselineStore<TSnapshot>(
  options: CriteriaBaselineStoreOptions<TSnapshot> = {},
) {
  const cloneSnapshot = options.cloneSnapshot ?? cloneCriteriaSnapshot;
  const areSnapshotsEqual = options.areSnapshotsEqual ?? areCriteriaSnapshotsEqual;
  const persistenceScope = options.persistence?.enabled === false ? null : options.persistence?.scope ?? null;
  const persistenceStorageKey = useMemo(() => {
    if (!persistenceScope) {
      return null;
    }
    return buildScopedPageStateStorageKey(persistenceScope);
  }, [persistenceScope]);
  const skipNextPersistWriteRef = useRef(false);
  const readPersistedBaselines = useCallback(() => {
    if (!persistenceScope) {
      return {};
    }
    const restored = readScopedPageState<unknown>(persistenceScope);
    return normalizePersistedBaselineMap(restored, options.persistence?.validateSnapshot);
  }, [options.persistence?.validateSnapshot, persistenceScope]);
  const [baselinesByCriteriaKey, setBaselinesByCriteriaKey] = useState<Record<string, TSnapshot>>(() => readPersistedBaselines());
  const [loadedCriteriaKey, setLoadedCriteriaKey] = useState<string | null>(null);

  useEffect(() => {
    if (!persistenceScope || !persistenceStorageKey) {
      setBaselinesByCriteriaKey({});
      setLoadedCriteriaKey(null);
      skipNextPersistWriteRef.current = true;
      return;
    }
    skipNextPersistWriteRef.current = true;
    setBaselinesByCriteriaKey(readPersistedBaselines());
    setLoadedCriteriaKey(null);
  }, [persistenceScope, persistenceStorageKey, readPersistedBaselines]);

  useEffect(() => {
    if (!persistenceScope) {
      return;
    }
    if (skipNextPersistWriteRef.current) {
      skipNextPersistWriteRef.current = false;
      return;
    }
    writeScopedPageState<Record<string, TSnapshot>>(persistenceScope, baselinesByCriteriaKey);
  }, [baselinesByCriteriaKey, persistenceScope]);

  const captureBaseline = useCallback((criteriaKey: CriteriaKeyInput, snapshot: TSnapshot) => {
    const normalizedCriteriaKey = normalizeCriteriaKey(criteriaKey);
    if (!normalizedCriteriaKey) {
      return;
    }
    setBaselinesByCriteriaKey((current) => ({
      ...current,
      [normalizedCriteriaKey]: cloneSnapshot(snapshot),
    }));
    setLoadedCriteriaKey(normalizedCriteriaKey);
  }, [cloneSnapshot]);

  const getBaseline = useCallback((criteriaKey: CriteriaKeyInput): TSnapshot | null => {
    const normalizedCriteriaKey = normalizeCriteriaKey(criteriaKey);
    if (!normalizedCriteriaKey) {
      return null;
    }
    const baseline = baselinesByCriteriaKey[normalizedCriteriaKey];
    return baseline === undefined ? null : cloneSnapshot(baseline);
  }, [baselinesByCriteriaKey, cloneSnapshot]);

  const hasBaseline = useCallback((criteriaKey: CriteriaKeyInput): boolean => {
    const normalizedCriteriaKey = normalizeCriteriaKey(criteriaKey);
    if (!normalizedCriteriaKey) {
      return false;
    }
    return Object.prototype.hasOwnProperty.call(baselinesByCriteriaKey, normalizedCriteriaKey);
  }, [baselinesByCriteriaKey]);

  const clearBaseline = useCallback((criteriaKey?: CriteriaKeyInput) => {
    if (criteriaKey === undefined) {
      setBaselinesByCriteriaKey({});
      setLoadedCriteriaKey(null);
      return;
    }

    const normalizedCriteriaKey = normalizeCriteriaKey(criteriaKey);
    if (!normalizedCriteriaKey) {
      return;
    }

    setBaselinesByCriteriaKey((current) => {
      if (!Object.prototype.hasOwnProperty.call(current, normalizedCriteriaKey)) {
        return current;
      }
      const next = { ...current };
      delete next[normalizedCriteriaKey];
      return next;
    });
    setLoadedCriteriaKey((current) => (current === normalizedCriteriaKey ? null : current));
  }, []);

  const isLoadedCriteria = useCallback((criteriaKey: CriteriaKeyInput): boolean => {
    const normalizedCriteriaKey = normalizeCriteriaKey(criteriaKey);
    return Boolean(normalizedCriteriaKey && normalizedCriteriaKey === loadedCriteriaKey);
  }, [loadedCriteriaKey]);

  const isDirty = useCallback((criteriaKey: CriteriaKeyInput, currentSnapshot: TSnapshot): boolean => {
    const baseline = getBaseline(criteriaKey);
    return !areSnapshotsEqual(currentSnapshot, baseline);
  }, [areSnapshotsEqual, getBaseline]);

  return {
    loadedCriteriaKey,
    captureBaseline,
    clearBaseline,
    getBaseline,
    hasBaseline,
    isDirty,
    isLoadedCriteria,
    resolveCriteriaLoadPlan,
  };
}
