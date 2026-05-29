import { type Dispatch, type SetStateAction, useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  buildScopedPageStateStorageKey,
  readScopedPageState,
  removeScopedPageState,
  type ScopedPageState,
  writeScopedPageState,
} from "@shared/cache";
import { useAuth } from "@shared/auth/useAuth";

type ScopedPersistentStateOptions<T> = {
  validate?: (value: unknown) => value is T;
  enabled?: boolean;
  userKey?: string | null;
  tenantSlug?: string | null;
};

type ScopedPersistentStateScope = {
  appCode: string;
  pageCode: string;
  stateKey: string;
};

type ScopedPersistentStateControls = {
  hydrated: boolean;
  canPersist: boolean;
  storageKey: string | null;
  reset: () => void;
};

function resolveDefaultValue<T>(defaultValue: T | (() => T)): T {
  return defaultValue instanceof Function ? defaultValue() : defaultValue;
}

function normalizeScopeSegment(value: string, fallback: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return fallback;
  }
  return normalized;
}

export function useScopedPersistentState<T>(
  scope: ScopedPersistentStateScope,
  defaultValue: T | (() => T),
  options: ScopedPersistentStateOptions<T> = {},
): [T, Dispatch<SetStateAction<T>>, ScopedPersistentStateControls] {
  const auth = useAuth();
  const defaultValueRef = useRef(defaultValue);
  const hydratedStorageKeyRef = useRef<string | null>(null);

  const [state, setState] = useState<T>(() => resolveDefaultValue(defaultValueRef.current));
  const [hydrated, setHydrated] = useState(false);

  const userKey = normalizeScopeSegment(
    options.userKey ?? auth.user?.id ?? auth.user?.email ?? "",
    "",
  );
  const tenantSlug = normalizeScopeSegment(
    options.tenantSlug ?? auth.tenantSlug ?? "",
    "",
  );

  const canPersist = Boolean(
    options.enabled !== false
      && auth.status === "authenticated"
      && userKey
      && tenantSlug,
  );

  const scopedPageState = useMemo<ScopedPageState | null>(() => {
    if (!canPersist) {
      return null;
    }
    return {
      userKey,
      tenantSlug,
      appCode: normalizeScopeSegment(scope.appCode, "app"),
      pageCode: `${normalizeScopeSegment(scope.pageCode, "page")}:${normalizeScopeSegment(scope.stateKey, "state")}`,
    };
  }, [canPersist, scope.appCode, scope.pageCode, scope.stateKey, tenantSlug, userKey]);

  const storageKey = useMemo(() => {
    if (!scopedPageState) {
      return null;
    }
    return buildScopedPageStateStorageKey(scopedPageState);
  }, [scopedPageState]);

  useEffect(() => {
    const fallbackValue = resolveDefaultValue(defaultValueRef.current);

    if (!storageKey || !scopedPageState) {
      hydratedStorageKeyRef.current = null;
      setHydrated(false);
      setState(fallbackValue);
      return;
    }

    if (hydratedStorageKeyRef.current === storageKey) {
      setHydrated(true);
      return;
    }

    const restored = readScopedPageState<T>(
      scopedPageState,
      options.validate,
    );

    hydratedStorageKeyRef.current = storageKey;
    setState(restored ?? fallbackValue);
    setHydrated(true);
  }, [options.validate, scopedPageState, storageKey]);

  useEffect(() => {
    if (!hydrated || !storageKey || !scopedPageState) {
      return;
    }
    writeScopedPageState<T>(scopedPageState, state);
  }, [hydrated, scopedPageState, state, storageKey]);

  const reset = useCallback(() => {
    const fallbackValue = resolveDefaultValue(defaultValueRef.current);
    setState(fallbackValue);
    if (!scopedPageState) {
      return;
    }
    removeScopedPageState(scopedPageState);
  }, [scopedPageState]);

  return [
    state,
    setState,
    {
      hydrated,
      canPersist,
      storageKey,
      reset,
    },
  ];
}
