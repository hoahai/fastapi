import { useCallback, useEffect, useRef, useState } from "react";

type GuardApplyOptions = {
  deferWhenDirty?: boolean;
};

type DeferredApply = {
  requestId: number;
  apply: () => void;
};

export function useDirtyRefreshGuard(isDirty: boolean) {
  const requestIdRef = useRef(0);
  const deferredApplyRef = useRef<DeferredApply | null>(null);
  const [hasDeferredUpdate, setHasDeferredUpdate] = useState(false);

  const clearDeferredUpdate = useCallback(() => {
    deferredApplyRef.current = null;
    setHasDeferredUpdate(false);
  }, []);

  const beginRequest = useCallback((): number => {
    requestIdRef.current += 1;
    clearDeferredUpdate();
    return requestIdRef.current;
  }, [clearDeferredUpdate]);

  const invalidateRequests = useCallback(() => {
    requestIdRef.current += 1;
    clearDeferredUpdate();
  }, [clearDeferredUpdate]);

  const isLatestRequest = useCallback((requestId: number): boolean => {
    return requestId === requestIdRef.current;
  }, []);

  const applyFromRequest = useCallback(
    (requestId: number, apply: () => void, options?: GuardApplyOptions): "applied" | "stale" | "deferred" => {
      if (!isLatestRequest(requestId)) {
        return "stale";
      }
      if (options?.deferWhenDirty !== false && isDirty) {
        deferredApplyRef.current = { requestId, apply };
        setHasDeferredUpdate(true);
        return "deferred";
      }
      clearDeferredUpdate();
      apply();
      return "applied";
    },
    [clearDeferredUpdate, isDirty, isLatestRequest],
  );

  const applyOrDefer = useCallback(
    (apply: () => void, options?: GuardApplyOptions): "applied" | "deferred" => {
      if (options?.deferWhenDirty !== false && isDirty) {
        deferredApplyRef.current = { requestId: requestIdRef.current, apply };
        setHasDeferredUpdate(true);
        return "deferred";
      }
      clearDeferredUpdate();
      apply();
      return "applied";
    },
    [clearDeferredUpdate, isDirty],
  );

  useEffect(() => {
    if (isDirty) {
      return;
    }
    const deferred = deferredApplyRef.current;
    if (!deferred) {
      return;
    }
    if (!isLatestRequest(deferred.requestId)) {
      clearDeferredUpdate();
      return;
    }
    clearDeferredUpdate();
    deferred.apply();
  }, [clearDeferredUpdate, isDirty, isLatestRequest]);

  return {
    hasDeferredUpdate,
    beginRequest,
    invalidateRequests,
    isLatestRequest,
    applyFromRequest,
    applyOrDefer,
    clearDeferredUpdate,
  };
}
