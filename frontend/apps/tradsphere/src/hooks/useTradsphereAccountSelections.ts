import { useCallback, useEffect, useState } from "react";

import type { AccountSelection } from "@/components/dashboard/types";
import type { ApiRequestOptions } from "@/hooks/useApiRequest";
import { readBrowserCacheSnapshot, writeBrowserCache } from "@/lib/browserCache";
import {
  TRADSPHERE_SELECTIONS_CACHE_KEY,
  TRADSPHERE_SELECTIONS_CACHE_TTL_MS,
  normalizeTradsphereAccountSelections,
} from "@/lib/tradsphereAccountSelections";
import { shouldFetchNetwork, type CachePolicy } from "@shared/cache";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type RequestJson = (url: string, options?: ApiRequestOptions) => Promise<unknown>;

type UseTradsphereAccountSelectionsOptions = {
  requestJson: RequestJson;
  requestHeaders: HeadersInit;
  autoLoad?: boolean;
  initialPolicy?: CachePolicy;
  loadErrorMessage?: string;
  requestOptions?: Omit<ApiRequestOptions, "headers">;
};

export function useTradsphereAccountSelections({
  requestJson,
  requestHeaders,
  autoLoad = true,
  initialPolicy = "stale-while-revalidate",
  loadErrorMessage = "Unable to load account selections.",
  requestOptions,
}: UseTradsphereAccountSelectionsOptions) {
  const [accountSelections, setAccountSelections] = useState<AccountSelection[]>([]);
  const [isLoadingSelections, setIsLoadingSelections] = useState(true);
  const [isRefreshingSelections, setIsRefreshingSelections] = useState(false);
  const [selectionsError, setSelectionsError] = useState<string | null>(null);
  const [selectionsCacheStatus, setSelectionsCacheStatus] = useState<CacheStatus | null>(null);

  const loadSelections = useCallback(
    async (policy: CachePolicy) => {
      const cacheSnapshot = readBrowserCacheSnapshot<AccountSelection[]>(TRADSPHERE_SELECTIONS_CACHE_KEY);
      const cachedSelections = normalizeTradsphereAccountSelections(cacheSnapshot?.data);
      const hasCachedSelections = cachedSelections.length > 0;
      const shouldUseCache = policy !== "network-only" && hasCachedSelections;
      const shouldFetchFromNetwork = shouldFetchNetwork(policy, cacheSnapshot);

      if (shouldUseCache) {
        setAccountSelections(cachedSelections);
        setSelectionsCacheStatus({
          source: "cache",
          fetchedAt: cacheSnapshot?.fetchedAt ?? Date.now(),
        });
      }

      if (!shouldFetchFromNetwork) {
        setIsLoadingSelections(false);
        setIsRefreshingSelections(false);
        return;
      }

      setIsLoadingSelections(!shouldUseCache);
      setIsRefreshingSelections(shouldUseCache);
      setSelectionsError(null);

      try {
        const payload = await requestJson("/api/tradsphere/v1/ui/main/selections", {
          ...requestOptions,
          headers: requestHeaders,
        });
        const selections = normalizeTradsphereAccountSelections(payload);
        const fetchedAt = Date.now();

        writeBrowserCache(
          TRADSPHERE_SELECTIONS_CACHE_KEY,
          selections,
          TRADSPHERE_SELECTIONS_CACHE_TTL_MS,
          { source: "network", fetchedAt },
        );

        setAccountSelections(selections);
        setSelectionsCacheStatus({
          source: "network",
          fetchedAt,
        });
      } catch (error) {
        if (!shouldUseCache) {
          const message =
            error instanceof Error && error.message.trim()
              ? error.message.trim()
              : loadErrorMessage;
          setSelectionsError(message);
          setAccountSelections([]);
        } else {
          setSelectionsError(null);
        }
      } finally {
        setIsLoadingSelections(false);
        setIsRefreshingSelections(false);
      }
    },
    [loadErrorMessage, requestHeaders, requestJson, requestOptions],
  );

  useEffect(() => {
    if (!autoLoad) {
      return;
    }
    void loadSelections(initialPolicy);
  }, [autoLoad, initialPolicy, loadSelections]);

  return {
    accountSelections,
    isLoadingSelections,
    isRefreshingSelections,
    selectionsError,
    selectionsCacheStatus,
    loadSelections,
  };
}
