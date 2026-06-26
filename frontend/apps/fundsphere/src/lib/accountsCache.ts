import { FRONTEND_CACHE_TTL_MS } from "@shared/cache";

import { readBrowserCacheSnapshot, writeBrowserCache } from "@fundsphere/lib/browserCache";
import type { FundsphereAccount } from "@fundsphere/lib/accountsApi";

export const FUNDSPHERE_ACCOUNTS_PAGE_CODE = "accounts";
const FUNDSPHERE_ACCOUNTS_CACHE_VERSION = "v1";
export const FUNDSPHERE_ACCOUNTS_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;

export type FundsphereAccountsCacheContext = {
  tenantSlug: string;
  userKey: string;
};

function normalizeCachePart(value: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized || "unknown";
}

export function buildFundsphereAccountsCacheKey(context: FundsphereAccountsCacheContext): string {
  return [
    "accounts",
    FUNDSPHERE_ACCOUNTS_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
  ].join(":");
}

export function readFundsphereAccountsCacheSnapshot(context: FundsphereAccountsCacheContext) {
  return readBrowserCacheSnapshot<FundsphereAccount[]>(buildFundsphereAccountsCacheKey(context));
}

export function writeFundsphereAccountsCache(
  context: FundsphereAccountsCacheContext,
  accounts: FundsphereAccount[],
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildFundsphereAccountsCacheKey(context),
    accounts,
    FUNDSPHERE_ACCOUNTS_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: FUNDSPHERE_ACCOUNTS_CACHE_VERSION,
    },
  );
}

export function syncFundsphereAccountsCache(
  context: FundsphereAccountsCacheContext,
  accounts: FundsphereAccount[],
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: FundsphereAccount[] | null;
} {
  const snapshot = readFundsphereAccountsCacheSnapshot(context);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(accounts);
  } catch {
    changed = true;
  }

  writeFundsphereAccountsCache(context, accounts, options);

  return {
    changed,
    previous,
  };
}
