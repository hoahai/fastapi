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

export type FundsphereAccountsCacheCriteria = {
  code: string;
  name: string;
  aeName: string;
  statusFilter: "" | "active" | "inactive";
};

function normalizeCachePart(value: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized || "unknown";
}

function normalizeCriteriaPart(value: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized || "all";
}

export function buildFundsphereAccountsCacheKey(
  context: FundsphereAccountsCacheContext,
  criteria?: FundsphereAccountsCacheCriteria | null,
): string {
  return [
    "accounts",
    FUNDSPHERE_ACCOUNTS_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
    normalizeCriteriaPart(criteria?.code ?? ""),
    normalizeCriteriaPart(criteria?.name ?? ""),
    normalizeCriteriaPart(criteria?.aeName ?? ""),
    normalizeCriteriaPart(criteria?.statusFilter ?? "active"),
  ].join(":");
}

export function readFundsphereAccountsCacheSnapshot(
  context: FundsphereAccountsCacheContext,
  criteria?: FundsphereAccountsCacheCriteria | null,
) {
  return readBrowserCacheSnapshot<FundsphereAccount[]>(buildFundsphereAccountsCacheKey(context, criteria));
}

export function writeFundsphereAccountsCache(
  context: FundsphereAccountsCacheContext,
  accounts: FundsphereAccount[],
  criteria?: FundsphereAccountsCacheCriteria | null,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildFundsphereAccountsCacheKey(context, criteria),
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
  criteria?: FundsphereAccountsCacheCriteria | null,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: FundsphereAccount[] | null;
} {
  const snapshot = readFundsphereAccountsCacheSnapshot(context, criteria);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(accounts);
  } catch {
    changed = true;
  }

  writeFundsphereAccountsCache(context, accounts, criteria, options);

  return {
    changed,
    previous,
  };
}
