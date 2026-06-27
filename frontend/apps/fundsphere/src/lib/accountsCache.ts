import { FRONTEND_CACHE_TTL_MS } from "@shared/cache";

import { readBrowserCacheSnapshot, writeBrowserCache } from "@fundsphere/lib/browserCache";
import type { FundsphereAccount } from "@fundsphere/lib/accountsApi";

export const FUNDSPHERE_ACCOUNTS_PAGE_CODE = "accounts";
const FUNDSPHERE_ACCOUNTS_CACHE_VERSION = "v1";
const FUNDSPHERE_ACCOUNT_DETAIL_CACHE_VERSION = "v1";
export const FUNDSPHERE_ACCOUNTS_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;

export type FundsphereAccountsCacheContext = {
  tenantSlug: string;
  userKey: string;
};

export type FundsphereAccountDetailCacheContext = FundsphereAccountsCacheContext;

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

export function buildFundsphereAccountDetailCacheKey(
  context: FundsphereAccountDetailCacheContext,
  accountCode: string,
): string {
  return [
    "accounts",
    "detail",
    FUNDSPHERE_ACCOUNT_DETAIL_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
    normalizeCachePart(accountCode.toUpperCase()),
  ].join(":");
}

export function readFundsphereAccountDetailCacheSnapshot(
  context: FundsphereAccountDetailCacheContext,
  accountCode: string,
) {
  return readBrowserCacheSnapshot<FundsphereAccount>(buildFundsphereAccountDetailCacheKey(context, accountCode));
}

export function writeFundsphereAccountDetailCache(
  context: FundsphereAccountDetailCacheContext,
  account: FundsphereAccount,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildFundsphereAccountDetailCacheKey(context, account.code),
    account,
    FUNDSPHERE_ACCOUNTS_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: FUNDSPHERE_ACCOUNT_DETAIL_CACHE_VERSION,
    },
  );
}

export function syncFundsphereAccountDetailCache(
  context: FundsphereAccountDetailCacheContext,
  account: FundsphereAccount,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: FundsphereAccount | null;
} {
  const snapshot = readFundsphereAccountDetailCacheSnapshot(context, account.code);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(account);
  } catch {
    changed = true;
  }

  writeFundsphereAccountDetailCache(context, account, options);

  return {
    changed,
    previous,
  };
}
