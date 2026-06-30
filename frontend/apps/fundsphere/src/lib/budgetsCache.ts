import { FRONTEND_CACHE_TTL_MS } from "@shared/cache";

import { readBrowserCacheSnapshot, writeBrowserCache } from "@fundsphere/lib/browserCache";
import type { FundsphereAccount } from "@fundsphere/lib/accountsApi";
import type { FundsphereBudgetMatrixResponse, FundsphereBudgetMatrixRow } from "@fundsphere/lib/budgetsApi";

export const FUNDSPHERE_BUDGETS_PAGE_CODE = "budgets";
const FUNDSPHERE_BUDGETS_MATRIX_CACHE_VERSION = "v1";
const FUNDSPHERE_BUDGETS_ACCOUNT_OPTIONS_CACHE_VERSION = "v1";
const FUNDSPHERE_BUDGETS_DETAIL_CACHE_VERSION = "v1";
export const FUNDSPHERE_BUDGETS_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;

export type FundsphereBudgetsCacheContext = {
  tenantSlug: string;
  userKey: string;
};

export type FundsphereBudgetsCacheCriteria = {
  accountCodes: string[];
  periods: string[];
};

function normalizeCachePart(value: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized || "unknown";
}

function normalizeCriteriaPart(values: string[]): string {
  const normalized = Array.from(
    new Set(
      (values ?? [])
        .map((value) => String(value || "").trim().toLowerCase())
        .filter(Boolean),
    ),
  );
  normalized.sort((left, right) => left.localeCompare(right));
  return normalized.length ? normalized.join(",") : "all";
}

export function buildFundsphereBudgetsMatrixCacheKey(
  context: FundsphereBudgetsCacheContext,
  criteria?: FundsphereBudgetsCacheCriteria | null,
): string {
  return [
    "budgets",
    "matrix",
    FUNDSPHERE_BUDGETS_MATRIX_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
    normalizeCriteriaPart(criteria?.accountCodes ?? []),
    normalizeCriteriaPart(criteria?.periods ?? []),
  ].join(":");
}

export function readFundsphereBudgetsMatrixCacheSnapshot(
  context: FundsphereBudgetsCacheContext,
  criteria?: FundsphereBudgetsCacheCriteria | null,
) {
  return readBrowserCacheSnapshot<FundsphereBudgetMatrixResponse>(buildFundsphereBudgetsMatrixCacheKey(context, criteria));
}

export function writeFundsphereBudgetsMatrixCache(
  context: FundsphereBudgetsCacheContext,
  response: FundsphereBudgetMatrixResponse,
  criteria?: FundsphereBudgetsCacheCriteria | null,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildFundsphereBudgetsMatrixCacheKey(context, criteria),
    response,
    FUNDSPHERE_BUDGETS_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: FUNDSPHERE_BUDGETS_MATRIX_CACHE_VERSION,
    },
  );
}

export function syncFundsphereBudgetsMatrixCache(
  context: FundsphereBudgetsCacheContext,
  response: FundsphereBudgetMatrixResponse,
  criteria?: FundsphereBudgetsCacheCriteria | null,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: FundsphereBudgetMatrixResponse | null;
} {
  const snapshot = readFundsphereBudgetsMatrixCacheSnapshot(context, criteria);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(response);
  } catch {
    changed = true;
  }

  writeFundsphereBudgetsMatrixCache(context, response, criteria, options);

  return {
    changed,
    previous,
  };
}

export type FundsphereBudgetDetailCacheContext = FundsphereBudgetsCacheContext;

export function buildFundsphereBudgetDetailCacheKey(context: FundsphereBudgetDetailCacheContext, budgetId: string): string {
  return [
    "budgets",
    "detail",
    FUNDSPHERE_BUDGETS_DETAIL_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
    normalizeCachePart(budgetId),
  ].join(":");
}

export function readFundsphereBudgetDetailCacheSnapshot(
  context: FundsphereBudgetDetailCacheContext,
  budgetId: string,
) {
  return readBrowserCacheSnapshot<FundsphereBudgetMatrixRow>(buildFundsphereBudgetDetailCacheKey(context, budgetId));
}

export function writeFundsphereBudgetDetailCache(
  context: FundsphereBudgetDetailCacheContext,
  budget: FundsphereBudgetMatrixRow,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildFundsphereBudgetDetailCacheKey(context, budget.budgetId ?? budget.accountCode),
    budget,
    FUNDSPHERE_BUDGETS_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: FUNDSPHERE_BUDGETS_DETAIL_CACHE_VERSION,
    },
  );
}

export function syncFundsphereBudgetDetailCache(
  context: FundsphereBudgetDetailCacheContext,
  budget: FundsphereBudgetMatrixRow,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: FundsphereBudgetMatrixRow | null;
} {
  const cacheKey = buildFundsphereBudgetDetailCacheKey(context, budget.budgetId ?? budget.accountCode);
  const snapshot = readBrowserCacheSnapshot<FundsphereBudgetMatrixRow>(cacheKey);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(budget);
  } catch {
    changed = true;
  }

  writeFundsphereBudgetDetailCache(context, budget, options);

  return {
    changed,
    previous,
  };
}

export function buildFundsphereBudgetAccountsCacheKey(context: FundsphereBudgetsCacheContext): string {
  return [
    "budgets",
    "accounts",
    FUNDSPHERE_BUDGETS_ACCOUNT_OPTIONS_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
  ].join(":");
}

export function readFundsphereBudgetAccountsCacheSnapshot(context: FundsphereBudgetsCacheContext) {
  return readBrowserCacheSnapshot<FundsphereAccount[]>(buildFundsphereBudgetAccountsCacheKey(context));
}

export function writeFundsphereBudgetAccountsCache(
  context: FundsphereBudgetsCacheContext,
  accounts: FundsphereAccount[],
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildFundsphereBudgetAccountsCacheKey(context),
    accounts,
    FUNDSPHERE_BUDGETS_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: FUNDSPHERE_BUDGETS_ACCOUNT_OPTIONS_CACHE_VERSION,
    },
  );
}

export function syncFundsphereBudgetAccountsCache(
  context: FundsphereBudgetsCacheContext,
  accounts: FundsphereAccount[],
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: FundsphereAccount[] | null;
} {
  const snapshot = readFundsphereBudgetAccountsCacheSnapshot(context);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(accounts);
  } catch {
    changed = true;
  }

  writeFundsphereBudgetAccountsCache(context, accounts, options);

  return {
    changed,
    previous,
  };
}
