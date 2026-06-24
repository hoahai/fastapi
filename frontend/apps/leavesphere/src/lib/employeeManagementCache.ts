import { FRONTEND_CACHE_TTL_MS } from "@shared/cache";

import { readBrowserCacheSnapshot, writeBrowserCache } from "@leavesphere/lib/browserCache";
import type { LeaveSphereEmployeeManagementWorkspace } from "@leavesphere/lib/employeeManagementApi";

export const LEAVESPHERE_EMPLOYEE_MANAGEMENT_PAGE_CODE = "employee-management";
const LEAVESPHERE_EMPLOYEE_MANAGEMENT_CACHE_VERSION = "v1";
export const LEAVESPHERE_EMPLOYEE_MANAGEMENT_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;
export const LEAVESPHERE_EMPLOYEE_MANAGEMENT_MANAGER_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;
const LEAVESPHERE_EMPLOYEE_MANAGEMENT_MANAGER_CACHE_VERSION = "v2";

export type LeaveSphereEmployeeManagementWorkspaceCacheContext = {
  tenantSlug: string;
  userKey: string;
};

export type LeaveSphereEmployeeManagementManagerCacheContext = LeaveSphereEmployeeManagementWorkspaceCacheContext;

export type LeaveSphereEmployeeManagementManagerCacheItem = {
  employeeId: string;
  managerId: string;
};

function normalizeCachePart(value: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized || "unknown";
}

export function buildLeaveSphereEmployeeManagementWorkspaceCacheKey(
  context: LeaveSphereEmployeeManagementWorkspaceCacheContext,
): string {
  return [
    "employee-management",
    LEAVESPHERE_EMPLOYEE_MANAGEMENT_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
  ].join(":");
}

export function readLeaveSphereEmployeeManagementWorkspaceCacheSnapshot(
  context: LeaveSphereEmployeeManagementWorkspaceCacheContext,
) {
  return readBrowserCacheSnapshot<LeaveSphereEmployeeManagementWorkspace>(
    buildLeaveSphereEmployeeManagementWorkspaceCacheKey(context),
  );
}

export function writeLeaveSphereEmployeeManagementWorkspaceCache(
  context: LeaveSphereEmployeeManagementWorkspaceCacheContext,
  workspace: LeaveSphereEmployeeManagementWorkspace,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildLeaveSphereEmployeeManagementWorkspaceCacheKey(context),
    workspace,
    LEAVESPHERE_EMPLOYEE_MANAGEMENT_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: LEAVESPHERE_EMPLOYEE_MANAGEMENT_CACHE_VERSION,
    },
  );
}

export function syncLeaveSphereEmployeeManagementWorkspaceCache(
  context: LeaveSphereEmployeeManagementWorkspaceCacheContext,
  workspace: LeaveSphereEmployeeManagementWorkspace,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: LeaveSphereEmployeeManagementWorkspace | null;
} {
  const snapshot = readLeaveSphereEmployeeManagementWorkspaceCacheSnapshot(context);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(workspace);
  } catch {
    changed = true;
  }

  writeLeaveSphereEmployeeManagementWorkspaceCache(context, workspace, options);

  return {
    changed,
    previous,
  };
}

function normalizeManagerCacheItems(value: Iterable<LeaveSphereEmployeeManagementManagerCacheItem> | null | undefined): LeaveSphereEmployeeManagementManagerCacheItem[] {
  const seen = new Set<string>();
  const normalized: LeaveSphereEmployeeManagementManagerCacheItem[] = [];
  for (const item of value ?? []) {
    const employeeId = normalizeCachePart(item?.employeeId ?? "");
    const managerId = normalizeCachePart(item?.managerId ?? "");
    if (!employeeId || !managerId) {
      continue;
    }
    const key = `${employeeId}::${managerId}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    normalized.push({
      employeeId,
      managerId,
    });
  }
  normalized.sort((left, right) => {
    const byEmployee = left.employeeId.localeCompare(right.employeeId);
    if (byEmployee !== 0) {
      return byEmployee;
    }
    return left.managerId.localeCompare(right.managerId);
  });
  return normalized;
}

function buildEmployeeManagerCacheKey(context: LeaveSphereEmployeeManagementManagerCacheContext): string {
  return [
    "employee-management",
    "manager-links",
    LEAVESPHERE_EMPLOYEE_MANAGEMENT_MANAGER_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
  ].join(":");
}

export function readLeaveSphereEmployeeManagementManagerCacheSnapshot(
  context: LeaveSphereEmployeeManagementManagerCacheContext,
) {
  return readBrowserCacheSnapshot<LeaveSphereEmployeeManagementManagerCacheItem[]>(buildEmployeeManagerCacheKey(context));
}

export function writeLeaveSphereEmployeeManagementManagerCache(
  context: LeaveSphereEmployeeManagementManagerCacheContext,
  managerLinks: Iterable<LeaveSphereEmployeeManagementManagerCacheItem> | null | undefined,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildEmployeeManagerCacheKey(context),
    normalizeManagerCacheItems(managerLinks),
    LEAVESPHERE_EMPLOYEE_MANAGEMENT_MANAGER_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: LEAVESPHERE_EMPLOYEE_MANAGEMENT_MANAGER_CACHE_VERSION,
    },
  );
}

export function syncLeaveSphereEmployeeManagementManagerCache(
  context: LeaveSphereEmployeeManagementManagerCacheContext,
  managerLinks: Iterable<LeaveSphereEmployeeManagementManagerCacheItem> | null | undefined,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: LeaveSphereEmployeeManagementManagerCacheItem[] | null;
} {
  const snapshot = readLeaveSphereEmployeeManagementManagerCacheSnapshot(context);
  const previous = snapshot?.data ?? null;
  const next = normalizeManagerCacheItems(managerLinks);
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(next);
  } catch {
    changed = true;
  }

  writeLeaveSphereEmployeeManagementManagerCache(context, next, options);

  return {
    changed,
    previous,
  };
}
