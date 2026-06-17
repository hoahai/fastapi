import { CACHE_TIME, type CacheSource } from "@shared/cache";

import { readBrowserCacheSnapshot, removeBrowserCache, writeBrowserCache } from "@leavesphere/lib/browserCache";

export const LEAVESPHERE_PTO_WORKSPACE_CACHE_TTL_MS = CACHE_TIME.WEEK;
const LEAVESPHERE_PTO_WORKSPACE_CACHE_VERSION = "v2";
export const LEAVESPHERE_MY_PTO_PAGE_CODE = "my-pto";
export const LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE = "leave-management";
export const LEAVESPHERE_LEAVE_MANAGEMENT_LEGACY_PAGE_CODE = "admin-pto";

export type LeaveSpherePtoWorkspaceCacheContext = {
  pageCode: typeof LEAVESPHERE_MY_PTO_PAGE_CODE
    | typeof LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE
    | typeof LEAVESPHERE_LEAVE_MANAGEMENT_LEGACY_PAGE_CODE;
  tenantSlug: string;
  userId: string;
  year: number;
};

function encodeCachePart(value: string): string {
  return encodeURIComponent(value.trim() || "unknown");
}

export function buildLeaveSpherePtoWorkspaceCacheKey(context: LeaveSpherePtoWorkspaceCacheContext): string {
  return [
    "pto-workspace",
    LEAVESPHERE_PTO_WORKSPACE_CACHE_VERSION,
    encodeCachePart(context.pageCode),
    encodeCachePart(context.tenantSlug),
    encodeCachePart(context.userId),
    encodeCachePart(String(context.year)),
  ].join(":");
}

export function readLeaveSpherePtoWorkspaceCacheSnapshot<T>(
  context: LeaveSpherePtoWorkspaceCacheContext,
) {
  return readBrowserCacheSnapshot<T>(buildLeaveSpherePtoWorkspaceCacheKey(context));
}

export function syncLeaveSpherePtoWorkspaceCache<T>(
  context: LeaveSpherePtoWorkspaceCacheContext,
  workspace: T,
  options?: {
    source?: CacheSource;
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: T | null;
} {
  const snapshot = readLeaveSpherePtoWorkspaceCacheSnapshot<T>(context);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(workspace);
  } catch {
    changed = true;
  }

  writeLeaveSpherePtoWorkspaceCache(context, workspace, options);

  return {
    changed,
    previous,
  };
}

export function writeLeaveSpherePtoWorkspaceCache<T>(
  context: LeaveSpherePtoWorkspaceCacheContext,
  workspace: T,
  options?: {
    source?: CacheSource;
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildLeaveSpherePtoWorkspaceCacheKey(context),
    workspace,
    LEAVESPHERE_PTO_WORKSPACE_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: LEAVESPHERE_PTO_WORKSPACE_CACHE_VERSION,
    },
  );
}

export function removeLeaveSpherePtoWorkspaceCache(
  context: LeaveSpherePtoWorkspaceCacheContext,
): void {
  removeBrowserCache(buildLeaveSpherePtoWorkspaceCacheKey(context));
}
