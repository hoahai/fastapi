import { CACHE_TIME, type CacheSource } from "@shared/cache";

import { readBrowserCacheSnapshot, writeBrowserCache } from "@leavesphere/lib/browserCache";

export const LEAVESPHERE_PTO_WORKSPACE_CACHE_TTL_MS = CACHE_TIME.WEEK;

export type LeaveSpherePtoWorkspaceCacheContext = {
  pageCode: "my-pto" | "admin-pto";
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
    },
  );
}
