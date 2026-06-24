import { FRONTEND_CACHE_TTL_MS } from "@shared/cache";

import { readBrowserCacheSnapshot, writeBrowserCache } from "@leavesphere/lib/browserCache";
import type { LeaveSphereEmployeeManagementWorkspace } from "@leavesphere/lib/employeeManagementApi";

export const LEAVESPHERE_EMPLOYEE_MANAGEMENT_PAGE_CODE = "employee-management";
const LEAVESPHERE_EMPLOYEE_MANAGEMENT_CACHE_VERSION = "v1";
export const LEAVESPHERE_EMPLOYEE_MANAGEMENT_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;

export type LeaveSphereEmployeeManagementWorkspaceCacheContext = {
  tenantSlug: string;
  userKey: string;
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
