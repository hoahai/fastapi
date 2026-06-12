import { CACHE_TIME } from "@shared/cache";

import { readBrowserCacheSnapshot, writeBrowserCache } from "@leavesphere/lib/browserCache";
import type { LeaveSpherePtoEmployeeLookupItem } from "@leavesphere/lib/ptoEmployeeLookup";

export const LEAVESPHERE_PTO_EMPLOYEE_CACHE_TTL_MS = CACHE_TIME.WEEK;
const LEAVESPHERE_PTO_EMPLOYEE_CACHE_VERSION = "v2";

export type LeaveSpherePtoEmployeeCacheContext = {
  tenantSlug: string;
};

function normalizeText(value: string): string {
  return value.trim();
}

function encodeCachePart(value: string): string {
  return encodeURIComponent(normalizeText(value) || "unknown");
}

export function buildLeaveSpherePtoEmployeeCacheKey(context: LeaveSpherePtoEmployeeCacheContext): string {
  return ["pto-employees", LEAVESPHERE_PTO_EMPLOYEE_CACHE_VERSION, encodeCachePart(context.tenantSlug)].join(":");
}

export function readLeaveSpherePtoEmployeeCacheSnapshot(
  context: LeaveSpherePtoEmployeeCacheContext,
) {
  return readBrowserCacheSnapshot<LeaveSpherePtoEmployeeLookupItem[]>(
    buildLeaveSpherePtoEmployeeCacheKey(context),
  );
}

export function normalizeLeaveSpherePtoEmployeeCacheItems(
  employees: Iterable<LeaveSpherePtoEmployeeLookupItem> | null | undefined,
): LeaveSpherePtoEmployeeLookupItem[] {
  const seen = new Set<string>();
  const result: LeaveSpherePtoEmployeeLookupItem[] = [];
  for (const employee of employees ?? []) {
    const employeeId = normalizeText(employee.employeeId);
    const employeeName = normalizeText(employee.employeeName);
    if (!employeeId || !employeeName || seen.has(employeeId)) {
      continue;
    }
    seen.add(employeeId);
    result.push({
      employeeId,
      employeeName,
      pictureUrl: employee.pictureUrl ?? null,
    });
  }
  return result;
}

export function mergeLeaveSpherePtoEmployeeCacheItems(
  primary: Iterable<LeaveSpherePtoEmployeeLookupItem> | null | undefined,
  fallback: Iterable<LeaveSpherePtoEmployeeLookupItem> | null | undefined,
): LeaveSpherePtoEmployeeLookupItem[] {
  return normalizeLeaveSpherePtoEmployeeCacheItems([
    ...(primary ?? []),
    ...(fallback ?? []),
  ]);
}

export function writeLeaveSpherePtoEmployeeCache(
  context: LeaveSpherePtoEmployeeCacheContext,
  employees: Iterable<LeaveSpherePtoEmployeeLookupItem> | null | undefined,
  options?: {
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildLeaveSpherePtoEmployeeCacheKey(context),
    normalizeLeaveSpherePtoEmployeeCacheItems(employees),
    LEAVESPHERE_PTO_EMPLOYEE_CACHE_TTL_MS,
    {
      source: "network",
      fetchedAt: options?.fetchedAt,
      version: LEAVESPHERE_PTO_EMPLOYEE_CACHE_VERSION,
    },
  );
}
