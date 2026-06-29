import { FRONTEND_CACHE_TTL_MS } from "@shared/cache";

import { readBrowserCacheSnapshot, writeBrowserCache } from "@fundsphere/lib/browserCache";
import type { FundsphereDepartment, FundsphereService } from "@fundsphere/lib/servicesApi";

export const FUNDSPHERE_SERVICES_PAGE_CODE = "services";
// Bump these when the cached service summary/detail shape changes so stale rows
// from older releases do not leak into the modal or search list.
const FUNDSPHERE_SERVICES_CACHE_VERSION = "v2";
const FUNDSPHERE_SERVICE_DETAIL_CACHE_VERSION = "v2";
export const FUNDSPHERE_SERVICES_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;
const FUNDSPHERE_SERVICES_DEPARTMENTS_CACHE_VERSION = "v1";
export const FUNDSPHERE_SERVICES_DEPARTMENTS_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;

export type FundsphereServicesCacheContext = {
  tenantSlug: string;
  userKey: string;
};

export type FundsphereServicesDepartmentsCacheContext = {
  tenantSlug: string;
};

export type FundsphereServiceDetailCacheContext = FundsphereServicesCacheContext;

export type FundsphereServicesCacheCriteria = {
  name: string;
  departmentCode: string;
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

export function buildFundsphereServicesCacheKey(
  context: FundsphereServicesCacheContext,
  criteria?: FundsphereServicesCacheCriteria | null,
): string {
  return [
    "services",
    FUNDSPHERE_SERVICES_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
    normalizeCriteriaPart(criteria?.name ?? ""),
    normalizeCriteriaPart(criteria?.departmentCode ?? ""),
    normalizeCriteriaPart(criteria?.statusFilter ?? "active"),
  ].join(":");
}

export function readFundsphereServicesCacheSnapshot(
  context: FundsphereServicesCacheContext,
  criteria?: FundsphereServicesCacheCriteria | null,
) {
  return readBrowserCacheSnapshot<FundsphereService[]>(buildFundsphereServicesCacheKey(context, criteria));
}

export function writeFundsphereServicesCache(
  context: FundsphereServicesCacheContext,
  services: FundsphereService[],
  criteria?: FundsphereServicesCacheCriteria | null,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildFundsphereServicesCacheKey(context, criteria),
    services,
    FUNDSPHERE_SERVICES_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: FUNDSPHERE_SERVICES_CACHE_VERSION,
    },
  );
}

export function syncFundsphereServicesCache(
  context: FundsphereServicesCacheContext,
  services: FundsphereService[],
  criteria?: FundsphereServicesCacheCriteria | null,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: FundsphereService[] | null;
} {
  const snapshot = readFundsphereServicesCacheSnapshot(context, criteria);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(services);
  } catch {
    changed = true;
  }

  writeFundsphereServicesCache(context, services, criteria, options);

  return {
    changed,
    previous,
  };
}

export function buildFundsphereServiceDetailCacheKey(
  context: FundsphereServiceDetailCacheContext,
  serviceId: string,
): string {
  return [
    "services",
    "detail",
    FUNDSPHERE_SERVICE_DETAIL_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
    normalizeCachePart(context.userKey),
    normalizeCachePart(serviceId),
  ].join(":");
}

export function readFundsphereServiceDetailCacheSnapshot(
  context: FundsphereServiceDetailCacheContext,
  serviceId: string,
) {
  return readBrowserCacheSnapshot<FundsphereService>(buildFundsphereServiceDetailCacheKey(context, serviceId));
}

export function writeFundsphereServiceDetailCache(
  context: FundsphereServiceDetailCacheContext,
  service: FundsphereService,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildFundsphereServiceDetailCacheKey(context, service.id),
    service,
    FUNDSPHERE_SERVICES_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: FUNDSPHERE_SERVICE_DETAIL_CACHE_VERSION,
    },
  );
}

export function syncFundsphereServiceDetailCache(
  context: FundsphereServiceDetailCacheContext,
  service: FundsphereService,
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: FundsphereService | null;
} {
  const snapshot = readFundsphereServiceDetailCacheSnapshot(context, service.id);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(service);
  } catch {
    changed = true;
  }

  writeFundsphereServiceDetailCache(context, service, options);

  return {
    changed,
    previous,
  };
}

export function buildFundsphereServicesDepartmentsCacheKey(context: FundsphereServicesDepartmentsCacheContext): string {
  return [
    "services",
    "departments",
    FUNDSPHERE_SERVICES_DEPARTMENTS_CACHE_VERSION,
    normalizeCachePart(context.tenantSlug),
  ].join(":");
}

export function readFundsphereServicesDepartmentsCacheSnapshot(context: FundsphereServicesDepartmentsCacheContext) {
  return readBrowserCacheSnapshot<FundsphereDepartment[]>(buildFundsphereServicesDepartmentsCacheKey(context));
}

export function writeFundsphereServicesDepartmentsCache(
  context: FundsphereServicesDepartmentsCacheContext,
  departments: FundsphereDepartment[],
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): void {
  writeBrowserCache(
    buildFundsphereServicesDepartmentsCacheKey(context),
    departments,
    FUNDSPHERE_SERVICES_DEPARTMENTS_CACHE_TTL_MS,
    {
      source: options?.source ?? "network",
      fetchedAt: options?.fetchedAt,
      version: FUNDSPHERE_SERVICES_DEPARTMENTS_CACHE_VERSION,
    },
  );
}

export function syncFundsphereServicesDepartmentsCache(
  context: FundsphereServicesDepartmentsCacheContext,
  departments: FundsphereDepartment[],
  options?: {
    source?: "cache" | "network";
    fetchedAt?: number;
  },
): {
  changed: boolean;
  previous: FundsphereDepartment[] | null;
} {
  const snapshot = readFundsphereServicesDepartmentsCacheSnapshot(context);
  const previous = snapshot?.data ?? null;
  let changed = true;
  try {
    changed = JSON.stringify(previous) !== JSON.stringify(departments);
  } catch {
    changed = true;
  }

  writeFundsphereServicesDepartmentsCache(context, departments, options);

  return {
    changed,
    previous,
  };
}
