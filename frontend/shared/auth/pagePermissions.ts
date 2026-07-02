import type { AccessProfile } from "./types";

export type AppPageDefinition = {
  key: string;
  label: string;
  route: string;
};

const APP_PAGE_CATALOG: Record<string, AppPageDefinition[]> = {
  tradsphere: [
    { key: "tradsphere_home", label: "Accounts", route: "/tradsphere/home" },
    { key: "tradsphere_estnums", label: "Estimate Numbers", route: "/tradsphere/estnums" },
    { key: "tradsphere_contacts", label: "Contacts", route: "/tradsphere/contacts" },
    { key: "tradsphere_stations", label: "Stations", route: "/tradsphere/stations" },
    { key: "tradsphere_traffic", label: "Traffic", route: "/tradsphere/traffic" },
    { key: "tradsphere_invoice_checklists", label: "Invoice Checklists", route: "/tradsphere/invoice-checklists" },
  ],
  shiftzy: [
    { key: "shiftzy_home", label: "Schedules", route: "/shiftzy/home" },
    { key: "shiftzy_employees", label: "Employees", route: "/shiftzy/employees" },
  ],
  leavesphere: [
    { key: "leavesphere_home", label: "My PTO / Manager PTO", route: "/leavesphere/home" },
    { key: "leavesphere_leave_management", label: "Leave Management", route: "/leavesphere/leave-management" },
    { key: "leavesphere_employees", label: "Employee Management", route: "/leavesphere/employees" },
  ],
  fundsphere: [
    { key: "fundsphere_accounts", label: "Accounts", route: "/fundsphere/accounts" },
    { key: "fundsphere_services", label: "Services", route: "/fundsphere/services" },
    { key: "fundsphere_budgets", label: "Budgets", route: "/fundsphere/budgets" },
  ],
};

const LEGACY_PAGE_KEY_ALIASES: Record<string, string> = {
  leavesphere_admin_pto: "leavesphere_leave_management",
};

const LEGACY_ROUTE_ALIASES: Record<string, string> = {
  "/leavesphere/admin-pto": "/leavesphere/leave-management",
};

function normalize(value: string): string {
  return String(value || "").trim().toLowerCase();
}

function normalizeRoute(route: string): string {
  const normalized = String(route || "").trim().toLowerCase();
  if (!normalized) {
    return "";
  }
  const stripped = normalized.length > 1 && normalized.endsWith("/") ? normalized.slice(0, -1) : normalized;
  return LEGACY_ROUTE_ALIASES[stripped] ?? stripped;
}

function normalizePageKey(pageKey: string): string {
  const normalized = normalize(pageKey);
  return LEGACY_PAGE_KEY_ALIASES[normalized] ?? normalized;
}

export function getAppPageCatalog(appCode: string): AppPageDefinition[] {
  return [...(APP_PAGE_CATALOG[normalize(appCode)] ?? [])];
}

function scopedPermissionKeys(
  accessProfile: AccessProfile | null,
  appCode: string,
  tenantSlug: string,
): Set<string> {
  const normalizedAppCode = normalize(appCode);
  const normalizedTenantSlug = normalize(tenantSlug);
  if (!accessProfile || !normalizedAppCode || !normalizedTenantSlug) {
    return new Set<string>();
  }

  const entries = Array.isArray(accessProfile.pagePermissions) ? accessProfile.pagePermissions : [];
  const keys = new Set<string>();
  for (const entry of entries) {
    const entryAppCode = normalize(entry?.appCode || "");
    const entryTenantSlug = normalize(entry?.tenantSlug || "");
    const pageKey = normalizePageKey(entry?.pageKey || "");
    if (!entryAppCode || !entryTenantSlug || !pageKey) {
      continue;
    }
    if (entryAppCode !== normalizedAppCode || entryTenantSlug !== normalizedTenantSlug) {
      continue;
    }
    keys.add(pageKey);
  }
  return keys;
}

export function hasAppPageRestrictions(
  accessProfile: AccessProfile | null,
  appCode: string,
  tenantSlug: string,
): boolean {
  return scopedPermissionKeys(accessProfile, appCode, tenantSlug).size > 0;
}

export function canAccessAppRoute(params: {
  accessProfile: AccessProfile | null;
  appCode: string;
  tenantSlug: string;
  route: string;
}): boolean {
  const normalizedAppCode = normalize(params.appCode);
  const normalizedRouteValue = normalizeRoute(params.route);
  if (!normalizedAppCode || !normalizedRouteValue) {
    return true;
  }

  const allowedKeys = scopedPermissionKeys(params.accessProfile, normalizedAppCode, params.tenantSlug);
  if (allowedKeys.size === 0) {
    return true;
  }

  const catalog = getAppPageCatalog(normalizedAppCode);
  const page = catalog.find((item) => normalizeRoute(item.route) === normalizedRouteValue);
  if (!page) {
    return false;
  }
  return allowedKeys.has(normalize(page.key));
}
