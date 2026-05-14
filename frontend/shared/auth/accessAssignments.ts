import type { AccessProfile } from "./types";

export type NormalizedAccessAssignment = {
  tenantId: string;
  tenantSlug: string;
  tenantName: string | null;
  appId: string;
  appCode: string;
  appName: string | null;
  role: string;
};

export function normalizeRoleKey(role: string | null | undefined): string {
  return String(role || "").trim().toLowerCase();
}

export function rolePriority(role: string): number {
  const normalized = normalizeRoleKey(role);
  if (normalized === "super_admin") {
    return 0;
  }
  if (normalized === "admin") {
    return 1;
  }
  if (normalized === "editor") {
    return 2;
  }
  if (normalized === "viewer") {
    return 3;
  }
  return 99;
}

export function roleLabel(role: string | null | undefined): string {
  const normalized = normalizeRoleKey(role);
  if (normalized === "super_admin") {
    return "Super Admin";
  }
  if (normalized === "admin") {
    return "Admin";
  }
  if (normalized === "editor") {
    return "Editor";
  }
  return "Viewer";
}

export function roleChipClass(role: string | null | undefined): string {
  const normalized = normalizeRoleKey(role);
  if (normalized === "super_admin") {
    return "border-fuchsia-200 bg-fuchsia-50 text-fuchsia-700";
  }
  if (normalized === "admin") {
    return "border-rose-200 bg-rose-50 text-rose-700";
  }
  if (normalized === "editor") {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  return "border-emerald-200 bg-emerald-50 text-emerald-700";
}

const TENANT_CHIP_TONES = [
  "border-indigo-200 bg-indigo-50 text-indigo-700",
  "border-cyan-200 bg-cyan-50 text-cyan-700",
  "border-violet-200 bg-violet-50 text-violet-700",
  "border-teal-200 bg-teal-50 text-teal-700",
  "border-sky-200 bg-sky-50 text-sky-700",
  "border-blue-200 bg-blue-50 text-blue-700",
  "border-lime-200 bg-lime-50 text-lime-700",
  "border-orange-200 bg-orange-50 text-orange-700",
] as const;

function tenantColorIndex(tenantSlug: string): number {
  const normalized = String(tenantSlug || "").trim().toLowerCase();
  if (!normalized) {
    return 0;
  }
  let hash = 0;
  for (let i = 0; i < normalized.length; i += 1) {
    hash = (hash * 31 + normalized.charCodeAt(i)) >>> 0;
  }
  return hash % TENANT_CHIP_TONES.length;
}

export function tenantChipClass(tenantSlug: string): string {
  return TENANT_CHIP_TONES[tenantColorIndex(tenantSlug)];
}

function readAssignmentsFromProfile(accessProfile: AccessProfile | null): NormalizedAccessAssignment[] {
  if (!Array.isArray(accessProfile?.assignments)) {
    return [];
  }
  const assignments: NormalizedAccessAssignment[] = [];
  for (const raw of accessProfile.assignments) {
    if (!raw || typeof raw !== "object") {
      continue;
    }
    const tenant = raw.tenant;
    const app = raw.app;
    const tenantId = String(tenant?.id || "").trim();
    const tenantSlug = String(tenant?.slug || "").trim().toLowerCase();
    const appId = String(app?.id || "").trim();
    const appCode = String(app?.code || "").trim().toLowerCase();
    const role = normalizeRoleKey(raw.role);
    if (!tenantId || !tenantSlug || !appId || !appCode || !role) {
      continue;
    }
    assignments.push({
      tenantId,
      tenantSlug,
      tenantName: String(tenant?.name || "").trim() || null,
      appId,
      appCode,
      appName: String(app?.name || "").trim() || null,
      role,
    });
  }
  return assignments;
}

function readFallbackAssignment(accessProfile: AccessProfile | null): NormalizedAccessAssignment[] {
  if (!accessProfile?.tenant || !accessProfile?.app) {
    return [];
  }
  const tenantId = String(accessProfile.tenant.id || "").trim();
  const tenantSlug = String(accessProfile.tenant.slug || "").trim().toLowerCase();
  const appId = String(accessProfile.app.id || "").trim();
  const appCode = String(accessProfile.app.code || "").trim().toLowerCase();
  const role = normalizeRoleKey(accessProfile.role || "viewer");
  if (!tenantId || !tenantSlug || !appId || !appCode) {
    return [];
  }
  return [
    {
      tenantId,
      tenantSlug,
      tenantName: null,
      appId,
      appCode,
      appName: null,
      role: role || "viewer",
    },
  ];
}

export function getAccessAssignments(accessProfile: AccessProfile | null): NormalizedAccessAssignment[] {
  const rawAssignments = readAssignmentsFromProfile(accessProfile);
  const candidates = rawAssignments.length > 0 ? rawAssignments : readFallbackAssignment(accessProfile);
  const deduped = new Map<string, NormalizedAccessAssignment>();
  for (const item of candidates) {
    const key = `${item.appCode}::${item.tenantSlug}::${item.role}`;
    if (!deduped.has(key)) {
      deduped.set(key, item);
    }
  }
  return Array.from(deduped.values()).sort((a, b) => {
    const appCmp = (a.appName || a.appCode).localeCompare(b.appName || b.appCode);
    if (appCmp !== 0) {
      return appCmp;
    }
    const tenantCmp = (a.tenantName || a.tenantSlug).localeCompare(b.tenantName || b.tenantSlug);
    if (tenantCmp !== 0) {
      return tenantCmp;
    }
    const roleCmp = rolePriority(a.role) - rolePriority(b.role);
    if (roleCmp !== 0) {
      return roleCmp;
    }
    return a.tenantId.localeCompare(b.tenantId);
  });
}
