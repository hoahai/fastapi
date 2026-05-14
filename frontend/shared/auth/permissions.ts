import type { AccessProfile } from "./types";
import { getAccessAssignments, normalizeRoleKey } from "./accessAssignments";

function normalizeAppCode(appCode: string): string {
  return String(appCode || "").trim().toLowerCase();
}

function permissionSet(accessProfile: AccessProfile | null): Set<string> {
  return new Set(accessProfile?.permissions ?? []);
}

export function hasSuperAdminAccess(accessProfile: AccessProfile | null): boolean {
  if (!accessProfile) {
    return false;
  }
  if (accessProfile.scope?.isSuperAdmin) {
    return true;
  }
  const permissions = permissionSet(accessProfile);
  return permissions.has("workspace.super_admin");
}

export function hasAppViewAccess(accessProfile: AccessProfile | null, appCode: string): boolean {
  const normalizedAppCode = normalizeAppCode(appCode);
  if (!normalizedAppCode) {
    return false;
  }
  if (hasSuperAdminAccess(accessProfile)) {
    return true;
  }
  const assignments = getAccessAssignments(accessProfile);
  if (assignments.some((item) => item.appCode === normalizedAppCode)) {
    return true;
  }
  const permissions = permissionSet(accessProfile);
  return (
    permissions.has(`${normalizedAppCode}.viewer`)
    || permissions.has(`${normalizedAppCode}.editor`)
    || permissions.has(`${normalizedAppCode}.admin`)
  );
}

export function hasAppEditAccess(accessProfile: AccessProfile | null, appCode: string): boolean {
  const normalizedAppCode = normalizeAppCode(appCode);
  if (!normalizedAppCode) {
    return false;
  }
  if (hasSuperAdminAccess(accessProfile)) {
    return true;
  }
  const permissions = permissionSet(accessProfile);
  return permissions.has(`${normalizedAppCode}.editor`) || permissions.has(`${normalizedAppCode}.admin`);
}

export function hasAppAdminAccess(accessProfile: AccessProfile | null, appCode: string): boolean {
  const normalizedAppCode = normalizeAppCode(appCode);
  if (!normalizedAppCode) {
    return false;
  }
  if (hasSuperAdminAccess(accessProfile)) {
    return true;
  }
  const assignments = getAccessAssignments(accessProfile);
  if (assignments.some((item) => item.appCode === normalizedAppCode && normalizeRoleKey(item.role) === "admin")) {
    return true;
  }
  const permissions = permissionSet(accessProfile);
  return permissions.has(`${normalizedAppCode}.admin`);
}

export function hasAnyAdminScope(accessProfile: AccessProfile | null, appCodes: string[]): boolean {
  if (!accessProfile) {
    return false;
  }
  if (accessProfile.scope?.hasAnyAdminScope) {
    return true;
  }
  if (hasSuperAdminAccess(accessProfile)) {
    return true;
  }
  const assignments = getAccessAssignments(accessProfile);
  if (assignments.some((item) => normalizeRoleKey(item.role) === "admin")) {
    return true;
  }
  for (const code of appCodes) {
    if (hasAppAdminAccess(accessProfile, code)) {
      return true;
    }
  }
  return false;
}
