import type { AccessProfile } from "./types";

function normalizeAppCode(appCode: string): string {
  return String(appCode || "").trim().toLowerCase();
}

function permissionSet(accessProfile: AccessProfile | null): Set<string> {
  return new Set(accessProfile?.permissions ?? []);
}

export function hasAppEditAccess(accessProfile: AccessProfile | null, appCode: string): boolean {
  const normalizedAppCode = normalizeAppCode(appCode);
  if (!normalizedAppCode) {
    return false;
  }
  const permissions = permissionSet(accessProfile);
  return permissions.has(`${normalizedAppCode}.editor`) || permissions.has(`${normalizedAppCode}.admin`);
}

