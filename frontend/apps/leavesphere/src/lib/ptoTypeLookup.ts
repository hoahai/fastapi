import type { LeaveSpherePtoTypeConfig } from "@leavesphere/lib/ptoTypes";

export type LeaveSpherePtoTypeMeta = {
  code: string;
  label: string;
  order: number;
};

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeLookupKey(value: string): string {
  return asString(value).toLowerCase().replace(/[^a-z0-9]+/g, "");
}

export function buildLeaveSpherePtoTypeMetaLookup(
  ptoTypes: Array<Pick<LeaveSpherePtoTypeConfig, "code" | "type" | "label">> | null | undefined,
): Map<string, LeaveSpherePtoTypeMeta> {
  const lookup = new Map<string, LeaveSpherePtoTypeMeta>();
  (ptoTypes ?? []).forEach((item, index) => {
    const code = normalizeLookupKey(asString(item.code));
    if (!code) {
      return;
    }
    const meta: LeaveSpherePtoTypeMeta = {
      code,
      label: asString(item.label) || asString(item.type) || code,
      order: index,
    };
    const keys = [
      code,
      normalizeLookupKey(asString(item.type) || code),
      normalizeLookupKey(asString(item.label)),
    ];
    for (const key of keys) {
      if (key && !lookup.has(key)) {
        lookup.set(key, meta);
      }
    }
  });
  return lookup;
}

export function resolveLeaveSpherePtoTypeMeta(
  rawValue: string,
  lookup: Map<string, LeaveSpherePtoTypeMeta>,
): LeaveSpherePtoTypeMeta | null {
  const normalized = normalizeLookupKey(rawValue);
  if (!normalized) {
    return null;
  }
  return lookup.get(normalized) || null;
}

export function resolveLeaveSpherePtoTypeLabel(
  rawValue: string,
  lookup: Map<string, LeaveSpherePtoTypeMeta>,
): string {
  const resolved = resolveLeaveSpherePtoTypeMeta(rawValue, lookup);
  if (resolved) {
    return resolved.label;
  }
  const fallback = asString(rawValue);
  return fallback ? fallback.toUpperCase() : "PTO";
}
