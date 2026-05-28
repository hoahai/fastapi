import type { AccountSelection } from "@/components/dashboard/types";
import { TRADSPHERE_CACHE_TTL_MS } from "@shared/cache";

export const TRADSPHERE_SELECTIONS_CACHE_KEY = "tradsphere:main:selections:v3";
export const TRADSPHERE_SELECTIONS_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.SELECTIONS;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function unwrapData(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return "";
}

function asActiveFlag(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) {
      return true;
    }
    if (["0", "false", "no", "n", "off"].includes(normalized)) {
      return false;
    }
  }
  return true;
}

function buildSelectionLabel(accountCode: string, name: string): string {
  if (accountCode && name) {
    return `${accountCode} - ${name}`;
  }
  return accountCode || name;
}

export function normalizeTradsphereAccountSelections(payload: unknown): AccountSelection[] {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  const output: AccountSelection[] = [];
  const seen = new Set<string>();

  for (const item of data) {
    if (!isRecord(item)) {
      continue;
    }

    const accountCode = asString(item.code ?? item.accountCode ?? item.value).toUpperCase();
    const name = asString(item.name);
    const label = asString(item.label) || buildSelectionLabel(accountCode, name);

    if (!accountCode || !label || seen.has(accountCode)) {
      continue;
    }

    seen.add(accountCode);
    output.push({
      accountCode,
      label,
      name: name || undefined,
      active: asActiveFlag(item.active),
    });
  }

  return output.sort((a, b) => {
    const activeDiff = Number(Boolean(b.active)) - Number(Boolean(a.active));
    if (activeDiff !== 0) {
      return activeDiff;
    }
    return a.accountCode.localeCompare(b.accountCode);
  });
}
