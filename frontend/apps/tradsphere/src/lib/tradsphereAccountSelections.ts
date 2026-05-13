import type { AccountSelection } from "@/components/dashboard/types";
import { TRADSPHERE_CACHE_TTL_MS } from "@shared/cache";

export const TRADSPHERE_SELECTIONS_CACHE_KEY = "tradsphere:main:selections:v2";
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
    output.push({ accountCode, label, name: name || undefined });
  }

  return output;
}
