import { FRONTEND_CACHE_TTL_OVERRIDES_MS } from "./frontendTtlOverrides";

export const CACHE_TIME = {
  MINUTE: 60 * 1000,
  HOUR: 60 * 60 * 1000,
  DAY: 24 * 60 * 60 * 1000,
  WEEK: 7 * 24 * 60 * 60 * 1000,
} as const;

const DEFAULT_FRONTEND_CACHE_TTL_MS = CACHE_TIME.WEEK;

function resolveFrontendTtlMs(key: string): number {
  const override = FRONTEND_CACHE_TTL_OVERRIDES_MS[key];
  if (typeof override === "number" && Number.isFinite(override) && override >= 0) {
    return override;
  }
  return DEFAULT_FRONTEND_CACHE_TTL_MS;
}

export const TRADSPHERE_CACHE_TTL_MS = {
  SELECTIONS: resolveFrontendTtlMs("SELECTIONS"),
  MAIN_LOAD: resolveFrontendTtlMs("MAIN_LOAD"),
  SCHEDULE_TIMELINE: resolveFrontendTtlMs("SCHEDULE_TIMELINE"),
  SCHEDULE_TABLE: resolveFrontendTtlMs("SCHEDULE_TABLE"),
  ESTNUM_DETAIL: resolveFrontendTtlMs("ESTNUM_DETAIL"),
  STATION_DETAIL: resolveFrontendTtlMs("STATION_DETAIL"),
  DELIVERY_METHOD_OPTIONS: resolveFrontendTtlMs("DELIVERY_METHOD_OPTIONS"),
  EXISTING_CONTACTS: resolveFrontendTtlMs("EXISTING_CONTACTS"),
  DELIVERY_METHOD_USAGE_MEMORY: resolveFrontendTtlMs("DELIVERY_METHOD_USAGE_MEMORY"),
} as const;

export const FRONTEND_CACHE_TTL_MS = {
  DEFAULT: DEFAULT_FRONTEND_CACHE_TTL_MS,
} as const;
