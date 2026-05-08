import { shouldFetchNetwork, type CachePolicy, type CacheSnapshot } from "@shared/cache";

function hasFilledSearchValue(value: unknown): boolean {
  if (typeof value === "string") {
    return value.trim().length > 0;
  }

  if (typeof value === "number") {
    return Number.isFinite(value);
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.some((item) => hasFilledSearchValue(item));
  }

  return value !== null && value !== undefined;
}

export function hasAtLeastOneSearchCriterion(values: Record<string, unknown>): boolean {
  return Object.values(values).some((value) => hasFilledSearchValue(value));
}

export function shouldFetchSubmittedSearchNetwork(
  policy: CachePolicy,
  snapshot: CacheSnapshot<unknown> | null,
): boolean {
  if (policy === "stale-while-revalidate") {
    return true;
  }

  return shouldFetchNetwork(policy, snapshot);
}
