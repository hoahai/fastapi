import type { LeaveSpherePtoBalance, LeaveSpherePtoTypeConfig } from "@leavesphere/lib/ptoTypes";

type LeaveSphereBalanceLike = Pick<
  LeaveSpherePtoBalance,
  "type" | "code" | "label" | "remainingHours" | "totalHours" | "usedHours" | "scheduledHours"
>;

type LeaveSpherePtoTypeLike = Pick<LeaveSpherePtoTypeConfig, "code" | "type" | "label">;

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function asNumber(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatHours(hours: number): string {
  if (!Number.isFinite(hours)) {
    return "0h";
  }
  return Number.isInteger(hours) ? `${hours}h` : `${hours.toFixed(1)}h`;
}

function resolveRemainingHours(balance: LeaveSphereBalanceLike | null | undefined): number | null {
  if (!balance) {
    return null;
  }
  const remainingHours = balance.remainingHours;
  if (typeof remainingHours === "number" && Number.isFinite(remainingHours)) {
    return Math.max(0, remainingHours);
  }

  const totalHours = asNumber(balance.totalHours);
  const usedHours = asNumber(balance.usedHours);
  const scheduledHours = asNumber(balance.scheduledHours);
  return Math.max(0, totalHours - usedHours - scheduledHours);
}

export function resolveLeaveSpherePtoAvailableHours(
  balances: Iterable<LeaveSphereBalanceLike> | null | undefined,
  ptoTypeCode: string,
): number | null {
  const normalizedType = asString(ptoTypeCode).toLowerCase();
  if (!normalizedType) {
    return null;
  }

  for (const balance of balances ?? []) {
    const balanceType = asString(balance.type).toLowerCase();
    const balanceCode = asString(balance.code).toLowerCase();
    if (balanceType !== normalizedType && balanceCode !== normalizedType) {
      continue;
    }
    return resolveRemainingHours(balance);
  }

  return null;
}

export function validateLeaveSpherePtoRequestedHours(params: {
  requestedHours: string | number;
  availableHours: number | null | undefined;
}): string | null {
  const requestedHours = typeof params.requestedHours === "number"
    ? params.requestedHours
    : Number(params.requestedHours);
  if (!Number.isFinite(requestedHours) || requestedHours <= 0) {
    return "Hours must be greater than zero.";
  }

  const availableHours = params.availableHours;
  if (availableHours == null || !Number.isFinite(availableHours)) {
    return null;
  }

  if (requestedHours > availableHours) {
    return `Requested hours exceed available balance (${formatHours(availableHours)} available).`;
  }

  return null;
}

export function buildLeaveSpherePtoTypeOptionsFromBalances(
  balances: Iterable<LeaveSphereBalanceLike> | null | undefined,
): Array<{ value: string; label: string }> {
  return buildLeaveSpherePtoTypeOptionsFromCatalog(null, balances);
}

export function buildLeaveSpherePtoTypeOptionsFromCatalog(
  ptoTypes: Iterable<LeaveSpherePtoTypeLike> | null | undefined,
  balances: Iterable<LeaveSphereBalanceLike> | null | undefined,
): Array<{ value: string; label: string }> {
  const availabilityByKey = new Map<string, number>();
  for (const balance of balances ?? []) {
    const availableHours = resolveRemainingHours(balance) ?? 0;
    const balanceCode = asString(balance.code).toLowerCase();
    const balanceType = asString(balance.type).toLowerCase();
    if (balanceCode && !availabilityByKey.has(balanceCode)) {
      availabilityByKey.set(balanceCode, availableHours);
    }
    if (balanceType && !availabilityByKey.has(balanceType)) {
      availabilityByKey.set(balanceType, availableHours);
    }
  }

  const seen = new Set<string>();
  const options: Array<{ value: string; label: string }> = [];

  for (const ptoType of ptoTypes ?? []) {
    const value = asString(ptoType.code) || asString(ptoType.type);
    if (!value) {
      continue;
    }
    const normalizedValue = value.toLowerCase();
    if (seen.has(normalizedValue)) {
      continue;
    }
    seen.add(normalizedValue);

    const availableHours = availabilityByKey.get(asString(ptoType.code).toLowerCase())
      ?? availabilityByKey.get(asString(ptoType.type).toLowerCase())
      ?? 0;
    const baseLabel = asString(ptoType.label) || asString(ptoType.type) || value;
    options.push({
      value,
      label: `${baseLabel} (${formatHours(availableHours)})`,
    });
  }

  if (options.length > 0) {
    return options;
  }

  for (const balance of balances ?? []) {
    const value = asString(balance.type);
    if (!value) {
      continue;
    }
    const normalizedValue = value.toLowerCase();
    if (seen.has(normalizedValue)) {
      continue;
    }
    seen.add(normalizedValue);

    const availableHours = resolveRemainingHours(balance) ?? 0;
    const baseLabel = asString(balance.label) || value;
    options.push({
      value,
      label: `${baseLabel} (${formatHours(availableHours)})`,
    });
  }

  return options;
}
