function normalizeCalendarLabel(value: string): string {
  return value.trim();
}

export function buildLeaveSpherePtoCalendarChipLabel(baseLabel: string, reason?: string | null): string {
  const normalizedBaseLabel = normalizeCalendarLabel(baseLabel);
  const normalizedReason = normalizeCalendarLabel(reason ?? "");

  if (!normalizedBaseLabel) {
    return normalizedReason;
  }

  if (!normalizedReason) {
    return normalizedBaseLabel;
  }

  return `${normalizedBaseLabel} · ${normalizedReason}`;
}
