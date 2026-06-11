function normalizeCalendarLabel(value: string): string {
  return value.trim();
}

export function buildLeaveSpherePtoCalendarChipLabel(
  baseLabel: string,
  reason?: string | null,
  hoursLabel?: string | null,
): string {
  const normalizedBaseLabel = normalizeCalendarLabel(baseLabel);
  const normalizedReason = normalizeCalendarLabel(reason ?? "");
  const normalizedHoursLabel = normalizeCalendarLabel(hoursLabel ?? "");

  if (!normalizedBaseLabel) {
    return normalizedReason || normalizedHoursLabel;
  }

  if (!normalizedReason) {
    return normalizedHoursLabel ? `${normalizedBaseLabel} · ${normalizedHoursLabel}` : normalizedBaseLabel;
  }

  if (!normalizedHoursLabel) {
    return normalizedBaseLabel;
  }

  return `${normalizedBaseLabel} · ${normalizedReason} · ${normalizedHoursLabel}`;
}

export function buildLeaveSpherePtoCalendarRequestChipLabel(
  employeeName: string,
  ptoType: string,
  requestNote?: string | null,
  hoursLabel?: string | null,
): string {
  return buildLeaveSpherePtoCalendarChipLabel(
    employeeName,
    buildLeaveSpherePtoCalendarChipLabel(ptoType, requestNote, hoursLabel),
  );
}

export function buildLeaveSpherePtoCalendarRequestTooltipLabel(
  employeeName: string,
  ptoType: string,
  requestNote?: string | null,
  hoursLabel?: string | null,
): string {
  return buildLeaveSpherePtoCalendarRequestChipLabel(employeeName, ptoType, requestNote, hoursLabel);
}
