function normalizeCalendarLabel(value: string): string {
  return value.trim();
}

function joinCalendarLabels(parts: Array<string | null | undefined>): string {
  return parts.map((part) => normalizeCalendarLabel(part ?? "")).filter(Boolean).join(" · ");
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
  hoursLabel?: string | null,
  requestNote?: string | null,
): string {
  return joinCalendarLabels([employeeName, ptoType, hoursLabel, requestNote]);
}

export function buildLeaveSpherePtoCalendarRequestTooltipLabel(
  employeeName: string,
  ptoType: string,
  hoursLabel?: string | null,
  requestNote?: string | null,
): string {
  return buildLeaveSpherePtoCalendarRequestChipLabel(employeeName, ptoType, hoursLabel, requestNote);
}
