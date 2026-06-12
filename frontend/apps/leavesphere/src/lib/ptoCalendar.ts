function normalizeCalendarLabel(value: string): string {
  return value.trim();
}

function joinCalendarLabels(parts: Array<string | null | undefined>): string {
  return parts.map((part) => normalizeCalendarLabel(part ?? "")).filter(Boolean).join(" · ");
}

export function buildLeaveSpherePtoCalendarChipLabel(
  baseLabel: string,
  description?: string | null,
  hoursLabel?: string | null,
): string {
  const normalizedBaseLabel = normalizeCalendarLabel(baseLabel);
  const normalizedDescription = normalizeCalendarLabel(description ?? "");
  const normalizedHoursLabel = normalizeCalendarLabel(hoursLabel ?? "");

  if (!normalizedBaseLabel) {
    return normalizedDescription || normalizedHoursLabel;
  }

  if (!normalizedDescription) {
    return normalizedHoursLabel ? `${normalizedBaseLabel} · ${normalizedHoursLabel}` : normalizedBaseLabel;
  }

  if (!normalizedHoursLabel) {
    return normalizedBaseLabel;
  }

  return `${normalizedBaseLabel} · ${normalizedDescription} · ${normalizedHoursLabel}`;
}

export function buildLeaveSpherePtoCalendarRequestChipLabel(
  employeeName: string,
  ptoType: string,
  hoursLabel?: string | null,
  description?: string | null,
): string {
  return joinCalendarLabels([employeeName, ptoType, hoursLabel, description]);
}

export function buildLeaveSpherePtoCalendarRequestTooltipLabel(
  employeeName: string,
  ptoType: string,
  hoursLabel?: string | null,
  description?: string | null,
): string {
  return buildLeaveSpherePtoCalendarRequestChipLabel(employeeName, ptoType, hoursLabel, description);
}
