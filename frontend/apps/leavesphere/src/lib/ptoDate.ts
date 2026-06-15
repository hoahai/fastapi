import { formatDateInTimeZone, shiftIsoDateByDays } from "@shared/utils/time";

export type LeaveSpherePtoRequestCardTone = "past" | "current" | "future";

export function formatMonthDayYearLabel(isoDate: string, timeZone?: string | null): string {
  if (!isoDate) {
    return "-";
  }
  return formatDateInTimeZone(isoDate, timeZone, {
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
  });
}

export function formatPtoRequestDateRangeLabel(startDate: string, endDate: string, timeZone?: string | null): string {
  if (!startDate || !endDate) {
    return "-";
  }
  if (startDate === endDate) {
    const weekday = formatDateInTimeZone(startDate, timeZone, { weekday: "long" });
    return `${weekday}, ${formatMonthDayYearLabel(startDate, timeZone)}`;
  }
  const startDateLabel = formatDateInTimeZone(startDate, timeZone, { weekday: "long" });
  const endDateLabel = formatDateInTimeZone(endDate, timeZone, { weekday: "long" });
  return `${startDateLabel}, ${formatMonthDayYearLabel(startDate, timeZone)} - ${endDateLabel}, ${formatMonthDayYearLabel(endDate, timeZone)}`;
}

export function formatMonthDayYearRangeLabel(startDate: string, endDate: string, timeZone?: string | null): string {
  if (!startDate || !endDate) {
    return "-";
  }
  const startLabel = formatDateInTimeZone(startDate, timeZone, {
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
  });
  const endLabel = formatDateInTimeZone(endDate, timeZone, {
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
  });
  if (startLabel === endLabel) {
    return startLabel;
  }
  return `${startLabel} - ${endLabel}`;
}

export function getLeaveSpherePtoRequestCardTone(
  startDate: string,
  endDate: string,
  todayIsoDate: string,
): LeaveSpherePtoRequestCardTone {
  if (!startDate || !endDate || !todayIsoDate) {
    return "future";
  }

  if (endDate < todayIsoDate) {
    return "past";
  }

  const next7DaysIsoDate = shiftIsoDateByDays(todayIsoDate, 7);
  if (startDate <= next7DaysIsoDate) {
    return "current";
  }

  return "future";
}
