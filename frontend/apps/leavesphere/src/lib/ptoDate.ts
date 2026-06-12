import { formatDateInTimeZone } from "@shared/utils/time";

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
