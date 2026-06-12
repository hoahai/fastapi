export const DEFAULT_TIME_ZONE = "America/Chicago";

type DateParts = {
  year: string;
  month: string;
  day: string;
};

function normalizeTimeZone(timeZone?: string | null): string {
  const normalized = String(timeZone || "").trim();
  return normalized || DEFAULT_TIME_ZONE;
}

function createDateFormatter(
  timeZone: string,
  options: Intl.DateTimeFormatOptions,
): Intl.DateTimeFormat {
  try {
    return new Intl.DateTimeFormat("en-US", {
      ...options,
      timeZone,
    });
  } catch {
    return new Intl.DateTimeFormat("en-US", options);
  }
}

function readDateParts(date: Date, timeZone: string): DateParts | null {
  try {
    const parts = createDateFormatter(timeZone, {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(date);
    const year = parts.find((part) => part.type === "year")?.value ?? "";
    const month = parts.find((part) => part.type === "month")?.value ?? "";
    const day = parts.find((part) => part.type === "day")?.value ?? "";
    if (year && month && day) {
      return { year, month, day };
    }
  } catch {
    // Fall through to the caller's fallback path.
  }
  return null;
}

function getUtcNoonDate(isoDate: string): Date {
  return new Date(`${isoDate}T12:00:00Z`);
}

export function getTodayIsoDateInTimeZone(timeZone?: string | null, referenceDate: Date = new Date()): string {
  const normalizedTimeZone = normalizeTimeZone(timeZone);
  const parts = readDateParts(referenceDate, normalizedTimeZone);
  if (parts) {
    return `${parts.year}-${parts.month}-${parts.day}`;
  }
  return referenceDate.toISOString().slice(0, 10);
}

export function getCurrentYearInTimeZone(timeZone?: string | null, referenceDate: Date = new Date()): number {
  const todayIso = getTodayIsoDateInTimeZone(timeZone, referenceDate);
  const year = Number(todayIso.slice(0, 4));
  return Number.isFinite(year) && year >= 1900 ? year : referenceDate.getFullYear();
}

export function getCurrentMonthKeyInTimeZone(timeZone?: string | null, referenceDate: Date = new Date()): string {
  const todayIso = getTodayIsoDateInTimeZone(timeZone, referenceDate);
  return todayIso.slice(0, 7);
}

export function formatDateInTimeZone(
  isoDate: string,
  timeZone?: string | null,
  options: Intl.DateTimeFormatOptions = {},
): string {
  const normalizedIsoDate = String(isoDate || "").trim();
  if (!normalizedIsoDate) {
    return "-";
  }
  const normalizedTimeZone = normalizeTimeZone(timeZone);
  const date = getUtcNoonDate(normalizedIsoDate);
  try {
    return createDateFormatter(normalizedTimeZone, options).format(date);
  } catch {
    return new Intl.DateTimeFormat("en-US", options).format(date);
  }
}

export function formatDatePartsInTimeZone(
  isoDate: string,
  timeZone?: string | null,
): { year: string; month: string; day: string } | null {
  const normalizedIsoDate = String(isoDate || "").trim();
  if (!normalizedIsoDate) {
    return null;
  }
  const normalizedTimeZone = normalizeTimeZone(timeZone);
  return readDateParts(getUtcNoonDate(normalizedIsoDate), normalizedTimeZone);
}

export function shiftIsoDateByDays(isoDate: string, days: number): string {
  const normalizedIsoDate = String(isoDate || "").trim();
  if (!normalizedIsoDate) {
    return "";
  }
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(normalizedIsoDate);
  if (!match) {
    return normalizedIsoDate;
  }
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]), 12, 0, 0));
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

export function isIsoDateWithinInclusiveRange(
  isoDate: string,
  startIsoDate: string,
  endIsoDate: string,
): boolean {
  const value = String(isoDate || "").trim();
  const start = String(startIsoDate || "").trim();
  const end = String(endIsoDate || "").trim();
  if (!value || !start || !end) {
    return false;
  }
  const lowerBound = start <= end ? start : end;
  const upperBound = start <= end ? end : start;
  return value >= lowerBound && value <= upperBound;
}
