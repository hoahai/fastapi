export const TRADSPHERE_BROADCAST_TIMEZONE = "America/Chicago";

export type BroadcastRange = {
  flightStart: string;
  flightEnd: string;
};

const MIN_SUPPORTED_YEAR = 1901;
const MAX_SUPPORTED_YEAR = 2155;

function createUtcDate(year: number, month: number, day: number): Date {
  return new Date(Date.UTC(year, month - 1, day));
}

function formatUtcDateToIso(dateValue: Date): string {
  const year = dateValue.getUTCFullYear();
  const month = String(dateValue.getUTCMonth() + 1).padStart(2, "0");
  const day = String(dateValue.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addDays(dateValue: Date, days: number): Date {
  const next = new Date(dateValue.getTime());
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function mondayOfWeek(dateValue: Date): Date {
  return addDays(dateValue, -dateValue.getUTCDay() + (dateValue.getUTCDay() === 0 ? -6 : 1));
}

function getLastDayOfMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

function ensureYear(year: number): void {
  if (!Number.isInteger(year) || year < MIN_SUPPORTED_YEAR || year > MAX_SUPPORTED_YEAR) {
    throw new Error(`Year must be between ${MIN_SUPPORTED_YEAR} and ${MAX_SUPPORTED_YEAR}.`);
  }
}

function ensureMonth(month: number): void {
  if (!Number.isInteger(month) || month < 1 || month > 12) {
    throw new Error("Month must be between 1 and 12.");
  }
}

function ensureQuarter(quarter: number): void {
  if (!Number.isInteger(quarter) || quarter < 1 || quarter > 4) {
    throw new Error("Quarter must be between 1 and 4.");
  }
}

// Mirrors backend TradSphere helper semantics:
// broadcast month starts at Monday of Gregorian day 1's week and ends
// at the Sunday of the final full broadcast week in that month.
export function getBroadcastMonthRange(month: number, year: number): BroadcastRange {
  ensureMonth(month);
  ensureYear(year);

  const monthStart = createUtcDate(year, month, 1);
  const monthEnd = createUtcDate(year, month, getLastDayOfMonth(year, month));
  const beginBroadcastMonth = mondayOfWeek(monthStart);

  const diffDays = Math.floor((monthEnd.getTime() - beginBroadcastMonth.getTime()) / 86400000) + 1;
  const numberOfBroadcastWeek = Math.floor(diffDays / 7);

  if (numberOfBroadcastWeek < 1) {
    throw new Error("Unable to compute broadcast month range.");
  }

  const endBroadcastMonth = addDays(beginBroadcastMonth, numberOfBroadcastWeek * 7 - 1);
  return {
    flightStart: formatUtcDateToIso(beginBroadcastMonth),
    flightEnd: formatUtcDateToIso(endBroadcastMonth),
  };
}

export function getBroadcastQuarterRange(quarter: 1 | 2 | 3 | 4, year: number): BroadcastRange {
  ensureQuarter(quarter);
  ensureYear(year);

  const startMonth = (quarter - 1) * 3 + 1;
  const firstMonthRange = getBroadcastMonthRange(startMonth, year);
  const lastMonthRange = getBroadcastMonthRange(startMonth + 2, year);

  return {
    flightStart: firstMonthRange.flightStart,
    flightEnd: lastMonthRange.flightEnd,
  };
}

export function getBroadcastYearRange(year: number): BroadcastRange {
  ensureYear(year);

  const firstMonthRange = getBroadcastMonthRange(1, year);
  const lastMonthRange = getBroadcastMonthRange(12, year);

  return {
    flightStart: firstMonthRange.flightStart,
    flightEnd: lastMonthRange.flightEnd,
  };
}

export function buildBroadcastYearOptions(centerYear: number, totalCount = 9): number[] {
  const safeCenter = Number.isInteger(centerYear) ? centerYear : new Date().getUTCFullYear();
  const half = Math.floor(totalCount / 2);
  const years: number[] = [];

  for (let offset = -half; offset <= half; offset += 1) {
    const year = safeCenter + offset;
    if (year < MIN_SUPPORTED_YEAR || year > MAX_SUPPORTED_YEAR) {
      continue;
    }
    years.push(year);
  }

  return years;
}
