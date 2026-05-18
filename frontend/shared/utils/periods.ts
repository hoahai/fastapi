export type PeriodValue = {
  year: number;
  month: number;
  quarter: number;
  value: string;
  label: string;
};

const MONTH_NAME_TO_NUMBER: Record<string, number> = {
  jan: 1,
  january: 1,
  feb: 2,
  february: 2,
  mar: 3,
  march: 3,
  apr: 4,
  april: 4,
  may: 5,
  jun: 6,
  june: 6,
  jul: 7,
  july: 7,
  aug: 8,
  august: 8,
  sep: 9,
  sept: 9,
  september: 9,
  oct: 10,
  october: 10,
  nov: 11,
  november: 11,
  dec: 12,
  december: 12,
};

function isValidPeriod(year: number, month: number): boolean {
  return Number.isInteger(year) && Number.isInteger(month) && year >= 1900 && year <= 9999 && month >= 1 && month <= 12;
}

export function formatPeriodValue(year: number, month: number): string {
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}`;
}

export function formatPeriodLabel(year: number, month: number): string {
  if (!isValidPeriod(year, month)) {
    return "-";
  }
  return new Date(Date.UTC(year, month - 1, 1)).toLocaleString("en-US", { month: "long", year: "numeric", timeZone: "UTC" });
}

export function formatPeriodMonthName(month: number): string {
  if (!Number.isInteger(month) || month < 1 || month > 12) {
    return "-";
  }
  return new Date(Date.UTC(2000, month - 1, 1)).toLocaleString("en-US", { month: "long", timeZone: "UTC" });
}

export function formatPeriodQuarterLabel(year: number, month: number): string {
  if (!isValidPeriod(year, month)) {
    return "-";
  }
  const quarter = Math.floor((month - 1) / 3) + 1;
  const shortYear = String(year % 100).padStart(2, "0");
  return `Q${quarter}.${shortYear} - ${formatPeriodMonthName(month)}`;
}

export function buildPeriodValue(year: number, month: number): PeriodValue | null {
  if (!isValidPeriod(year, month)) {
    return null;
  }
  return {
    year,
    month,
    quarter: Math.floor((month - 1) / 3) + 1,
    value: formatPeriodValue(year, month),
    label: formatPeriodLabel(year, month),
  };
}

export function parsePeriodInput(rawValue: string): PeriodValue | null {
  const text = String(rawValue || "").trim();
  if (!text) {
    return null;
  }

  let year: number | null = null;
  let month: number | null = null;

  let match = text.match(/^(\d{4})-(\d{1,2})$/);
  if (match) {
    year = Number(match[1]);
    month = Number(match[2]);
  }

  if (year === null || month === null) {
    match = text.match(/^(\d{1,2})[\/-](\d{4})$/);
    if (match) {
      month = Number(match[1]);
      year = Number(match[2]);
    }
  }

  if (year === null || month === null) {
    match = text.match(/^([A-Za-z]+)\s+(\d{4})$/);
    if (match) {
      month = MONTH_NAME_TO_NUMBER[String(match[1]).toLowerCase()] ?? null;
      year = Number(match[2]);
    }
  }

  if (year === null || month === null) {
    match = text.match(/^(\d{4})\s+([A-Za-z]+)$/);
    if (match) {
      year = Number(match[1]);
      month = MONTH_NAME_TO_NUMBER[String(match[2]).toLowerCase()] ?? null;
    }
  }

  if (year === null || month === null) {
    return null;
  }
  return buildPeriodValue(year, month);
}

export function buildRollingPeriods(monthsBefore = 3, monthsAfter = 3, anchorDate: Date = new Date()): PeriodValue[] {
  const anchorYear = anchorDate.getFullYear();
  const anchorMonth = anchorDate.getMonth() + 1;
  const anchorIndex = anchorYear * 12 + (anchorMonth - 1);
  const periods: PeriodValue[] = [];

  for (let offset = -monthsBefore; offset <= monthsAfter; offset += 1) {
    const index = anchorIndex + offset;
    const year = Math.floor(index / 12);
    const month = (index % 12 + 12) % 12 + 1;
    const period = buildPeriodValue(year, month);
    if (period) {
      periods.push(period);
    }
  }

  return periods;
}

export function shiftMonthAnchor(anchorDate: Date = new Date(), monthOffset = 0): Date {
  const year = anchorDate.getFullYear();
  const monthIndex = anchorDate.getMonth() + monthOffset;
  return new Date(year, monthIndex, 1);
}
