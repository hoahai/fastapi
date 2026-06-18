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

export function formatMonthYearLabel(monthKey: string, timeZone?: string | null): string {
  if (!monthKey) {
    return "-";
  }
  return formatDateInTimeZone(`${monthKey}-01`, timeZone, {
    month: "long",
    year: "numeric",
  });
}

type SubmissionDeadline = {
  month: number;
  day: number;
};

function parseSubmissionDeadline(value: string | null | undefined): SubmissionDeadline | null {
  const text = typeof value === "string" ? value.trim().replace(/\//g, "-") : "";
  if (!text) {
    return null;
  }
  const match = /^(0?[1-9]|1[0-2])-(0?[1-9]|[12]\d|3[01])$/.exec(text);
  if (!match) {
    return null;
  }
  const month = Number(match[1]);
  const day = Number(match[2]);
  const maxDay = new Date(Date.UTC(2000, month, 0)).getUTCDate();
  if (day > maxDay) {
    return null;
  }
  return { month, day };
}

export function formatLeaveSphereSubmissionDeadlineLabel(value: string | null | undefined): string | null {
  const parsed = parseSubmissionDeadline(value);
  if (!parsed) {
    return null;
  }
  return `${String(parsed.month).padStart(2, "0")}/${String(parsed.day).padStart(2, "0")}`;
}

export function isLeaveSphereSubmissionBlockedForYear(params: {
  lastSubmissionDate?: string | null;
  selectedYear: number;
  todayIsoDate: string;
}): boolean {
  const parsedDeadline = parseSubmissionDeadline(params.lastSubmissionDate);
  if (!parsedDeadline) {
    return false;
  }
  const todayYear = Number.parseInt(params.todayIsoDate.slice(0, 4), 10);
  if (!Number.isInteger(todayYear) || todayYear !== params.selectedYear) {
    return false;
  }
  const todayMonth = Number.parseInt(params.todayIsoDate.slice(5, 7), 10);
  const todayDay = Number.parseInt(params.todayIsoDate.slice(8, 10), 10);
  if (!Number.isInteger(todayMonth) || !Number.isInteger(todayDay)) {
    return false;
  }
  const todayMonthDay = `${String(todayMonth).padStart(2, "0")}-${String(todayDay).padStart(2, "0")}`;
  const deadlineMonthDay = `${String(parsedDeadline.month).padStart(2, "0")}-${String(parsedDeadline.day).padStart(2, "0")}`;
  return todayMonthDay > deadlineMonthDay;
}

export function getLeaveSphereSubmissionDeadlineError(params: {
  lastSubmissionDate?: string | null;
  selectedYear: number;
  todayIsoDate: string;
}): string | null {
  if (!isLeaveSphereSubmissionBlockedForYear(params)) {
    return null;
  }
  const deadlineLabel = formatLeaveSphereSubmissionDeadlineLabel(params.lastSubmissionDate) || "this date";
  return `New PTO requests for ${params.selectedYear} are closed after ${deadlineLabel}. Submit a request for ${params.selectedYear + 1} or later.`;
}

export function buildMonthKeyForYear(monthKey: string, year: number): string | null {
  const match = /^(\d{4})-(\d{2})$/.exec(monthKey);
  if (!match || !Number.isInteger(year)) {
    return null;
  }
  return `${year}-${match[2]}`;
}

export type LeaveSpherePtoMonthGroup<T> = {
  monthKey: string;
  monthLabel: string;
  requests: T[];
};

export function groupLeaveSpherePtoRequestsByEndMonth<T extends { startDate: string; endDate: string }>(
  requests: T[],
  timeZone?: string | null,
): LeaveSpherePtoMonthGroup<T>[] {
  const groups: LeaveSpherePtoMonthGroup<T>[] = [];
  const groupByMonth = new Map<string, LeaveSpherePtoMonthGroup<T>>();

  for (const request of requests) {
    const monthKey = (request.endDate || request.startDate).slice(0, 7);
    if (!monthKey) {
      continue;
    }
    let group = groupByMonth.get(monthKey);
    if (!group) {
      group = {
        monthKey,
        monthLabel: formatMonthYearLabel(monthKey, timeZone),
        requests: [],
      };
      groupByMonth.set(monthKey, group);
      groups.push(group);
    }
    group.requests.push(request);
  }

  return groups;
}
