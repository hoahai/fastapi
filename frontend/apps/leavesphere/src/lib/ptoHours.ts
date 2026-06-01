function parseIsoDate(value: string): Date | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value || "").trim());
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return null;
  }
  const date = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
  if (
    date.getUTCFullYear() !== year
    || date.getUTCMonth() + 1 !== month
    || date.getUTCDate() !== day
  ) {
    return null;
  }
  return date;
}

function toIsoDateFromUtc(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

export function calculateLeaveSpherePtoHours(
  startDate: string,
  endDate: string,
  holidayDates: Set<string>,
): string {
  if (!startDate || !endDate || startDate > endDate) {
    return "";
  }
  const start = parseIsoDate(startDate);
  const end = parseIsoDate(endDate);
  if (!start || !end) {
    return "";
  }

  let countedDays = 0;
  const cursor = new Date(start);
  while (cursor <= end) {
    const dayOfWeek = cursor.getUTCDay();
    const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
    const isoDate = toIsoDateFromUtc(cursor);
    if (!isWeekend && !holidayDates.has(isoDate)) {
      countedDays += 1;
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return String(countedDays * 8);
}
