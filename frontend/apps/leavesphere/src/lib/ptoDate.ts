export function formatWeekdayDateLabel(isoDate: string): string {
  if (!isoDate) {
    return "-";
  }
  const date = new Date(`${isoDate}T00:00:00`);
  const weekday = date.toLocaleDateString(undefined, { weekday: "long" });
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const year = date.getFullYear();
  return `${weekday}, ${month}/${day}/${year}`;
}

export function formatPtoRequestDateRangeLabel(startDate: string, endDate: string): string {
  if (!startDate || !endDate) {
    return "-";
  }
  if (startDate === endDate) {
    return formatWeekdayDateLabel(startDate);
  }
  return `${formatWeekdayDateLabel(startDate)} - ${formatWeekdayDateLabel(endDate)}`;
}
