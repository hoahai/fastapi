export function formatMonthDayYearLabel(isoDate: string): string {
  if (!isoDate) {
    return "-";
  }
  const date = new Date(`${isoDate}T00:00:00`);
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const year = date.getFullYear();
  return `${month}/${day}/${year}`;
}

export function formatPtoRequestDateRangeLabel(startDate: string, endDate: string): string {
  if (!startDate || !endDate) {
    return "-";
  }
  if (startDate === endDate) {
    const date = new Date(`${startDate}T00:00:00`);
    const weekday = date.toLocaleDateString(undefined, { weekday: "long" });
    return `${weekday}, ${formatMonthDayYearLabel(startDate)}`;
  }
  const startDateLabel = new Date(`${startDate}T00:00:00`).toLocaleDateString(undefined, { weekday: "long" });
  const endDateLabel = new Date(`${endDate}T00:00:00`).toLocaleDateString(undefined, { weekday: "long" });
  return `${startDateLabel}, ${formatMonthDayYearLabel(startDate)} - ${endDateLabel}, ${formatMonthDayYearLabel(endDate)}`;
}
