import { normalizeLeaveSphereTeamRegion, type LeaveSphereHoliday, type LeaveSphereTeamRegion } from "@leavesphere/lib/ptoTypes";

type LeaveSphereHolidaySource = Pick<LeaveSphereHoliday, "date" | "teamRegion">;

function normalizeHolidayDate(value: string): string {
  return value.trim().slice(0, 10);
}

export function buildLeaveSphereHolidayDateSet(
  holidays: Iterable<LeaveSphereHolidaySource> | null | undefined,
  teamRegion?: LeaveSphereTeamRegion | null,
): Set<string> {
  const normalizedTeamRegion = teamRegion ? normalizeLeaveSphereTeamRegion(teamRegion) : null;
  const holidayDates = new Set<string>();

  for (const holiday of holidays ?? []) {
    const holidayDate = normalizeHolidayDate(holiday.date);
    if (!holidayDate) {
      continue;
    }
    if (normalizedTeamRegion) {
      const holidayRegion = normalizeLeaveSphereTeamRegion(holiday.teamRegion);
      if (holidayRegion !== normalizedTeamRegion) {
        continue;
      }
    }
    holidayDates.add(holidayDate);
  }

  return holidayDates;
}
