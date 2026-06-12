import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";

import { Button } from "@tradsphere/components/ui/button";
import { ActionIconButton } from "@tradsphere/components/dashboard/ActionIconButton";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@tradsphere/components/ui/dialog";
import { Tooltip } from "@shared/components/actions/Tooltip";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { formatDateInTimeZone } from "@shared/utils/time";
import {
  getLeaveSpherePtoChipMeta,
  LeaveSpherePtoChipLabel,
  type LeaveSpherePtoChipTone,
} from "@leavesphere/components/PtoStatusChip";

export type LeaveSphereMonthCalendarEventTone = LeaveSpherePtoChipTone;

export type LeaveSphereMonthCalendarEvent = {
  id: string;
  label: string;
  tone: LeaveSphereMonthCalendarEventTone;
  startDate: string;
  endDate: string;
  title?: string;
};

type CalendarDay = {
  isoDate: string;
  dayNumber: number;
  inCurrentMonth: boolean;
};

type LeaveSphereMonthCalendarProps = {
  title: ReactNode;
  description?: ReactNode;
  monthKey: string;
  onMonthChange: (nextMonthKey: string) => void;
  minMonthKey?: string;
  maxMonthKey?: string;
  events: LeaveSphereMonthCalendarEvent[];
  onEventClick?: (event: LeaveSphereMonthCalendarEvent) => void;
  onDateClick?: (isoDate: string) => void;
  dayMinHeightClassName?: string;
  maxVisibleEventRows?: number;
  legend?: ReactNode;
  todayIsoDate?: string;
};

function toIsoDate(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getUtcTodayIsoDate(): string {
  return new Date().toISOString().slice(0, 10);
}

function parseMonthKey(monthKey: string, todayIsoDate?: string): Date {
  const match = /^(\d{4})-(\d{2})$/.exec(monthKey);
  if (!match) {
    const fallbackIsoDate = todayIsoDate || getUtcTodayIsoDate();
    const fallbackMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(fallbackIsoDate);
    if (fallbackMatch) {
      return new Date(Date.UTC(Number(fallbackMatch[1]), Number(fallbackMatch[2]) - 1, 1));
    }
    const now = new Date();
    return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  }
  return new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, 1));
}

function formatMonthHeading(monthKey: string, todayIsoDate?: string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  }).format(parseMonthKey(monthKey, todayIsoDate));
}

function addMonths(monthKey: string, offset: number): string {
  const date = parseMonthKey(monthKey);
  date.setUTCMonth(date.getUTCMonth() + offset);
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
}

function isBeforeMonthKey(left: string, right: string): boolean {
  return left < right;
}

function isAfterMonthKey(left: string, right: string): boolean {
  return left > right;
}

function clampMonthKey(monthKey: string, minMonthKey?: string, maxMonthKey?: string): string {
  let nextMonthKey = monthKey;
  if (minMonthKey && isBeforeMonthKey(nextMonthKey, minMonthKey)) {
    nextMonthKey = minMonthKey;
  }
  if (maxMonthKey && isAfterMonthKey(nextMonthKey, maxMonthKey)) {
    nextMonthKey = maxMonthKey;
  }
  return nextMonthKey;
}

function buildCalendarDays(monthKey: string, todayIsoDate?: string): CalendarDay[] {
  const monthStart = parseMonthKey(monthKey, todayIsoDate);
  const monthEnd = new Date(Date.UTC(monthStart.getUTCFullYear(), monthStart.getUTCMonth() + 1, 0));
  const startWeekday = (monthStart.getUTCDay() + 6) % 7;
  const gridStart = new Date(monthStart);
  gridStart.setUTCDate(monthStart.getUTCDate() - startWeekday);

  const days: CalendarDay[] = [];
  for (let index = 0; index < 42; index += 1) {
    const date = new Date(gridStart);
    date.setUTCDate(gridStart.getUTCDate() + index);
    days.push({
      isoDate: toIsoDate(date),
      dayNumber: date.getUTCDate(),
      inCurrentMonth: date.getTime() >= monthStart.getTime() && date.getTime() <= monthEnd.getTime(),
    });
  }
  return days;
}

function formatRequestTooltipDate(isoDate: string): string {
  return formatDateInTimeZone(isoDate, "UTC", {
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
  });
}

function formatLongDateLabel(isoDate: string): string {
  return formatDateInTimeZone(isoDate, "UTC", {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
  });
}

type CalendarDayCellProps = {
  day: CalendarDay;
  dayMinHeightClassName: string;
  onDateClick?: (isoDate: string) => void;
  hiddenEventCount?: number;
  onMoreEventsClick?: () => void;
  isToday?: boolean;
};

function CalendarDayCell({
  day,
  dayMinHeightClassName,
  onDateClick,
  hiddenEventCount = 0,
  onMoreEventsClick,
  isToday = false,
}: CalendarDayCellProps) {
  const anchorRef = useRef<HTMLButtonElement | null>(null);
  const [isTooltipOpen, setIsTooltipOpen] = useState(false);
  const tooltipText = `Request PTO for ${formatRequestTooltipDate(day.isoDate)}`;

  return (
    <article
      key={day.isoDate}
      className={[
        `relative ${dayMinHeightClassName} rounded-xl border p-2`,
        onDateClick ? "cursor-pointer hover:border-blue-300" : "",
        isToday
          ? "border-sky-300 bg-gradient-to-br from-sky-50 via-white to-indigo-50 text-slate-900 shadow-sm"
          : day.inCurrentMonth
            ? "border-blue-100 bg-white"
            : "border-slate-200 bg-slate-50/75 text-slate-400",
      ].join(" ")}
    >
      {onDateClick ? (
        <>
          <button
            ref={anchorRef}
            type="button"
            onClick={() => onDateClick(day.isoDate)}
            aria-label={tooltipText}
            onMouseEnter={() => setIsTooltipOpen(true)}
            onMouseLeave={() => setIsTooltipOpen(false)}
            onFocus={() => setIsTooltipOpen(true)}
            onBlur={() => setIsTooltipOpen(false)}
            className="absolute inset-0 z-[1] rounded-xl"
          />
          <Tooltip open={isTooltipOpen} anchorRef={anchorRef} text={tooltipText} />
        </>
      ) : null}
      <p className="relative z-[2] text-xs font-semibold">{day.dayNumber}</p>
      {hiddenEventCount > 0 && onMoreEventsClick ? (
        <button
          type="button"
          className="absolute bottom-1.5 left-2 z-[4] inline-flex rounded-full border border-slate-300 bg-white/95 px-2 py-0.5 text-[10px] font-semibold text-slate-600 transition-colors hover:border-slate-400 hover:text-slate-800"
          onClick={(event) => {
            event.stopPropagation();
            onMoreEventsClick();
          }}
        >
          +{hiddenEventCount} more
        </button>
      ) : null}
    </article>
  );
}

type WeekEventSegment = {
  event: LeaveSphereMonthCalendarEvent;
  startCol: number;
  endCol: number;
  row: number;
};

function buildWeekEventSegments(
  weekDays: CalendarDay[],
  events: LeaveSphereMonthCalendarEvent[],
  maxRows: number,
): { segments: WeekEventSegment[]; hiddenSegments: WeekEventSegment[] } {
  if (weekDays.length !== 7) {
    return { segments: [], hiddenSegments: [] };
  }
  const weekStart = weekDays[0].isoDate;
  const weekEnd = weekDays[6].isoDate;

  const candidates = events
    .filter((event) => event.startDate <= weekEnd && event.endDate >= weekStart)
    .map((event) => {
      let startCol = 0;
      for (let index = 0; index < weekDays.length; index += 1) {
        if (weekDays[index].isoDate >= event.startDate) {
          startCol = index;
          break;
        }
      }
      let endCol = 6;
      for (let index = weekDays.length - 1; index >= 0; index -= 1) {
        if (weekDays[index].isoDate <= event.endDate) {
          endCol = index;
          break;
        }
      }
      return { event, startCol, endCol };
    })
    .sort((left, right) => {
      if (left.startCol !== right.startCol) {
        return left.startCol - right.startCol;
      }
      return (right.endCol - right.startCol) - (left.endCol - left.startCol);
    });

  const rowOccupancy = Array.from({ length: maxRows }, () => Array(7).fill(false));
  const segments: WeekEventSegment[] = [];
  const hiddenSegments: WeekEventSegment[] = [];

  for (const candidate of candidates) {
    let assignedRow = -1;
    for (let row = 0; row < maxRows; row += 1) {
      let available = true;
      for (let col = candidate.startCol; col <= candidate.endCol; col += 1) {
        if (rowOccupancy[row][col]) {
          available = false;
          break;
        }
      }
      if (!available) {
        continue;
      }
      assignedRow = row;
      for (let col = candidate.startCol; col <= candidate.endCol; col += 1) {
        rowOccupancy[row][col] = true;
      }
      break;
    }

    if (assignedRow === -1) {
      hiddenSegments.push({
        event: candidate.event,
        startCol: candidate.startCol,
        endCol: candidate.endCol,
        row: -1,
      });
      continue;
    }
    segments.push({
      event: candidate.event,
      startCol: candidate.startCol,
      endCol: candidate.endCol,
      row: assignedRow,
    });
  }

  return { segments, hiddenSegments };
}

function eventOverlapsDay(event: LeaveSphereMonthCalendarEvent, isoDate: string): boolean {
  return event.startDate <= isoDate && event.endDate >= isoDate;
}

function sortDayEvents(left: LeaveSphereMonthCalendarEvent, right: LeaveSphereMonthCalendarEvent): number {
  if (left.startDate !== right.startDate) {
    return left.startDate.localeCompare(right.startDate);
  }
  if (left.endDate !== right.endDate) {
    return left.endDate.localeCompare(right.endDate);
  }
  return left.label.localeCompare(right.label);
}

function parseMinHeightRem(dayMinHeightClassName: string): number | null {
  const match = /min-h-\[(\d+(?:\.\d+)?)rem\]/.exec(dayMinHeightClassName);
  if (!match) {
    return null;
  }
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

function resolveEffectiveVisibleEventRows(maxVisibleEventRows: number, dayMinHeightClassName: string): number {
  if (maxVisibleEventRows <= 2) {
    return Math.max(1, maxVisibleEventRows);
  }
  const minHeightRem = parseMinHeightRem(dayMinHeightClassName);
  if (minHeightRem !== null && minHeightRem < 8) {
    return 2;
  }
  return maxVisibleEventRows;
}

export function LeaveSphereMonthCalendar({
  title,
  description,
  monthKey,
  onMonthChange,
  minMonthKey,
  maxMonthKey,
  events,
  onEventClick,
  onDateClick,
  dayMinHeightClassName = "min-h-[7.6rem]",
  maxVisibleEventRows = 3,
  legend,
  todayIsoDate,
}: LeaveSphereMonthCalendarProps) {
  const boundedMonthKey = clampMonthKey(monthKey, minMonthKey, maxMonthKey);
  useEffect(() => {
    if (boundedMonthKey !== monthKey) {
      onMonthChange(boundedMonthKey);
    }
  }, [boundedMonthKey, monthKey, onMonthChange]);

  const calendarDays = buildCalendarDays(boundedMonthKey, todayIsoDate);
  const weeks = Array.from({ length: 6 }, (_, weekIndex) => (
    calendarDays.slice(weekIndex * 7, weekIndex * 7 + 7)
  ));
  const effectiveVisibleEventRows = resolveEffectiveVisibleEventRows(maxVisibleEventRows, dayMinHeightClassName);
  const isPreviousMonthDisabled = Boolean(minMonthKey && isBeforeMonthKey(addMonths(boundedMonthKey, -1), minMonthKey));
  const isNextMonthDisabled = Boolean(maxMonthKey && isAfterMonthKey(addMonths(boundedMonthKey, 1), maxMonthKey));
  const [selectedOverflowDay, setSelectedOverflowDay] = useState<{
    isoDate: string;
    events: LeaveSphereMonthCalendarEvent[];
  } | null>(null);
  const dayEventsByIso = useMemo(() => {
    const map = new Map<string, LeaveSphereMonthCalendarEvent[]>();
    for (const day of calendarDays) {
      const matching = events
        .filter((event) => eventOverlapsDay(event, day.isoDate))
        .sort(sortDayEvents);
      map.set(day.isoDate, matching);
    }
    return map;
  }, [calendarDays, events]);

  return (
    <>
      <SectionCard
        title={title}
        description={description}
        actions={(
          <div className="flex items-center gap-1">
            <ActionIconButton
              aria-label="Load previous month"
              tooltip="Load previous month"
              onClick={() => {
                if (!isPreviousMonthDisabled) {
                  onMonthChange(addMonths(boundedMonthKey, -1));
                }
              }}
              icon={<ChevronLeft className="size-5" />}
              className="h-9 w-9"
              disabled={isPreviousMonthDisabled}
            />
            <p className="min-w-[9rem] text-center text-sm font-semibold text-slate-800">{formatMonthHeading(boundedMonthKey, todayIsoDate)}</p>
            <ActionIconButton
              aria-label="Load next month"
              tooltip="Load next month"
              onClick={() => {
                if (!isNextMonthDisabled) {
                  onMonthChange(addMonths(boundedMonthKey, 1));
                }
              }}
              icon={<ChevronRight className="size-5" />}
              className="h-9 w-9"
              disabled={isNextMonthDisabled}
            />
          </div>
        )}
        contentClassName="space-y-3"
      >
        <div className="grid grid-cols-7 gap-1 text-center text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-500">
          {["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].map((day) => (
            <div key={day}>{day}</div>
          ))}
        </div>

        <div className="space-y-1.5">
          {weeks.map((weekDays, weekIndex) => {
            const weekEventRows = buildWeekEventSegments(weekDays, events, effectiveVisibleEventRows);
            const hiddenCountByDate = new Map<string, number>();
            for (const segment of weekEventRows.hiddenSegments) {
              for (let col = segment.startCol; col <= segment.endCol; col += 1) {
                const isoDate = weekDays[col]?.isoDate;
                if (!isoDate) {
                  continue;
                }
                hiddenCountByDate.set(isoDate, (hiddenCountByDate.get(isoDate) ?? 0) + 1);
              }
            }

            return (
              <div key={`week-${weekIndex}`} className="relative space-y-1">
                <div className="grid grid-cols-7 gap-1.5">
                  {weekDays.map((day) => (
                    <CalendarDayCell
                      key={day.isoDate}
                      day={day}
                      dayMinHeightClassName={dayMinHeightClassName}
                      onDateClick={onDateClick}
                      hiddenEventCount={hiddenCountByDate.get(day.isoDate) ?? 0}
                      isToday={day.isoDate === todayIsoDate}
                      onMoreEventsClick={() => {
                        const dayEvents = dayEventsByIso.get(day.isoDate) ?? [];
                        setSelectedOverflowDay({
                          isoDate: day.isoDate,
                          events: dayEvents,
                        });
                      }}
                    />
                  ))}
                </div>

                {weekEventRows.segments.length > 0 ? (
                  <div className="pointer-events-none absolute inset-0 grid grid-cols-7 gap-1.5">
                    {weekEventRows.segments.map((segment) => {
                      const columnStart = segment.startCol + 1;
                      const columnEnd = segment.endCol + 2;
                      const chipClassName = `z-[3] flex items-center self-start rounded-full border px-2 py-1 text-[10px] font-medium leading-none ${getLeaveSpherePtoChipMeta(segment.event.tone).className}`;
                      const chipStyle = {
                        gridColumn: `${columnStart} / ${columnEnd}`,
                        gridRow: "1 / 2",
                        marginTop: `calc(1.6rem + ${segment.row} * 1.85rem)`,
                        marginLeft: "0.4rem",
                        marginRight: "0.4rem",
                      } as const;
                      return onEventClick ? (
                        <button
                          key={`${segment.event.id}-${weekIndex}-${segment.row}-${segment.startCol}`}
                          type="button"
                          style={chipStyle}
                          className={`${chipClassName} pointer-events-auto text-left transition-opacity hover:opacity-90`}
                          onClick={() => onEventClick(segment.event)}
                        >
                          <LeaveSpherePtoChipLabel
                            tone={segment.event.tone}
                            label={segment.event.label}
                            expandHitArea
                            truncate
                            showTooltip
                            tooltipText={segment.event.title || segment.event.label}
                            tooltipOpenDelayMs={180}
                          />
                        </button>
                      ) : (
                        <div
                          key={`${segment.event.id}-${weekIndex}-${segment.row}-${segment.startCol}`}
                          style={chipStyle}
                          className={`${chipClassName} pointer-events-auto`}
                        >
                          <LeaveSpherePtoChipLabel
                            tone={segment.event.tone}
                            label={segment.event.label}
                            expandHitArea
                            truncate
                            showTooltip
                            tooltipText={segment.event.title || segment.event.label}
                            tooltipOpenDelayMs={180}
                          />
                        </div>
                      );
                    })}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>

        {legend}
      </SectionCard>

      <Dialog
        open={Boolean(selectedOverflowDay)}
        onOpenChange={(open) => {
          if (!open) {
            setSelectedOverflowDay(null);
          }
        }}
      >
        <DialogContent className="max-w-xl">
          <DialogHeader>
            <DialogTitle>Day events</DialogTitle>
            <DialogDescription>
              {selectedOverflowDay ? formatLongDateLabel(selectedOverflowDay.isoDate) : ""}
            </DialogDescription>
          </DialogHeader>

          <div className="max-h-[22rem] space-y-2 overflow-y-auto pr-1">
            {(selectedOverflowDay?.events ?? []).map((event) => {
              const itemClassName = `w-full rounded-xl border px-3 py-2 text-left text-sm ${getLeaveSpherePtoChipMeta(event.tone).className}`;
              return onEventClick ? (
                <button
                  key={`${selectedOverflowDay?.isoDate || "day"}:${event.id}`}
                  type="button"
                  className={`${itemClassName} transition-opacity hover:opacity-90`}
                  onClick={() => {
                    onEventClick(event);
                  }}
                >
                  <LeaveSpherePtoChipLabel
                    tone={event.tone}
                    label={event.label}
                    expandHitArea
                    showTooltip
                    tooltipText={event.title || event.label}
                    tooltipOpenDelayMs={180}
                  />
                </button>
              ) : (
                <div key={`${selectedOverflowDay?.isoDate || "day"}:${event.id}`} className={itemClassName}>
                  <LeaveSpherePtoChipLabel
                    tone={event.tone}
                    label={event.label}
                    expandHitArea
                    showTooltip
                    tooltipText={event.title || event.label}
                    tooltipOpenDelayMs={180}
                  />
                </div>
              );
            })}
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setSelectedOverflowDay(null)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
