import { createPortal } from "react-dom";
import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { CalendarDays } from "lucide-react";

import { TRADSPHERE_BROADCAST_TIMEZONE } from "@/lib/broadcastCalendar";
import { Input } from "@/components/ui/input";
import { TooltipTarget } from "@shared/components/actions/TooltipTarget";
import { FlightRangeSelector, type FlightRangePresetState } from "./FlightRangeSelector";

const MONDAY_WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const DAY_CELL_COUNT = 42;
const CALENDAR_POPUP_WIDTH_PX = 312;
const CALENDAR_POPUP_EDGE_PADDING_PX = 12;
const CALENDAR_POPUP_OFFSET_PX = 8;
const CALENDAR_POPUP_MIN_HEIGHT_PX = 240;
export const FLIGHT_DATE_PICKER_POPOVER_SELECTOR = '[data-flight-date-picker-popover="true"]';

const CHICAGO_DATE_PARTS_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: TRADSPHERE_BROADCAST_TIMEZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

const MONTH_LABEL_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: TRADSPHERE_BROADCAST_TIMEZONE,
  month: "long",
  year: "numeric",
});

type ParsedIsoDate = {
  year: number;
  month: number;
  day: number;
};

type CalendarDay = {
  isoDate: string;
  dayNumber: number;
  inCurrentMonth: boolean;
};

function toIsoDate(year: number, month: number, day: number): string {
  return `${year.toString().padStart(4, "0")}-${month.toString().padStart(2, "0")}-${day
    .toString()
    .padStart(2, "0")}`;
}

function toStableUtcDate(year: number, month: number, day: number): Date {
  // Use UTC noon so timezone formatting never shifts into adjacent dates/months.
  return new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
}

function parseIsoDate(value: string): ParsedIsoDate | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value.trim());
  if (!match) {
    return null;
  }

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return null;
  }
  if (month < 1 || month > 12) {
    return null;
  }

  const maxDay = new Date(Date.UTC(year, month, 0)).getUTCDate();
  if (day < 1 || day > maxDay) {
    return null;
  }

  return { year, month, day };
}

function formatIsoDateForDisplay(value: string): string {
  const parsed = parseIsoDate(value);
  if (!parsed) {
    return value.trim();
  }
  return `${parsed.month.toString().padStart(2, "0")}/${parsed.day
    .toString()
    .padStart(2, "0")}/${parsed.year.toString().padStart(4, "0")}`;
}

function getTodayInChicago(): ParsedIsoDate {
  const parts = CHICAGO_DATE_PARTS_FORMATTER.formatToParts(new Date());
  const values = parts.reduce<Record<string, string>>((output, part) => {
    output[part.type] = part.value;
    return output;
  }, {});

  const year = Number(values.year);
  const month = Number(values.month);
  const day = Number(values.day);

  return {
    year: Number.isFinite(year) ? year : 1970,
    month: Number.isFinite(month) ? month : 1,
    day: Number.isFinite(day) ? day : 1,
  };
}

function shiftMonth(year: number, month: number, delta: number): { year: number; month: number } {
  const date = new Date(Date.UTC(year, month - 1 + delta, 1));
  return { year: date.getUTCFullYear(), month: date.getUTCMonth() + 1 };
}

function buildMondayFirstCalendarDays(year: number, month: number): CalendarDay[] {
  const firstDay = new Date(Date.UTC(year, month - 1, 1)).getUTCDay();
  const startOffset = (firstDay + 6) % 7;
  const daysInCurrentMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
  const daysInPreviousMonth = new Date(Date.UTC(year, month - 1, 0)).getUTCDate();

  const cells: CalendarDay[] = [];
  for (let index = 0; index < DAY_CELL_COUNT; index += 1) {
    const rawDay = index - startOffset + 1;
    if (rawDay < 1) {
      const prev = shiftMonth(year, month, -1);
      const dayNumber = daysInPreviousMonth + rawDay;
      cells.push({
        isoDate: toIsoDate(prev.year, prev.month, dayNumber),
        dayNumber,
        inCurrentMonth: false,
      });
      continue;
    }

    if (rawDay > daysInCurrentMonth) {
      const next = shiftMonth(year, month, 1);
      const dayNumber = rawDay - daysInCurrentMonth;
      cells.push({
        isoDate: toIsoDate(next.year, next.month, dayNumber),
        dayNumber,
        inCurrentMonth: false,
      });
      continue;
    }

    cells.push({
      isoDate: toIsoDate(year, month, rawDay),
      dayNumber: rawDay,
      inCurrentMonth: true,
    });
  }

  return cells;
}

export function DateInputField({
  id,
  value,
  onChange,
  disabled,
  label,
  minDate,
  maxDate,
  openCalendarSignal,
}: {
  id: string;
  value: string;
  onChange: (nextValue: string) => void;
  disabled: boolean;
  label: string;
  minDate?: string;
  maxDate?: string;
  openCalendarSignal?: number;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const calendarPopoverRef = useRef<HTMLDivElement | null>(null);
  const [isCalendarOpen, setIsCalendarOpen] = useState(false);
  const lastOpenSignalRef = useRef<number | undefined>(openCalendarSignal);
  const [calendarPopoverStyle, setCalendarPopoverStyle] = useState<CSSProperties | null>(null);
  const [calendarPopoverMaxHeight, setCalendarPopoverMaxHeight] = useState<number>(440);

  const today = useMemo(() => getTodayInChicago(), []);
  const parsed = useMemo(() => parseIsoDate(value), [value]);
  const displayValue = useMemo(() => formatIsoDateForDisplay(value), [value]);
  const [displayMonth, setDisplayMonth] = useState<number>(parsed?.month ?? today.month);
  const [displayYear, setDisplayYear] = useState<number>(parsed?.year ?? today.year);

  const calendarDays = useMemo(
    () => buildMondayFirstCalendarDays(displayYear, displayMonth),
    [displayMonth, displayYear],
  );
  const monthLabel = useMemo(() => {
    return MONTH_LABEL_FORMATTER.format(toStableUtcDate(displayYear, displayMonth, 1));
  }, [displayMonth, displayYear]);

  useEffect(() => {
    const selected = parseIsoDate(value);
    if (!selected) {
      return;
    }
    setDisplayMonth(selected.month);
    setDisplayYear(selected.year);
  }, [value]);

  useEffect(() => {
    if (!isCalendarOpen) {
      return;
    }

    function updatePopoverPosition() {
      const anchor = containerRef.current;
      if (!anchor) {
        return;
      }

      const rect = anchor.getBoundingClientRect();
      const availableBelow = window.innerHeight - rect.bottom - CALENDAR_POPUP_EDGE_PADDING_PX;
      const availableAbove = rect.top - CALENDAR_POPUP_EDGE_PADDING_PX;
      const preferAbove = availableBelow < CALENDAR_POPUP_MIN_HEIGHT_PX && availableAbove > availableBelow;
      const availableSpace = preferAbove ? availableAbove : availableBelow;
      const resolvedMaxHeight = Math.max(
        CALENDAR_POPUP_MIN_HEIGHT_PX,
        Math.min(440, availableSpace - CALENDAR_POPUP_OFFSET_PX),
      );
      const resolvedWidth = Math.min(
        CALENDAR_POPUP_WIDTH_PX,
        Math.max(0, window.innerWidth - (CALENDAR_POPUP_EDGE_PADDING_PX * 2)),
      );
      const resolvedLeft = Math.min(
        Math.max(CALENDAR_POPUP_EDGE_PADDING_PX, rect.left),
        Math.max(CALENDAR_POPUP_EDGE_PADDING_PX, window.innerWidth - CALENDAR_POPUP_EDGE_PADDING_PX - resolvedWidth),
      );

      setCalendarPopoverMaxHeight(resolvedMaxHeight);
      setCalendarPopoverStyle({
        position: "fixed",
        left: resolvedLeft,
        top: preferAbove ? rect.top - CALENDAR_POPUP_OFFSET_PX : rect.bottom + CALENDAR_POPUP_OFFSET_PX,
        width: resolvedWidth,
        zIndex: 90,
        transform: preferAbove ? "translateY(-100%)" : "none",
      });
    }

    function handleClickOutside(event: MouseEvent) {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (calendarPopoverRef.current?.contains(target)) {
        return;
      }
      if (!containerRef.current?.contains(target)) {
        setIsCalendarOpen(false);
      }
    }

    updatePopoverPosition();
    document.addEventListener("mousedown", handleClickOutside);
    window.addEventListener("resize", updatePopoverPosition);
    window.addEventListener("scroll", updatePopoverPosition, true);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      window.removeEventListener("resize", updatePopoverPosition);
      window.removeEventListener("scroll", updatePopoverPosition, true);
    };
  }, [isCalendarOpen]);

  useEffect(() => {
    if (disabled || openCalendarSignal === undefined) {
      return;
    }
    if (lastOpenSignalRef.current === openCalendarSignal) {
      return;
    }
    lastOpenSignalRef.current = openCalendarSignal;
    setIsCalendarOpen(true);
  }, [disabled, openCalendarSignal]);

  function handleToggleCalendar() {
    if (disabled) {
      return;
    }
    setIsCalendarOpen((current) => !current);
  }

  function handleSelectDate(isoDate: string) {
    if (minDate && isoDate < minDate) {
      return;
    }
    if (maxDate && isoDate > maxDate) {
      return;
    }
    onChange(isoDate);
    setIsCalendarOpen(false);
  }

  function handlePreviousMonth() {
    const previous = shiftMonth(displayYear, displayMonth, -1);
    setDisplayMonth(previous.month);
    setDisplayYear(previous.year);
  }

  function handleNextMonth() {
    const next = shiftMonth(displayYear, displayMonth, 1);
    setDisplayMonth(next.month);
    setDisplayYear(next.year);
  }

  return (
    <div ref={containerRef} className="relative">
      <TooltipTarget text={`Scheduling date in ${TRADSPHERE_BROADCAST_TIMEZONE}. Calendar starts Monday.`}>
        <Input
          id={id}
          type="text"
          value={displayValue}
          readOnly
          onClick={handleToggleCalendar}
          disabled={disabled}
          className="cursor-pointer pr-9"
          placeholder="MM/DD/YYYY"
        />
      </TooltipTarget>
      <button
        type="button"
        onClick={handleToggleCalendar}
        disabled={disabled}
        className="absolute inset-y-0 right-0 inline-flex w-9 items-center justify-center rounded-r-md text-slate-500 transition-colors hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
        aria-label={`${isCalendarOpen ? "Close" : "Open"} ${label} date picker`}
      >
        <CalendarDays className="size-4" />
      </button>

      {isCalendarOpen && calendarPopoverStyle && typeof document !== "undefined"
        ? createPortal(
            <div
              ref={calendarPopoverRef}
              data-flight-date-picker-popover="true"
              className="pointer-events-auto rounded-xl border border-slate-200 bg-white p-3 shadow-xl"
              style={calendarPopoverStyle}
            >
              <div className="overflow-auto" style={{ maxHeight: calendarPopoverMaxHeight }}>
                <div className="mb-2 flex items-center justify-between">
                  <button
                    type="button"
                    onClick={handlePreviousMonth}
                    className="inline-flex h-8 w-8 cursor-pointer items-center justify-center rounded-md border border-slate-200 text-slate-700 transition-colors hover:bg-slate-100"
                    aria-label="Show previous month"
                  >
                    {"<"}
                  </button>
                  <p className="text-sm font-medium text-slate-800">{monthLabel}</p>
                  <button
                    type="button"
                    onClick={handleNextMonth}
                    className="inline-flex h-8 w-8 cursor-pointer items-center justify-center rounded-md border border-slate-200 text-slate-700 transition-colors hover:bg-slate-100"
                    aria-label="Show next month"
                  >
                    {">"}
                  </button>
                </div>

                <div className="mb-1 grid grid-cols-7">
                  {MONDAY_WEEKDAY_LABELS.map((dayLabel) => (
                    <div
                      key={dayLabel}
                      className="py-1 text-center text-xs font-medium uppercase tracking-wide text-slate-500"
                    >
                      {dayLabel}
                    </div>
                  ))}
                </div>

                <div className="grid grid-cols-7 gap-1">
                  {calendarDays.map((day) => {
                    const isSelected = day.isoDate === value;
                    const isToday = day.isoDate === toIsoDate(today.year, today.month, today.day);
                    const isOutOfRange = Boolean(
                      (minDate && day.isoDate < minDate)
                      || (maxDate && day.isoDate > maxDate),
                    );
                    return (
                      <button
                        key={day.isoDate}
                        type="button"
                        onClick={() => handleSelectDate(day.isoDate)}
                        disabled={isOutOfRange}
                        className={[
                          "h-9 rounded-md text-sm transition-colors",
                          day.inCurrentMonth ? "text-slate-800" : "text-slate-400",
                          isSelected
                            ? "bg-blue-600 font-semibold text-white hover:bg-blue-600"
                            : "hover:bg-slate-100",
                          !isSelected && isToday ? "border border-blue-300" : "",
                          isOutOfRange ? "cursor-not-allowed opacity-35 hover:bg-transparent" : "",
                        ].join(" ")}
                      >
                        {day.dayNumber}
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>,
            document.body,
          )
        : null}
    </div>
  );
}

function formatRangeDisplay(flightStart: string, flightEnd: string): string {
  const startDisplay = flightStart ? formatIsoDateForDisplay(flightStart) : "--";
  const endDisplay = flightEnd ? formatIsoDateForDisplay(flightEnd) : "--";
  if (!flightStart && !flightEnd) {
    return "Select date range";
  }
  return `${startDisplay} → ${endDisplay}`;
}

export function FlightDateRangeField({
  flightStart,
  flightEnd,
  onFlightStartChange,
  onFlightEndChange,
  flightRangePreset,
  onFlightRangePresetChange,
  onApplyFlightRangePreset,
  onFlightRangeError,
  defaultMonth,
  defaultYear,
  disabled,
}: {
  flightStart: string;
  flightEnd: string;
  onFlightStartChange: (nextValue: string) => void;
  onFlightEndChange: (nextValue: string) => void;
  flightRangePreset: FlightRangePresetState;
  onFlightRangePresetChange: (nextValue: FlightRangePresetState) => void;
  onApplyFlightRangePreset: (range: { flightStart: string; flightEnd: string }) => void;
  onFlightRangeError: (message: string | null) => void;
  defaultMonth: number;
  defaultYear: number;
  disabled: boolean;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [isRangePickerOpen, setIsRangePickerOpen] = useState(false);

  useEffect(() => {
    if (!isRangePickerOpen) {
      return;
    }

    function handleClickOutside(event: MouseEvent) {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      // AppDropdown menus render in a portal outside this container.
      // Keep this popover open while users interact with quick-range dropdowns.
      if (target instanceof Element) {
        if (target.closest('[data-app-dropdown-menu="true"]')) {
          return;
        }
      }
      if (!containerRef.current?.contains(target)) {
        setIsRangePickerOpen(false);
      }
    }

    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [isRangePickerOpen]);

  return (
    <div ref={containerRef} className="relative">
      <TooltipTarget text={`Scheduling dates in ${TRADSPHERE_BROADCAST_TIMEZONE}. Calendar starts Monday.`}>
        <Input
          id="estnum-flight-dates"
          type="text"
          value={formatRangeDisplay(flightStart, flightEnd)}
          readOnly
          onClick={() => {
            if (!disabled) {
              setIsRangePickerOpen((current) => !current);
            }
          }}
          disabled={disabled}
          className="cursor-pointer pr-10"
          placeholder="MM/DD/YYYY → MM/DD/YYYY"
        />
      </TooltipTarget>
      <button
        type="button"
        onClick={() => {
          if (!disabled) {
            setIsRangePickerOpen((current) => !current);
          }
        }}
        disabled={disabled}
        className="absolute inset-y-0 right-0 inline-flex w-10 items-center justify-center rounded-r-md text-slate-500 transition-colors hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
        aria-label="Toggle flight date range picker"
      >
        <CalendarDays className="size-4" />
      </button>

      {isRangePickerOpen ? (
        <div className="absolute left-0 top-[calc(100%+0.5rem)] z-40 w-full max-w-[430px] rounded-lg border border-slate-200 bg-white p-3 shadow-lg">
          <FlightRangeSelector
            value={flightRangePreset}
            onChange={onFlightRangePresetChange}
            onApply={onApplyFlightRangePreset}
            onError={onFlightRangeError}
            disabled={disabled}
            defaultMonth={defaultMonth}
            defaultYear={defaultYear}
            variant="compact-inline"
          />
          <div className="my-2 flex items-center gap-2 text-[11px] text-slate-400">
            <span className="h-px flex-1 bg-slate-200" />
            <span className="shrink-0">or</span>
            <span className="h-px flex-1 bg-slate-200" />
          </div>
          <p className="mb-2 text-xs text-slate-500">Pick a start and end date.</p>
          <div className="space-y-2">
            <div>
              <p className="mb-1 text-xs font-medium text-slate-600">Flight Start</p>
              <DateInputField
                id="estnum-flight-start"
                value={flightStart}
                onChange={onFlightStartChange}
                disabled={disabled}
                label="flight start"
              />
            </div>
            <div>
              <p className="mb-1 text-xs font-medium text-slate-600">Flight End</p>
              <DateInputField
                id="estnum-flight-end"
                value={flightEnd}
                onChange={onFlightEndChange}
                disabled={disabled}
                label="flight end"
              />
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
