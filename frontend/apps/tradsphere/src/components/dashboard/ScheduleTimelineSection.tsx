import { useEffect, useMemo, useRef, useState } from "react";
import {
  CalendarRange,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronUp,
  Loader2,
} from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useApiRequest } from "@/hooks/useApiRequest";
import { usePersistentState } from "@/hooks/usePersistentState";
import {
  listBrowserCacheSnapshotsByPrefix,
  readBrowserCacheSnapshot,
  removeBrowserCache,
  writeBrowserCache,
} from "@/lib/browserCache";
import { TRADSPHERE_BROADCAST_TIMEZONE } from "@/lib/broadcastCalendar";
import {
  TRADSPHERE_CACHE_TTL_MS,
  shouldFetchNetwork,
  type CachePolicy,
} from "@shared/cache";

type TimelineWeek = {
  weekStart: string;
  weekEnd: string;
  label: string;
};

type TimelineItem = {
  stationCode: string;
  stationName: string;
  estNum: string;
  mediaType: string;
  activeWeeks: string[];
};

type TimelineResponse = {
  accountCode: string;
  timezone: string;
  startDate: string;
  endDate: string;
  weeks: TimelineWeek[];
  items: TimelineItem[];
};

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type TimelineSegment = {
  startIndex: number;
  endIndex: number;
  activeWeeks: string[];
};

type TimelineDetail = {
  item: TimelineItem;
  segment: TimelineSegment;
  rangeStart: string;
  rangeEnd: string;
};

interface ScheduleTimelineSectionProps {
  accountCode: string;
  headers: HeadersInit;
  disabled?: boolean;
}

const TIMELINE_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.SCHEDULE_TIMELINE;
const TIMELINE_CACHE_SCHEMA = "v2";
const DEFAULT_WINDOW_WEEKS = 9;
const MAX_WINDOW_WEEKS = 13;
const TIMELINE_CACHE_ENTRY_CAP = 12;
const LABEL_COLUMN_PX = 260;
const WEEK_COLUMN_PX = 84;
const TIMELINE_COLLAPSED_STATE_KEY = "tradsphere.home.scheduleTimeline.collapsed.v1";
const TIMELINE_VISIBLE_START_KEY = "tradsphere.home.scheduleTimeline.visibleStart.v1";

const MONTH_LABEL_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: TRADSPHERE_BROADCAST_TIMEZONE,
  month: "long",
  year: "numeric",
});

const DATE_LABEL_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: TRADSPHERE_BROADCAST_TIMEZONE,
  month: "2-digit",
  day: "2-digit",
  year: "numeric",
});

type MonthGroup = {
  key: string;
  label: string;
  span: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return "";
}

function parseIsoDate(value: string): { year: number; month: number; day: number } | null {
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

  const lastDay = new Date(Date.UTC(year, month, 0)).getUTCDate();
  if (day < 1 || day > lastDay) {
    return null;
  }

  return { year, month, day };
}

function toIsoDate(year: number, month: number, day: number): string {
  return `${year.toString().padStart(4, "0")}-${month.toString().padStart(2, "0")}-${day
    .toString()
    .padStart(2, "0")}`;
}

function addDays(isoDate: string, days: number): string {
  const parsed = parseIsoDate(isoDate);
  if (!parsed) {
    return isoDate;
  }
  const next = new Date(Date.UTC(parsed.year, parsed.month - 1, parsed.day + days));
  return toIsoDate(next.getUTCFullYear(), next.getUTCMonth() + 1, next.getUTCDate());
}

function mondayOfIsoDate(isoDate: string): string {
  const parsed = parseIsoDate(isoDate);
  if (!parsed) {
    return isoDate;
  }
  const dateValue = new Date(Date.UTC(parsed.year, parsed.month - 1, parsed.day));
  const weekday = dateValue.getUTCDay();
  const mondayOffset = weekday === 0 ? -6 : 1 - weekday;
  dateValue.setUTCDate(dateValue.getUTCDate() + mondayOffset);
  return toIsoDate(dateValue.getUTCFullYear(), dateValue.getUTCMonth() + 1, dateValue.getUTCDate());
}

function getTodayInChicagoIso(): string {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: TRADSPHERE_BROADCAST_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts = formatter.formatToParts(new Date());
  const values = parts.reduce<Record<string, string>>((output, part) => {
    output[part.type] = part.value;
    return output;
  }, {});
  return `${values.year ?? "1970"}-${values.month ?? "01"}-${values.day ?? "01"}`;
}

function formatRelativeTime(timestamp: number): string {
  const seconds = Math.max(1, Math.floor((Date.now() - timestamp) / 1000));
  if (seconds < 60) {
    return `${seconds}s ago`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ago`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h ago`;
  }
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function buildWeeksFromStart(startMonday: string, count: number): TimelineWeek[] {
  const weeks: TimelineWeek[] = [];
  for (let index = 0; index < count; index += 1) {
    const weekStart = addDays(startMonday, index * 7);
    const weekEnd = addDays(weekStart, 6);
    const parsed = parseIsoDate(weekStart);
    weeks.push({
      weekStart,
      weekEnd,
      label: parsed ? `${parsed.month}/${parsed.day}` : weekStart,
    });
  }
  return weeks;
}

function isBoolean(value: unknown): value is boolean {
  return typeof value === "boolean";
}

function isIsoDateString(value: unknown): value is string {
  return typeof value === "string" && parseIsoDate(value) !== null;
}

function buildTimelineCacheKey(accountCode: string, startDate: string, endDate: string): string {
  return `tradsphere:schedule-timeline:${TIMELINE_CACHE_SCHEMA}:${accountCode.toUpperCase()}:${startDate}:${endDate}`;
}

function pruneTimelineCacheEntries(accountCode: string): void {
  const prefix = `tradsphere:schedule-timeline:${TIMELINE_CACHE_SCHEMA}:${accountCode.toUpperCase()}:`;
  const snapshots = listBrowserCacheSnapshotsByPrefix<unknown>(prefix, {
    allowExpired: true,
    limit: 200,
  });
  if (snapshots.length <= TIMELINE_CACHE_ENTRY_CAP) {
    return;
  }

  for (const entry of snapshots.slice(TIMELINE_CACHE_ENTRY_CAP)) {
    removeBrowserCache(entry.key);
  }
}

function normalizeWeek(payload: unknown): TimelineWeek | null {
  if (!isRecord(payload)) {
    return null;
  }
  const weekStart = asString(payload.weekStart);
  const weekEnd = asString(payload.weekEnd);
  const label = asString(payload.label);
  if (!weekStart || !weekEnd || !label) {
    return null;
  }
  return { weekStart, weekEnd, label };
}

function normalizeTimeline(payload: unknown): TimelineResponse | null {
  const unwrapped = isRecord(payload) && "data" in payload ? payload.data : payload;
  if (!isRecord(unwrapped)) {
    return null;
  }

  const accountCode = asString(unwrapped.accountCode).toUpperCase();
  const timezone = asString(unwrapped.timezone) || TRADSPHERE_BROADCAST_TIMEZONE;
  const startDate = asString(unwrapped.startDate);
  const endDate = asString(unwrapped.endDate);
  if (!accountCode || !startDate || !endDate) {
    return null;
  }

  const weeks = (Array.isArray(unwrapped.weeks) ? unwrapped.weeks : [])
    .map((week) => normalizeWeek(week))
    .filter((week): week is TimelineWeek => week !== null);
  const weekSet = new Set(weeks.map((week) => week.weekStart));

  const items = (Array.isArray(unwrapped.items) ? unwrapped.items : []).reduce<TimelineItem[]>((output, row) => {
    if (!isRecord(row)) {
      return output;
    }

    const stationCode = asString(row.stationCode).toUpperCase();
    const estNum = asString(row.estNum);
    if (!stationCode || !estNum) {
      return output;
    }

    const activeWeeks = (Array.isArray(row.activeWeeks) ? row.activeWeeks : [])
      .map((value) => asString(value))
      .filter((value) => value && weekSet.has(value));

    if (!activeWeeks.length) {
      return output;
    }

    const uniqueSortedWeeks = [...new Set(activeWeeks)].sort();
    output.push({
      stationCode,
      stationName: asString(row.stationName),
      estNum,
      mediaType: asString(row.mediaType).toUpperCase(),
      activeWeeks: uniqueSortedWeeks,
    });
    return output;
  }, []);

  items.sort((left, right) => {
    const stationCompare = left.stationCode.localeCompare(right.stationCode);
    if (stationCompare !== 0) {
      return stationCompare;
    }
    return left.estNum.localeCompare(right.estNum);
  });

  return {
    accountCode,
    timezone,
    startDate,
    endDate,
    weeks,
    items,
  };
}

function buildMonthGroups(weeks: TimelineWeek[]): MonthGroup[] {
  const groups: MonthGroup[] = [];
  for (const week of weeks) {
    const parsed = parseIsoDate(week.weekStart);
    if (!parsed) {
      continue;
    }
    const monthDate = new Date(Date.UTC(parsed.year, parsed.month - 1, 1));
    const key = `${parsed.year}-${parsed.month}`;
    const label = MONTH_LABEL_FORMATTER.format(monthDate);
    const previous = groups[groups.length - 1];
    if (previous && previous.key === key) {
      previous.span += 1;
      continue;
    }
    groups.push({ key, label, span: 1 });
  }
  return groups;
}

function buildSegments(activeWeeks: string[], weekIndexByStart: Map<string, number>): TimelineSegment[] {
  const indexedWeeks = activeWeeks
    .map((weekStart) => ({ index: weekIndexByStart.get(weekStart), weekStart }))
    .filter((item): item is { index: number; weekStart: string } => typeof item.index === "number")
    .sort((left, right) => left.index - right.index);

  if (!indexedWeeks.length) {
    return [];
  }

  const segments: TimelineSegment[] = [];
  let segmentStart = indexedWeeks[0].index;
  let segmentEnd = indexedWeeks[0].index;
  let segmentWeeks = [indexedWeeks[0].weekStart];

  for (let index = 1; index < indexedWeeks.length; index += 1) {
    const current = indexedWeeks[index].index;
    const currentWeek = indexedWeeks[index].weekStart;
    if (current === segmentEnd + 1) {
      segmentEnd = current;
      if (currentWeek) {
        segmentWeeks.push(currentWeek);
      }
      continue;
    }

    segments.push({
      startIndex: segmentStart,
      endIndex: segmentEnd,
      activeWeeks: segmentWeeks.filter(Boolean),
    });
    segmentStart = current;
    segmentEnd = current;
    segmentWeeks = currentWeek ? [currentWeek] : [];
  }

  segments.push({
    startIndex: segmentStart,
    endIndex: segmentEnd,
    activeWeeks: segmentWeeks.filter(Boolean),
  });
  return segments;
}

function colorForRow(seed: string): {
  background: string;
  border: string;
} {
  let hash = 0;
  for (let index = 0; index < seed.length; index += 1) {
    hash = (hash * 31 + seed.charCodeAt(index)) % 360;
  }
  const hue = (hash + 360) % 360;
  return {
    background: `hsl(${hue} 76% 84%)`,
    border: `hsl(${hue} 58% 44%)`,
  };
}

function formatPeriodRange(startDate: string, endDate: string): string {
  const start = parseIsoDate(startDate);
  const end = parseIsoDate(endDate);
  if (!start || !end) {
    return `${startDate} - ${endDate}`;
  }

  const startText = DATE_LABEL_FORMATTER.format(new Date(Date.UTC(start.year, start.month - 1, start.day)));
  const endText = DATE_LABEL_FORMATTER.format(new Date(Date.UTC(end.year, end.month - 1, end.day)));
  return `${startText} - ${endText}`;
}

export function ScheduleTimelineSection({
  accountCode,
  headers,
  disabled = false,
}: ScheduleTimelineSectionProps) {
  const { requestJson } = useApiRequest();
  const currentWeekStart = useMemo(() => mondayOfIsoDate(getTodayInChicagoIso()), []);
  const [isCollapsed, setIsCollapsed] = usePersistentState<boolean>(
    TIMELINE_COLLAPSED_STATE_KEY,
    false,
    { storage: "session", validate: isBoolean },
  );
  const [visibleStart, setVisibleStart] = usePersistentState<string>(
    TIMELINE_VISIBLE_START_KEY,
    currentWeekStart,
    { storage: "session", validate: isIsoDateString },
  );

  const [timeline, setTimeline] = useState<TimelineResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [selectedDetail, setSelectedDetail] = useState<TimelineDetail | null>(null);
  const requestIdRef = useRef(0);

  const requestedEnd = useMemo(
    () => addDays(visibleStart, DEFAULT_WINDOW_WEEKS * 7 - 1),
    [visibleStart],
  );

  const cacheKey = useMemo(() => {
    if (!accountCode) {
      return "";
    }
    return buildTimelineCacheKey(accountCode, visibleStart, requestedEnd);
  }, [accountCode, requestedEnd, visibleStart]);

  useEffect(() => {
    if (!accountCode) {
      setTimeline(null);
      setCacheStatus(null);
      setError(null);
      setIsLoading(false);
      setIsRefreshing(false);
      return;
    }

    if (isCollapsed) {
      return;
    }

    void fetchTimeline("stale-while-revalidate");
  }, [accountCode, cacheKey, isCollapsed]);

  async function fetchTimeline(policy: CachePolicy): Promise<void> {
    if (!accountCode || !cacheKey) {
      return;
    }

    const requestId = ++requestIdRef.current;
    const snapshot = readBrowserCacheSnapshot<unknown>(cacheKey);
    const cachedTimeline = normalizeTimeline(snapshot?.data);
    const canUseCache = policy !== "network-only" && cachedTimeline !== null;
    const shouldFetchFromNetwork = shouldFetchNetwork(policy, snapshot);

    if (canUseCache && cachedTimeline) {
      setTimeline(cachedTimeline);
      setCacheStatus({ source: "cache", fetchedAt: snapshot?.fetchedAt ?? Date.now() });
      setError(null);
      if (visibleStart !== cachedTimeline.startDate) {
        setVisibleStart(cachedTimeline.startDate);
      }
    }

    if (!shouldFetchFromNetwork) {
      setIsLoading(false);
      setIsRefreshing(false);
      return;
    }

    setIsLoading(!canUseCache);
    setIsRefreshing(canUseCache);
    if (!canUseCache) {
      setError(null);
    }

    try {
      const query = new URLSearchParams({
        accountCode: accountCode.toUpperCase(),
        startDate: visibleStart,
        endDate: requestedEnd,
        timezone: TRADSPHERE_BROADCAST_TIMEZONE,
      });

      const payload = await requestJson(`/api/tradsphere/v1/schedules/timeline?${query.toString()}`, {
        headers,
      });
      const normalized = normalizeTimeline(payload);
      if (!normalized) {
        throw new Error("Timeline response was empty or invalid.");
      }

      writeBrowserCache(cacheKey, normalized, TIMELINE_CACHE_TTL_MS, { source: "network" });
      pruneTimelineCacheEntries(accountCode);

      if (requestId !== requestIdRef.current) {
        return;
      }

      setTimeline(normalized);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });
      setError(null);
      if (visibleStart !== normalized.startDate) {
        setVisibleStart(normalized.startDate);
      }
    } catch (requestError) {
      if (requestId !== requestIdRef.current) {
        return;
      }
      const message =
        requestError instanceof Error && requestError.message
          ? requestError.message
          : "Unable to load schedule timeline.";
      if (!canUseCache) {
        setTimeline(null);
        setError(message);
      } else {
        setError(null);
      }
    } finally {
      if (requestId === requestIdRef.current) {
        setIsLoading(false);
        setIsRefreshing(false);
      }
    }
  }

  const weeks = timeline?.weeks ?? buildWeeksFromStart(visibleStart, DEFAULT_WINDOW_WEEKS);
  const items = timeline?.items ?? [];
  const timelineStart = timeline?.startDate ?? visibleStart;
  const timelineEnd = timeline?.endDate ?? requestedEnd;
  const periodText = formatPeriodRange(timelineStart, timelineEnd);
  const canInteract = Boolean(accountCode) && !disabled;

  const statusText = useMemo(() => {
    if (isLoading) {
      return "Loading...";
    }
    if (isRefreshing) {
      return "Refreshing...";
    }
    if (!cacheStatus) {
      return null;
    }
    return `Timeline source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [cacheStatus, isLoading, isRefreshing]);

  const monthGroups = useMemo(() => buildMonthGroups(weeks), [weeks]);
  const weekIndexByStart = useMemo(() => {
    const output = new Map<string, number>();
    weeks.forEach((week, index) => {
      output.set(week.weekStart, index);
    });
    return output;
  }, [weeks]);

  const timelinePixelWidth = weeks.length * WEEK_COLUMN_PX;

  return (
    <>
      <Card>
        <CardHeader className="border-b border-blue-100 bg-secondary/60 py-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <CardTitle className="flex items-center gap-3">
              <CalendarRange className="size-5 text-blue-600" />
              <span className="text-lg font-semibold text-blue-700">Schedule Timeline</span>
            </CardTitle>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setVisibleStart((current) => addDays(current, -DEFAULT_WINDOW_WEEKS * 7))}
                disabled={!canInteract || isLoading}
              >
                <ChevronLeft className="size-4" />
                Previous period
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setVisibleStart((current) => addDays(current, DEFAULT_WINDOW_WEEKS * 7))}
                disabled={!canInteract || isLoading}
              >
                Next period
                <ChevronRight className="size-4" />
              </Button>
              <Button
                variant="secondary"
                size="sm"
                onClick={() => setVisibleStart(currentWeekStart)}
                disabled={!canInteract || isLoading}
              >
                Current period
              </Button>
              <Button
                variant="ghost"
                size="icon"
                aria-label={isCollapsed ? "Expand schedule timeline" : "Collapse schedule timeline"}
                onClick={() => setIsCollapsed((current) => !current)}
              >
                {isCollapsed ? <ChevronDown className="size-4" /> : <ChevronUp className="size-4" />}
              </Button>
            </div>
          </div>
          <p className="pt-2 text-sm text-slate-600">
            Weekly broadcast schedule activity by station and estimate number.
          </p>
          <p className="text-xs font-medium text-slate-500">
            {periodText} · Bounded window ({DEFAULT_WINDOW_WEEKS} weeks default, {MAX_WINDOW_WEEKS} weeks max)
          </p>
        </CardHeader>

        {!isCollapsed ? (
          <CardContent className="space-y-4 p-5">
            {!accountCode ? (
              <p className="text-sm text-slate-500">Load an account to view schedule timeline.</p>
            ) : null}

            {accountCode && error ? <p className="text-sm text-rose-600">{error}</p> : null}

            {accountCode && isLoading && !timeline ? (
              <div className="flex items-center gap-2 rounded-md border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
                <Loader2 className="size-4 animate-spin text-blue-600" />
                Loading timeline data...
              </div>
            ) : null}

            {accountCode && !isLoading && timeline && !items.length ? (
              <p className="text-sm text-slate-500">No scheduled activity found for this period.</p>
            ) : null}

            {accountCode && items.length > 0 ? (
              <div className="overflow-hidden rounded-lg border border-slate-200">
                <div className="max-h-[30rem] overflow-auto">
                  <div className="min-w-max" style={{ width: LABEL_COLUMN_PX + timelinePixelWidth }}>
                    <div className="sticky top-0 z-30 border-b border-slate-200 bg-slate-100/95 backdrop-blur">
                      <div className="flex h-8 border-b border-slate-200">
                        <div
                          className="sticky left-0 z-40 flex items-center border-r border-slate-200 bg-slate-100 px-3 text-xs font-semibold uppercase tracking-wide text-slate-600"
                          style={{ width: LABEL_COLUMN_PX, minWidth: LABEL_COLUMN_PX }}
                        >
                          Station / EstNum
                        </div>
                        <div className="flex" style={{ width: timelinePixelWidth, minWidth: timelinePixelWidth }}>
                          {monthGroups.map((group) => (
                            <div
                              key={group.key}
                              className="flex items-center justify-center border-r border-slate-200 text-[11px] font-semibold uppercase tracking-wide text-slate-600"
                              style={{ width: group.span * WEEK_COLUMN_PX }}
                            >
                              {group.label}
                            </div>
                          ))}
                        </div>
                      </div>
                      <div className="flex h-9">
                        <div
                          className="sticky left-0 z-40 border-r border-slate-200 bg-slate-50"
                          style={{ width: LABEL_COLUMN_PX, minWidth: LABEL_COLUMN_PX }}
                        />
                        <div className="flex" style={{ width: timelinePixelWidth, minWidth: timelinePixelWidth }}>
                          {weeks.map((week) => {
                            const isCurrentWeek = week.weekStart === currentWeekStart;
                            return (
                              <div
                                key={week.weekStart}
                                className={`flex items-center justify-center border-r border-slate-200 text-xs font-semibold text-slate-600 ${
                                  isCurrentWeek ? "bg-blue-100 text-blue-800" : "bg-slate-50"
                                }`}
                                style={{ width: WEEK_COLUMN_PX, minWidth: WEEK_COLUMN_PX }}
                              >
                                {week.label}
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    </div>

                    <div>
                      {items.map((item) => {
                        const color = colorForRow(`${item.stationCode}:${item.estNum}`);
                        const segments = buildSegments(item.activeWeeks, weekIndexByStart);

                        return (
                          <div key={`${item.stationCode}:${item.estNum}`} className="flex border-b border-slate-200 last:border-b-0">
                            <div
                              className="sticky left-0 z-20 border-r border-slate-200 bg-white px-3 py-2"
                              style={{ width: LABEL_COLUMN_PX, minWidth: LABEL_COLUMN_PX }}
                            >
                              <p className="text-sm font-medium text-slate-900">
                                {item.stationCode} · {item.estNum}
                              </p>
                              <p className="text-xs text-slate-500">{item.stationName || "Station name unavailable"}</p>
                            </div>

                            <div className="relative h-14" style={{ width: timelinePixelWidth, minWidth: timelinePixelWidth }}>
                              <div className="absolute inset-0 flex">
                                {weeks.map((week) => {
                                  const isCurrentWeek = week.weekStart === currentWeekStart;
                                  return (
                                    <div
                                      key={`${item.stationCode}:${item.estNum}:${week.weekStart}`}
                                      className={`h-full border-r border-slate-200 ${isCurrentWeek ? "bg-blue-50/70" : "bg-white"}`}
                                      style={{ width: WEEK_COLUMN_PX, minWidth: WEEK_COLUMN_PX }}
                                    />
                                  );
                                })}
                              </div>

                              {segments.map((segment, segmentIndex) => {
                                const left = segment.startIndex * WEEK_COLUMN_PX + 6;
                                const width = (segment.endIndex - segment.startIndex + 1) * WEEK_COLUMN_PX - 12;
                                const rangeStart = segment.activeWeeks[0] ?? weeks[segment.startIndex]?.weekStart ?? "";
                                const rangeEnd = segment.activeWeeks[segment.activeWeeks.length - 1] ?? weeks[segment.endIndex]?.weekEnd ?? "";

                                return (
                                  <button
                                    key={`${item.stationCode}:${item.estNum}:segment:${segmentIndex}`}
                                    type="button"
                                    className="absolute top-3 h-8 rounded-full border shadow-sm transition-all hover:brightness-95 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                                    style={{
                                      left,
                                      width: Math.max(width, 16),
                                      backgroundColor: color.background,
                                      borderColor: color.border,
                                    }}
                                    title={`${item.stationCode} · ${item.estNum} · ${rangeStart} to ${rangeEnd}`}
                                    onClick={() =>
                                      setSelectedDetail({
                                        item,
                                        segment,
                                        rangeStart,
                                        rangeEnd,
                                      })
                                    }
                                  />
                                );
                              })}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>
              </div>
            ) : null}

            {statusText ? (
              <div className="flex justify-end">
                <CacheStatusChip
                  text={statusText}
                  onRefresh={() => {
                    void fetchTimeline("network-only");
                  }}
                  disabled={!canInteract || isRefreshing || isLoading}
                  refreshing={isRefreshing}
                  refreshLabel="Refresh timeline"
                  tooltipText="Refresh timeline for the current account and visible range"
                  className="max-w-[min(90vw,34rem)]"
                />
              </div>
            ) : null}
          </CardContent>
        ) : null}
      </Card>

      <Dialog open={Boolean(selectedDetail)} onOpenChange={(open) => !open && setSelectedDetail(null)}>
        <DialogContent className="max-w-xl">
          <DialogHeader>
            <DialogTitle>Schedule Timeline Detail</DialogTitle>
            <DialogDescription>
              Read-only active range detail for the selected timeline segment.
            </DialogDescription>
          </DialogHeader>

          {selectedDetail ? (
            <div className="grid grid-cols-1 gap-2 text-sm text-slate-700 sm:grid-cols-2">
              <p>
                <span className="font-semibold text-slate-900">Station code:</span> {selectedDetail.item.stationCode}
              </p>
              <p>
                <span className="font-semibold text-slate-900">Station name:</span> {selectedDetail.item.stationName || "N/A"}
              </p>
              <p>
                <span className="font-semibold text-slate-900">EstNum:</span> {selectedDetail.item.estNum}
              </p>
              <p>
                <span className="font-semibold text-slate-900">Media type:</span> {selectedDetail.item.mediaType || "N/A"}
              </p>
              <p>
                <span className="font-semibold text-slate-900">Active range start:</span> {selectedDetail.rangeStart}
              </p>
              <p>
                <span className="font-semibold text-slate-900">Active range end:</span> {selectedDetail.rangeEnd}
              </p>
              <p className="sm:col-span-2">
                <span className="font-semibold text-slate-900">Related active weeks:</span>{" "}
                {selectedDetail.segment.activeWeeks.join(", ") || "N/A"}
              </p>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </>
  );
}
