import { useEffect, useMemo, useRef, useState } from "react";
import {
  CalendarRange,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronUp,
  Loader2,
  X,
} from "lucide-react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useApiRequest } from "@/hooks/useApiRequest";
import { useOnlineStatus } from "@/hooks/useOnlineStatus";
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
import { TooltipTarget } from "@shared/components/actions/TooltipTarget";
import { SectionCard } from "@shared/components/layout/SectionCard";
import type { EsnumItem } from "./types";

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

type EstGroup = {
  estNum: string;
  estNumName: string;
  estNumNote: string;
  mediaType: string;
  rangeStart: string;
  rangeEnd: string;
  stations: TimelineItem[];
};

type GroupCollapseState = Record<string, boolean>;

interface ScheduleTimelineSectionProps {
  accountCode: string;
  esnums?: EsnumItem[];
  headers: HeadersInit;
  disabled?: boolean;
  presentation?: "card" | "table-only";
  anchorStartDate?: string | null;
  anchorEndDate?: string | null;
  onLoadingChange?: (isLoading: boolean) => void;
}

const TIMELINE_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.SCHEDULE_TIMELINE;
const TIMELINE_CACHE_SCHEMA = "v3";
const DEFAULT_WINDOW_WEEKS = 9;
const MAX_EXPANDED_WEEKS = 26;
const TIMELINE_CACHE_ENTRY_CAP = 18;
const LABEL_COLUMN_PX = 300;
const WEEK_COLUMN_PX = 84;
const TIMELINE_COLLAPSED_STATE_KEY = "tradsphere.home.scheduleTimeline.collapsed.v1";
const TIMELINE_VISIBLE_START_KEY = "tradsphere.home.scheduleTimeline.visibleStart.v1";
const TIMELINE_GROUP_COLLAPSE_KEY = "tradsphere.home.scheduleTimeline.groupCollapse.v2";

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

function toStableUtcDate(year: number, month: number, day: number): Date {
  // Use UTC noon so timezone rendering never shifts date/month backward.
  return new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
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

function isGroupCollapseState(value: unknown): value is GroupCollapseState {
  if (!isRecord(value)) {
    return false;
  }
  return Object.values(value).every((item) => typeof item === "boolean");
}

function buildTimelineCacheKey(accountCode: string, startDate: string, endDate: string): string {
  return `tradsphere:schedule-timeline:${TIMELINE_CACHE_SCHEMA}:${accountCode.toUpperCase()}:${startDate}:${endDate}`;
}

function pruneTimelineCacheEntries(accountCode: string): void {
  const prefix = `tradsphere:schedule-timeline:${TIMELINE_CACHE_SCHEMA}:${accountCode.toUpperCase()}:`;
  const snapshots = listBrowserCacheSnapshotsByPrefix<unknown>(prefix, {
    allowExpired: true,
    limit: 240,
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
    .filter((week): week is TimelineWeek => week !== null)
    .sort((left, right) => left.weekStart.localeCompare(right.weekStart));

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
    const estCompare = left.estNum.localeCompare(right.estNum, "en", {
      numeric: true,
      sensitivity: "base",
    });
    if (estCompare !== 0) {
      return estCompare;
    }
    return left.stationCode.localeCompare(right.stationCode);
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
    const monthDate = toStableUtcDate(parsed.year, parsed.month, 1);
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

function formatIsoDateMmDdYyyy(isoDate: string): string {
  const parsed = parseIsoDate(isoDate);
  if (!parsed) {
    return isoDate;
  }
  return DATE_LABEL_FORMATTER.format(toStableUtcDate(parsed.year, parsed.month, parsed.day));
}

function mergeTimelineData(current: TimelineResponse | null, incoming: TimelineResponse): TimelineResponse {
  if (!current) {
    return incoming;
  }

  const weekByStart = new Map<string, TimelineWeek>();
  for (const week of current.weeks) {
    weekByStart.set(week.weekStart, week);
  }
  for (const week of incoming.weeks) {
    weekByStart.set(week.weekStart, week);
  }

  const mergedWeeks = [...weekByStart.values()].sort((left, right) => left.weekStart.localeCompare(right.weekStart));
  const mergedWeekStarts = new Set(mergedWeeks.map((week) => week.weekStart));

  const itemByKey = new Map<string, TimelineItem>();
  for (const row of [...current.items, ...incoming.items]) {
    const key = `${row.estNum}::${row.stationCode}`;
    const existing = itemByKey.get(key);
    if (!existing) {
      itemByKey.set(key, {
        stationCode: row.stationCode,
        stationName: row.stationName,
        estNum: row.estNum,
        mediaType: row.mediaType,
        activeWeeks: [...new Set(row.activeWeeks)].filter((week) => mergedWeekStarts.has(week)).sort(),
      });
      continue;
    }

    const mergedActiveWeeks = [...new Set([...existing.activeWeeks, ...row.activeWeeks])]
      .filter((week) => mergedWeekStarts.has(week))
      .sort();

    itemByKey.set(key, {
      stationCode: existing.stationCode,
      stationName: existing.stationName || row.stationName,
      estNum: existing.estNum,
      mediaType: existing.mediaType || row.mediaType,
      activeWeeks: mergedActiveWeeks,
    });
  }

  const mergedItems = [...itemByKey.values()].filter((item) => item.activeWeeks.length > 0);
  mergedItems.sort((left, right) => {
    const estCompare = left.estNum.localeCompare(right.estNum, "en", {
      numeric: true,
      sensitivity: "base",
    });
    if (estCompare !== 0) {
      return estCompare;
    }
    return left.stationCode.localeCompare(right.stationCode);
  });

  return {
    accountCode: incoming.accountCode || current.accountCode,
    timezone: incoming.timezone || current.timezone,
    startDate: mergedWeeks[0]?.weekStart ?? current.startDate,
    endDate: mergedWeeks[mergedWeeks.length - 1]?.weekEnd ?? current.endDate,
    weeks: mergedWeeks,
    items: mergedItems,
  };
}

function clampInitialAnchor(anchorStart: string): string {
  return mondayOfIsoDate(anchorStart);
}

export function ScheduleTimelineSection({
  accountCode,
  esnums = [],
  headers,
  disabled = false,
  presentation = "card",
  anchorStartDate = null,
  anchorEndDate = null,
  onLoadingChange,
}: ScheduleTimelineSectionProps) {
  const { requestJson } = useApiRequest();
  const { isOnline } = useOnlineStatus();
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
  const [collapsedGroups, setCollapsedGroups] = usePersistentState<GroupCollapseState>(
    TIMELINE_GROUP_COLLAPSE_KEY,
    {},
    { storage: "session", validate: isGroupCollapseState },
  );

  const [timeline, setTimeline] = useState<TimelineResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isLoadingPrevious, setIsLoadingPrevious] = useState(false);
  const [isLoadingNext, setIsLoadingNext] = useState(false);
  const [rangeLimitMessage, setRangeLimitMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [selectedDetail, setSelectedDetail] = useState<TimelineDetail | null>(null);

  const loadedWindowKeysRef = useRef<Set<string>>(new Set());
  const loadedWindowsRef = useRef<Array<{ start: string; end: string }>>([]);
  const scrollContainerRef = useRef<HTMLDivElement | null>(null);
  const requestIdRef = useRef(0);
  const isTableOnlyPresentation = presentation === "table-only";
  const normalizedAnchorStart = useMemo(() => {
    const raw = asString(anchorStartDate);
    return raw ? mondayOfIsoDate(raw) : "";
  }, [anchorStartDate]);
  const normalizedAnchorEnd = useMemo(() => {
    const raw = asString(anchorEndDate);
    return raw || "";
  }, [anchorEndDate]);

  const canInteract = Boolean(accountCode) && !disabled;

  const fallbackWeeks = useMemo(
    () => buildWeeksFromStart(clampInitialAnchor(visibleStart), DEFAULT_WINDOW_WEEKS),
    [visibleStart],
  );
  const weeks = timeline?.weeks.length ? timeline.weeks : fallbackWeeks;
  const items = timeline?.items ?? [];

  const maxReached = weeks.length >= MAX_EXPANDED_WEEKS;

  const statusText = useMemo(() => {
    if (isLoading) {
      return "Loading...";
    }
    if (isRefreshing) {
      return "Refreshing...";
    }
    if (!isOnline && cacheStatus) {
      return `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
    }
    if (!cacheStatus) {
      return null;
    }
    return `Timeline source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [cacheStatus, isLoading, isOnline, isRefreshing]);

  const monthGroups = useMemo(() => buildMonthGroups(weeks), [weeks]);
  const weekIndexByStart = useMemo(() => {
    const output = new Map<string, number>();
    weeks.forEach((week, index) => {
      output.set(week.weekStart, index);
    });
    return output;
  }, [weeks]);

  const weekByStart = useMemo(() => {
    const output = new Map<string, TimelineWeek>();
    for (const week of weeks) {
      output.set(week.weekStart, week);
    }
    return output;
  }, [weeks]);

  const groupedRows = useMemo<EstGroup[]>(() => {
    const esnumMetaByKey = new Map<string, { name: string; note: string }>();
    for (const esnum of esnums) {
      const key = String(esnum.estnum);
      esnumMetaByKey.set(key, {
        name: String(esnum.name ?? "").trim(),
        note: String(esnum.note ?? "").trim(),
      });
    }

    const groupMap = new Map<string, TimelineItem[]>();
    for (const row of items) {
      const list = groupMap.get(row.estNum);
      if (list) {
        list.push(row);
      } else {
        groupMap.set(row.estNum, [row]);
      }
    }

    const groups: EstGroup[] = [];
    for (const [estNum, stations] of groupMap.entries()) {
      stations.sort((left, right) => left.stationCode.localeCompare(right.stationCode));
      const mediaTypes = [...new Set(stations.map((row) => row.mediaType).filter(Boolean))];
      const allWeeks = [...new Set(stations.flatMap((row) => row.activeWeeks))].sort();
      const firstWeek = allWeeks[0];
      const lastWeek = allWeeks[allWeeks.length - 1];
      const rangeStart = firstWeek ?? "";
      const rangeEnd = lastWeek ? weekByStart.get(lastWeek)?.weekEnd ?? lastWeek : "";
      const estnumMeta = esnumMetaByKey.get(estNum);

      groups.push({
        estNum,
        estNumName: estnumMeta?.name || estNum,
        estNumNote: estnumMeta?.note || "",
        mediaType: mediaTypes.join(" / "),
        rangeStart,
        rangeEnd,
        stations,
      });
    }

    groups.sort((left, right) =>
      left.estNum.localeCompare(right.estNum, "en", {
        numeric: true,
        sensitivity: "base",
      }),
    );
    return groups;
  }, [esnums, items, weekByStart]);

  const timelinePixelWidth = weeks.length * WEEK_COLUMN_PX;

  function isGroupCollapsed(estNum: string): boolean {
    const groupKey = `${accountCode.toUpperCase()}::${estNum}`;
    // Default behavior: collapsed unless explicitly expanded.
    return collapsedGroups[groupKey] !== false;
  }

  function setGroupCollapsed(estNum: string, collapsed: boolean): void {
    const groupKey = `${accountCode.toUpperCase()}::${estNum}`;
    setCollapsedGroups((current) => ({
      ...current,
      [groupKey]: collapsed,
    }));
  }

  function applyWindowData(incoming: TimelineResponse, mode: "replace" | "prepend" | "append"): number {
    let prependedWeeks = 0;
    setTimeline((current) => {
      if (mode === "replace" || !current) {
        return incoming;
      }

      const currentFirstWeek = current.weeks[0]?.weekStart;
      const merged = mergeTimelineData(current, incoming);
      if (mode === "prepend" && currentFirstWeek) {
        prependedWeeks = merged.weeks.filter((week) => week.weekStart < currentFirstWeek).length;
      }
      return merged;
    });
    return prependedWeeks;
  }

  function upsertLoadedWindow(start: string, end: string): void {
    const nextKey = `${start}::${end}`;
    const withoutDuplicate = loadedWindowsRef.current.filter(
      (windowValue) => `${windowValue.start}::${windowValue.end}` !== nextKey,
    );
    withoutDuplicate.push({ start, end });
    withoutDuplicate.sort((left, right) => left.start.localeCompare(right.start));
    loadedWindowsRef.current = withoutDuplicate;
  }

  async function fetchWindow({
    startDate,
    endDate,
    mode,
    policy,
    requestId,
  }: {
    startDate: string;
    endDate: string;
    mode: "replace" | "prepend" | "append";
    policy: CachePolicy;
    requestId: number;
  }): Promise<void> {
    const normalizedStart = mondayOfIsoDate(startDate);
    const normalizedEnd = endDate;
    const cacheKey = buildTimelineCacheKey(accountCode, normalizedStart, normalizedEnd);

    const snapshot = readBrowserCacheSnapshot<unknown>(cacheKey);
    const cachedTimeline = normalizeTimeline(snapshot?.data);
    const canUseCache = policy !== "network-only" && cachedTimeline !== null;
    const shouldFetchFromNetwork = shouldFetchNetwork(policy, snapshot);

    if (canUseCache && cachedTimeline) {
      const prependedWeeks = applyWindowData(cachedTimeline, mode);
      loadedWindowKeysRef.current.add(cacheKey);
      upsertLoadedWindow(normalizedStart, normalizedEnd);
      setCacheStatus({ source: "cache", fetchedAt: snapshot?.fetchedAt ?? Date.now() });
      setError(null);

      if (mode === "prepend" && prependedWeeks > 0) {
        const previousLeft = scrollContainerRef.current?.scrollLeft ?? 0;
        requestAnimationFrame(() => {
          if (!scrollContainerRef.current) {
            return;
          }
          scrollContainerRef.current.scrollLeft = previousLeft + prependedWeeks * WEEK_COLUMN_PX;
        });
      }
    }

    if (!shouldFetchFromNetwork) {
      return;
    }
    if (!isOnline) {
      if (!canUseCache && mode === "replace") {
        setTimeline(null);
        setError("You're offline. Connect to load schedule timeline.");
      }
      return;
    }

    try {
      const query = new URLSearchParams({
        accountCode: accountCode.toUpperCase(),
        startDate: normalizedStart,
        endDate: normalizedEnd,
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

      const prependedWeeks = applyWindowData(normalized, mode);
      loadedWindowKeysRef.current.add(cacheKey);
      upsertLoadedWindow(normalizedStart, normalizedEnd);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });
      setError(null);

      if (mode === "prepend" && prependedWeeks > 0) {
        const previousLeft = scrollContainerRef.current?.scrollLeft ?? 0;
        requestAnimationFrame(() => {
          if (!scrollContainerRef.current) {
            return;
          }
          scrollContainerRef.current.scrollLeft = previousLeft + prependedWeeks * WEEK_COLUMN_PX;
        });
      }
    } catch (requestError) {
      if (requestId !== requestIdRef.current) {
        return;
      }

      const message =
        requestError instanceof Error && requestError.message
          ? requestError.message
          : "Unable to load schedule timeline.";
      if (!canUseCache && mode === "replace") {
        setTimeline(null);
      }
      setError(message);
    }
  }

  async function loadInitialWindow(policy: CachePolicy): Promise<void> {
    if (!accountCode || (isCollapsed && !isTableOnlyPresentation)) {
      return;
    }

    const requestId = ++requestIdRef.current;
    const useAnchoredRange =
      isTableOnlyPresentation
      && normalizedAnchorStart
      && normalizedAnchorEnd;
    const start = useAnchoredRange
      ? normalizedAnchorStart
      : clampInitialAnchor(visibleStart);
    const end = useAnchoredRange
      ? normalizedAnchorEnd
      : addDays(start, DEFAULT_WINDOW_WEEKS * 7 - 1);

    setRangeLimitMessage(null);
    setIsLoading(true);
    setIsRefreshing(false);
    setError(null);
    setTimeline(null);
    loadedWindowKeysRef.current = new Set();
    loadedWindowsRef.current = [];

    await fetchWindow({
      startDate: start,
      endDate: end,
      mode: "replace",
      policy,
      requestId,
    });

    if (requestId === requestIdRef.current) {
      setIsLoading(false);
      setIsRefreshing(false);
    }
  }

  async function loadAdjacentWindow(direction: "previous" | "next"): Promise<void> {
    if (!canInteract || isLoading || isLoadingPrevious || isLoadingNext) {
      return;
    }

    const baseWeeks = timeline?.weeks.length ?? fallbackWeeks.length;
    const remainingWeeks = MAX_EXPANDED_WEEKS - baseWeeks;
    if (remainingWeeks <= 0) {
      setRangeLimitMessage("Maximum timeline range loaded.");
      return;
    }
    const requestWeeks = Math.min(DEFAULT_WINDOW_WEEKS, remainingWeeks);

    setRangeLimitMessage(null);
    const requestId = ++requestIdRef.current;

    const effectiveStart = timeline?.startDate ?? fallbackWeeks[0]?.weekStart ?? clampInitialAnchor(visibleStart);
    const effectiveEnd =
      timeline?.endDate ?? fallbackWeeks[fallbackWeeks.length - 1]?.weekEnd ?? addDays(effectiveStart, DEFAULT_WINDOW_WEEKS * 7 - 1);

    const adjacentStart =
      direction === "previous"
        ? addDays(mondayOfIsoDate(effectiveStart), -requestWeeks * 7)
        : addDays(mondayOfIsoDate(effectiveEnd), 1);
    const adjacentEnd = addDays(adjacentStart, requestWeeks * 7 - 1);
    const cacheKey = buildTimelineCacheKey(accountCode, adjacentStart, adjacentEnd);

    if (loadedWindowKeysRef.current.has(cacheKey)) {
      return;
    }

    if (direction === "previous") {
      setIsLoadingPrevious(true);
    } else {
      setIsLoadingNext(true);
    }

    await fetchWindow({
      startDate: adjacentStart,
      endDate: adjacentEnd,
      mode: direction === "previous" ? "prepend" : "append",
      policy: "stale-while-revalidate",
      requestId,
    });

    if (requestId === requestIdRef.current) {
      if (direction === "previous") {
        setIsLoadingPrevious(false);
      } else {
        setIsLoadingNext(false);
      }
    }
  }

  async function handleRefreshVisibleWindows(): Promise<void> {
    if (!accountCode || !timeline || !timeline.weeks.length) {
      return;
    }

    const requestId = ++requestIdRef.current;
    setIsRefreshing(true);

    const windows = loadedWindowsRef.current.map((windowValue, index) => ({
      start: windowValue.start,
      end: windowValue.end,
      mode: index === 0 ? ("replace" as const) : ("append" as const),
    }));
    if (!windows.length) {
      return;
    }

    loadedWindowKeysRef.current = new Set();
    loadedWindowsRef.current = [];

    for (const windowRequest of windows) {
      await fetchWindow({
        startDate: windowRequest.start,
        endDate: windowRequest.end,
        mode: windowRequest.mode,
        policy: "network-only",
        requestId,
      });
      if (requestId !== requestIdRef.current) {
        break;
      }
    }

    if (requestId === requestIdRef.current) {
      setIsRefreshing(false);
    }
  }

  function handleCurrentPeriod(): void {
    if (visibleStart === currentWeekStart) {
      void loadInitialWindow("stale-while-revalidate");
      return;
    }
    setVisibleStart(currentWeekStart);
  }

  useEffect(() => {
    if (!accountCode) {
      setTimeline(null);
      setCacheStatus(null);
      setError(null);
      setIsLoading(false);
      setIsRefreshing(false);
      setIsLoadingPrevious(false);
      setIsLoadingNext(false);
      setRangeLimitMessage(null);
      loadedWindowKeysRef.current = new Set();
      loadedWindowsRef.current = [];
      return;
    }

    if (isCollapsed && !isTableOnlyPresentation) {
      return;
    }
    if (isTableOnlyPresentation && normalizedAnchorStart && visibleStart !== normalizedAnchorStart) {
      return;
    }

    void loadInitialWindow("stale-while-revalidate");
  }, [accountCode, isCollapsed, isTableOnlyPresentation, normalizedAnchorEnd, normalizedAnchorStart, visibleStart]);

  useEffect(() => {
    if (!isTableOnlyPresentation || !accountCode || !normalizedAnchorStart) {
      return;
    }
    if (visibleStart === normalizedAnchorStart) {
      return;
    }
    setVisibleStart(normalizedAnchorStart);
  }, [accountCode, isTableOnlyPresentation, normalizedAnchorStart, setVisibleStart, visibleStart]);

  useEffect(() => {
    onLoadingChange?.(isLoading || isRefreshing || isLoadingPrevious || isLoadingNext);
  }, [isLoading, isLoadingNext, isLoadingPrevious, isRefreshing, onLoadingChange]);

  const timelineBody = (
    <>
      {!isTableOnlyPresentation ? (
        <div className="flex items-center justify-end">
          <div className="flex items-center gap-1">
            <ActionIconButton
              aria-label="Load previous period"
              tooltip="Load previous period"
              onClick={() => {
                void loadAdjacentWindow("previous");
              }}
              disabled={!canInteract || isLoading || isLoadingPrevious || maxReached}
              icon={
                isLoadingPrevious ? (
                  <Loader2 className="size-5 animate-spin" />
                ) : (
                  <ChevronLeft className="size-5" />
                )
              }
            />
            <ActionIconButton
              aria-label="Current period"
              tooltip="Current period"
              onClick={handleCurrentPeriod}
              disabled={!canInteract || isLoading}
              icon={<CalendarRange className="size-5" />}
            />
            <ActionIconButton
              aria-label="Load next period"
              tooltip="Load next period"
              onClick={() => {
                void loadAdjacentWindow("next");
              }}
              disabled={!canInteract || isLoading || isLoadingNext || maxReached}
              icon={
                isLoadingNext ? (
                  <Loader2 className="size-5 animate-spin" />
                ) : (
                  <ChevronRight className="size-5" />
                )
              }
            />
          </div>
        </div>
      ) : null}

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
          <div ref={scrollContainerRef} className="max-h-[32rem] overflow-auto">
            <div className="min-w-max" style={{ width: LABEL_COLUMN_PX + timelinePixelWidth }}>
              <div className="sticky top-0 z-30 border-b border-slate-200 bg-slate-100/95 backdrop-blur">
                <div className="flex h-8 border-b border-slate-200">
                  <div
                    className="sticky left-0 z-40 flex items-center border-r border-slate-200 bg-slate-100 px-3 text-xs font-semibold uppercase tracking-wide text-slate-600"
                    style={{ width: LABEL_COLUMN_PX, minWidth: LABEL_COLUMN_PX }}
                  >
                    EstNum / Station
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
                {groupedRows.map((group) => {
                  const collapsed = isGroupCollapsed(group.estNum);
                  const groupActiveWeeks = [...new Set(group.stations.flatMap((station) => station.activeWeeks))].sort();
                  const groupSegments = buildSegments(groupActiveWeeks, weekIndexByStart);
                  const groupColor = colorForRow(`group:${group.estNum}`);

                  return (
                    <div key={`group:${group.estNum}`} className="border-b border-slate-200 last:border-b-0">
                      <button
                        type="button"
                        className="flex w-full border-b border-slate-200 bg-slate-50/60 text-left hover:bg-slate-100/70"
                        aria-label={collapsed ? `Expand EstNum ${group.estNum}` : `Collapse EstNum ${group.estNum}`}
                        onClick={() => setGroupCollapsed(group.estNum, !collapsed)}
                      >
                        <div
                          className="sticky left-0 z-20 flex items-center gap-2 border-r border-slate-200 bg-slate-50 px-3 py-2"
                          style={{ width: LABEL_COLUMN_PX, minWidth: LABEL_COLUMN_PX }}
                        >
                          <span className="inline-flex size-6 items-center justify-center text-slate-600">
                            {collapsed ? <ChevronRight className="size-3" /> : <ChevronDown className="size-3" />}
                          </span>
                          <div className="min-w-0">
                            <p className="truncate text-sm font-semibold text-slate-900">EstNum {group.estNum}</p>
                            <p className="truncate text-xs text-slate-500">{group.estNumName}</p>
                            {group.estNumNote ? (
                              <p className="truncate text-xs text-slate-500">{group.estNumNote}</p>
                            ) : null}
                          </div>
                        </div>

                        <div className="relative h-12" style={{ width: timelinePixelWidth, minWidth: timelinePixelWidth }}>
                          <div className="absolute inset-0 flex">
                            {weeks.map((week) => {
                              const isCurrentWeek = week.weekStart === currentWeekStart;
                              return (
                                <div
                                  key={`group:${group.estNum}:${week.weekStart}`}
                                  className={`h-full border-r border-slate-200 ${isCurrentWeek ? "bg-blue-50/70" : "bg-slate-50/20"}`}
                                  style={{ width: WEEK_COLUMN_PX, minWidth: WEEK_COLUMN_PX }}
                                />
                              );
                            })}
                          </div>
                          {collapsed
                            ? groupSegments.map((segment, segmentIndex) => {
                                const left = segment.startIndex * WEEK_COLUMN_PX + 6;
                                const width = (segment.endIndex - segment.startIndex + 1) * WEEK_COLUMN_PX - 12;
                                const rangeStart =
                                  segment.activeWeeks[0] ?? weeks[segment.startIndex]?.weekStart ?? "";
                                const rangeEnd =
                                  segment.activeWeeks[segment.activeWeeks.length - 1] ??
                                  weeks[segment.endIndex]?.weekEnd ??
                                  "";
                                return (
                                  <TooltipTarget
                                    key={`group:${group.estNum}:segment:${segmentIndex}`}
                                    text={`EstNum ${group.estNum} · ${formatIsoDateMmDdYyyy(rangeStart)} to ${formatIsoDateMmDdYyyy(rangeEnd)}`}
                                  >
                                    <div
                                      className="absolute top-2.5 h-7 rounded-full border shadow-sm"
                                      style={{
                                        left,
                                        width: Math.max(width, 16),
                                        backgroundColor: groupColor.background,
                                        borderColor: groupColor.border,
                                      }}
                                    />
                                  </TooltipTarget>
                                );
                              })
                            : null}
                        </div>
                      </button>

                      {!collapsed
                        ? group.stations.map((item) => {
                            const color = colorForRow(`${item.stationCode}:${item.estNum}`);
                            const segments = buildSegments(item.activeWeeks, weekIndexByStart);

                            return (
                              <div key={`${item.stationCode}:${item.estNum}`} className="flex border-b border-slate-200 last:border-b-0">
                                <div
                                  className="sticky left-0 z-20 border-r border-slate-200 bg-white px-3 py-2"
                                  style={{ width: LABEL_COLUMN_PX, minWidth: LABEL_COLUMN_PX }}
                                >
                                  <p className="text-sm font-medium text-slate-900">{item.stationCode}</p>
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
                                    const rangeEnd =
                                      segment.activeWeeks[segment.activeWeeks.length - 1] ??
                                      weeks[segment.endIndex]?.weekEnd ??
                                      "";

                                    return (
                                      <TooltipTarget
                                        key={`${item.stationCode}:${item.estNum}:segment:${segmentIndex}`}
                                        text={`${item.stationCode} · ${item.estNum} · ${rangeStart} to ${rangeEnd}`}
                                      >
                                        <button
                                          type="button"
                                          className="absolute top-3 h-8 rounded-full border shadow-sm transition-all hover:brightness-95 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                                          style={{
                                            left,
                                            width: Math.max(width, 16),
                                            backgroundColor: color.background,
                                            borderColor: color.border,
                                          }}
                                          onClick={() =>
                                            setSelectedDetail({
                                              item,
                                              segment,
                                              rangeStart,
                                              rangeEnd,
                                            })
                                          }
                                        />
                                      </TooltipTarget>
                                    );
                                  })}
                                </div>
                              </div>
                            );
                          })
                        : null}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {statusText ? (
        isTableOnlyPresentation ? (
          <div className="flex justify-start border-t border-slate-200 px-4 py-3">
            <CacheStatusChip
              text={statusText}
              onRefresh={() => {
                void handleRefreshVisibleWindows();
              }}
              disabled={!canInteract || !isOnline || isRefreshing || isLoading || isLoadingNext || isLoadingPrevious}
              refreshing={isRefreshing}
              refreshLabel="Refresh timeline"
              tooltipText={
                !isOnline
                  ? "Offline. Reconnect to refresh timeline data."
                  : "Refresh timeline for the current account and loaded visible range"
              }
              className="max-w-[min(92vw,42rem)]"
            />
          </div>
        ) : (
          <div className="flex justify-start">
            <CacheStatusChip
              text={statusText}
              onRefresh={() => {
                void handleRefreshVisibleWindows();
              }}
              disabled={!canInteract || !isOnline || isRefreshing || isLoading || isLoadingNext || isLoadingPrevious}
              refreshing={isRefreshing}
              refreshLabel="Refresh timeline"
              tooltipText={
                !isOnline
                  ? "Offline. Reconnect to refresh timeline data."
                  : "Refresh timeline for the current account and loaded visible range"
              }
              className="max-w-[min(90vw,34rem)]"
            />
          </div>
        )
      ) : null}
    </>
  );

  return (
    <>
      {isTableOnlyPresentation ? (
        <div className="space-y-4">{timelineBody}</div>
      ) : (
        <SectionCard
          title={(
            <span className="flex items-center gap-2">
              <CalendarRange className="size-5 text-blue-700" />
              <span>Schedule Timeline</span>
            </span>
          )}
          description={rangeLimitMessage ?? undefined}
          actions={(
            <ActionIconButton
              aria-label={isCollapsed ? "Expand schedule timeline" : "Collapse schedule timeline"}
              tooltip={isCollapsed ? "Expand timeline" : "Collapse timeline"}
              onClick={() => setIsCollapsed((current) => !current)}
              icon={isCollapsed ? <ChevronDown className="size-5" /> : <ChevronUp className="size-5" />}
            />
          )}
          contentClassName="space-y-4"
        >
          {!isCollapsed ? timelineBody : null}
        </SectionCard>
      )}

      <Dialog open={Boolean(selectedDetail)} onOpenChange={(open) => !open && setSelectedDetail(null)}>
        <DialogContent className="max-w-xl">
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label="Close schedule timeline detail modal"
          >
            <X className="size-4" />
          </DialogClose>
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
