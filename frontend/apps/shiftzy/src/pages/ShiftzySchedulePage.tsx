import { useEffect, useMemo, useRef, useState } from "react";
import {
  AlertCircle,
  BriefcaseBusiness,
  CalendarDays,
  ChevronLeft,
  ChevronRight,
  Copy,
  CircleUserRound,
  FileText,
  Grip,
  Loader2,
  MoreHorizontal,
  Trash2,
  Users,
  X,
} from "lucide-react";

import { PageBanner } from "@shell/components/layout/PageBanner";
import { FloatingActionMenu, type FloatingActionMenuItem } from "@shared/components/actions/FloatingActionMenu";
import { Button } from "@tradsphere/components/ui/button";
import { AppDropdown } from "@tradsphere/components/ui/app-dropdown";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@tradsphere/components/ui/dialog";
import { useToast } from "@shell/components/ui/toast";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import {
  createShiftzySchedules,
  deleteShiftzySchedules,
  duplicateShiftzySchedules,
  exportShiftzySchedulesPdf,
  fetchShiftzyBootstrap,
  fetchShiftzySchedules,
  fetchShiftzyWeeks,
  updateShiftzySchedule,
  type ShiftzyBootstrapPayload,
  type ShiftzySchedule,
  type ShiftzyScheduleUpdateInput,
  type ShiftzyWeek,
} from "@shiftzy/lib/shiftzyApi";
import { hasAppEditAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { CacheStatusChip } from "@shared/components/status/CacheStatusChip";
import { PageLoadingOverlay } from "@shared/components/status/LoadingOverlay";
import { FormRow } from "@shared/components/form/FormRow";
import { readCacheSnapshot, setCacheData, type CacheSource } from "@shared/cache";

import { ShiftzyScheduleEditModal } from "@shiftzy/components/ShiftzyScheduleEditModal";

type BoardState = "loading" | "ready" | "empty" | "error";
type CacheStatus = {
  source: CacheSource;
  fetchedAt: number;
};

type ShiftzyBoardCachePayload = {
  bootstrap: ShiftzyBootstrapPayload;
  weeks: ShiftzyWeek[];
  selectedWeekNo: number | null;
  schedules: ShiftzySchedule[];
};

type WeekDay = {
  isoDate: string;
  dayLabel: string;
  dateLabel: string;
  isToday: boolean;
};

const WEEKDAY_SHORT = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"] as const;
const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";
const SHIFTZY_CACHE_NAMESPACE = "shiftzy:cache:";
const SHIFTZY_BOARD_CACHE_KEY = "board:weekly:v1";
const SHIFTZY_WEEK_SCHEDULE_CACHE_KEY_PREFIX = "schedules:week:";
const SHIFTZY_BOARD_CACHE_TTL_MS = 10 * 60 * 1000;

function readSidebarCollapsedState(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  try {
    const nextValue = window.localStorage.getItem(SIDEBAR_COLLAPSED_STORAGE_KEY);
    if (nextValue !== null) {
      return nextValue === "1";
    }
    return window.localStorage.getItem(LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

function parseIsoDate(value: string): Date {
  return new Date(`${value}T00:00:00`);
}

function addDays(value: Date, days: number): Date {
  const next = new Date(value);
  next.setDate(next.getDate() + days);
  return next;
}

function toIsoDate(value: Date): string {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatDateLabel(value: string): string {
  const date = parseIsoDate(value);
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

function formatDateNumeric(value: string): string {
  const date = parseIsoDate(value);
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const year = String(date.getFullYear());
  return `${month}/${day}/${year}`;
}

function formatWeekLabel(week: ShiftzyWeek): string {
  return `Week ${week.weekNo} · ${formatDateLabel(week.startDate)} - ${formatDateLabel(week.endDate)}`;
}

function buildWeekDays(week: ShiftzyWeek | null): WeekDay[] {
  if (!week) {
    return [];
  }
  const today = toIsoDate(new Date());
  const start = parseIsoDate(week.startDate);
  return WEEKDAY_SHORT.map((label, index) => {
    const nextDay = addDays(start, index);
    const isoDate = toIsoDate(nextDay);
    return {
      isoDate,
      dayLabel: label,
      dateLabel: nextDay.toLocaleDateString(undefined, { month: "short", day: "numeric" }),
      isToday: isoDate === today,
    };
  });
}

function formatRelativeTime(timestamp: number): string {
  const diffMs = Date.now() - timestamp;
  const diffMinutes = Math.round(diffMs / 60000);
  if (!Number.isFinite(diffMinutes) || diffMinutes < 1) {
    return "just now";
  }
  if (diffMinutes < 60) {
    return `${diffMinutes}m ago`;
  }
  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours}h ago`;
  }
  const diffDays = Math.round(diffHours / 24);
  return `${diffDays}d ago`;
}

function normalizeTimeLabel(value: string): string {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return "--:--";
  }
  if (/^\d{2}:\d{2}:\d{2}$/.test(normalized)) {
    return normalized.slice(0, 5);
  }
  return normalized.slice(0, 5);
}

function resolvePositionIconUrl(schedule: ShiftzySchedule, bootstrap: ShiftzyBootstrapPayload): string | null {
  const matchedPosition = bootstrap.positions.find((item) => item.code === schedule.positionCode);
  const url = String(matchedPosition?.icon || "").trim();
  return url || null;
}

function normalizeComparableTime(value: string): string {
  return normalizeTimeLabel(value);
}

function normalizeComparableNote(value: string | null): string | null {
  const normalized = String(value || "").trim();
  return normalized || null;
}

function isScheduleDifferent(current: ShiftzySchedule, baseline: ShiftzySchedule | undefined): boolean {
  if (!baseline) {
    return true;
  }
  return (
    current.employeeId !== baseline.employeeId
    || current.positionCode !== baseline.positionCode
    || (current.shiftId || null) !== (baseline.shiftId || null)
    || current.date !== baseline.date
    || normalizeComparableTime(current.startTime) !== normalizeComparableTime(baseline.startTime)
    || normalizeComparableTime(current.endTime) !== normalizeComparableTime(baseline.endTime)
    || normalizeComparableNote(current.note) !== normalizeComparableNote(baseline.note)
  );
}

function toScheduleUpdateInput(schedule: ShiftzySchedule): ShiftzyScheduleUpdateInput {
  return {
    id: schedule.id,
    employeeId: schedule.employeeId,
    positionCode: schedule.positionCode,
    shiftId: schedule.shiftId || null,
    date: schedule.date,
    startTime: normalizeComparableTime(schedule.startTime),
    endTime: normalizeComparableTime(schedule.endTime),
    note: normalizeComparableNote(schedule.note),
  };
}

function EmptyPanel({ icon, message }: { icon: JSX.Element; message: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-6 py-10 text-center">
      <div className="inline-flex items-center gap-2 text-sm text-slate-600">
        {icon}
        <span>{message}</span>
      </div>
    </div>
  );
}

export default function ShiftzySchedulePage() {
  const auth = useAuth();
  const { requestJson } = useApiRequest();
  const toast = useToast();
  const requestTokenRef = useRef(0);
  const topActionMenuAnchorRef = useRef<HTMLSpanElement | null>(null);
  const topActionMenuCloseTimeoutRef = useRef<number | null>(null);
  const [bootstrap, setBootstrap] = useState<ShiftzyBootstrapPayload>({
    weeks: [],
    employees: [],
    positions: [],
    shifts: [],
  });
  const [weeks, setWeeks] = useState<ShiftzyWeek[]>([]);
  const [selectedWeekNo, setSelectedWeekNo] = useState<number | null>(null);
  const [schedules, setSchedules] = useState<ShiftzySchedule[]>([]);
  const [state, setState] = useState<BoardState>("loading");
  const [error, setError] = useState<string | null>(null);
  const [loadingPage, setLoadingPage] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [backgroundRefreshing, setBackgroundRefreshing] = useState(false);
  const [savingChanges, setSavingChanges] = useState(false);
  const [draggingScheduleId, setDraggingScheduleId] = useState<string | null>(null);
  const [dropTargetDate, setDropTargetDate] = useState<string | null>(null);
  const [deleteDropZoneActive, setDeleteDropZoneActive] = useState(false);
  const [pendingScheduleIds, setPendingScheduleIds] = useState<Record<string, true>>({});
  const [pendingDeletedScheduleIds, setPendingDeletedScheduleIds] = useState<Record<string, true>>({});
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [editingSchedule, setEditingSchedule] = useState<ShiftzySchedule | null>(null);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [editorMode, setEditorMode] = useState<"create" | "edit">("edit");
  const [topActionMenuOpen, setTopActionMenuOpen] = useState(false);
  const [isDuplicateModalOpen, setIsDuplicateModalOpen] = useState(false);
  const [duplicateFromWeekNo, setDuplicateFromWeekNo] = useState<number | null>(null);
  const [duplicateToWeekNo, setDuplicateToWeekNo] = useState<number | null>(null);
  const [duplicatingSchedules, setDuplicatingSchedules] = useState(false);
  const [exportingSchedulesPdf, setExportingSchedulesPdf] = useState(false);
  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());
  const baseSchedulesByIdRef = useRef<Record<string, ShiftzySchedule>>({});
  const schedulesRef = useRef<ShiftzySchedule[]>([]);
  const pendingScheduleIdsRef = useRef<Record<string, true>>({});
  const pendingDeletedScheduleIdsRef = useRef<Record<string, true>>({});
  const canEditShiftzy = hasAppEditAccess(auth.accessProfile, "shiftzy");
  const hasSessionToken = Boolean(auth.session?.accessToken);

  const weekOptions = useMemo(
    () => weeks.map((item) => ({ label: formatWeekLabel(item), value: String(item.weekNo) })),
    [weeks],
  );

  const selectedWeek = useMemo(
    () => weeks.find((item) => item.weekNo === selectedWeekNo) || null,
    [weeks, selectedWeekNo],
  );

  const weekDays = useMemo(() => buildWeekDays(selectedWeek), [selectedWeek]);

  const positionRankByCode = useMemo(() => {
    const rankMap = new Map<string, number>();
    bootstrap.positions.forEach((position, index) => {
      rankMap.set(position.code, index);
    });
    return rankMap;
  }, [bootstrap.positions]);

  const compareSchedulesByPosition = useMemo(() => (
    (left: ShiftzySchedule, right: ShiftzySchedule) => {
      const leftRank = positionRankByCode.get(left.positionCode) ?? Number.MAX_SAFE_INTEGER;
      const rightRank = positionRankByCode.get(right.positionCode) ?? Number.MAX_SAFE_INTEGER;
      if (leftRank !== rightRank) {
        return leftRank - rightRank;
      }
      const byPositionName = left.positionName.localeCompare(right.positionName);
      if (byPositionName !== 0) {
        return byPositionName;
      }
      const byStartTime = normalizeComparableTime(left.startTime).localeCompare(normalizeComparableTime(right.startTime));
      if (byStartTime !== 0) {
        return byStartTime;
      }
      return left.employeeName.localeCompare(right.employeeName);
    }
  ), [positionRankByCode]);

  const schedulesByDate = useMemo(() => {
    const grouped = new Map<string, ShiftzySchedule[]>();
    for (const item of schedules) {
      const next = grouped.get(item.date) || [];
      next.push(item);
      grouped.set(item.date, next);
    }
    grouped.forEach((items, date) => {
      grouped.set(date, [...items].sort(compareSchedulesByPosition));
    });
    return grouped;
  }, [compareSchedulesByPosition, schedules]);

  const weekIndex = useMemo(
    () => weeks.findIndex((item) => item.weekNo === selectedWeekNo),
    [weeks, selectedWeekNo],
  );

  const canGoPreviousWeek = weekIndex > 0;
  const canGoNextWeek = weekIndex >= 0 && weekIndex < weeks.length - 1;
  const pendingChangeIds = useMemo(() => Object.keys(pendingScheduleIds), [pendingScheduleIds]);
  const pendingDeletedIds = useMemo(() => Object.keys(pendingDeletedScheduleIds), [pendingDeletedScheduleIds]);
  const hasPendingChanges = pendingChangeIds.length > 0 || pendingDeletedIds.length > 0;
  const hasBusyAction = loadingPage || refreshing || savingChanges || duplicatingSchedules || exportingSchedulesPdf;
  const canExportSchedulesPdf = Boolean(selectedWeekNo && state === "ready" && schedules.length > 0);
  const canSubmitDuplicate = Boolean(
    duplicateFromWeekNo
    && duplicateToWeekNo
    && duplicateFromWeekNo !== duplicateToWeekNo,
  );

  function getCacheScope(): string {
    return String(auth.tenantSlug || "default").trim().toLowerCase() || "default";
  }

  function getWeekScheduleCacheKey(weekNo: number): string {
    return `${SHIFTZY_WEEK_SCHEDULE_CACHE_KEY_PREFIX}${Math.trunc(weekNo)}`;
  }

  function writeWeekScheduleCache(weekNo: number, rows: ShiftzySchedule[]) {
    setCacheData(getWeekScheduleCacheKey(weekNo), rows, {
      namespace: SHIFTZY_CACHE_NAMESPACE,
      scope: getCacheScope(),
      storage: "persistent",
      ttlMs: SHIFTZY_BOARD_CACHE_TTL_MS,
      source: "network",
      fetchedAt: Date.now(),
    });
  }

  useEffect(() => {
    schedulesRef.current = schedules;
  }, [schedules]);

  useEffect(() => {
    pendingScheduleIdsRef.current = pendingScheduleIds;
  }, [pendingScheduleIds]);

  useEffect(() => {
    pendingDeletedScheduleIdsRef.current = pendingDeletedScheduleIds;
  }, [pendingDeletedScheduleIds]);

  function buildPendingScheduleMap(
    rows: ShiftzySchedule[],
    baselineById: Record<string, ShiftzySchedule>,
  ): Record<string, true> {
    const nextPending: Record<string, true> = {};
    for (const row of rows) {
      const baseline = baselineById[row.id];
      if (isScheduleDifferent(row, baseline)) {
        nextPending[row.id] = true;
      }
    }
    return nextPending;
  }

  function applyServerSchedules(rows: ShiftzySchedule[], options: { preservePending?: boolean } = {}) {
    const preservePending = Boolean(options.preservePending);
    const nextBaseSchedules: Record<string, ShiftzySchedule> = {};
    for (const row of rows) {
      nextBaseSchedules[row.id] = { ...row };
    }
    baseSchedulesByIdRef.current = nextBaseSchedules;
    if (!preservePending) {
      setSchedules(rows);
      schedulesRef.current = rows;
      setPendingScheduleIds({});
      pendingScheduleIdsRef.current = {};
      setPendingDeletedScheduleIds({});
      pendingDeletedScheduleIdsRef.current = {};
      return;
    }

    const currentPendingIds = Object.keys(pendingScheduleIdsRef.current);
    const currentDeletedIds = Object.keys(pendingDeletedScheduleIdsRef.current);
    if (!currentPendingIds.length && !currentDeletedIds.length) {
      setSchedules(rows);
      schedulesRef.current = rows;
      setPendingScheduleIds({});
      pendingScheduleIdsRef.current = {};
      return;
    }

    const mergedById = new Map<string, ShiftzySchedule>(rows.map((row) => [row.id, row]));
    const currentById = new Map<string, ShiftzySchedule>(schedulesRef.current.map((row) => [row.id, row]));
    for (const id of currentPendingIds) {
      const pendingRow = currentById.get(id);
      if (pendingRow) {
        mergedById.set(id, pendingRow);
      }
    }
    for (const id of currentDeletedIds) {
      mergedById.delete(id);
    }
    const mergedRows = Array.from(mergedById.values());
    const nextPendingMap = buildPendingScheduleMap(mergedRows, nextBaseSchedules);
    setSchedules(mergedRows);
    schedulesRef.current = mergedRows;
    setPendingScheduleIds(nextPendingMap);
    pendingScheduleIdsRef.current = nextPendingMap;
    setPendingDeletedScheduleIds((currentDeleted) => {
      const nextDeleted: Record<string, true> = {};
      for (const id of Object.keys(currentDeleted)) {
        if (nextBaseSchedules[id]) {
          nextDeleted[id] = true;
        }
      }
      pendingDeletedScheduleIdsRef.current = nextDeleted;
      return nextDeleted;
    });
  }

  function syncPendingFlagForSchedule(nextSchedule: ShiftzySchedule) {
    const baseline = baseSchedulesByIdRef.current[nextSchedule.id];
    setPendingScheduleIds((currentPending) => {
      if (isScheduleDifferent(nextSchedule, baseline)) {
        return { ...currentPending, [nextSchedule.id]: true };
      }
      const nextPending = { ...currentPending };
      delete nextPending[nextSchedule.id];
      return nextPending;
    });
  }

  function buildDraftScheduleForDate(date: string): ShiftzySchedule | null {
    if (!bootstrap.employees.length || !bootstrap.positions.length) {
      return null;
    }
    const nextId = globalThis.crypto?.randomUUID?.()
      || `draft-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    return {
      id: nextId,
      employeeId: "",
      positionCode: "",
      shiftId: null,
      date,
      startTime: "",
      endTime: "",
      note: null,
      employeeName: "",
      scheduleSection: "",
      positionName: "",
      shiftName: "",
    };
  }

  async function loadSchedulesForWeek(weekNo: number, background = false): Promise<void> {
    const cacheScope = getCacheScope();
    const cachedWeekSnapshot = readCacheSnapshot<ShiftzySchedule[]>(getWeekScheduleCacheKey(weekNo), {
      namespace: SHIFTZY_CACHE_NAMESPACE,
      scope: cacheScope,
      storage: "persistent",
      allowExpired: true,
    });
    const cachedWeekRows = Array.isArray(cachedWeekSnapshot?.data) ? cachedWeekSnapshot?.data : null;

    if (!background) {
      setState("loading");
      setBackgroundRefreshing(false);
    } else {
      setRefreshing(true);
      if (cachedWeekRows) {
        applyServerSchedules(cachedWeekRows, { preservePending: true });
        setState(cachedWeekRows.length ? "ready" : "empty");
        setError(null);
        setCacheStatus({
          source: "cache",
          fetchedAt: cachedWeekSnapshot?.fetchedAt ?? Date.now(),
        });
      }
    }
    const requestToken = ++requestTokenRef.current;
    try {
      const rows = await fetchShiftzySchedules(requestJson, weekNo);
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      applyServerSchedules(rows, { preservePending: true });
      setState(rows.length ? "ready" : "empty");
      setError(null);
      setRefreshMessage(null);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });
      writeWeekScheduleCache(weekNo, rows);
      const nextCachePayload: ShiftzyBoardCachePayload = {
        bootstrap,
        weeks,
        selectedWeekNo: weekNo,
        schedules: rows,
      };
      setCacheData(SHIFTZY_BOARD_CACHE_KEY, nextCachePayload, {
        namespace: SHIFTZY_CACHE_NAMESPACE,
        scope: cacheScope,
        storage: "persistent",
        ttlMs: SHIFTZY_BOARD_CACHE_TTL_MS,
        source: "network",
        fetchedAt: Date.now(),
      });
    } catch (loadError) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      if (cachedWeekRows) {
        applyServerSchedules(cachedWeekRows, { preservePending: true });
        setState(cachedWeekRows.length ? "ready" : "empty");
        setError(null);
        setCacheStatus({
          source: "cache",
          fetchedAt: cachedWeekSnapshot?.fetchedAt ?? Date.now(),
        });
        setRefreshMessage("Showing cached schedule for this week. Could not refresh.");
        return;
      }
      setState("error");
      setError(loadError instanceof Error ? loadError.message : "Could not load schedules.");
    } finally {
      if (requestToken === requestTokenRef.current) {
        setLoadingPage(false);
        setRefreshing(false);
        setBackgroundRefreshing(false);
      }
    }
  }

  async function loadInitialData(options: { showRefreshing?: boolean } = {}) {
    if (!hasSessionToken || auth.status !== "authenticated" || !auth.tenantSlug) {
      return;
    }
    const showRefreshing = Boolean(options.showRefreshing);
    const requestToken = ++requestTokenRef.current;
    const cacheScope = String(auth.tenantSlug || "default").trim().toLowerCase() || "default";

    const cachedSnapshot = readCacheSnapshot<ShiftzyBoardCachePayload>(SHIFTZY_BOARD_CACHE_KEY, {
      namespace: SHIFTZY_CACHE_NAMESPACE,
      scope: cacheScope,
      storage: "persistent",
      allowExpired: true,
    });
    const cachedData = cachedSnapshot?.data;
    const canUseCached = Boolean(
      cachedData
      && Array.isArray(cachedData.weeks)
      && Array.isArray(cachedData.schedules)
      && cachedData.bootstrap
      && Array.isArray(cachedData.bootstrap.employees)
      && Array.isArray(cachedData.bootstrap.positions)
      && Array.isArray(cachedData.bootstrap.shifts),
    );
    const isCachedExpired = Boolean(cachedSnapshot?.isExpired);
    const shouldFetchNetwork = showRefreshing || !canUseCached || isCachedExpired;

    if (showRefreshing) {
      setRefreshing(true);
      setBackgroundRefreshing(false);
      if (canUseCached && cachedData && schedules.length === 0) {
        setBootstrap(cachedData.bootstrap);
        setWeeks(cachedData.weeks);
        setSelectedWeekNo(cachedData.selectedWeekNo);
        applyServerSchedules(cachedData.schedules, { preservePending: true });
        setState(cachedData.schedules.length ? "ready" : "empty");
        setCacheStatus({
          source: "cache",
          fetchedAt: cachedSnapshot?.fetchedAt ?? Date.now(),
        });
        setLoadingPage(false);
      }
    } else {
      setLoadingPage(true);
      setBackgroundRefreshing(false);
      if (canUseCached && cachedData) {
        setBootstrap(cachedData.bootstrap);
        setWeeks(cachedData.weeks);
        setSelectedWeekNo(cachedData.selectedWeekNo);
        applyServerSchedules(cachedData.schedules, { preservePending: true });
        setState(cachedData.schedules.length ? "ready" : "empty");
        setCacheStatus({
          source: "cache",
          fetchedAt: cachedSnapshot?.fetchedAt ?? Date.now(),
        });
        setLoadingPage(false);
        setBackgroundRefreshing(shouldFetchNetwork);
      }
    }

    setError(null);
    setRefreshMessage(null);
    if (!shouldFetchNetwork) {
      setRefreshing(false);
      setBackgroundRefreshing(false);
      return;
    }

    try {
      const [bootstrapPayload, weekRows] = await Promise.all([
        fetchShiftzyBootstrap(requestJson),
        fetchShiftzyWeeks(requestJson, 2, 2),
      ]);
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      setBootstrap(bootstrapPayload);
      const resolvedWeeks = weekRows.length ? weekRows : bootstrapPayload.weeks;
      setWeeks(resolvedWeeks);
      const cachedWeekNo = canUseCached && cachedData ? cachedData.selectedWeekNo : null;
      const hasCachedWeek = typeof cachedWeekNo === "number" && resolvedWeeks.some((item) => item.weekNo === cachedWeekNo);
      const preferredWeekNo = hasCachedWeek
        ? cachedWeekNo
        : (resolvedWeeks.find((item) => item.isTodayWeek)?.weekNo ?? resolvedWeeks[0]?.weekNo ?? null);

      if (preferredWeekNo === null) {
        setSelectedWeekNo(null);
        setSchedules([]);
        setState("empty");
        setCacheStatus({ source: "network", fetchedAt: Date.now() });
        setBackgroundRefreshing(false);
        setLoadingPage(false);
        setRefreshing(false);
        return;
      }
      setSelectedWeekNo(preferredWeekNo);
      const rows = await fetchShiftzySchedules(requestJson, preferredWeekNo);
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      applyServerSchedules(rows, { preservePending: true });
      writeWeekScheduleCache(preferredWeekNo, rows);
      setState(rows.length ? "ready" : "empty");
      setCacheStatus({ source: "network", fetchedAt: Date.now() });
      setBackgroundRefreshing(false);
      setLoadingPage(false);
      setRefreshing(false);
      const nextCachePayload: ShiftzyBoardCachePayload = {
        bootstrap: bootstrapPayload,
        weeks: resolvedWeeks,
        selectedWeekNo: preferredWeekNo,
        schedules: rows,
      };
      setCacheData(SHIFTZY_BOARD_CACHE_KEY, nextCachePayload, {
        namespace: SHIFTZY_CACHE_NAMESPACE,
        scope: cacheScope,
        storage: "persistent",
        ttlMs: SHIFTZY_BOARD_CACHE_TTL_MS,
        source: "network",
        fetchedAt: Date.now(),
      });
    } catch (loadError) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      if (canUseCached) {
        if (cachedData) {
          setBootstrap(cachedData.bootstrap);
          setWeeks(cachedData.weeks);
          setSelectedWeekNo(cachedData.selectedWeekNo);
          applyServerSchedules(cachedData.schedules, { preservePending: true });
          setState(cachedData.schedules.length ? "ready" : "empty");
          setCacheStatus({
            source: "cache",
            fetchedAt: cachedSnapshot?.fetchedAt ?? Date.now(),
          });
        }
        setRefreshMessage("Showing cached Shiftzy data. Could not refresh.");
        setBackgroundRefreshing(false);
        setRefreshing(false);
        setLoadingPage(false);
        return;
      }
      setState("error");
      setError(loadError instanceof Error ? loadError.message : "Could not load Shiftzy data.");
      setLoadingPage(false);
      setRefreshing(false);
      setBackgroundRefreshing(false);
    }
  }

  useEffect(() => {
    if (auth.status !== "authenticated" || !hasSessionToken || !auth.tenantSlug) {
      return;
    }
    void loadInitialData();
  }, [auth.status, auth.tenantSlug, hasSessionToken]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const handleStorage = () => setSidebarVisuallyExpanded(!readSidebarCollapsedState());
    const handleSidebarEvent = (event: Event) => {
      const customEvent = event as CustomEvent<{ collapsed?: boolean; visuallyExpanded?: boolean }>;
      if (typeof customEvent.detail?.visuallyExpanded === "boolean") {
        setSidebarVisuallyExpanded(customEvent.detail.visuallyExpanded);
        return;
      }
      if (typeof customEvent.detail?.collapsed === "boolean") {
        setSidebarVisuallyExpanded(!customEvent.detail.collapsed);
        return;
      }
      setSidebarVisuallyExpanded(!readSidebarCollapsedState());
    };
    window.addEventListener("storage", handleStorage);
    window.addEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    return () => {
      window.removeEventListener("storage", handleStorage);
      window.removeEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    };
  }, []);

  useEffect(() => () => {
    if (topActionMenuCloseTimeoutRef.current !== null) {
      window.clearTimeout(topActionMenuCloseTimeoutRef.current);
    }
  }, []);

  async function handleSelectWeek(nextWeekNo: number) {
    if (!Number.isFinite(nextWeekNo) || nextWeekNo === selectedWeekNo || hasPendingChanges) {
      return;
    }
    setSelectedWeekNo(nextWeekNo);
    await loadSchedulesForWeek(nextWeekNo, true);
  }

  function handleOpenEdit(schedule: ShiftzySchedule) {
    const latestSchedule = schedules.find((item) => item.id === schedule.id) || schedule;
    setEditorMode("edit");
    setEditingSchedule(latestSchedule);
    setIsEditModalOpen(true);
  }

  function handleOpenCreate(date: string) {
    if (!canEditShiftzy || hasBusyAction) {
      return;
    }
    const draftSchedule = buildDraftScheduleForDate(date);
    if (!draftSchedule) {
      setRefreshMessage("Employees and positions are required before adding schedules.");
      return;
    }
    setEditorMode("create");
    setEditingSchedule(draftSchedule);
    setIsEditModalOpen(true);
  }

  async function handleStageScheduleChange(payload: ShiftzyScheduleUpdateInput) {
    if (!canEditShiftzy) {
      return;
    }
    const nextEmployee = bootstrap.employees.find((item) => item.id === payload.employeeId);
    const nextPosition = bootstrap.positions.find((item) => item.code === payload.positionCode);
    const nextShift = payload.shiftId ? bootstrap.shifts.find((item) => item.id === payload.shiftId) : null;
    let updatedSchedule: ShiftzySchedule | null = null;
    let matched = false;
    const nextSchedules = schedules.map((item) => {
      if (item.id !== payload.id) {
        return item;
      }
      matched = true;
      const nextSchedule: ShiftzySchedule = {
        ...item,
        employeeId: payload.employeeId,
        positionCode: payload.positionCode,
        shiftId: payload.shiftId || null,
        date: payload.date,
        startTime: payload.startTime,
        endTime: payload.endTime,
        note: payload.note,
        employeeName: nextEmployee?.name || item.employeeName,
        scheduleSection: nextEmployee?.scheduleSection || item.scheduleSection,
        positionName: nextPosition?.name || item.positionName,
        shiftName: nextShift?.name || item.shiftName,
      };
      updatedSchedule = nextSchedule;
      return nextSchedule;
    });
    if (!matched) {
      updatedSchedule = {
        id: payload.id,
        employeeId: payload.employeeId,
        positionCode: payload.positionCode,
        shiftId: payload.shiftId || null,
        date: payload.date,
        startTime: payload.startTime,
        endTime: payload.endTime,
        note: payload.note,
        employeeName: nextEmployee?.name || "Unassigned",
        scheduleSection: nextEmployee?.scheduleSection || "General",
        positionName: nextPosition?.name || payload.positionCode,
        shiftName: nextShift?.name || "Custom",
      };
      nextSchedules.push(updatedSchedule);
    }
    setSchedules(nextSchedules);
    if (updatedSchedule) {
      syncPendingFlagForSchedule(updatedSchedule);
    }
    setIsEditModalOpen(false);
    setEditingSchedule(null);
  }

  async function handleDropSchedule(targetDate: string) {
    if (!canEditShiftzy || !draggingScheduleId) {
      setDraggingScheduleId(null);
      setDropTargetDate(null);
      setDeleteDropZoneActive(false);
      return;
    }

    const sourceSchedule = schedules.find((item) => item.id === draggingScheduleId);
    if (!sourceSchedule) {
      setDraggingScheduleId(null);
      setDropTargetDate(null);
      setDeleteDropZoneActive(false);
      return;
    }

    if (sourceSchedule.date === targetDate) {
      setDraggingScheduleId(null);
      setDropTargetDate(null);
      setDeleteDropZoneActive(false);
      return;
    }

    const movedSchedule: ShiftzySchedule = {
      ...sourceSchedule,
      date: targetDate,
    };

    const nextSchedules = schedules.map((item) => (item.id === movedSchedule.id ? movedSchedule : item));
    setSchedules(nextSchedules);
    syncPendingFlagForSchedule(movedSchedule);
    setDraggingScheduleId(null);
    setDropTargetDate(null);
    setDeleteDropZoneActive(false);
  }

  function stageScheduleRemoval(scheduleId: string) {
    const schedule = schedulesRef.current.find((item) => item.id === scheduleId);
    if (!schedule) {
      return;
    }
    setSchedules((currentSchedules) => {
      const nextSchedules = currentSchedules.filter((item) => item.id !== scheduleId);
      schedulesRef.current = nextSchedules;
      return nextSchedules;
    });
    setPendingScheduleIds((currentPending) => {
      const nextPending = { ...currentPending };
      delete nextPending[scheduleId];
      return nextPending;
    });
    const existsInBaseline = Boolean(baseSchedulesByIdRef.current[scheduleId]);
    setPendingDeletedScheduleIds((currentDeleted) => {
      const nextDeleted = { ...currentDeleted };
      if (existsInBaseline) {
        nextDeleted[scheduleId] = true;
      } else {
        delete nextDeleted[scheduleId];
      }
      return nextDeleted;
    });
    if (editingSchedule?.id === scheduleId) {
      setEditingSchedule(null);
      setIsEditModalOpen(false);
    }
  }

  function handleDropScheduleDelete() {
    if (!canEditShiftzy || !draggingScheduleId) {
      setDraggingScheduleId(null);
      setDropTargetDate(null);
      setDeleteDropZoneActive(false);
      return;
    }
    stageScheduleRemoval(draggingScheduleId);
    setDraggingScheduleId(null);
    setDropTargetDate(null);
    setDeleteDropZoneActive(false);
  }

  async function handleSavePendingChanges() {
    if (!canEditShiftzy || !hasPendingChanges) {
      return;
    }
    const updates = schedules.filter((item) => pendingScheduleIds[item.id]);
    const deletedIds = Object.keys(pendingDeletedScheduleIds);
    if (!updates.length && !deletedIds.length) {
      setPendingScheduleIds({});
      setPendingDeletedScheduleIds({});
      return;
    }
    const creates = updates.filter((item) => !baseSchedulesByIdRef.current[item.id]);
    const edits = updates.filter((item) => Boolean(baseSchedulesByIdRef.current[item.id]));
    setSavingChanges(true);
    setRefreshMessage(null);
    try {
      if (creates.length) {
        await createShiftzySchedules(requestJson, creates.map((item) => ({
          id: item.id,
          employeeId: item.employeeId,
          positionCode: item.positionCode,
          shiftId: item.shiftId,
          date: item.date,
          startTime: item.startTime,
          endTime: item.endTime,
          note: item.note,
        })));
      }
      await Promise.all(
        edits.map((item) => updateShiftzySchedule(requestJson, toScheduleUpdateInput(item))),
      );
      if (deletedIds.length) {
        await deleteShiftzySchedules(requestJson, deletedIds);
      }
      if (selectedWeekNo) {
        await loadSchedulesForWeek(selectedWeekNo, true);
      }
      setCacheStatus({ source: "network", fetchedAt: Date.now() });
    } catch (saveError) {
      setRefreshMessage(saveError instanceof Error ? saveError.message : "Could not save schedule changes.");
    } finally {
      setSavingChanges(false);
    }
  }

  function handleOpenDuplicateModal() {
    if (!selectedWeekNo || !weeks.length) {
      return;
    }
    const currentIndex = weeks.findIndex((item) => item.weekNo === selectedWeekNo);
    const previousWeek = currentIndex > 0 ? weeks[currentIndex - 1] : null;
    setDuplicateFromWeekNo(previousWeek?.weekNo ?? selectedWeekNo);
    setDuplicateToWeekNo(selectedWeekNo);
    setIsDuplicateModalOpen(true);
  }

  async function handleDuplicateSchedules() {
    if (!canSubmitDuplicate || !duplicateFromWeekNo || !duplicateToWeekNo) {
      return;
    }
    setDuplicatingSchedules(true);
    setRefreshMessage(null);
    try {
      await duplicateShiftzySchedules(requestJson, {
        weekStart: duplicateFromWeekNo,
        weekEnd: duplicateToWeekNo,
      });
      setIsDuplicateModalOpen(false);
      setSelectedWeekNo(duplicateToWeekNo);
      await loadSchedulesForWeek(duplicateToWeekNo, true);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });
    } catch (duplicateError) {
      setRefreshMessage(duplicateError instanceof Error ? duplicateError.message : "Could not duplicate schedules.");
    } finally {
      setDuplicatingSchedules(false);
    }
  }

  async function handleExportSchedulesPdf() {
    if (!selectedWeekNo || !canExportSchedulesPdf || hasPendingChanges) {
      if (hasPendingChanges) {
        toast.error("Could not export schedules PDF", "Please save schedule changes before exporting PDF.");
      }
      return;
    }
    setExportingSchedulesPdf(true);
    setRefreshMessage(null);
    try {
      const pdfBytes = await exportShiftzySchedulesPdf(requestJson, selectedWeekNo);
      const blob = new Blob([pdfBytes], { type: "application/pdf" });
      const url = URL.createObjectURL(blob);
      const previewWindow = window.open(url, "_blank");
      if (!previewWindow) {
        URL.revokeObjectURL(url);
        throw new Error("Popup blocked. Please allow popups to preview the PDF.");
      }
      window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
    } catch (exportError) {
      const message = exportError instanceof Error ? exportError.message : "Fetching PDF failed. Please try again.";
      toast.error("Could not export schedules PDF", message);
    } finally {
      setExportingSchedulesPdf(false);
    }
  }

  const topActionMenuItems: FloatingActionMenuItem[] = useMemo(() => ([
    {
      key: "export-schedules-pdf",
      label: "Export to PDF",
      icon: <FileText className="size-4" />,
      disabled: !canExportSchedulesPdf || hasBusyAction,
      onSelect: () => {
        void handleExportSchedulesPdf();
      },
    },
    {
      key: "duplicate-schedules",
      label: "Duplicate schedules",
      icon: <Copy className="size-4" />,
      disabled: !selectedWeekNo || hasBusyAction || !weeks.length,
      onSelect: handleOpenDuplicateModal,
    },
  ]), [canExportSchedulesPdf, hasBusyAction, selectedWeekNo, weeks.length]);

  function clearTopActionMenuCloseTimeout() {
    if (topActionMenuCloseTimeoutRef.current !== null) {
      window.clearTimeout(topActionMenuCloseTimeoutRef.current);
      topActionMenuCloseTimeoutRef.current = null;
    }
  }

  function scheduleTopActionMenuClose() {
    clearTopActionMenuCloseTimeout();
    topActionMenuCloseTimeoutRef.current = window.setTimeout(() => {
      setTopActionMenuOpen(false);
      topActionMenuCloseTimeoutRef.current = null;
    }, 120);
  }

  function handleDiscardPendingChanges() {
    if (!hasPendingChanges) {
      return;
    }
    setSchedules(Object.values(baseSchedulesByIdRef.current).map((item) => ({ ...item })));
    setPendingScheduleIds({});
    setPendingDeletedScheduleIds({});
    setDraggingScheduleId(null);
    setDropTargetDate(null);
    setDeleteDropZoneActive(false);
    setEditingSchedule(null);
    setIsEditModalOpen(false);
  }

  const cacheStatusText = useMemo(() => {
    if (refreshing || backgroundRefreshing) {
      return "Refreshing Shiftzy data...";
    }
    if (!cacheStatus) {
      return "Loading Shiftzy board...";
    }
    return `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [backgroundRefreshing, cacheStatus, refreshing]);

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-5 pb-12">
      <PageBanner
        eyebrow="Shiftzy"
        title="Schedules"
        description="Manage weekly workforce schedules by day, then update any shift card in-place."
        gradientVariant="shiftzy"
      />
      <FloatingActionMenu
        open={topActionMenuOpen}
        align="left"
        anchorRef={topActionMenuAnchorRef}
        items={topActionMenuItems}
        onClose={() => setTopActionMenuOpen(false)}
        onPointerEnter={() => {
          clearTopActionMenuCloseTimeout();
        }}
        onPointerLeave={() => {
          scheduleTopActionMenuClose();
        }}
      />

      <section className="relative rounded-2xl border border-blue-100 bg-white/95 p-4 shadow-soft sm:p-5">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Week</p>
            <h2 className="mt-1 text-lg font-semibold text-slate-900">
              {selectedWeek ? `Week ${selectedWeek.weekNo}` : "Select a week"}
            </h2>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button
              variant="ghost"
              className="group h-10 transform-gpu gap-1.5 rounded-md px-2 !text-slate-500 transition-transform duration-200 ease-out hover:scale-[1.03] hover:!bg-transparent hover:!text-blue-600 focus-visible:scale-[1.03] focus-visible:!bg-transparent focus-visible:!text-blue-600 motion-reduce:transform-none"
              onClick={() => {
                const nextWeek = weeks[weekIndex - 1];
                if (!nextWeek) {
                  return;
                }
                void handleSelectWeek(nextWeek.weekNo);
              }}
              disabled={!canGoPreviousWeek || hasBusyAction || hasPendingChanges}
              aria-label="Previous week"
              title="Previous week"
            >
              <ChevronLeft className="size-4 !text-slate-500 transition-transform duration-200 ease-out group-hover:scale-125 group-hover:!text-blue-600 group-focus-visible:scale-125 group-focus-visible:!text-blue-600" />
              Previous
            </Button>
            <div className="min-w-[16rem] max-w-[22rem]">
              <AppDropdown
                value={selectedWeekNo ? String(selectedWeekNo) : ""}
                options={weekOptions}
                searchable={false}
                onValueChange={(nextValue) => {
                  const nextWeekNo = Number(nextValue);
                  if (!Number.isFinite(nextWeekNo) || nextWeekNo <= 0) {
                    return;
                  }
                  void handleSelectWeek(nextWeekNo);
                }}
                placeholder="Week"
                ariaLabel="Week"
                disabled={!weeks.length || hasBusyAction || hasPendingChanges}
              />
            </div>
            <div className="flex items-center gap-3">
              <Button
                variant="ghost"
                className="group h-10 transform-gpu gap-1.5 rounded-md px-2 !text-slate-500 transition-transform duration-200 ease-out hover:scale-[1.03] hover:!bg-transparent hover:!text-blue-600 focus-visible:scale-[1.03] focus-visible:!bg-transparent focus-visible:!text-blue-600 motion-reduce:transform-none"
                onClick={() => {
                  const nextWeek = weeks[weekIndex + 1];
                  if (!nextWeek) {
                    return;
                  }
                  void handleSelectWeek(nextWeek.weekNo);
                }}
                disabled={!canGoNextWeek || hasBusyAction || hasPendingChanges}
                aria-label="Next week"
                title="Next week"
              >
                Next
                <ChevronRight className="size-4 !text-slate-500 transition-transform duration-200 ease-out group-hover:scale-125 group-hover:!text-blue-600 group-focus-visible:scale-125 group-focus-visible:!text-blue-600" />
              </Button>
              <div className="h-6 w-px self-center bg-slate-300" aria-hidden />
              <span
                ref={topActionMenuAnchorRef}
                className="inline-flex"
                onMouseEnter={() => {
                  if (hasPendingChanges || hasBusyAction) {
                    return;
                  }
                  clearTopActionMenuCloseTimeout();
                  setTopActionMenuOpen(true);
                }}
                onMouseLeave={() => {
                  scheduleTopActionMenuClose();
                }}
              >
                <Button
                  variant="default"
                  onClick={() => setTopActionMenuOpen((current) => !current)}
                  disabled={hasBusyAction || hasPendingChanges}
                >
                  Actions
                  <MoreHorizontal className="size-4" />
                </Button>
              </span>
            </div>
          </div>
        </div>

        <div className="mt-4 border-t border-slate-200/80 pt-4">
          {refreshMessage ? (
            <p className="mb-3 rounded-xl border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800">{refreshMessage}</p>
          ) : null}

          {state === "error" ? (
            <EmptyPanel
              icon={<AlertCircle className="size-5 text-rose-500" />}
              message={error || "Could not load Shiftzy schedules."}
            />
          ) : null}

          {state === "empty" ? (
            <EmptyPanel
              icon={<CalendarDays className="size-5 text-slate-400" />}
              message="No schedules found for this week."
            />
          ) : null}

          {state === "ready" ? (
            <>
              {refreshing ? (
                <div className="absolute inset-0 z-20 flex items-center justify-center rounded-2xl bg-white/70 backdrop-blur-[1px]">
                  <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft">
                    <Loader2 className="size-4 animate-spin text-blue-600" />
                    <span>Refreshing board...</span>
                  </div>
                </div>
              ) : null}

              <div className="overflow-x-auto pb-1">
                <div className="grid w-max grid-flow-col auto-cols-[230px] gap-3">
                  {weekDays.map((day) => {
                    const daySchedules = schedulesByDate.get(day.isoDate) || [];
                    return (
                  <div
                    key={day.isoDate}
                    onDragOver={(event) => {
                      if (!canEditShiftzy || !draggingScheduleId) {
                        return;
                      }
                      event.preventDefault();
                      setDeleteDropZoneActive(false);
                      setDropTargetDate(day.isoDate);
                    }}
                    onDrop={(event) => {
                      if (!canEditShiftzy) {
                        return;
                      }
                      event.preventDefault();
                      void handleDropSchedule(day.isoDate);
                    }}
                    onDragLeave={() => {
                      if (dropTargetDate === day.isoDate) {
                        setDropTargetDate(null);
                      }
                    }}
                    className={`group/day flex min-h-[24rem] flex-col rounded-2xl border p-3 transition-colors ${
                      dropTargetDate === day.isoDate && draggingScheduleId
                        ? "border-blue-400 bg-blue-50/55"
                        : day.isToday
                          ? "border-blue-300 bg-[linear-gradient(172deg,rgba(239,246,255,0.96)_0%,rgba(255,255,255,0.98)_52%,rgba(236,253,245,0.94)_100%)] shadow-[0_14px_30px_-18px_rgba(37,99,235,0.55)]"
                          : "border-blue-100 bg-white/95"
                    }`}
                  >
                    <div className="border-b border-slate-100 pb-2">
                      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{day.dayLabel}</p>
                      <p className={`mt-1 text-sm font-semibold ${day.isToday ? "text-blue-900" : "text-slate-900"}`}>{day.dateLabel}</p>
                    </div>

                        <div className="mt-3 flex flex-1 flex-col gap-2">
                          {daySchedules.length ? (
                            daySchedules.map((item) => {
                              const isPendingChange = Boolean(pendingScheduleIds[item.id]);
                              return (
                              <button
                                key={item.id}
                                type="button"
                                onClick={() => handleOpenEdit(item)}
                                draggable={canEditShiftzy && !hasBusyAction}
                                onDragStart={(event) => {
                                  if (!canEditShiftzy || hasBusyAction) {
                                    event.preventDefault();
                                    return;
                                  }
                                  setDraggingScheduleId(item.id);
                                  setDeleteDropZoneActive(false);
                                  setDropTargetDate(item.date);
                                  event.dataTransfer.effectAllowed = "move";
                                  event.dataTransfer.setData("text/plain", item.id);
                                }}
                                onDragEnd={() => {
                                  setDraggingScheduleId(null);
                                  setDropTargetDate(null);
                                  setDeleteDropZoneActive(false);
                                }}
                                className={`group rounded-md border p-3 text-left shadow-sm transition hover:-translate-y-0.5 hover:shadow-md ${
                                  isPendingChange
                                    ? "border-sky-400 bg-[linear-gradient(165deg,rgba(224,242,254,0.96)_0%,rgba(219,234,254,0.94)_58%,rgba(255,255,255,0.98)_100%)] shadow-[0_0_0_1px_rgba(56,189,248,0.5)] hover:border-sky-500 hover:bg-[linear-gradient(165deg,rgba(219,234,254,0.98)_0%,rgba(191,219,254,0.95)_58%,rgba(239,246,255,0.98)_100%)]"
                                    : "border-slate-200 bg-[linear-gradient(165deg,rgba(248,250,252,0.96)_0%,rgba(241,245,249,0.93)_55%,rgba(255,255,255,0.98)_100%)] hover:border-slate-300 hover:bg-[linear-gradient(165deg,rgba(241,245,249,0.98)_0%,rgba(226,232,240,0.95)_55%,rgba(248,250,252,0.98)_100%)]"
                                } ${
                                  draggingScheduleId === item.id ? "opacity-50" : ""
                                }`}
                              >
                                <div className="flex items-start justify-between gap-2">
                                  <div className="flex items-center gap-2">
                                    <span className="inline-flex size-8 items-center justify-center rounded-full border border-blue-300 bg-white text-blue-500">
                                      <CircleUserRound className="size-5" />
                                    </span>
                                    <p className="text-base font-semibold leading-tight text-slate-800">{item.employeeName}</p>
                                  </div>
                                  <Grip className="mt-1 size-4 text-slate-400" />
                                </div>

                                <div className="mt-3 border-t border-slate-300 pt-3">
                                  <p className="text-sm font-semibold leading-tight text-slate-800">
                                    {item.shiftName} {normalizeTimeLabel(item.startTime)} - {normalizeTimeLabel(item.endTime)}
                                  </p>
                                  <div className="mt-2 flex items-end justify-between gap-2">
                                    <p className="text-sm text-slate-700">{item.positionName}</p>
                                    <PositionIcon iconUrl={resolvePositionIconUrl(item, bootstrap)} />
                                  </div>
                                  {item.note ? <p className="mt-2 line-clamp-2 text-[11px] text-slate-500">{item.note}</p> : null}
                                </div>
                              </button>
                              );
                            })
                          ) : (
                            <div className="flex flex-1 items-center justify-center rounded-xl border border-dashed border-slate-200 bg-slate-50 px-3 py-6 text-center text-xs text-slate-500">
                              No shifts
                            </div>
                          )}
                          {canEditShiftzy ? (
                            <button
                              type="button"
                              onClick={() => handleOpenCreate(day.isoDate)}
                              disabled={hasBusyAction}
                              className="flex h-16 items-center justify-center rounded-md border border-dashed border-slate-300 bg-slate-100/75 px-3 text-center text-xs text-slate-500 opacity-0 transition group-hover/day:opacity-85 hover:border-slate-400 hover:bg-slate-100 disabled:pointer-events-none disabled:opacity-0"
                            >
                              + Add schedule
                            </button>
                          ) : null}
                        </div>
                      </div>
                    );
                  })}
                </div>
                {canEditShiftzy && draggingScheduleId ? (
                  <div
                    onDragOver={(event) => {
                      if (!draggingScheduleId) {
                        return;
                      }
                      event.preventDefault();
                      setDropTargetDate(null);
                      setDeleteDropZoneActive(true);
                    }}
                    onDrop={(event) => {
                      if (!canEditShiftzy) {
                        return;
                      }
                      event.preventDefault();
                      handleDropScheduleDelete();
                    }}
                    onDragLeave={() => {
                      setDeleteDropZoneActive(false);
                    }}
                    className={`mx-auto mt-3 flex h-12 w-[260px] max-w-[80vw] items-center justify-center gap-2 rounded-xl border border-dashed px-4 text-sm transition-colors ${
                      deleteDropZoneActive
                        ? "border-rose-500 bg-rose-100/70 text-rose-700 opacity-100"
                        : "border-rose-300 bg-rose-100/35 text-rose-500 opacity-75"
                    }`}
                  >
                    <Trash2 className="size-4" />
                    <span>Drop here to remove shift</span>
                  </div>
                ) : null}
              </div>
              {canEditShiftzy && hasPendingChanges ? (
                <div className="mt-4 flex items-center justify-end gap-2 border-t border-slate-200/80 pt-4">
                  <Button
                    variant="ghost"
                    className="h-9 px-2 text-slate-600 hover:bg-slate-100 hover:text-slate-900"
                    onClick={handleDiscardPendingChanges}
                    disabled={hasBusyAction}
                  >
                    Discard
                  </Button>
                  <Button
                    variant="default"
                    className="h-9 px-3"
                    onClick={() => void handleSavePendingChanges()}
                    disabled={hasBusyAction}
                  >
                    {savingChanges ? <Loader2 className="size-4 animate-spin" /> : null}
                    Save
                  </Button>
                </div>
              ) : null}
            </>
          ) : null}
        </div>
      </section>

      <section className="relative overflow-hidden rounded-2xl border border-blue-100 bg-white/90 p-4 shadow-soft">
        <div className="grid gap-3 sm:grid-cols-3">
          <StatCard
            label="Week Range"
            value={selectedWeek ? `${formatDateNumeric(selectedWeek.startDate)} to ${formatDateNumeric(selectedWeek.endDate)}` : "-"}
          />
          <StatCard label="Total Shifts" value={String(schedules.length)} />
          <StatCard
            label="Team Members"
            value={String(new Set(schedules.map((item) => item.employeeId)).size)}
            icon={<Users className="size-4 text-blue-600" />}
          />
        </div>
        {(loadingPage || refreshing || backgroundRefreshing) ? (
          <div className="absolute inset-0 z-10 flex items-center justify-center rounded-2xl bg-white/70 backdrop-blur-[1px]">
            <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft">
              <Loader2 className="size-4 animate-spin text-blue-600" />
              <span>Refreshing summary...</span>
            </div>
          </div>
        ) : null}
      </section>

      {selectedWeekNo ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40">
          <div className={`mx-4 sm:mx-6 lg:mr-6 ${sidebarVisuallyExpanded ? "lg:ml-[18.75rem]" : "lg:ml-[6.5rem]"}`}>
            <div className="mx-auto w-full max-w-[1600px]">
              <CacheStatusChip
                text={cacheStatusText}
                onRefresh={() => void loadSchedulesForWeek(selectedWeekNo, true)}
                disabled={refreshing || loadingPage || savingChanges || hasPendingChanges}
                refreshing={refreshing || savingChanges}
                refreshLabel="Refresh Shiftzy board"
                tooltipText="Reload schedules for selected week"
                containerClassName="pointer-events-auto"
                className="max-w-[min(90vw,34rem)]"
              />
            </div>
          </div>
        </div>
      ) : null}

      <ShiftzyScheduleEditModal
        open={isEditModalOpen}
        onOpenChange={setIsEditModalOpen}
        mode={editorMode}
        schedule={editingSchedule}
        employees={bootstrap.employees}
        positions={bootstrap.positions}
        shifts={bootstrap.shifts}
        canEdit={canEditShiftzy}
        saving={savingChanges}
        onSave={handleStageScheduleChange}
      />

      <Dialog
        open={isDuplicateModalOpen}
        onOpenChange={(nextOpen) => {
          if (duplicatingSchedules) {
            return;
          }
          setIsDuplicateModalOpen(nextOpen);
        }}
      >
        <DialogContent className="max-w-[520px] rounded-xl bg-white p-6">
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70"
            aria-label="Close duplicate schedules modal"
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader>
            <DialogTitle>Duplicate schedules</DialogTitle>
            <DialogDescription>
              Copy schedules from one week to another week.
            </DialogDescription>
          </DialogHeader>
          <div className="mt-4 space-y-3">
            <FormRow label="From Week">
              <AppDropdown
                value={duplicateFromWeekNo ? String(duplicateFromWeekNo) : ""}
                options={weekOptions}
                searchable={false}
                placeholder=""
                ariaLabel="From week"
                onValueChange={(nextValue) => {
                  const nextWeekNo = Number(nextValue);
                  if (!Number.isFinite(nextWeekNo) || nextWeekNo <= 0) {
                    return;
                  }
                  setDuplicateFromWeekNo(nextWeekNo);
                }}
                disabled={duplicatingSchedules}
              />
            </FormRow>
            <FormRow label="To Week">
              <AppDropdown
                value={duplicateToWeekNo ? String(duplicateToWeekNo) : ""}
                options={weekOptions}
                searchable={false}
                placeholder=""
                ariaLabel="To week"
                onValueChange={(nextValue) => {
                  const nextWeekNo = Number(nextValue);
                  if (!Number.isFinite(nextWeekNo) || nextWeekNo <= 0) {
                    return;
                  }
                  setDuplicateToWeekNo(nextWeekNo);
                }}
                disabled={duplicatingSchedules}
              />
            </FormRow>
          </div>
          <DialogFooter>
            <Button onClick={() => void handleDuplicateSchedules()} disabled={!canSubmitDuplicate || duplicatingSchedules}>
              {duplicatingSchedules ? (
                <>
                  <Loader2 className="size-4 animate-spin" />
                  Duplicating...
                </>
              ) : (
                "Duplicate schedules"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {loadingPage ? (
        <div className="fixed inset-0 z-30 flex items-center justify-center bg-slate-950/25 backdrop-blur-[1.5px]">
          <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft">
            <Loader2 className="size-4 animate-spin text-blue-600" />
            <span>Loading Shiftzy board...</span>
          </div>
        </div>
      ) : null}

      {!hasSessionToken ? (
        <div className="fixed inset-0 z-30 flex items-center justify-center bg-slate-950/25 backdrop-blur-[1.5px]">
          <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft">
            <Loader2 className="size-4 animate-spin text-blue-600" />
            <span>Preparing authenticated session...</span>
          </div>
        </div>
      ) : null}
      {exportingSchedulesPdf ? <PageLoadingOverlay className="z-40" message="Generating PDF preview..." /> : null}
    </div>
  );
}

function StatCard({ label, value, icon }: { label: string; value: string; icon?: JSX.Element }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
      <p className="text-xs uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <p className="mt-1 inline-flex items-center gap-1.5 text-sm font-semibold text-slate-900">
        {icon}
        <span>{value}</span>
      </p>
    </div>
  );
}

function PositionIcon({ iconUrl }: { iconUrl: string | null }) {
  const [failed, setFailed] = useState(false);
  if (iconUrl && !failed) {
    return (
      <img
        src={iconUrl}
        alt=""
        loading="lazy"
        onError={() => setFailed(true)}
        className="size-5 rounded-sm object-contain"
      />
    );
  }
  return <BriefcaseBusiness className="size-4 text-slate-500" />;
}
