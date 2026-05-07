import { useEffect, useRef, useState } from "react";
import { Loader2, X } from "lucide-react";

import type { EsnumItem } from "@/components/dashboard/types";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import { useApiRequest } from "@/hooks/useApiRequest";
import {
  readBrowserCacheSnapshot,
  removeBrowserCacheByPrefix,
  writeBrowserCache,
} from "@/lib/browserCache";
import { cn } from "@/lib/utils";
import { TRADSPHERE_CACHE_TTL_MS, shouldFetchNetwork, type CachePolicy } from "@shared/cache";

import { ScheduleTable, type ScheduleTableData, type ScheduleViewMode } from "./ScheduleTable";
import { ScheduleViewModeToggle } from "./ScheduleViewModeToggle";

interface ScheduleModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  selectedEstnum: EsnumItem | null;
  accountCode?: string;
  billingType?: string | null;
  headers: HeadersInit;
  cacheInvalidationToken?: number;
  invalidatedEstnum?: number | null;
}

const SCHEDULE_TABLE_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.SCHEDULE_TABLE;

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

function asNumber(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return 0;
}

function unwrapData(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

function toViewMode(value: unknown): ScheduleViewMode {
  return String(value ?? "").trim().toLowerCase() === "detail" ? "detail" : "compact";
}

function buildScheduleTableCacheKey(estNum: number, viewMode: ScheduleViewMode): string {
  return `${estNum}:${viewMode}`;
}

function buildBrowserScheduleCacheKey(
  accountCode: string,
  estNum: number,
  viewMode: ScheduleViewMode,
  billingType: string,
): string {
  return `schedule-table:${accountCode}:${estNum}:${viewMode}:${billingType.trim().toLowerCase()}`;
}

function normalizeTableData(payload: unknown): ScheduleTableData | null {
  const data = unwrapData(payload);
  if (!isRecord(data)) {
    return null;
  }

  const rawTable = isRecord(data.table) ? data.table : null;
  const rawSummary = isRecord(data.summary) ? data.summary : null;
  if (!rawTable || !rawSummary) {
    return null;
  }

  const staticColumns = Array.isArray(rawTable.staticColumns)
    ? rawTable.staticColumns.map((value) => asString(value)).filter(Boolean)
    : [];
  const weekColumns = Array.isArray(rawTable.weekColumns)
    ? rawTable.weekColumns
        .filter((column): column is Record<string, unknown> => isRecord(column))
        .map((column) => ({
          key: asString(column.key),
          label: asString(column.label),
          monthLabel: asString(column.monthLabel),
        }))
        .filter((column) => column.key)
    : [];
  const monthGroups = Array.isArray(rawTable.monthGroups)
    ? rawTable.monthGroups
        .filter((group): group is Record<string, unknown> => isRecord(group))
        .map((group) => ({
          label: asString(group.label),
          count: asNumber(group.count),
        }))
        .filter((group) => group.count > 0)
    : [];

  const zeroSpotWeekKeys = Array.isArray(rawTable.zeroSpotWeekKeys)
    ? rawTable.zeroSpotWeekKeys.map((key) => asString(key)).filter(Boolean)
    : [];

  const rawTotals = isRecord(rawTable.totals) ? rawTable.totals : {};
  const totals = {
    weeklySpotTotals: Array.isArray(rawTotals.weeklySpotTotals)
      ? rawTotals.weeklySpotTotals.map((value) => asNumber(value))
      : [],
    weeklyGrossTotals: Array.isArray(rawTotals.weeklyGrossTotals)
      ? rawTotals.weeklyGrossTotals.map((value) => asString(value))
      : [],
    monthlyGrossTotals: Array.isArray(rawTotals.monthlyGrossTotals)
      ? rawTotals.monthlyGrossTotals.map((value) => asString(value))
      : [],
    totalSpot: asNumber(rawTotals.totalSpot),
    totalGross: asString(rawTotals.totalGross),
  };

  const rows = Array.isArray(rawTable.rows)
    ? rawTable.rows
        .filter((row): row is Record<string, unknown> => isRecord(row))
        .map((row) => ({
          kind: "data" as const,
          values: Array.isArray(row.values) ? row.values.map((value) => asString(value)) : [],
          weekValues: Array.isArray(row.weekValues) ? row.weekValues.map((value) => asNumber(value)) : [],
          totalSpot: asNumber(row.totalSpot),
          totalGross: asString(row.totalGross),
        }))
    : undefined;

  const groups = Array.isArray(rawTable.groups)
    ? rawTable.groups
        .filter((group): group is Record<string, unknown> => isRecord(group))
        .map((group) => {
          const subtotal = isRecord(group.subtotal) ? group.subtotal : {};
          const groupRows = Array.isArray(group.rows)
            ? group.rows
                .filter((row): row is Record<string, unknown> => isRecord(row))
                .map((row) => ({
                  kind: "data" as const,
                  values: Array.isArray(row.values) ? row.values.map((value) => asString(value)) : [],
                  weekValues: Array.isArray(row.weekValues) ? row.weekValues.map((value) => asNumber(value)) : [],
                  totalSpot: asNumber(row.totalSpot),
                  totalGross: asString(row.totalGross),
                }))
            : [];

          return {
            groupKey: asString(group.groupKey),
            groupLabel: asString(group.groupLabel),
            rows: groupRows,
            subtotal: {
              weeklySpotTotals: Array.isArray(subtotal.weeklySpotTotals)
                ? subtotal.weeklySpotTotals.map((value) => asNumber(value))
                : [],
              weeklyGrossTotals: Array.isArray(subtotal.weeklyGrossTotals)
                ? subtotal.weeklyGrossTotals.map((value) => asString(value))
                : [],
              monthlyGrossTotals: Array.isArray(subtotal.monthlyGrossTotals)
                ? subtotal.monthlyGrossTotals.map((value) => asString(value))
                : [],
              totalSpot: asNumber(subtotal.totalSpot),
              totalGross: asString(subtotal.totalGross),
            },
          };
        })
    : undefined;

  return {
    estNum: asNumber(data.estNum),
    estNumNote: asString(data.estNumNote),
    viewMode: toViewMode(data.viewMode),
    billingType: asString(data.billingType),
    contextTitle: asString(data.contextTitle),
    summary: {
      gross: asString(rawSummary.gross),
      spots: asNumber(rawSummary.spots),
    },
    table: {
      staticColumns,
      weekColumns,
      monthGroups,
      zeroSpotWeekKeys,
      rows,
      groups,
      totals,
    },
  };
}

export function ScheduleModal({
  open,
  onOpenChange,
  selectedEstnum,
  accountCode,
  billingType,
  headers,
  cacheInvalidationToken,
  invalidatedEstnum,
}: ScheduleModalProps) {
  const { requestJson } = useApiRequest();
  const [mode, setMode] = useState<ScheduleViewMode>("compact");
  const [isLoading, setIsLoading] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tableData, setTableData] = useState<ScheduleTableData | null>(null);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
  const [refreshRequestId, setRefreshRequestId] = useState(0);
  const scheduleTableCacheRef = useRef<Record<string, { data: ScheduleTableData; fetchedAt: number }>>({});
  const inFlightRequestRef = useRef<Record<string, Promise<ScheduleTableData>>>({});
  const handledRefreshRequestRef = useRef(0);
  const lastInvalidationRef = useRef<{ token: number; estnum: number | null } | null>(null);

  const estnum = selectedEstnum?.estnum ?? null;
  const normalizedAccountCode = accountCode?.trim().toUpperCase() || "UNKNOWN";
  const hasSchedule = Boolean(selectedEstnum?.hasSchedule);
  const isReportLoadingState = isLoading && hasSchedule && estnum !== null && tableData === null;
  const shouldUseViewportConstrainedHeight = mode === "detail" || isReportLoadingState;

  useEffect(() => {
    if (!open) {
      return;
    }
    setMode("compact");
  }, [open, estnum]);

  useEffect(() => {
    if (cacheInvalidationToken === undefined || invalidatedEstnum === undefined) {
      return;
    }

    const previousInvalidation = lastInvalidationRef.current;
    const hasInvalidationChanged =
      !previousInvalidation ||
      previousInvalidation.token !== cacheInvalidationToken ||
      previousInvalidation.estnum !== invalidatedEstnum;
    lastInvalidationRef.current = {
      token: cacheInvalidationToken,
      estnum: invalidatedEstnum,
    };

    // Do not wipe caches on initial mount. Only react to explicit invalidation changes.
    if (!previousInvalidation || !hasInvalidationChanged) {
      return;
    }

    if (invalidatedEstnum === null) {
      scheduleTableCacheRef.current = {};
      inFlightRequestRef.current = {};
      removeBrowserCacheByPrefix(`schedule-table:${normalizedAccountCode}:`);
      return;
    }

    const cachePrefix = `${invalidatedEstnum}:`;
    const nextCache: Record<string, { data: ScheduleTableData; fetchedAt: number }> = {};
    const nextInFlight: Record<string, Promise<ScheduleTableData>> = {};

    for (const [cacheKey, value] of Object.entries(scheduleTableCacheRef.current)) {
      if (!cacheKey.startsWith(cachePrefix)) {
        nextCache[cacheKey] = value;
      }
    }

    for (const [cacheKey, promise] of Object.entries(inFlightRequestRef.current)) {
      if (!cacheKey.startsWith(cachePrefix)) {
        nextInFlight[cacheKey] = promise;
      }
    }

    scheduleTableCacheRef.current = nextCache;
    inFlightRequestRef.current = nextInFlight;
    removeBrowserCacheByPrefix(`schedule-table:${normalizedAccountCode}:${invalidatedEstnum}:`);
  }, [cacheInvalidationToken, invalidatedEstnum, normalizedAccountCode]);

  useEffect(() => {
    let cancelled = false;

    async function fetchScheduleTable(targetEstnum: number, targetMode: ScheduleViewMode): Promise<ScheduleTableData> {
      const qs = new URLSearchParams({
        estNum: String(targetEstnum),
        mode: targetMode,
        billingType: billingType?.trim() || "Calendar",
      });

      const payload = await requestJson(`/api/tradsphere/v1/schedules/table?${qs.toString()}`, {
        headers,
      });
      const normalized = normalizeTableData(payload);
      if (!normalized) {
        throw new Error("Schedule table response is invalid.");
      }
      return normalized;
    }

    async function loadScheduleTable(
      targetEstnum: number,
      targetMode: ScheduleViewMode,
      policy: CachePolicy,
    ): Promise<void> {
      const resolvedBillingType = billingType?.trim() || "Calendar";
      const cacheKey = buildScheduleTableCacheKey(targetEstnum, targetMode);
      const browserCacheKey = buildBrowserScheduleCacheKey(
        normalizedAccountCode,
        targetEstnum,
        targetMode,
        resolvedBillingType,
      );
      const memoryCached = scheduleTableCacheRef.current[cacheKey] ?? null;
      const browserCached = readBrowserCacheSnapshot<ScheduleTableData>(browserCacheKey);
      const hasFreshMemoryCache = memoryCached
        ? Date.now() - memoryCached.fetchedAt <= SCHEDULE_TABLE_CACHE_TTL_MS
        : false;

      const effectiveCache =
        memoryCached && (!browserCached || memoryCached.fetchedAt >= browserCached.fetchedAt)
          ? {
              data: memoryCached.data,
              fetchedAt: memoryCached.fetchedAt,
              isExpired: !hasFreshMemoryCache,
            }
          : browserCached
            ? {
                data: browserCached.data,
                fetchedAt: browserCached.fetchedAt,
                isExpired: browserCached.isExpired,
              }
            : null;
      const canUseCache = policy !== "network-only" && !!effectiveCache;
      const needsNetwork = shouldFetchNetwork(
        policy,
        effectiveCache
          ? {
              data: effectiveCache.data,
              fetchedAt: effectiveCache.fetchedAt,
              ttlMs: SCHEDULE_TABLE_CACHE_TTL_MS,
              source: "cache",
              ageMs: Math.max(0, Date.now() - effectiveCache.fetchedAt),
              isExpired: effectiveCache.isExpired,
            }
          : null,
      );

      if (canUseCache && effectiveCache) {
        setTableData(effectiveCache.data);
        setLastUpdatedAt(effectiveCache.fetchedAt);
        setError(null);
      }

      if (!needsNetwork) {
        setIsLoading(false);
        setIsRefreshing(false);
        return;
      }

      if (!effectiveCache) {
        setTableData(null);
      }
      setIsLoading(!effectiveCache);
      setIsRefreshing(Boolean(effectiveCache));
      setError(null);

      let requestPromise = inFlightRequestRef.current[cacheKey];
      if (!requestPromise) {
        requestPromise = fetchScheduleTable(targetEstnum, targetMode);
        inFlightRequestRef.current[cacheKey] = requestPromise;
      }

      try {
        const normalized = await requestPromise;
        if (cancelled) {
          return;
        }
        const fetchedAt = Date.now();
        scheduleTableCacheRef.current[cacheKey] = {
          data: normalized,
          fetchedAt,
        };
        writeBrowserCache(
          browserCacheKey,
          normalized,
          SCHEDULE_TABLE_CACHE_TTL_MS,
          {
            source: "network",
            fetchedAt,
          },
        );
        setTableData(normalized);
        setLastUpdatedAt(fetchedAt);
      } catch (requestError) {
        if (cancelled) {
          return;
        }
        if (!effectiveCache) {
          const message =
            requestError instanceof Error && requestError.message
              ? requestError.message
              : "Unable to load schedule data.";
          setError(message);
          setTableData(null);
        } else {
          setError(null);
        }
      } finally {
        if (inFlightRequestRef.current[cacheKey] === requestPromise) {
          delete inFlightRequestRef.current[cacheKey];
        }
        if (!cancelled) {
          setIsLoading(false);
          setIsRefreshing(false);
        }
      }
    }

    if (!open || estnum === null || !hasSchedule) {
      setTableData(null);
      setError(null);
      setIsLoading(false);
      setIsRefreshing(false);
      setLastUpdatedAt(null);
      return () => {
        cancelled = true;
      };
    }

    const isManualRefresh = refreshRequestId !== handledRefreshRequestRef.current;
    const policy: CachePolicy = isManualRefresh ? "network-only" : "stale-while-revalidate";
    if (isManualRefresh) {
      handledRefreshRequestRef.current = refreshRequestId;
    }
    void loadScheduleTable(estnum, mode, policy);

    return () => {
      cancelled = true;
    };
  }, [
    open,
    estnum,
    hasSchedule,
    mode,
    billingType,
    headers,
    cacheInvalidationToken,
    normalizedAccountCode,
    refreshRequestId,
  ]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className={cn(
          "flex min-h-0 max-h-[min(90vh,calc(100dvh-1rem))] flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white !inset-0 !m-auto !p-0",
          shouldUseViewportConstrainedHeight
            ? "!h-[min(90vh,calc(100dvh-1rem))] min-h-[65vh] !w-fit !max-w-[85vw] sm:min-h-[70vh]"
            : "!h-fit !w-fit !max-w-[85vw]",
        )}
      >
        <div className="shrink-0 border-b border-slate-200 bg-white px-6 py-5 sm:px-8">
          <div className="flex items-start justify-between gap-4">
            <DialogTitle asChild>
              <h2 className="text-xl font-semibold text-slate-900">Schedules</h2>
            </DialogTitle>
            <button
              type="button"
              onClick={() => onOpenChange(false)}
              className="rounded-md p-1 text-slate-500 transition-colors hover:text-slate-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-400"
              aria-label="Close schedules modal"
            >
              <X className="size-5" />
            </button>
          </div>
          <div className="mt-3 flex flex-wrap items-center gap-4">
            <ScheduleViewModeToggle mode={mode} onModeChange={setMode} detailEnabled />
          </div>
        </div>

        <div
          className={cn(
            "overflow-x-auto overscroll-contain px-6 py-5 sm:px-8 sm:py-6",
            shouldUseViewportConstrainedHeight ? "min-h-0 flex-1 overflow-y-auto" : "flex-none overflow-y-visible",
          )}
        >
          {isLoading ? (
            <div className="flex min-h-[240px] items-center justify-center gap-3 text-slate-600">
              <Loader2 className="size-5 animate-spin" />
              <span className="text-sm">Loading schedules...</span>
            </div>
          ) : error ? (
            <div className="rounded-md border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</div>
          ) : estnum === null ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
              Select an EstNum to view schedules.
            </div>
          ) : !hasSchedule ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
              No schedule yet for this EstNum.
            </div>
          ) : tableData ? (
            <div className="inline-block align-top">
              <ScheduleTable data={tableData} />
            </div>
          ) : (
            <div className="rounded-md border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
              Schedule table data is unavailable.
            </div>
          )}
        </div>

        <footer className="shrink-0 border-t border-slate-100 bg-white px-4 pt-2 pb-[calc(1rem+env(safe-area-inset-bottom))]">
          <CacheStatusChip
            text={
              isLoading
                ? "Loading..."
                : isRefreshing
                  ? "Refreshing..."
                  : lastUpdatedAt
                    ? `Last updated ${formatRelativeTime(lastUpdatedAt)}`
                    : "No cached data yet"
            }
            onRefresh={() => {
              if (!isLoading && !isRefreshing) {
                setRefreshRequestId((current) => current + 1);
              }
            }}
            disabled={isLoading || isRefreshing}
            refreshing={isRefreshing}
            refreshLabel="Refresh schedule data"
            tooltipText="Click to refresh schedule data"
            className="max-w-[min(92vw,28rem)]"
          />
        </footer>
      </DialogContent>
    </Dialog>
  );
}

function formatRelativeTime(timestamp: number): string {
  const ageMs = Math.max(0, Date.now() - timestamp);
  if (ageMs < 30_000) {
    return "just now";
  }

  const minutes = Math.floor(ageMs / 60_000);
  if (minutes < 60) {
    return `${minutes} min ago`;
  }

  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours} hr ago`;
  }

  const days = Math.floor(hours / 24);
  return `${days} day${days > 1 ? "s" : ""} ago`;
}
