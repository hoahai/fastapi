import { useEffect, useMemo, useRef, useState } from "react";
import { AlertCircle } from "lucide-react";

import { EstimateNumberResults } from "@/components/estnums/EstimateNumberResults";
import {
  EstimateNumberSearch,
  type EstimateNumberSearchInterpretationOption,
} from "@/components/estnums/EstimateNumberSearch";
import type { EstimateAccountGroup, EstimateSearchItem, EstimateSearchPage } from "@/components/estnums/types";
import {
  EstimateNumberModal,
  type EstimateNumberModalData,
  type EstimateNumberModalMode,
  type EstimateNumberModalSaveResult,
} from "@/components/dashboard/EstimateNumberModal";
import { ScheduleModal } from "@/components/dashboard/ScheduleModal";
import type { EsnumItem } from "@/components/dashboard/types";
import { PageBanner } from "@/components/layout/PageBanner";
import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import { useToast } from "@/components/ui/toast";
import { useApiRequest } from "@/hooks/useApiRequest";
import {
  readBrowserCacheSnapshot,
  removeBrowserCacheByPrefix,
  writeBrowserCache,
} from "@/lib/browserCache";
import { shouldFetchNetwork, type CachePolicy } from "@shared/cache";

const SEARCH_LIMIT = 50;
const SEARCH_MIN_TEXT_LENGTH = 3;
const SEARCH_CACHE_TTL_MS = 5 * 60 * 1000;
const SEARCH_TIMEZONE = "America/Chicago";
const ESTNUMS_PAGE_CACHE_VERSION = "v3";
const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";

type SearchUiState = "idle" | "min-query" | "loading" | "ready" | "empty" | "error";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type AccountDirectoryItem = {
  accountCode: string;
  name: string;
  billingType?: string | null;
};

type SearchLoadOptions = {
  policy: CachePolicy;
  append: boolean;
};

type ConfirmedSearchParams =
  | { mode: "default" }
  | { mode: "today"; raw: string }
  | { mode: "text"; raw: string }
  | { mode: "estnum"; estNum: string; raw: string }
  | { mode: "year"; year: number; raw: string }
  | { mode: "month-year"; year: number; month: number; raw: string }
  | { mode: "quarter-year"; year: number; quarter: number; raw: string };

type PendingSearchInterpretation = {
  raw: string;
  options: Array<EstimateNumberSearchInterpretationOption & { params: ConfirmedSearchParams }>;
};

const MONTH_BY_NAME: Record<string, number> = {
  jan: 1,
  january: 1,
  feb: 2,
  february: 2,
  mar: 3,
  march: 3,
  apr: 4,
  april: 4,
  may: 5,
  jun: 6,
  june: 6,
  jul: 7,
  july: 7,
  aug: 8,
  august: 8,
  sep: 9,
  sept: 9,
  september: 9,
  oct: 10,
  october: 10,
  nov: 11,
  november: 11,
  dec: 12,
  december: 12,
};

const MONTH_FULL_LABELS = [
  "",
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function unwrapData(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
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

function asNullableString(value: unknown): string | null {
  const normalized = asString(value);
  return normalized || null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function asBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return ["1", "true", "yes", "y", "on"].includes(normalized);
  }
  return false;
}

function asIntArray(value: unknown): number[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const output: number[] = [];
  for (const item of value) {
    const parsed = asNumber(item);
    if (parsed === null) {
      continue;
    }
    output.push(Math.trunc(parsed));
  }
  return output;
}

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }
  return fallback;
}

function normalizeQueryKey(query: string): string {
  return query.trim().toLowerCase();
}

function isFourDigitYearQuery(query: string): boolean {
  return /^\d{4}$/.test(query.trim());
}

function isExactEstNumQuery(query: string): boolean {
  return /^\d+$/.test(query.trim());
}

function parseYearSearchQuery(query: string): number | null {
  const match = query.trim().match(/^year\s*:\s*(\d{4})$/i);
  if (!match) {
    return null;
  }
  const parsed = Number(match[1]);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return Math.trunc(parsed);
}

function normalizeYearToken(value: string): number | null {
  const trimmed = value.trim();
  if (!/^\d{2}(\d{2})?$/.test(trimmed)) {
    return null;
  }
  if (trimmed.length === 4) {
    const parsed = Number(trimmed);
    if (parsed >= 1901 && parsed <= 2155) {
      return parsed;
    }
    return null;
  }
  const twoDigit = Number(trimmed);
  const normalized = 2000 + twoDigit;
  if (normalized >= 1901 && normalized <= 2155) {
    return normalized;
  }
  return null;
}

function parseMonthYearQuery(query: string): { month: number; year: number } | null {
  const compact = query.trim();
  if (!compact) {
    return null;
  }

  const monthNameMatch = compact.match(/^([a-zA-Z]+)\s+(\d{2}|\d{4})$/);
  if (monthNameMatch) {
    const month = MONTH_BY_NAME[monthNameMatch[1].toLowerCase()];
    const year = normalizeYearToken(monthNameMatch[2]);
    if (month && year !== null) {
      return { month, year };
    }
  }

  const slashMatch = compact.match(/^(\d{1,2})\s*\/\s*(\d{2}|\d{4})$/);
  if (slashMatch) {
    const month = Number(slashMatch[1]);
    const year = normalizeYearToken(slashMatch[2]);
    if (month >= 1 && month <= 12 && year !== null) {
      return { month, year };
    }
  }

  return null;
}

function parseQuarterYearQuery(query: string): { quarter: number; year: number } | null {
  const compact = query.trim();
  if (!compact) {
    return null;
  }

  const quarterLeadingMatch = compact.match(/^q([1-4])(?:\s*['’]\s*|\s+)(\d{2}|\d{4})$/i);
  if (quarterLeadingMatch) {
    const quarter = Number(quarterLeadingMatch[1]);
    const year = normalizeYearToken(quarterLeadingMatch[2]);
    if (year !== null) {
      return { quarter, year };
    }
  }

  const yearLeadingMatch = compact.match(/^(\d{2}|\d{4})(?:\s*['’]\s*|\s+)q([1-4])$/i);
  if (yearLeadingMatch) {
    const year = normalizeYearToken(yearLeadingMatch[1]);
    const quarter = Number(yearLeadingMatch[2]);
    if (year !== null) {
      return { quarter, year };
    }
  }

  return null;
}

function isTodayQuery(query: string): boolean {
  return normalizeQueryKey(query) === "today";
}

function formatMonthYearLabel(month: number, year: number): string {
  const monthLabel = MONTH_FULL_LABELS[month] || `Month ${month}`;
  return `${monthLabel} ${year}`;
}

function buildSearchInterpretationOptions(query: string): PendingSearchInterpretation["options"] {
  const trimmed = query.trim();
  if (!trimmed || isTodayQuery(trimmed)) {
    return [];
  }

  if (isFourDigitYearQuery(trimmed)) {
    return [
      {
        id: "year",
        label: `Search by year "${trimmed}"`,
        description: `Fetch all estimate numbers in broadcast year ${trimmed}.`,
        params: { mode: "year", year: Number(trimmed), raw: trimmed },
      },
      {
        id: "estnum",
        label: `Search by EstNum "${trimmed}"`,
        description: `Fetch the exact estimate number ${trimmed}.`,
        params: { mode: "estnum", estNum: trimmed, raw: trimmed },
      },
    ];
  }

  const monthYear = parseMonthYearQuery(trimmed);
  if (monthYear) {
    return [
      {
        id: "month-year",
        label: `Search by month/year "${formatMonthYearLabel(monthYear.month, monthYear.year)}"`,
        description: "Use broadcast month and year filters.",
        params: { mode: "month-year", year: monthYear.year, month: monthYear.month, raw: trimmed },
      },
      {
        id: "text",
        label: `Search as text "${trimmed}"`,
        description: "Run a general text search across estimate fields.",
        params: { mode: "text", raw: trimmed },
      },
    ];
  }

  const quarterYear = parseQuarterYearQuery(trimmed);
  if (quarterYear) {
    return [
      {
        id: "quarter-year",
        label: `Search by quarter "Q${quarterYear.quarter} ${quarterYear.year}"`,
        description: "Use broadcast quarter and year filters.",
        params: { mode: "quarter-year", year: quarterYear.year, quarter: quarterYear.quarter, raw: trimmed },
      },
      {
        id: "text",
        label: `Search as text "${trimmed}"`,
        description: "Run a general text search across estimate fields.",
        params: { mode: "text", raw: trimmed },
      },
    ];
  }

  return [];
}

function toConfirmedSearchParams(query: string): ConfirmedSearchParams {
  const trimmed = query.trim();
  if (!trimmed) {
    return { mode: "default" };
  }
  if (isTodayQuery(trimmed)) {
    return { mode: "today", raw: trimmed };
  }

  const explicitYear = parseYearSearchQuery(trimmed);
  if (explicitYear !== null) {
    return { mode: "year", year: explicitYear, raw: trimmed };
  }

  if (isExactEstNumQuery(trimmed)) {
    return { mode: "estnum", estNum: trimmed, raw: trimmed };
  }

  return { mode: "text", raw: trimmed };
}

function isSearchEndpointMissing(error: unknown): boolean {
  const message = getErrorMessage(error, "").toLowerCase();
  if (!message) {
    return false;
  }
  return message.includes("404") || message.includes("not found") || message.includes("/estnums/search");
}

function deriveQuarter(month: number | null | undefined): number | null {
  if (typeof month !== "number" || month < 1 || month > 12) {
    return null;
  }
  return Math.floor((month - 1) / 3) + 1;
}

function deriveYear(item: EstimateSearchItem): number | null {
  if (typeof item.year === "number") {
    return item.year;
  }
  const years = item.broadcastYears || [];
  if (years.length > 0) {
    return years[0] ?? null;
  }
  const flightStart = asString(item.flightStart);
  if (/^\d{4}-\d{2}-\d{2}$/.test(flightStart)) {
    return Number(flightStart.slice(0, 4));
  }
  return null;
}

function deriveMonth(item: EstimateSearchItem): number | null {
  if (typeof item.month === "number") {
    return item.month;
  }
  const months = item.broadcastMonths || [];
  if (months.length > 0) {
    return months[0] ?? null;
  }
  const flightStart = asString(item.flightStart);
  if (/^\d{4}-\d{2}-\d{2}$/.test(flightStart)) {
    return Number(flightStart.slice(5, 7));
  }
  return null;
}

function toNumberOrNull(value: unknown): number | null {
  const parsed = asNumber(value);
  if (parsed === null) {
    return null;
  }
  return Math.trunc(parsed);
}

function withDerivedPeriod(item: EstimateSearchItem): EstimateSearchItem {
  const year = deriveYear(item);
  const month = deriveMonth(item);
  const quarter = item.quarter ?? deriveQuarter(month);
  return {
    ...item,
    year,
    month,
    quarter,
  };
}

function sortItems(items: EstimateSearchItem[]): EstimateSearchItem[] {
  return [...items].sort((a, b) => {
    const accountDiff = a.accountCode.localeCompare(b.accountCode);
    if (accountDiff !== 0) {
      return accountDiff;
    }
    const yearA = a.year ?? -1;
    const yearB = b.year ?? -1;
    if (yearA !== yearB) {
      return yearB - yearA;
    }
    const quarterA = a.quarter ?? -1;
    const quarterB = b.quarter ?? -1;
    if (quarterA !== quarterB) {
      return quarterB - quarterA;
    }
    return b.estNum - a.estNum;
  });
}

function buildGroups(items: EstimateSearchItem[]): EstimateAccountGroup[] {
  const accountBuckets = new Map<string, EstimateSearchItem[]>();
  for (const item of items) {
    const key = item.accountCode.toUpperCase();
    const bucket = accountBuckets.get(key);
    if (bucket) {
      bucket.push(item);
    } else {
      accountBuckets.set(key, [item]);
    }
  }

  const groups: EstimateAccountGroup[] = [];
  for (const [accountCode, accountItems] of accountBuckets.entries()) {
    const yearBuckets = new Map<string, EstimateSearchItem[]>();
    for (const item of accountItems) {
      const year = item.year;
      const quarter = item.quarter;
      const key = year ? `${year}:q${quarter ?? "na"}` : "unknown";
      const bucket = yearBuckets.get(key);
      if (bucket) {
        bucket.push(item);
      } else {
        yearBuckets.set(key, [item]);
      }
    }

    const years = [...yearBuckets.entries()]
      .map(([key, bucket]) => {
        const first = bucket[0];
        const year = first?.year;
        const quarter = first?.quarter;
        const label = year ? (quarter ? `${year} • Q${quarter}` : String(year)) : "Unknown period";
        return {
          key,
          label,
          items: [...bucket].sort((a, b) => b.estNum - a.estNum),
        };
      })
      .sort((a, b) => {
        if (a.key === "unknown" && b.key !== "unknown") {
          return 1;
        }
        if (b.key === "unknown" && a.key !== "unknown") {
          return -1;
        }
        const yearA = Number(a.key.split(":")[0] || "0");
        const yearB = Number(b.key.split(":")[0] || "0");
        if (yearA !== yearB) {
          return yearB - yearA;
        }
        const quarterA = Number(a.key.split("q")[1] || "0");
        const quarterB = Number(b.key.split("q")[1] || "0");
        return quarterB - quarterA;
      });

    groups.push({
      key: accountCode,
      accountCode,
      accountName: accountItems[0]?.accountName ?? null,
      years,
    });
  }

  return groups.sort((a, b) => a.accountCode.localeCompare(b.accountCode));
}

function parseAccountDirectory(payload: unknown): AccountDirectoryItem[] {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  const output: AccountDirectoryItem[] = [];
  const seen = new Set<string>();

  for (const row of data) {
    if (!isRecord(row)) {
      continue;
    }
    const accountCode = asString(row.accountCode ?? row.code).toUpperCase();
    if (!accountCode || seen.has(accountCode)) {
      continue;
    }

    seen.add(accountCode);
    output.push({
      accountCode,
      name: asString(row.name),
      billingType: asNullableString(row.billingType),
    });
  }

  return output.sort((a, b) => a.accountCode.localeCompare(b.accountCode));
}

function parseEstimateItem(
  row: Record<string, unknown>,
  accountDirectoryByCode: Record<string, AccountDirectoryItem>,
): EstimateSearchItem | null {
  const estNum = asNumber(row.estNum ?? row.estnum);
  if (estNum === null) {
    return null;
  }

  const accountCode = asString(
    row.accountCode ?? row.account ?? row.clientCode ?? (isRecord(row.client) ? row.client.code : undefined),
  ).toUpperCase();
  if (!accountCode) {
    return null;
  }

  const accountInfo = accountDirectoryByCode[accountCode];
  const accountName = asNullableString(
    row.accountName ?? row.clientName ?? row.accountNameLabel ?? (isRecord(row.client) ? row.client.name : undefined),
  ) || accountInfo?.name || null;

  const month = toNumberOrNull(row.month);
  const quarter = toNumberOrNull(row.quarter);
  const year = toNumberOrNull(row.year);

  return withDerivedPeriod({
    estNum: Math.trunc(estNum),
    accountCode,
    accountName,
    buyer: asNullableString(row.buyer),
    mediaType: asNullableString(row.mediaType ?? row.media),
    note: asNullableString(row.note),
    hasSchedule: asBoolean(row.hasSchedule ?? row.hasScheduled ?? row.scheduled),
    flightStart: asNullableString(row.flightStart),
    flightEnd: asNullableString(row.flightEnd),
    year,
    quarter,
    month,
    broadcastYears: asIntArray(row.broadcastYears),
    broadcastMonths: asIntArray(row.broadcastMonths),
  });
}

function parseLegacyEstNumsResponse(
  payload: unknown,
  accountDirectoryByCode: Record<string, AccountDirectoryItem>,
): EstimateSearchPage {
  const data = unwrapData(payload);
  const rows = Array.isArray(data) ? data : [];
  const items = rows
    .filter((row): row is Record<string, unknown> => isRecord(row))
    .map((row) => parseEstimateItem(row, accountDirectoryByCode))
    .filter((item): item is EstimateSearchItem => item !== null);

  const sorted = sortItems(items);
  return {
    items: sorted,
    total: sorted.length,
    limit: SEARCH_LIMIT,
    nextCursor: null,
    nextOffset: null,
    backendMode: "legacy-exact",
  };
}

function parseSearchResponse(
  payload: unknown,
  accountDirectoryByCode: Record<string, AccountDirectoryItem>,
): EstimateSearchPage {
  const data = unwrapData(payload);

  if (Array.isArray(data)) {
    const legacy = parseLegacyEstNumsResponse(payload, accountDirectoryByCode);
    return {
      ...legacy,
      backendMode: "search",
    };
  }

  const container = isRecord(data) ? data : {};
  const rawItems = Array.isArray(container.items) ? container.items : [];
  const items = rawItems
    .filter((row): row is Record<string, unknown> => isRecord(row))
    .map((row) => parseEstimateItem(row, accountDirectoryByCode))
    .filter((item): item is EstimateSearchItem => item !== null);

  const total = asNumber(container.total);
  const limit = asNumber(container.limit);
  const nextCursor = asNullableString(container.nextCursor ?? container.cursor ?? container.next);
  const nextOffset = toNumberOrNull(container.nextOffset);

  const sorted = sortItems(items);
  return {
    items: sorted,
    total: total !== null ? Math.trunc(total) : null,
    limit: limit !== null ? Math.max(1, Math.trunc(limit)) : SEARCH_LIMIT,
    nextCursor,
    nextOffset,
    backendMode: "search",
  };
}

function mergeSearchPages(current: EstimateSearchPage, nextPage: EstimateSearchPage): EstimateSearchPage {
  const mergedMap = new Map<string, EstimateSearchItem>();
  for (const item of [...current.items, ...nextPage.items]) {
    mergedMap.set(`${item.accountCode}:${item.estNum}`, item);
  }

  return {
    items: sortItems([...mergedMap.values()]),
    total: nextPage.total ?? current.total,
    limit: nextPage.limit || current.limit,
    nextCursor: nextPage.nextCursor,
    nextOffset: nextPage.nextOffset,
    backendMode: nextPage.backendMode,
  };
}

function formatResultText(page: EstimateSearchPage | null): string | null {
  if (!page) {
    return null;
  }
  if (page.total !== null) {
    if (page.total > page.items.length) {
      return `Showing ${page.items.length} of ${page.total} matches.`;
    }
    return `${page.total} matches.`;
  }
  return `${page.items.length} matches.`;
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

function getCurrentYearInChicago(): number {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: SEARCH_TIMEZONE,
    year: "numeric",
  });
  const year = Number(formatter.format(new Date()));
  return Number.isFinite(year) ? year : new Date().getFullYear();
}

function buildSearchCacheKey(
  confirmedSearch: ConfirmedSearchParams,
  currentYear: number,
  previousYear: number,
): string {
  switch (confirmedSearch.mode) {
    case "default":
      return `estnums:default:${previousYear}:${currentYear}:limit=${SEARCH_LIMIT}:${ESTNUMS_PAGE_CACHE_VERSION}`;
    case "today":
      return `estnums:search:today:tz=${SEARCH_TIMEZONE}:limit=${SEARCH_LIMIT}:${ESTNUMS_PAGE_CACHE_VERSION}`;
    case "text":
      return `estnums:search:text:${encodeURIComponent(normalizeQueryKey(confirmedSearch.raw))}:limit=${SEARCH_LIMIT}:${ESTNUMS_PAGE_CACHE_VERSION}`;
    case "estnum":
      return `estnums:search:estnum:${encodeURIComponent(confirmedSearch.estNum)}:${ESTNUMS_PAGE_CACHE_VERSION}`;
    case "year":
      return `estnums:search:year:${confirmedSearch.year}:${ESTNUMS_PAGE_CACHE_VERSION}`;
    case "month-year":
      return `estnums:search:month:${confirmedSearch.year}-${String(confirmedSearch.month).padStart(2, "0")}:${ESTNUMS_PAGE_CACHE_VERSION}`;
    case "quarter-year":
      return `estnums:search:quarter:${confirmedSearch.year}-Q${confirmedSearch.quarter}:${ESTNUMS_PAGE_CACHE_VERSION}`;
    default:
      return `estnums:search:unknown:${ESTNUMS_PAGE_CACHE_VERSION}`;
  }
}

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

function buildAuthHeaders(includeJsonContentType: boolean): HeadersInit {
  return {
    "X-API-Key": "6ad13c1f7c17c32fb5a4582b4be42df5",
    "X-Tenant-Id": "taaa",
    "X-User-Name": "Hai Truong",
    ...(includeJsonContentType ? { "Content-Type": "application/json" } : {}),
  };
}

function withDirectoryAccountNames(
  page: EstimateSearchPage | null,
  directoryByCode: Record<string, AccountDirectoryItem>,
): EstimateSearchPage | null {
  if (!page) {
    return null;
  }

  const items = page.items.map((item) => {
    if (item.accountName?.trim()) {
      return item;
    }
    const accountCode = item.accountCode.toUpperCase();
    const accountName = directoryByCode[accountCode]?.name || null;
    if (!accountName) {
      return item;
    }
    return {
      ...item,
      accountName,
    };
  });

  return {
    ...page,
    items,
  };
}

export default function EstimateNumbersPage() {
  const toast = useToast();
  const { requestJson } = useApiRequest();
  const requestHeaders = useMemo(() => buildAuthHeaders(false), []);
  const currentYear = useMemo(() => getCurrentYearInChicago(), []);
  const previousYear = currentYear - 1;

  const [accountDirectory, setAccountDirectory] = useState<AccountDirectoryItem[]>([]);
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(() => readSidebarCollapsedState());

  const [inputDraft, setInputDraft] = useState("");
  const [confirmedSearch, setConfirmedSearch] = useState<ConfirmedSearchParams>({ mode: "default" });
  const [pendingInterpretation, setPendingInterpretation] = useState<PendingSearchInterpretation | null>(null);
  const [submissionVersion, setSubmissionVersion] = useState(0);

  const [state, setState] = useState<SearchUiState>("loading");
  const [error, setError] = useState<string | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [page, setPage] = useState<EstimateSearchPage | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [backendSearchUnavailable, setBackendSearchUnavailable] = useState(false);

  const [isScheduleModalOpen, setIsScheduleModalOpen] = useState(false);
  const [selectedScheduleEstnum, setSelectedScheduleEstnum] = useState<EsnumItem | null>(null);
  const [selectedScheduleAccountCode, setSelectedScheduleAccountCode] = useState("");
  const [selectedScheduleBillingType, setSelectedScheduleBillingType] = useState<string | null>(null);

  const [isEstimateModalOpen, setIsEstimateModalOpen] = useState(false);
  const [estimateModalMode, setEstimateModalMode] = useState<EstimateNumberModalMode>("create");
  const [estimateModalInitialData, setEstimateModalInitialData] = useState<EstimateNumberModalData | null>(null);
  const [estimateModalAccountCode, setEstimateModalAccountCode] = useState("");
  const [createAccountCode, setCreateAccountCode] = useState("");

  const pageRef = useRef<EstimateSearchPage | null>(null);
  const requestTokenRef = useRef(0);
  const inFlightRef = useRef<Record<string, Promise<EstimateSearchPage>>>({});

  const accountDirectoryByCode = useMemo(() => {
    return accountDirectory.reduce<Record<string, AccountDirectoryItem>>((map, item) => {
      map[item.accountCode] = item;
      return map;
    }, {});
  }, [accountDirectory]);

  const displayPage = useMemo(
    () => withDirectoryAccountNames(page, accountDirectoryByCode),
    [accountDirectoryByCode, page],
  );

  const groupedResults = useMemo(() => buildGroups(displayPage?.items ?? []), [displayPage]);
  const resultText = useMemo(() => formatResultText(displayPage), [displayPage]);

  const createAccountOptions = useMemo(
    () =>
      accountDirectory.map((item) => ({
        value: item.accountCode,
        label: item.name ? `${item.accountCode} - ${item.name}` : item.accountCode,
      })),
    [accountDirectory],
  );

  const estimateModalAccountName = accountDirectoryByCode[estimateModalAccountCode]?.name || estimateModalAccountCode;

  const canLoadMore = Boolean(
    displayPage &&
      (displayPage.nextCursor ||
        (displayPage.total !== null && displayPage.items.length < displayPage.total) ||
        (displayPage.nextOffset !== null && displayPage.nextOffset > displayPage.items.length)),
  );

  const isAnyModalOpen = isScheduleModalOpen || isEstimateModalOpen;
  const cacheStatusText =
    state === "loading" && !displayPage
      ? "Loading..."
      : isRefreshing
        ? "Refreshing..."
        : cacheStatus
          ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
          : "No cached data yet";

  const interpretationOptions = pendingInterpretation?.options ?? [];

  useEffect(() => {
    pageRef.current = page;
  }, [page]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleStorage = () => {
      setSidebarCollapsed(readSidebarCollapsedState());
    };
    const handleSidebarEvent = (event: Event) => {
      const customEvent = event as CustomEvent<{ collapsed?: boolean }>;
      if (typeof customEvent.detail?.collapsed === "boolean") {
        setSidebarCollapsed(customEvent.detail.collapsed);
        return;
      }
      setSidebarCollapsed(readSidebarCollapsedState());
    };

    window.addEventListener("storage", handleStorage);
    window.addEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    return () => {
      window.removeEventListener("storage", handleStorage);
      window.removeEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function loadAccounts() {
      try {
        const payload = await requestJson("/api/tradsphere/v1/accounts?active=false", {
          headers: requestHeaders,
          errorToast: false,
        });
        if (cancelled) {
          return;
        }

        const parsed = parseAccountDirectory(payload);
        setAccountDirectory(parsed);
        setCreateAccountCode((current) => current || parsed[0]?.accountCode || "");
      } catch (loadError) {
        if (cancelled) {
          return;
        }
        toast.error("Account load failed", getErrorMessage(loadError, "Unable to load TradSphere accounts."));
      }
    }

    void loadAccounts();

    return () => {
      cancelled = true;
    };
  }, [requestHeaders, requestJson, toast]);

  async function fetchDefaultYearWindowPage(): Promise<EstimateSearchPage> {
    const [currentPayload, previousPayload] = await Promise.all([
      requestJson(`/api/tradsphere/v1/estNums?year=${encodeURIComponent(String(currentYear))}`, {
        headers: requestHeaders,
        errorToast: false,
      }),
      requestJson(`/api/tradsphere/v1/estNums?year=${encodeURIComponent(String(previousYear))}`, {
        headers: requestHeaders,
        errorToast: false,
      }),
    ]);

    const currentPage = parseLegacyEstNumsResponse(currentPayload, accountDirectoryByCode);
    const previousPage = parseLegacyEstNumsResponse(previousPayload, accountDirectoryByCode);

    const mergedMap = new Map<string, EstimateSearchItem>();
    for (const item of [...currentPage.items, ...previousPage.items]) {
      mergedMap.set(`${item.accountCode}:${item.estNum}`, item);
    }

    const items = sortItems([...mergedMap.values()]);
    return {
      items,
      total: items.length,
      limit: SEARCH_LIMIT,
      nextCursor: null,
      nextOffset: null,
      backendMode: "legacy-exact",
    };
  }

  async function fetchFirstPageForConfirmedSearch(search: ConfirmedSearchParams): Promise<EstimateSearchPage> {
    if (search.mode === "default") {
      return fetchDefaultYearWindowPage();
    }

    if (search.mode === "year") {
      const payload = await requestJson(`/api/tradsphere/v1/estNums?year=${encodeURIComponent(String(search.year))}`, {
        headers: requestHeaders,
        errorToast: false,
      });
      return parseLegacyEstNumsResponse(payload, accountDirectoryByCode);
    }

    if (search.mode === "month-year") {
      const params = new URLSearchParams();
      params.set("year", String(search.year));
      params.set("month", String(search.month));
      const payload = await requestJson(`/api/tradsphere/v1/estNums?${params.toString()}`, {
        headers: requestHeaders,
        errorToast: false,
      });
      return parseLegacyEstNumsResponse(payload, accountDirectoryByCode);
    }

    if (search.mode === "quarter-year") {
      const params = new URLSearchParams();
      params.set("year", String(search.year));
      params.set("quarter", String(search.quarter));
      const payload = await requestJson(`/api/tradsphere/v1/estNums?${params.toString()}`, {
        headers: requestHeaders,
        errorToast: false,
      });
      return parseLegacyEstNumsResponse(payload, accountDirectoryByCode);
    }

    if (search.mode === "estnum") {
      const payload = await requestJson(`/api/tradsphere/v1/estNums?estNum=${encodeURIComponent(search.estNum)}`, {
        headers: requestHeaders,
        errorToast: false,
      });
      return parseLegacyEstNumsResponse(payload, accountDirectoryByCode);
    }

    const textQuery = search.raw.trim();
    if (search.mode === "text" && textQuery.length < SEARCH_MIN_TEXT_LENGTH) {
      throw new Error("MIN_QUERY_LENGTH");
    }

    const params = new URLSearchParams();
    params.set("q", textQuery);
    params.set("limit", String(SEARCH_LIMIT));
    if (search.mode === "today") {
      params.set("timezone", SEARCH_TIMEZONE);
    }

    const payload = await requestJson(`/api/tradsphere/v1/estNums/search?${params.toString()}`, {
      headers: requestHeaders,
      errorToast: false,
    });
    return parseSearchResponse(payload, accountDirectoryByCode);
  }

  async function fetchNextPageForConfirmedSearch(
    search: ConfirmedSearchParams,
    cursor: string | null,
    offset: number | null,
  ): Promise<EstimateSearchPage> {
    if (search.mode !== "text" && search.mode !== "today") {
      return {
        items: [],
        total: 0,
        limit: SEARCH_LIMIT,
        nextCursor: null,
        nextOffset: null,
        backendMode: "search",
      };
    }

    const params = new URLSearchParams();
    params.set("q", search.raw.trim());
    params.set("limit", String(SEARCH_LIMIT));
    if (cursor) {
      params.set("cursor", cursor);
    } else if (offset !== null && offset >= 0) {
      params.set("offset", String(offset));
    }
    if (search.mode === "today") {
      params.set("timezone", SEARCH_TIMEZONE);
    }

    const payload = await requestJson(`/api/tradsphere/v1/estNums/search?${params.toString()}`, {
      headers: requestHeaders,
      errorToast: false,
    });
    return parseSearchResponse(payload, accountDirectoryByCode);
  }

  async function loadPageData(options: SearchLoadOptions): Promise<void> {
    const activeSearch = confirmedSearch;
    const requestToken = ++requestTokenRef.current;
    const cacheKey = buildSearchCacheKey(activeSearch, currentYear, previousYear);

    if (!options.append && activeSearch.mode === "text" && activeSearch.raw.trim().length < SEARCH_MIN_TEXT_LENGTH) {
      setState("min-query");
      setError(null);
      setPage(null);
      setCacheStatus(null);
      setIsRefreshing(false);
      setIsLoadingMore(false);
      return;
    }

    const snapshot = !options.append ? readBrowserCacheSnapshot<EstimateSearchPage>(cacheKey) : null;

    if (!options.append && snapshot?.data && options.policy !== "network-only") {
      setPage(snapshot.data);
      setState(snapshot.data.items.length ? "ready" : "empty");
      setError(null);
      setCacheStatus({
        source: "cache",
        fetchedAt: snapshot.fetchedAt,
      });
    }

    const shouldFetch = options.append
      ? true
      : shouldFetchNetwork(options.policy, snapshot);
    if (!shouldFetch) {
      return;
    }

    if (options.append) {
      setIsLoadingMore(true);
    } else if (snapshot?.data) {
      setIsRefreshing(true);
    } else {
      setState("loading");
      setError(null);
    }

    const currentPage = pageRef.current;
    const requestKey = options.append
      ? `${cacheKey}:append:${currentPage?.nextCursor || "none"}:${String(currentPage?.nextOffset ?? "none")}`
      : `${cacheKey}:first`;

    let requestPromise = inFlightRef.current[requestKey];
    if (!requestPromise) {
      requestPromise = options.append
        ? fetchNextPageForConfirmedSearch(
            activeSearch,
            currentPage?.nextCursor ?? null,
            currentPage?.nextOffset ?? currentPage?.items.length ?? null,
          )
        : fetchFirstPageForConfirmedSearch(activeSearch);
      inFlightRef.current[requestKey] = requestPromise;
    }

    try {
      const nextPage = await requestPromise;
      if (requestToken !== requestTokenRef.current) {
        return;
      }

      const resolvedPage = options.append && currentPage
        ? mergeSearchPages(currentPage, nextPage)
        : nextPage;

      setPage(resolvedPage);
      pageRef.current = resolvedPage;
      setState(resolvedPage.items.length ? "ready" : "empty");
      setError(null);
      setCacheStatus({
        source: "network",
        fetchedAt: Date.now(),
      });
      setBackendSearchUnavailable(false);

      writeBrowserCache(
        cacheKey,
        resolvedPage,
        SEARCH_CACHE_TTL_MS,
        {
          source: "network",
          fetchedAt: Date.now(),
        },
      );
    } catch (loadError) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }

      const message = getErrorMessage(loadError, "Unable to load estimate numbers.");
      if (message === "MIN_QUERY_LENGTH") {
        setState("min-query");
        setError(null);
      } else if ((activeSearch.mode === "text" || activeSearch.mode === "today") && isSearchEndpointMissing(loadError)) {
        setBackendSearchUnavailable(true);
        setState("error");
        setError(
          "Backend EstNum search endpoint is unavailable. Exact EstNum search works now. Proposed API: GET /api/tradsphere/v1/estNums/search?q=<query>&limit=50.",
        );
      } else {
        setState("error");
        setError(message);
      }
    } finally {
      if (inFlightRef.current[requestKey] === requestPromise) {
        delete inFlightRef.current[requestKey];
      }
      if (requestToken === requestTokenRef.current) {
        setIsRefreshing(false);
        setIsLoadingMore(false);
      }
    }
  }

  useEffect(() => {
    void loadPageData({
      policy: "stale-while-revalidate",
      append: false,
    });
  }, [confirmedSearch, submissionVersion]);

  function handleDraftChange(nextValue: string) {
    setInputDraft(nextValue);
    if (pendingInterpretation) {
      setPendingInterpretation(null);
    }
  }

  function handleSubmitSearch() {
    const nextQuery = inputDraft.trim();
    const options = buildSearchInterpretationOptions(nextQuery);
    if (options.length > 0) {
      setPendingInterpretation({
        raw: nextQuery,
        options,
      });
      return;
    }

    setPendingInterpretation(null);
    setConfirmedSearch(toConfirmedSearchParams(nextQuery));
    setSubmissionVersion((current) => current + 1);
  }

  function handleSelectInterpretation(optionId: string) {
    if (!pendingInterpretation) {
      return;
    }
    const selected = pendingInterpretation.options.find((option) => option.id === optionId);
    if (!selected) {
      return;
    }
    setPendingInterpretation(null);
    setConfirmedSearch(selected.params);
    setSubmissionVersion((current) => current + 1);
  }

  function handleDismissInterpretation() {
    setPendingInterpretation(null);
  }

  function handleRefreshSearch() {
    void loadPageData({
      policy: "network-only",
      append: false,
    });
  }

  function handleLoadMore() {
    if (!canLoadMore || isLoadingMore) {
      return;
    }
    void loadPageData({
      policy: "network-only",
      append: true,
    });
  }

  function handleOpenSchedule(item: EstimateSearchItem) {
    if (!item.hasSchedule) {
      return;
    }
    const accountInfo = accountDirectoryByCode[item.accountCode.toUpperCase()];
    setSelectedScheduleEstnum({
      estnum: item.estNum,
      name: String(item.estNum),
      hasSchedule: true,
      note: item.note ?? undefined,
    });
    setSelectedScheduleAccountCode(item.accountCode.toUpperCase());
    setSelectedScheduleBillingType(accountInfo?.billingType ?? null);
    setIsScheduleModalOpen(true);
  }

  function handleOpenEdit(item: EstimateSearchItem) {
    setEstimateModalMode("edit");
    setEstimateModalAccountCode(item.accountCode.toUpperCase());
    setEstimateModalInitialData({
      estNum: item.estNum,
      accountCode: item.accountCode.toUpperCase(),
      note: item.note ?? "",
    });
    setIsEstimateModalOpen(true);
  }

  function handleOpenCreate() {
    if (!createAccountCode) {
      toast.error("Account required", "Select an account before adding an estimate number.");
      return;
    }
    setEstimateModalMode("create");
    setEstimateModalAccountCode(createAccountCode);
    setEstimateModalInitialData(null);
    setIsEstimateModalOpen(true);
  }

  async function handleEstimateSaved(result: EstimateNumberModalSaveResult): Promise<void> {
    removeBrowserCacheByPrefix(`schedule-table:${result.accountCode.toUpperCase()}:${result.estNum}:`);
    removeBrowserCacheByPrefix("estnums:");
    void loadPageData({
      policy: "network-only",
      append: false,
    });
  }

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-5 pb-12">
      <PageBanner
        eyebrow="TradSphere"
        title="Estimate Numbers"
        description="Search estimate numbers and open schedules."
        gradientVariant="app"
        action={
          <div className="flex flex-col items-start gap-2 sm:flex-row sm:items-center">
            <div className="w-[280px] max-w-full">
              <AppDropdown
                value={createAccountCode}
                options={createAccountOptions}
                onValueChange={setCreateAccountCode}
                ariaLabel="Select account for new estimate"
                placeholder="Select account"
                size="sm"
                emptyText="No accounts found."
              />
            </div>
            <Button onClick={handleOpenCreate} disabled={!createAccountCode}>
              Add Estimate
            </Button>
          </div>
        }
      />

      <EstimateNumberSearch
        value={inputDraft}
        onChange={handleDraftChange}
        onSubmit={handleSubmitSearch}
        onRefresh={handleRefreshSearch}
        searching={state === "loading"}
        refreshing={isRefreshing}
        disabled={isLoadingMore}
        resultText={resultText}
        interpretationOptions={interpretationOptions}
        onSelectInterpretation={handleSelectInterpretation}
        onDismissInterpretation={handleDismissInterpretation}
      />

      {backendSearchUnavailable && (confirmedSearch.mode === "text" || confirmedSearch.mode === "today") ? (
        <p className="flex items-center gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          <AlertCircle className="size-4" />
          Text search depends on backend endpoint `/api/tradsphere/v1/estNums/search`.
        </p>
      ) : null}

      <EstimateNumberResults
        state={state}
        groups={groupedResults}
        error={error}
        minQueryLength={SEARCH_MIN_TEXT_LENGTH}
        loadMoreVisible={canLoadMore}
        loadMoreLoading={isLoadingMore}
        onLoadMore={handleLoadMore}
        onViewSchedule={handleOpenSchedule}
        onEditEstimate={handleOpenEdit}
      />

      {!isAnyModalOpen ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40">
          <div className={`mx-4 sm:mx-6 lg:mr-6 ${sidebarCollapsed ? "lg:ml-[6.5rem]" : "lg:ml-[18.75rem]"}`}>
            <div className="mx-auto w-full max-w-[1600px]">
              <CacheStatusChip
                text={cacheStatusText}
                onRefresh={handleRefreshSearch}
                disabled={isRefreshing || isLoadingMore}
                refreshing={isRefreshing}
                refreshLabel="Refresh estimate numbers"
                tooltipText="Click to refresh estimate numbers"
                containerClassName="pointer-events-auto"
                className="max-w-[min(90vw,34rem)]"
              />
            </div>
          </div>
        </div>
      ) : null}

      <EstimateNumberModal
        open={isEstimateModalOpen}
        onOpenChange={setIsEstimateModalOpen}
        mode={estimateModalMode}
        initialData={estimateModalInitialData}
        accountCode={estimateModalAccountCode}
        accountName={estimateModalAccountName}
        headers={requestHeaders}
        onSuccess={handleEstimateSaved}
      />

      <ScheduleModal
        open={isScheduleModalOpen}
        onOpenChange={setIsScheduleModalOpen}
        selectedEstnum={selectedScheduleEstnum}
        accountCode={selectedScheduleAccountCode}
        billingType={selectedScheduleBillingType}
        headers={requestHeaders}
      />
    </div>
  );
}
