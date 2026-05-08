import { useEffect, useMemo, useRef, useState } from "react";
import { AlertCircle, Loader2 } from "lucide-react";

import { EstimateNumberResults } from "@/components/estnums/EstimateNumberResults";
import {
  EstimateNumberSearch,
  type EstimateNumberSearchFormValues,
} from "@/components/estnums/EstimateNumberSearch";
import type { EstimateAccountGroup, EstimateSearchItem, EstimateSearchPage } from "@/components/estnums/types";
import {
  EstimateNumberModal,
  type EstimateNumberModalData,
  type EstimateNumberModalMode,
  type EstimateNumberModalSaveResult,
} from "@/components/dashboard/EstimateNumberModal";
import { ScheduleModal } from "@/components/dashboard/ScheduleModal";
import type { AccountSelection, EsnumItem } from "@/components/dashboard/types";
import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import { useToast } from "@/components/ui/toast";
import { useApiRequest } from "@/hooks/useApiRequest";
import { usePersistentState } from "@/hooks/usePersistentState";
import {
  listBrowserCacheSnapshotsByPrefix,
  readBrowserCacheSnapshot,
  removeBrowserCacheByPrefix,
  writeBrowserCache,
} from "@/lib/browserCache";
import {
  TRADSPHERE_CACHE_TTL_MS,
  shouldFetchNetwork,
  type CachePolicy,
} from "@shared/cache";
import { hasAtLeastOneSearchCriterion, shouldFetchSubmittedSearchNetwork } from "@shared/search";

const SEARCH_LIMIT = 50;
const SEARCH_CACHE_TTL_MS = 5 * 60 * 1000;
const SEARCH_TIMEZONE = "America/Chicago";
const ESTNUMS_PAGE_CACHE_VERSION = "v4";
const ESTNUMS_SEARCH_CACHE_COLLECTION_PREFIX = "estnums:form-search:";
const ESTNUMS_SEARCH_CACHE_COLLECTION_LIMIT = 40;
const SELECTIONS_CACHE_KEY = "tradsphere:main:selections:v2";
const SELECTIONS_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.SELECTIONS;
const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";
const ESTNUMS_SEARCH_DRAFT_STORAGE_KEY = "tradsphere.estnums.searchDraft.v1";
const ESTNUMS_SUBMITTED_SEARCH_STORAGE_KEY = "tradsphere.estnums.submittedSearch.v1";

type SearchUiState = "idle" | "loading" | "ready" | "empty" | "error";

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

type SearchPlan =
  | {
      type: "list";
      query: string;
    }
  | {
      type: "search";
      query: string;
      includeCreatedToday: boolean;
    };

type SubmittedSearch = {
  params: EstimateNumberSearchFormValues;
  plan: SearchPlan;
  cacheKey: string;
};

type BuildSearchPlanResult =
  | {
      ok: true;
      submitted: SubmittedSearch;
    }
  | {
      ok: false;
      clearResults: boolean;
      message: string;
    };

const INITIAL_SEARCH_FORM: EstimateNumberSearchFormValues = {
  estimateNumber: "",
  account: "",
  buyer: "",
  note: "",
  months: [],
  year: "",
  quarter: "",
  createdToday: false,
};

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

function buildSelectionLabel(code: string, name: string): string {
  if (code && name) {
    return `${code} - ${name}`;
  }
  return code || name;
}

function normalizeSelectionsResponse(payload: unknown): AccountSelection[] {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  const output: AccountSelection[] = [];
  const seen = new Set<string>();

  for (const item of data) {
    if (!isRecord(item)) {
      continue;
    }

    const accountCode = asString(item.code ?? item.accountCode ?? item.value).toUpperCase();
    const name = asString(item.name);
    const label = asString(item.label) || buildSelectionLabel(accountCode, name);

    if (!accountCode || !label || seen.has(accountCode)) {
      continue;
    }

    seen.add(accountCode);
    output.push({ accountCode, label, name: name || undefined });
  }

  return output;
}

function accountDirectoryFromSelections(selections: AccountSelection[]): AccountDirectoryItem[] {
  return selections
    .map((selection) => ({
      accountCode: selection.accountCode.toUpperCase(),
      name: asString(selection.name),
      billingType: null,
    }))
    .sort((a, b) => a.accountCode.localeCompare(b.accountCode));
}

function mergeDirectoryWithBillingTypes(
  baseDirectory: AccountDirectoryItem[],
  billingDirectory: AccountDirectoryItem[],
): AccountDirectoryItem[] {
  const merged = new Map<string, AccountDirectoryItem>();

  for (const item of baseDirectory) {
    const code = item.accountCode.toUpperCase();
    merged.set(code, {
      accountCode: code,
      name: item.name || "",
      billingType: item.billingType ?? null,
    });
  }

  for (const item of billingDirectory) {
    const code = item.accountCode.toUpperCase();
    const existing = merged.get(code);
    if (existing) {
      merged.set(code, {
        accountCode: code,
        name: existing.name || item.name || "",
        billingType: item.billingType ?? existing.billingType ?? null,
      });
      continue;
    }
    merged.set(code, {
      accountCode: code,
      name: item.name || "",
      billingType: item.billingType ?? null,
    });
  }

  return [...merged.values()].sort((a, b) => a.accountCode.localeCompare(b.accountCode));
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

function encodeKeyPart(value: string): string {
  return encodeURIComponent(value.trim().toLowerCase());
}

function buildSearchCacheKey(params: EstimateNumberSearchFormValues): string {
  const normalizedMonths = [...params.months].map((value) => value.trim()).filter(Boolean).sort((a, b) => Number(a) - Number(b));
  return [
    "estnums:form-search",
    `estnum=${encodeKeyPart(params.estimateNumber)}`,
    `account=${encodeKeyPart(params.account)}`,
    `buyer=${encodeKeyPart(params.buyer)}`,
    `note=${encodeKeyPart(params.note)}`,
    `months=${encodeKeyPart(normalizedMonths.join(","))}`,
    `year=${encodeKeyPart(params.year)}`,
    `quarter=${encodeKeyPart(params.quarter)}`,
    `today=${params.createdToday ? "1" : "0"}`,
    ESTNUMS_PAGE_CACHE_VERSION,
  ].join(":");
}

function normalizeSearchText(value: string | null | undefined): string {
  return (value ?? "").trim().toLowerCase();
}

function isEstimateItemMatchTextQuery(item: EstimateSearchItem, query: string): boolean {
  const normalizedQuery = normalizeSearchText(query);
  if (!normalizedQuery) {
    return true;
  }
  const candidates = [
    String(item.estNum),
    item.accountCode,
    item.accountName ?? "",
    item.buyer ?? "",
    item.note ?? "",
    item.mediaType ?? "",
  ].map((value) => normalizeSearchText(value));
  return candidates.some((value) => value.includes(normalizedQuery));
}

function isEstimateItemMatchListQuery(item: EstimateSearchItem, query: string): boolean {
  if (!query) {
    return true;
  }
  const params = new URLSearchParams(query);

  const estNumParam = params.get("estNum");
  if (estNumParam) {
    const estNum = Number(estNumParam);
    if (!Number.isFinite(estNum) || item.estNum !== Math.trunc(estNum)) {
      return false;
    }
  }

  const accountCodeParam = asString(params.get("accountCode")).toUpperCase();
  if (accountCodeParam && item.accountCode.toUpperCase() !== accountCodeParam) {
    return false;
  }

  const yearParam = params.get("year");
  if (yearParam) {
    const year = Number(yearParam);
    if (!Number.isFinite(year) || (item.year ?? null) !== Math.trunc(year)) {
      return false;
    }
  }

  const quarterParam = params.get("quarter");
  if (quarterParam) {
    const quarter = Number(quarterParam);
    if (!Number.isFinite(quarter) || (item.quarter ?? null) !== Math.trunc(quarter)) {
      return false;
    }
  }

  const monthParam = params.get("month");
  if (monthParam) {
    const month = Number(monthParam);
    if (!Number.isFinite(month) || (item.month ?? null) !== Math.trunc(month)) {
      return false;
    }
  }

  return true;
}

function isEstimateItemMatchSubmittedSearch(item: EstimateSearchItem, submitted: SubmittedSearch): boolean {
  if (submitted.plan.type === "list") {
    return isEstimateItemMatchListQuery(item, submitted.plan.query);
  }
  const params = new URLSearchParams(submitted.plan.query);
  const query = asString(params.get("q"));
  return isEstimateItemMatchTextQuery(item, query);
}

function toEstimateSearchItemFromSaveResult(
  result: EstimateNumberModalSaveResult,
  accountDirectoryByCode: Record<string, AccountDirectoryItem>,
): EstimateSearchItem {
  const accountCode = result.accountCode.trim().toUpperCase();
  const accountName = accountDirectoryByCode[accountCode]?.name || null;
  return withDerivedPeriod({
    estNum: result.estNum,
    accountCode,
    accountName,
    buyer: result.buyer || null,
    mediaType: result.mediaType || null,
    note: result.note || null,
    hasSchedule: false,
    flightStart: result.flightStart || null,
    flightEnd: result.flightEnd || null,
    year: null,
    quarter: null,
    month: null,
    broadcastYears: [],
    broadcastMonths: [],
  });
}

function applySavedEstimateToPage(
  currentPage: EstimateSearchPage | null,
  submitted: SubmittedSearch,
  result: EstimateNumberModalSaveResult,
  accountDirectoryByCode: Record<string, AccountDirectoryItem>,
): EstimateSearchPage | null {
  const savedItem = toEstimateSearchItemFromSaveResult(result, accountDirectoryByCode);
  const shouldInclude = isEstimateItemMatchSubmittedSearch(savedItem, submitted);
  const basePage = currentPage ?? {
    items: [],
    total: 0,
    limit: SEARCH_LIMIT,
    nextCursor: null,
    nextOffset: null,
    backendMode: submitted.plan.type === "search" ? "search" : ("legacy-exact" as const),
  };

  const currentItems = [...basePage.items];
  const existingIndex = currentItems.findIndex(
    (item) =>
      item.estNum === result.estNum &&
      item.accountCode.toUpperCase() === result.accountCode.trim().toUpperCase(),
  );
  const existingItem = existingIndex >= 0 ? currentItems[existingIndex] : null;

  let nextItems = currentItems;
  let nextTotal = basePage.total;

  if (existingIndex >= 0) {
    if (shouldInclude) {
      const mergedItem: EstimateSearchItem = {
        ...existingItem,
        ...savedItem,
        hasSchedule: existingItem?.hasSchedule ?? false,
      };
      nextItems[existingIndex] = mergedItem;
    } else {
      nextItems.splice(existingIndex, 1);
      if (nextTotal !== null) {
        nextTotal = Math.max(0, nextTotal - 1);
      }
    }
  } else if (shouldInclude) {
    nextItems = [...nextItems, savedItem];
    if (nextTotal !== null) {
      nextTotal += 1;
    }
  }

  return {
    ...basePage,
    items: sortItems(nextItems),
    total: nextTotal,
  };
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

function parseYear(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  if (!/^\d{4}$/.test(trimmed)) {
    return null;
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 1901 || parsed > 2155) {
    return null;
  }
  return Math.trunc(parsed);
}

function parseQuarter(value: string): number | null {
  const trimmed = value.trim().toUpperCase();
  if (!trimmed) {
    return null;
  }
  const matched = trimmed.match(/^Q?([1-4])$/);
  if (!matched) {
    return null;
  }
  const parsed = Number(matched[1]);
  return Number.isFinite(parsed) ? parsed : null;
}

function looksLikeAccountCode(value: string): boolean {
  return /^[A-Za-z0-9_-]+$/.test(value.trim());
}

function normalizeDraft(draft: EstimateNumberSearchFormValues): EstimateNumberSearchFormValues {
  return {
    estimateNumber: draft.estimateNumber.trim(),
    account: draft.account.trim(),
    buyer: draft.buyer.trim(),
    note: draft.note.trim(),
    months: [...draft.months].map((value) => value.trim()).filter((value) => /^\d{1,2}$/.test(value)),
    year: draft.year.trim(),
    quarter: draft.quarter.trim().toUpperCase(),
    createdToday: Boolean(draft.createdToday),
  };
}

function isEstimateNumberSearchFormValues(value: unknown): value is EstimateNumberSearchFormValues {
  if (!isRecord(value)) {
    return false;
  }
  const months = value.months;
  return (
    typeof value.estimateNumber === "string" &&
    typeof value.account === "string" &&
    typeof value.buyer === "string" &&
    typeof value.note === "string" &&
    Array.isArray(months) &&
    months.every((item) => typeof item === "string") &&
    typeof value.year === "string" &&
    typeof value.quarter === "string" &&
    typeof value.createdToday === "boolean"
  );
}

function isSearchPlan(value: unknown): value is SearchPlan {
  if (!isRecord(value) || typeof value.type !== "string" || typeof value.query !== "string") {
    return false;
  }
  if (value.type === "list") {
    return true;
  }
  if (value.type === "search") {
    return typeof value.includeCreatedToday === "boolean";
  }
  return false;
}

function isSubmittedSearch(value: unknown): value is SubmittedSearch {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.cacheKey === "string" &&
    isEstimateNumberSearchFormValues(value.params) &&
    isSearchPlan(value.plan)
  );
}

function isEstimateSearchItem(value: unknown): value is EstimateSearchItem {
  if (!isRecord(value)) {
    return false;
  }
  return typeof value.estNum === "number" && Number.isFinite(value.estNum) && typeof value.accountCode === "string";
}

function isEstimateSearchPage(value: unknown): value is EstimateSearchPage {
  if (!isRecord(value) || !Array.isArray(value.items)) {
    return false;
  }
  return value.items.every((item) => isEstimateSearchItem(item));
}

function buildLocalCachedSearchPage(submitted: SubmittedSearch): {
  page: EstimateSearchPage | null;
  fetchedAt: number | null;
  isPartial: boolean;
} {
  const exactSnapshot = readBrowserCacheSnapshot<EstimateSearchPage>(submitted.cacheKey);
  const exactPage = isEstimateSearchPage(exactSnapshot?.data) ? exactSnapshot?.data : null;
  const matchedItems: EstimateSearchItem[] = [];
  const seen = new Set<string>();
  let newestFetchedAt = exactSnapshot?.fetchedAt ?? 0;

  const appendItem = (item: EstimateSearchItem) => {
    const dedupeKey = `${item.accountCode.toUpperCase()}:${item.estNum}`;
    if (seen.has(dedupeKey)) {
      return;
    }
    seen.add(dedupeKey);
    matchedItems.push(withDerivedPeriod(item));
  };

  if (exactPage?.items?.length) {
    for (const item of exactPage.items) {
      appendItem(item);
    }
  }

  const cacheEntries = listBrowserCacheSnapshotsByPrefix<EstimateSearchPage>(
    ESTNUMS_SEARCH_CACHE_COLLECTION_PREFIX,
    {
      allowExpired: true,
      limit: ESTNUMS_SEARCH_CACHE_COLLECTION_LIMIT,
    },
  );

  for (const entry of cacheEntries) {
    newestFetchedAt = Math.max(newestFetchedAt, entry.snapshot.fetchedAt);
    const cachedPage = isEstimateSearchPage(entry.snapshot.data) ? entry.snapshot.data : null;
    if (!cachedPage?.items?.length) {
      continue;
    }
    for (const item of cachedPage.items) {
      if (!isEstimateItemMatchSubmittedSearch(item, submitted)) {
        continue;
      }
      appendItem(item);
    }
  }

  if (!matchedItems.length && exactPage) {
    return {
      page: exactPage,
      fetchedAt: exactSnapshot?.fetchedAt ?? Date.now(),
      isPartial: false,
    };
  }

  if (!matchedItems.length) {
    return {
      page: null,
      fetchedAt: null,
      isPartial: false,
    };
  }

  const nextPage: EstimateSearchPage = {
    items: sortItems(matchedItems),
    total: exactPage?.total ?? matchedItems.length,
    limit: exactPage?.limit ?? SEARCH_LIMIT,
    nextCursor: exactPage?.nextCursor ?? null,
    nextOffset: exactPage?.nextOffset ?? null,
    backendMode: exactPage?.backendMode ?? (submitted.plan.type === "search" ? "search" : "legacy-exact"),
  };

  return {
    page: nextPage,
    fetchedAt: newestFetchedAt || Date.now(),
    isPartial: !exactPage,
  };
}

function resolveInitialEstimateSearchView(submittedSearch: SubmittedSearch | null): {
  state: SearchUiState;
  page: EstimateSearchPage | null;
  cacheStatus: CacheStatus | null;
} {
  if (!submittedSearch) {
    return {
      state: "idle",
      page: null,
      cacheStatus: null,
    };
  }

  const localResult = buildLocalCachedSearchPage(submittedSearch);
  if (!localResult.page) {
    return {
      state: "idle",
      page: null,
      cacheStatus: null,
    };
  }

  return {
    state: localResult.page.items.length ? "ready" : "empty",
    page: localResult.page,
    cacheStatus: {
      source: "cache",
      fetchedAt: localResult.fetchedAt ?? Date.now(),
    },
  };
}

function toQuarterLabel(quarter: number): string {
  return `Q${quarter}`;
}

function getTodayIsoInTimezone(timeZone: string): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const year = parts.find((part) => part.type === "year")?.value ?? "";
  const month = parts.find((part) => part.type === "month")?.value ?? "";
  const day = parts.find((part) => part.type === "day")?.value ?? "";
  if (year && month && day) {
    return `${year}-${month}-${day}`;
  }
  return new Date().toISOString().slice(0, 10);
}

function buildSearchPlan(draft: EstimateNumberSearchFormValues): BuildSearchPlanResult {
  const normalized = normalizeDraft(draft);
  const selectedMonths = Array.from(
    new Set(
      normalized.months
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value) && value >= 1 && value <= 12)
        .map((value) => Math.trunc(value)),
    ),
  ).sort((a, b) => a - b);

  const hasAnyInput =
    Boolean(normalized.estimateNumber) ||
    Boolean(normalized.account) ||
    Boolean(normalized.buyer) ||
    Boolean(normalized.note) ||
    selectedMonths.length > 0 ||
    Boolean(normalized.year) ||
    Boolean(normalized.quarter) ||
    normalized.createdToday;

  if (!hasAnyInput) {
    return {
      ok: false,
      clearResults: true,
      message: "Enter at least one field or enable Created today.",
    };
  }

  const yearValue = normalized.year ? parseYear(normalized.year) : null;
  if (normalized.year && yearValue === null) {
    return {
      ok: false,
      clearResults: false,
      message: "Year must be a 4-digit value between 1901 and 2155.",
    };
  }

  const quarterValue = normalized.quarter ? parseQuarter(normalized.quarter) : null;
  if (normalized.quarter && quarterValue === null) {
    return {
      ok: false,
      clearResults: false,
      message: "Quarter must be Q1, Q2, Q3, or Q4.",
    };
  }
  if (quarterValue !== null && yearValue === null) {
    return {
      ok: false,
      clearResults: false,
      message: "Quarter search requires Year.",
    };
  }
  if (selectedMonths.length > 0 && yearValue === null) {
    return {
      ok: false,
      clearResults: false,
      message: "Months search requires Year.",
    };
  }
  if (quarterValue !== null && selectedMonths.length > 0) {
    for (const monthValue of selectedMonths) {
      const derivedQuarter = Math.floor((monthValue - 1) / 3) + 1;
      if (derivedQuarter !== quarterValue) {
        return {
          ok: false,
          clearResults: false,
          message: `Month ${monthValue} does not belong to ${toQuarterLabel(quarterValue)}.`,
        };
      }
    }
  }

  const estimateExact = /^\d+$/.test(normalized.estimateNumber);
  const accountExact = looksLikeAccountCode(normalized.account);

  const periodUsed = yearValue !== null || quarterValue !== null || selectedMonths.length > 0;

  const textFields: string[] = [];
  if (normalized.estimateNumber && !estimateExact) {
    textFields.push(normalized.estimateNumber);
  }
  if (normalized.account && !accountExact) {
    textFields.push(normalized.account);
  }
  if (normalized.buyer) {
    textFields.push(normalized.buyer);
  }
  if (normalized.note) {
    textFields.push(normalized.note);
  }

  if (periodUsed) {
    if (normalized.createdToday) {
      return {
        ok: false,
        clearResults: false,
        message:
          "Created today cannot be combined with Year, Quarter, or structured Month using the current backend search API.",
      };
    }

    if (textFields.length > 0) {
      return {
        ok: false,
        clearResults: false,
        message:
          "This combination needs fielded backend search (period + text filters). Current APIs cannot apply them together in one efficient query.",
      };
    }
    if (selectedMonths.length > 1) {
      return {
        ok: false,
        clearResults: false,
        message:
          "Current backend list API supports one month per request. Multiple month selection needs a backend multi-month search endpoint.",
      };
    }

    if (normalized.estimateNumber && !estimateExact) {
      return {
        ok: false,
        clearResults: false,
        message: "Estimate Number must be numeric when used with period filters.",
      };
    }

    if (normalized.account && !accountExact) {
      return {
        ok: false,
        clearResults: false,
        message: "Account must be an account code when used with period filters.",
      };
    }

    const params = new URLSearchParams();
    if (estimateExact && normalized.estimateNumber) {
      params.set("estNum", normalized.estimateNumber);
    }
    if (normalized.account) {
      params.set("accountCode", normalized.account.toUpperCase());
    }
    if (yearValue !== null) {
      params.set("year", String(yearValue));
    }
    if (selectedMonths.length === 1) {
      params.set("month", String(selectedMonths[0]));
    }
    if (quarterValue !== null) {
      params.set("quarter", String(quarterValue));
    }

    return {
      ok: true,
      submitted: {
        params: normalized,
        cacheKey: buildSearchCacheKey(normalized),
        plan: {
          type: "list",
          query: params.toString(),
        },
      },
    };
  }

  if (normalized.createdToday) {
    const todayTokens: string[] = [];
    if (normalized.estimateNumber) {
      todayTokens.push(normalized.estimateNumber);
    }
    if (normalized.account) {
      todayTokens.push(normalized.account);
    }
    if (normalized.buyer) {
      todayTokens.push(normalized.buyer);
    }
    if (normalized.note) {
      todayTokens.push(normalized.note);
    }

    if (todayTokens.length > 1) {
      return {
        ok: false,
        clearResults: false,
        message:
          "Current backend search accepts a single text query with Created today. Multiple text fields need a fielded search endpoint.",
      };
    }

    const params = new URLSearchParams();
    if (todayTokens[0]) {
      params.set("q", todayTokens[0]);
    }
    params.set("limit", String(SEARCH_LIMIT));
    params.set("timezone", SEARCH_TIMEZONE);

    return {
      ok: true,
      submitted: {
        params: normalized,
        cacheKey: buildSearchCacheKey(normalized),
        plan: {
          type: "search",
          query: params.toString(),
          includeCreatedToday: true,
        },
      },
    };
  }

  const hasFuzzyText = Boolean(normalized.buyer || normalized.note);

  if (hasFuzzyText) {
    const fuzzyTokens: string[] = [];
    if (normalized.estimateNumber) {
      fuzzyTokens.push(normalized.estimateNumber);
    }
    if (normalized.account) {
      fuzzyTokens.push(normalized.account);
    }
    if (normalized.buyer) {
      fuzzyTokens.push(normalized.buyer);
    }
    if (normalized.note) {
      fuzzyTokens.push(normalized.note);
    }

    if (fuzzyTokens.length > 1) {
      return {
        ok: false,
        clearResults: false,
        message:
          "Current backend search accepts a single text query. Multiple text fields need a fielded search endpoint.",
      };
    }

    const params = new URLSearchParams();
    params.set("q", fuzzyTokens[0] || "");
    params.set("limit", String(SEARCH_LIMIT));

    return {
      ok: true,
      submitted: {
        params: normalized,
        cacheKey: buildSearchCacheKey(normalized),
        plan: {
          type: "search",
          query: params.toString(),
          includeCreatedToday: false,
        },
      },
    };
  }

  if ((normalized.estimateNumber && !estimateExact) || (normalized.account && !accountExact)) {
    const singleText = normalized.estimateNumber || normalized.account;
    const bothFilled = Boolean(normalized.estimateNumber && normalized.account);
    if (bothFilled) {
      return {
        ok: false,
        clearResults: false,
        message:
          "Current backend search accepts one text query at a time. Estimate Number + Account text combination needs a fielded search endpoint.",
      };
    }
    const params = new URLSearchParams();
    params.set("q", singleText);
    params.set("limit", String(SEARCH_LIMIT));

    return {
      ok: true,
      submitted: {
        params: normalized,
        cacheKey: buildSearchCacheKey(normalized),
        plan: {
          type: "search",
          query: params.toString(),
          includeCreatedToday: false,
        },
      },
    };
  }

  const listParams = new URLSearchParams();
  if (normalized.estimateNumber) {
    listParams.set("estNum", normalized.estimateNumber);
  }
  if (normalized.account) {
    listParams.set("accountCode", normalized.account.toUpperCase());
  }

  return {
    ok: true,
    submitted: {
      params: normalized,
      cacheKey: buildSearchCacheKey(normalized),
      plan: {
        type: "list",
        query: listParams.toString(),
      },
    },
  };
}

export default function EstimateNumbersPage() {
  const toast = useToast();
  const { requestJson } = useApiRequest();
  const requestHeaders = useMemo(() => buildAuthHeaders(false), []);

  const [accountDirectory, setAccountDirectory] = useState<AccountDirectoryItem[]>([]);
  const [isLoadingAccountSelections, setIsLoadingAccountSelections] = useState(true);
  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());

  const [draft, setDraft] = usePersistentState<EstimateNumberSearchFormValues>(
    ESTNUMS_SEARCH_DRAFT_STORAGE_KEY,
    INITIAL_SEARCH_FORM,
    { storage: "session", validate: isEstimateNumberSearchFormValues },
  );
  const [submittedSearch, setSubmittedSearch] = usePersistentState<SubmittedSearch | null>(
    ESTNUMS_SUBMITTED_SEARCH_STORAGE_KEY,
    null,
    { storage: "session", validate: (value): value is SubmittedSearch | null => value === null || isSubmittedSearch(value) },
  );
  const [submissionVersion, setSubmissionVersion] = useState(0);
  const [searchMessage, setSearchMessage] = useState<string | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);

  const [state, setState] = useState<SearchUiState>(() => resolveInitialEstimateSearchView(submittedSearch).state);
  const [error, setError] = useState<string | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [page, setPage] = useState<EstimateSearchPage | null>(() => resolveInitialEstimateSearchView(submittedSearch).page);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(() => resolveInitialEstimateSearchView(submittedSearch).cacheStatus);
  const [backendSearchUnavailable, setBackendSearchUnavailable] = useState(false);

  const [isScheduleModalOpen, setIsScheduleModalOpen] = useState(false);
  const [selectedScheduleEstnum, setSelectedScheduleEstnum] = useState<EsnumItem | null>(null);
  const [selectedScheduleAccountCode, setSelectedScheduleAccountCode] = useState("");
  const [selectedScheduleBillingType, setSelectedScheduleBillingType] = useState<string | null>(null);

  const [isEstimateModalOpen, setIsEstimateModalOpen] = useState(false);
  const [estimateModalMode, setEstimateModalMode] = useState<EstimateNumberModalMode>("create");
  const [estimateModalInitialData, setEstimateModalInitialData] = useState<EstimateNumberModalData | null>(null);
  const [estimateModalAccountCode, setEstimateModalAccountCode] = useState("");

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
  const normalizedDraft = useMemo(() => normalizeDraft(draft), [draft]);
  const canClearDraft = useMemo(() => hasAtLeastOneSearchCriterion(normalizedDraft), [normalizedDraft]);
  const draftSearchPlan = useMemo(() => buildSearchPlan(draft), [draft]);
  const canSubmitSearch = draftSearchPlan.ok;

  const estimateModalAccountOptions = useMemo(
    () =>
      accountDirectory.map((item) => ({
        value: item.accountCode,
        label: item.name ? `${item.accountCode} - ${item.name}` : item.accountCode,
      })),
    [accountDirectory],
  );

  const canLoadMore = Boolean(
    submittedSearch?.plan.type === "search" &&
      displayPage &&
      (displayPage.nextCursor ||
        (displayPage.total !== null && displayPage.items.length < displayPage.total) ||
        (displayPage.nextOffset !== null && displayPage.nextOffset > displayPage.items.length)),
  );

  const isAnyModalOpen = isScheduleModalOpen || isEstimateModalOpen;
  const cacheStatusText = isRefreshing
    ? "Refreshing..."
    : cacheStatus
      ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : "No cached data yet";

  const showCacheChip = Boolean(
    submittedSearch && !isAnyModalOpen && state === "ready" && (displayPage?.items.length ?? 0) > 0,
  );
  const isPageBusy = isLoadingAccountSelections || state === "loading" || isLoadingMore;
  const pageBusyMessage = isLoadingAccountSelections
    ? "Loading account selections..."
    : isLoadingMore
      ? "Loading more estimate numbers..."
      : "Searching estimate numbers...";

  useEffect(() => {
    pageRef.current = page;
  }, [page]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleStorage = () => {
      const collapsed = readSidebarCollapsedState();
      setSidebarVisuallyExpanded(!collapsed);
    };
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
      const collapsed = readSidebarCollapsedState();
      setSidebarVisuallyExpanded(!collapsed);
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
      let selectionBackedDirectory: AccountDirectoryItem[] = [];
      if (!cancelled) {
        setIsLoadingAccountSelections(true);
      }
      try {
        const selectionCacheSnapshot = readBrowserCacheSnapshot<AccountSelection[]>(SELECTIONS_CACHE_KEY);
        const cachedSelections = normalizeSelectionsResponse(selectionCacheSnapshot?.data);
        const hasCachedSelections = cachedSelections.length > 0;
        if (hasCachedSelections) {
          selectionBackedDirectory = accountDirectoryFromSelections(cachedSelections);
          if (!cancelled) {
            setAccountDirectory(selectionBackedDirectory);
          }
        }

        const shouldFetchSelectionsFromNetwork = shouldFetchNetwork(
          "stale-while-revalidate",
          selectionCacheSnapshot,
        );
        if (shouldFetchSelectionsFromNetwork || !hasCachedSelections) {
          const selectionPayload = await requestJson("/api/tradsphere/v1/ui/main/selections", {
            headers: requestHeaders,
            errorToast: false,
          });
          const selections = normalizeSelectionsResponse(selectionPayload);
          writeBrowserCache(SELECTIONS_CACHE_KEY, selections, SELECTIONS_CACHE_TTL_MS, {
            source: "network",
            fetchedAt: Date.now(),
          });
          selectionBackedDirectory = accountDirectoryFromSelections(selections);
          if (!cancelled) {
            setAccountDirectory(selectionBackedDirectory);
          }
        }

        const accountPayload = await requestJson("/api/tradsphere/v1/accounts?active=false", {
          headers: requestHeaders,
          errorToast: false,
        });
        if (cancelled) {
          return;
        }

        const billingDirectory = parseAccountDirectory(accountPayload);
        const merged = mergeDirectoryWithBillingTypes(selectionBackedDirectory, billingDirectory);
        setAccountDirectory(merged);
      } catch (loadError) {
        if (cancelled) {
          return;
        }
        if (!selectionBackedDirectory.length) {
          toast.error("Account load failed", getErrorMessage(loadError, "Unable to load TradSphere accounts."));
        }
      } finally {
        if (!cancelled) {
          setIsLoadingAccountSelections(false);
        }
      }
    }

    void loadAccounts();

    return () => {
      cancelled = true;
    };
  }, [requestHeaders, requestJson, toast]);

  function buildSearchRequestQuery(plan: SearchPlan, options: { append: boolean; cursor: string | null; offset: number | null }): string {
    if (plan.type !== "search") {
      return plan.query;
    }

    const params = new URLSearchParams(plan.query);
    if (options.append) {
      if (options.cursor) {
        params.set("cursor", options.cursor);
      } else if (options.offset !== null && options.offset >= 0) {
        params.set("offset", String(options.offset));
      }
    }

    if (plan.includeCreatedToday) {
      const today = getTodayIsoInTimezone(SEARCH_TIMEZONE);
      params.set("createdFrom", today);
      params.set("createdTo", today);
      params.set("timezone", SEARCH_TIMEZONE);
    }

    return params.toString();
  }

  async function fetchFirstPageForSubmittedSearch(search: SubmittedSearch): Promise<EstimateSearchPage> {
    if (search.plan.type === "list") {
      const endpoint = search.plan.query ? `/api/tradsphere/v1/estNums?${search.plan.query}` : "/api/tradsphere/v1/estNums";
      const payload = await requestJson(endpoint, {
        headers: requestHeaders,
        errorToast: false,
      });
      return parseLegacyEstNumsResponse(payload, accountDirectoryByCode);
    }

    const query = buildSearchRequestQuery(search.plan, {
      append: false,
      cursor: null,
      offset: null,
    });
    const payload = await requestJson(`/api/tradsphere/v1/estNums/search?${query}`, {
      headers: requestHeaders,
      errorToast: false,
    });
    return parseSearchResponse(payload, accountDirectoryByCode);
  }

  async function fetchNextPageForSubmittedSearch(
    search: SubmittedSearch,
    cursor: string | null,
    offset: number | null,
  ): Promise<EstimateSearchPage> {
    if (search.plan.type !== "search") {
      return {
        items: [],
        total: 0,
        limit: SEARCH_LIMIT,
        nextCursor: null,
        nextOffset: null,
        backendMode: "legacy-exact",
      };
    }

    const query = buildSearchRequestQuery(search.plan, {
      append: true,
      cursor,
      offset,
    });
    const payload = await requestJson(`/api/tradsphere/v1/estNums/search?${query}`, {
      headers: requestHeaders,
      errorToast: false,
    });
    return parseSearchResponse(payload, accountDirectoryByCode);
  }

  async function loadPageData(options: SearchLoadOptions): Promise<void> {
    if (!submittedSearch) {
      if (!options.append) {
        setState("idle");
        setError(null);
        setPage(null);
        setCacheStatus(null);
        setRefreshMessage(null);
      }
      return;
    }

    if (options.append && submittedSearch.plan.type !== "search") {
      return;
    }

    const activeSearch = submittedSearch;
    const requestToken = ++requestTokenRef.current;
    const cacheKey = activeSearch.cacheKey;

    const snapshot = !options.append ? readBrowserCacheSnapshot<EstimateSearchPage>(cacheKey) : null;
    const localCacheResult = !options.append ? buildLocalCachedSearchPage(activeSearch) : null;

    if (!options.append && localCacheResult?.page && options.policy !== "network-only") {
      setPage(localCacheResult.page);
      pageRef.current = localCacheResult.page;
      setState(localCacheResult.page.items.length ? "ready" : "empty");
      setError(null);
      setCacheStatus({
        source: "cache",
        fetchedAt: localCacheResult.fetchedAt ?? Date.now(),
      });
      setRefreshMessage(localCacheResult.isPartial ? "Cached matches shown while refreshing." : null);
    }

    const shouldFetch = options.append ? true : shouldFetchSubmittedSearchNetwork(options.policy, snapshot);
    if (!shouldFetch) {
      return;
    }

    if (options.append) {
      setIsLoadingMore(true);
    } else if (localCacheResult?.page) {
      setIsRefreshing(true);
    } else {
      setPage(null);
      pageRef.current = null;
      setState("loading");
      setError(null);
      setRefreshMessage(null);
    }

    const currentPage = pageRef.current;
    const requestKey = options.append
      ? `${cacheKey}:append:${currentPage?.nextCursor || "none"}:${String(currentPage?.nextOffset ?? "none")}`
      : `${cacheKey}:first`;

    let requestPromise = inFlightRef.current[requestKey];
    if (!requestPromise) {
      requestPromise = options.append
        ? fetchNextPageForSubmittedSearch(
            activeSearch,
            currentPage?.nextCursor ?? null,
            currentPage?.nextOffset ?? currentPage?.items.length ?? null,
          )
        : fetchFirstPageForSubmittedSearch(activeSearch);
      inFlightRef.current[requestKey] = requestPromise;
    }

    try {
      const nextPage = await requestPromise;
      if (requestToken !== requestTokenRef.current) {
        return;
      }

      const resolvedPage = options.append && currentPage ? mergeSearchPages(currentPage, nextPage) : nextPage;

      setPage(resolvedPage);
      pageRef.current = resolvedPage;
      setState(resolvedPage.items.length ? "ready" : "empty");
      setError(null);
      setRefreshMessage(null);
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

      const hasVisibleCachedResults = !options.append && (pageRef.current?.items.length ?? 0) > 0;
      const message = getErrorMessage(loadError, "Unable to load estimate numbers.");
      if (activeSearch.plan.type === "search" && isSearchEndpointMissing(loadError)) {
        setBackendSearchUnavailable(true);
        if (hasVisibleCachedResults) {
          setRefreshMessage("Showing cached results. Could not refresh.");
          setState("ready");
          setError(null);
        } else {
          setState("error");
          setError(
            "Backend EstNum search endpoint is unavailable. Proposed backend support: GET /api/tradsphere/v1/estNums/search with fielded params (estnum, account, buyer, note, months, year, quarter, createdToday).",
          );
        }
      } else {
        if (hasVisibleCachedResults) {
          setRefreshMessage("Showing cached results. Could not refresh.");
          setState("ready");
          setError(null);
        } else {
          setState("error");
          setError(message);
        }
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
    if (!submittedSearch) {
      return;
    }
    void loadPageData({
      policy: "stale-while-revalidate",
      append: false,
    });
  }, [submittedSearch, submissionVersion]);

  function handleDraftChange<K extends keyof EstimateNumberSearchFormValues>(
    field: K,
    nextValue: EstimateNumberSearchFormValues[K],
  ) {
    setDraft((current) => ({
      ...current,
      [field]: nextValue,
    }));
    if (searchMessage) {
      setSearchMessage(null);
    }
    if (refreshMessage) {
      setRefreshMessage(null);
    }
  }

  function handleSubmitSearch() {
    const result = draftSearchPlan;
    if (!result.ok) {
      setSearchMessage(result.message);
      if (result.clearResults) {
        setSubmittedSearch(null);
        setPage(null);
        setError(null);
        setCacheStatus(null);
        setRefreshMessage(null);
        setState("idle");
      }
      return;
    }

    setSearchMessage(null);
    setRefreshMessage(null);
    setBackendSearchUnavailable(false);
    const isSameSubmittedSearch = submittedSearch?.cacheKey === result.submitted.cacheKey;
    if (isSameSubmittedSearch) {
      void loadPageData({
        policy: "stale-while-revalidate",
        append: false,
      });
      return;
    }
    setSubmittedSearch(result.submitted);
    setSubmissionVersion((current) => current + 1);
  }

  function handleClearDraft() {
    const draftCacheKey = buildSearchCacheKey(normalizedDraft);
    const shouldClearSubmittedResults = Boolean(submittedSearch && submittedSearch.cacheKey === draftCacheKey);

    setDraft(INITIAL_SEARCH_FORM);
    setSearchMessage(null);
    setRefreshMessage(null);

    if (!shouldClearSubmittedResults) {
      return;
    }

    requestTokenRef.current += 1;
    setSubmittedSearch(null);
    setState("idle");
    setError(null);
    setPage(null);
    pageRef.current = null;
    setCacheStatus(null);
    setIsRefreshing(false);
    setIsLoadingMore(false);
  }

  function handleRefreshSearch() {
    if (!submittedSearch) {
      return;
    }
    void loadPageData({
      policy: "network-only",
      append: false,
    });
  }

  function handleLoadMore() {
    if (!canLoadMore || isLoadingMore || !submittedSearch) {
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
      flightStart: item.flightStart ?? "",
      flightEnd: item.flightEnd ?? "",
      mediaType: item.mediaType ?? "",
      buyer: item.buyer ?? "",
      note: item.note ?? "",
    });
    setIsEstimateModalOpen(true);
  }

  function handleOpenCreate() {
    if (!estimateModalAccountOptions.length) {
      toast.error("Accounts unavailable", "Unable to load TradSphere accounts.");
      return;
    }
    setEstimateModalMode("create");
    setEstimateModalAccountCode("");
    setEstimateModalInitialData(null);
    setIsEstimateModalOpen(true);
  }

  async function handleEstimateSaved(result: EstimateNumberModalSaveResult): Promise<void> {
    removeBrowserCacheByPrefix(`schedule-table:${result.accountCode.toUpperCase()}:${result.estNum}:`);

    if (!submittedSearch) {
      return;
    }
    const nextPage = applySavedEstimateToPage(pageRef.current, submittedSearch, result, accountDirectoryByCode);
    if (!nextPage) {
      return;
    }

    const fetchedAt = Date.now();
    setPage(nextPage);
    pageRef.current = nextPage;
    setState(nextPage.items.length ? "ready" : "empty");
    setError(null);
    setCacheStatus({
      source: "cache",
      fetchedAt,
    });

    writeBrowserCache(submittedSearch.cacheKey, nextPage, SEARCH_CACHE_TTL_MS, {
      source: "cache",
      fetchedAt,
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
          <Button onClick={handleOpenCreate} disabled={isLoadingAccountSelections || !estimateModalAccountOptions.length}>
            Add Estimate
          </Button>
        }
      />

      <EstimateNumberSearch
        value={draft}
        onChange={handleDraftChange}
        onSubmit={handleSubmitSearch}
        onClear={handleClearDraft}
        canClear={canClearDraft}
        searching={state === "loading"}
        disabled={isLoadingMore || isRefreshing || isLoadingAccountSelections}
        resultText={resultText}
        canSubmit={canSubmitSearch}
        message={searchMessage}
      />

      {refreshMessage ? (
        <p className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800">{refreshMessage}</p>
      ) : null}

      {backendSearchUnavailable && submittedSearch?.plan.type === "search" ? (
        <p className="flex items-center gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          <AlertCircle className="size-4" />
          Text search depends on backend endpoint `/api/tradsphere/v1/estNums/search`.
        </p>
      ) : null}

      <EstimateNumberResults
        state={state}
        groups={groupedResults}
        error={error}
        minQueryLength={1}
        loadMoreVisible={canLoadMore}
        loadMoreLoading={isLoadingMore}
        onLoadMore={handleLoadMore}
        onViewSchedule={handleOpenSchedule}
        onEditEstimate={handleOpenEdit}
      />

      {showCacheChip ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40">
          <div className={`mx-4 sm:mx-6 lg:mr-6 ${sidebarVisuallyExpanded ? "lg:ml-[18.75rem]" : "lg:ml-[6.5rem]"}`}>
            <div className="mx-auto w-full max-w-[1600px]">
              <CacheStatusChip
                text={cacheStatusText}
                onRefresh={handleRefreshSearch}
                disabled={isRefreshing || isLoadingMore}
                refreshing={isRefreshing}
                refreshLabel="Refresh estimate numbers"
                tooltipText="Click to refresh last submitted search"
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
        accountName={accountDirectoryByCode[estimateModalAccountCode]?.name || estimateModalAccountCode}
        accountOptions={estimateModalAccountOptions}
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

      {isPageBusy ? (
        <div className="fixed inset-0 z-30 flex items-center justify-center bg-slate-950/25 backdrop-blur-[1.5px]">
          <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft">
            <Loader2 className="size-4 animate-spin text-blue-600" />
            <span>{pageBusyMessage}</span>
          </div>
        </div>
      ) : null}
    </div>
  );
}
