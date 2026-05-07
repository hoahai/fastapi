import { useEffect, useMemo, useRef, useState } from "react";
import { AlertCircle } from "lucide-react";

import { EstimateNumberResults } from "@/components/estnums/EstimateNumberResults";
import { EstimateNumberSearch } from "@/components/estnums/EstimateNumberSearch";
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
import { useToast } from "@/components/ui/toast";
import { useApiRequest } from "@/hooks/useApiRequest";
import { removeBrowserCacheByPrefix } from "@/lib/browserCache";

const SEARCH_LIMIT = 50;
const SEARCH_DEBOUNCE_MS = 300;
const SEARCH_MIN_TEXT_LENGTH = 3;
const SEARCH_CACHE_TTL_MS = 2 * 60 * 1000;

type SearchUiState = "idle" | "min-query" | "loading" | "ready" | "empty" | "error";

type AccountDirectoryItem = {
  accountCode: string;
  name: string;
  billingType?: string | null;
};

type SearchCacheEntry = {
  page: EstimateSearchPage;
  fetchedAt: number;
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

function normalizeQueryKey(query: string): string {
  return query.trim().toLowerCase();
}

function isExactEstNumQuery(query: string): boolean {
  return /^\d+$/.test(query.trim());
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

function buildAuthHeaders(includeJsonContentType: boolean): HeadersInit {
  return {
    "X-API-Key": "6ad13c1f7c17c32fb5a4582b4be42df5",
    "X-Tenant-Id": "taaa",
    "X-User-Name": "Hai Truong",
    ...(includeJsonContentType ? { "Content-Type": "application/json" } : {}),
  };
}

export default function EstimateNumbersPage() {
  const toast = useToast();
  const { requestJson } = useApiRequest();
  const requestHeaders = useMemo(() => buildAuthHeaders(false), []);

  const [accountDirectory, setAccountDirectory] = useState<AccountDirectoryItem[]>([]);
  const [queryInput, setQueryInput] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [state, setState] = useState<SearchUiState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [activeQuery, setActiveQuery] = useState("");
  const [page, setPage] = useState<EstimateSearchPage | null>(null);
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

  const searchCacheRef = useRef<Record<string, SearchCacheEntry>>({});
  const searchRequestSeqRef = useRef(0);

  const accountDirectoryByCode = useMemo(() => {
    return accountDirectory.reduce<Record<string, AccountDirectoryItem>>((map, item) => {
      map[item.accountCode] = item;
      return map;
    }, {});
  }, [accountDirectory]);

  const groupedResults = useMemo(() => buildGroups(page?.items ?? []), [page]);
  const resultText = useMemo(() => formatResultText(page), [page]);
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
    page &&
      (page.nextCursor ||
        (page.total !== null && page.items.length < page.total) ||
        (page.nextOffset !== null && page.nextOffset > page.items.length)),
  );

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      setDebouncedQuery(queryInput);
    }, SEARCH_DEBOUNCE_MS);

    return () => {
      window.clearTimeout(timeout);
    };
  }, [queryInput]);

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

  async function fetchSearchPage(query: string, cursor: string | null, offset: number | null): Promise<EstimateSearchPage> {
    if (isExactEstNumQuery(query)) {
      const payload = await requestJson(`/api/tradsphere/v1/estNums?estNum=${encodeURIComponent(query)}`, {
        headers: requestHeaders,
        errorToast: false,
      });
      return parseLegacyEstNumsResponse(payload, accountDirectoryByCode);
    }

    const params = new URLSearchParams();
    params.set("q", query);
    params.set("limit", String(SEARCH_LIMIT));
    if (cursor) {
      params.set("cursor", cursor);
    }
    if (!cursor && offset !== null && offset >= 0) {
      params.set("offset", String(offset));
    }

    const payload = await requestJson(`/api/tradsphere/v1/estNums/search?${params.toString()}`, {
      headers: requestHeaders,
      errorToast: false,
    });
    return parseSearchResponse(payload, accountDirectoryByCode);
  }

  async function runSearch(options: {
    query: string;
    forceNetwork: boolean;
    append: boolean;
    cursor: string | null;
    offset: number | null;
  }) {
    const normalizedQuery = options.query.trim();
    const queryKey = normalizeQueryKey(normalizedQuery);
    const requestId = ++searchRequestSeqRef.current;
    const now = Date.now();

    const cached = searchCacheRef.current[queryKey] || null;
    const hasFreshCache = cached ? now - cached.fetchedAt <= SEARCH_CACHE_TTL_MS : false;

    if (!options.append && cached) {
      setPage(cached.page);
      setState(cached.page.items.length ? "ready" : "empty");
      setError(null);
      setActiveQuery(normalizedQuery);
    }

    if (!options.forceNetwork && !options.append && cached && hasFreshCache) {
      return;
    }

    if (options.append) {
      setIsLoadingMore(true);
    } else if (cached) {
      setIsRefreshing(true);
    } else {
      setState("loading");
      setError(null);
    }

    try {
      const nextPageRaw = await fetchSearchPage(normalizedQuery, options.cursor, options.offset);
      if (requestId !== searchRequestSeqRef.current) {
        return;
      }

      const resolvedPage = options.append && page ? mergeSearchPages(page, nextPageRaw) : nextPageRaw;
      setPage(resolvedPage);
      setState(resolvedPage.items.length ? "ready" : "empty");
      setError(null);
      setActiveQuery(normalizedQuery);
      setBackendSearchUnavailable(false);
      searchCacheRef.current[queryKey] = {
        page: resolvedPage,
        fetchedAt: Date.now(),
      };
    } catch (searchError) {
      if (requestId !== searchRequestSeqRef.current) {
        return;
      }

      if (!isExactEstNumQuery(normalizedQuery) && isSearchEndpointMissing(searchError)) {
        setBackendSearchUnavailable(true);
        setState("error");
        setError(
          "Backend EstNum search endpoint is unavailable. Exact EstNum search works now. Proposed API: GET /api/tradsphere/v1/estNums/search?q=<query>&limit=50.",
        );
      } else {
        setState("error");
        setError(getErrorMessage(searchError, "Unable to search estimate numbers."));
      }
    } finally {
      if (requestId === searchRequestSeqRef.current) {
        setIsRefreshing(false);
        setIsLoadingMore(false);
      }
    }
  }

  useEffect(() => {
    const query = debouncedQuery.trim();

    if (!query) {
      setState("idle");
      setError(null);
      setPage(null);
      setIsRefreshing(false);
      setIsLoadingMore(false);
      setActiveQuery("");
      return;
    }

    if (!isExactEstNumQuery(query) && query.length < SEARCH_MIN_TEXT_LENGTH) {
      setState("min-query");
      setError(null);
      setPage(null);
      setActiveQuery("");
      return;
    }

    void runSearch({
      query,
      forceNetwork: false,
      append: false,
      cursor: null,
      offset: null,
    });
  }, [debouncedQuery]);

  function handleRefreshSearch() {
    const query = debouncedQuery.trim();
    if (!query) {
      return;
    }

    void runSearch({
      query,
      forceNetwork: true,
      append: false,
      cursor: null,
      offset: null,
    });
  }

  function handleLoadMore() {
    if (!page || !activeQuery) {
      return;
    }

    const nextOffset = page.nextOffset !== null ? page.nextOffset : page.items.length;
    void runSearch({
      query: activeQuery,
      forceNetwork: true,
      append: true,
      cursor: page.nextCursor,
      offset: nextOffset,
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
    searchCacheRef.current = {};

    const query = debouncedQuery.trim();
    if (!query) {
      return;
    }

    await runSearch({
      query,
      forceNetwork: true,
      append: false,
      cursor: null,
      offset: null,
    });
  }

  async function handleCopyEstNum(item: EstimateSearchItem) {
    try {
      await navigator.clipboard.writeText(String(item.estNum));
      toast.success("Copied", `EstNum ${item.estNum} copied.`);
    } catch {
      toast.error("Copy failed", "Unable to copy EstNum to clipboard.");
    }
  }

  const resultsState: SearchUiState = state;

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
        value={queryInput}
        onChange={setQueryInput}
        onRefresh={handleRefreshSearch}
        searching={state === "loading"}
        refreshing={isRefreshing}
        disabled={isLoadingMore}
        resultText={resultText}
      />

      {backendSearchUnavailable && !isExactEstNumQuery(debouncedQuery.trim()) ? (
        <p className="flex items-center gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          <AlertCircle className="size-4" />
          Text search depends on backend endpoint `/api/tradsphere/v1/estNums/search`.
        </p>
      ) : null}

      <EstimateNumberResults
        state={resultsState}
        groups={groupedResults}
        error={error}
        minQueryLength={SEARCH_MIN_TEXT_LENGTH}
        loadMoreVisible={canLoadMore}
        loadMoreLoading={isLoadingMore}
        onLoadMore={handleLoadMore}
        onViewSchedule={handleOpenSchedule}
        onEditEstimate={handleOpenEdit}
        onCopyEstNum={handleCopyEstNum}
      />

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
