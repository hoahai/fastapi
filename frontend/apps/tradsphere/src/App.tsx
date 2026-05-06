import { useEffect, useState } from "react";
import {
  type ColumnDef,
  type FilterFn,
  getCoreRowModel,
  getFilteredRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { AlertCircle, CalendarDays, CloudUpload, Monitor, Plus } from "lucide-react";

import { AccountInformationCard } from "@/components/dashboard/AccountInformationCard";
import { AccountSelector } from "@/components/dashboard/AccountSelector";
import { AppHeader } from "@/components/dashboard/AppHeader";
import { DashboardPanel } from "@/components/dashboard/DashboardPanel";
import { HeroBanner } from "@/components/dashboard/HeroBanner";
import { ScheduleCard } from "@/components/dashboard/ScheduleCard";
import { ScheduleUploadDialog } from "@/components/dashboard/ScheduleUploadDialog";
import { StationCard } from "@/components/dashboard/StationCard";
import type {
  AccountInfo,
  AccountSelection,
  ApiMainLoadResponse,
  EsnumItem,
  MainLoadResponse,
  StationItem,
} from "@/components/dashboard/types";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";

const scheduleColumns: ColumnDef<EsnumItem>[] = [{ accessorKey: "estnum" }, { accessorKey: "name" }];

const stationColumns: ColumnDef<StationItem>[] = [{ accessorKey: "code" }, { accessorKey: "name" }];

const SELECTIONS_CACHE_KEY = "tradsphere:main:selections:v2";
const ONE_HOUR_MS = 60 * 60 * 1000;
const SELECTIONS_CACHE_TTL_MS = ONE_HOUR_MS;
const LOAD_CACHE_TTL_MS = ONE_HOUR_MS;
const SCHEDULE_IMPORT_URL = "/api/tradsphere/v1/schedules/import/file?skipBlankLines=false";

const scheduleFilterFn: FilterFn<EsnumItem> = (row, _columnId, filterValue) => {
  const query = String(filterValue ?? "").trim().toLowerCase();
  if (!query) {
    return true;
  }
  const esnum = row.original;
  return [String(esnum.estnum), esnum.name, esnum.note ?? ""]
    .join(" ")
    .toLowerCase()
    .includes(query);
};

const stationFilterFn: FilterFn<StationItem> = (row, _columnId, filterValue) => {
  const query = String(filterValue ?? "").trim().toLowerCase();
  if (!query) {
    return true;
  }
  const station = row.original;
  const repText = (station.repContacts ?? [])
    .map((contact) => `${contact.fullName ?? ""} ${contact.email ?? ""}`.trim())
    .join(" ");
  return [station.code, station.name ?? "", repText].join(" ").toLowerCase().includes(query);
};

function App() {
  const [accountSelections, setAccountSelections] = useState<AccountSelection[]>([]);
  const [selectedAccountCode, setSelectedAccountCode] = useState("");
  const [isLoadingSelections, setIsLoadingSelections] = useState(true);
  const [selectionsError, setSelectionsError] = useState<string | null>(null);

  const [isLoadingAccount, setIsLoadingAccount] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [hasLoadedDashboard, setHasLoadedDashboard] = useState(false);

  const [accountOriginal, setAccountOriginal] = useState<AccountInfo | null>(null);
  const [accountForm, setAccountForm] = useState<AccountInfo | null>(null);

  const [esnums, setEsnums] = useState<EsnumItem[]>([]);
  const [stations, setStations] = useState<StationItem[]>([]);

  const [scheduleSearch, setScheduleSearch] = useState("");
  const [stationSearch, setStationSearch] = useState("");

  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [isScheduleUploadOpen, setIsScheduleUploadOpen] = useState(false);
  const [scheduleUploadSuccessMessage, setScheduleUploadSuccessMessage] = useState<string | null>(null);

  useEffect(() => {
    void fetchSelections();
  }, []);

  const schedulesTable = useReactTable({
    data: esnums,
    columns: scheduleColumns,
    state: { globalFilter: scheduleSearch },
    onGlobalFilterChange: setScheduleSearch,
    globalFilterFn: scheduleFilterFn,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  });

  const stationsTable = useReactTable({
    data: stations,
    columns: stationColumns,
    state: { globalFilter: stationSearch },
    onGlobalFilterChange: setStationSearch,
    globalFilterFn: stationFilterFn,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  });

  const filteredSchedules = schedulesTable.getFilteredRowModel().rows.map((row) => row.original);
  const filteredStations = stationsTable.getFilteredRowModel().rows.map((row) => row.original);

  const hasEditableChanges = Boolean(
    accountOriginal &&
      accountForm &&
      (accountOriginal.billingType !== accountForm.billingType ||
        (accountOriginal.market ?? "") !== (accountForm.market ?? "") ||
        (accountOriginal.note ?? "") !== (accountForm.note ?? "")),
  );

  function resetLoadedDashboardData() {
    setAccountOriginal(null);
    setAccountForm(null);
    setEsnums([]);
    setStations([]);
    setScheduleSearch("");
    setStationSearch("");
    setScheduleUploadSuccessMessage(null);
  }

  function applyLoadedData(data: MainLoadResponse) {
    setAccountOriginal(data.account);
    setAccountForm(data.account);
    setEsnums(data.esnums);
    setStations(data.stations);
    setScheduleSearch("");
    setStationSearch("");
  }

  function handleAccountChange(value: string) {
    setSelectedAccountCode(value);
    setLoadError(null);
    setSaveError(null);
    setHasLoadedDashboard(false);
    resetLoadedDashboardData();
  }

  async function fetchSelections() {
    setIsLoadingSelections(true);
    setSelectionsError(null);

    try {
      const cachedSelections = getCachedData<AccountSelection[]>(SELECTIONS_CACHE_KEY);
      if (cachedSelections) {
        setAccountSelections(cachedSelections);
        if (!selectedAccountCode && cachedSelections.length > 0) {
          setSelectedAccountCode(cachedSelections[0].accountCode);
        }
        return;
      }

      const payload = await requestJson("/api/tradsphere/v1/ui/main/selections");
      const selections = normalizeSelectionsResponse(payload);
      setCachedData(SELECTIONS_CACHE_KEY, selections, SELECTIONS_CACHE_TTL_MS);
      setAccountSelections(selections);
      if (!selectedAccountCode && selections.length > 0) {
        setSelectedAccountCode(selections[0].accountCode);
      }
    } catch (error) {
      setSelectionsError(getErrorMessage(error, "Unable to load account selections."));
      setAccountSelections([]);
      setSelectedAccountCode("");
      setHasLoadedDashboard(false);
      resetLoadedDashboardData();
    } finally {
      setIsLoadingSelections(false);
    }
  }

  async function handleLoadAccount() {
    if (!selectedAccountCode || isLoadingAccount || isSaving) {
      return;
    }

    setIsLoadingAccount(true);
    setLoadError(null);
    setSaveError(null);
    setHasLoadedDashboard(false);

    try {
      const loadCacheKey = getLoadCacheKey(selectedAccountCode);
      const cachedDashboard = normalizeCachedMainLoadResponse(getCachedData<unknown>(loadCacheKey));
      if (cachedDashboard && shouldUseCachedLoad(cachedDashboard, selectedAccountCode)) {
        applyLoadedData(cachedDashboard);
        setHasLoadedDashboard(true);
        return;
      }

      const payload = await requestJson(
        `/api/tradsphere/v1/ui/main/load?accountCode=${encodeURIComponent(selectedAccountCode)}`,
      );
      const data = normalizeMainLoadResponse(payload);

      if (!data?.account) {
        throw new Error("Load response did not include account data.");
      }

      setCachedData(loadCacheKey, data, LOAD_CACHE_TTL_MS);
      applyLoadedData(data);
      setHasLoadedDashboard(true);
    } catch (error) {
      setLoadError(getErrorMessage(error, "Unable to load account dashboard data."));
      resetLoadedDashboardData();
      setHasLoadedDashboard(false);
    } finally {
      setIsLoadingAccount(false);
    }
  }

  async function handleSaveAccount() {
    if (!accountForm || isSaving || isLoadingAccount) {
      return;
    }

    setIsSaving(true);
    setSaveError(null);

    try {
      await saveAccountChanges(accountForm);
      setAccountOriginal(accountForm);

      const cacheKey = getLoadCacheKey(accountForm.code);
      const cached = getCachedData<MainLoadResponse>(cacheKey);
      if (cached) {
        setCachedData(
          cacheKey,
          {
            ...cached,
            account: accountForm,
          },
          LOAD_CACHE_TTL_MS,
        );
      }
    } catch (error) {
      setSaveError(getErrorMessage(error, "Unable to save account changes."));
    } finally {
      setIsSaving(false);
    }
  }

  return (
    <div className="relative min-h-screen overflow-hidden bg-app-gradient px-4 py-5 text-foreground sm:px-6 lg:px-8">
      <div className="pointer-events-none absolute -left-24 top-0 size-72 rounded-full bg-blue-200/40 blur-3xl" />
      <div className="pointer-events-none absolute right-10 top-28 size-64 rounded-full bg-orange-200/35 blur-3xl" />
      <div className="pointer-events-none absolute left-[40%] top-16 size-64 rounded-full bg-violet-200/30 blur-3xl" />

      <div className="relative mx-auto flex w-full max-w-[1600px] flex-col gap-6">
        <AppHeader />
        <HeroBanner />

        <AccountSelector
          selectedAccountCode={selectedAccountCode}
          options={accountSelections}
          isLoadingSelections={isLoadingSelections}
          selectionsError={selectionsError}
          isLoadingAccount={isLoadingAccount}
          isSavingAccount={isSaving}
          onAccountChange={handleAccountChange}
          onLoad={handleLoadAccount}
        />

        {loadError ? (
          <p className="flex items-center gap-2 text-sm text-rose-600">
            <AlertCircle className="size-4" />
            {loadError}
          </p>
        ) : null}

        {hasLoadedDashboard ? (
          <>
            <Separator />

            <main className="grid gap-5 lg:grid-cols-[360px_minmax(0,1fr)] lg:items-start">
              <AccountInformationCard
                account={accountForm}
                isSaving={isSaving}
                hasEditableChanges={hasEditableChanges}
                saveError={saveError}
                onBillingTypeChange={(billingType) =>
                  setAccountForm((current) =>
                    current
                      ? {
                          ...current,
                          billingType,
                        }
                      : current,
                  )
                }
                onMarketChange={(market) =>
                  setAccountForm((current) => (current ? { ...current, market } : current))
                }
                onNoteChange={(note) =>
                  setAccountForm((current) => (current ? { ...current, note } : current))
                }
                onSave={handleSaveAccount}
              />

              <div className="space-y-5">
                <DashboardPanel
                  title="EstNums - Schedules"
                  icon={<CalendarDays className="size-5 text-blue-600" />}
                  searchValue={scheduleSearch}
                  onSearchChange={setScheduleSearch}
                  actions={
                    <>
                      <Button
                        variant="ghost"
                        size="icon"
                        aria-label="Upload schedules"
                        onClick={() => {
                          setScheduleUploadSuccessMessage(null);
                          setIsScheduleUploadOpen(true);
                        }}
                        disabled={isLoadingAccount || isSaving}
                      >
                        <CloudUpload className="size-4 text-blue-500" />
                      </Button>
                      <Button variant="ghost" size="icon" aria-label="Add schedule">
                        <Plus className="size-4 text-blue-500" />
                      </Button>
                    </>
                  }
                >
                  {scheduleUploadSuccessMessage ? (
                    <p className="rounded-md border border-emerald-100 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
                      {scheduleUploadSuccessMessage}
                    </p>
                  ) : null}
                  <div className="flex gap-3 overflow-x-auto pb-2">
                    {filteredSchedules.map((esnum) => (
                      <ScheduleCard
                        key={esnum.estnum}
                        esnum={esnum}
                      />
                    ))}
                  </div>
                  {!filteredSchedules.length ? (
                    <p className="text-sm text-slate-500">No EstNums or schedules were returned for this account.</p>
                  ) : null}
                </DashboardPanel>

                <DashboardPanel
                  title="Stations"
                  icon={<Monitor className="size-5 text-blue-600" />}
                  searchValue={stationSearch}
                  onSearchChange={setStationSearch}
                  actions={
                    <Button variant="ghost" size="icon" aria-label="Add station">
                      <Plus className="size-4 text-blue-500" />
                    </Button>
                  }
                >
                  <div className="flex gap-3 overflow-x-auto pb-2">
                    {filteredStations.map((station) => (
                      <StationCard key={station.code} station={station} />
                    ))}
                  </div>
                  {!filteredStations.length ? (
                    <p className="text-sm text-slate-500">No stations were returned for this account.</p>
                  ) : null}
                </DashboardPanel>
              </div>
            </main>
          </>
        ) : null}
      </div>

      <ScheduleUploadDialog
        open={isScheduleUploadOpen}
        onOpenChange={setIsScheduleUploadOpen}
        uploadUrl={SCHEDULE_IMPORT_URL}
        headers={buildAuthHeaders(false)}
        onUploadSuccess={(fileName) => {
          setScheduleUploadSuccessMessage(`Upload completed for "${fileName}".`);
        }}
      />
    </div>
  );
}

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
  if (typeof value === "number") {
    return String(value);
  }
  return "";
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function buildLabel(code: string, name: string): string {
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
    const label = asString(item.label) || buildLabel(accountCode, name);

    if (!accountCode || !label || seen.has(accountCode)) {
      continue;
    }

    seen.add(accountCode);
    output.push({ accountCode, label, name: name || undefined });
  }

  return output;
}

function normalizeMainLoadResponse(payload: unknown): MainLoadResponse | null {
  const apiResponse = parseApiMainLoadResponse(payload);
  if (!apiResponse) {
    return null;
  }

  return {
    account: {
      code: apiResponse.data.account.code,
      name: apiResponse.data.account.name,
      logoUrl: apiResponse.data.account.logoUrl ?? undefined,
      billingType: apiResponse.data.account.billingType || "Calendar",
      market: apiResponse.data.account.market ?? undefined,
      note: apiResponse.data.account.note ?? undefined,
    },
    esnums: apiResponse.data.esnums ?? [],
    stations: apiResponse.data.stations ?? [],
  };
}

function normalizeCachedMainLoadResponse(payload: unknown): MainLoadResponse | null {
  if (!isRecord(payload)) {
    return null;
  }

  const accountRow = isRecord(payload.account) ? payload.account : null;
  if (!accountRow) {
    return null;
  }

  const code = asString(accountRow.code).toUpperCase();
  const name = asString(accountRow.name);
  if (!code || !name) {
    return null;
  }

  const esnums = (Array.isArray(payload.esnums) ? payload.esnums : []).reduce<EsnumItem[]>((items, item) => {
    if (!isRecord(item)) {
      return items;
    }

    const estnum = asNumber(item.estnum);
    if (estnum === null) {
      return items;
    }

    items.push({
      estnum,
      name: asString(item.name) || String(estnum),
      hasSchedule: Boolean(item.hasSchedule),
      note: asNullableString(item.note),
    });
    return items;
  }, []);

  const stations = (Array.isArray(payload.stations) ? payload.stations : []).reduce<StationItem[]>((items, item) => {
    if (!isRecord(item)) {
      return items;
    }

    const stationCode = asString(item.code).toUpperCase();
    if (!stationCode) {
      return items;
    }

    const repContacts = (Array.isArray(item.repContacts) ? item.repContacts : []).reduce<NonNullable<StationItem["repContacts"]>>((contacts, contact) => {
      if (!isRecord(contact)) {
        return contacts;
      }
      const fullName = asNullableString(contact.fullName);
      const email = asNullableString(contact.email);
      if (!fullName && !email) {
        return contacts;
      }
      contacts.push({ fullName, email });
      return contacts;
    }, []);

    items.push({
      code: stationCode,
      name: asString(item.name),
      repContacts: repContacts.length ? repContacts : undefined,
    });
    return items;
  }, []);

  return {
    account: {
      code,
      name,
      logoUrl: asNullableString(accountRow.logoUrl),
      billingType: asNullableString(accountRow.billingType) || "Calendar",
      market: asNullableString(accountRow.market),
      note: asNullableString(accountRow.note),
    },
    esnums,
    stations,
  };
}

function shouldUseCachedLoad(cached: MainLoadResponse, selectedAccountCode: string): boolean {
  if (cached.account.code.toUpperCase() !== selectedAccountCode.toUpperCase()) {
    return false;
  }
  if (cached.esnums.length === 0 && cached.stations.length === 0) {
    return false;
  }
  return true;
}

function parseApiMainLoadResponse(payload: unknown): ApiMainLoadResponse | null {
  if (!isRecord(payload)) {
    return null;
  }

  const data = isRecord(payload.data) ? payload.data : payload;
  if (!data) {
    return null;
  }

  const accountRow = isRecord(data.account) ? data.account : null;
  if (!accountRow) {
    return null;
  }

  const accountCode = asString(accountRow.code).toUpperCase();
  const accountName = asString(accountRow.name);
  if (!accountCode || !accountName) {
    return null;
  }

  const esnums = (Array.isArray(data.esnums) ? data.esnums : []).reduce<EsnumItem[]>((items, item) => {
    if (!isRecord(item)) {
      return items;
    }

    const estnum = asNumber(item.estnum);
    if (estnum === null) {
      return items;
    }

    items.push({
      estnum,
      name: asString(item.name) || String(estnum),
      hasSchedule: Boolean(item.hasSchedule),
      note: asNullableString(item.note),
    });
    return items;
  }, []);

  const stations = (Array.isArray(data.stations) ? data.stations : []).reduce<StationItem[]>((items, item) => {
    if (!isRecord(item)) {
      return items;
    }

    const stationCode = asString(item.code).toUpperCase();
    if (!stationCode) {
      return items;
    }

    const repContacts = (Array.isArray(item.repContacts) ? item.repContacts : []).reduce<NonNullable<StationItem["repContacts"]>>((contacts, contact) => {
      if (!isRecord(contact)) {
        return contacts;
      }

      const fullName = asNullableString(contact.fullName);
      const email = asNullableString(contact.email);
      if (!fullName && !email) {
        return contacts;
      }

      contacts.push({ fullName, email });
      return contacts;
    }, []);

    items.push({
      code: stationCode,
      name: asString(item.name),
      repContacts: repContacts.length ? repContacts : undefined,
    });
    return items;
  }, []);

  return {
    meta: isRecord(payload.meta)
      ? {
          timestamp: asString(payload.meta.timestamp) || undefined,
          duration_ms: asNumber(payload.meta.duration_ms) ?? undefined,
          duration_hms: asString(payload.meta.duration_hms) || undefined,
          client_id: asString(payload.meta.client_id) || undefined,
          request_id: asString(payload.meta.request_id) || undefined,
        }
      : undefined,
    data: {
      account: {
        code: accountCode,
        name: accountName,
        logoUrl: asNullableString(accountRow.logoUrl),
        billingType: asNullableString(accountRow.billingType),
        market: asNullableString(accountRow.market),
        note: asNullableString(accountRow.note),
      },
      esnums,
      stations,
    },
  };
}

function asNullableString(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const text = asString(value);
  return text || null;
}

type CachedEntry<T> = {
  timestamp: number;
  ttlMs: number;
  data: T;
};

function getLoadCacheKey(accountCode: string): string {
  return `tradsphere:main:load:${accountCode.toUpperCase()}:v2`;
}

function getCacheStorage(): Storage | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function isCacheValid(entry: CachedEntry<unknown>): boolean {
  return Date.now() - entry.timestamp <= entry.ttlMs;
}

function readCachedEntry<T>(key: string): CachedEntry<T> | null {
  const storage = getCacheStorage();
  if (!storage) {
    return null;
  }

  const raw = storage.getItem(key);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as CachedEntry<T>;
    if (
      !parsed ||
      typeof parsed !== "object" ||
      typeof parsed.timestamp !== "number" ||
      typeof parsed.ttlMs !== "number" ||
      !("data" in parsed)
    ) {
      storage.removeItem(key);
      return null;
    }
    return parsed;
  } catch {
    storage.removeItem(key);
    return null;
  }
}

function getCachedData<T>(key: string): T | null {
  const entry = readCachedEntry<T>(key);
  if (!entry) {
    return null;
  }
  if (!isCacheValid(entry)) {
    return null;
  }
  return entry.data;
}

function setCachedData<T>(key: string, data: T, ttlMs: number): void {
  const storage = getCacheStorage();
  if (!storage) {
    return;
  }

  const entry: CachedEntry<T> = {
    timestamp: Date.now(),
    ttlMs,
    data,
  };
  storage.setItem(key, JSON.stringify(entry));
}

function buildAuthHeaders(includeJsonContentType: boolean): HeadersInit {
  return {
    "X-API-Key": "6ad13c1f7c17c32fb5a4582b4be42df5",
    "X-Tenant-Id": "taaa",
    "X-User-Name": "Hai Truong",
    ...(includeJsonContentType ? { "Content-Type": "application/json" } : {}),
  };
}

type RequestOptions = {
  method?: "GET" | "PUT" | "POST";
  body?: unknown;
};

async function requestJson(url: string, options: RequestOptions = {}): Promise<unknown> {
  const method = options.method ?? "GET";
  const hasBody = options.body !== undefined;

  const response = await fetch(url, {
    method,
    headers: buildAuthHeaders(hasBody),
    body: hasBody ? JSON.stringify(options.body) : undefined,
  });

  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(extractApiError(payload) || `Request failed with status ${response.status}.`);
  }

  return payload;
}

function extractApiError(payload: unknown): string | null {
  if (!isRecord(payload)) {
    return null;
  }

  const error = isRecord(payload.error) ? payload.error : null;
  if (error) {
    const message = asString(error.message);
    const detail = asString(error.detail);
    return message || detail || null;
  }

  return asString(payload.detail) || null;
}

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return fallback;
}

async function saveAccountChanges(account: AccountInfo): Promise<void> {
  const payload = {
    accountCode: account.code,
    billingType: account.billingType || "Calendar",
    market: account.market ?? "",
    note: account.note ?? "",
  };

  await requestJson("/api/tradsphere/v1/accounts", {
    method: "PUT",
    body: payload,
  });
}

export default App;
