import { useEffect, useMemo, useRef, useState } from "react";
import {
  type ColumnDef,
  type FilterFn,
  getCoreRowModel,
  getFilteredRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { AlertCircle, CalendarDays, CloudUpload, Loader2, Monitor, Plus, X } from "lucide-react";

import { AccountInformationCard } from "@/components/dashboard/AccountInformationCard";
import { ActionIconButton } from "@/components/dashboard/ActionIconButton";
import { AccountSelector } from "@/components/dashboard/AccountSelector";
import { AppHeader } from "@/components/dashboard/AppHeader";
import { DashboardPanel } from "@/components/dashboard/DashboardPanel";
import { AccountEditableFields, ACCOUNT_BILLING_OPTIONS } from "@/components/dashboard/AccountEditableFields";
import {
  EstimateNumberModal,
  type EstimateNumberModalData,
  type EstimateNumberModalMode,
  type EstimateNumberModalSaveResult,
} from "@/components/dashboard/EstimateNumberModal";
import { HeroBanner } from "@/components/dashboard/HeroBanner";
import { ScheduleCard } from "@/components/dashboard/ScheduleCard";
import { ScheduleTimelineSection } from "@/components/dashboard/ScheduleTimelineSection";
import { ScheduleModal } from "@/components/dashboard/ScheduleModal";
import { ScheduleUploadDialog } from "@/components/dashboard/ScheduleUploadDialog";
import { StationCard } from "@/components/dashboard/StationCard";
import {
  StationModal,
  type StationModalMode,
  type StationModalSaveResult,
} from "@/components/dashboard/StationModal";
import { LabeledField } from "@/components/dashboard/FormFieldRow";
import { Button } from "@/components/ui/button";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { canModalClose, shouldBlockOutsideClose } from "@/components/ui/modal-close-guard";
import type {
  AccountInfo,
  AccountSelection,
  ApiMainLoadResponse,
  EsnumItem,
  MainLoadResponse,
  StationItem,
} from "@/components/dashboard/types";
import { Separator } from "@/components/ui/separator";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { useToast } from "@/components/ui/toast";
import { useApiRequest, type ApiRequestOptions } from "@/hooks/useApiRequest";
import { usePersistentState } from "@/hooks/usePersistentState";
import {
  readBrowserCacheSnapshot,
  removeBrowserCacheByPrefix,
  writeBrowserCache,
} from "@/lib/browserCache";
import { buildAuthHeaders as buildSharedAuthHeaders } from "@shared/api/authHeaders";
import { TRADSPHERE_CACHE_TTL_MS, shouldFetchNetwork, type CachePolicy } from "@shared/cache";
import { useAuth } from "@shared/auth/useAuth";
import { shouldProtectTradsphereFrontend } from "@shared/auth/guards";
import { hasAppEditAccess } from "@shared/auth/permissions";

const scheduleColumns: ColumnDef<EsnumItem>[] = [{ accessorKey: "estnum" }, { accessorKey: "name" }];

const stationColumns: ColumnDef<StationItem>[] = [{ accessorKey: "code" }, { accessorKey: "name" }];

const SELECTIONS_CACHE_KEY = "tradsphere:main:selections:v2";
const SELECTIONS_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.SELECTIONS;
const LOAD_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.MAIN_LOAD;
const SCHEDULE_IMPORT_URL = "/api/tradsphere/v1/schedules/import/file?skipBlankLines=false";
const HOME_SELECTED_ACCOUNT_STORAGE_KEY = "tradsphere.home.selectedAccount";
const HOME_SCHEDULE_SEARCH_STORAGE_KEY = "tradsphere.home.searchText.schedule";
const HOME_STATION_SEARCH_STORAGE_KEY = "tradsphere.home.searchText.station";
const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type CreateAccountFormState = {
  accountCode: string;
  billingType: string;
  market: string;
  note: string;
};

const CREATE_ACCOUNT_DEFAULT_FORM: CreateAccountFormState = {
  accountCode: "",
  billingType: ACCOUNT_BILLING_OPTIONS[0].value,
  market: "",
  note: "",
};

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

function App() {
  const toast = useToast();
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());
  const [accountSelections, setAccountSelections] = useState<AccountSelection[]>([]);
  const [selectedAccountCode, setSelectedAccountCode] = usePersistentState<string>(
    HOME_SELECTED_ACCOUNT_STORAGE_KEY,
    "",
    { storage: "session", validate: (value: unknown): value is string => typeof value === "string" },
  );
  const [isLoadingSelections, setIsLoadingSelections] = useState(true);
  const [isRefreshingSelections, setIsRefreshingSelections] = useState(false);
  const [selectionsError, setSelectionsError] = useState<string | null>(null);
  const [selectionsCacheStatus, setSelectionsCacheStatus] = useState<CacheStatus | null>(null);

  const [isLoadingAccount, setIsLoadingAccount] = useState(false);
  const [isRefreshingAccount, setIsRefreshingAccount] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [hasLoadedDashboard, setHasLoadedDashboard] = useState(false);
  const [dashboardCacheStatus, setDashboardCacheStatus] = useState<CacheStatus | null>(null);

  const [accountOriginal, setAccountOriginal] = useState<AccountInfo | null>(null);
  const [accountForm, setAccountForm] = useState<AccountInfo | null>(null);

  const [esnums, setEsnums] = useState<EsnumItem[]>([]);
  const [stations, setStations] = useState<StationItem[]>([]);

  const [scheduleSearch, setScheduleSearch] = usePersistentState<string>(
    HOME_SCHEDULE_SEARCH_STORAGE_KEY,
    "",
    { storage: "session", validate: (value: unknown): value is string => typeof value === "string" },
  );
  const [stationSearch, setStationSearch] = usePersistentState<string>(
    HOME_STATION_SEARCH_STORAGE_KEY,
    "",
    { storage: "session", validate: (value: unknown): value is string => typeof value === "string" },
  );

  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [isScheduleUploadOpen, setIsScheduleUploadOpen] = useState(false);
  const [isScheduleModalOpen, setIsScheduleModalOpen] = useState(false);
  const [selectedScheduleEstnum, setSelectedScheduleEstnum] = useState<EsnumItem | null>(null);
  const [scheduleCacheInvalidationToken, setScheduleCacheInvalidationToken] = useState(0);
  const [invalidatedScheduleEstnum, setInvalidatedScheduleEstnum] = useState<number | null>(null);
  const [isCreateAccountModalOpen, setIsCreateAccountModalOpen] = useState(false);
  const [isCreateAccountUnsavedDialogOpen, setIsCreateAccountUnsavedDialogOpen] = useState(false);
  const [isCreatingAccount, setIsCreatingAccount] = useState(false);
  const [createAccountForm, setCreateAccountForm] = useState<CreateAccountFormState>({ ...CREATE_ACCOUNT_DEFAULT_FORM });
  const [createAccountError, setCreateAccountError] = useState<string | null>(null);
  const [isEstimateNumberModalOpen, setIsEstimateNumberModalOpen] = useState(false);
  const [estimateModalMode, setEstimateModalMode] = useState<EstimateNumberModalMode>("create");
  const [estimateModalInitialData, setEstimateModalInitialData] = useState<EstimateNumberModalData | null>(null);
  const [isStationModalOpen, setIsStationModalOpen] = useState(false);
  const [stationModalMode, setStationModalMode] = useState<StationModalMode>("create");
  const [stationModalCode, setStationModalCode] = useState<string | null>(null);
  const [scheduleUploadSuccessMessage, setScheduleUploadSuccessMessage] = useState<string | null>(null);
  const hasAttemptedDashboardRestoreRef = useRef(false);
  const requestHeaders = useMemo(
    () => buildSharedAuthHeaders(auth.session, auth.tenantSlug, false),
    [auth.session, auth.tenantSlug],
  );
  const canEditTradsphere = useMemo(() => {
    if (!shouldProtectTradsphereFrontend()) {
      return true;
    }
    return hasAppEditAccess(auth.accessProfile, "tradsphere");
  }, [auth.accessProfile]);

  useEffect(() => {
    void fetchSelections("stale-while-revalidate");
  }, []);

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
    setScheduleUploadSuccessMessage(null);
  }

  function applyLoadedData(data: MainLoadResponse) {
    setAccountOriginal(data.account);
    setAccountForm(data.account);
    setEsnums(data.esnums);
    setStations(data.stations);
  }

  function handleAccountChange(value: string) {
    setSelectedAccountCode(value.toUpperCase());
    setLoadError(null);
    setSaveError(null);
    setDashboardCacheStatus(null);
    setIsRefreshingAccount(false);
    setIsEstimateNumberModalOpen(false);
    setEstimateModalMode("create");
    setEstimateModalInitialData(null);
    setIsStationModalOpen(false);
    setStationModalMode("create");
    setStationModalCode(null);
    setIsScheduleModalOpen(false);
    setSelectedScheduleEstnum(null);
    setHasLoadedDashboard(false);
    resetLoadedDashboardData();
  }

  async function fetchSelections(policy: CachePolicy) {
    const cacheSnapshot = readBrowserCacheSnapshot<AccountSelection[]>(SELECTIONS_CACHE_KEY);
    const cachedSelections = normalizeSelectionsResponse(cacheSnapshot?.data);
    const hasCachedSelections = cachedSelections.length > 0;
    const shouldUseCache = policy !== "network-only" && hasCachedSelections;
    const shouldFetchFromNetwork = shouldFetchNetwork(policy, cacheSnapshot);

    if (shouldUseCache) {
      applySelections(cachedSelections);
      setSelectionsCacheStatus({
        source: "cache",
        fetchedAt: cacheSnapshot?.fetchedAt ?? Date.now(),
      });
    }

    if (!shouldFetchFromNetwork) {
      setIsLoadingSelections(false);
      setIsRefreshingSelections(false);
      return;
    }

    setIsLoadingSelections(!shouldUseCache);
    setIsRefreshingSelections(shouldUseCache);
    setSelectionsError(null);

    try {
      const payload = await requestJson("/api/tradsphere/v1/ui/main/selections", {
        headers: requestHeaders,
      });
      const selections = normalizeSelectionsResponse(payload);
      writeBrowserCache(SELECTIONS_CACHE_KEY, selections, SELECTIONS_CACHE_TTL_MS, { source: "network" });
      applySelections(selections);
      setSelectionsCacheStatus({
        source: "network",
        fetchedAt: Date.now(),
      });
    } catch (error) {
      if (!shouldUseCache) {
        setSelectionsError(getErrorMessage(error, "Unable to load account selections."));
        setAccountSelections([]);
        setSelectedAccountCode("");
        setHasLoadedDashboard(false);
        resetLoadedDashboardData();
      } else {
        setSelectionsError(null);
      }
    } finally {
      setIsLoadingSelections(false);
      setIsRefreshingSelections(false);
    }
  }

  function applySelections(selections: AccountSelection[]) {
    setAccountSelections(selections);
    if (!selections.length) {
      setSelectedAccountCode("");
      return;
    }

    const normalizedSelectedAccountCode = selectedAccountCode.toUpperCase();
    const matchingSelection = selections.find(
      (option) => option.accountCode.toUpperCase() === normalizedSelectedAccountCode,
    );
    if (matchingSelection) {
      if (matchingSelection.accountCode !== selectedAccountCode) {
        setSelectedAccountCode(matchingSelection.accountCode);
      }
      return;
    }

    setSelectedAccountCode(selections[0].accountCode);
    setHasLoadedDashboard(false);
    resetLoadedDashboardData();
  }

  useEffect(() => {
    if (isLoadingSelections || hasAttemptedDashboardRestoreRef.current) {
      return;
    }

    if (!selectedAccountCode || hasLoadedDashboard || isLoadingAccount || isSaving) {
      return;
    }
    hasAttemptedDashboardRestoreRef.current = true;

    const cacheSnapshot = readBrowserCacheSnapshot<unknown>(getLoadCacheKey(selectedAccountCode));
    const cachedDashboard = normalizeCachedMainLoadResponse(cacheSnapshot?.data);
    if (!cachedDashboard || !shouldUseCachedLoad(cachedDashboard, selectedAccountCode)) {
      return;
    }

    void loadAccountDashboard("stale-while-revalidate");
  }, [hasLoadedDashboard, isLoadingAccount, isLoadingSelections, isSaving, selectedAccountCode]);

  async function loadAccountDashboard(
    policy: CachePolicy,
  ): Promise<{ success: boolean; source: "cache" | "network" | null }> {
    if (!selectedAccountCode || isLoadingAccount || isSaving) {
      return { success: false, source: null };
    }

    const loadCacheKey = getLoadCacheKey(selectedAccountCode);
    const cacheSnapshot = readBrowserCacheSnapshot<unknown>(loadCacheKey);
    const cachedDashboard = normalizeCachedMainLoadResponse(cacheSnapshot?.data);
    const canUseCachedData =
      policy !== "network-only" &&
      !!cachedDashboard &&
      shouldUseCachedLoad(cachedDashboard, selectedAccountCode);
    const shouldFetchFromNetwork = shouldFetchNetwork(policy, cacheSnapshot);

    if (canUseCachedData && cachedDashboard) {
      applyLoadedData(cachedDashboard);
      setHasLoadedDashboard(true);
      setDashboardCacheStatus({
        source: "cache",
        fetchedAt: cacheSnapshot?.fetchedAt ?? Date.now(),
      });
    }

    if (!shouldFetchFromNetwork) {
      return { success: true, source: "cache" };
    }

    setIsLoadingAccount(!canUseCachedData);
    setIsRefreshingAccount(canUseCachedData);
    setLoadError(null);
    setSaveError(null);

    try {
      const payload = await requestJson(
        `/api/tradsphere/v1/ui/accounts/load?accountCode=${encodeURIComponent(selectedAccountCode)}`,
        {
          headers: requestHeaders,
        },
      );
      const data = normalizeMainLoadResponse(payload);

      if (!data?.account) {
        throw new Error("Load response did not include account data.");
      }

      writeBrowserCache(loadCacheKey, data, LOAD_CACHE_TTL_MS, { source: "network" });
      applyLoadedData(data);
      setHasLoadedDashboard(true);
      setDashboardCacheStatus({
        source: "network",
        fetchedAt: Date.now(),
      });
      return { success: true, source: "network" };
    } catch (error) {
      const errorMessage = getErrorMessage(error, "Unable to load account dashboard data.");
      if (canUseCachedData) {
        setLoadError(null);
        return { success: true, source: "cache" };
      } else {
        setLoadError(errorMessage);
        resetLoadedDashboardData();
        setHasLoadedDashboard(false);
        return { success: false, source: null };
      }
    } finally {
      setIsLoadingAccount(false);
      setIsRefreshingAccount(false);
    }
  }

  async function handleLoadAccount() {
    const isSameLoadedAccount =
      hasLoadedDashboard &&
      accountOriginal?.code?.trim().toUpperCase() === selectedAccountCode.trim().toUpperCase();
    const result = await loadAccountDashboard(isSameLoadedAccount ? "network-only" : "cache-first");
    if (result.success && selectedAccountCode) {
      if (isSameLoadedAccount) {
        toast.success("Dashboard refreshed", `Fetched fresh data for ${selectedAccountCode}.`);
      } else {
        const suffix = result.source === "cache" ? " from cache." : ".";
        toast.success("Account loaded", `Loaded dashboard for ${selectedAccountCode}${suffix}`);
      }
    }
  }

  async function handleRefreshAccount(): Promise<void> {
    if (!selectedAccountCode) {
      return;
    }
    const result = await loadAccountDashboard("network-only");
    if (result.success) {
      toast.success("Dashboard refreshed", `Fetched fresh data for ${selectedAccountCode}.`);
    }
  }

  async function handleSaveAccount() {
    if (!canEditTradsphere || !accountForm || isSaving || isLoadingAccount) {
      return;
    }

    setIsSaving(true);
    setSaveError(null);

    try {
      await saveAccountChanges(requestJson, accountForm, requestHeaders);
      setAccountOriginal(accountForm);

      const cacheKey = getLoadCacheKey(accountForm.code);
      const cached = normalizeCachedMainLoadResponse(readBrowserCacheSnapshot<unknown>(cacheKey)?.data);
      if (cached) {
        writeBrowserCache(
          cacheKey,
          {
            ...cached,
            account: accountForm,
          },
          LOAD_CACHE_TTL_MS,
          { source: "network" },
        );
        setDashboardCacheStatus({
          source: "network",
          fetchedAt: Date.now(),
        });
      }
    } catch (error) {
      const errorMessage = getErrorMessage(error, "Unable to save account changes.");
      setSaveError(errorMessage);
      toast.error("Save failed", errorMessage);
    } finally {
      setIsSaving(false);
    }
  }

  async function handleEstimateNumberSaved(result: EstimateNumberModalSaveResult): Promise<void> {
    if (!selectedAccountCode || result.accountCode.toUpperCase() !== selectedAccountCode.toUpperCase()) {
      return;
    }

    setEsnums((current) => {
      const existing = current.find((item) => item.estnum === result.estNum);
      const fallbackName = String(result.estNum);
      const nextItem: EsnumItem = {
        estnum: result.estNum,
        name: existing?.name || fallbackName,
        hasSchedule: existing?.hasSchedule ?? false,
        note: result.note,
      };

      const next = existing
        ? current.map((item) => (item.estnum === result.estNum ? { ...item, ...nextItem } : item))
        : [...current, nextItem].sort((a, b) => a.estnum - b.estnum);

      const loadCacheKey = getLoadCacheKey(selectedAccountCode);
      const cachedDashboard = normalizeCachedMainLoadResponse(readBrowserCacheSnapshot<unknown>(loadCacheKey)?.data);
      const accountCacheSource = cachedDashboard?.account ?? accountForm ?? accountOriginal;
      if (accountCacheSource) {
        writeBrowserCache(
          loadCacheKey,
          {
            account: accountCacheSource,
            esnums: next,
            stations,
          },
          LOAD_CACHE_TTL_MS,
          { source: "network" },
        );
        setDashboardCacheStatus({
          source: "network",
          fetchedAt: Date.now(),
        });
      }

      return next;
    });

    setInvalidatedScheduleEstnum(result.estNum);
    setScheduleCacheInvalidationToken((current) => current + 1);
    removeBrowserCacheByPrefix(`schedule-table:${selectedAccountCode.toUpperCase()}:${result.estNum}:`);
  }

  async function handleStationSaved(result: StationModalSaveResult): Promise<void> {
    setStations((current) => {
      const nextStation: StationItem = {
        code: result.stationCode,
        name: result.stationName || result.stationCode,
        repContacts: result.repContacts?.length ? result.repContacts : undefined,
      };
      const existingIndex = current.findIndex(
        (item) => item.code.toUpperCase() === result.stationCode.toUpperCase(),
      );
      const next =
        existingIndex >= 0
          ? current.map((item, index) => (index === existingIndex ? { ...item, ...nextStation } : item))
          : [...current, nextStation].sort((a, b) => a.code.localeCompare(b.code));

      const loadCacheKey = getLoadCacheKey(selectedAccountCode);
      const cachedDashboard = normalizeCachedMainLoadResponse(readBrowserCacheSnapshot<unknown>(loadCacheKey)?.data);
      const accountCacheSource = cachedDashboard?.account ?? accountForm ?? accountOriginal;
      if (accountCacheSource) {
        writeBrowserCache(
          loadCacheKey,
          {
            account: accountCacheSource,
            esnums,
            stations: next,
          },
          LOAD_CACHE_TTL_MS,
          { source: "network" },
        );
        setDashboardCacheStatus({
          source: "network",
          fetchedAt: Date.now(),
        });
      }
      return next;
    });
  }

  function openEstimateCreateModal() {
    if (!canEditTradsphere) {
      return;
    }
    setEstimateModalMode("create");
    setEstimateModalInitialData(null);
    setIsEstimateNumberModalOpen(true);
  }

  function openEstimateEditModal(esnum: EsnumItem) {
    setEstimateModalMode("edit");
    setEstimateModalInitialData({
      estNum: esnum.estnum,
      accountCode: selectedAccountCode,
      note: esnum.note ?? "",
    });
    setIsEstimateNumberModalOpen(true);
  }

  function openStationCreateModal() {
    if (!canEditTradsphere) {
      return;
    }
    setStationModalMode("create");
    setStationModalCode(null);
    setIsStationModalOpen(true);
  }

  function openStationEditModal(station: StationItem) {
    setStationModalMode("edit");
    setStationModalCode(station.code);
    setIsStationModalOpen(true);
  }

  function openScheduleModal(esnum: EsnumItem) {
    if (!esnum.hasSchedule || !selectedAccountCode || isLoadingAccount || isSaving) {
      return;
    }
    setSelectedScheduleEstnum(esnum);
    setIsScheduleModalOpen(true);
  }

  function resetCreateAccountForm() {
    setCreateAccountForm({ ...CREATE_ACCOUNT_DEFAULT_FORM });
    setCreateAccountError(null);
  }

  function hasCreateAccountUnsavedChanges() {
    return (
      createAccountForm.accountCode !== CREATE_ACCOUNT_DEFAULT_FORM.accountCode ||
      createAccountForm.billingType !== CREATE_ACCOUNT_DEFAULT_FORM.billingType ||
      createAccountForm.market !== CREATE_ACCOUNT_DEFAULT_FORM.market ||
      createAccountForm.note !== CREATE_ACCOUNT_DEFAULT_FORM.note
    );
  }

  function getCreateAccountValidationError(): string | null {
    const accountCode = createAccountForm.accountCode.trim().toUpperCase();
    const market = createAccountForm.market.trim();
    const note = createAccountForm.note.trim();
    if (!accountCode) {
      return "Account code is required.";
    }
    if (market.length > 255) {
      return "Market must be 255 characters or fewer.";
    }
    if (note.length > 2048) {
      return "Note must be 2048 characters or fewer.";
    }
    return null;
  }

  function openCreateAccountModal() {
    if (!canEditTradsphere) {
      return;
    }
    resetCreateAccountForm();
    setIsCreateAccountModalOpen(true);
  }

  function handleCreateAccountDialogOpenChange(nextOpen: boolean) {
    if (
      canModalClose({
        nextOpen,
        isBusy: isCreatingAccount,
        hasUnsavedChanges: hasCreateAccountUnsavedChanges(),
      })
    ) {
      setIsCreateAccountModalOpen(nextOpen);
      if (!nextOpen) {
        setIsCreateAccountUnsavedDialogOpen(false);
        resetCreateAccountForm();
      }
      return;
    }
    setIsCreateAccountUnsavedDialogOpen(true);
  }

  async function handleCreateAccount() {
    if (!canEditTradsphere) {
      return;
    }
    const validationError = getCreateAccountValidationError();
    if (validationError) {
      setCreateAccountError(validationError);
      return;
    }

    const accountCode = createAccountForm.accountCode.trim().toUpperCase();
    setIsCreatingAccount(true);
    setCreateAccountError(null);

    try {
      await requestJson("/api/tradsphere/v1/accounts", {
        method: "POST",
        headers: requestHeaders,
        body: {
          accountCode,
          billingType: createAccountForm.billingType,
          market: createAccountForm.market.trim(),
          note: createAccountForm.note.trim(),
        },
        successToast: {
          title: "Account created",
          message: `Account ${accountCode} was created successfully.`,
        },
        errorToast: {
          title: "Create failed",
        },
      });

      removeBrowserCacheByPrefix(SELECTIONS_CACHE_KEY);
      await fetchSelections("network-only");
      handleAccountChange(accountCode);
      setIsCreateAccountModalOpen(false);
      setIsCreateAccountUnsavedDialogOpen(false);
      resetCreateAccountForm();
    } catch (error) {
      setCreateAccountError(getErrorMessage(error, "Unable to create account."));
    } finally {
      setIsCreatingAccount(false);
    }
  }

  const createAccountValidationError = getCreateAccountValidationError();
  const canCreateAccount = !createAccountValidationError;
  const shouldShowCreateAccountSubmit = canCreateAccount || isCreatingAccount;

  const selectionsStatusText = isLoadingSelections
    ? "Loading..."
    : isRefreshingSelections
      ? "Refreshing..."
      : selectionsCacheStatus
        ? `Selections source: ${selectionsCacheStatus.source}. Last updated ${formatRelativeTime(selectionsCacheStatus.fetchedAt)}.`
        : null;
  const dashboardStatusText = isLoadingAccount
    ? "Loading..."
    : isRefreshingAccount
      ? "Refreshing..."
      : dashboardCacheStatus
        ? `Data source: ${dashboardCacheStatus.source}. Last updated ${formatRelativeTime(dashboardCacheStatus.fetchedAt)}.`
        : null;
  const pageCacheStatusText = dashboardStatusText ?? selectionsStatusText;
  const shouldBlockForSelectionsLoad = isLoadingSelections && accountSelections.length === 0;
  const shouldBlockForAccountLoad = isLoadingAccount && !hasLoadedDashboard;
  const isPageBusy = shouldBlockForSelectionsLoad || shouldBlockForAccountLoad || isSaving;
  const pageBusyMessage = isSaving
    ? "Saving account changes..."
    : shouldBlockForAccountLoad
      ? "Loading account dashboard..."
    : "Loading account selections...";
  const isAnyModalOpen = isScheduleModalOpen || isEstimateNumberModalOpen || isStationModalOpen || isScheduleUploadOpen;

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-6 pb-12">
      <AppHeader />
      <HeroBanner
        action={
          <Button onClick={openCreateAccountModal} className="min-w-40" disabled={!canEditTradsphere}>
            Add Account
          </Button>
        }
      />

      <AccountSelector
        selectedAccountCode={selectedAccountCode}
        options={accountSelections}
        isLoadingSelections={isLoadingSelections}
        selectionsError={selectionsError}
        isLoadingAccount={isLoadingAccount}
        isRefreshingAccount={isRefreshingAccount}
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
              canEdit={canEditTradsphere}
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
              <ScheduleTimelineSection
                accountCode={selectedAccountCode}
                esnums={esnums}
                headers={requestHeaders}
                disabled={isLoadingAccount || isSaving}
              />

              <DashboardPanel
                title="EstNums - Schedules"
                icon={<CalendarDays className="size-5 text-blue-600" />}
                searchValue={scheduleSearch}
                onSearchChange={setScheduleSearch}
                actions={
                  <>
                    <ActionIconButton
                      aria-label="Upload schedules"
                      tooltip="Upload schedules"
                      onClick={() => {
                        setScheduleUploadSuccessMessage(null);
                        setIsScheduleUploadOpen(true);
                      }}
                      disabled={!canEditTradsphere || isLoadingAccount || isSaving}
                      icon={<CloudUpload />}
                    />
                    <ActionIconButton
                      aria-label="Add estimate"
                      tooltip="Add estimate"
                      onClick={openEstimateCreateModal}
                      disabled={!canEditTradsphere || !selectedAccountCode || isLoadingAccount || isSaving}
                      icon={<Plus />}
                    />
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
                      onClick={() => openScheduleModal(esnum)}
                      onEditEstimate={() => openEstimateEditModal(esnum)}
                      disabled={!selectedAccountCode || isLoadingAccount || isSaving}
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
                  <ActionIconButton
                    aria-label="Add station"
                    tooltip="Add station"
                    icon={<Plus />}
                    onClick={openStationCreateModal}
                    disabled={!canEditTradsphere || !selectedAccountCode || isLoadingAccount || isSaving}
                  />
                }
              >
                <div className="flex gap-3 overflow-x-auto pb-2">
                  {filteredStations.map((station) => (
                    <StationCard
                      key={station.code}
                      station={station}
                      onClick={() => openStationEditModal(station)}
                      disabled={!selectedAccountCode || isLoadingAccount || isSaving}
                    />
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

      {pageCacheStatusText && !isAnyModalOpen ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40">
          <div
            className={`mx-4 sm:mx-6 lg:mr-6 ${sidebarVisuallyExpanded ? "lg:ml-[18.75rem]" : "lg:ml-[6.5rem]"}`}
          >
            <div className="mx-auto w-full max-w-[1600px]">
              <CacheStatusChip
                text={pageCacheStatusText}
                onRefresh={() => {
                  void handleRefreshAccount();
                }}
                disabled={!selectedAccountCode || isLoadingAccount || isRefreshingAccount || isSaving}
                refreshing={isRefreshingAccount}
                refreshLabel="Refresh data"
                tooltipText="Click to refresh data"
                containerClassName="pointer-events-auto"
                className="max-w-[min(90vw,32rem)]"
              />
            </div>
          </div>
        </div>
      ) : null}

      <ScheduleUploadDialog
        open={isScheduleUploadOpen}
        onOpenChange={setIsScheduleUploadOpen}
        uploadUrl={SCHEDULE_IMPORT_URL}
        headers={requestHeaders}
        onUploadSuccess={(fileName) => {
          setScheduleUploadSuccessMessage(`Upload completed for "${fileName}".`);
          setInvalidatedScheduleEstnum(null);
          setScheduleCacheInvalidationToken((current) => current + 1);
          removeBrowserCacheByPrefix(`schedule-table:${selectedAccountCode.toUpperCase()}:`);
          void loadAccountDashboard("network-only");
        }}
      />

      <Dialog open={isCreateAccountModalOpen} onOpenChange={handleCreateAccountDialogOpenChange}>
        <DialogContent
          className="max-w-[620px] rounded-xl bg-white p-6"
          onEscapeKeyDown={(event) => {
            if (isCreatingAccount) {
              event.preventDefault();
            }
          }}
          onInteractOutside={(event) => {
            if (
              shouldBlockOutsideClose({
                isBusy: isCreatingAccount,
                hasUnsavedChanges: hasCreateAccountUnsavedChanges(),
              })
            ) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close create account modal"
            disabled={isCreatingAccount}
          >
            <X className="size-4" />
          </DialogClose>

          <DialogHeader>
            <DialogTitle>Create Account</DialogTitle>
            <DialogDescription>Add a TradSphere account mapping for an existing master account code.</DialogDescription>
          </DialogHeader>

          <div className="mt-4 space-y-4">
            <LabeledField
              label={
                <>
                  Account Code<RequiredMark />
                </>
              }
            >
              <Input
                value={createAccountForm.accountCode}
                onChange={(event) => {
                  const value = event.target.value.toUpperCase();
                  setCreateAccountForm((current) => ({ ...current, accountCode: value }));
                  if (createAccountError) {
                    setCreateAccountError(null);
                  }
                }}
                placeholder="e.g. TAAA"
                disabled={isCreatingAccount}
              />
            </LabeledField>

            <AccountEditableFields
              billingType={createAccountForm.billingType}
              market={createAccountForm.market}
              note={createAccountForm.note}
              onBillingTypeChange={(value) => {
                setCreateAccountForm((current) => ({ ...current, billingType: value }));
              }}
              onMarketChange={(value) => {
                setCreateAccountForm((current) => ({ ...current, market: value }));
                if (createAccountError) {
                  setCreateAccountError(null);
                }
              }}
              onNoteChange={(value) => {
                setCreateAccountForm((current) => ({ ...current, note: value }));
                if (createAccountError) {
                  setCreateAccountError(null);
                }
              }}
              disabled={isCreatingAccount}
              billingAriaLabel="Create account billing type"
              marketPlaceholder="e.g. Los Angeles"
              notePlaceholder="Optional note"
            />

            {createAccountError ? <p className="text-sm text-rose-600">{createAccountError}</p> : null}

            <DialogFooter>
              {shouldShowCreateAccountSubmit ? (
                <Button onClick={handleCreateAccount} disabled={!canCreateAccount || isCreatingAccount}>
                  {isCreatingAccount ? (
                    <>
                      <Loader2 className="size-4 animate-spin" />
                      Creating
                    </>
                  ) : (
                    "Create Account"
                  )}
                </Button>
              ) : null}
            </DialogFooter>
          </div>
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isCreateAccountUnsavedDialogOpen}
        onKeepEditing={() => setIsCreateAccountUnsavedDialogOpen(false)}
        onDiscardChanges={() => {
          setIsCreateAccountUnsavedDialogOpen(false);
          setIsCreateAccountModalOpen(false);
          resetCreateAccountForm();
        }}
      />

      <EstimateNumberModal
        open={isEstimateNumberModalOpen}
        onOpenChange={(nextOpen) => {
          setIsEstimateNumberModalOpen(nextOpen);
          if (!nextOpen) {
            setEstimateModalInitialData(null);
            setEstimateModalMode("create");
          }
        }}
        mode={estimateModalMode}
        canEdit={canEditTradsphere}
        initialData={estimateModalInitialData}
        accountCode={selectedAccountCode}
        accountName={
          accountForm?.name ||
          accountSelections.find((option) => option.accountCode === selectedAccountCode)?.name ||
          ""
        }
        headers={requestHeaders}
        onSuccess={handleEstimateNumberSaved}
      />

      <StationModal
        open={isStationModalOpen}
        onOpenChange={(nextOpen) => {
          setIsStationModalOpen(nextOpen);
          if (!nextOpen) {
            setStationModalMode("create");
            setStationModalCode(null);
          }
        }}
        mode={stationModalMode}
        canEdit={canEditTradsphere}
        stationCode={stationModalCode}
        stationCatalog={stations}
        headers={requestHeaders}
        onSuccess={handleStationSaved}
      />

      <ScheduleModal
        open={isScheduleModalOpen}
        onOpenChange={(nextOpen) => {
          setIsScheduleModalOpen(nextOpen);
          if (!nextOpen) {
            setSelectedScheduleEstnum(null);
          }
        }}
        selectedEstnum={selectedScheduleEstnum}
        accountCode={selectedAccountCode}
        billingType={accountForm?.billingType}
        headers={requestHeaders}
        cacheInvalidationToken={scheduleCacheInvalidationToken}
        invalidatedEstnum={invalidatedScheduleEstnum}
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
      deliveryMethodId: asNumber(item.deliveryMethodId ?? item.delivery_method_id),
      mediaType: asNullableString(item.mediaType),
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
      deliveryMethodId: asNumber(item.deliveryMethodId ?? item.delivery_method_id),
      mediaType: asNullableString(item.mediaType),
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

function getLoadCacheKey(accountCode: string): string {
  return `tradsphere:main:load:${accountCode.toUpperCase()}:v2`;
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

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return fallback;
}

function RequiredMark() {
  return <span className="pl-1 text-rose-600">*</span>;
}

async function saveAccountChanges(
  requestJson: (url: string, options?: ApiRequestOptions) => Promise<unknown>,
  account: AccountInfo,
  headers: HeadersInit,
): Promise<void> {
  const payload = {
    accountCode: account.code,
    billingType: account.billingType || "Calendar",
    market: account.market ?? "",
    note: account.note ?? "",
  };

  await requestJson("/api/tradsphere/v1/accounts", {
    method: "PUT",
    headers,
    body: payload,
    successToast: {
      title: "Account saved",
      message: `Account ${account.code} was updated successfully.`,
    },
    errorToast: {
      title: "Save failed",
    },
  });
}

export default App;
