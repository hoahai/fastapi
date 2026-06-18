import { useCallback, useEffect, useMemo, useRef, useState, type UIEvent } from "react";
import {
  type ColumnDef,
  type FilterFn,
  getCoreRowModel,
  getFilteredRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { CalendarDays, CloudUpload, Loader2, Monitor, Plus, X } from "lucide-react";

import { AccountInformationCard } from "@/components/dashboard/AccountInformationCard";
import { ActionIconButton } from "@/components/dashboard/ActionIconButton";
import { AccountSelector } from "@/components/dashboard/AccountSelector";
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
  ApiMainLoadResponse,
  EsnumItem,
  MainLoadResponse,
  StationItem,
} from "@/components/dashboard/types";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { useToast } from "@/components/ui/toast";
import { useApiRequest, type ApiRequestOptions } from "@/hooks/useApiRequest";
import { useTradsphereAccountSelections } from "@/hooks/useTradsphereAccountSelections";
import { useOnlineStatus } from "@/hooks/useOnlineStatus";
import { useDirtyRefreshGuard } from "@/hooks/useDirtyRefreshGuard";
import {
  readBrowserCacheSnapshot,
  removeBrowserCacheByPrefix,
  writeBrowserCache,
} from "@/lib/browserCache";
import { buildAuthHeaders as buildSharedAuthHeaders } from "@shared/api/authHeaders";
import { TRADSPHERE_CACHE_TTL_MS, shouldFetchNetwork, type CachePolicy } from "@shared/cache";
import { useAuth } from "@shared/auth/useAuth";
import { shouldProtectFrontendAuth } from "@shared/auth/guards";
import { hasAppEditAccess } from "@shared/auth/permissions";
import { useScopedPersistentState } from "@shared/hooks/useScopedPersistentState";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { ModalCloseButton, ModalHeaderRow, ModalShell } from "@shared/components";
import { PageLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import { resolveCriteriaLoadPlan } from "@shared/hooks/useCriteriaLoadPolicy";

const scheduleColumns: ColumnDef<EsnumItem>[] = [{ accessorKey: "estnum" }, { accessorKey: "name" }];

const stationColumns: ColumnDef<StationItem>[] = [{ accessorKey: "code" }, { accessorKey: "name" }];

const LOAD_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.MAIN_LOAD;
const SCHEDULE_IMPORT_URL = "/api/tradsphere/v1/schedules/import/file?skipBlankLines=false";
const TRADSPHERE_ACCOUNTS_PAGE_CODE = "home";
const HOME_SELECTED_ACCOUNT_STATE_KEY = "selectedAccountCode";
const HOME_SCHEDULE_SEARCH_STATE_KEY = "scheduleSearch";
const HOME_STATION_SEARCH_STATE_KEY = "stationSearch";
const HOME_HAS_LOADED_DASHBOARD_STATE_KEY = "hasLoadedDashboard";
const ESTNUM_BATCH_SIZE = 25;
const ESTNUM_SCROLL_END_THRESHOLD_PX = 24;

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

type AccountCreationLookup = {
  accountCode: string;
  accountName: string;
  logoUrl: string | null;
  existsInTradSphere: boolean;
  tradSphereAccountCode: string | null;
};

type CreateAccountLookupState =
  | { status: "idle" }
  | { status: "checking"; accountCode: string }
  | { status: "available"; lookup: AccountCreationLookup }
  | { status: "duplicate"; lookup: AccountCreationLookup }
  | { status: "missing"; accountCode: string; message: string }
  | { status: "error"; accountCode: string; message: string };

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

function App() {
  const toast = useToast();
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const { isOnline } = useOnlineStatus();
  const [selectedAccountCode, setSelectedAccountCode] = useScopedPersistentState<string>(
    {
      appCode: "tradsphere",
      pageCode: TRADSPHERE_ACCOUNTS_PAGE_CODE,
      stateKey: HOME_SELECTED_ACCOUNT_STATE_KEY,
    },
    "",
    { validate: (value: unknown): value is string => typeof value === "string" },
  );

  const [isLoadingAccount, setIsLoadingAccount] = useState(false);
  const [isRefreshingAccount, setIsRefreshingAccount] = useState(false);
  const [isLoadActionOverlayVisible, setIsLoadActionOverlayVisible] = useState(false);
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [hasLoadedDashboard, setHasLoadedDashboard] = useScopedPersistentState<boolean>(
    {
      appCode: "tradsphere",
      pageCode: TRADSPHERE_ACCOUNTS_PAGE_CODE,
      stateKey: HOME_HAS_LOADED_DASHBOARD_STATE_KEY,
    },
    false,
    { validate: (value: unknown): value is boolean => typeof value === "boolean" },
  );
  const [dashboardCacheStatus, setDashboardCacheStatus] = useState<CacheStatus | null>(null);

  const [accountOriginal, setAccountOriginal] = useState<AccountInfo | null>(null);
  const [accountForm, setAccountForm] = useState<AccountInfo | null>(null);

  const [esnums, setEsnums] = useState<EsnumItem[]>([]);
  const [stations, setStations] = useState<StationItem[]>([]);

  const [scheduleSearch, setScheduleSearch] = useScopedPersistentState<string>(
    {
      appCode: "tradsphere",
      pageCode: TRADSPHERE_ACCOUNTS_PAGE_CODE,
      stateKey: HOME_SCHEDULE_SEARCH_STATE_KEY,
    },
    "",
    { validate: (value: unknown): value is string => typeof value === "string" },
  );
  const [stationSearch, setStationSearch] = useScopedPersistentState<string>(
    {
      appCode: "tradsphere",
      pageCode: TRADSPHERE_ACCOUNTS_PAGE_CODE,
      stateKey: HOME_STATION_SEARCH_STATE_KEY,
    },
    "",
    { validate: (value: unknown): value is string => typeof value === "string" },
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
  const [createAccountLookupState, setCreateAccountLookupState] = useState<CreateAccountLookupState>({
    status: "idle",
  });
  const createAccountLookupRequestIdRef = useRef(0);
  const [isEstimateNumberModalOpen, setIsEstimateNumberModalOpen] = useState(false);
  const [estimateModalMode, setEstimateModalMode] = useState<EstimateNumberModalMode>("create");
  const [estimateModalInitialData, setEstimateModalInitialData] = useState<EstimateNumberModalData | null>(null);
  const [isStationModalOpen, setIsStationModalOpen] = useState(false);
  const [stationModalMode, setStationModalMode] = useState<StationModalMode>("create");
  const [stationModalCode, setStationModalCode] = useState<string | null>(null);
  const [dashboardDeferredMessage, setDashboardDeferredMessage] = useState<string | null>(null);
  const requestHeaders = useMemo(
    () => buildSharedAuthHeaders(auth.session, auth.tenantSlug, false),
    [auth.session, auth.tenantSlug],
  );
  const canEditTradsphere = useMemo(() => {
    if (!shouldProtectFrontendAuth()) {
      return true;
    }
    return hasAppEditAccess(auth.accessProfile, "tradsphere");
  }, [auth.accessProfile]);
  const {
    accountSelections,
    isLoadingSelections,
    isRefreshingSelections,
    selectionsError,
    isOfflineSelections,
    selectionsCacheStatus,
    refreshSelections,
  } = useTradsphereAccountSelections({
    requestJson,
    requestHeaders,
    loadErrorMessage: "Unable to load account selections.",
  });

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
  const [visibleScheduleCount, setVisibleScheduleCount] = useState(ESTNUM_BATCH_SIZE);
  const scheduleListRef = useRef<HTMLDivElement | null>(null);
  const visibleSchedules = useMemo(
    () => filteredSchedules.slice(0, visibleScheduleCount),
    [filteredSchedules, visibleScheduleCount],
  );
  const canLoadMoreSchedules = visibleScheduleCount < filteredSchedules.length;

  const loadMoreSchedules = useCallback(() => {
    setVisibleScheduleCount((current) => Math.min(filteredSchedules.length, current + ESTNUM_BATCH_SIZE));
  }, [filteredSchedules.length]);

  const handleScheduleListScroll = useCallback(
    (event: UIEvent<HTMLDivElement>) => {
      if (!canLoadMoreSchedules || isLoadingAccount || isSaving) {
        return;
      }
      const container = event.currentTarget;
      const reachedEnd =
        container.scrollLeft + container.clientWidth >= container.scrollWidth - ESTNUM_SCROLL_END_THRESHOLD_PX;
      if (reachedEnd) {
        loadMoreSchedules();
      }
    },
    [canLoadMoreSchedules, isLoadingAccount, isSaving, loadMoreSchedules],
  );

  useEffect(() => {
    setVisibleScheduleCount(Math.min(ESTNUM_BATCH_SIZE, filteredSchedules.length));
    if (scheduleListRef.current) {
      scheduleListRef.current.scrollLeft = 0;
    }
  }, [selectedAccountCode, scheduleSearch, filteredSchedules.length]);

  const hasEditableChanges = Boolean(
    accountOriginal &&
      accountForm &&
      (accountOriginal.billingType !== accountForm.billingType ||
        (accountOriginal.market ?? "") !== (accountForm.market ?? "") ||
        (accountOriginal.note ?? "") !== (accountForm.note ?? "") ||
        Boolean(accountOriginal.active) !== Boolean(accountForm.active)),
  );
  const hasBlockingLocalEdits = hasEditableChanges || isEstimateNumberModalOpen || isStationModalOpen || isCreateAccountModalOpen;
  const {
    hasDeferredUpdate: hasDeferredDashboardUpdate,
    beginRequest: beginDashboardLoadRequest,
    invalidateRequests: invalidateDashboardLoadRequests,
    isLatestRequest: isLatestDashboardLoadRequest,
    applyFromRequest: applyDashboardLoadFromRequest,
    clearDeferredUpdate: clearDeferredDashboardUpdate,
  } = useDirtyRefreshGuard(hasBlockingLocalEdits);

  function resetLoadedDashboardData() {
    setAccountOriginal(null);
    setAccountForm(null);
    setEsnums([]);
    setStations([]);
  }

  function applyLoadedData(data: MainLoadResponse) {
    setAccountOriginal(data.account);
    setAccountForm(data.account);
    setEsnums(data.esnums);
    setStations(data.stations);
  }

  function handleAccountChange(value: string) {
    invalidateDashboardLoadRequests();
    clearDeferredDashboardUpdate();
    setDashboardDeferredMessage(null);
    setSelectedAccountCode(value.toUpperCase());
    setLoadError(null);
    setSaveError(null);
    setDashboardCacheStatus(null);
    setIsRefreshingAccount(false);
    setIsLoadActionOverlayVisible(false);
    setIsChipRefreshOverlayVisible(false);
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

  function applySelections(selections: typeof accountSelections) {
    if (!selections.length) {
      setHasLoadedDashboard(false);
      resetLoadedDashboardData();
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
    if (isLoadingSelections) {
      return;
    }
    applySelections(accountSelections);
  }, [accountSelections, isLoadingSelections, selectedAccountCode]);

  useEffect(() => {
    if (!selectedAccountCode || !hasLoadedDashboard || accountOriginal) {
      return;
    }
    const loadCacheKey = getLoadCacheKey(selectedAccountCode);
    const cacheSnapshot = readBrowserCacheSnapshot<unknown>(loadCacheKey);
    const cachedDashboard = normalizeCachedMainLoadResponse(cacheSnapshot?.data);
    if (!cachedDashboard || !shouldUseCachedLoad(cachedDashboard, selectedAccountCode)) {
      setHasLoadedDashboard(false);
      return;
    }
    applyLoadedData(cachedDashboard);
    setDashboardCacheStatus({
      source: "cache",
      fetchedAt: cacheSnapshot?.fetchedAt ?? Date.now(),
    });
  }, [accountOriginal, hasLoadedDashboard, selectedAccountCode, setHasLoadedDashboard]);

  useEffect(() => {
    if (!hasDeferredDashboardUpdate) {
      setDashboardDeferredMessage(null);
      return;
    }
    if (hasBlockingLocalEdits) {
      setDashboardDeferredMessage("Newer dashboard data is ready and will apply after current edits are finished.");
      return;
    }
    setDashboardDeferredMessage(null);
  }, [hasBlockingLocalEdits, hasDeferredDashboardUpdate]);

  async function loadAccountDashboard(
    policy: CachePolicy,
  ): Promise<{ success: boolean; source: "cache" | "network" | null }> {
    if (!selectedAccountCode || isLoadingAccount || isSaving) {
      return { success: false, source: null };
    }
    const requestId = beginDashboardLoadRequest();

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

    if (!isOnline) {
      if (canUseCachedData) {
        return { success: true, source: "cache" };
      }
      setLoadError("You're offline. Dashboard data will load when connection is restored.");
      setIsLoadingAccount(false);
      setIsRefreshingAccount(false);
      return { success: false, source: null };
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

      if (!isLatestDashboardLoadRequest(requestId)) {
        return { success: false, source: null };
      }
      const fetchedAt = Date.now();
      writeBrowserCache(loadCacheKey, data, LOAD_CACHE_TTL_MS, { source: "network", fetchedAt });
      applyDashboardLoadFromRequest(
        requestId,
        () => {
          applyLoadedData(data);
          setHasLoadedDashboard(true);
          setDashboardCacheStatus({
            source: "network",
            fetchedAt,
          });
          setDashboardDeferredMessage(null);
          setLoadError(null);
        },
        { deferWhenDirty: true },
      );
      return { success: true, source: "network" };
    } catch (error) {
      if (!isLatestDashboardLoadRequest(requestId)) {
        return { success: false, source: null };
      }
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
      if (isLatestDashboardLoadRequest(requestId)) {
        setIsLoadingAccount(false);
        setIsRefreshingAccount(false);
      }
    }
  }

  async function handleLoadAccount() {
    setIsLoadActionOverlayVisible(true);
    const loadPlan = resolveCriteriaLoadPlan({
      trigger: "load-button",
      criteriaKey: selectedAccountCode,
      loadedCriteriaKey: accountOriginal?.code,
    });
    try {
      const result = await loadAccountDashboard(loadPlan.shouldIgnoreCache ? "network-only" : "cache-first");
      if (result.success && selectedAccountCode) {
        if (loadPlan.isSameCriteria) {
          toast.success("Dashboard refreshed", `Fetched fresh data for ${selectedAccountCode}.`);
        } else {
          const suffix = result.source === "cache" ? " from cache." : ".";
          toast.success("Account loaded", `Loaded dashboard for ${selectedAccountCode}${suffix}`);
        }
      }
    } finally {
      setIsLoadActionOverlayVisible(false);
    }
  }

  async function handleRefreshAccount(): Promise<void> {
    if (!selectedAccountCode) {
      return;
    }
    setIsChipRefreshOverlayVisible(true);
    try {
      const refreshPlan = resolveCriteriaLoadPlan({
        trigger: "cache-chip",
        criteriaKey: selectedAccountCode,
        loadedCriteriaKey: accountOriginal?.code,
      });
      await refreshSelections();
      const result = await loadAccountDashboard(refreshPlan.shouldIgnoreCache ? "network-only" : "cache-first");
      if (result.success) {
        toast.success("Dashboard refreshed", `Fetched fresh data for ${selectedAccountCode}.`);
      }
    } finally {
      setIsChipRefreshOverlayVisible(false);
    }
  }

  async function handleSaveAccount() {
    if (!canEditTradsphere || !accountForm || isSaving || isLoadingAccount) {
      return;
    }

    invalidateDashboardLoadRequests();
    clearDeferredDashboardUpdate();
    setDashboardDeferredMessage(null);
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
    createAccountLookupRequestIdRef.current += 1;
    setCreateAccountLookupState({ status: "idle" });
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

  async function lookupCreateAccountCode(accountCodeInput: string): Promise<void> {
    const normalizedAccountCode = accountCodeInput.trim().toUpperCase();
    if (!normalizedAccountCode) {
      createAccountLookupRequestIdRef.current += 1;
      setCreateAccountLookupState({ status: "idle" });
      return;
    }

    const requestId = createAccountLookupRequestIdRef.current + 1;
    createAccountLookupRequestIdRef.current = requestId;
    setCreateAccountError(null);
    setCreateAccountLookupState({ status: "checking", accountCode: normalizedAccountCode });

    try {
      const payload = await requestJson(
        `/api/tradsphere/v1/ui/accounts/create-lookup?accountCode=${encodeURIComponent(normalizedAccountCode)}`,
        {
          headers: requestHeaders,
          errorToast: false,
        },
      );
      if (createAccountLookupRequestIdRef.current !== requestId) {
        return;
      }

      const lookup = parseAccountCreationLookup(payload);
      if (!lookup) {
        setCreateAccountLookupState({
          status: "error",
          accountCode: normalizedAccountCode,
          message: "Unable to verify the selected account code.",
        });
        return;
      }

      setCreateAccountLookupState(
        lookup.existsInTradSphere
          ? { status: "duplicate", lookup }
          : { status: "available", lookup },
      );
    } catch (error) {
      if (createAccountLookupRequestIdRef.current !== requestId) {
        return;
      }
      const message = getErrorMessage(error, "");
      const isMissingAccount = message.toLowerCase().includes("unknown master accountcode values");
      setCreateAccountLookupState({
        status: isMissingAccount ? "missing" : "error",
        accountCode: normalizedAccountCode,
        message: isMissingAccount
          ? `No master account was found for ${normalizedAccountCode}.`
          : getCreateAccountLookupErrorMessage(error, normalizedAccountCode),
      });
    }
  }

  async function handleCreateAccount() {
    if (!canEditTradsphere) {
      return;
    }
    if (createAccountLookupState.status !== "available") {
      setCreateAccountError("Verify a master account code before creating the TradSphere account.");
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

      await refreshSelections();
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
  const canCreateAccount = createAccountLookupState.status === "available" && !createAccountValidationError;
  const shouldShowCreateAccountSubmit = createAccountLookupState.status === "available" || isCreatingAccount;
  const createAccountLookupMessage = getCreateAccountLookupMessage(createAccountLookupState);

  const selectionsStatusText = isLoadingSelections
    ? "Loading..."
    : isRefreshingSelections
      ? "Refreshing..."
      : selectionsCacheStatus
        ? `Selections source: ${selectionsCacheStatus.source}. Last updated ${formatRelativeTime(selectionsCacheStatus.fetchedAt)}.`
        : isOfflineSelections
          ? "Offline. No cached selections yet."
          : null;
  const dashboardStatusText = isLoadingAccount
    ? "Loading..."
    : isRefreshingAccount
      ? "Refreshing..."
      : dashboardCacheStatus
        ? `Data source: ${dashboardCacheStatus.source}. Last updated ${formatRelativeTime(dashboardCacheStatus.fetchedAt)}.`
        : !isOnline && hasLoadedDashboard
          ? "Offline. Showing the last available dashboard snapshot."
          : null;
  const pageCacheStatusText = dashboardStatusText ?? selectionsStatusText;
  const shouldBlockForSelectionsLoad = isLoadingSelections && accountSelections.length === 0;
  const shouldBlockForAccountLoad = isLoadingAccount && !hasLoadedDashboard;
  const pageLoadingContract = resolveSharedLoadingContract(
    {
      pageInitializing: shouldBlockForSelectionsLoad || shouldBlockForAccountLoad,
      pageRefreshing: isSaving || isLoadActionOverlayVisible || isRefreshingAccount || isLoadingAccount,
      cacheChipRefreshing: isChipRefreshOverlayVisible,
    },
    {
      pageInitializing: shouldBlockForAccountLoad
        ? "Loading account dashboard..."
        : "Loading account selections...",
      pageRefreshing: isSaving
        ? "Saving account changes..."
        : "Refreshing account dashboard...",
      cacheChipRefreshing: "Refreshing account dashboard...",
    },
  );
  const pageMessages: StackMessage[] = [];
  if (loadError) {
    pageMessages.push({
      id: "account-load-error",
      variant: "error",
      message: loadError,
    });
  }
  if (dashboardDeferredMessage) {
    pageMessages.push({
      id: "account-deferred-refresh",
      variant: "warning",
      message: dashboardDeferredMessage,
    });
  }

  return (
    <AppPageLayout
      className="gap-6"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <HeroBanner
          action={(
            <Button onClick={openCreateAccountModal} className="min-w-40" disabled={!canEditTradsphere}>
              Add Account
            </Button>
          )}
        />
      )}
      footer={pageCacheStatusText ? (
        <PageCacheFooter
          text={pageCacheStatusText}
          onRefresh={() => {
            void handleRefreshAccount();
          }}
          disabled={
            !selectedAccountCode ||
            !isOnline ||
            isLoadingAccount ||
            isRefreshingSelections ||
            isRefreshingAccount ||
            isChipRefreshOverlayVisible ||
            isSaving
          }
          refreshing={isRefreshingSelections || isRefreshingAccount || isChipRefreshOverlayVisible}
          refreshLabel="Refresh data"
          tooltipText={isOnline ? "Click to refresh data" : "Offline. Reconnect to refresh data."}
          containerClassName="w-full"
        />
      ) : null}
    >

      <SectionCard title="Account & Load" divider={false} contentClassName="pt-1">
        <AccountSelector
          selectedAccountCode={selectedAccountCode}
          options={accountSelections}
          isLoadingSelections={isLoadingSelections}
          selectionsError={selectionsError}
          isLoadingAccount={isLoadingAccount}
          isRefreshingAccount={isRefreshingSelections || isRefreshingAccount}
          isSavingAccount={isSaving}
          onAccountChange={handleAccountChange}
          onLoad={handleLoadAccount}
        />
      </SectionCard>

      {hasLoadedDashboard ? (
        <>
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
              onActiveChange={(active) =>
                setAccountForm((current) => (current ? { ...current, active } : current))
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
                icon={<CalendarDays className="size-5 text-blue-700" />}
                searchValue={scheduleSearch}
                onSearchChange={setScheduleSearch}
                actions={
                  <>
                    <ActionIconButton
                      aria-label="Upload schedules"
                      tooltip="Upload schedules"
                      onClick={() => {
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
                <div
                  ref={scheduleListRef}
                  onScroll={handleScheduleListScroll}
                  className="flex gap-3 overflow-x-auto pb-2"
                >
                  {visibleSchedules.map((esnum) => (
                    <ScheduleCard
                      key={esnum.estnum}
                      esnum={esnum}
                      onClick={() => openScheduleModal(esnum)}
                      onEditEstimate={() => openEstimateEditModal(esnum)}
                      disabled={!selectedAccountCode || isLoadingAccount || isSaving}
                    />
                  ))}
                </div>
                {canLoadMoreSchedules ? (
                  <p className="flex items-center gap-2 text-xs text-slate-500">
                    <Loader2 className="size-3.5 animate-spin text-blue-600" />
                    Showing {visibleSchedules.length} of {filteredSchedules.length}. Scroll right to load more.
                  </p>
                ) : null}
                {!filteredSchedules.length ? (
                  <p className="text-sm text-slate-500">No EstNums or schedules were returned for this account.</p>
                ) : null}
              </DashboardPanel>

              <DashboardPanel
                title="Stations"
                icon={<Monitor className="size-5 text-blue-700" />}
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

      <ScheduleUploadDialog
        open={isScheduleUploadOpen}
        onOpenChange={setIsScheduleUploadOpen}
        uploadUrl={SCHEDULE_IMPORT_URL}
        headers={requestHeaders}
        onUploadSuccess={(fileName) => {
          toast.success("Upload completed", `Upload completed for "${fileName}".`);
          setInvalidatedScheduleEstnum(null);
          setScheduleCacheInvalidationToken((current) => current + 1);
          removeBrowserCacheByPrefix(`schedule-table:${selectedAccountCode.toUpperCase()}:`);
          void loadAccountDashboard("network-only");
        }}
      />

      <Dialog open={isCreateAccountModalOpen} onOpenChange={handleCreateAccountDialogOpenChange}>
        <DialogContent
          className="flex max-h-[90vh] max-w-[620px] flex-col overflow-hidden rounded-xl bg-white p-6"
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
          <ModalShell busy={isCreatingAccount} busyMessage="Creating account..." className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close create account modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>Create Account</DialogTitle>
                <DialogDescription>Add a TradSphere account mapping for an existing master account code.</DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>

            <div className="mt-4 space-y-4">
              <LabeledField
                alignStart
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
                    setCreateAccountLookupState({ status: "idle" });
                    if (createAccountError) {
                      setCreateAccountError(null);
                    }
                  }}
                  placeholder="e.g. TAAA"
                  onBlur={() => {
                    void lookupCreateAccountCode(createAccountForm.accountCode);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      void lookupCreateAccountCode(createAccountForm.accountCode);
                    }
                  }}
                  disabled={isCreatingAccount}
                />
                <div className="mt-1 text-xs leading-5" aria-live="polite">
                  {createAccountLookupState.status === "checking" ? (
                    <div className="flex items-center gap-2 text-slate-500">
                      <Loader2 className="size-3.5 animate-spin text-slate-400" />
                      <span>{`Checking ${createAccountLookupState.accountCode} against master Accounts...`}</span>
                    </div>
                  ) : null}
                  {renderCreateAccountLookupPreview(createAccountLookupState)}
                  {createAccountLookupMessage ? (
                    <p
                      className={
                        createAccountLookupMessage.tone === "success"
                          ? "text-emerald-700"
                          : createAccountLookupMessage.tone === "warning"
                            ? "text-amber-700"
                            : createAccountLookupMessage.tone === "error"
                              ? "text-rose-600"
                              : "text-slate-500"
                      }
                    >
                      {createAccountLookupMessage.text}
                    </p>
                  ) : null}
                </div>
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
                disabled={isCreatingAccount || createAccountLookupState.status !== "available"}
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
          </ModalShell>
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

      <PageLoadingLayer active={pageLoadingContract.pageOverlayActive} message={pageLoadingContract.pageOverlayMessage} />
    </AppPageLayout>
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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

function asBoolean(value: unknown, defaultValue = false): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) {
      return true;
    }
    if (["0", "false", "no", "n", "off"].includes(normalized)) {
      return false;
    }
  }
  return defaultValue;
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
      active: asBoolean(apiResponse.data.account.active, true),
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
      active: asBoolean(accountRow.active, true),
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
        active: asBoolean(accountRow.active, true),
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
  return `tradsphere:main:load:${accountCode.toUpperCase()}:v3`;
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

function parseAccountCreationLookup(payload: unknown): AccountCreationLookup | null {
  if (!isRecord(payload)) {
    return null;
  }

  const data = isRecord(payload.data) ? payload.data : payload;
  if (!isRecord(data)) {
    return null;
  }

  const accountCode = asString(data.accountCode).toUpperCase();
  const accountName = asString(data.accountName);
  if (!accountCode || !accountName) {
    return null;
  }

  return {
    accountCode,
    accountName,
    logoUrl: asNullableString(data.logoUrl),
    existsInTradSphere: asBoolean(data.existsInTradSphere),
    tradSphereAccountCode: asNullableString(data.tradSphereAccountCode)?.toUpperCase() || null,
  };
}

function getCreateAccountLookupErrorMessage(error: unknown, accountCode: string): string {
  const message = getErrorMessage(error, "").trim();
  if (!message) {
    return `No master account was found for ${accountCode}.`;
  }
  if (message.toLowerCase().includes("unknown master accountcode values")) {
    return `No master account was found for ${accountCode}.`;
  }
  return message;
}

function getCreateAccountLookupMessage(
  state: CreateAccountLookupState,
): { text: string; tone: "neutral" | "success" | "warning" | "error" } | null {
  switch (state.status) {
    case "idle":
      return null;
    case "available":
      return {
        text: `Found master account: ${state.lookup.accountName}. You can add the TradSphere mapping below.`,
        tone: "success",
      };
    case "duplicate":
      return {
        text: `Found master account: ${state.lookup.accountName}, but it already exists in TradSphere.`,
        tone: "warning",
      };
    case "missing":
    case "error":
      return {
        text: state.message,
        tone: "error",
      };
    default:
      return null;
  }
}

function renderCreateAccountLookupPreview(state: CreateAccountLookupState) {
  if (state.status !== "available" && state.status !== "duplicate") {
    return null;
  }

  const lookup = state.lookup;
  return (
    <div className="mt-1 flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-2.5 py-2">
      <div className="flex size-8 shrink-0 items-center justify-center overflow-hidden rounded-md bg-white ring-1 ring-slate-200">
        {lookup.logoUrl ? (
          <img
            src={lookup.logoUrl}
            alt={`${lookup.accountName} logo`}
            className="h-full w-full object-contain p-1"
            loading="lazy"
          />
        ) : (
          <span className="text-xs font-semibold text-slate-500">
            {lookup.accountName.slice(0, 2).toUpperCase() || "AC"}
          </span>
        )}
      </div>
      <div className="min-w-0">
        <p className="truncate text-sm font-medium text-slate-900">{lookup.accountName}</p>
      </div>
    </div>
  );
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
    active: Boolean(account.active),
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
