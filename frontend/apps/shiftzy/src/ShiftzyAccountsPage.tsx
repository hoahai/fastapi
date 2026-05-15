import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Plus } from "lucide-react";

import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useToast } from "@/components/ui/toast";
import { useApiRequest } from "@/hooks/useApiRequest";
import { hasAppEditAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { readCacheSnapshot, setCacheData, type CacheSource } from "@shared/cache";
import { CacheStatusChip } from "@shared/components/status/CacheStatusChip";
import { PageLoadingOverlay } from "@shared/components/status/LoadingOverlay";
import {
  createShiftzyEmployees,
  deleteShiftzyEmployees,
  fetchShiftzyEmployees,
  fetchShiftzyPositions,
  updateShiftzyEmployees,
  type ShiftzyEmployee,
  type ShiftzyPosition,
} from "@shiftzy/lib/shiftzyApi";

import { ShiftzyAccountEditModal, type ShiftzyAccountFormPayload } from "@shiftzy/components/accounts/ShiftzyAccountEditModal";
import { ShiftzyAccountResults } from "@shiftzy/components/accounts/ShiftzyAccountResults";
import { ShiftzyAccountSearch } from "@shiftzy/components/accounts/ShiftzyAccountSearch";
import type {
  ShiftzyAccountItem,
  ShiftzyAccountSearchFormValues,
  ShiftzyAccountSectionGroup,
} from "@shiftzy/components/accounts/types";

type ResultsState = "loading" | "idle" | "ready" | "empty" | "error";

type CacheStatus = {
  source: CacheSource | "local";
  fetchedAt: number;
};

type ShiftzyEmployeesCachePayload = {
  employees: ShiftzyEmployee[];
  positions: ShiftzyPosition[];
};

type DeactivateTarget = {
  id: string;
  name: string;
};

const SHIFTZY_CACHE_NAMESPACE = "shiftzy:cache:";
const SHIFTZY_EMPLOYEES_CACHE_KEY = "employees:search:v1";
const SHIFTZY_EMPLOYEES_CACHE_TTL_MS = 10 * 60 * 1000;

const INITIAL_SEARCH_FORM: ShiftzyAccountSearchFormValues = {
  name: "",
  scheduleSection: "",
  positionCode: "",
  status: "",
};

function normalizeText(value: string | null | undefined): string {
  return String(value || "").trim().toLowerCase();
}

function normalizeSearchValues(values: ShiftzyAccountSearchFormValues): ShiftzyAccountSearchFormValues {
  return {
    name: String(values.name || "").trim(),
    scheduleSection: String(values.scheduleSection || "").trim(),
    positionCode: String(values.positionCode || "").trim(),
    status: values.status,
  };
}

function isSameSearchValues(
  left: ShiftzyAccountSearchFormValues,
  right: ShiftzyAccountSearchFormValues,
): boolean {
  const a = normalizeSearchValues(left);
  const b = normalizeSearchValues(right);
  return (
    a.name === b.name
    && a.scheduleSection === b.scheduleSection
    && a.positionCode === b.positionCode
    && a.status === b.status
  );
}

function asPositionName(refPositionCode: string | null, positionMap: Map<string, ShiftzyPosition>): string {
  const code = String(refPositionCode || "").trim();
  if (!code) {
    return "-";
  }
  const position = positionMap.get(code);
  return position?.name || code;
}

function toPositionMap(positions: ShiftzyPosition[]): Map<string, ShiftzyPosition> {
  const map = new Map<string, ShiftzyPosition>();
  for (const item of positions) {
    map.set(item.code, item);
  }
  return map;
}

function buildGroups(items: ShiftzyAccountItem[]): ShiftzyAccountSectionGroup[] {
  const sectionMap = new Map<string, { area: string; position: string; items: ShiftzyAccountItem[] }>();
  for (const item of items) {
    const area = String(item.scheduleSection || "").trim() || "Unassigned Area";
    const position = String(item.positionName || "").trim() || "Unassigned Position";
    const key = `${area}::${position}`;
    const bucket = sectionMap.get(key) ?? { area, position, items: [] };
    bucket.items.push(item);
    sectionMap.set(key, bucket);
  }

  return Array.from(sectionMap.entries())
    .sort((left, right) => {
      const areaCompare = left[1].area.localeCompare(right[1].area);
      if (areaCompare !== 0) {
        return areaCompare;
      }
      return left[1].position.localeCompare(right[1].position);
    })
    .map(([key, bucket]) => ({
      key,
      label: bucket.position,
      items: bucket.items.sort((left, right) => {
        if (left.active !== right.active) {
          return left.active ? -1 : 1;
        }
        return left.name.localeCompare(right.name);
      }),
    }));
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

export default function ShiftzyEmployeesPage() {
  const auth = useAuth();
  const requestTokenRef = useRef(0);
  const { requestJson } = useApiRequest();
  const toast = useToast();
  const hasSessionToken = Boolean(auth.session?.accessToken);
  const canEditShiftzy = hasAppEditAccess(auth.accessProfile, "shiftzy");

  const [draft, setDraft] = useState<ShiftzyAccountSearchFormValues>(INITIAL_SEARCH_FORM);
  const [submittedSearch, setSubmittedSearch] = useState<ShiftzyAccountSearchFormValues | null>(null);
  const [employees, setEmployees] = useState<ShiftzyEmployee[]>([]);
  const [positions, setPositions] = useState<ShiftzyPosition[]>([]);
  const [groups, setGroups] = useState<ShiftzyAccountSectionGroup[]>([]);
  const [resultsState, setResultsState] = useState<ResultsState>("loading");
  const [error, setError] = useState<string | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [loadingPage, setLoadingPage] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [backgroundRefreshing, setBackgroundRefreshing] = useState(false);
  const [searching, setSearching] = useState(false);
  const [saving, setSaving] = useState(false);
  const [processingDeactivate, setProcessingDeactivate] = useState(false);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);

  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [editMode, setEditMode] = useState<"create" | "edit">("create");
  const [editingEmployee, setEditingEmployee] = useState<ShiftzyEmployee | null>(null);
  const [deactivateTarget, setDeactivateTarget] = useState<DeactivateTarget | null>(null);

  const positionMap = useMemo(() => toPositionMap(positions), [positions]);

  const sectionOptions = useMemo(() => {
    const seen = new Set<string>();
    const sections: string[] = [];
    for (const item of employees) {
      const section = String(item.scheduleSection || "").trim();
      if (!section || seen.has(section)) {
        continue;
      }
      seen.add(section);
      sections.push(section);
    }
    if (draft.scheduleSection.trim() && !seen.has(draft.scheduleSection.trim())) {
      sections.push(draft.scheduleSection.trim());
    }
    sections.sort((left, right) => left.localeCompare(right));
    return [{ value: "", label: "" }, ...sections.map((item) => ({ value: item, label: item }))];
  }, [draft.scheduleSection, employees]);

  const positionOptions = useMemo(() => {
    return [{ value: "", label: "" }, ...positions.map((item) => ({
      value: item.code,
      label: item.name,
    }))];
  }, [positions]);

  const resultCount = useMemo(() => {
    return groups.reduce((sum, group) => sum + group.items.length, 0);
  }, [groups]);

  const resultText = useMemo(() => {
    if (resultsState === "ready") {
      return `${resultCount} employee${resultCount === 1 ? "" : "s"} found.`;
    }
    if (resultsState === "empty") {
      return "No employees found.";
    }
    return "";
  }, [resultCount, resultsState]);

  const canClearSearch = useMemo(() => {
    return Object.entries(draft).some(([key, value]) => {
      if (key === "status") {
        return value !== "";
      }
      return String(value || "").trim().length > 0;
    });
  }, [draft]);

  const canSubmitSearch = useMemo(() => {
    const applied = submittedSearch ?? INITIAL_SEARCH_FORM;
    return !isSameSearchValues(draft, applied);
  }, [draft, submittedSearch]);

  const cacheStatusText = useMemo(() => {
    if (refreshing || backgroundRefreshing) {
      return "Refreshing Shiftzy employees...";
    }
    if (!cacheStatus) {
      return "Loading Shiftzy employees...";
    }
    return `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [backgroundRefreshing, cacheStatus, refreshing]);

  function getCacheScope(): string {
    return String(auth.tenantSlug || "default").trim().toLowerCase() || "default";
  }

  const applySearch = useCallback((
    criteria: ShiftzyAccountSearchFormValues,
    sourceRows: ShiftzyEmployee[],
    sourcePositionMap: Map<string, ShiftzyPosition>,
  ) => {
    const nameQuery = normalizeText(criteria.name);
    const sectionFilter = String(criteria.scheduleSection || "").trim();
    const positionFilter = String(criteria.positionCode || "").trim();

    const nextItems = sourceRows
      .filter((item) => {
        if (criteria.status === "active" && !item.active) {
          return false;
        }
        if (criteria.status === "inactive" && item.active) {
          return false;
        }
        if (nameQuery && !normalizeText(item.name).includes(nameQuery)) {
          return false;
        }
        if (sectionFilter && item.scheduleSection !== sectionFilter) {
          return false;
        }
        if (positionFilter && String(item.refPositionCode || "") !== positionFilter) {
          return false;
        }
        return true;
      })
      .map<ShiftzyAccountItem>((item) => ({
        ...item,
        positionName: asPositionName(item.refPositionCode, sourcePositionMap),
      }))
      .sort((left, right) => left.name.localeCompare(right.name));

    const nextGroups = buildGroups(nextItems);
    setGroups(nextGroups);
    setResultsState(nextGroups.length ? "ready" : "empty");
    setError(null);
  }, []);

  const loadData = useCallback(async (
    options: {
      refreshing?: boolean;
      criteria?: ShiftzyAccountSearchFormValues | null;
      networkOnly?: boolean;
    } = {},
  ) => {
    if (!hasSessionToken || auth.status !== "authenticated" || !auth.tenantSlug) {
      return;
    }
    const requestToken = ++requestTokenRef.current;
    const nextRefreshing = options.refreshing === true;
    const networkOnly = options.networkOnly === true;
    const criteria = options.criteria ?? null;
    const syncResults = (
      nextCriteria: ShiftzyAccountSearchFormValues | null,
      sourceEmployees: ShiftzyEmployee[],
      sourcePositions: ShiftzyPosition[],
    ) => {
      if (!nextCriteria) {
        setSubmittedSearch(null);
        setGroups([]);
        setResultsState("idle");
        setError(null);
        return;
      }
      setSubmittedSearch(nextCriteria);
      applySearch(nextCriteria, sourceEmployees, toPositionMap(sourcePositions));
    };
    const cacheScope = getCacheScope();
    const cachedSnapshot = networkOnly
      ? null
      : readCacheSnapshot<ShiftzyEmployeesCachePayload>(SHIFTZY_EMPLOYEES_CACHE_KEY, {
        namespace: SHIFTZY_CACHE_NAMESPACE,
        scope: cacheScope,
        storage: "persistent",
        allowExpired: true,
      });
    const cachedData = cachedSnapshot?.data;
    const canUseCached = Boolean(
      cachedData
      && Array.isArray(cachedData.employees)
      && Array.isArray(cachedData.positions),
    );
    const shouldFetchNetwork = networkOnly || !canUseCached || Boolean(cachedSnapshot?.isExpired);

    if (nextRefreshing) {
      setRefreshing(true);
      setBackgroundRefreshing(false);
    } else {
      setLoadingPage(true);
      setResultsState("loading");
      setBackgroundRefreshing(false);
    }
    setRefreshMessage(null);
    setError(null);

    if (canUseCached && cachedData) {
      setEmployees(cachedData.employees);
      setPositions(cachedData.positions);
      syncResults(criteria, cachedData.employees, cachedData.positions);
      setCacheStatus({
        source: "cache",
        fetchedAt: cachedSnapshot?.fetchedAt ?? Date.now(),
      });
      setLoadingPage(false);
      if (!nextRefreshing && shouldFetchNetwork) {
        setBackgroundRefreshing(true);
      }
    }

    if (!shouldFetchNetwork) {
      setRefreshing(false);
      setBackgroundRefreshing(false);
      setLoadingPage(false);
      return;
    }

    try {
      const [nextEmployees, nextPositions] = await Promise.all([
        fetchShiftzyEmployees(requestJson, { includeAll: true }),
        fetchShiftzyPositions(requestJson, { includeAll: true }),
      ]);
      if (requestToken !== requestTokenRef.current) {
        return;
      }

      setEmployees(nextEmployees);
      setPositions(nextPositions);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });
      setCacheData(SHIFTZY_EMPLOYEES_CACHE_KEY, { employees: nextEmployees, positions: nextPositions }, {
        namespace: SHIFTZY_CACHE_NAMESPACE,
        scope: cacheScope,
        storage: "persistent",
        ttlMs: SHIFTZY_EMPLOYEES_CACHE_TTL_MS,
        source: "network",
        fetchedAt: Date.now(),
      });

      syncResults(criteria, nextEmployees, nextPositions);
    } catch (loadError) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      if (canUseCached && cachedData) {
        setEmployees(cachedData.employees);
        setPositions(cachedData.positions);
        syncResults(criteria, cachedData.employees, cachedData.positions);
        setCacheStatus({
          source: "cache",
          fetchedAt: cachedSnapshot?.fetchedAt ?? Date.now(),
        });
        setRefreshMessage("Showing cached Shiftzy employees. Could not refresh.");
      } else {
        const message = loadError instanceof Error ? loadError.message : "Could not load Shiftzy employees.";
        setError(message);
        setResultsState("error");
        setRefreshMessage(message);
      }
    } finally {
      if (requestToken === requestTokenRef.current) {
        setLoadingPage(false);
        setRefreshing(false);
        setBackgroundRefreshing(false);
      }
    }
  }, [applySearch, auth.status, auth.tenantSlug, hasSessionToken, requestJson]);

  useEffect(() => {
    if (auth.status !== "authenticated" || !hasSessionToken || !auth.tenantSlug) {
      return;
    }
    void loadData();
  }, [auth.status, auth.tenantSlug, hasSessionToken, loadData]);

  function handleDraftChange<K extends keyof ShiftzyAccountSearchFormValues>(
    field: K,
    nextValue: ShiftzyAccountSearchFormValues[K],
  ) {
    setDraft((current) => ({ ...current, [field]: nextValue }));
  }

  function handleSubmitSearch() {
    if (!canSubmitSearch || searching) {
      return;
    }
    setSearching(true);
    setSubmittedSearch(draft);
    applySearch(draft, employees, positionMap);
    setCacheStatus((current) => current ? { ...current, source: "local" } : current);
    setSearching(false);
  }

  function handleClearSearch() {
    setDraft(INITIAL_SEARCH_FORM);
    setSubmittedSearch(INITIAL_SEARCH_FORM);
    applySearch(INITIAL_SEARCH_FORM, employees, positionMap);
  }

  function handleOpenCreate() {
    if (!canEditShiftzy) {
      return;
    }
    setEditMode("create");
    setEditingEmployee(null);
    setIsEditModalOpen(true);
  }

  function handleOpenEdit(item: ShiftzyAccountItem) {
    setEditMode("edit");
    setEditingEmployee(item);
    setIsEditModalOpen(true);
  }

  async function handleSaveAccount(payload: ShiftzyAccountFormPayload) {
    if (!canEditShiftzy) {
      return;
    }
    setSaving(true);
    setRefreshMessage(null);
    try {
      if (editMode === "create") {
        await createShiftzyEmployees(requestJson, {
          name: payload.name,
          scheduleSection: payload.scheduleSection,
          note: payload.note || null,
          refPositionCode: payload.refPositionCode || null,
          active: payload.active,
        });
      } else {
        const id = String(payload.id || "").trim();
        if (!id) {
          throw new Error("Employee id is required for update.");
        }
        await updateShiftzyEmployees(requestJson, {
          id,
          name: payload.name,
          scheduleSection: payload.scheduleSection,
          note: payload.note || null,
          refPositionCode: payload.refPositionCode || null,
          active: payload.active,
        });
      }
      setIsEditModalOpen(false);
      await loadData({
        refreshing: true,
        networkOnly: true,
        criteria: submittedSearch,
      });
    } catch (saveError) {
      const message = saveError instanceof Error ? saveError.message : "Could not save employee.";
      toast.error("Save failed", message);
    } finally {
      setSaving(false);
    }
  }

  async function handleToggleActive(item: ShiftzyAccountItem) {
    if (!canEditShiftzy || saving || processingDeactivate) {
      return;
    }
    setSaving(true);
    setRefreshMessage(null);
    try {
      if (item.active) {
        await deleteShiftzyEmployees(requestJson, [item.id]);
      } else {
        await updateShiftzyEmployees(requestJson, {
          id: item.id,
          active: true,
        });
      }
      await loadData({
        refreshing: true,
        networkOnly: true,
        criteria: submittedSearch,
      });
    } catch (toggleError) {
      const message = toggleError instanceof Error ? toggleError.message : "Could not update employee status.";
      toast.error("Update failed", message);
    } finally {
      setSaving(false);
    }
  }

  function handleRequestToggleActive(item: ShiftzyAccountItem) {
    if (item.active) {
      setDeactivateTarget({ id: item.id, name: item.name });
      return;
    }
    void handleToggleActive(item);
  }

  async function handleDeactivateConfirmed() {
    if (!deactivateTarget || !canEditShiftzy || processingDeactivate || saving) {
      return;
    }
    setProcessingDeactivate(true);
    setRefreshMessage(null);
    try {
      await deleteShiftzyEmployees(requestJson, [deactivateTarget.id]);
      setDeactivateTarget(null);
      await loadData({
        refreshing: true,
        networkOnly: true,
        criteria: submittedSearch,
      });
    } catch (deactivateError) {
      const message = deactivateError instanceof Error ? deactivateError.message : "Could not update employee status.";
      toast.error("Update failed", message);
    } finally {
      setProcessingDeactivate(false);
    }
  }

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-5 pb-12">
      <PageBanner
        eyebrow="Shiftzy"
        title="Employees"
        description="Search and manage Shiftzy employees used by schedule workflows."
        gradientVariant="shiftzy"
        action={(
          <Button onClick={handleOpenCreate} disabled={!canEditShiftzy || loadingPage || refreshing || saving}>
            <Plus className="size-4" />
            Add Employee
          </Button>
        )}
      />

      <ShiftzyAccountSearch
        value={draft}
        onChange={handleDraftChange}
        onSubmit={handleSubmitSearch}
        onClear={handleClearSearch}
        canSubmit={canSubmitSearch}
        canClear={canClearSearch}
        searching={searching}
        disabled={loadingPage || refreshing || saving}
        resultText={resultText}
        sectionOptions={sectionOptions}
        positionOptions={positionOptions}
      />

      {refreshMessage ? (
        <p className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800">{refreshMessage}</p>
      ) : null}

      <ShiftzyAccountResults
        state={resultsState}
        groups={groups}
        error={error}
        canEdit={canEditShiftzy}
        disabled={saving || processingDeactivate || refreshing || loadingPage}
        onEdit={handleOpenEdit}
        onToggleActive={handleRequestToggleActive}
      />

      <ShiftzyAccountEditModal
        open={isEditModalOpen}
        mode={editMode}
        employee={editingEmployee}
        positions={positions}
        canEdit={canEditShiftzy}
        saving={saving}
        onOpenChange={setIsEditModalOpen}
        onSave={handleSaveAccount}
      />

      <Dialog
        open={Boolean(deactivateTarget)}
        onOpenChange={(open) => {
          if (!open && !processingDeactivate) {
            setDeactivateTarget(null);
          }
        }}
      >
        <DialogContent className="max-w-[560px]">
          <DialogHeader>
            <DialogTitle>Deactivate employee</DialogTitle>
          </DialogHeader>

          <p className="text-sm text-slate-700">
            Deactivate <span className="font-semibold">{deactivateTarget?.name || deactivateTarget?.id}</span>?
          </p>

          <DialogFooter>
            <Button
              className="border border-rose-700 bg-rose-600 text-white hover:bg-rose-700"
              onClick={() => void handleDeactivateConfirmed()}
              disabled={processingDeactivate}
            >
              {processingDeactivate ? "Deactivating..." : "Deactivate"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <div className="pointer-events-none fixed inset-x-0 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40">
        <div className="mx-4 sm:mx-6 lg:mx-8">
          <div className="mx-auto w-full max-w-[1600px]">
            <CacheStatusChip
              text={cacheStatusText}
              onRefresh={() => void loadData({
                refreshing: true,
                networkOnly: true,
                criteria: submittedSearch,
              })}
              disabled={loadingPage || refreshing || saving || processingDeactivate}
              refreshing={refreshing || backgroundRefreshing}
              refreshLabel="Refresh Shiftzy employees"
              tooltipText="Click to refresh Shiftzy employees"
              containerClassName="pointer-events-auto"
              className="max-w-[min(90vw,34rem)]"
            />
          </div>
        </div>
      </div>

      {loadingPage ? (
        <PageLoadingOverlay message="Loading Shiftzy employees..." />
      ) : null}
    </div>
  );
}
