import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  CalendarDays,
  Clock3,
  Plus,
  Settings2,
  Sparkles,
  UserRound,
  Users,
  X,
} from "lucide-react";

import { PageBanner } from "@shell/components/layout/PageBanner";
import { Button } from "@tradsphere/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@tradsphere/components/ui/dialog";
import { AppDropdown } from "@tradsphere/components/ui/app-dropdown";
import { Input } from "@tradsphere/components/ui/input";
import { Textarea } from "@tradsphere/components/ui/textarea";
import { ConfirmDialog } from "@tradsphere/components/ui/confirm-dialog";
import { useToast } from "@shell/components/ui/toast";
import { hasAppAdminAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import {
  buildScopedPageStateStorageKey,
  readScopedPageState,
  writeScopedPageState,
  type ScopedPageState,
} from "@shared/cache";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { LoadActionArea } from "@shared/components/layout/LoadActionArea";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import { TooltipTarget } from "@shared/components/actions/TooltipTarget";
import { LeaveSphereMonthCalendar, type LeaveSphereMonthCalendarEvent } from "@leavesphere/components/MonthCalendar";
import { LeaveSpherePtoRequestDetailModal } from "@leavesphere/components/PtoRequestDetailModal";
import {
  mapLeaveSphereHolidayRegionToChipTone,
  mapLeaveSpherePtoStatusToChipTone,
  LeaveSpherePtoStatusChip,
  LeaveSpherePtoToneChip,
} from "@leavesphere/components/PtoStatusChip";
import {
  adjustLeaveSphereAdminPtoBalance,
  createLeaveSphereAdminPtoRequest,
  loadLeaveSphereAdminPtoWorkspace,
  reviewLeaveSphereAdminPtoRequest,
  updateLeaveSphereAdminSetupData,
  type LeaveSphereAdminCreateRequestInput,
  type LeaveSphereAdminEmployee,
  type LeaveSphereAdminPtoTypeConfig,
  type LeaveSphereAdminSetupInput,
  type LeaveSphereAdminWorkspaceData,
} from "@leavesphere/lib/adminPtoMocks";
import { calculateLeaveSpherePtoHours } from "@leavesphere/lib/ptoHours";
import { getPtoRequestActionConfig } from "@leavesphere/lib/ptoRequestActionConfig";
import {
  getLeaveSphereReviewActionConfirmCopy,
  type LeaveSphereReviewAction,
} from "@leavesphere/lib/reviewActionConfirm";
import { LEAVESPHERE_TEAM_REGION_OPTIONS } from "@leavesphere/lib/ptoMocks";
import type { LeaveSpherePtoRequest, LeaveSpherePtoStatus, LeaveSpherePtoType, LeaveSphereTeamRegion } from "@leavesphere/lib/ptoMocks";

type CacheStatus = {
  source: "mock" | "network";
  fetchedAt: number;
};

type AdminTab = "calendar" | "requests" | "balances" | "setup";

type CreateRequestForm = {
  employeeId: string;
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: string;
  reason: string;
};

type AdjustBalanceForm = {
  employeeId: string;
  type: LeaveSpherePtoType;
  mode: "add" | "subtract" | "set";
  hours: string;
  note: string;
};

type SetupForm = {
  kind: LeaveSphereAdminSetupInput["kind"];
  name: string;
  code: string;
  label: string;
  detail: string;
  employeeName: string;
  title: string;
  managerId: string;
  employeeId: string;
  date: string;
  teamRegion: LeaveSphereTeamRegion;
  active: boolean;
};

type PersistedLeaveSphereAdminPtoPageState = {
  selectedYear?: string;
  loadedYear?: number | null;
  tab: AdminTab;
  calendarMonth: string;
  selectedRequestId: string | null;
  reviewNote: string;
  isCreateModalOpen: boolean;
  createForm: CreateRequestForm;
  isAdjustModalOpen: boolean;
  adjustForm: AdjustBalanceForm;
  isSetupModalOpen: boolean;
  setupForm: SetupForm;
  recentHistorySearch?: string;
  recentHistorySearchDraft?: string;
  recentHistorySearchApplied?: string;
  scrollY: number;
};

const PTO_TYPE_OPTIONS: Array<{ value: LeaveSpherePtoType; label: string }> = [
  { value: "vacation", label: "Vacation" },
  { value: "sick", label: "Sick" },
  { value: "personal", label: "Personal" },
  { value: "floating", label: "Floating Holiday" },
];

const TAB_OPTIONS: Array<{ id: AdminTab; label: string }> = [
  { id: "calendar", label: "Calendar" },
  { id: "requests", label: "Requests" },
  { id: "balances", label: "Balances" },
  { id: "setup", label: "Setup" },
];

const EMPTY_CREATE_FORM: CreateRequestForm = {
  employeeId: "",
  type: "vacation",
  startDate: "",
  endDate: "",
  hours: "",
  reason: "",
};

const EMPTY_ADJUST_FORM: AdjustBalanceForm = {
  employeeId: "",
  type: "vacation",
  mode: "add",
  hours: "",
  note: "",
};

const EMPTY_SETUP_FORM: SetupForm = {
  kind: "pto_type",
  name: "",
  code: "",
  label: "",
  detail: "",
  employeeName: "",
  title: "",
  managerId: "",
  employeeId: "",
  date: "",
  teamRegion: "US",
  active: true,
};

const LEAVESPHERE_APP_CODE = "leavesphere";
const LEAVESPHERE_ADMIN_PTO_PAGE_CODE = "admin-pto";

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeSearchKeyword(value: string): string {
  return asString(value).toLowerCase();
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

function formatRelativeTime(timestamp: number): string {
  const diffMs = Math.max(0, Date.now() - timestamp);
  if (diffMs < 60_000) {
    return "just now";
  }
  const minutes = Math.round(diffMs / 60_000);
  if (minutes < 60) {
    return `${minutes}m ago`;
  }
  const hours = Math.round(minutes / 60);
  if (hours < 24) {
    return `${hours}h ago`;
  }
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}

function formatDateLabel(isoDate: string): string {
  if (!isoDate) {
    return "-";
  }
  const date = new Date(`${isoDate}T00:00:00`);
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function formatHoursLabel(hours: number): string {
  if (!Number.isFinite(hours)) {
    return "0h";
  }
  return Number.isInteger(hours) ? `${hours}h` : `${hours.toFixed(1)}h`;
}

function statusLabel(status: LeaveSpherePtoStatus): string {
  if (status === "approved") {
    return "Approved";
  }
  if (status === "rejected") {
    return "Rejected";
  }
  if (status === "cancelled") {
    return "Cancelled";
  }
  return "Pending";
}

function normalizeOptionalNote(value: string | null | undefined): string {
  return asString(value);
}

function requestTypeLabel(type: LeaveSpherePtoType): string {
  return PTO_TYPE_OPTIONS.find((item) => item.value === type)?.label || "PTO";
}

function monthKeyFromDate(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function toIsoDate(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function requestIsOut(request: LeaveSpherePtoRequest): boolean {
  return request.status === "approved" || request.status === "pending";
}

function rangeIsValid(start: string, end: string): boolean {
  return Boolean(start) && Boolean(end) && start <= end;
}

function nonNegativeNumber(value: string): boolean {
  return asNumber(value) >= 0;
}

function requestOverlapsYear(request: LeaveSpherePtoRequest, year: number): boolean {
  const yearStart = `${year}-01-01`;
  const yearEnd = `${year}-12-31`;
  return request.startDate <= yearEnd && request.endDate >= yearStart;
}

function filterWorkspaceByYear(workspace: LeaveSphereAdminWorkspaceData, year: number): LeaveSphereAdminWorkspaceData {
  const yearPrefix = `${year}-`;
  return {
    ...workspace,
    requests: workspace.requests.filter((item) => requestOverlapsYear(item, year)),
    holidays: workspace.holidays.filter((item) => item.date.startsWith(yearPrefix)),
  };
}

function employeeNameById(workspace: LeaveSphereAdminWorkspaceData | null): Map<string, string> {
  const map = new Map<string, string>();
  for (const employee of workspace?.employees ?? []) {
    map.set(employee.employeeId, employee.employeeName);
  }
  return map;
}

function countUpcomingOutRequests(requests: LeaveSpherePtoRequest[]): number {
  const today = toIsoDate(new Date());
  const inTwoWeeksDate = new Date();
  inTwoWeeksDate.setDate(inTwoWeeksDate.getDate() + 14);
  const horizon = toIsoDate(inTwoWeeksDate);
  let count = 0;
  for (const request of requests) {
    if (!requestIsOut(request)) {
      continue;
    }
    if (request.endDate < today || request.startDate > horizon) {
      continue;
    }
    count += 1;
  }
  return count;
}

function isPtoTypeValue(value: unknown): value is LeaveSpherePtoType {
  return value === "vacation" || value === "sick" || value === "personal" || value === "floating";
}

function isAdminTab(value: unknown): value is AdminTab {
  return value === "calendar" || value === "requests" || value === "balances" || value === "setup";
}

function isCreateRequestForm(value: unknown): value is CreateRequestForm {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    typeof record.employeeId === "string"
    && isPtoTypeValue(record.type)
    && typeof record.startDate === "string"
    && typeof record.endDate === "string"
    && typeof record.hours === "string"
    && typeof record.reason === "string"
  );
}

function isAdjustBalanceForm(value: unknown): value is AdjustBalanceForm {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    typeof record.employeeId === "string"
    && isPtoTypeValue(record.type)
    && (record.mode === "add" || record.mode === "subtract" || record.mode === "set")
    && typeof record.hours === "string"
    && typeof record.note === "string"
  );
}

function isSetupForm(value: unknown): value is SetupForm {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  const kind = record.kind;
  return (
    (kind === "pto_type" || kind === "pto_action" || kind === "employee" || kind === "employee_manager" || kind === "holiday")
    && typeof record.name === "string"
    && typeof record.code === "string"
    && typeof record.label === "string"
    && typeof record.detail === "string"
    && typeof record.employeeName === "string"
    && typeof record.title === "string"
    && typeof record.managerId === "string"
    && typeof record.employeeId === "string"
    && typeof record.date === "string"
    && (
      record.teamRegion === undefined
      || record.teamRegion === "US"
      || record.teamRegion === "Mexico"
      || record.teamRegion === "Philippines"
    )
    && typeof record.active === "boolean"
  );
}

function isPersistedLeaveSphereAdminPtoPageState(value: unknown): value is PersistedLeaveSphereAdminPtoPageState {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  const scrollY = record.scrollY;
  const loadedYear = record.loadedYear;
  return (
    (record.selectedYear === undefined || typeof record.selectedYear === "string")
    && (loadedYear === undefined || loadedYear === null || (typeof loadedYear === "number" && Number.isInteger(loadedYear)))
    && isAdminTab(record.tab)
    && typeof record.calendarMonth === "string"
    && (record.selectedRequestId === null || typeof record.selectedRequestId === "string")
    && typeof record.reviewNote === "string"
    && typeof record.isCreateModalOpen === "boolean"
    && isCreateRequestForm(record.createForm)
    && typeof record.isAdjustModalOpen === "boolean"
    && isAdjustBalanceForm(record.adjustForm)
    && typeof record.isSetupModalOpen === "boolean"
    && isSetupForm(record.setupForm)
    && (record.recentHistorySearch === undefined || typeof record.recentHistorySearch === "string")
    && (record.recentHistorySearchDraft === undefined || typeof record.recentHistorySearchDraft === "string")
    && (record.recentHistorySearchApplied === undefined || typeof record.recentHistorySearchApplied === "string")
    && typeof scrollY === "number"
    && Number.isFinite(scrollY)
  );
}

export default function LeaveSphereAdminPtoPage() {
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const toast = useToast();

  const currentUserId = asString(auth.user?.id) || "local-admin";
  const currentUserName = asString(auth.user?.fullName) || asString(auth.user?.email) || "LeaveSphere Admin";
  const canAdmin = hasAppAdminAccess(auth.accessProfile, "leavesphere");

  const workspaceKey = useMemo(() => {
    const tenant = asString(auth.tenantSlug) || "default";
    return `leavesphere:admin:${tenant}:${currentUserId}`;
  }, [auth.tenantSlug, currentUserId]);
  const tenantSlug = asString(auth.tenantSlug);
  const canRestorePageState = auth.status === "authenticated" && Boolean(tenantSlug) && Boolean(currentUserId);
  const pageStateScope = useMemo<ScopedPageState | null>(() => {
    if (!canRestorePageState) {
      return null;
    }
    return {
      userKey: currentUserId,
      tenantSlug,
      appCode: LEAVESPHERE_APP_CODE,
      pageCode: LEAVESPHERE_ADMIN_PTO_PAGE_CODE,
    };
  }, [canRestorePageState, currentUserId, tenantSlug]);
  const pageStateStorageKey = useMemo(() => {
    if (!pageStateScope) {
      return null;
    }
    return buildScopedPageStateStorageKey(pageStateScope);
  }, [pageStateScope]);

  const [workspace, setWorkspace] = useState<LeaveSphereAdminWorkspaceData | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [pageErrorMessage, setPageErrorMessage] = useState<string | null>(null);
  const currentYear = new Date().getFullYear();
  const [selectedYear, setSelectedYear] = useState(() => String(currentYear));
  const [loadedYear, setLoadedYear] = useState<number | null>(null);

  const [tab, setTab] = useState<AdminTab>("calendar");
  const [calendarMonth, setCalendarMonth] = useState(() => monthKeyFromDate(new Date()));

  const [isInitializing, setIsInitializing] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [isMutating, setIsMutating] = useState(false);

  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [createForm, setCreateForm] = useState<CreateRequestForm>(EMPTY_CREATE_FORM);
  const [createError, setCreateError] = useState<string | null>(null);

  const [selectedRequestId, setSelectedRequestId] = useState<string | null>(null);
  const [selectedHolidayId, setSelectedHolidayId] = useState<string | null>(null);
  const [reviewNote, setReviewNote] = useState("");
  const [pendingReviewAction, setPendingReviewAction] = useState<LeaveSphereReviewAction | null>(null);
  const [isNoteSaveWarningDialogOpen, setIsNoteSaveWarningDialogOpen] = useState(false);

  const [isAdjustModalOpen, setIsAdjustModalOpen] = useState(false);
  const [adjustForm, setAdjustForm] = useState<AdjustBalanceForm>(EMPTY_ADJUST_FORM);
  const [adjustError, setAdjustError] = useState<string | null>(null);

  const [isSetupModalOpen, setIsSetupModalOpen] = useState(false);
  const [setupForm, setSetupForm] = useState<SetupForm>(EMPTY_SETUP_FORM);
  const [setupError, setSetupError] = useState<string | null>(null);
  const [draftRecentHistorySearch, setDraftRecentHistorySearch] = useState("");
  const [appliedRecentHistorySearch, setAppliedRecentHistorySearch] = useState("");
  const [scrollY, setScrollY] = useState(0);
  const [hasHydratedPageState, setHasHydratedPageState] = useState(false);
  const hydratedPageStateScopeRef = useRef<string | null>(null);
  const restoredScrollScopeRef = useRef<string | null>(null);
  const restoredWorkspaceScopeRef = useRef<string | null>(null);
  const noteSaveWarningResolverRef = useRef<((confirmed: boolean) => void) | null>(null);

  useEffect(() => {
    if (!canRestorePageState || !pageStateScope || !pageStateStorageKey) {
      hydratedPageStateScopeRef.current = null;
      restoredScrollScopeRef.current = null;
      restoredWorkspaceScopeRef.current = null;
      setHasHydratedPageState(false);
      return;
    }
    if (hydratedPageStateScopeRef.current === pageStateStorageKey) {
      setHasHydratedPageState(true);
      return;
    }

    hydratedPageStateScopeRef.current = pageStateStorageKey;
    const persisted = readScopedPageState<PersistedLeaveSphereAdminPtoPageState>(
      pageStateScope,
      isPersistedLeaveSphereAdminPtoPageState,
    );
    if (persisted) {
      if (typeof persisted.selectedYear === "string") {
        setSelectedYear(persisted.selectedYear);
      }
      if (persisted.loadedYear === null || typeof persisted.loadedYear === "number") {
        setLoadedYear(persisted.loadedYear ?? null);
      }
      setTab(persisted.tab);
      setCalendarMonth(persisted.calendarMonth);
      setSelectedRequestId(persisted.selectedRequestId);
      setReviewNote(persisted.reviewNote);
      setIsCreateModalOpen(persisted.isCreateModalOpen);
      setCreateForm(persisted.createForm);
      setIsAdjustModalOpen(persisted.isAdjustModalOpen);
      setAdjustForm(persisted.adjustForm);
      setIsSetupModalOpen(persisted.isSetupModalOpen);
      setSetupForm({
        ...persisted.setupForm,
        teamRegion: persisted.setupForm.teamRegion ?? "US",
      });
      const persistedDraft = asString(persisted.recentHistorySearchDraft);
      const persistedApplied = asString(persisted.recentHistorySearchApplied);
      if (persistedDraft || persistedApplied) {
        setDraftRecentHistorySearch(persistedDraft);
        setAppliedRecentHistorySearch(persistedApplied);
      } else if (typeof persisted.recentHistorySearch === "string") {
        const legacySearch = asString(persisted.recentHistorySearch);
        setDraftRecentHistorySearch(legacySearch);
        setAppliedRecentHistorySearch(legacySearch);
      }
      setScrollY(Math.max(0, persisted.scrollY));
    }
    setHasHydratedPageState(true);
  }, [canRestorePageState, pageStateScope, pageStateStorageKey]);

  useEffect(() => {
    if (!hasHydratedPageState || !pageStateStorageKey) {
      return;
    }
    if (restoredScrollScopeRef.current === pageStateStorageKey) {
      return;
    }
    restoredScrollScopeRef.current = pageStateStorageKey;
    const frame = window.requestAnimationFrame(() => {
      window.scrollTo({ top: Math.max(0, scrollY), left: 0, behavior: "auto" });
    });
    return () => {
      window.cancelAnimationFrame(frame);
    };
  }, [hasHydratedPageState, pageStateStorageKey, scrollY]);

  useEffect(() => {
    if (!hasHydratedPageState) {
      return;
    }
    let frameId = 0;
    const handleScroll = () => {
      if (frameId) {
        window.cancelAnimationFrame(frameId);
      }
      frameId = window.requestAnimationFrame(() => {
        setScrollY(window.scrollY || window.pageYOffset || 0);
      });
    };
    window.addEventListener("scroll", handleScroll, { passive: true });
    return () => {
      if (frameId) {
        window.cancelAnimationFrame(frameId);
      }
      window.removeEventListener("scroll", handleScroll);
    };
  }, [hasHydratedPageState]);

  useEffect(() => {
    if (!canRestorePageState || !hasHydratedPageState || !pageStateScope || !pageStateStorageKey) {
      return;
    }
    writeScopedPageState<PersistedLeaveSphereAdminPtoPageState>(pageStateScope, {
      selectedYear,
      loadedYear,
      tab,
      calendarMonth,
      selectedRequestId,
      reviewNote,
      isCreateModalOpen,
      createForm,
      isAdjustModalOpen,
      adjustForm,
      isSetupModalOpen,
      setupForm,
      recentHistorySearch: appliedRecentHistorySearch,
      recentHistorySearchDraft: draftRecentHistorySearch,
      recentHistorySearchApplied: appliedRecentHistorySearch,
      scrollY,
    });
  }, [
    adjustForm,
    calendarMonth,
    canRestorePageState,
    createForm,
    hasHydratedPageState,
    isAdjustModalOpen,
    isCreateModalOpen,
    isSetupModalOpen,
    loadedYear,
    pageStateScope,
    pageStateStorageKey,
    appliedRecentHistorySearch,
    draftRecentHistorySearch,
    reviewNote,
    selectedYear,
    scrollY,
    selectedRequestId,
    setupForm,
    tab,
  ]);

  const selectedYearNumber = useMemo(() => Number(selectedYear), [selectedYear]);
  const isSelectedYearLoaded = useMemo(
    () => Number.isInteger(selectedYearNumber) && loadedYear === selectedYearNumber,
    [loadedYear, selectedYearNumber],
  );
  const workspaceForYear = useMemo(() => {
    if (!workspace || !isSelectedYearLoaded || !Number.isInteger(selectedYearNumber)) {
      return null;
    }
    return filterWorkspaceByYear(workspace, selectedYearNumber);
  }, [isSelectedYearLoaded, selectedYearNumber, workspace]);

  const requests = useMemo(
    () => [...(workspaceForYear?.requests ?? [])].sort((left, right) => right.submittedAt.localeCompare(left.submittedAt)),
    [workspaceForYear?.requests],
  );
  const pendingRequests = useMemo(
    () => requests.filter((item) => item.status === "pending"),
    [requests],
  );
  const recentRequests = useMemo(
    () => requests.slice(0, 10),
    [requests],
  );
  const normalizedRecentHistorySearch = useMemo(
    () => normalizeSearchKeyword(appliedRecentHistorySearch),
    [appliedRecentHistorySearch],
  );
  const filteredRecentRequests = useMemo(() => {
    const keyword = normalizedRecentHistorySearch;
    if (!keyword) {
      return recentRequests;
    }
    return recentRequests.filter((request) => {
      const tokens = [
        request.employeeName,
        requestTypeLabel(request.type),
        statusLabel(request.status),
        formatDateLabel(request.startDate),
        formatDateLabel(request.endDate),
        formatDateLabel(request.submittedAt),
        request.reason,
      ];
      return tokens.some((token) => normalizeSearchKeyword(token).includes(keyword));
    });
  }, [normalizedRecentHistorySearch, recentRequests]);

  const selectedRequest = useMemo(
    () => requests.find((item) => item.id === selectedRequestId) || null,
    [requests, selectedRequestId],
  );
  const selectedRequestActionConfig = useMemo(
    () => (selectedRequest
      ? getPtoRequestActionConfig({
        role: "admin",
        status: selectedRequest.status,
        startDate: selectedRequest.startDate,
      })
      : null),
    [selectedRequest],
  );
  const isReviewNoteDirty = useMemo(
    () => selectedRequest
      ? normalizeOptionalNote(reviewNote) !== normalizeOptionalNote(selectedRequest.managerNote)
      : false,
    [reviewNote, selectedRequest],
  );
  const canEditAdminNote = Boolean(
    selectedRequest?.status === "pending"
    && (
      selectedRequestActionConfig?.canEditForm
      || selectedRequestActionConfig?.canApprove
      || selectedRequestActionConfig?.canReject
      || selectedRequestActionConfig?.canCancel
    ),
  );
  const pendingReviewActionCopy = useMemo(
    () => (pendingReviewAction ? getLeaveSphereReviewActionConfirmCopy(pendingReviewAction, "admin") : null),
    [pendingReviewAction],
  );
  const selectedHoliday = useMemo(
    () => (workspaceForYear?.holidays ?? []).find((item) => item.id === selectedHolidayId) || null,
    [selectedHolidayId, workspaceForYear?.holidays],
  );

  useEffect(() => {
    if (!selectedRequest) {
      return;
    }
    setReviewNote(selectedRequest.managerNote || "");
  }, [selectedRequest?.id]);

  const employeeById = useMemo(() => employeeNameById(workspaceForYear), [workspaceForYear]);

  const employeeOptions = useMemo(
    () => (workspaceForYear?.employees ?? []).map((item) => ({ value: item.employeeId, label: item.employeeName })),
    [workspaceForYear?.employees],
  );

  const ptoTypeOptions = useMemo(
    () => (workspaceForYear?.ptoTypes ?? []).filter((item) => item.active).map((item) => ({ value: item.code, label: item.label })),
    [workspaceForYear?.ptoTypes],
  );

  const overview = useMemo(() => {
    return {
      pendingCount: pendingRequests.length,
      upcomingOutCount: countUpcomingOutRequests(requests),
      employeeCount: workspaceForYear?.employees.filter((item) => item.active).length ?? 0,
      holidayCount: workspaceForYear?.holidays.length ?? 0,
    };
  }, [pendingRequests.length, requests, workspaceForYear?.employees, workspaceForYear?.holidays.length]);
  const holidayDates = useMemo(
    () => new Set((workspaceForYear?.holidays ?? []).map((item) => item.date)),
    [workspaceForYear?.holidays],
  );
  const yearOptions = useMemo(
    () => [
      { value: String(currentYear - 1), label: String(currentYear - 1) },
      { value: String(currentYear), label: String(currentYear) },
      { value: String(currentYear + 1), label: String(currentYear + 1) },
    ],
    [currentYear],
  );

  const cacheStatusText = useMemo(() => {
    if (isRefreshing) {
      return "Refreshing LeaveSphere Admin PTO workspace...";
    }
    if (loadedYear === null) {
      return "Select a year and click Load.";
    }
    if (!cacheStatus) {
      return "Loading LeaveSphere Admin PTO workspace...";
    }
    return `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [cacheStatus, isRefreshing, loadedYear]);

  const applyRecentHistorySearchKeyword = useCallback((rawValue: string) => {
    const normalized = asString(rawValue);
    setDraftRecentHistorySearch(normalized);
    setAppliedRecentHistorySearch(normalized);
  }, []);

  const applyRecentHistorySearchFromDraft = useCallback(() => {
    const normalizedDraft = asString(draftRecentHistorySearch);
    if (normalizedDraft !== draftRecentHistorySearch) {
      setDraftRecentHistorySearch(normalizedDraft);
    }
    if (normalizeSearchKeyword(normalizedDraft) === normalizeSearchKeyword(appliedRecentHistorySearch)) {
      return;
    }
    applyRecentHistorySearchKeyword(normalizedDraft);
  }, [appliedRecentHistorySearch, applyRecentHistorySearchKeyword, draftRecentHistorySearch]);

  const handleRecentHistorySearchInputChange = useCallback((nextValue: string) => {
    setDraftRecentHistorySearch(nextValue);
    if (!asString(nextValue)) {
      applyRecentHistorySearchKeyword("");
    }
  }, [applyRecentHistorySearchKeyword]);

  const handleRecentHistorySearchInputKeyDown = useCallback((event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") {
      event.preventDefault();
      applyRecentHistorySearchFromDraft();
      return;
    }
    if (event.key === "Tab") {
      applyRecentHistorySearchFromDraft();
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      if (draftRecentHistorySearch) {
        applyRecentHistorySearchKeyword("");
      }
      event.currentTarget.blur();
    }
  }, [applyRecentHistorySearchFromDraft, applyRecentHistorySearchKeyword, draftRecentHistorySearch]);

  const handleClearRecentHistorySearch = useCallback(() => {
    applyRecentHistorySearchKeyword("");
  }, [applyRecentHistorySearchKeyword]);

  useEffect(() => {
    const normalizedDraft = asString(draftRecentHistorySearch);
    if (!normalizedDraft) {
      return;
    }
    if (normalizeSearchKeyword(normalizedDraft) === normalizeSearchKeyword(appliedRecentHistorySearch)) {
      return;
    }
    const timer = window.setTimeout(() => {
      applyRecentHistorySearchKeyword(normalizedDraft);
    }, 1000);
    return () => {
      window.clearTimeout(timer);
    };
  }, [appliedRecentHistorySearch, applyRecentHistorySearchKeyword, draftRecentHistorySearch]);

  const pageMessages: StackMessage[] = [];
  if (refreshMessage) {
    pageMessages.push({
      id: "leavesphere-admin-refresh",
      variant: "info",
      message: refreshMessage,
    });
  }
  if (pageErrorMessage) {
    pageMessages.push({
      id: "leavesphere-admin-error",
      variant: "warning",
      message: pageErrorMessage,
    });
  }

  const loadingContract = resolveSharedLoadingContract(
    {
      pageInitializing: isInitializing,
      pageRefreshing: isRefreshing,
      cacheChipRefreshing: isChipRefreshOverlayVisible,
      sectionLoading: isMutating,
    },
    {
      pageInitializing: "Preparing LeaveSphere admin PTO workspace...",
      pageRefreshing: "Refreshing admin PTO workspace...",
      cacheChipRefreshing: "Refreshing admin PTO workspace...",
      sectionLoading: "Applying admin changes...",
    },
  );

  const loadWorkspace = useCallback(async (freshData = false): Promise<boolean> => {
    if (freshData) {
      setIsRefreshing(true);
    } else {
      setIsInitializing(true);
    }
    setPageErrorMessage(null);

    try {
      const result = await loadLeaveSphereAdminPtoWorkspace({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        freshData,
      });
      setWorkspace(result.workspace);
      setCacheStatus({ source: result.source, fetchedAt: Date.now() });
      setRefreshMessage(result.refreshMessage);
      return true;
    } catch {
      setPageErrorMessage("Unable to load Admin PTO workspace right now. Please try again.");
      return false;
    } finally {
      setIsInitializing(false);
      setIsRefreshing(false);
    }
  }, [currentUserId, currentUserName, requestJson, workspaceKey]);

  useEffect(() => {
    if (!hasHydratedPageState || !pageStateStorageKey || loadedYear === null) {
      return;
    }
    if (restoredWorkspaceScopeRef.current === pageStateStorageKey) {
      return;
    }
    restoredWorkspaceScopeRef.current = pageStateStorageKey;
    void loadWorkspace(false);
  }, [hasHydratedPageState, loadedYear, loadWorkspace, pageStateStorageKey]);

  const handleLoadByYear = useCallback(async () => {
    const parsedYear = Number(selectedYear);
    if (!Number.isInteger(parsedYear)) {
      return;
    }
    setCalendarMonth((current) => {
      const [, month] = current.split("-");
      const nextMonth = month || "01";
      return `${parsedYear}-${nextMonth}`;
    });
    const didLoad = await loadWorkspace(false);
    if (didLoad) {
      setLoadedYear(parsedYear);
      applyRecentHistorySearchKeyword("");
    }
  }, [applyRecentHistorySearchKeyword, loadWorkspace, selectedYear]);

  useEffect(() => {
    if (!isCreateModalOpen) {
      return;
    }
    setCreateForm((current) => ({
      ...current,
      employeeId: current.employeeId || employeeOptions[0]?.value || "",
      type: current.type || (ptoTypeOptions[0]?.value || "vacation"),
    }));
  }, [employeeOptions, isCreateModalOpen, ptoTypeOptions]);

  useEffect(() => {
    if (!isAdjustModalOpen) {
      return;
    }
    setAdjustForm((current) => ({
      ...current,
      employeeId: current.employeeId || employeeOptions[0]?.value || "",
      type: current.type || (ptoTypeOptions[0]?.value || "vacation"),
    }));
  }, [employeeOptions, isAdjustModalOpen, ptoTypeOptions]);

  const applyWorkspace = useCallback((next: LeaveSphereAdminWorkspaceData, source: "mock" | "network") => {
    setWorkspace(next);
    setCacheStatus({ source, fetchedAt: Date.now() });
  }, []);

  const handleCreateRequest = useCallback(async () => {
    const payload: LeaveSphereAdminCreateRequestInput = {
      employeeId: asString(createForm.employeeId),
      type: createForm.type,
      startDate: asString(createForm.startDate),
      endDate: asString(createForm.endDate),
      hours: asNumber(createForm.hours),
      reason: asString(createForm.reason),
    };

    if (!payload.employeeId) {
      setCreateError("Employee is required.");
      return;
    }
    if (!rangeIsValid(payload.startDate, payload.endDate)) {
      setCreateError("Start date must be on or before end date.");
      return;
    }
    if (!nonNegativeNumber(createForm.hours)) {
      setCreateError("Hours must be 0 or greater.");
      return;
    }
    if (!payload.reason) {
      setCreateError("Reason is required.");
      return;
    }

    setCreateError(null);
    setIsMutating(true);
    try {
      const result = await createLeaveSphereAdminPtoRequest({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        payload,
      });
      applyWorkspace(result.workspace, result.source);
      setIsCreateModalOpen(false);
      setCreateForm(EMPTY_CREATE_FORM);
      toast.success("Request created", "PTO request was created on behalf of the selected employee.");
    } catch {
      toast.error("Create failed", "Unable to create PTO request right now.");
    } finally {
      setIsMutating(false);
    }
  }, [
    applyWorkspace,
    createForm.employeeId,
    createForm.endDate,
    createForm.hours,
    createForm.reason,
    createForm.startDate,
    createForm.type,
    currentUserId,
    currentUserName,
    requestJson,
    toast,
    workspaceKey,
  ]);

  const handleReviewRequest = useCallback(async (approve: boolean) => {
    if (!selectedRequest) {
      return;
    }
    setIsMutating(true);
    try {
      if (selectedRequest.status !== "pending") {
        const reviewedAt = toIsoDate(new Date());
        const nextStatus: LeaveSpherePtoStatus = approve ? "approved" : "rejected";
        setWorkspace((current) => {
          if (!current) {
            return current;
          }
          return {
            ...current,
            requests: current.requests.map((item) => (
              item.id === selectedRequest.id
                ? {
                    ...item,
                    status: nextStatus,
                    reviewedAt,
                    reviewerName: currentUserName,
                    managerNote: normalizeOptionalNote(reviewNote) || null,
                  }
                : item
            )),
          };
        });
        setSelectedRequestId(null);
        setReviewNote("");
        toast.success(
          approve ? "Request approved" : "Request rejected",
          approve ? "Employee request status was updated to approved." : "Employee request status was updated to rejected.",
        );
        return;
      }

      const result = await reviewLeaveSphereAdminPtoRequest({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        payload: {
          requestId: selectedRequest.id,
          approve,
          note: reviewNote,
        },
      });
      applyWorkspace(result.workspace, result.source);
      setSelectedRequestId(null);
      setReviewNote("");
      toast.success(
        approve ? "Request approved" : "Request rejected",
        approve ? "Employee request status was updated to approved." : "Employee request status was updated to rejected.",
      );
    } catch {
      toast.error("Decision failed", "Unable to update request decision right now.");
    } finally {
      setIsMutating(false);
    }
  }, [
    applyWorkspace,
    currentUserId,
    currentUserName,
    requestJson,
    reviewNote,
    selectedRequest,
    toast,
    workspaceKey,
  ]);

  const handleCancelReviewRequest = useCallback(async () => {
    if (!selectedRequest) {
      return;
    }
    setIsMutating(true);
    try {
      const reviewedAt = toIsoDate(new Date());
      setWorkspace((current) => {
        if (!current) {
          return current;
        }
        return {
          ...current,
          requests: current.requests.map((item) => (
            item.id === selectedRequest.id
              ? {
                  ...item,
                  status: "cancelled",
                  reviewedAt,
                  reviewerName: currentUserName,
                  managerNote: normalizeOptionalNote(reviewNote) || null,
                }
              : item
          )),
        };
      });
      setSelectedRequestId(null);
      setReviewNote("");
      toast.success("Request cancelled", "Employee request status was updated to cancelled.");
    } finally {
      setIsMutating(false);
    }
  }, [currentUserName, reviewNote, selectedRequest, toast]);

  const handleRevertReviewDecision = useCallback(async () => {
    if (!selectedRequest) {
      return;
    }
    setIsMutating(true);
    try {
      setWorkspace((current) => {
        if (!current) {
          return current;
        }
        return {
          ...current,
          requests: current.requests.map((item) => (
            item.id === selectedRequest.id
              ? {
                  ...item,
                  status: "pending",
                  reviewedAt: null,
                  reviewerName: null,
                }
              : item
          )),
        };
      });
      toast.success("Decision reverted", "Request status was changed back to pending.");
    } finally {
      setIsMutating(false);
    }
  }, [selectedRequest, toast]);

  const handleConfirmReviewAction = useCallback(async () => {
    if (!pendingReviewAction) {
      return;
    }
    const action = pendingReviewAction;
    setPendingReviewAction(null);
    if (action === "approve") {
      await handleReviewRequest(true);
      return;
    }
    if (action === "reject") {
      await handleReviewRequest(false);
      return;
    }
    if (action === "cancel") {
      await handleCancelReviewRequest();
      return;
    }
    await handleRevertReviewDecision();
  }, [handleCancelReviewRequest, handleReviewRequest, handleRevertReviewDecision, pendingReviewAction]);

  const handleAdjustBalance = useCallback(async () => {
    if (!asString(adjustForm.employeeId)) {
      setAdjustError("Employee is required.");
      return;
    }
    if (!nonNegativeNumber(adjustForm.hours)) {
      setAdjustError("Hours must be 0 or greater.");
      return;
    }

    setAdjustError(null);
    setIsMutating(true);
    try {
      const result = await adjustLeaveSphereAdminPtoBalance({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        payload: {
          employeeId: adjustForm.employeeId,
          type: adjustForm.type,
          mode: adjustForm.mode,
          hours: asNumber(adjustForm.hours),
          note: asString(adjustForm.note),
        },
      });
      applyWorkspace(result.workspace, result.source);
      setIsAdjustModalOpen(false);
      setAdjustForm(EMPTY_ADJUST_FORM);
      toast.success("Balance updated", "Employee PTO balance was updated.");
    } catch {
      toast.error("Adjustment failed", "Unable to adjust employee PTO balance right now.");
    } finally {
      setIsMutating(false);
    }
  }, [
    adjustForm.employeeId,
    adjustForm.hours,
    adjustForm.mode,
    adjustForm.note,
    adjustForm.type,
    applyWorkspace,
    currentUserId,
    currentUserName,
    requestJson,
    toast,
    workspaceKey,
  ]);

  const handleSetupSave = useCallback(async () => {
    let payload: LeaveSphereAdminSetupInput | null = null;
    if (setupForm.kind === "pto_type") {
      const code = asString(setupForm.code) as LeaveSpherePtoType;
      if (!PTO_TYPE_OPTIONS.some((item) => item.value === code)) {
        setSetupError("Select a valid PTO type code.");
        return;
      }
      if (!asString(setupForm.label)) {
        setSetupError("Type label is required.");
        return;
      }
      payload = {
        kind: "pto_type",
        code,
        label: asString(setupForm.label),
        active: setupForm.active,
      };
    } else if (setupForm.kind === "pto_action") {
      if (!asString(setupForm.label)) {
        setSetupError("Action label is required.");
        return;
      }
      payload = {
        kind: "pto_action",
        code: asString(setupForm.code),
        label: asString(setupForm.label),
        detail: asString(setupForm.detail),
      };
    } else if (setupForm.kind === "employee") {
      if (!asString(setupForm.employeeName)) {
        setSetupError("Employee name is required.");
        return;
      }
        payload = {
          kind: "employee",
          employeeName: asString(setupForm.employeeName),
          title: asString(setupForm.title),
          managerId: asString(setupForm.managerId) || (workspaceForYear?.currentUserId ?? currentUserId),
          teamRegion: setupForm.teamRegion,
        };
    } else if (setupForm.kind === "employee_manager") {
      if (!asString(setupForm.employeeId) || !asString(setupForm.managerId)) {
        setSetupError("Employee and manager are required.");
        return;
      }
      payload = {
        kind: "employee_manager",
        employeeId: asString(setupForm.employeeId),
        managerId: asString(setupForm.managerId),
      };
    } else if (setupForm.kind === "holiday") {
      if (!asString(setupForm.name) || !asString(setupForm.date)) {
        setSetupError("Holiday name and date are required.");
        return;
      }
      payload = {
        kind: "holiday",
        name: asString(setupForm.name),
        date: asString(setupForm.date),
        teamRegion: setupForm.teamRegion,
      };
    }

    if (!payload) {
      setSetupError("Invalid setup payload.");
      return;
    }

    setSetupError(null);
    setIsMutating(true);
    try {
      const result = await updateLeaveSphereAdminSetupData({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        payload,
      });
      applyWorkspace(result.workspace, result.source);
      setIsSetupModalOpen(false);
      setSetupForm(EMPTY_SETUP_FORM);
      toast.success("Setup saved", "LeaveSphere admin setup data was updated.");
    } catch {
      toast.error("Setup failed", "Unable to update setup data right now.");
    } finally {
      setIsMutating(false);
    }
  }, [
    applyWorkspace,
    currentUserId,
    currentUserName,
    requestJson,
    setupForm.active,
    setupForm.code,
    setupForm.date,
    setupForm.detail,
    setupForm.employeeId,
    setupForm.employeeName,
    setupForm.kind,
    setupForm.label,
    setupForm.managerId,
    setupForm.name,
    setupForm.teamRegion,
    setupForm.title,
    toast,
    workspaceForYear?.currentUserId,
    workspaceKey,
  ]);

  const handleSaveRequestDetail = useCallback(async (params: {
    requestId: string | null;
    payload: {
      type: LeaveSpherePtoType;
      startDate: string;
      endDate: string;
      hours: number;
      reason: string;
    };
  }) => {
    if (!params.requestId) {
      return;
    }
    setIsMutating(true);
    try {
      setWorkspace((current) => {
        if (!current) {
          return current;
        }
        return {
          ...current,
          requests: current.requests.map((item) => {
            if (item.id !== params.requestId) {
              return item;
            }
            return {
              ...item,
              type: params.payload.type,
              startDate: params.payload.startDate,
              endDate: params.payload.endDate,
              hours: params.payload.hours,
              reason: params.payload.reason,
            };
          }),
        };
      });
      setSelectedRequestId(null);
      setReviewNote("");
      toast.success("Request updated", "PTO request details were updated.");
    } finally {
      setIsMutating(false);
    }
  }, [toast]);
  const handleSaveRequestDetailWithNoteWarning = useCallback(async (params: {
    requestId: string | null;
    payload: {
      type: LeaveSpherePtoType;
      startDate: string;
      endDate: string;
      hours: number;
      reason: string;
    };
  }) => {
    async function requestNoteSaveWarningConfirmation(): Promise<boolean> {
      setIsNoteSaveWarningDialogOpen(true);
      return new Promise<boolean>((resolve) => {
        noteSaveWarningResolverRef.current = resolve;
      });
    }

    if (canEditAdminNote && isReviewNoteDirty) {
      const shouldContinue = await requestNoteSaveWarningConfirmation();
      if (!shouldContinue) {
        return false;
      }
    }
    await handleSaveRequestDetail(params);
  }, [canEditAdminNote, handleSaveRequestDetail, isReviewNoteDirty]);

  const resolveNoteSaveWarningDialog = useCallback((confirmed: boolean) => {
    const resolver = noteSaveWarningResolverRef.current;
    noteSaveWarningResolverRef.current = null;
    setIsNoteSaveWarningDialogOpen(false);
    resolver?.(confirmed);
  }, []);

  useEffect(() => {
    return () => {
      if (noteSaveWarningResolverRef.current) {
        noteSaveWarningResolverRef.current(false);
        noteSaveWarningResolverRef.current = null;
      }
    };
  }, []);

  const renderCalendarTab = () => {
    const calendarEvents: LeaveSphereMonthCalendarEvent[] = [];
    for (const holiday of workspaceForYear?.holidays ?? []) {
      calendarEvents.push({
        id: `holiday:${holiday.id}`,
        label: `${holiday.name} (${holiday.teamRegion})`,
        tone: mapLeaveSphereHolidayRegionToChipTone(holiday.teamRegion),
        startDate: holiday.date,
        endDate: holiday.date,
        title: `${holiday.name} · ${holiday.teamRegion}`,
      });
    }
    for (const request of requests) {
      calendarEvents.push({
        id: `request:${request.id}`,
        label: `${request.employeeName} (${statusLabel(request.status)})`,
        tone: mapLeaveSpherePtoStatusToChipTone(request.status),
        startDate: request.startDate,
        endDate: request.endDate,
        title: `${request.employeeName} · ${requestTypeLabel(request.type)} · ${statusLabel(request.status)}`,
      });
    }

    return (
      <LeaveSphereMonthCalendar
        title="Month calendar"
        description="Who is out and company holidays"
        monthKey={calendarMonth}
        onMonthChange={setCalendarMonth}
        dayMinHeightClassName="min-h-[8.2rem]"
        events={calendarEvents}
        onEventClick={(event) => {
          if (event.id.startsWith("request:")) {
            setSelectedRequestId(event.id.slice("request:".length));
            return;
          }
          if (event.id.startsWith("holiday:")) {
            setSelectedHolidayId(event.id.slice("holiday:".length));
          }
        }}
        legend={(
          <div className="flex flex-wrap gap-2 text-[11px] text-slate-600">
            <LeaveSpherePtoToneChip tone="holiday_us" />
            <LeaveSpherePtoToneChip tone="holiday_mexico" />
            <LeaveSpherePtoToneChip tone="holiday_philippines" />
            <LeaveSpherePtoToneChip tone="pending" label="Pending PTO" />
            <LeaveSpherePtoToneChip tone="approved" label="Approved PTO" />
            <LeaveSpherePtoToneChip tone="rejected" label="Rejected PTO" />
            <LeaveSpherePtoToneChip tone="cancelled" label="Canceled PTO" />
          </div>
        )}
      />
    );
  };

  const renderRequestsTab = () => {
    return (
      <div className="grid gap-4 xl:grid-cols-[minmax(18rem,22rem)_minmax(0,1fr)]">
        <SectionCard
          title="Pending requests"
          description={`${pendingRequests.length} waiting for decision`}
          contentClassName="space-y-3"
        >
          <div className="max-h-[40rem] space-y-2 overflow-y-auto pr-1">
            {pendingRequests.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                No pending PTO requests.
              </div>
            ) : (
              pendingRequests.map((request) => (
                <button
                  type="button"
                  key={request.id}
                  className="w-full rounded-xl border border-blue-100 bg-white px-3.5 py-3 text-left transition-colors hover:border-blue-300 hover:bg-blue-50/35"
                  onClick={() => {
                    setSelectedRequestId(request.id);
                    setReviewNote(request.managerNote || "");
                  }}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <p className="text-sm font-semibold text-slate-900">{request.employeeName}</p>
                      <p className="text-xs text-slate-600">
                        {requestTypeLabel(request.type)} · {formatDateLabel(request.startDate)} - {formatDateLabel(request.endDate)}
                      </p>
                    </div>
                    <LeaveSpherePtoStatusChip status={request.status} label={statusLabel(request.status)} />
                  </div>
                  <p className="mt-2 text-xs text-slate-700">{request.reason}</p>
                  <div className="mt-2 flex items-center justify-between text-[11px] text-slate-500">
                    <span>{formatHoursLabel(request.hours)}</span>
                    <span>Submitted {formatDateLabel(request.submittedAt)}</span>
                  </div>
                </button>
              ))
            )}
          </div>
        </SectionCard>

        <SectionCard
          title="Recent request history"
          description="All employee PTO request statuses"
          contentClassName="space-y-4"
        >
          <div className="grid gap-3 md:grid-cols-3">
            <article className="rounded-2xl border border-blue-100 bg-white p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Total requests</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-slate-900">{requests.length}</p>
            </article>
            <article className="rounded-2xl border border-amber-200 bg-amber-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-amber-800">Pending approvals</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-amber-900">{pendingRequests.length}</p>
            </article>
            <article className="rounded-2xl border border-emerald-200 bg-emerald-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-emerald-800">Reviewed</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-emerald-900">
                {Math.max(0, requests.length - pendingRequests.length)}
              </p>
            </article>
          </div>

          <div className="flex justify-end">
            <div className="relative w-full max-w-[26rem]">
              <Input
                value={draftRecentHistorySearch}
                onChange={(event) => handleRecentHistorySearchInputChange(event.target.value)}
                onBlur={applyRecentHistorySearchFromDraft}
                onKeyDown={handleRecentHistorySearchInputKeyDown}
                placeholder="Filter by employee, type, date, or status"
                className="pr-9 transition !outline-none ![box-shadow:none] !focus:outline-none !focus:ring-0 !focus:ring-offset-0 !focus:border-slate-300 !focus:shadow-none !focus:[box-shadow:none] !focus-visible:outline-none !focus-visible:ring-0 !focus-visible:ring-offset-0 !focus-visible:border-slate-300 !focus-visible:shadow-none !focus-visible:[box-shadow:none]"
              />
              {draftRecentHistorySearch ? (
                <TooltipTarget text="Clear recent request history search">
                  <button
                    type="button"
                    onClick={handleClearRecentHistorySearch}
                    className="absolute right-2 top-1/2 inline-flex size-6 -translate-y-1/2 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300"
                    aria-label="Clear recent request history search"
                  >
                    <X className="size-3.5" />
                  </button>
                </TooltipTarget>
              ) : null}
            </div>
          </div>

          <div className="overflow-x-auto rounded-xl border border-blue-100">
            <table className="min-w-full text-left text-sm">
              <thead className="bg-blue-50/70 text-xs uppercase tracking-[0.08em] text-slate-600">
                <tr>
                  <th className="px-3 py-2.5">Employee</th>
                  <th className="px-3 py-2.5">Type</th>
                  <th className="px-3 py-2.5">Date range</th>
                  <th className="px-3 py-2.5">Status</th>
                  <th className="px-3 py-2.5">Hours</th>
                </tr>
              </thead>
              <tbody>
                {recentRequests.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-3 py-6 text-center text-sm text-slate-600">No requests found.</td>
                  </tr>
                ) : filteredRecentRequests.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-3 py-6 text-center text-sm text-slate-600">
                      No recent requests match your keyword filter.
                    </td>
                  </tr>
                ) : (
                  filteredRecentRequests.map((request) => (
                    <tr
                      key={request.id}
                      className="cursor-pointer border-t border-blue-100/80 bg-white text-slate-700 transition-colors hover:bg-blue-50/40"
                      onClick={() => {
                        setSelectedRequestId(request.id);
                        setReviewNote(request.managerNote || "");
                      }}
                    >
                      <td className="px-3 py-2.5">
                        <div className="flex items-center gap-2">
                          <span className="inline-flex size-7 items-center justify-center rounded-full border border-blue-100 bg-blue-50 text-blue-700">
                            <UserRound className="size-4" />
                          </span>
                          <div>
                            <p className="text-sm font-medium text-slate-900">{request.employeeName}</p>
                            <p className="text-xs text-slate-500">Submitted {formatDateLabel(request.submittedAt)}</p>
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-2.5">{requestTypeLabel(request.type)}</td>
                      <td className="px-3 py-2.5">{formatDateLabel(request.startDate)} - {formatDateLabel(request.endDate)}</td>
                      <td className="px-3 py-2.5">
                        <LeaveSpherePtoStatusChip status={request.status} label={statusLabel(request.status)} />
                      </td>
                      <td className="px-3 py-2.5">{formatHoursLabel(request.hours)}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </SectionCard>
      </div>
    );
  };

  const renderBalancesTab = () => {
    return (
      <SectionCard
        title="Employee PTO balances"
        description="Load/add/adjust PTO hours by employee"
        actions={(
          <Button
            variant="outline"
            onClick={() => {
              setAdjustError(null);
              setAdjustForm({
                ...EMPTY_ADJUST_FORM,
                employeeId: employeeOptions[0]?.value || "",
                type: (ptoTypeOptions[0]?.value as LeaveSpherePtoType) || "vacation",
              });
              setIsAdjustModalOpen(true);
            }}
          >
            <Sparkles className="size-4" />
            Adjust Balance
          </Button>
        )}
      >
        <div className="overflow-x-auto rounded-xl border border-blue-100">
          <table className="min-w-full text-left text-sm">
            <thead className="bg-blue-50/70 text-xs uppercase tracking-[0.08em] text-slate-600">
              <tr>
                <th className="px-3 py-2.5">Employee</th>
                <th className="px-3 py-2.5">Vacation</th>
                <th className="px-3 py-2.5">Sick</th>
                <th className="px-3 py-2.5">Personal</th>
                <th className="px-3 py-2.5">Floating</th>
              </tr>
            </thead>
            <tbody>
              {(workspaceForYear?.employeeBalances ?? []).map((row) => {
                const balanceByType = new Map(row.balances.map((item) => [item.type, item]));
                const renderCell = (type: LeaveSpherePtoType) => {
                  const balance = balanceByType.get(type);
                  if (!balance) {
                    return "-";
                  }
                  const remaining = Math.max(0, balance.totalHours - balance.usedHours - balance.scheduledHours);
                  return `${formatHoursLabel(remaining)} / ${formatHoursLabel(balance.totalHours)}`;
                };
                return (
                  <tr key={row.employeeId} className="border-t border-blue-100/80 bg-white text-slate-700">
                    <td className="px-3 py-2.5 font-medium text-slate-900">{row.employeeName}</td>
                    <td className="px-3 py-2.5">{renderCell("vacation")}</td>
                    <td className="px-3 py-2.5">{renderCell("sick")}</td>
                    <td className="px-3 py-2.5">{renderCell("personal")}</td>
                    <td className="px-3 py-2.5">{renderCell("floating")}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </SectionCard>
    );
  };

  const renderSetupTab = () => {
    return (
      <div className="grid gap-4 xl:grid-cols-2">
        <SectionCard
          title="Setup data"
          description="PTO types, actions, employees, managers, and holidays"
          actions={(
            <Button
              variant="outline"
              onClick={() => {
                setSetupError(null);
                setSetupForm({
                  ...EMPTY_SETUP_FORM,
                  managerId: workspaceForYear?.currentUserId || currentUserId,
                });
                setIsSetupModalOpen(true);
              }}
            >
              <Settings2 className="size-4" />
              Edit Setup
            </Button>
          )}
          contentClassName="space-y-3"
        >
          <div className="rounded-xl border border-blue-100 bg-white p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">PTO types</p>
            <div className="mt-2 flex flex-wrap gap-2">
              {(workspaceForYear?.ptoTypes ?? []).map((item: LeaveSphereAdminPtoTypeConfig) => (
                <span key={item.code} className="inline-flex rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-700">
                  {item.label}
                </span>
              ))}
            </div>
          </div>

          <div className="rounded-xl border border-blue-100 bg-white p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">PTO actions</p>
            <ul className="mt-2 space-y-2 text-sm text-slate-700">
              {(workspaceForYear?.ptoActions ?? []).slice(0, 4).map((item) => (
                <li key={item.code} className="rounded-lg border border-slate-200 bg-slate-50 px-2.5 py-2">
                  <p className="font-medium text-slate-800">{item.label}</p>
                  <p className="text-xs text-slate-600">{item.detail || "-"}</p>
                </li>
              ))}
            </ul>
          </div>
        </SectionCard>

        <SectionCard
          title="Employees / managers / holidays"
          description="Current manager mapping and holiday list"
          contentClassName="space-y-3"
        >
          <div className="rounded-xl border border-blue-100 bg-white p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">Employees</p>
            <ul className="mt-2 max-h-44 space-y-2 overflow-y-auto pr-1 text-sm text-slate-700">
              {(workspaceForYear?.employees ?? []).map((item: LeaveSphereAdminEmployee) => (
                <li key={item.employeeId} className="rounded-lg border border-slate-200 bg-slate-50 px-2.5 py-2">
                  <p className="font-medium text-slate-800">{item.employeeName}</p>
                  <p className="text-xs text-slate-600">{item.title} · Team: {item.teamRegion} · Manager: {item.managerName}</p>
                </li>
              ))}
            </ul>
          </div>

          <div className="rounded-xl border border-blue-100 bg-white p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">Holidays</p>
            <ul className="mt-2 max-h-40 space-y-2 overflow-y-auto pr-1 text-sm text-slate-700">
              {(workspaceForYear?.holidays ?? []).map((item) => (
                <li key={item.id} className="rounded-lg border border-slate-200 bg-slate-50 px-2.5 py-2">
                  <p className="font-medium text-slate-800">{item.name} ({item.teamRegion})</p>
                  <p className="text-xs text-slate-600">{formatDateLabel(item.date)}</p>
                </li>
              ))}
            </ul>
          </div>
        </SectionCard>
      </div>
    );
  };

  if (!canAdmin) {
    return (
      <AppPageLayout
        className="pb-5"
        pageMessages={null}
        banner={(
          <PageBanner
            eyebrow="LeaveSphere"
            title="Admin PTO"
            description="This page requires LeaveSphere admin access."
            gradientVariant="workspace"
          />
        )}
      >
        <SectionCard title="Access required" description="Only LeaveSphere admins can access this workspace.">
          <p className="text-sm text-slate-600">Contact your workspace administrator if you need admin access.</p>
        </SectionCard>
      </AppPageLayout>
    );
  }

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          eyebrow="LeaveSphere"
          title="Admin PTO"
          description="Admin overview of PTO activity, requests, balances, and setup data."
          gradientVariant="workspace"
          action={(
            <div className="flex flex-wrap gap-2">
              <Button
                disabled={!workspaceForYear}
                onClick={() => {
                  setCreateError(null);
                  setCreateForm({
                    ...EMPTY_CREATE_FORM,
                    employeeId: employeeOptions[0]?.value || "",
                    type: (ptoTypeOptions[0]?.value as LeaveSpherePtoType) || "vacation",
                  });
                  setIsCreateModalOpen(true);
                }}
              >
                <Plus className="size-4" />
                Create Request
              </Button>
              <Button
                variant="outline"
                disabled={!workspaceForYear}
                onClick={() => {
                  setAdjustError(null);
                  setAdjustForm({
                    ...EMPTY_ADJUST_FORM,
                    employeeId: employeeOptions[0]?.value || "",
                    type: (ptoTypeOptions[0]?.value as LeaveSpherePtoType) || "vacation",
                  });
                  setIsAdjustModalOpen(true);
                }}
              >
                <Sparkles className="size-4" />
                Adjust Balance
              </Button>
            </div>
          )}
        />
      )}
      footer={cacheStatus && loadedYear !== null ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            setIsChipRefreshOverlayVisible(true);
            void loadWorkspace(true).finally(() => {
              setIsChipRefreshOverlayVisible(false);
            });
          }}
          disabled={isInitializing || isRefreshing || isMutating}
          refreshing={isRefreshing || isChipRefreshOverlayVisible}
          refreshLabel="Refresh admin PTO workspace"
          tooltipText="Click to refresh requests, balances, and setup data"
          containerClassName="w-full"
        />
      ) : null}
    >
      <SectionCard title="Year & Load" divider={false}>
        <LoadActionArea
          className="rounded-none bg-transparent p-0"
          controls={(
            <AppDropdown
              value={selectedYear}
              onValueChange={setSelectedYear}
              options={yearOptions}
              searchable={false}
              disabled={isInitializing || isRefreshing || isMutating}
            />
          )}
          actions={(
            <Button onClick={() => void handleLoadByYear()} disabled={isInitializing || isRefreshing || isMutating}>
              {isInitializing ? "Loading..." : "Load"}
            </Button>
          )}
        />
      </SectionCard>
      {workspaceForYear && (
        <>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <article className="rounded-2xl border border-amber-200 bg-amber-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-amber-800">Pending requests</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-amber-900">{overview.pendingCount}</p>
            </article>
            <article className="rounded-2xl border border-blue-200 bg-blue-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-blue-800">Upcoming PTO</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-blue-900">{overview.upcomingOutCount}</p>
            </article>
            <article className="rounded-2xl border border-violet-200 bg-violet-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-violet-800">Total employees</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-violet-900">{overview.employeeCount}</p>
            </article>
            <article className="rounded-2xl border border-indigo-200 bg-indigo-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-indigo-800">Holiday marks</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-indigo-900">{overview.holidayCount}</p>
            </article>
          </div>

          <div className="grid w-full grid-cols-2 gap-1.5 rounded-xl border border-blue-200 bg-gradient-to-r from-blue-50/85 via-indigo-50/50 to-violet-50/70 p-1.5 sm:grid-cols-4">
            {TAB_OPTIONS.map((item) => {
              const active = tab === item.id;
              return (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => setTab(item.id)}
                  className={[
                    "inline-flex items-center justify-center gap-1.5 rounded-lg border px-3 py-2 text-xs font-semibold transition",
                    active
                      ? "border-blue-200 bg-white text-blue-700 shadow-sm"
                      : "border-transparent bg-transparent text-slate-600 hover:border-blue-100 hover:bg-white/75 hover:text-blue-700",
                  ].join(" ")}
                >
                  {item.id === "calendar" ? <CalendarDays className="size-3.5" /> : null}
                  {item.id === "requests" ? <Clock3 className="size-3.5" /> : null}
                  {item.id === "balances" ? <Users className="size-3.5" /> : null}
                  {item.id === "setup" ? <Settings2 className="size-3.5" /> : null}
                  {item.label}
                </button>
              );
            })}
          </div>

          <div className="relative">
            {tab === "calendar" ? renderCalendarTab() : null}
            {tab === "requests" ? renderRequestsTab() : null}
            {tab === "balances" ? renderBalancesTab() : null}
            {tab === "setup" ? renderSetupTab() : null}

            <SectionLoadingLayer
              active={loadingContract.sectionOverlayActive}
              message={loadingContract.sectionOverlayMessage}
            />
          </div>
        </>
      )}

      <Dialog open={isCreateModalOpen} onOpenChange={setIsCreateModalOpen}>
        <DialogContent className="max-w-2xl">
          <DialogClose className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" />
          <DialogHeader>
            <DialogTitle>Create PTO request</DialogTitle>
            <DialogDescription>Create a PTO request on behalf of an employee.</DialogDescription>
          </DialogHeader>

          {createError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{createError}</div>
          ) : null}

          <div className="grid gap-3 sm:grid-cols-2">
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Employee</span>
              <AppDropdown
                value={createForm.employeeId}
                onValueChange={(value) => setCreateForm((current) => ({ ...current, employeeId: value }))}
                options={employeeOptions}
                searchable
                disabled={isMutating}
              />
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">PTO type</span>
              <AppDropdown
                value={createForm.type}
                onValueChange={(value) => setCreateForm((current) => ({ ...current, type: value as LeaveSpherePtoType }))}
                options={ptoTypeOptions}
                searchable={false}
                disabled={isMutating}
              />
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Start date</span>
              <Input type="date" value={createForm.startDate} onChange={(event) => setCreateForm((current) => ({ ...current, startDate: event.target.value }))} disabled={isMutating} />
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">End date</span>
              <Input type="date" value={createForm.endDate} onChange={(event) => setCreateForm((current) => ({ ...current, endDate: event.target.value }))} disabled={isMutating} />
            </label>
          </div>

          <label className="space-y-1 text-sm">
            <span className="text-slate-600">Hours</span>
            <Input type="number" min={0} step={0.5} value={createForm.hours} onChange={(event) => setCreateForm((current) => ({ ...current, hours: event.target.value }))} disabled={isMutating} />
          </label>

          <label className="space-y-1 text-sm">
            <span className="text-slate-600">Reason</span>
            <Textarea
              value={createForm.reason}
              onChange={(event) => setCreateForm((current) => ({ ...current, reason: event.target.value }))}
              className="min-h-[110px]"
              disabled={isMutating}
            />
          </label>

          <DialogFooter>
            <Button variant="outline" onClick={() => setIsCreateModalOpen(false)} disabled={isMutating}>Cancel</Button>
            <Button onClick={() => void handleCreateRequest()} disabled={isMutating}>Create request</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <LeaveSpherePtoRequestDetailModal
        open={Boolean(selectedRequest)}
        request={selectedRequest}
        layoutVariant="my-pto-detail"
        readOnly={!selectedRequestActionConfig?.canEditForm}
        title="Request detail"
        description="Review and update employee PTO request details."
        ptoTypeOptions={PTO_TYPE_OPTIONS.map((item) => ({ value: item.value, label: item.label }))}
        statusLabel={statusLabel}
        saving={isMutating}
        calculateHours={(startDate, endDate) => calculateLeaveSpherePtoHours(startDate, endDate, holidayDates)}
        onOpenChange={(open) => {
          if (!open) {
            setSelectedRequestId(null);
            setReviewNote("");
            setPendingReviewAction(null);
          }
        }}
        onClose={() => {
          setSelectedRequestId(null);
          setReviewNote("");
          setPendingReviewAction(null);
        }}
        onSave={selectedRequestActionConfig?.canSubmit ? handleSaveRequestDetailWithNoteWarning : undefined}
        externalDirty={Boolean(
          selectedRequest
          && canEditAdminNote
          && isReviewNoteDirty
        )}
        details={selectedRequest ? (
          <div className="grid gap-2 text-sm text-slate-700 sm:grid-cols-2">
            <p className="min-w-0">
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Employee</span>
              <span className="mt-0.5 block truncate font-semibold text-slate-900" title={selectedRequest.employeeName}>
                {selectedRequest.employeeName}
              </span>
            </p>
            <p className="min-w-0">
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Manager</span>
              <span
                className="mt-0.5 block truncate font-semibold text-slate-900"
                title={employeeById.get(selectedRequest.managerId) || selectedRequest.managerId}
              >
                {employeeById.get(selectedRequest.managerId) || selectedRequest.managerId}
              </span>
            </p>
            <p>
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Submitted</span>
              <span className="mt-0.5 block font-semibold text-slate-900">{formatDateLabel(selectedRequest.submittedAt)}</span>
            </p>
            {selectedRequest.reviewerName ? (
              <p>
                <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Reviewed by</span>
                <span className="mt-0.5 block font-semibold text-slate-900">
                  {selectedRequest.reviewerName}
                  {selectedRequest.reviewedAt ? ` · ${formatDateLabel(selectedRequest.reviewedAt)}` : ""}
                </span>
              </p>
            ) : (
              <p>
                <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Reviewed by</span>
                <span className="mt-0.5 block font-semibold text-slate-900">Pending review</span>
              </p>
            )}
          </div>
        ) : null}
        extraContent={selectedRequest ? (
          <label className="space-y-1 text-sm">
            <span className="text-slate-600">Admin note</span>
            <Textarea
              value={reviewNote}
              onChange={(event) => setReviewNote(event.target.value)}
              className="min-h-[100px]"
              disabled={isMutating || !canEditAdminNote}
            />
          </label>
        ) : null}
        footerActions={selectedRequest ? (
          <>
            {selectedRequestActionConfig?.canRevert ? (
              <Button
                variant="outline"
                onClick={() => setPendingReviewAction("revert")}
                disabled={isMutating}
              >
                Revert
              </Button>
            ) : null}
            {selectedRequestActionConfig?.canCancel ? (
              <Button
                variant="outline"
                onClick={() => setPendingReviewAction("cancel")}
                disabled={isMutating}
              >
                Cancel
              </Button>
            ) : null}
            {selectedRequestActionConfig?.canReject ? (
              <Button
                variant="outline"
                className="border-rose-200 text-rose-700 hover:bg-rose-50"
                onClick={() => setPendingReviewAction("reject")}
                disabled={isMutating}
              >
                Reject
              </Button>
            ) : null}
            {selectedRequestActionConfig?.canApprove ? (
              <Button
                variant="outline"
                className="border-emerald-300 text-emerald-700 hover:bg-emerald-50"
                onClick={() => setPendingReviewAction("approve")}
                disabled={isMutating}
              >
                Approve
              </Button>
            ) : null}
          </>
        ) : null}
      />

      {pendingReviewActionCopy ? (
        <ConfirmDialog
          open={Boolean(pendingReviewAction)}
          title={pendingReviewActionCopy.title}
          description={pendingReviewActionCopy.description}
          confirmLabel={pendingReviewActionCopy.confirmLabel}
          cancelLabel="Go back"
          onCancel={() => setPendingReviewAction(null)}
          onConfirm={() => {
            void handleConfirmReviewAction();
          }}
        />
      ) : null}

      <ConfirmDialog
        open={isNoteSaveWarningDialogOpen}
        title="Admin note won't be saved"
        description="Save changes only updates request details. Use Approve, Reject, or Cancel to save the admin note."
        cancelLabel="Go back"
        confirmLabel="Save details only"
        onCancel={() => resolveNoteSaveWarningDialog(false)}
        onConfirm={() => resolveNoteSaveWarningDialog(true)}
      />

      <Dialog open={Boolean(selectedHoliday)} onOpenChange={(open) => {
        if (!open) {
          setSelectedHolidayId(null);
        }
      }}>
        <DialogContent className="max-w-xl">
          <DialogHeader>
            <DialogTitle>Holiday detail</DialogTitle>
            <DialogDescription>
              Team holiday information for the selected date.
            </DialogDescription>
          </DialogHeader>

          {selectedHoliday ? (
            <div className="rounded-xl border border-indigo-100 bg-indigo-50/40 p-3 text-sm text-slate-700">
              <p><span className="font-semibold text-slate-900">Holiday:</span> {selectedHoliday.name}</p>
              <p><span className="font-semibold text-slate-900">Team region:</span> {selectedHoliday.teamRegion}</p>
              <p><span className="font-semibold text-slate-900">Date:</span> {formatDateLabel(selectedHoliday.date)}</p>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>

      <Dialog open={isAdjustModalOpen} onOpenChange={setIsAdjustModalOpen}>
        <DialogContent className="max-w-xl">
          <DialogHeader>
            <DialogTitle>Adjust PTO balance</DialogTitle>
            <DialogDescription>Load/add/adjust PTO hours for an employee.</DialogDescription>
          </DialogHeader>

          {adjustError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{adjustError}</div>
          ) : null}

          <div className="grid gap-3">
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Employee</span>
              <AppDropdown
                value={adjustForm.employeeId}
                onValueChange={(value) => setAdjustForm((current) => ({ ...current, employeeId: value }))}
                options={employeeOptions}
                searchable
                disabled={isMutating}
              />
            </label>
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">PTO type</span>
                <AppDropdown
                  value={adjustForm.type}
                  onValueChange={(value) => setAdjustForm((current) => ({ ...current, type: value as LeaveSpherePtoType }))}
                  options={ptoTypeOptions}
                  searchable={false}
                  disabled={isMutating}
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Mode</span>
                <AppDropdown
                  value={adjustForm.mode}
                  onValueChange={(value) => setAdjustForm((current) => ({ ...current, mode: value as AdjustBalanceForm["mode"] }))}
                  options={[
                    { value: "add", label: "Add hours" },
                    { value: "subtract", label: "Subtract hours" },
                    { value: "set", label: "Set total" },
                  ]}
                  searchable={false}
                  disabled={isMutating}
                />
              </label>
            </div>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Hours</span>
              <Input type="number" min={0} step={0.5} value={adjustForm.hours} onChange={(event) => setAdjustForm((current) => ({ ...current, hours: event.target.value }))} disabled={isMutating} />
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Adjustment note</span>
              <Textarea value={adjustForm.note} onChange={(event) => setAdjustForm((current) => ({ ...current, note: event.target.value }))} className="min-h-[96px]" disabled={isMutating} />
            </label>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setIsAdjustModalOpen(false)} disabled={isMutating}>Cancel</Button>
            <Button onClick={() => void handleAdjustBalance()} disabled={isMutating}>Save adjustment</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isSetupModalOpen} onOpenChange={setIsSetupModalOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Setup / admin data</DialogTitle>
            <DialogDescription>Manage PTO types, actions, employees, employee managers, and holidays.</DialogDescription>
          </DialogHeader>

          {setupError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{setupError}</div>
          ) : null}

          <div className="grid gap-3">
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Setup section</span>
              <AppDropdown
                value={setupForm.kind}
                onValueChange={(value) => setSetupForm((current) => ({ ...current, kind: value as SetupForm["kind"] }))}
                options={[
                  { value: "pto_type", label: "PTO Types" },
                  { value: "pto_action", label: "PTO Actions" },
                  { value: "employee", label: "Employees" },
                  { value: "employee_manager", label: "Employee Managers" },
                  { value: "holiday", label: "Holidays" },
                ]}
                searchable={false}
                disabled={isMutating}
              />
            </label>

            {setupForm.kind === "pto_type" ? (
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Type code</span>
                  <AppDropdown
                    value={setupForm.code}
                    onValueChange={(value) => setSetupForm((current) => ({ ...current, code: value }))}
                    options={PTO_TYPE_OPTIONS.map((item) => ({ value: item.value, label: item.label }))}
                    searchable={false}
                    disabled={isMutating}
                  />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Label</span>
                  <Input value={setupForm.label} onChange={(event) => setSetupForm((current) => ({ ...current, label: event.target.value }))} disabled={isMutating} />
                </label>
              </div>
            ) : null}

            {setupForm.kind === "pto_action" ? (
              <div className="grid gap-3">
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Action code (optional)</span>
                  <Input value={setupForm.code} onChange={(event) => setSetupForm((current) => ({ ...current, code: event.target.value }))} disabled={isMutating} />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Action label</span>
                  <Input value={setupForm.label} onChange={(event) => setSetupForm((current) => ({ ...current, label: event.target.value }))} disabled={isMutating} />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Detail</span>
                  <Textarea value={setupForm.detail} onChange={(event) => setSetupForm((current) => ({ ...current, detail: event.target.value }))} className="min-h-[90px]" disabled={isMutating} />
                </label>
              </div>
            ) : null}

            {setupForm.kind === "employee" ? (
              <div className="grid gap-3">
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Employee name</span>
                  <Input value={setupForm.employeeName} onChange={(event) => setSetupForm((current) => ({ ...current, employeeName: event.target.value }))} disabled={isMutating} />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Title</span>
                  <Input value={setupForm.title} onChange={(event) => setSetupForm((current) => ({ ...current, title: event.target.value }))} disabled={isMutating} />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Team region</span>
                  <AppDropdown
                    value={setupForm.teamRegion}
                    onValueChange={(value) => setSetupForm((current) => ({ ...current, teamRegion: value as LeaveSphereTeamRegion }))}
                    options={LEAVESPHERE_TEAM_REGION_OPTIONS}
                    searchable={false}
                    disabled={isMutating}
                  />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Manager</span>
                  <AppDropdown
                    value={setupForm.managerId}
                    onValueChange={(value) => setSetupForm((current) => ({ ...current, managerId: value }))}
                    options={employeeOptions}
                    searchable
                    disabled={isMutating}
                  />
                </label>
              </div>
            ) : null}

            {setupForm.kind === "employee_manager" ? (
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Employee</span>
                  <AppDropdown
                    value={setupForm.employeeId}
                    onValueChange={(value) => setSetupForm((current) => ({ ...current, employeeId: value }))}
                    options={employeeOptions}
                    searchable
                    disabled={isMutating}
                  />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Manager</span>
                  <AppDropdown
                    value={setupForm.managerId}
                    onValueChange={(value) => setSetupForm((current) => ({ ...current, managerId: value }))}
                    options={employeeOptions}
                    searchable
                    disabled={isMutating}
                  />
                </label>
              </div>
            ) : null}

            {setupForm.kind === "holiday" ? (
              <div className="grid gap-3 sm:grid-cols-3">
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Holiday name</span>
                  <Input value={setupForm.name} onChange={(event) => setSetupForm((current) => ({ ...current, name: event.target.value }))} disabled={isMutating} />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Date</span>
                  <Input type="date" value={setupForm.date} onChange={(event) => setSetupForm((current) => ({ ...current, date: event.target.value }))} disabled={isMutating} />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Team region</span>
                  <AppDropdown
                    value={setupForm.teamRegion}
                    onValueChange={(value) => setSetupForm((current) => ({ ...current, teamRegion: value as LeaveSphereTeamRegion }))}
                    options={LEAVESPHERE_TEAM_REGION_OPTIONS}
                    searchable={false}
                    disabled={isMutating}
                  />
                </label>
              </div>
            ) : null}
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setIsSetupModalOpen(false)} disabled={isMutating}>Cancel</Button>
            <Button onClick={() => void handleSetupSave()} disabled={isMutating}>Save setup</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <PageLoadingLayer
        active={loadingContract.pageOverlayActive}
        message={loadingContract.pageOverlayMessage}
      />
    </AppPageLayout>
  );
}
