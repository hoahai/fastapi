import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  CalendarDays,
  Clock3,
  Plus,
  Settings2,
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
import { canModalClose, shouldBlockOutsideClose } from "@tradsphere/components/ui/modal-close-guard";
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";
import { useToast } from "@shell/components/ui/toast";
import { hasAppAdminAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { useOnlineStatus } from "@shared/hooks/useOnlineStatus";
import { shouldFetchNetwork, type CachePolicy } from "@shared/cache";
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
import { ModalCloseButton, ModalShell } from "@shared/components";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import { DEFAULT_TIME_ZONE, formatDateInTimeZone, getCurrentMonthKeyInTimeZone, getCurrentYearInTimeZone, getTodayIsoDateInTimeZone, isIsoDateWithinInclusiveRange, shiftIsoDateByDays } from "@shared/utils/time";
import { TooltipTarget } from "@shared/components/actions/TooltipTarget";
import { LeaveSpherePtoRequestCard } from "@leavesphere/components/LeaveSpherePtoRequestCard";
import { LeaveSphereMonthCalendar, type LeaveSphereMonthCalendarEvent } from "@leavesphere/components/MonthCalendar";
import { LeaveSpherePtoRequestDetailModal, type LeaveSpherePtoRequestFormState } from "@leavesphere/components/PtoRequestDetailModal";
import { LeaveSpherePtoTypeChip } from "@leavesphere/components/PtoTypeChip";
import { LeaveSpherePtoRequestTable } from "@leavesphere/components/LeaveSpherePtoRequestTable";
import {
  buildLeaveSpherePtoEmployeeLookup,
  resolveLeaveSpherePtoEmployeeDisplay,
} from "@leavesphere/lib/ptoEmployeeLookup";
import {
  mergeLeaveSpherePtoEmployeeCacheItems,
  readLeaveSpherePtoEmployeeCacheSnapshot,
  writeLeaveSpherePtoEmployeeCache,
} from "@leavesphere/lib/ptoEmployeeCache";
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
  updateLeaveSphereAdminPtoRequest,
  updateLeaveSphereAdminSetupData,
  type LeaveSphereAdminEmployee,
  type LeaveSphereAdminPtoTypeConfig,
  type LeaveSphereAdminSetupInput,
  type LeaveSphereAdminWorkspaceData,
} from "@leavesphere/lib/adminPtoMocks";
import {
  deriveLeaveSphereAdminEmployeeBalances,
  type LeaveSphereAdminLoadRequest,
  type LeaveSphereAdminPtoActionCode,
} from "@leavesphere/lib/adminPtoBalanceLedger";
import { calculateLeaveSpherePtoHours } from "@leavesphere/lib/ptoHours";
import {
  buildLeaveSpherePtoCalendarRequestChipLabel,
  buildLeaveSpherePtoCalendarRequestTooltipLabel,
} from "@leavesphere/lib/ptoCalendar";
import { getPtoRequestActionConfig } from "@leavesphere/lib/ptoRequestActionConfig";
import {
  getLeaveSphereReviewActionConfirmCopy,
  type LeaveSphereReviewAction,
} from "@leavesphere/lib/reviewActionConfirm";
import { LEAVESPHERE_TEAM_REGION_OPTIONS } from "@leavesphere/lib/ptoMocks";
import type { LeaveSpherePtoRequest, LeaveSpherePtoStatus, LeaveSpherePtoType, LeaveSphereTeamRegion } from "@leavesphere/lib/ptoMocks";
import { ActionIconButton } from "@tradsphere/components/dashboard/ActionIconButton";
import { formatMonthDayYearLabel, formatMonthDayYearRangeLabel, formatPtoRequestDateRangeLabel } from "@leavesphere/lib/ptoDate";
import {
  readLeaveSpherePtoWorkspaceCacheSnapshot,
  writeLeaveSpherePtoWorkspaceCache,
} from "@leavesphere/lib/ptoWorkspaceCache";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type AdminTab = "calendar" | "requests" | "balances" | "setup";

type CreateRequestForm = {
  employeeId: string;
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: string;
  description: string;
};

type PendingCreateRequest = {
  employeeId: string;
  payload: {
    type: LeaveSpherePtoType;
    startDate: string;
    endDate: string;
    hours: number;
    description: string;
  };
};

type AdjustBalanceForm = {
  employeeId: string;
  ptoTypeCode: LeaveSpherePtoType;
  ptoActionCode: LeaveSphereAdminPtoActionCode;
  transactionId: string;
  hours: string;
  approverNote: string;
};

type LegacyAdjustBalanceForm = {
  employeeId: string;
  type: LeaveSpherePtoType;
  mode: "add" | "subtract" | "set";
  hours: string;
  approverNote: string;
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
  description: "",
};

const EMPTY_ADJUST_FORM: AdjustBalanceForm = {
  employeeId: "",
  ptoTypeCode: "vacation",
  ptoActionCode: "load_grant",
  transactionId: "",
  hours: "",
  approverNote: "",
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

function formatDateLabel(isoDate: string, timeZone: string): string {
  if (!isoDate) {
    return "-";
  }
  return formatDateInTimeZone(isoDate, timeZone, {
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

function adjustFormsEqual(left: AdjustBalanceForm, right: AdjustBalanceForm): boolean {
  return (
    left.employeeId === right.employeeId
    && left.ptoTypeCode === right.ptoTypeCode
    && left.ptoActionCode === right.ptoActionCode
    && left.transactionId === right.transactionId
    && Number(left.hours) === Number(right.hours)
    && normalizeOptionalNote(left.approverNote) === normalizeOptionalNote(right.approverNote)
  );
}

function requestTypeLabel(type: string): string {
  const normalized = asString(type).toLowerCase();
  const matched = PTO_TYPE_OPTIONS.find((item) => item.value === normalized);
  if (matched) {
    return matched.label;
  }
  return asString(type).toUpperCase() || "PTO";
}

function buildYearDateBounds(year: number): { minDate: string; maxDate: string } {
  return {
    minDate: `${year}-01-01`,
    maxDate: `${year}-12-31`,
  };
}

function requestIsOut(request: LeaveSpherePtoRequest): boolean {
  return request.status === "approved" || request.status === "pending";
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
    employeeBalances: deriveLeaveSphereAdminEmployeeBalances({
      usageRows: workspace.employeeBalanceUsage,
      transactions: workspace.balanceTransactions,
      year,
    }),
    requests: workspace.requests.filter((item) => requestOverlapsYear(item, year)),
    holidays: workspace.holidays.filter((item) => item.date.startsWith(yearPrefix)),
  };
}

function countUpcomingOutRequests(requests: LeaveSpherePtoRequest[], todayIsoDate: string): number {
  const horizon = shiftIsoDateByDays(todayIsoDate, 14);
  let count = 0;
  for (const request of requests) {
    if (!requestIsOut(request)) {
      continue;
    }
    if (request.endDate < todayIsoDate || request.startDate > horizon) {
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
    && typeof record.description === "string"
  );
}

function isAdjustBalanceForm(value: unknown): value is AdjustBalanceForm {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    typeof record.employeeId === "string"
    && isPtoTypeValue(record.ptoTypeCode)
    && (record.ptoActionCode === "load_grant" || record.ptoActionCode === "adjustment")
    && (typeof record.transactionId === "string" || record.transactionId === undefined)
    && typeof record.hours === "string"
    && typeof record.approverNote === "string"
  );
}

function isLegacyAdjustBalanceForm(value: unknown): value is LegacyAdjustBalanceForm {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    typeof record.employeeId === "string"
    && isPtoTypeValue(record.type)
    && (record.mode === "add" || record.mode === "subtract" || record.mode === "set")
    && typeof record.hours === "string"
    && typeof record.approverNote === "string"
  );
}

function normalizePersistedAdjustBalanceForm(value: unknown): AdjustBalanceForm {
  if (isAdjustBalanceForm(value)) {
    return value;
  }
  if (isLegacyAdjustBalanceForm(value)) {
    return {
      employeeId: value.employeeId,
      ptoTypeCode: value.type,
      ptoActionCode: value.mode === "set" ? "load_grant" : "adjustment",
      transactionId: "",
      hours: value.hours,
      approverNote: value.approverNote,
    };
  }
  return EMPTY_ADJUST_FORM;
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
    && (isAdjustBalanceForm(record.adjustForm) || isLegacyAdjustBalanceForm(record.adjustForm))
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
  const { isOnline } = useOnlineStatus();

  const currentUserId = asString(auth.user?.id) || "local-admin";
  const currentUserName = asString(auth.user?.fullName) || asString(auth.user?.email) || "LeaveSphere Admin";
  const canAdmin = hasAppAdminAccess(auth.accessProfile, "leavesphere");
  const pageStateUserKey = asString(auth.user?.id || auth.user?.email);

  const workspaceKey = useMemo(() => {
    const tenant = asString(auth.tenantSlug) || "default";
    return `leavesphere:admin:${tenant}:${currentUserId}`;
  }, [auth.tenantSlug, currentUserId]);
  const tenantSlug = asString(auth.tenantSlug);
  const canRestorePageState = auth.status === "authenticated" && Boolean(tenantSlug) && Boolean(pageStateUserKey);
  const pageStateScope = useMemo<ScopedPageState | null>(() => {
    if (!canRestorePageState) {
      return null;
    }
    return {
      userKey: pageStateUserKey,
      tenantSlug,
      appCode: LEAVESPHERE_APP_CODE,
      pageCode: LEAVESPHERE_ADMIN_PTO_PAGE_CODE,
    };
  }, [canRestorePageState, pageStateUserKey, tenantSlug]);
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
  const tenantTimeZone = asString(auth.accessProfile?.tenant?.timezone) || DEFAULT_TIME_ZONE;
  const currentYear = useMemo(() => getCurrentYearInTimeZone(tenantTimeZone), [tenantTimeZone]);
  const currentMonthKey = useMemo(() => getCurrentMonthKeyInTimeZone(tenantTimeZone), [tenantTimeZone]);
  const todayIsoDate = useMemo(() => getTodayIsoDateInTimeZone(tenantTimeZone), [tenantTimeZone]);
  const initialTimeZoneRef = useRef(tenantTimeZone);
  const initialYearRef = useRef(currentYear);
  const initialMonthKeyRef = useRef(currentMonthKey);
  const [selectedYear, setSelectedYear] = useState(() => String(currentYear));
  const [loadedYear, setLoadedYear] = useState<number | null>(null);

  const [tab, setTab] = useState<AdminTab>("calendar");
  const [calendarMonth, setCalendarMonth] = useState(() => currentMonthKey);

  const [isInitializing, setIsInitializing] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [isMutating, setIsMutating] = useState(false);

  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [createForm, setCreateForm] = useState<CreateRequestForm>(EMPTY_CREATE_FORM);
  const [pendingCreateRequest, setPendingCreateRequest] = useState<PendingCreateRequest | null>(null);
  const [isCreateDecisionDialogOpen, setIsCreateDecisionDialogOpen] = useState(false);

  const [selectedRequestId, setSelectedRequestId] = useState<string | null>(null);
  const [selectedHolidayId, setSelectedHolidayId] = useState<string | null>(null);
  const [reviewNote, setReviewNote] = useState("");
  const [pendingReviewAction, setPendingReviewAction] = useState<LeaveSphereReviewAction | null>(null);

  const [isAdjustModalOpen, setIsAdjustModalOpen] = useState(false);
  const [adjustForm, setAdjustForm] = useState<AdjustBalanceForm>(EMPTY_ADJUST_FORM);
  const [adjustError, setAdjustError] = useState<string | null>(null);
  const [adjustBaselineForm, setAdjustBaselineForm] = useState<AdjustBalanceForm>(EMPTY_ADJUST_FORM);
  const [isAdjustEditMode, setIsAdjustEditMode] = useState(false);
  const [isAdjustRequestPickerOpen, setIsAdjustRequestPickerOpen] = useState(false);
  const [adjustRequestPickerTarget, setAdjustRequestPickerTarget] = useState<{
    employeeId: string;
    ptoTypeCode: LeaveSpherePtoType;
  } | null>(null);
  const [pendingAdjustAction, setPendingAdjustAction] = useState<"cancel" | null>(null);
  const [isAdjustDiscardDialogOpen, setIsAdjustDiscardDialogOpen] = useState(false);

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
  const adjustWasOpenRef = useRef(false);
  const workspaceLoadRequestTokenRef = useRef(0);

  useEffect(() => {
    if (loadedYear !== null) {
      return;
    }
    if (tenantTimeZone === initialTimeZoneRef.current) {
      return;
    }
    const nextYear = getCurrentYearInTimeZone(tenantTimeZone);
    const nextMonthKey = getCurrentMonthKeyInTimeZone(tenantTimeZone);
    setSelectedYear((current) => (current === String(initialYearRef.current) ? String(nextYear) : current));
    setCalendarMonth((current) => (current === initialMonthKeyRef.current ? nextMonthKey : current));
    initialTimeZoneRef.current = tenantTimeZone;
    initialYearRef.current = nextYear;
    initialMonthKeyRef.current = nextMonthKey;
  }, [loadedYear, tenantTimeZone]);

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
      setAdjustForm(normalizePersistedAdjustBalanceForm(persisted.adjustForm));
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
  const loadedYearDateBounds = useMemo(
    () => (loadedYear === null ? null : buildYearDateBounds(loadedYear)),
    [loadedYear],
  );
  const workspaceForYear = useMemo(() => {
    if (!workspace || !isSelectedYearLoaded || !Number.isInteger(selectedYearNumber)) {
      return null;
    }
    return filterWorkspaceByYear(workspace, selectedYearNumber);
  }, [isSelectedYearLoaded, selectedYearNumber, workspace]);
  const commitWorkspace = useCallback((
    nextWorkspace: LeaveSphereAdminWorkspaceData,
    source: "cache" | "network",
    year: number,
    fetchedAt = Date.now(),
  ) => {
    setWorkspace(nextWorkspace);
    setLoadedYear(year);
    setSelectedYear(String(year));
    setCacheStatus({
      source,
      fetchedAt,
    });
    writeLeaveSpherePtoWorkspaceCache(
      {
        pageCode: "admin-pto",
        tenantSlug,
        userId: currentUserId,
        year,
      },
      nextWorkspace,
      {
        source,
        fetchedAt,
      },
    );
  }, [currentUserId, tenantSlug]);

  const cachedEmployeeItems = useMemo(
    () => readLeaveSpherePtoEmployeeCacheSnapshot({ tenantSlug })?.data ?? [],
    [tenantSlug],
  );
  const employeeLookupById = useMemo(
    () => buildLeaveSpherePtoEmployeeLookup(
      mergeLeaveSpherePtoEmployeeCacheItems(workspaceForYear?.employees, cachedEmployeeItems),
    ),
    [cachedEmployeeItems, workspaceForYear?.employees],
  );
  const resolveRequestEmployee = useCallback(
    (request: LeaveSpherePtoRequest) => resolveLeaveSpherePtoEmployeeDisplay(employeeLookupById, request.employeeId),
    [employeeLookupById],
  );

  useEffect(() => {
    if (!tenantSlug || !workspaceForYear) {
      return;
    }
    writeLeaveSpherePtoEmployeeCache(
      { tenantSlug },
      mergeLeaveSpherePtoEmployeeCacheItems(workspaceForYear?.employees, cachedEmployeeItems),
      { fetchedAt: Date.now() },
    );
  }, [cachedEmployeeItems, tenantSlug, workspaceForYear]);

  const requests = useMemo(
    () => [...(workspaceForYear?.requests ?? [])].sort((left, right) => {
      const leftEmployeeName = resolveRequestEmployee(left).employeeName;
      const rightEmployeeName = resolveRequestEmployee(right).employeeName;

      const startDateCompare = right.startDate.localeCompare(left.startDate);
      if (startDateCompare !== 0) {
        return startDateCompare;
      }

      const employeeCompare = leftEmployeeName.localeCompare(rightEmployeeName);
      if (employeeCompare !== 0) {
        return employeeCompare;
      }

      const typeCompare = left.type.localeCompare(right.type);
      if (typeCompare !== 0) {
        return typeCompare;
      }

      return right.submittedAt.localeCompare(left.submittedAt);
    }),
    [resolveRequestEmployee, workspaceForYear?.requests],
  );
  const pendingRequests = useMemo(
    () => requests.filter((item) => item.status === "pending"),
    [requests],
  );
  const recentRequests = useMemo(
    () => requests,
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
      const employee = resolveRequestEmployee(request);
      const tokens = [
        employee.employeeName,
        requestTypeLabel(request.type),
        statusLabel(request.status),
        formatDateLabel(request.startDate, tenantTimeZone),
        formatDateLabel(request.endDate, tenantTimeZone),
        formatDateLabel(request.submittedAt, tenantTimeZone),
        request.description,
      ];
      return tokens.some((token) => normalizeSearchKeyword(token).includes(keyword));
    });
  }, [normalizedRecentHistorySearch, recentRequests, resolveRequestEmployee]);

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
        todayIsoDate,
      })
      : null),
    [selectedRequest, todayIsoDate],
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
    setReviewNote(selectedRequest.approverNote || "");
  }, [selectedRequest?.id]);

  const selectedRequestManagerLabel = useMemo(() => {
    if (!selectedRequest) {
      return "Unassigned";
    }
    return employeeLookupById.get(selectedRequest.managerId ?? "")?.employeeName || selectedRequest.managerId || "Unassigned";
  }, [employeeLookupById, selectedRequest]);

  const employeeOptions = useMemo(
    () => (workspaceForYear?.employees ?? []).map((item) => ({ value: item.employeeId, label: item.employeeName })),
    [workspaceForYear?.employees],
  );

  const balanceColumns = useMemo(() => {
    const columns = new Map<string, { type: string; label: string }>();
    for (const row of workspaceForYear?.employeeBalances ?? []) {
      for (const balance of row.balances) {
        const type = asString(balance.type);
        if (!type || columns.has(type)) {
          continue;
        }
        columns.set(type, {
          type,
          label: asString(balance.label) || requestTypeLabel(type),
        });
      }
    }
    if (columns.size === 0) {
      for (const item of PTO_TYPE_OPTIONS) {
        columns.set(item.value, { type: item.value, label: item.label });
      }
    }
    return [...columns.values()];
  }, [workspaceForYear?.employeeBalances]);
  const loadRequestsByBalanceKey = useMemo(() => {
    const requestsByKey = new Map<string, LeaveSphereAdminLoadRequest[]>();
    if (typeof loadedYear !== "number") {
      return requestsByKey;
    }

    for (const transaction of workspaceForYear?.balanceTransactions ?? []) {
      if (
        transaction.year !== loadedYear
        || transaction.status !== "Approved"
        || transaction.ptoActionCode !== "load_grant"
      ) {
        continue;
      }
      const key = `${transaction.employeeId}::${transaction.ptoTypeCode}`;
      const currentRequests = requestsByKey.get(key);
      if (currentRequests) {
        currentRequests.push(transaction);
        continue;
      }
      requestsByKey.set(key, [transaction]);
    }

    for (const requests of requestsByKey.values()) {
      requests.sort((left, right) => right.createdAt.localeCompare(left.createdAt));
    }

    return requestsByKey;
  }, [loadedYear, workspaceForYear?.balanceTransactions]);

  const ptoTypeOptions = useMemo(
    () => (workspaceForYear?.ptoTypes ?? []).filter((item) => item.active).map((item) => ({ value: item.code, label: item.label })),
    [workspaceForYear?.ptoTypes],
  );
  const resolveAdjustLoadRequests = useCallback((employeeId: string, ptoTypeCode: LeaveSpherePtoType) => {
    if (!employeeId || !ptoTypeCode) {
      return [];
    }
    return loadRequestsByBalanceKey.get(`${employeeId}::${ptoTypeCode}`) || [];
  }, [loadRequestsByBalanceKey]);
  const openAdjustRequestEditor = useCallback((request: { id: string; employeeId: string; ptoTypeCode: LeaveSpherePtoType; hours: number }) => {
    setAdjustRequestPickerTarget(null);
    setIsAdjustRequestPickerOpen(false);
    setAdjustError(null);
    setPendingAdjustAction(null);
    setIsAdjustEditMode(true);
    const nextForm = {
      ...EMPTY_ADJUST_FORM,
      employeeId: request.employeeId,
      ptoTypeCode: request.ptoTypeCode,
      transactionId: request.id,
      hours: String(request.hours),
    };
    setAdjustForm(nextForm);
    setAdjustBaselineForm(nextForm);
    setIsAdjustModalOpen(true);
  }, []);
  const openAdjustRequestPicker = useCallback((employeeId: string, ptoTypeCode: LeaveSpherePtoType) => {
    setAdjustError(null);
    setPendingAdjustAction(null);
    setAdjustRequestPickerTarget({ employeeId, ptoTypeCode });
    setIsAdjustEditMode(true);
    setIsAdjustModalOpen(false);
    setIsAdjustRequestPickerOpen(true);
  }, []);
  const selectedAdjustRequestList = useMemo(() => {
    return resolveAdjustLoadRequests(adjustForm.employeeId, adjustForm.ptoTypeCode);
  }, [adjustForm.employeeId, adjustForm.ptoTypeCode, resolveAdjustLoadRequests]);
  const selectedAdjustRequest = useMemo(() => {
    if (!isAdjustEditMode) {
      return null;
    }
    return selectedAdjustRequestList.find((item) => item.id === adjustForm.transactionId) || selectedAdjustRequestList[0] || null;
  }, [adjustForm.transactionId, isAdjustEditMode, selectedAdjustRequestList]);
  const adjustRequestPickerRequests = useMemo(() => {
    if (!adjustRequestPickerTarget) {
      return [];
    }
    return resolveAdjustLoadRequests(adjustRequestPickerTarget.employeeId, adjustRequestPickerTarget.ptoTypeCode);
  }, [adjustRequestPickerTarget, resolveAdjustLoadRequests]);
  const hasAdjustFormChanges = useMemo(
    () => !adjustFormsEqual(adjustForm, adjustBaselineForm),
    [adjustBaselineForm, adjustForm],
  );
  const isAdjustFormValid = useMemo(
    () => Boolean(
      Number.isInteger(loadedYear)
      && asString(adjustForm.employeeId)
      && asNumber(adjustForm.hours) > 0
      && (!isAdjustEditMode || Boolean(adjustForm.transactionId))
    ),
    [adjustForm.employeeId, adjustForm.hours, adjustForm.transactionId, isAdjustEditMode, loadedYear],
  );
  const canSubmitAdjustForm = useMemo(
    () => hasAdjustFormChanges && isAdjustFormValid,
    [hasAdjustFormChanges, isAdjustFormValid],
  );
  const shouldShowAdjustSubmitButton = useMemo(
    () => Boolean(canSubmitAdjustForm || isMutating),
    [canSubmitAdjustForm, isMutating],
  );
  const shouldShowAdjustCancelButton = useMemo(
    () => Boolean(isAdjustEditMode && selectedAdjustRequest),
    [isAdjustEditMode, selectedAdjustRequest],
  );
  const adjustDialogTitle = useMemo(
    () => (isAdjustEditMode ? "Loaded PTO Hours Detail" : "Load PTO Hours"),
    [isAdjustEditMode],
  );
  const adjustSubmitLabel = useMemo(
    () => (isAdjustEditMode ? "Save changes" : "Load Hours"),
    [isAdjustEditMode],
  );
  const pendingAdjustActionCopy = useMemo(() => {
    if (!pendingAdjustAction) {
      return null;
    }
    return {
      title: "Cancel Load Request?",
      description: "This will cancel the selected approved PTO load request and remove it from the balance.",
      confirmLabel: "Cancel request",
    };
  }, [pendingAdjustAction]);

  const resolvePtoHoursForEmployee = useCallback((startDate: string, endDate: string, employeeId: string) => {
    const employeeRegion = (workspaceForYear?.employees ?? []).find((item) => item.employeeId === employeeId)?.teamRegion;
    const employeeHolidayDates = new Set(
      (workspaceForYear?.holidays ?? [])
        .filter((item) => !employeeRegion || item.teamRegion === employeeRegion)
        .map((item) => item.date),
    );
    return calculateLeaveSpherePtoHours(startDate, endDate, employeeHolidayDates);
  }, [workspaceForYear?.employees, workspaceForYear?.holidays]);
  const overview = useMemo(() => {
    return {
      pendingCount: pendingRequests.length,
      upcomingOutCount: countUpcomingOutRequests(requests, todayIsoDate),
      employeeCount: workspaceForYear?.employees.filter((item) => item.active).length ?? 0,
      holidayCount: workspaceForYear?.holidays.length ?? 0,
    };
  }, [pendingRequests.length, requests, todayIsoDate, workspaceForYear?.employees, workspaceForYear?.holidays.length]);
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
      return "Refreshing LeaveSphere Leave Management workspace...";
    }
    if (loadedYear === null) {
      return "Select a year and click Load.";
    }
    if (!cacheStatus) {
      return "Loading LeaveSphere Leave Management workspace...";
    }
    if (!isOnline) {
      return `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
    }
    return `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [cacheStatus, isOnline, isRefreshing, loadedYear]);

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
      pageInitializing: "Preparing LeaveSphere Leave Management workspace...",
      pageRefreshing: "Refreshing Leave Management workspace...",
      cacheChipRefreshing: "Refreshing Leave Management workspace...",
      sectionLoading: "Applying admin changes...",
    },
  );

  const loadWorkspace = useCallback(async (year: number, policy: CachePolicy = "stale-while-revalidate"): Promise<boolean> => {
    const requestToken = ++workspaceLoadRequestTokenRef.current;
    const cacheSnapshot = readLeaveSpherePtoWorkspaceCacheSnapshot<LeaveSphereAdminWorkspaceData>({
      pageCode: "admin-pto",
      tenantSlug,
      userId: currentUserId,
      year,
    });
    const cachedWorkspace = cacheSnapshot?.data ?? null;
    const hasCachedWorkspace = Boolean(cachedWorkspace);
    const shouldUseCache = policy !== "network-only" && hasCachedWorkspace;
    const shouldFetchFromNetwork = shouldFetchNetwork(policy, cacheSnapshot);
    const shouldShowRefreshing = hasCachedWorkspace || (loadedYear === year && workspaceForYear !== null);

    if (shouldUseCache && cachedWorkspace) {
      commitWorkspace(cachedWorkspace, "cache", year, cacheSnapshot?.fetchedAt ?? Date.now());
      setRefreshMessage(null);
    }

    if (!shouldFetchFromNetwork) {
      setIsInitializing(false);
      setIsRefreshing(false);
      setPageErrorMessage(null);
      return true;
    }

    if (!isOnline) {
      setIsInitializing(false);
      setIsRefreshing(false);
      if (hasCachedWorkspace && cachedWorkspace) {
        commitWorkspace(cachedWorkspace, "cache", year, cacheSnapshot?.fetchedAt ?? Date.now());
        setRefreshMessage("You're offline. Showing cached Leave Management workspace.");
        return true;
      }
      setPageErrorMessage("You're offline. Connect to the internet to load Leave Management workspace.");
      return false;
    }

    setIsInitializing(!shouldShowRefreshing);
    setIsRefreshing(shouldShowRefreshing);
    setPageErrorMessage(null);
    if (shouldShowRefreshing && hasCachedWorkspace) {
      setRefreshMessage("Cached Leave Management workspace shown while refreshing.");
    } else {
      setRefreshMessage(null);
    }

    try {
      const result = await loadLeaveSphereAdminPtoWorkspace({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        timeZone: tenantTimeZone,
        freshData: policy !== "network-only",
      });
      if (requestToken !== workspaceLoadRequestTokenRef.current) {
        return false;
      }
      commitWorkspace(result.workspace, result.source === "mock" ? "cache" : "network", year, Date.now());
      setRefreshMessage(result.refreshMessage);
      return true;
    } catch {
      if (requestToken !== workspaceLoadRequestTokenRef.current) {
        return false;
      }
      if (hasCachedWorkspace && cachedWorkspace) {
        commitWorkspace(cachedWorkspace, "cache", year, cacheSnapshot?.fetchedAt ?? Date.now());
        setRefreshMessage("Showing cached Leave Management workspace. Could not refresh.");
        return true;
      }
      setPageErrorMessage("Unable to load Leave Management workspace right now. Please try again.");
      return false;
    } finally {
      if (requestToken === workspaceLoadRequestTokenRef.current) {
        setIsInitializing(false);
        setIsRefreshing(false);
      }
    }
  }, [
    commitWorkspace,
    currentUserId,
    currentUserName,
    isOnline,
    loadedYear,
    requestJson,
    tenantSlug,
    tenantTimeZone,
    workspaceForYear,
    workspaceKey,
  ]);

  useEffect(() => {
    if (!hasHydratedPageState || !pageStateStorageKey || loadedYear === null) {
      return;
    }
    if (restoredWorkspaceScopeRef.current === pageStateStorageKey) {
      return;
    }
    restoredWorkspaceScopeRef.current = pageStateStorageKey;
    void loadWorkspace(loadedYear, "stale-while-revalidate");
  }, [hasHydratedPageState, loadedYear, loadWorkspace, pageStateStorageKey]);

  const handleLoadByYear = useCallback(async () => {
    const parsedYear = Number(selectedYear);
    if (!Number.isInteger(parsedYear)) {
      return;
    }
    const didLoad = await loadWorkspace(parsedYear, loadedYear === parsedYear ? "network-only" : "cache-first");
    if (didLoad) {
      const defaultMonth = parsedYear === currentYear ? Number(currentMonthKey.slice(5, 7)) : 1;
      setCalendarMonth(`${parsedYear}-${String(defaultMonth).padStart(2, "0")}`);
      applyRecentHistorySearchKeyword("");
    }
  }, [applyRecentHistorySearchKeyword, currentMonthKey, currentYear, loadWorkspace, loadedYear, selectedYear]);

  const openCreateRequestModal = useCallback(() => {
    if (!loadedYearDateBounds) {
      return;
    }
    setPendingCreateRequest(null);
    setIsCreateDecisionDialogOpen(false);
    const defaultIsoDate = todayIsoDate >= loadedYearDateBounds.minDate && todayIsoDate <= loadedYearDateBounds.maxDate
      ? todayIsoDate
      : loadedYearDateBounds.minDate;
    const defaultEmployeeId = employeeOptions[0]?.value || "";
    setCreateForm({
      ...EMPTY_CREATE_FORM,
      employeeId: defaultEmployeeId,
      type: (ptoTypeOptions[0]?.value as LeaveSpherePtoType) || "vacation",
      startDate: defaultIsoDate,
      endDate: defaultIsoDate,
      hours: resolvePtoHoursForEmployee(defaultIsoDate, defaultIsoDate, defaultEmployeeId),
    });
    setIsCreateModalOpen(true);
  }, [employeeOptions, loadedYearDateBounds, ptoTypeOptions, resolvePtoHoursForEmployee, todayIsoDate]);

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
      ptoTypeCode: current.ptoTypeCode || (ptoTypeOptions[0]?.value || "vacation"),
    }));
  }, [employeeOptions, isAdjustModalOpen, ptoTypeOptions]);

  useEffect(() => {
    const didJustOpen = isAdjustModalOpen && !adjustWasOpenRef.current;
    adjustWasOpenRef.current = isAdjustModalOpen;
    if (!didJustOpen) {
      return;
    }
    setAdjustError(null);
    setIsAdjustDiscardDialogOpen(false);
    setAdjustBaselineForm({ ...adjustForm });
  }, [adjustForm, isAdjustModalOpen]);

  useEffect(() => {
    if (!isAdjustModalOpen || !isAdjustEditMode) {
      return;
    }
    const resolvedRequest = selectedAdjustRequestList.find((item) => item.id === adjustForm.transactionId) || selectedAdjustRequestList[0] || null;
    setAdjustForm((current) => {
      if (!resolvedRequest) {
        if (!current.transactionId && !current.hours) {
          return current;
        }
        return {
          ...current,
          transactionId: "",
          hours: "",
        };
      }
      const nextHours = String(resolvedRequest.hours);
      if (current.transactionId === resolvedRequest.id && current.hours === nextHours) {
        return current;
      }
      return {
        ...current,
        transactionId: resolvedRequest.id,
        hours: nextHours,
      };
    });
  }, [adjustForm.transactionId, isAdjustEditMode, isAdjustModalOpen, selectedAdjustRequestList]);

  const applyWorkspace = useCallback((next: LeaveSphereAdminWorkspaceData, source: "mock" | "network") => {
    const effectiveYear = loadedYear ?? (Number.isInteger(selectedYearNumber) ? selectedYearNumber : currentYear);
    commitWorkspace(next, source === "mock" ? "cache" : "network", effectiveYear);
  }, [commitWorkspace, currentYear, loadedYear, selectedYearNumber]);

  const openLoadHoursModal = useCallback((params?: {
    employeeId?: string;
    ptoTypeCode?: LeaveSpherePtoType;
    hours?: number;
    mode?: "create" | "edit";
    transactionId?: string;
  }) => {
    const mode = params?.mode || "create";
    const isEditMode = mode === "edit";
    const employeeId = params?.employeeId || employeeOptions[0]?.value || "";
    const ptoTypeCode = params?.ptoTypeCode || (ptoTypeOptions[0]?.value as LeaveSpherePtoType) || "vacation";
    const loadRequests = isEditMode ? resolveAdjustLoadRequests(employeeId, ptoTypeCode) : [];
    const nextTransactionId = isEditMode ? (params?.transactionId || loadRequests[0]?.id || "") : "";
    const selectedRequest = loadRequests.find((item) => item.id === nextTransactionId) || loadRequests[0] || null;
    const nextHours = isEditMode ? (params?.hours ?? selectedRequest?.hours) : undefined;
    const nextForm = {
      ...EMPTY_ADJUST_FORM,
      employeeId,
      ptoTypeCode,
      transactionId: nextTransactionId,
      hours: nextHours === undefined ? "" : String(nextHours),
    };

    setAdjustError(null);
    setPendingAdjustAction(null);
    setIsAdjustEditMode(isEditMode);
    setAdjustRequestPickerTarget(null);
    setAdjustForm(nextForm);
    setAdjustBaselineForm(nextForm);
    setIsAdjustModalOpen(true);
  }, [employeeOptions, ptoTypeOptions, resolveAdjustLoadRequests]);

  const handlePromptCreateRequest = useCallback(async ({ payload }: {
    requestId: string | null;
    payload: {
      type: LeaveSpherePtoType;
      startDate: string;
      endDate: string;
      hours: number;
      description: string;
    };
  }) => {
    const employeeId = asString(createForm.employeeId);
    if (!employeeId) {
      toast.error("Create failed", "Employee is required.");
      return false;
    }
    setPendingCreateRequest({
      employeeId,
      payload,
    });
    setIsCreateDecisionDialogOpen(true);
    return false;
  }, [
    createForm.employeeId,
    toast,
  ]);

  const handleExecuteCreateRequest = useCallback(async (approveImmediately: boolean) => {
    const nextPendingCreateRequest = pendingCreateRequest;
    if (!nextPendingCreateRequest) {
      setIsCreateDecisionDialogOpen(false);
      return;
    }

    setIsMutating(true);
    try {
      const createdResult = await createLeaveSphereAdminPtoRequest({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        payload: {
          employeeId: nextPendingCreateRequest.employeeId,
          ...nextPendingCreateRequest.payload,
        },
      });
      applyWorkspace(createdResult.workspace, createdResult.source);

      if (approveImmediately) {
        const createdRequestId = createdResult.createdRequestId || createdResult.workspace.requests[0]?.id || null;
        if (createdRequestId) {
          const approvedResult = await reviewLeaveSphereAdminPtoRequest({
            requestJson,
            workspaceKey,
            currentUserId,
          currentUserName,
          payload: {
            requestId: createdRequestId,
            approve: true,
            approverNote: "",
          },
        });
          applyWorkspace(approvedResult.workspace, approvedResult.source);
          toast.success("Request approved", "PTO request was created and approved.");
        } else {
          toast.error("Approval failed", "The request was created, but the approval target could not be resolved.");
        }
      } else {
        toast.success("Request created", "PTO request was created on behalf of the selected employee.");
      }

      setIsCreateModalOpen(false);
      setCreateForm(EMPTY_CREATE_FORM);
    } catch {
      toast.error(
        approveImmediately ? "Create and approve failed" : "Create failed",
        approveImmediately
          ? "Unable to create and approve PTO request right now."
          : "Unable to create PTO request right now.",
      );
    } finally {
      setPendingCreateRequest(null);
      setIsCreateDecisionDialogOpen(false);
      setIsMutating(false);
    }
  }, [
    applyWorkspace,
    currentUserId,
    currentUserName,
    pendingCreateRequest,
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
        const reviewedAt = todayIsoDate;
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
                    approverNote: normalizeOptionalNote(reviewNote) || null,
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
          approverNote: reviewNote,
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
    todayIsoDate,
    toast,
    workspaceKey,
  ]);

  const handleCancelReviewRequest = useCallback(async () => {
    if (!selectedRequest) {
      return;
    }
    setIsMutating(true);
    try {
      const reviewedAt = todayIsoDate;
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
                  approverNote: normalizeOptionalNote(reviewNote) || null,
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
  }, [currentUserName, reviewNote, selectedRequest, todayIsoDate, toast]);

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
      setReviewNote(selectedRequest.approverNote || "");
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

  const closeAdjustModal = useCallback((discardChanges = false) => {
    if (discardChanges) {
      setAdjustForm(adjustBaselineForm);
    }
    setAdjustError(null);
    setPendingAdjustAction(null);
    setIsAdjustEditMode(false);
    setIsAdjustRequestPickerOpen(false);
    setAdjustRequestPickerTarget(null);
    setIsAdjustModalOpen(false);
  }, [adjustBaselineForm]);

  const handleRevertAdjustForm = useCallback(() => {
    if (!hasAdjustFormChanges || isMutating) {
      return;
    }
    setAdjustForm(adjustBaselineForm);
    setAdjustError(null);
    setPendingAdjustAction(null);
  }, [adjustBaselineForm, hasAdjustFormChanges, isMutating]);

  const handleAdjustModalOpenChange = useCallback((nextOpen: boolean) => {
    const allowClose = canModalClose({
      nextOpen,
      isBusy: isMutating,
      hasUnsavedChanges: hasAdjustFormChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasAdjustFormChanges && !isMutating) {
        setIsAdjustDiscardDialogOpen(true);
      }
      return;
    }
    if (!nextOpen) {
      closeAdjustModal(false);
      return;
    }
    setIsAdjustModalOpen(true);
  }, [closeAdjustModal, hasAdjustFormChanges, isMutating]);

  const handleAdjustBalance = useCallback(async () => {
    if (!asString(adjustForm.employeeId)) {
      setAdjustError("Employee is required.");
      return;
    }
    if (adjustForm.hours === "") {
      setAdjustError("Hours are required.");
      return;
    }
    if (isAdjustEditMode && !selectedAdjustRequest) {
      setAdjustError("Select a load request to adjust.");
      return;
    }
    if (!Number.isInteger(loadedYear)) {
      setAdjustError("Load a year before recording PTO hours.");
      return;
    }

    const transactionYear = Number(loadedYear);
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
          ptoTypeCode: adjustForm.ptoTypeCode,
          ptoActionCode: adjustForm.ptoActionCode,
          transactionId: selectedAdjustRequest?.id || null,
          hours: asNumber(adjustForm.hours),
          year: transactionYear,
          status: "Approved",
          approverNote: asString(adjustForm.approverNote),
        },
      });
      applyWorkspace(result.workspace, result.source);
      setIsAdjustModalOpen(false);
      setAdjustForm(EMPTY_ADJUST_FORM);
      toast.success("PTO hours updated", "Loaded PTO hours were updated for the selected employee.");
    } catch {
      toast.error("Update failed", "Unable to update PTO balance hours right now.");
    } finally {
      setIsMutating(false);
    }
  }, [
    adjustForm.employeeId,
    adjustForm.hours,
    adjustForm.approverNote,
    adjustForm.ptoActionCode,
    adjustForm.ptoTypeCode,
    applyWorkspace,
    currentUserId,
    currentUserName,
    loadedYear,
    requestJson,
    isAdjustEditMode,
    toast,
    workspaceKey,
  ]);

  const handleCancelAdjustRequest = useCallback(async () => {
    if (!selectedAdjustRequest || !Number.isInteger(loadedYear)) {
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
          employeeId: selectedAdjustRequest.employeeId,
          ptoTypeCode: selectedAdjustRequest.ptoTypeCode,
          ptoActionCode: "load_grant",
          transactionId: selectedAdjustRequest.id,
          hours: selectedAdjustRequest.hours,
          year: Number(loadedYear),
          status: "Canceled",
          approverNote: asString(adjustForm.approverNote),
        },
      });
      applyWorkspace(result.workspace, result.source);
      closeAdjustModal(false);
      setAdjustForm(EMPTY_ADJUST_FORM);
      toast.success("Load request canceled", "Approved PTO load request was canceled.");
    } catch {
      toast.error("Cancel failed", "Unable to cancel PTO load request right now.");
    } finally {
      setPendingAdjustAction(null);
      setIsMutating(false);
    }
  }, [
    adjustForm.approverNote,
    applyWorkspace,
    closeAdjustModal,
    currentUserId,
    currentUserName,
    loadedYear,
    requestJson,
    selectedAdjustRequest,
    toast,
    workspaceKey,
  ]);

  const handleConfirmAdjustAction = useCallback(async () => {
    if (pendingAdjustAction === "cancel") {
      await handleCancelAdjustRequest();
    }
  }, [handleCancelAdjustRequest, pendingAdjustAction]);

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
        description: string;
      };
  }) => {
    if (!params.requestId) {
      return;
    }
    setIsMutating(true);
    try {
      const result = await updateLeaveSphereAdminPtoRequest({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        payload: {
          transactionId: params.requestId,
          type: params.payload.type,
          startDate: params.payload.startDate,
          endDate: params.payload.endDate,
          hours: params.payload.hours,
          description: params.payload.description,
          year: loadedYear,
        },
      });
      applyWorkspace(result.workspace, result.source);
      setSelectedRequestId(null);
      setReviewNote("");
      toast.success("Request updated", "PTO request details were updated.");
    } finally {
      setIsMutating(false);
    }
  }, [applyWorkspace, currentUserId, currentUserName, loadedYear, requestJson, toast, workspaceKey]);
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
      const employee = resolveRequestEmployee(request);
      const requestType = requestTypeLabel(request.type);
      const hoursLabel = formatHoursLabel(request.hours);
      const chipLabel = buildLeaveSpherePtoCalendarRequestChipLabel(
        employee.employeeName,
        requestType,
        hoursLabel,
        request.description,
      );
      calendarEvents.push({
        id: `request:${request.id}`,
        label: chipLabel,
        tone: mapLeaveSpherePtoStatusToChipTone(request.status),
        startDate: request.startDate,
        endDate: request.endDate,
        title: buildLeaveSpherePtoCalendarRequestTooltipLabel(
          employee.employeeName,
          requestType,
          hoursLabel,
          request.description,
        ),
      });
    }

    return (
      <LeaveSphereMonthCalendar
        title="Month Calendar"
        description="Who is out and company holidays"
        monthKey={calendarMonth}
        onMonthChange={setCalendarMonth}
        minMonthKey={loadedYearDateBounds?.minDate.slice(0, 7)}
        maxMonthKey={loadedYearDateBounds?.maxDate.slice(0, 7)}
        todayIsoDate={todayIsoDate}
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
          title="Pending Requests"
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
                <LeaveSpherePtoRequestCard
                  key={request.id}
                  onClick={() => {
                    setSelectedRequestId(request.id);
                    setReviewNote(request.approverNote || "");
                  }}
                  employeeName={resolveRequestEmployee(request).employeeName}
                  title={resolveRequestEmployee(request).employeeName}
                  pictureUrl={resolveRequestEmployee(request).pictureUrl}
                  typeChip={<LeaveSpherePtoTypeChip type={request.type} label={requestTypeLabel(request.type)} />}
                  statusChip={<LeaveSpherePtoStatusChip status={request.status} label={statusLabel(request.status)} />}
                  dateLabel={formatPtoRequestDateRangeLabel(request.startDate, request.endDate, tenantTimeZone)}
                  detailLabel={request.description}
                  hoursLabel={formatHoursLabel(request.hours)}
                  submittedLabel={`Submitted ${formatMonthDayYearLabel(request.submittedAt, tenantTimeZone)}`}
                />
              ))
            )}
          </div>
        </SectionCard>

      <SectionCard
        title="Request History"
        description="All PTO requests loaded for the selected year"
        actions={(
          <ActionIconButton
            tooltip="Create Request"
            onClick={() => {
              openCreateRequestModal();
            }}
            icon={<Plus />}
          />
        )}
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

          <LeaveSpherePtoRequestTable
            requests={filteredRecentRequests}
            emptyMessage={recentRequests.length === 0
              ? "No requests found."
              : "No recent requests match your keyword filter."}
            resolveEmployee={resolveRequestEmployee}
            requestTypeLabel={requestTypeLabel}
            statusLabel={statusLabel}
            formatSubmittedLabel={(request) => `Submitted ${formatDateInTimeZone(request.submittedAt, tenantTimeZone, {
              month: "numeric",
              day: "numeric",
              year: "numeric",
            })}`}
            formatDateRangeLabel={(request) => formatMonthDayYearRangeLabel(request.startDate, request.endDate, tenantTimeZone)}
            formatHoursLabel={formatHoursLabel}
            onRequestClick={(request) => {
              setSelectedRequestId(request.id);
              setReviewNote(request.approverNote || "");
            }}
            showDescription
            isRowHighlighted={(request) => isIsoDateWithinInclusiveRange(
              request.startDate,
              todayIsoDate,
              shiftIsoDateByDays(todayIsoDate, 7),
            )}
            employeeColumnClassName="w-[19rem] px-3 py-2.5"
            dateRangeColumnClassName="w-[18rem] whitespace-nowrap px-3 py-2.5"
            descriptionColumnClassName="px-3 py-2.5 text-slate-600"
          />
        </SectionCard>
      </div>
    );
  };

  const renderBalancesTab = () => {
    return (
      <SectionCard
        title="Employee PTO Balances"
        description="Click a balance to edit loaded PTO hours by employee"
        actions={(
          <ActionIconButton
            tooltip="Load PTO Hours"
            onClick={() => {
              openLoadHoursModal({ mode: "create" });
            }}
            disabled={!workspaceForYear}
            icon={<Plus />}
          />
        )}
      >
        <div className="overflow-x-auto rounded-xl border border-blue-100">
          <table className="min-w-full text-left text-sm">
            <thead className="bg-blue-50/70 text-xs uppercase tracking-[0.08em] text-slate-600">
              <tr>
                <th className="px-3 py-2.5">Employee</th>
                {balanceColumns.map((column) => (
                  <th key={column.type} className="px-3 py-2.5 text-center">{column.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(workspaceForYear?.employeeBalances ?? []).map((row) => {
                const balanceByType = new Map(row.balances.map((item) => [item.type, item]));
                const renderCell = (type: string) => {
                  const balance = balanceByType.get(type);
                  if (!balance) {
                    return "-";
                  }
                  const usedHours = balance.usedHours + balance.scheduledHours;
                  const usedRatio = balance.totalHours > 0
                    ? Math.min(1, usedHours / balance.totalHours)
                    : 0;
                  const loadRequests = resolveAdjustLoadRequests(row.employeeId, type as LeaveSpherePtoType);
                  return (
                    <TooltipTarget text="click to edit hours">
                      <button
                        type="button"
                        onClick={() => {
                          if (loadRequests.length > 1) {
                            openAdjustRequestPicker(row.employeeId, type as LeaveSpherePtoType);
                            return;
                          }

                          const latestRequest = loadRequests[0] || null;
                          if (latestRequest) {
                            openAdjustRequestEditor(latestRequest);
                            return;
                          }

                          openLoadHoursModal({
                            mode: "create",
                            employeeId: row.employeeId,
                            ptoTypeCode: type as LeaveSpherePtoType,
                          });
                        }}
                        className="group flex w-full flex-col items-center rounded-lg border border-transparent px-2 py-1.5 text-center transition hover:border-blue-200 hover:bg-blue-50/55 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300"
                        aria-label={`Edit ${requestTypeLabel(type)} hours for ${row.employeeName}`}
                      >
                        <span className="text-sm font-semibold text-slate-900 transition group-hover:text-blue-700">
                          {formatHoursLabel(usedHours)}
                          {" / "}
                          {formatHoursLabel(balance.totalHours)}
                        </span>
                        <span className="mt-1.5 h-1.5 w-full overflow-hidden rounded-full bg-blue-100">
                          <span
                            className="block h-full rounded-full bg-blue-500 transition-[width] duration-300 ease-out"
                            style={{ width: `${usedRatio * 100}%` }}
                          />
                        </span>
                      </button>
                    </TooltipTarget>
                  );
                };
                return (
                  <tr key={row.employeeId} className="border-t border-blue-100/80 bg-white text-slate-700">
                    <td className="px-3 py-2.5 font-medium text-slate-900">{row.employeeName}</td>
                    {balanceColumns.map((column) => (
                      <td key={column.type} className="px-3 py-2.5 text-center">{renderCell(column.type)}</td>
                    ))}
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
          title="Setup Data"
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
          title="Employees / Managers / Holidays"
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
                  <p className="text-xs text-slate-600">{formatDateLabel(item.date, tenantTimeZone)}</p>
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
            title="Leave Management"
            description="This page requires LeaveSphere admin access."
            gradientVariant="workspace"
          />
        )}
      >
        <SectionCard title="Access Required" description="Only LeaveSphere admins can access this workspace.">
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
        title="Leave Management"
        description="Admin overview of PTO activity, requests, balances, and setup data."
        gradientVariant="workspace"
      />
      )}
      footer={cacheStatus && loadedYear !== null ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            setIsChipRefreshOverlayVisible(true);
            void loadWorkspace(loadedYear ?? currentYear, "network-only").finally(() => {
              setIsChipRefreshOverlayVisible(false);
            });
          }}
          disabled={isInitializing || isRefreshing || isMutating || !isOnline}
          refreshing={isRefreshing || isChipRefreshOverlayVisible}
          refreshLabel="Refresh Leave Management workspace"
          tooltipText={isOnline ? "Click to refresh requests, balances, and setup data" : "Offline. Reconnect to refresh Leave Management workspace."}
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
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-amber-800">Pending Requests</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-amber-900">{overview.pendingCount}</p>
            </article>
            <article className="rounded-2xl border border-blue-200 bg-blue-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-blue-800">Upcoming PTO</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-blue-900">{overview.upcomingOutCount}</p>
            </article>
            <article className="rounded-2xl border border-violet-200 bg-violet-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-violet-800">Total Employees</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-violet-900">{overview.employeeCount}</p>
            </article>
            <article className="rounded-2xl border border-indigo-200 bg-indigo-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-indigo-800">Holiday Marks</p>
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

      <LeaveSpherePtoRequestDetailModal
        mode="create"
        open={isCreateModalOpen}
        request={null}
        initialForm={createForm as LeaveSpherePtoRequestFormState}
        title="Submit PTO Request"
        description="Enter request details. Your manager can approve or reject from the Manager PTO queue."
        ptoTypeOptions={ptoTypeOptions}
        statusLabel={statusLabel}
        saving={isMutating}
        calculateHours={(startDate, endDate) => resolvePtoHoursForEmployee(startDate, endDate, createForm.employeeId)}
        saveLabel="Submit request"
        onOpenChange={setIsCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onFormChange={(nextForm) => setCreateForm((current) => ({ ...current, ...nextForm }))}
        onSubmit={handlePromptCreateRequest}
        canSubmitOverride={Boolean(createForm.employeeId)}
        allowedDateRange={loadedYearDateBounds ?? undefined}
        details={(
          <label className="block space-y-1 text-sm">
            <span className="text-slate-600">Employee</span>
            <AppDropdown
              value={createForm.employeeId}
              onValueChange={(value) => {
                setCreateForm((current) => ({
                  ...current,
                  employeeId: value,
                  hours: resolvePtoHoursForEmployee(current.startDate, current.endDate, value),
                }));
              }}
              options={employeeOptions}
              searchable
              disabled={isMutating}
            />
          </label>
        )}
      />

      <Dialog open={isCreateDecisionDialogOpen} onOpenChange={(open) => {
        setIsCreateDecisionDialogOpen(open);
        if (!open) {
          setPendingCreateRequest(null);
        }
      }}>
        <DialogContent className="max-w-lg">
          <DialogClose asChild aria-label="Close submit choice dialog" disabled={isMutating}>
            <ModalCloseButton icon={<X className="size-4" />} className="absolute right-0 top-0 z-20" />
          </DialogClose>
          <DialogHeader className="pr-8">
            <DialogTitle>Submit PTO Request?</DialogTitle>
            <DialogDescription>
              Choose whether this admin-created request should be submitted as pending or submitted and approved immediately.
            </DialogDescription>
          </DialogHeader>

          <DialogFooter className="gap-2 sm:gap-2">
            <Button
              variant="outline"
              onClick={() => void handleExecuteCreateRequest(false)}
              disabled={isMutating}
            >
              {isMutating ? "Submitting..." : "Just submit"}
            </Button>
            <Button
              onClick={() => void handleExecuteCreateRequest(true)}
              disabled={isMutating}
            >
              {isMutating ? "Submitting..." : "Submit and approve"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <LeaveSpherePtoRequestDetailModal
        open={Boolean(selectedRequest)}
        request={selectedRequest}
        layoutVariant="my-pto-detail"
        readOnly={!selectedRequestActionConfig?.canEditForm}
        title="Request Detail"
        description="Review and update employee PTO request details."
        ptoTypeOptions={PTO_TYPE_OPTIONS.map((item) => ({ value: item.value, label: item.label }))}
        statusLabel={statusLabel}
        saving={isMutating}
        calculateHours={(startDate, endDate) => resolvePtoHoursForEmployee(startDate, endDate, selectedRequest?.employeeId || "")}
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
        onSave={selectedRequestActionConfig?.canSubmit ? handleSaveRequestDetail : undefined}
        details={selectedRequest ? (
          <div className="grid gap-2 text-sm text-slate-700 sm:grid-cols-2">
            <p className="min-w-0">
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Employee</span>
              <span
                className="mt-0.5 block truncate font-semibold text-slate-900"
                title={resolveRequestEmployee(selectedRequest).employeeName}
              >
                {resolveRequestEmployee(selectedRequest).employeeName}
              </span>
            </p>
            <p className="min-w-0">
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Manager</span>
              <span
                className="mt-0.5 block truncate font-semibold text-slate-900"
                title={selectedRequestManagerLabel}
              >
                {selectedRequestManagerLabel}
              </span>
            </p>
            <p>
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Submitted</span>
              <span className="mt-0.5 block font-semibold text-slate-900">{formatMonthDayYearLabel(selectedRequest.submittedAt, tenantTimeZone)}</span>
            </p>
            {selectedRequest.reviewerName ? (
              <p>
                <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Reviewed by</span>
                <span className="mt-0.5 block font-semibold text-slate-900">
                  {selectedRequest.reviewerName}
                  {selectedRequest.reviewedAt ? ` · ${formatMonthDayYearLabel(selectedRequest.reviewedAt, tenantTimeZone)}` : ""}
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
          <div className="text-sm text-slate-700">
            <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Admin note</p>
            {selectedRequest.approverNote ? (
              <p className="mt-1 whitespace-pre-wrap text-slate-800">
                {selectedRequest.approverNote}
              </p>
            ) : (
              <p className="mt-1 whitespace-pre-wrap italic text-slate-400">
                No admin note.
              </p>
            )}
          </div>
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

      <ConfirmDialog
        open={Boolean(pendingReviewAction)}
        title={pendingReviewActionCopy?.title ?? ""}
        description={pendingReviewActionCopy?.description ?? ""}
        confirmLabel={pendingReviewActionCopy?.confirmLabel ?? "Confirm"}
        cancelLabel="Go back"
        onCancel={() => setPendingReviewAction(null)}
        onConfirm={() => {
          void handleConfirmReviewAction();
        }}
        note={pendingReviewAction === "revert" ? undefined : {
          label: "Admin note / reason",
          value: reviewNote,
          onChange: setReviewNote,
          placeholder: "Add a note or reason for this decision",
          disabled: isMutating,
          helpText: pendingReviewActionCopy?.noteHelpText,
        }}
      />

      <Dialog open={Boolean(selectedHoliday)} onOpenChange={(open) => {
        if (!open) {
          setSelectedHolidayId(null);
        }
      }}>
        <DialogContent className="max-w-xl">
          <DialogHeader>
            <DialogTitle>Holiday Detail</DialogTitle>
            <DialogDescription>
              Team holiday information for the selected date.
            </DialogDescription>
          </DialogHeader>

          {selectedHoliday ? (
            <div className="rounded-xl border border-indigo-100 bg-indigo-50/40 p-3 text-sm text-slate-700">
              <p><span className="font-semibold text-slate-900">Holiday:</span> {selectedHoliday.name}</p>
              <p><span className="font-semibold text-slate-900">Team region:</span> {selectedHoliday.teamRegion}</p>
              <p><span className="font-semibold text-slate-900">Date:</span> {formatDateLabel(selectedHoliday.date, tenantTimeZone)}</p>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>

      <Dialog
        open={isAdjustRequestPickerOpen}
        onOpenChange={(open) => {
          if (!open) {
            setIsAdjustRequestPickerOpen(false);
            setAdjustRequestPickerTarget(null);
          }
        }}
      >
        <DialogContent className="max-w-xl">
          <DialogClose asChild aria-label="Close request selection modal">
            <ModalCloseButton icon={<X className="size-4" />} className="absolute right-0 top-0 z-20" />
          </DialogClose>
          <DialogHeader className="pr-8">
            <DialogTitle>Choose Load Request</DialogTitle>
            <DialogDescription>
              Select the approved load request you want to adjust.
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 grid gap-2">
            {adjustRequestPickerRequests.map((request) => (
              <button
                key={request.id}
                type="button"
                onClick={() => openAdjustRequestEditor(request)}
                className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-left transition hover:border-blue-200 hover:bg-blue-50/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300"
              >
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-sm font-semibold text-slate-900">{formatDateLabel(request.createdAt, tenantTimeZone)}</p>
                    <p className="text-xs text-slate-500">{request.approverNote || "No note"}</p>
                  </div>
                  <p className="text-sm font-semibold text-slate-900">{formatHoursLabel(request.hours)}</p>
                </div>
              </button>
            ))}
            {adjustRequestPickerRequests.length === 0 ? (
              <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                No approved load requests were found for this balance.
              </div>
            ) : null}
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isAdjustModalOpen} onOpenChange={handleAdjustModalOpenChange}>
        <DialogContent
          className="flex max-h-[90vh] max-w-xl flex-col overflow-hidden rounded-xl bg-white p-6"
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy: isMutating, hasUnsavedChanges: hasAdjustFormChanges })) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={isMutating} busyMessage="Saving PTO hours..." className="min-h-0 flex-1">
            <DialogClose asChild aria-label="Close load PTO hours modal">
              <ModalCloseButton icon={<X className="size-4" />} className="absolute right-0 top-0 z-20" />
            </DialogClose>
            <DialogHeader className="pr-8">
              <DialogTitle>{adjustDialogTitle}</DialogTitle>
              <DialogDescription>
                {isAdjustEditMode
                  ? "Update the selected approved PTO load request."
                  : "Create a new approved PTO load request."}
              </DialogDescription>
            </DialogHeader>

            {adjustError ? (
              <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{adjustError}</div>
            ) : null}

            <div className="mt-3 grid gap-3">
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Employee</span>
                <AppDropdown
                  value={adjustForm.employeeId}
                  onValueChange={(value) => {
                    const nextRequests = resolveAdjustLoadRequests(value, adjustForm.ptoTypeCode);
                    const nextRequest = nextRequests[0] || null;
                    setAdjustForm((current) => ({
                      ...current,
                      employeeId: value,
                      transactionId: isAdjustEditMode ? (nextRequest?.id || "") : current.transactionId,
                      hours: isAdjustEditMode ? (nextRequest ? String(nextRequest.hours) : "") : current.hours,
                    }));
                  }}
                  options={employeeOptions}
                  searchable
                  disabled={isMutating || isAdjustEditMode}
                />
              </label>
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">PTO type</span>
                  <AppDropdown
                    value={adjustForm.ptoTypeCode}
                    onValueChange={(value) => {
                      const nextType = value as LeaveSpherePtoType;
                      const nextRequests = resolveAdjustLoadRequests(adjustForm.employeeId, nextType);
                      const nextRequest = nextRequests[0] || null;
                      setAdjustForm((current) => ({
                        ...current,
                        ptoTypeCode: nextType,
                        transactionId: isAdjustEditMode ? (nextRequest?.id || "") : current.transactionId,
                        hours: isAdjustEditMode ? (nextRequest ? String(nextRequest.hours) : "") : current.hours,
                      }));
                    }}
                    options={ptoTypeOptions}
                    searchable={false}
                    disabled={isMutating || isAdjustEditMode}
                  />
                </label>
              </div>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Hours</span>
                <Input
                  type="number"
                  min={0}
                  step={0.5}
                  value={adjustForm.hours}
                  onChange={(event) => setAdjustForm((current) => ({
                    ...current,
                    hours: event.target.value,
                  }))}
                  disabled={isMutating}
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Approver note</span>
                <Textarea value={adjustForm.approverNote} onChange={(event) => setAdjustForm((current) => ({ ...current, approverNote: event.target.value }))} className="min-h-[96px]" disabled={isMutating} />
              </label>
            </div>

            {(hasAdjustFormChanges || shouldShowAdjustCancelButton || shouldShowAdjustSubmitButton) ? (
              <DialogFooter className="gap-2">
                {hasAdjustFormChanges ? (
                  <Button variant="outline" onClick={handleRevertAdjustForm} disabled={isMutating}>
                    Revert
                  </Button>
                ) : null}
                {shouldShowAdjustCancelButton ? (
                  <Button
                    variant="outline"
                    className="border-rose-200 text-rose-700 hover:bg-rose-50"
                    onClick={() => setPendingAdjustAction("cancel")}
                    disabled={isMutating}
                  >
                    Cancel
                  </Button>
                ) : null}
                {shouldShowAdjustSubmitButton ? (
                  <Button onClick={() => void handleAdjustBalance()} disabled={isMutating || !canSubmitAdjustForm}>
                    {isMutating ? "Saving..." : adjustSubmitLabel}
                  </Button>
                ) : null}
              </DialogFooter>
            ) : null}
          </ModalShell>
        </DialogContent>
      </Dialog>

      {pendingAdjustActionCopy ? (
        <ConfirmDialog
          open={Boolean(pendingAdjustAction)}
          title={pendingAdjustActionCopy.title}
          description={pendingAdjustActionCopy.description}
          confirmLabel={pendingAdjustActionCopy.confirmLabel}
          cancelLabel="Go back"
          onCancel={() => setPendingAdjustAction(null)}
          onConfirm={() => {
            void handleConfirmAdjustAction();
          }}
        />
      ) : null}

      <UnsavedChangesDialog
        open={isAdjustDiscardDialogOpen}
        onKeepEditing={() => {
          setIsAdjustDiscardDialogOpen(false);
        }}
        onDiscardChanges={() => {
          setIsAdjustDiscardDialogOpen(false);
          closeAdjustModal(true);
        }}
      />

      <Dialog open={isSetupModalOpen} onOpenChange={setIsSetupModalOpen}>
        <DialogContent className="flex max-h-[90vh] max-w-2xl flex-col overflow-hidden rounded-xl bg-white p-6">
          <ModalShell busy={isMutating} busyMessage="Saving setup..." className="min-h-0 flex-1">
            <DialogHeader>
              <DialogTitle>Setup / Admin Data</DialogTitle>
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
              <Button variant="outline" onClick={() => setIsSetupModalOpen(false)}>
                Cancel
              </Button>
              <Button onClick={() => void handleSetupSave()} disabled={isMutating}>Save setup</Button>
            </DialogFooter>
          </ModalShell>
        </DialogContent>
      </Dialog>

      <PageLoadingLayer
        active={loadingContract.pageOverlayActive}
        message={loadingContract.pageOverlayMessage}
      />
    </AppPageLayout>
  );
}
