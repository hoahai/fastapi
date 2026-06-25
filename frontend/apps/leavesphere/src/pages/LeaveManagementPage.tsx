import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  CalendarDays,
  Copy,
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
import { useToast } from "@shell/components/ui/toast";
import { hasAppAdminAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { useOnlineStatus } from "@shared/hooks/useOnlineStatus";
import { useObservedElementHeight } from "@shared/hooks/useObservedElementHeight";
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
import { ModalCloseButton, ModalHeaderRow, ModalShell } from "@shared/components";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import {
  resolveCriteriaLoadPlan,
  type CriteriaLoadPlan,
  useCriteriaBaselineStore,
} from "@shared/hooks/useCriteriaLoadPolicy";
import { DEFAULT_TIME_ZONE, formatDateInTimeZone, getCurrentMonthKeyInTimeZone, getCurrentYearInTimeZone, getTodayIsoDateInTimeZone, shiftIsoDateByDays } from "@shared/utils/time";
import { TooltipTarget } from "@shared/components/actions/TooltipTarget";
import { LeaveSpherePtoEmployeeHeader } from "@leavesphere/components/LeaveSpherePtoEmployeeHeader";
import { LeaveSpherePtoRequestList } from "@leavesphere/components/LeaveSpherePtoRequestList";
import { LeaveSphereMonthCalendar, type LeaveSphereMonthCalendarEvent } from "@leavesphere/components/MonthCalendar";
import { LeaveSpherePtoRequestDetailModal, type LeaveSpherePtoRequestFormState } from "@leavesphere/components/PtoRequestDetailModal";
import {
  LeaveSpherePtoLoadHoursModal,
  type LeaveSpherePtoLoadHoursFormState,
  type LeaveSpherePtoLoadHoursSubmitParams,
} from "@leavesphere/components/PtoLoadHoursModal";
import {
  LeaveSpherePtoDuplicateBalancesModal,
  type LeaveSpherePtoDuplicateBalancesSubmitParams,
} from "@leavesphere/components/PtoDuplicateBalancesModal";
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
  LeaveSpherePtoToneChip,
  LeaveSpherePtoStatusChip,
} from "@leavesphere/components/PtoStatusChip";
import {
  adjustLeaveManagementBalance,
  createLeaveManagementRequest,
  disconnectLeaveManagementGoogleCalendarConnection,
  duplicateLeaveManagementBalances,
  loadLeaveManagementGoogleCalendarAuthorizationUrl,
  loadLeaveManagementGoogleCalendarConnectionStatus,
  loadLeaveManagementWorkspace,
  mergeLeaveManagementWorkspace,
  reviewLeaveManagementRequest,
  updateLeaveManagementRequest,
  updateLeaveManagementSetupData,
  type LeaveManagementGoogleCalendarConnectionStatus,
  type LeaveManagementSetupInput,
  type LeaveManagementWorkspaceData,
} from "@leavesphere/lib/leaveManagementApi";
import {
  type LeaveManagementLoadRequest,
} from "@leavesphere/lib/leaveManagementBalanceLedger";
import { calculateLeaveSpherePtoHours } from "@leavesphere/lib/ptoHours";
import { buildLeaveSphereHolidayDateSet } from "@leavesphere/lib/ptoHolidayScopes";
import {
  buildLeaveSpherePtoTypeOptionsFromBalances,
  buildLeaveSpherePtoTypeOptionsFromCatalog,
  resolveLeaveSpherePtoAvailableHours,
  validateLeaveSpherePtoRequestedHours,
} from "@leavesphere/lib/ptoAvailability";
import {
  buildLeaveSpherePtoCalendarRequestChipLabel,
  buildLeaveSpherePtoCalendarRequestTooltipLabel,
} from "@leavesphere/lib/ptoCalendar";
import { getPtoRequestActionConfig } from "@leavesphere/lib/ptoRequestActionConfig";
import {
  getLeaveSphereApprovalActionConfirmCopy,
  type LeaveSphereApprovalAction as LeaveSphereReviewAction,
} from "@leavesphere/lib/approvalActionConfirm";
import { LEAVESPHERE_TEAM_REGION_OPTIONS } from "@leavesphere/lib/ptoTypes";
import type { LeaveSpherePtoRequest, LeaveSpherePtoType, LeaveSphereTeamRegion } from "@leavesphere/lib/ptoTypes";
import type { LeaveSpherePtoBalance } from "@leavesphere/lib/ptoTypes";
import { ActionIconButton } from "@tradsphere/components/dashboard/ActionIconButton";
import { buildMonthKeyForYear, formatMonthDayYearLabel } from "@leavesphere/lib/ptoDate";
import { formatLeaveSpherePtoStatusLabel, getLeaveSpherePtoRequestSurfaceClassName, normalizeLeaveSpherePtoStatus } from "@leavesphere/lib/ptoStatus";
import {
  readLeaveSpherePtoWorkspaceCacheSnapshot,
  syncLeaveSpherePtoWorkspaceCache,
  LEAVESPHERE_LEAVE_MANAGEMENT_LEGACY_CACHE_PAGE_CODE,
  LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
} from "@leavesphere/lib/ptoWorkspaceCache";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type WorkspaceLoadOptions = {
  historyStartDate?: string | null;
  historyEndDate?: string | null;
  overlapMonth?: string | null;
  includePending?: boolean;
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

type LegacyAdjustBalanceForm = {
  employeeId: string;
  type: LeaveSpherePtoType;
  mode: "add" | "subtract" | "set";
  hours: string;
  approverNote: string;
};

type SetupForm = {
  kind: LeaveManagementSetupInput["kind"];
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

type BalanceRowDisplay = {
  employeeId: string;
  employeeName: string;
  pictureUrl: string | null;
  subtitle: string | null;
  balances: LeaveSpherePtoBalance[];
};

type PersistedLeaveManagementPageState = {
  selectedYear?: string;
  loadedYear?: number | null;
  tab: AdminTab;
  calendarMonth: string;
  selectedRequestId: string | null;
  reviewNote: string;
  isCreateModalOpen: boolean;
  createForm: CreateRequestForm;
  isAdjustModalOpen: boolean;
  adjustForm: LeaveSpherePtoLoadHoursFormState;
  isSetupModalOpen: boolean;
  setupForm: SetupForm;
  recentHistorySearch?: string;
  recentHistorySearchDraft?: string;
  recentHistorySearchApplied?: string;
  scrollY: number;
};

type LeaveManagementDraftSnapshot = {
  reviewNote: string;
  createForm: CreateRequestForm;
  adjustForm: LeaveSpherePtoLoadHoursFormState;
  setupForm: SetupForm;
  draftRecentHistorySearch: string;
  appliedRecentHistorySearch: string;
};

const PTO_TYPE_OPTIONS: Array<{ value: LeaveSpherePtoType; label: string }> = [
  { value: "vacation", label: "Vacation" },
  { value: "sick", label: "Sick" },
  { value: "personal", label: "Personal" },
];

const TAB_OPTIONS: Array<{ id: AdminTab; label: string }> = [
  { id: "calendar", label: "Calendar" },
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

const EMPTY_ADJUST_FORM: LeaveSpherePtoLoadHoursFormState = {
  employeeId: "",
  ptoTypeCode: "vacation",
  ptoActionCode: "load_grant",
  transactionId: "",
  hours: "",
  description: "",
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
const BALANCE_EMPLOYEE_COLUMN_WIDTH = "28rem";

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

function requestTypeLabel(type: string, lookup: Map<string, PtoTypeMeta> | null | undefined): string {
  if (lookup) {
    const resolved = resolvePtoTypeMeta(type, lookup);
    if (resolved) {
      return resolved.label;
    }
  }
  const normalized = asString(type).toLowerCase();
  const matched = PTO_TYPE_OPTIONS.find((item) => item.value === normalized);
  if (matched) {
    return matched.label;
  }
  return asString(type).toUpperCase() || "PTO";
}

function hasBalanceActivity(balance: { totalHours: number; usedHours: number; scheduledHours: number }): boolean {
  return balance.totalHours !== 0 || balance.usedHours !== 0 || balance.scheduledHours !== 0;
}

function buildLeaveManagementDraftSnapshot(params: {
  reviewNote: string;
  createForm: CreateRequestForm;
  adjustForm: LeaveSpherePtoLoadHoursFormState;
  setupForm: SetupForm;
  draftRecentHistorySearch: string;
  appliedRecentHistorySearch: string;
}): LeaveManagementDraftSnapshot {
  return {
    reviewNote: params.reviewNote,
    createForm: { ...params.createForm },
    adjustForm: { ...params.adjustForm },
    setupForm: { ...params.setupForm },
    draftRecentHistorySearch: params.draftRecentHistorySearch,
    appliedRecentHistorySearch: params.appliedRecentHistorySearch,
  };
}

function areLeaveManagementLoadSnapshotsEqual(
  left: LeaveManagementDraftSnapshot | null,
  right: LeaveManagementDraftSnapshot | null,
): boolean {
  if (left === right) {
    return true;
  }
  if (!left || !right) {
    return false;
  }

  return (
    left.reviewNote.trim() === right.reviewNote.trim()
    && JSON.stringify(left.createForm) === JSON.stringify(right.createForm)
    && JSON.stringify(left.adjustForm) === JSON.stringify(right.adjustForm)
    && JSON.stringify(left.setupForm) === JSON.stringify(right.setupForm)
    && left.draftRecentHistorySearch.trim() === right.draftRecentHistorySearch.trim()
    && left.appliedRecentHistorySearch.trim() === right.appliedRecentHistorySearch.trim()
  );
}

function isLeaveManagementLoadSnapshot(value: unknown): value is LeaveManagementDraftSnapshot {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    typeof record.reviewNote === "string"
    && isCreateRequestForm(record.createForm)
    && isAdjustBalanceForm(record.adjustForm)
    && isSetupForm(record.setupForm)
    && typeof record.draftRecentHistorySearch === "string"
    && typeof record.appliedRecentHistorySearch === "string"
  );
}

function restoreLeaveManagementLoadSnapshot(
  snapshot: LeaveManagementDraftSnapshot,
  actions: {
    setReviewNote: (value: string) => void;
    setCreateForm: (value: CreateRequestForm) => void;
    setAdjustForm: (value: LeaveSpherePtoLoadHoursFormState) => void;
    setSetupForm: (value: SetupForm) => void;
    setDraftRecentHistorySearch: (value: string) => void;
    setAppliedRecentHistorySearch: (value: string) => void;
    setSetupError: (value: string | null) => void;
  },
): void {
  actions.setReviewNote(snapshot.reviewNote);
  actions.setCreateForm({ ...snapshot.createForm });
  actions.setAdjustForm({ ...snapshot.adjustForm });
  actions.setSetupForm({ ...snapshot.setupForm });
  actions.setDraftRecentHistorySearch(snapshot.draftRecentHistorySearch);
  actions.setAppliedRecentHistorySearch(snapshot.appliedRecentHistorySearch);
  actions.setSetupError(null);
}

type PtoTypeMeta = {
  code: string;
  label: string;
  order: number;
};

function normalizePtoTypeLookupKey(value: string): string {
  return asString(value).toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function buildPtoTypeMetaLookup(ptoTypes: Array<{ code?: string | null; type?: string | null; label?: string | null }> | null | undefined): Map<string, PtoTypeMeta> {
  const lookup = new Map<string, PtoTypeMeta>();
  (ptoTypes ?? []).forEach((item, index) => {
    const code = normalizePtoTypeLookupKey(asString(item.code));
    if (!code) {
      return;
    }
    const meta: PtoTypeMeta = {
      code,
      label: asString(item.label) || asString(item.type) || code,
      order: index,
    };
    const keys = [
      code,
      normalizePtoTypeLookupKey(asString(item.type) || code),
      normalizePtoTypeLookupKey(asString(item.label)),
    ];
    for (const key of keys) {
      if (key && !lookup.has(key)) {
        lookup.set(key, meta);
      }
    }
  });
  return lookup;
}

function resolvePtoTypeMeta(
  rawValue: string,
  lookup: Map<string, PtoTypeMeta>,
): PtoTypeMeta | null {
  const normalized = normalizePtoTypeLookupKey(rawValue);
  if (!normalized) {
    return null;
  }
  return lookup.get(normalized) || null;
}

function buildYearDateBounds(year: number): { minDate: string; maxDate: string } {
  return {
    minDate: `${year}-01-01`,
    maxDate: `${year}-12-31`,
  };
}

function parseMonthKey(monthKey: string): { year: number; month: number } | null {
  const match = /^(\d{4})-(\d{2})$/.exec(monthKey);
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
    return null;
  }
  return { year, month };
}

function normalizeMonthKey(monthKey: string): string {
  const parsed = parseMonthKey(monthKey);
  if (!parsed) {
    return "";
  }
  return `${parsed.year}-${String(parsed.month).padStart(2, "0")}`;
}

function shiftMonthKey(monthKey: string, offset: number): string {
  const parsed = parseMonthKey(monthKey);
  if (!parsed) {
    return monthKey;
  }
  const date = new Date(Date.UTC(parsed.year, parsed.month - 1 + offset, 1));
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
}

function buildMonthStartDate(monthKey: string): string {
  const normalized = normalizeMonthKey(monthKey);
  return normalized ? `${normalized}-01` : "";
}

function buildInitialRequestLoadWindow(monthKey: string, year: number): {
  historyStartDate: string;
  historyEndDate: string;
  overlapMonth: string;
} {
  const normalizedMonthKey = normalizeMonthKey(monthKey) || `${year}-01`;
  const previousMonthKey = shiftMonthKey(normalizedMonthKey || monthKey, -1);
  return {
    historyStartDate: buildMonthStartDate(previousMonthKey),
    historyEndDate: `${year}-12-31`,
    overlapMonth: normalizedMonthKey,
  };
}

function buildMonthKeyRange(startMonthKey: string, endMonthKey: string): string[] {
  const normalizedStart = normalizeMonthKey(startMonthKey);
  const normalizedEnd = normalizeMonthKey(endMonthKey);
  if (!normalizedStart || !normalizedEnd || normalizedStart > normalizedEnd) {
    return [];
  }
  const monthKeys: string[] = [];
  let currentMonthKey = normalizedStart;
  while (currentMonthKey <= normalizedEnd) {
    monthKeys.push(currentMonthKey);
    if (currentMonthKey === normalizedEnd) {
      break;
    }
    const nextMonthKey = shiftMonthKey(currentMonthKey, 1);
    if (!nextMonthKey || nextMonthKey === currentMonthKey) {
      break;
    }
    currentMonthKey = nextMonthKey;
  }
  return monthKeys;
}

function requestIsOut(request: LeaveSpherePtoRequest): boolean {
  return request.status === "approved" || request.status === "pending";
}

function requestIsWithinYear(request: LeaveSpherePtoRequest, year: number): boolean {
  const yearStart = `${year}-01-01`;
  const yearEnd = `${year}-12-31`;
  return request.startDate >= yearStart && request.endDate <= yearEnd;
}

function filterWorkspaceByYear(workspace: LeaveManagementWorkspaceData, year: number): LeaveManagementWorkspaceData {
  const yearPrefix = `${year}-`;
  return {
    ...workspace,
    requests: workspace.requests.filter((item) => requestIsWithinYear(item, year)),
    holidays: workspace.holidays.filter((item) => item.date.startsWith(yearPrefix)),
  };
}

function filterWorkspaceByLoadedRequestMonths(
  workspace: LeaveManagementWorkspaceData,
  year: number,
  loadedRequestMonthKeys: Set<string>,
): LeaveManagementWorkspaceData {
  const yearScopedWorkspace = filterWorkspaceByYear(workspace, year);
  if (loadedRequestMonthKeys.size === 0) {
    return yearScopedWorkspace;
  }

  return {
    ...yearScopedWorkspace,
    requests: yearScopedWorkspace.requests.filter((item) => loadedRequestMonthKeys.has(item.startDate.slice(0, 7))),
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
  return value === "vacation" || value === "sick" || value === "personal";
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

function isAdjustBalanceForm(value: unknown): value is LeaveSpherePtoLoadHoursFormState {
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
    && (typeof record.description === "string" || record.description === undefined)
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

function normalizePersistedAdjustBalanceForm(value: unknown): LeaveSpherePtoLoadHoursFormState {
  if (isAdjustBalanceForm(value)) {
    return {
      ...value,
      description: asString(value.description),
    };
  }
  if (isLegacyAdjustBalanceForm(value)) {
    return {
      employeeId: value.employeeId,
      ptoTypeCode: value.type,
      ptoActionCode: value.mode === "set" ? "load_grant" : "adjustment",
      transactionId: "",
      hours: value.hours,
      description: "",
    };
  }
  return { ...EMPTY_ADJUST_FORM };
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

function isPersistedLeaveManagementPageState(value: unknown): value is PersistedLeaveManagementPageState {
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

export default function LeaveManagementPage() {
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const toast = useToast();
  const { isOnline } = useOnlineStatus();

  const currentUserId = asString(auth.user?.id) || "local-admin";
  const currentUserName = asString(auth.user?.fullName) || asString(auth.user?.email) || "LeaveSphere Admin";
  const canAdmin = hasAppAdminAccess(auth.accessProfile, "leavesphere");
  const pageStateUserKey = asString(auth.user?.id || auth.user?.email);

  const leaveManagementWorkspaceKey = useMemo(() => {
    const tenant = asString(auth.tenantSlug) || "default";
    return `leavesphere:leave-management:${tenant}:${currentUserId}`;
  }, [auth.tenantSlug, currentUserId]);
  const tenantSlug = asString(auth.tenantSlug);
  const canUseGoogleCalendarAdminApi = canAdmin && auth.status === "authenticated" && Boolean(tenantSlug);
  const canRestorePageState = auth.status === "authenticated" && Boolean(tenantSlug) && Boolean(pageStateUserKey);
  const pageStateScope = useMemo<ScopedPageState | null>(() => {
    if (!canRestorePageState) {
      return null;
    }
    return {
      userKey: pageStateUserKey,
      tenantSlug,
      appCode: LEAVESPHERE_APP_CODE,
      pageCode: LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
    };
  }, [canRestorePageState, pageStateUserKey, tenantSlug]);
  const legacyPageStateScope = useMemo<ScopedPageState | null>(() => {
    if (!canRestorePageState) {
      return null;
    }
    return {
      userKey: pageStateUserKey,
      tenantSlug,
      appCode: LEAVESPHERE_APP_CODE,
      pageCode: LEAVESPHERE_LEAVE_MANAGEMENT_LEGACY_CACHE_PAGE_CODE,
    };
  }, [canRestorePageState, pageStateUserKey, tenantSlug]);
  const pageStateStorageKey = useMemo(() => {
    if (!pageStateScope) {
      return null;
    }
    return buildScopedPageStateStorageKey(pageStateScope);
  }, [pageStateScope]);

  const [workspace, setWorkspace] = useState<LeaveManagementWorkspaceData | null>(null);
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
  const createRequestEmployeeIdRef = useRef<string>("");

  const [selectedRequestId, setSelectedRequestId] = useState<string | null>(null);
  const [selectedHolidayId, setSelectedHolidayId] = useState<string | null>(null);
  const [reviewNote, setReviewNote] = useState("");
  const [pendingReviewAction, setPendingReviewAction] = useState<LeaveSphereReviewAction | null>(null);
  const [pendingReloadYear, setPendingReloadYear] = useState<number | null>(null);

  const [isAdjustModalOpen, setIsAdjustModalOpen] = useState(false);
  const [adjustForm, setAdjustForm] = useState<LeaveSpherePtoLoadHoursFormState>(EMPTY_ADJUST_FORM);
  const [adjustModalRequest, setAdjustModalRequest] = useState<LeaveManagementLoadRequest | null>(null);
  const [adjustCancelNote, setAdjustCancelNote] = useState("");
  const [isAdjustRequestPickerOpen, setIsAdjustRequestPickerOpen] = useState(false);
  const [adjustRequestPickerTarget, setAdjustRequestPickerTarget] = useState<{
    employeeId: string;
    ptoTypeCode: LeaveSpherePtoType;
  } | null>(null);
  const [pendingAdjustAction, setPendingAdjustAction] = useState<"cancel" | null>(null);

  const [isDuplicateBalancesModalOpen, setIsDuplicateBalancesModalOpen] = useState(false);
  const [isSetupModalOpen, setIsSetupModalOpen] = useState(false);
  const [setupForm, setSetupForm] = useState<SetupForm>(EMPTY_SETUP_FORM);
  const [setupError, setSetupError] = useState<string | null>(null);
  const [googleCalendarConnectionStatus, setGoogleCalendarConnectionStatus] = useState<LeaveManagementGoogleCalendarConnectionStatus | null>(null);
  const [isGoogleCalendarStatusLoading, setIsGoogleCalendarStatusLoading] = useState(false);
  const [isGoogleCalendarConnecting, setIsGoogleCalendarConnecting] = useState(false);
  const [isGoogleCalendarDisconnecting, setIsGoogleCalendarDisconnecting] = useState(false);
  const [isGoogleCalendarDisconnectConfirmOpen, setIsGoogleCalendarDisconnectConfirmOpen] = useState(false);
  const [draftRecentHistorySearch, setDraftRecentHistorySearch] = useState("");
  const [appliedRecentHistorySearch, setAppliedRecentHistorySearch] = useState("");
  const [scrollY, setScrollY] = useState(0);
  const [hasHydratedPageState, setHasHydratedPageState] = useState(false);
  const [isPendingRequestsModalOpen, setIsPendingRequestsModalOpen] = useState(false);
  const [calendarSectionRef, calendarSectionHeight] = useObservedElementHeight<HTMLElement>();
  const activeTab = tab === "requests" ? "calendar" : tab;
  const hydratedPageStateScopeRef = useRef<string | null>(null);
  const restoredScrollScopeRef = useRef<string | null>(null);
  const restoredWorkspaceScopeRef = useRef<string | null>(null);
  const workspaceLoadRequestTokenRef = useRef(0);
  const workspaceRef = useRef<LeaveManagementWorkspaceData | null>(null);
  const loadedYearRef = useRef<number | null>(null);
  const loadedRequestMonthKeysRef = useRef<Set<string>>(new Set());
  const {
    captureBaseline: captureLeaveManagementLoadBaseline,
    getBaseline: getLeaveManagementLoadBaseline,
    isDirty: isLeaveManagementLoadDirty,
  } = useCriteriaBaselineStore<LeaveManagementDraftSnapshot>({
    areSnapshotsEqual: areLeaveManagementLoadSnapshotsEqual,
    persistence: {
      scope: pageStateScope,
      validateSnapshot: isLeaveManagementLoadSnapshot,
    },
  });

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
    workspaceRef.current = workspace;
  }, [workspace]);

  useEffect(() => {
    loadedYearRef.current = loadedYear;
  }, [loadedYear]);

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
    const persisted = readScopedPageState<PersistedLeaveManagementPageState>(
      pageStateScope,
      isPersistedLeaveManagementPageState,
    ) ?? (
      legacyPageStateScope
        ? readScopedPageState<PersistedLeaveManagementPageState>(
            legacyPageStateScope,
            isPersistedLeaveManagementPageState,
          )
        : null
    );
    if (persisted) {
      if (typeof persisted.selectedYear === "string") {
        setSelectedYear(persisted.selectedYear);
      }
      setTab(persisted.tab === "requests" ? "calendar" : persisted.tab);
      setCalendarMonth(persisted.calendarMonth);
      setSelectedRequestId(persisted.selectedRequestId);
      setReviewNote(persisted.reviewNote);
      setIsCreateModalOpen(persisted.isCreateModalOpen);
      setCreateForm(persisted.createForm);
      setIsAdjustModalOpen(persisted.isAdjustModalOpen);
      const restoredAdjustForm = normalizePersistedAdjustBalanceForm(persisted.adjustForm);
      setAdjustForm(restoredAdjustForm);
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
      const restoredYear = typeof persisted.loadedYear === "number" ? persisted.loadedYear : null;
      if (restoredYear !== null && restoredWorkspaceScopeRef.current !== pageStateStorageKey) {
        restoredWorkspaceScopeRef.current = pageStateStorageKey;
        const currentCacheSnapshot = readLeaveSpherePtoWorkspaceCacheSnapshot<LeaveManagementWorkspaceData>({
          pageCode: LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
          tenantSlug,
          userId: currentUserId,
          year: restoredYear,
        });
        const legacyCacheSnapshot = currentCacheSnapshot ? null : readLeaveSpherePtoWorkspaceCacheSnapshot<LeaveManagementWorkspaceData>({
          pageCode: LEAVESPHERE_LEAVE_MANAGEMENT_LEGACY_CACHE_PAGE_CODE,
          tenantSlug,
          userId: currentUserId,
          year: restoredYear,
        });
        const cacheSnapshot = currentCacheSnapshot ?? legacyCacheSnapshot;
        if (cacheSnapshot?.data) {
          setWorkspace(cacheSnapshot.data);
          setLoadedYear(restoredYear);
          setCacheStatus({
            source: cacheSnapshot.source,
            fetchedAt: cacheSnapshot.fetchedAt ?? Date.now(),
          });
          setAdjustModalRequest(
            persisted.isAdjustModalOpen && restoredAdjustForm.transactionId
              ? (cacheSnapshot.data.balanceTransactions.find((item) => item.id === restoredAdjustForm.transactionId) || null)
              : null,
          );
          if (!currentCacheSnapshot && legacyCacheSnapshot?.data) {
            syncLeaveSpherePtoWorkspaceCache(
              {
                pageCode: LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
                tenantSlug,
                userId: currentUserId,
                year: restoredYear,
              },
              legacyCacheSnapshot.data,
              {
                source: legacyCacheSnapshot.source,
                fetchedAt: legacyCacheSnapshot.fetchedAt ?? Date.now(),
              },
            );
          }
        } else {
          setLoadedYear(null);
          setWorkspace(null);
          setCacheStatus(null);
          setAdjustModalRequest(null);
        }
      } else {
        setLoadedYear(null);
        setWorkspace(null);
        setCacheStatus(null);
        setAdjustModalRequest(null);
      }
    }
    setHasHydratedPageState(true);
  }, [canRestorePageState, currentUserId, legacyPageStateScope, pageStateScope, pageStateStorageKey, tenantSlug]);

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
    writeScopedPageState<PersistedLeaveManagementPageState>(pageStateScope, {
      selectedYear,
      loadedYear,
      tab: activeTab,
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
    activeTab,
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
    nextWorkspace: LeaveManagementWorkspaceData,
    source: "cache" | "network",
    year: number,
    fetchedAt = Date.now(),
  ) => {
    workspaceRef.current = nextWorkspace;
    loadedYearRef.current = year;
    setWorkspace(nextWorkspace);
    setLoadedYear(year);
    setSelectedYear(String(year));
    setCacheStatus({
      source,
      fetchedAt,
    });
    syncLeaveSpherePtoWorkspaceCache(
      {
        pageCode: LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
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

  const recordLoadedRequestMonths = useCallback((options: WorkspaceLoadOptions) => {
    const nextLoadedMonthKeys = new Set(loadedRequestMonthKeysRef.current);
    const addMonthKey = (monthKey: string | null | undefined) => {
      const normalizedMonthKey = normalizeMonthKey(asString(monthKey));
      if (normalizedMonthKey) {
        nextLoadedMonthKeys.add(normalizedMonthKey);
      }
    };

    addMonthKey(options.overlapMonth);

    const historyStartMonthKey = options.historyStartDate ? normalizeMonthKey(options.historyStartDate.slice(0, 7)) : "";
    const historyEndMonthKey = options.historyEndDate ? normalizeMonthKey(options.historyEndDate.slice(0, 7)) : "";
    if (historyStartMonthKey || historyEndMonthKey) {
      const startMonthKey = historyStartMonthKey || historyEndMonthKey;
      const endMonthKey = historyEndMonthKey || historyStartMonthKey;
      for (const monthKey of buildMonthKeyRange(startMonthKey, endMonthKey)) {
        nextLoadedMonthKeys.add(monthKey);
      }
    }

    loadedRequestMonthKeysRef.current = nextLoadedMonthKeys;
  }, []);

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
          requestTypeLabel(request.ptoTypeCode ?? request.type, ptoTypeMetaLookup),
          formatLeaveSpherePtoStatusLabel(request.status),
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
    () => (pendingReviewAction ? getLeaveSphereApprovalActionConfirmCopy(pendingReviewAction, "admin") : null),
    [pendingReviewAction],
  );
  const pendingReviewActionLabel = pendingReviewActionCopy?.noteLabel ?? "Approver note / reason";
  const pendingReviewActionRequiresNote = Boolean(pendingReviewActionCopy?.noteRequired);
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
  const activeEmployeeOptions = useMemo(
    () => (workspaceForYear?.employees ?? [])
      .filter((item) => item.active)
      .map((item) => ({ value: item.employeeId, label: item.employeeName })),
    [workspaceForYear?.employees],
  );

  const employeeById = useMemo(
    () => new Map((workspaceForYear?.employees ?? []).map((item) => [item.employeeId, item] as const)),
    [workspaceForYear?.employees],
  );
  const ptoTypeMetaLookup = useMemo(
    () => buildPtoTypeMetaLookup(workspaceForYear?.ptoTypes),
    [workspaceForYear?.ptoTypes],
  );
  const resolveBalanceTypeMeta = useCallback((rawValue: string) => {
    const resolved = resolvePtoTypeMeta(rawValue, ptoTypeMetaLookup);
    if (resolved) {
      return resolved;
    }
    const normalized = normalizePtoTypeLookupKey(rawValue);
    return {
      code: normalized,
      label: requestTypeLabel(rawValue, ptoTypeMetaLookup),
      order: Number.MAX_SAFE_INTEGER,
    };
  }, [ptoTypeMetaLookup]);
  const balanceTableState = useMemo(() => {
    const typeLabels = new Map<string, string>();
    const typeOrders = new Map<string, number>();

    const rows = (workspaceForYear?.employeeBalances ?? [])
      .map((row) => {
        const employeeId = asString(row.employeeId);
        const employee = employeeById.get(employeeId) || null;
        const employeeDisplay = employeeLookupById.get(employeeId) || null;
        const balances = (row.balances ?? [])
          .map((balance) => {
            const normalizedType = asString(balance.type);
            const meta = resolveBalanceTypeMeta(normalizedType);
            const label = asString(balance.label) || typeLabels.get(normalizedType) || meta.label;
            const order = meta.order;
            if (normalizedType) {
              if (!typeLabels.has(normalizedType)) {
                typeLabels.set(normalizedType, label);
              }
              if (!typeOrders.has(normalizedType)) {
                typeOrders.set(normalizedType, order);
              }
            }
            return {
              type: normalizedType,
              label,
              totalHours: asNumber(balance.totalHours),
              usedHours: asNumber(balance.usedHours),
              scheduledHours: asNumber(balance.scheduledHours),
              remainingHours: typeof balance.remainingHours === "number" && Number.isFinite(balance.remainingHours)
                ? balance.remainingHours
                : undefined,
            };
          })
          .filter((cell) => cell.type && (cell.totalHours !== 0 || cell.usedHours !== 0 || cell.scheduledHours !== 0 || cell.remainingHours !== 0))
          .sort((left, right) => {
            const leftOrder = typeOrders.get(left.type) ?? Number.MAX_SAFE_INTEGER;
            const rightOrder = typeOrders.get(right.type) ?? Number.MAX_SAFE_INTEGER;
            if (leftOrder !== rightOrder) {
              return leftOrder - rightOrder;
            }
            const leftLabel = typeLabels.get(left.type) || left.label;
            const rightLabel = typeLabels.get(right.type) || right.label;
            const labelCompare = leftLabel.localeCompare(rightLabel);
            if (labelCompare !== 0) {
              return labelCompare;
            }
            return left.type.localeCompare(right.type);
          });
        const displayRow: BalanceRowDisplay = {
          employeeId,
          employeeName: employeeDisplay?.employeeName || employee?.employeeName || row.employeeName || employeeId,
          pictureUrl: employeeDisplay?.pictureUrl || employee?.pictureUrl || null,
          subtitle: employee?.title || null,
          balances,
        };
        return displayRow;
      })
      .filter((row) => row.balances.length > 0);

    const columns = [...typeOrders.keys()]
      .map((type) => ({
        type,
        label: typeLabels.get(type) || resolveBalanceTypeMeta(type).label,
        order: typeOrders.get(type) ?? Number.MAX_SAFE_INTEGER,
      }))
      .filter((column) => rows.some((row) => row.balances.some((balance) => balance.type === column.type)))
      .sort((left, right) => {
        if (left.order !== right.order) {
          return left.order - right.order;
        }
        const labelCompare = left.label.localeCompare(right.label);
        if (labelCompare !== 0) {
          return labelCompare;
        }
        return left.type.localeCompare(right.type);
      });

    return {
      rows,
      columns,
    };
  }, [employeeById, employeeLookupById, resolveBalanceTypeMeta, workspaceForYear?.employeeBalances]);
  const visibleBalanceRows = balanceTableState.rows;
  const balanceColumns = balanceTableState.columns;
  const balanceTypeColumnWidth = useMemo(
    () => (balanceColumns.length > 0
      ? `calc((100% - ${BALANCE_EMPLOYEE_COLUMN_WIDTH}) / ${balanceColumns.length})`
      : "0px"),
    [balanceColumns.length],
  );
  const loadRequestsByBalanceKey = useMemo(() => {
    const requestsByKey = new Map<string, LeaveManagementLoadRequest[]>();
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
      const key = `${transaction.employeeId}::${resolveBalanceTypeMeta(transaction.ptoTypeCode).code}`;
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
  }, [loadedYear, resolveBalanceTypeMeta, workspaceForYear?.balanceTransactions]);
  const duplicateBalancesDefaultEmployeeIds = useMemo(() => {
    const currentLoadedYear = loadedYear;
    if (!Number.isInteger(currentLoadedYear)) {
      return [];
    }
    const activeEmployeeIds = new Set(activeEmployeeOptions.map((item) => item.value));
    const employeeIdsWithLoadTransactions = new Set<string>();
    for (const transaction of workspaceForYear?.balanceTransactions ?? []) {
      if (
        transaction.year !== currentLoadedYear
        || transaction.status !== "Approved"
        || transaction.ptoActionCode !== "load_grant"
      ) {
        continue;
      }
      if (activeEmployeeIds.has(transaction.employeeId)) {
        employeeIdsWithLoadTransactions.add(transaction.employeeId);
      }
    }
    return activeEmployeeOptions
      .filter((item) => employeeIdsWithLoadTransactions.has(item.value))
      .map((item) => item.value);
  }, [activeEmployeeOptions, loadedYear, workspaceForYear?.balanceTransactions]);
  const duplicateBalancesDefaultYearTo = loadedYear ?? currentYear;
  const duplicateBalancesDefaultYearFrom = duplicateBalancesDefaultYearTo - 1;

  const getEmployeePtoTypeOptions = useCallback((employeeId: string) => {
    const employeeBalanceRow = (workspaceForYear?.employeeBalances ?? []).find((row) => row.employeeId === employeeId) || null;
    return buildLeaveSpherePtoTypeOptionsFromBalances(employeeBalanceRow?.balances);
  }, [workspaceForYear?.employeeBalances]);
  const resolveAdjustLoadRequests = useCallback((employeeId: string, ptoTypeCode: LeaveSpherePtoType) => {
    if (!employeeId || !ptoTypeCode) {
      return [];
    }
    return loadRequestsByBalanceKey.get(`${employeeId}::${ptoTypeCode}`) || [];
  }, [loadRequestsByBalanceKey]);
  const openAdjustRequestEditor = useCallback((request: LeaveManagementLoadRequest) => {
    setAdjustRequestPickerTarget(null);
    setIsAdjustRequestPickerOpen(false);
    setPendingAdjustAction(null);
    setAdjustModalRequest(request);
    setAdjustForm({
      ...EMPTY_ADJUST_FORM,
      employeeId: request.employeeId,
      ptoTypeCode: request.ptoTypeCode as LeaveSpherePtoType,
      ptoActionCode: request.ptoActionCode,
      transactionId: request.id,
      hours: String(request.hours),
      description: asString(request.description),
    });
    setIsAdjustModalOpen(true);
  }, []);
  const openAdjustRequestPicker = useCallback((employeeId: string, ptoTypeCode: LeaveSpherePtoType) => {
    setPendingAdjustAction(null);
    setAdjustRequestPickerTarget({ employeeId, ptoTypeCode });
    setIsAdjustModalOpen(false);
    setIsAdjustRequestPickerOpen(true);
  }, []);
  const adjustRequestPickerRequests = useMemo(() => {
    if (!adjustRequestPickerTarget) {
      return [];
    }
    return resolveAdjustLoadRequests(adjustRequestPickerTarget.employeeId, adjustRequestPickerTarget.ptoTypeCode);
  }, [adjustRequestPickerTarget, resolveAdjustLoadRequests]);
  const adjustRequestPickerEmployeeName = useMemo(() => {
    if (!adjustRequestPickerTarget) {
      return "";
    }
    const employee = employeeById.get(adjustRequestPickerTarget.employeeId);
    return employee?.employeeName || adjustRequestPickerTarget.employeeId;
  }, [adjustRequestPickerTarget, employeeById]);
  const adjustCancelConfirmCopy = useMemo(() => ({
    title: "Cancel Load Request?",
    description: "This will cancel the selected approved PTO load request and remove it from the balance.",
    confirmLabel: "Cancel request",
    noteHelpText: "Optional. Add a reason for cancelling this load request.",
  }), []);
  const pendingAdjustActionCopy = useMemo(() => {
    if (!pendingAdjustAction) {
      return null;
    }
    return adjustCancelConfirmCopy;
  }, [pendingAdjustAction]);

  const resolvePtoHoursForEmployee = useCallback((startDate: string, endDate: string, employeeId: string) => {
    const employeeRegion = (workspaceForYear?.employees ?? []).find((item) => item.employeeId === employeeId)?.teamRegion;
    const employeeHolidayDates = buildLeaveSphereHolidayDateSet(workspaceForYear?.holidays, employeeRegion);
    return calculateLeaveSpherePtoHours(startDate, endDate, employeeHolidayDates);
  }, [workspaceForYear?.employees, workspaceForYear?.holidays]);
  const createRequestEmployeeBalanceRow = useMemo(
    () => (workspaceForYear?.employeeBalances ?? []).find((row) => row.employeeId === createForm.employeeId) || null,
    [createForm.employeeId, workspaceForYear?.employeeBalances],
  );
  const createRequestPtoTypeOptions = useMemo(
    () => buildLeaveSpherePtoTypeOptionsFromBalances(createRequestEmployeeBalanceRow?.balances),
    [createRequestEmployeeBalanceRow?.balances],
  );
  const createRequestAvailableHours = useMemo(
    () => resolveLeaveSpherePtoAvailableHours(createRequestEmployeeBalanceRow?.balances, createForm.type),
    [createForm.type, createRequestEmployeeBalanceRow?.balances],
  );
  const canSubmitCreateRequestWithinBalance = useMemo(() => {
    const requestedHours = Number(createForm.hours);
    if (!Number.isFinite(requestedHours) || requestedHours <= 0) {
      return false;
    }
    if (createRequestAvailableHours == null) {
      return false;
    }
    return requestedHours <= createRequestAvailableHours;
  }, [createForm.hours, createRequestAvailableHours]);
  const validateCreateRequestHours = useCallback((form: LeaveSpherePtoRequestFormState) => {
    const employeeBalanceRow = (workspaceForYear?.employeeBalances ?? []).find((row) => row.employeeId === createForm.employeeId) || null;
    const availableHours = resolveLeaveSpherePtoAvailableHours(employeeBalanceRow?.balances, form.type);
    return validateLeaveSpherePtoRequestedHours({
      requestedHours: form.hours,
      availableHours,
    });
  }, [createForm.employeeId, workspaceForYear?.employeeBalances]);
  const selectedRequestEmployeeBalanceRow = useMemo(
    () => (workspaceForYear?.employeeBalances ?? []).find((row) => row.employeeId === selectedRequest?.employeeId) || null,
    [selectedRequest?.employeeId, workspaceForYear?.employeeBalances],
  );
  const selectedRequestPtoTypeOptions = useMemo(
    () => buildLeaveSpherePtoTypeOptionsFromCatalog(workspaceForYear?.ptoTypes, selectedRequestEmployeeBalanceRow?.balances),
    [selectedRequestEmployeeBalanceRow?.balances, workspaceForYear?.ptoTypes],
  );
  const overview = useMemo(() => {
    return {
      totalCount: requests.length,
      pendingCount: pendingRequests.length,
      upcomingRequestCount: countUpcomingOutRequests(requests, todayIsoDate),
    };
  }, [pendingRequests.length, requests, todayIsoDate]);
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

  const currentLoadSnapshot = useMemo(
    () => buildLeaveManagementDraftSnapshot({
      reviewNote,
      createForm,
      adjustForm,
      setupForm,
      draftRecentHistorySearch,
      appliedRecentHistorySearch,
    }),
    [
      adjustForm,
      appliedRecentHistorySearch,
      createForm,
      draftRecentHistorySearch,
      reviewNote,
      setupForm,
    ],
  );
  const hasUnsavedLoadChanges = useMemo(
    () => Number.isInteger(loadedYear) && isLeaveManagementLoadDirty(loadedYear, currentLoadSnapshot),
    [currentLoadSnapshot, isLeaveManagementLoadDirty, loadedYear],
  );
  const captureCurrentLoadBaseline = useCallback((year: number, overrides?: Partial<LeaveManagementDraftSnapshot>) => {
    captureLeaveManagementLoadBaseline(year, buildLeaveManagementDraftSnapshot({
      reviewNote: overrides?.reviewNote ?? reviewNote,
      createForm: overrides?.createForm ?? createForm,
      adjustForm: overrides?.adjustForm ?? adjustForm,
      setupForm: overrides?.setupForm ?? setupForm,
      draftRecentHistorySearch: overrides?.draftRecentHistorySearch ?? draftRecentHistorySearch,
      appliedRecentHistorySearch: overrides?.appliedRecentHistorySearch ?? appliedRecentHistorySearch,
    }));
  }, [
    adjustForm,
    appliedRecentHistorySearch,
    captureLeaveManagementLoadBaseline,
    createForm,
    draftRecentHistorySearch,
    reviewNote,
    setupForm,
  ]);

  const restoreCurrentLoadBaseline = useCallback((year: number) => {
    const baseline = getLeaveManagementLoadBaseline(year);
    if (!baseline) {
      return false;
    }
    restoreLeaveManagementLoadSnapshot(baseline, {
      setReviewNote,
      setCreateForm,
      setAdjustForm,
      setSetupForm,
      setDraftRecentHistorySearch,
      setAppliedRecentHistorySearch,
      setSetupError,
    });
    return true;
  }, [
    getLeaveManagementLoadBaseline,
    setAdjustForm,
    setAppliedRecentHistorySearch,
    setCreateForm,
    setDraftRecentHistorySearch,
    setReviewNote,
    setSetupError,
    setSetupForm,
  ]);

  useEffect(() => {
    if (!hasHydratedPageState || loadedYear === null || !workspaceForYear) {
      return;
    }
    if (getLeaveManagementLoadBaseline(loadedYear)) {
      return;
    }
    captureCurrentLoadBaseline(loadedYear);
  }, [
    captureCurrentLoadBaseline,
    getLeaveManagementLoadBaseline,
    hasHydratedPageState,
    loadedYear,
    workspaceForYear,
  ]);

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

  const handleConnectGoogleCalendar = useCallback(async () => {
    setIsGoogleCalendarConnecting(true);
    const popup = window.open("", "_blank", "width=640,height=760");
    if (!popup) {
      setIsGoogleCalendarConnecting(false);
      toast.error(
        "Google Calendar connection failed",
        "Your browser blocked the sign-in tab. Allow popups for LeaveSphere and try again.",
      );
      return;
    }
    popup.document.write("<!doctype html><title>LeaveSphere Google Calendar</title><p style=\"font-family:system-ui,sans-serif;padding:24px;color:#0f172a\">Opening Google sign-in...</p>");
    try {
      const result = await loadLeaveManagementGoogleCalendarAuthorizationUrl({
        requestJson,
      });
      const authorizationUrl = result.authorizationUrl?.trim();
      if (!authorizationUrl) {
        try {
          popup.close();
        } catch {
          // ignore popup close failures
        }
        toast.error(
          "Google Calendar connection failed",
          "The server did not return an authorization URL. Check the Google Calendar OAuth config and redirect URI.",
        );
        return;
      }
      if (popup.closed) {
        toast.error(
          "Google Calendar connection failed",
          "The sign-in tab was closed before Google Calendar authorization could start.",
        );
        return;
      }
      popup.location.assign(authorizationUrl);
      popup.focus();
    } catch (error) {
      try {
        popup.close();
      } catch {
        // ignore popup close failures
      }
      const message = error instanceof Error ? error.message : "Unable to start Google Calendar OAuth.";
      toast.error("Google Calendar connection failed", message);
    } finally {
      setIsGoogleCalendarConnecting(false);
    }
  }, [requestJson, toast]);

  const handleDisconnectGoogleCalendar = useCallback(async () => {
    setIsGoogleCalendarDisconnecting(true);
    try {
      const result = await disconnectLeaveManagementGoogleCalendarConnection({
        requestJson,
      });
      setGoogleCalendarConnectionStatus({
        connected: false,
        connectedAt: null,
        lastAuthorizedAt: null,
        scopes: [],
      });
      if (result.revocationError) {
        toast.info(
          "Google Calendar disconnected",
          `Stored credentials were removed, but remote revocation reported: ${result.revocationError}`,
        );
      } else {
        toast.success("Google Calendar disconnected", "Stored calendar credentials were revoked for LeaveSphere.");
      }
    } finally {
      setIsGoogleCalendarDisconnecting(false);
      setIsGoogleCalendarDisconnectConfirmOpen(false);
    }
  }, [requestJson, toast]);

  const handleGoogleCalendarCardActivate = useCallback(() => {
    if (isGoogleCalendarStatusLoading || isGoogleCalendarConnecting || isGoogleCalendarDisconnecting) {
      return;
    }
    if (Boolean(googleCalendarConnectionStatus?.connected)) {
      setIsGoogleCalendarDisconnectConfirmOpen(true);
      return;
    }
    void handleConnectGoogleCalendar();
  }, [
    googleCalendarConnectionStatus?.connected,
    handleConnectGoogleCalendar,
    isGoogleCalendarConnecting,
    isGoogleCalendarDisconnecting,
    isGoogleCalendarStatusLoading,
  ]);

  const handleGoogleCalendarCardKeyDown = useCallback((event: React.KeyboardEvent<HTMLElement>) => {
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    event.preventDefault();
    handleGoogleCalendarCardActivate();
  }, [handleGoogleCalendarCardActivate]);

  const refreshGoogleCalendarConnectionStatus = useCallback(async () => {
    if (!canUseGoogleCalendarAdminApi) {
      setGoogleCalendarConnectionStatus(null);
      return;
    }
    setIsGoogleCalendarStatusLoading(true);
    try {
      const status = await loadLeaveManagementGoogleCalendarConnectionStatus({
        requestJson,
      });
      setGoogleCalendarConnectionStatus(status);
    } catch {
      setGoogleCalendarConnectionStatus(null);
    } finally {
      setIsGoogleCalendarStatusLoading(false);
    }
  }, [canUseGoogleCalendarAdminApi, requestJson]);

  useEffect(() => {
    if (!canUseGoogleCalendarAdminApi) {
      return;
    }
    const handleMessage = (event: MessageEvent) => {
      if (event.origin !== window.location.origin) {
        return;
      }
      const data = event.data;
      if (!data || typeof data !== "object") {
        return;
      }
      const record = data as Record<string, unknown>;
      if (record.type !== "leavesphere-google-calendar-oauth") {
        return;
      }
      void refreshGoogleCalendarConnectionStatus();
      const status = String(record.status || "").trim().toLowerCase();
      const message = String(record.message || "").trim();
      if (status === "connected") {
        toast.success("Google Calendar connected", message || "LeaveSphere can now create and update calendar events.");
        return;
      }
      if (status === "error") {
        toast.error("Google Calendar connection failed", message || "Unable to complete Google Calendar connection.");
      }
    };
    window.addEventListener("message", handleMessage);
    return () => {
      window.removeEventListener("message", handleMessage);
    };
  }, [canUseGoogleCalendarAdminApi, refreshGoogleCalendarConnectionStatus, toast]);

  useEffect(() => {
    if (!canUseGoogleCalendarAdminApi) {
      return;
    }
    void refreshGoogleCalendarConnectionStatus();
  }, [canUseGoogleCalendarAdminApi, refreshGoogleCalendarConnectionStatus]);

  useEffect(() => {
    if (!canUseGoogleCalendarAdminApi) {
      return;
    }
    const searchParams = new URLSearchParams(window.location.search);
    const calendarStatus = asString(searchParams.get("google_calendar"));
    const calendarError = asString(searchParams.get("google_calendar_error"));
    if (!calendarStatus && !calendarError) {
      return;
    }

    if (calendarStatus === "connected") {
      toast.success("Google Calendar connected", "LeaveSphere can now create and update calendar events.");
    } else if (calendarStatus === "disconnected") {
      toast.success("Google Calendar disconnected", "LeaveSphere no longer has calendar access.");
    }

    if (calendarError) {
      toast.error("Google Calendar connection failed", calendarError);
    }

    searchParams.delete("google_calendar");
    searchParams.delete("google_calendar_error");
    const nextQuery = searchParams.toString();
    window.history.replaceState(
      {},
      "",
      `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ""}${window.location.hash || ""}`,
    );
    void refreshGoogleCalendarConnectionStatus();
  }, [canUseGoogleCalendarAdminApi, refreshGoogleCalendarConnectionStatus, toast]);

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
      id: "leavesphere-leave-management-refresh",
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

  const loadWorkspace = useCallback(async (
    year: number,
    policy: CachePolicy = "cache-first",
    options: WorkspaceLoadOptions = {},
    freshData = false,
  ): Promise<boolean> => {
    const requestToken = ++workspaceLoadRequestTokenRef.current;
    const cacheSnapshot = readLeaveSpherePtoWorkspaceCacheSnapshot<LeaveManagementWorkspaceData>({
      pageCode: LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
      tenantSlug,
      userId: currentUserId,
      year,
    });
    const cachedWorkspace = cacheSnapshot?.data ?? null;
    const hasCachedWorkspace = Boolean(cachedWorkspace);
    const shouldUseCache = policy !== "network-only" && hasCachedWorkspace;
    const shouldFetchFromNetwork = shouldFetchNetwork(policy, cacheSnapshot);
    const shouldShowRefreshing = hasCachedWorkspace || (loadedYear === year && workspaceForYear !== null);
    const includePending = options.includePending ?? true;

    if (shouldUseCache && cachedWorkspace) {
      commitWorkspace(cachedWorkspace, "cache", year, cacheSnapshot?.fetchedAt ?? Date.now());
      recordLoadedRequestMonths(options);
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
        recordLoadedRequestMonths(options);
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
      const result = await loadLeaveManagementWorkspace({
        requestJson,
        workspaceKey: leaveManagementWorkspaceKey,
        currentUserId,
        currentUserName,
        timeZone: tenantTimeZone,
        freshData,
        includePending,
        calendarMonth: options.overlapMonth ?? null,
        historyStartDate: options.historyStartDate ?? null,
        historyEndDate: options.historyEndDate ?? null,
      });
      if (requestToken !== workspaceLoadRequestTokenRef.current) {
        return false;
      }
      commitWorkspace(result.workspace, "network", year, Date.now());
      recordLoadedRequestMonths(options);
      setRefreshMessage(result.refreshMessage);
      return true;
    } catch {
      if (requestToken !== workspaceLoadRequestTokenRef.current) {
        return false;
      }
      if (hasCachedWorkspace && cachedWorkspace) {
        commitWorkspace(cachedWorkspace, "cache", year, cacheSnapshot?.fetchedAt ?? Date.now());
        recordLoadedRequestMonths(options);
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
    recordLoadedRequestMonths,
    workspaceForYear,
    leaveManagementWorkspaceKey,
  ]);

  const executeYearLoad = useCallback(async (parsedYear: number, plan: CriteriaLoadPlan) => {
    const requestedMonthKey = buildMonthKeyForYear(currentMonthKey, parsedYear) || `${parsedYear}-01`;
    if (plan.shouldResetToBaseline) {
      restoreCurrentLoadBaseline(parsedYear);
    }
    setCalendarMonth(requestedMonthKey);
    const didLoad = await loadWorkspace(
      parsedYear,
      plan.shouldIgnoreCache ? "network-only" : "cache-first",
      buildInitialRequestLoadWindow(requestedMonthKey, parsedYear),
      plan.shouldIgnoreCache,
    );
    if (didLoad) {
      applyRecentHistorySearchKeyword("");
      captureCurrentLoadBaseline(parsedYear, {
        draftRecentHistorySearch: "",
        appliedRecentHistorySearch: "",
      });
    }
    return didLoad;
  }, [
    applyRecentHistorySearchKeyword,
    captureCurrentLoadBaseline,
    currentMonthKey,
    loadWorkspace,
    restoreCurrentLoadBaseline,
  ]);

  const handleLoadByYear = useCallback(async () => {
    const parsedYear = Number(selectedYear);
    if (!Number.isInteger(parsedYear)) {
      return;
    }
    const loadPlan = resolveCriteriaLoadPlan({
      trigger: "load-button",
      criteriaKey: parsedYear,
      loadedCriteriaKey: loadedYear,
      hasDirtyState: hasUnsavedLoadChanges,
    });
    if (loadPlan.shouldPromptBeforeReload) {
      setPendingReloadYear(parsedYear);
      return;
    }
    await executeYearLoad(parsedYear, loadPlan);
  }, [executeYearLoad, hasUnsavedLoadChanges, loadedYear, selectedYear]);

  const handleConfirmReloadLoad = useCallback(async () => {
    if (pendingReloadYear === null || !Number.isInteger(pendingReloadYear)) {
      setPendingReloadYear(null);
      return;
    }
    const parsedYear = pendingReloadYear;
    setPendingReloadYear(null);
    const loadPlan = resolveCriteriaLoadPlan({
      trigger: "load-button",
      criteriaKey: parsedYear,
      loadedCriteriaKey: loadedYear,
      hasDirtyState: false,
    });
    await executeYearLoad(parsedYear, loadPlan);
  }, [executeYearLoad, loadedYear, pendingReloadYear]);

  const isRequestMonthAlreadyLoaded = useCallback((monthKey: string) => {
    const normalizedMonthKey = normalizeMonthKey(monthKey);
    if (!normalizedMonthKey) {
      return false;
    }
    return loadedRequestMonthKeysRef.current.has(normalizedMonthKey);
  }, []);

  const handleCalendarMonthChange = useCallback((nextMonthKey: string) => {
    const normalizedMonthKey = normalizeMonthKey(nextMonthKey) || nextMonthKey;
    setCalendarMonth(normalizedMonthKey);
    if (isRequestMonthAlreadyLoaded(normalizedMonthKey)) {
      return;
    }
    const effectiveYear = loadedYearRef.current ?? Number(selectedYear);
    if (!Number.isInteger(effectiveYear)) {
      return;
    }
    void loadWorkspace(
      effectiveYear,
      "network-only",
      {
        overlapMonth: normalizedMonthKey,
        includePending: true,
      },
    );
  }, [isRequestMonthAlreadyLoaded, loadWorkspace, selectedYear]);

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
    createRequestEmployeeIdRef.current = defaultEmployeeId;
    const defaultType = getEmployeePtoTypeOptions(defaultEmployeeId)[0]?.value || "";
    setCreateForm({
      ...EMPTY_CREATE_FORM,
      employeeId: defaultEmployeeId,
      type: defaultType as LeaveSpherePtoType,
      startDate: defaultIsoDate,
      endDate: defaultIsoDate,
      hours: resolvePtoHoursForEmployee(defaultIsoDate, defaultIsoDate, defaultEmployeeId),
    });
    setIsCreateModalOpen(true);
  }, [employeeOptions, getEmployeePtoTypeOptions, loadedYearDateBounds, resolvePtoHoursForEmployee, todayIsoDate]);

  useEffect(() => {
    if (!isCreateModalOpen) {
      return;
    }
    setCreateForm((current) => ({
      ...current,
      employeeId: current.employeeId || employeeOptions[0]?.value || "",
      type: (() => {
        const nextEmployeeId = current.employeeId || employeeOptions[0]?.value || "";
        const nextTypeOptions = getEmployeePtoTypeOptions(nextEmployeeId);
        return nextTypeOptions.some((item) => item.value === current.type)
          ? current.type
          : (nextTypeOptions[0]?.value || "");
      })(),
    }));
  }, [employeeOptions, getEmployeePtoTypeOptions, isCreateModalOpen]);

  const applyWorkspace = useCallback((next: LeaveManagementWorkspaceData) => {
    const effectiveYear = loadedYear ?? (Number.isInteger(selectedYearNumber) ? selectedYearNumber : currentYear);
    const merged = mergeLeaveManagementWorkspace(workspaceRef.current, next);
    const scopedWorkspace = filterWorkspaceByLoadedRequestMonths(
      merged,
      effectiveYear,
      loadedRequestMonthKeysRef.current,
    );
    commitWorkspace(scopedWorkspace, "network", effectiveYear);
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
    const ptoTypeCode = params?.ptoTypeCode || getEmployeePtoTypeOptions(employeeId)[0]?.value || "";
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
      description: isEditMode ? asString(selectedRequest?.description) : "",
    };

    setPendingAdjustAction(null);
    setAdjustRequestPickerTarget(null);
    setAdjustModalRequest(isEditMode ? (selectedRequest || null) : null);
    setAdjustForm(nextForm);
    setIsAdjustModalOpen(true);
  }, [employeeOptions, getEmployeePtoTypeOptions, resolveAdjustLoadRequests]);

  const openDuplicateBalancesModal = useCallback(() => {
    if (!workspaceForYear || !Number.isInteger(loadedYear)) {
      return;
    }
    setIsDuplicateBalancesModalOpen(true);
  }, [loadedYear, workspaceForYear]);

  const closeDuplicateBalancesModal = useCallback(() => {
    setIsDuplicateBalancesModalOpen(false);
  }, []);

  const handleDuplicateBalances = useCallback(async ({
    yearFrom,
    yearTo,
    employeeIds,
  }: LeaveSpherePtoDuplicateBalancesSubmitParams) => {
    const currentLoadedYear = loadedYear;
    if (!Number.isInteger(yearFrom) || !Number.isInteger(yearTo) || !Number.isInteger(currentLoadedYear)) {
      return false;
    }
    setIsMutating(true);
    try {
      const result = await duplicateLeaveManagementBalances({
        requestJson,
        workspaceKey: leaveManagementWorkspaceKey,
        currentUserId,
        currentUserName,
        currentWorkspace: workspaceRef.current,
        payload: {
          yearFrom,
          yearTo,
          employeeIds,
        },
      });
      if (yearTo !== currentLoadedYear) {
        setCalendarMonth(buildMonthKeyForYear(currentMonthKey, yearTo) || `${yearTo}-01`);
        loadedRequestMonthKeysRef.current = new Set();
      }
      commitWorkspace(result.workspace, "network", yearTo);
      const employeeLabel = employeeIds.length === 0
        ? "all employees"
        : `${employeeIds.length} employee${employeeIds.length === 1 ? "" : "s"}`;
      toast.success(
        "Balances duplicated",
        `Approved LOAD transactions from ${yearFrom} were duplicated to ${yearTo} for ${employeeLabel}.`,
      );
      closeDuplicateBalancesModal();
      return true;
    } catch {
      toast.error("Duplicate failed", "Unable to duplicate PTO balances right now.");
      return false;
    } finally {
      setIsMutating(false);
    }
  }, [
    commitWorkspace,
    currentMonthKey,
    currentUserId,
    currentUserName,
    closeDuplicateBalancesModal,
    loadedYear,
    requestJson,
    toast,
    leaveManagementWorkspaceKey,
  ]);

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
    const employeeId = asString(createRequestEmployeeIdRef.current) || asString(createForm.employeeId);
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
      const createdResult = await createLeaveManagementRequest({
        requestJson,
        workspaceKey: leaveManagementWorkspaceKey,
        currentUserId,
        currentUserName,
        currentWorkspace: workspaceRef.current,
        payload: {
          employeeId: nextPendingCreateRequest.employeeId,
          ...nextPendingCreateRequest.payload,
          approveImmediately,
        },
      });
      applyWorkspace(createdResult.workspace);
      toast.success(
        approveImmediately ? "Request approved" : "Request created",
        approveImmediately
          ? "PTO request was created and approved."
          : "PTO request was created on behalf of the selected employee.",
      );
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
    leaveManagementWorkspaceKey,
  ]);

  const handleReviewRequest = useCallback(async (action: LeaveSphereReviewAction) => {
    if (!selectedRequest) {
      return;
    }
    setIsMutating(true);
    try {
      const result = await reviewLeaveManagementRequest({
        requestJson,
        workspaceKey: leaveManagementWorkspaceKey,
        currentUserId,
        currentUserName,
        currentWorkspace: workspaceRef.current,
        payload: {
          requestId: selectedRequest.id,
          action,
          approverNote: reviewNote,
        },
      });
      applyWorkspace(result.workspace);
      setSelectedRequestId(null);
      setReviewNote("");
      if (action === "approve") {
        toast.success("Request approved", "Employee request status was updated to approved.");
      } else if (action === "reject") {
        toast.success("Request rejected", "Employee request status was updated to rejected.");
      } else if (action === "cancel") {
        toast.success("Request cancelled", "Employee request status was updated to cancelled.");
      } else {
        toast.success("Decision reverted to pending", "Request status was changed back to pending.");
      }
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
    leaveManagementWorkspaceKey,
  ]);

  const handleConfirmReviewAction = useCallback(async () => {
    if (!pendingReviewAction) {
      return;
    }
    if (pendingReviewActionRequiresNote && !reviewNote.trim()) {
      toast.error("Admin note required", "Add a note before rejecting or cancelling this request.");
      return;
    }
    const action = pendingReviewAction;
    setPendingReviewAction(null);
    await handleReviewRequest(action);
  }, [handleReviewRequest, pendingReviewAction, pendingReviewActionRequiresNote, reviewNote, toast]);

  const closeAdjustModal = useCallback(() => {
    setPendingAdjustAction(null);
    setAdjustCancelNote("");
    setIsAdjustRequestPickerOpen(false);
    setAdjustRequestPickerTarget(null);
    setAdjustModalRequest(null);
    setAdjustForm(EMPTY_ADJUST_FORM);
    setIsAdjustModalOpen(false);
  }, []);

  const handleAdjustModalOpenChange = useCallback((nextOpen: boolean) => {
    setIsAdjustModalOpen(nextOpen);
  }, []);

  const handleAdjustBalance = useCallback(async ({
    requestId,
    payload,
  }: LeaveSpherePtoLoadHoursSubmitParams) => {
    if (!Number.isInteger(loadedYear)) {
      return false;
    }
    setIsMutating(true);
    try {
      const result = await adjustLeaveManagementBalance({
        requestJson,
        workspaceKey: leaveManagementWorkspaceKey,
        currentUserId,
        currentUserName,
        currentWorkspace: workspaceRef.current,
        payload: {
          ...payload,
          transactionId: requestId || payload.transactionId,
        },
      });
      applyWorkspace(result.workspace);
      toast.success("PTO hours updated", "Loaded PTO hours were updated for the selected employee.");
      return true;
    } catch {
      toast.error("Update failed", "Unable to update PTO balance hours right now.");
      return false;
    } finally {
      setIsMutating(false);
    }
  }, [
    applyWorkspace,
    currentUserId,
    currentUserName,
    loadedYear,
    requestJson,
    toast,
    leaveManagementWorkspaceKey,
  ]);

  const handleCancelAdjustRequest = useCallback(async (note: string) => {
    if (!adjustModalRequest || !Number.isInteger(loadedYear)) {
      return;
    }
    setIsMutating(true);
    try {
      const result = await adjustLeaveManagementBalance({
        requestJson,
        workspaceKey: leaveManagementWorkspaceKey,
        currentUserId,
        currentUserName,
        currentWorkspace: workspaceRef.current,
        payload: {
          employeeId: adjustModalRequest.employeeId,
          ptoTypeCode: adjustModalRequest.ptoTypeCode,
          ptoActionCode: "load_grant",
          transactionId: adjustModalRequest.id,
          hours: adjustModalRequest.hours,
          year: Number(loadedYear),
          status: "Canceled",
          description: asString(adjustModalRequest.description),
          approverNote: asString(note),
        },
      });
      applyWorkspace(result.workspace);
      closeAdjustModal();
      toast.success("Load request canceled", "Approved PTO load request was canceled.");
    } catch {
      toast.error("Cancel failed", "Unable to cancel PTO load request right now.");
    } finally {
      setPendingAdjustAction(null);
      setIsMutating(false);
    }
  }, [
    applyWorkspace,
    closeAdjustModal,
    currentUserId,
    currentUserName,
    loadedYear,
    requestJson,
    toast,
    adjustModalRequest,
    leaveManagementWorkspaceKey,
  ]);

  const handleConfirmAdjustAction = useCallback(async () => {
    if (pendingAdjustAction === "cancel") {
      await handleCancelAdjustRequest(adjustCancelNote);
    }
  }, [adjustCancelNote, handleCancelAdjustRequest, pendingAdjustAction]);

  const handleSetupSave = useCallback(async () => {
    let payload: LeaveManagementSetupInput | null = null;
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
      const result = await updateLeaveManagementSetupData({
        requestJson,
        workspaceKey: leaveManagementWorkspaceKey,
        currentUserId,
        currentUserName,
        currentWorkspace: workspaceRef.current,
        payload,
      });
      applyWorkspace(result.workspace);
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
    leaveManagementWorkspaceKey,
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
      const result = await updateLeaveManagementRequest({
        requestJson,
        workspaceKey: leaveManagementWorkspaceKey,
        currentUserId,
        currentUserName,
        currentWorkspace: workspaceRef.current,
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
      applyWorkspace(result.workspace);
      setSelectedRequestId(null);
      setReviewNote("");
      toast.success("Request updated", "PTO request details were updated.");
    } finally {
      setIsMutating(false);
    }
  }, [applyWorkspace, currentUserId, currentUserName, loadedYear, requestJson, toast, leaveManagementWorkspaceKey]);
  const renderRequestsSection = () => {
    return (
      <SectionCard
        title="Requests"
        description="Search and review PTO requests as cards."
        actions={(
          <ActionIconButton
            tooltip="Create Request"
            onClick={() => {
              openCreateRequestModal();
            }}
            icon={<Plus />}
          />
        )}
        style={calendarSectionHeight > 0 ? { height: `${calendarSectionHeight}px` } : undefined}
        contentClassName="flex min-h-0 flex-1 flex-col gap-3"
      >
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
              <TooltipTarget text="Clear request search">
                <button
                  type="button"
                  onClick={handleClearRecentHistorySearch}
                  className="absolute right-2 top-1/2 inline-flex size-6 -translate-y-1/2 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300"
                  aria-label="Clear request search"
                >
                  <X className="size-3.5" />
                </button>
              </TooltipTarget>
            ) : null}
          </div>
        </div>

        <div className="min-h-0 flex-1 space-y-4 overflow-y-auto pr-1 pt-1.5">
          <LeaveSpherePtoRequestList
            requests={filteredRecentRequests}
            emptyMessage={recentRequests.length === 0
              ? "No requests loaded yet."
              : "No requests match your keyword filter."}
            timeZone={tenantTimeZone}
            todayIsoDate={todayIsoDate}
            resolveEmployee={resolveRequestEmployee}
            getTypeLabel={(request) => requestTypeLabel(request.ptoTypeCode ?? request.type, ptoTypeMetaLookup)}
            getHoursLabel={(request) => formatHoursLabel(request.hours)}
            onRequestClick={(request) => {
              setSelectedRequestId(request.id);
              setReviewNote(request.approverNote || "");
            }}
          />
        </div>
      </SectionCard>
    );
  };

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
      const requestType = requestTypeLabel(request.ptoTypeCode ?? request.type, ptoTypeMetaLookup);
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
      <div className="grid items-stretch gap-4 xl:grid-cols-[minmax(19rem,25rem)_minmax(0,1fr)]">
        {renderRequestsSection()}

        <LeaveSphereMonthCalendar
          sectionRef={calendarSectionRef}
          title="Calendar"
          description="Who is out and company holidays"
          monthKey={calendarMonth}
          className="self-start"
          onMonthChange={handleCalendarMonthChange}
          onMonthHeadingClick={loadedYear === currentYear ? () => setCalendarMonth(currentMonthKey) : undefined}
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
      </div>
    );
  };

  const renderBalancesTab = () => {
    return (
      <SectionCard
        title="Employee PTO Balances"
        description="Only employees with PTO activity in the loaded year are shown."
        actions={(
          <>
            <ActionIconButton
              tooltip="Duplicate Balances"
              onClick={openDuplicateBalancesModal}
              disabled={!workspaceForYear}
              icon={<Copy />}
            />
            <ActionIconButton
              tooltip="Load PTO Hours"
              onClick={() => {
                openLoadHoursModal({ mode: "create" });
              }}
              disabled={!workspaceForYear}
              icon={<Plus />}
            />
          </>
        )}
      >
        {visibleBalanceRows.length === 0 ? (
          <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-center text-sm text-slate-600">
            No PTO transactions were found for the selected year.
          </div>
        ) : (
          <div className="overflow-x-auto rounded-xl border border-blue-100">
            <table className="w-full table-fixed text-left text-sm">
              <thead className="bg-blue-50/70 text-xs uppercase tracking-[0.08em] text-slate-600">
                <tr>
                  <th className="px-3 py-2.5" style={{ width: BALANCE_EMPLOYEE_COLUMN_WIDTH, minWidth: BALANCE_EMPLOYEE_COLUMN_WIDTH, maxWidth: BALANCE_EMPLOYEE_COLUMN_WIDTH }}>
                    Employee
                  </th>
                  {balanceColumns.map((column) => (
                    <th
                      key={column.type}
                      className="px-3 py-2.5 text-center"
                      style={{ width: balanceTypeColumnWidth, minWidth: balanceTypeColumnWidth, maxWidth: balanceTypeColumnWidth }}
                    >
                      {column.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {visibleBalanceRows.map((row) => {
                  const balanceByType = new Map(row.balances.map((item) => [asString(item.type).toLowerCase(), item] as const));
                  const renderCell = (type: string) => {
                    const normalizedType = asString(type).toLowerCase();
                    const balance = balanceByType.get(normalizedType) || null;
                    if (!balance || !hasBalanceActivity(balance)) {
                      return <span className="text-slate-400">-</span>;
                    }
                    const usedHours = balance.usedHours + balance.scheduledHours;
                    const usedRatio = balance.totalHours > 0
                      ? Math.min(1, usedHours / balance.totalHours)
                      : 0;
                    const loadRequests = resolveAdjustLoadRequests(row.employeeId, normalizedType as LeaveSpherePtoType);
                    return (
                      <TooltipTarget text="Click to edit hours">
                        <button
                          type="button"
                          onClick={() => {
                            if (loadRequests.length > 1) {
                              openAdjustRequestPicker(row.employeeId, normalizedType as LeaveSpherePtoType);
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
                              ptoTypeCode: normalizedType as LeaveSpherePtoType,
                            });
                          }}
                          className="group flex w-full flex-col items-center rounded-lg border border-transparent px-2 py-1.5 text-center transition hover:border-blue-200 hover:bg-blue-50/55 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300"
                          aria-label={`Edit ${requestTypeLabel(normalizedType, ptoTypeMetaLookup)} hours for ${row.employeeName}`}
                        >
                          <span className="text-sm font-semibold text-slate-900 transition group-hover:text-blue-700">
                            {formatHoursLabel(usedHours)}
                            {" / "}
                            {formatHoursLabel(balance.totalHours)}
                          </span>
                          <span className="mt-1.5 h-1.5 w-full overflow-hidden rounded-full bg-blue-100">
                            <span
                              className="relative block h-full overflow-hidden rounded-full bg-gradient-to-r from-blue-800 via-blue-600 to-sky-500 shadow-[0_0_8px_rgba(37,99,235,0.18)] transition-[width] duration-300 ease-out"
                              style={{ width: `${usedRatio * 100}%` }}
                            >
                              <span className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(255,255,255,0.28)_0%,rgba(255,255,255,0.10)_42%,rgba(255,255,255,0)_76%)]" />
                              <span className="pointer-events-none absolute inset-x-0 top-0 h-px bg-white/30" />
                              <span
                                aria-hidden="true"
                                className="pointer-events-none absolute inset-0 bg-[linear-gradient(110deg,transparent_0%,rgba(255,255,255,0.08)_28%,rgba(255,255,255,0.42)_50%,rgba(255,255,255,0.10)_72%,transparent_100%)] bg-[length:200%_100%] opacity-0 transition-opacity duration-150 group-hover:opacity-100 motion-safe:group-hover:animate-[shimmer_2.6s_linear_infinite]"
                              />
                            </span>
                          </span>
                        </button>
                      </TooltipTarget>
                    );
                  };
                  return (
                    <tr key={row.employeeId} className="border-t border-blue-100/80 bg-white text-slate-700">
                      <td
                        className="px-3 py-2.5 align-top"
                        style={{ width: BALANCE_EMPLOYEE_COLUMN_WIDTH, minWidth: BALANCE_EMPLOYEE_COLUMN_WIDTH, maxWidth: BALANCE_EMPLOYEE_COLUMN_WIDTH }}
                      >
                        <LeaveSpherePtoEmployeeHeader
                          employeeName={row.employeeName}
                          pictureUrl={row.pictureUrl}
                          title={row.employeeName}
                          subtitle={row.subtitle || undefined}
                          titleClassName="font-medium"
                        />
                      </td>
                      {balanceColumns.map((column) => (
                        <td
                          key={column.type}
                          className="px-3 py-2.5 text-center align-top"
                          style={{ width: balanceTypeColumnWidth, minWidth: balanceTypeColumnWidth, maxWidth: balanceTypeColumnWidth }}
                        >
                          {renderCell(column.type)}
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </SectionCard>
    );
  };

  const renderSetupTab = () => {
    const googleCalendarConnected = Boolean(googleCalendarConnectionStatus?.connected);
    const googleCalendarStatusTone = isGoogleCalendarStatusLoading
      ? "border-blue-200 bg-blue-50/70"
      : googleCalendarConnected
        ? "border-emerald-200 bg-emerald-50/80"
        : "border-amber-200 bg-amber-50/80";
    const googleCalendarStatusPillTone = isGoogleCalendarStatusLoading
      ? "bg-blue-100 text-blue-700"
      : googleCalendarConnected
        ? "bg-emerald-100 text-emerald-700"
        : "bg-amber-100 text-amber-700";
    const googleCalendarHeaderBadgeTone = isGoogleCalendarStatusLoading
      ? "border-blue-200 bg-blue-50 text-blue-800"
      : googleCalendarConnected
        ? "border-emerald-200 bg-emerald-50 text-emerald-800"
        : "border-amber-200 bg-amber-50 text-amber-800";
    const googleCalendarCardHint = isGoogleCalendarStatusLoading
      ? "Checking connection..."
      : googleCalendarConnected
        ? "Click to disconnect."
        : "Click to connect.";
    return (
      <div className="grid gap-4">
        <SectionCard
          title={(
            <span className="inline-flex flex-wrap items-center gap-2">
              <span>Google Calendar</span>
              <span className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-[11px] font-semibold tracking-[0.02em] ${googleCalendarHeaderBadgeTone}`}>
                {isGoogleCalendarStatusLoading
                  ? "Loading"
                  : googleCalendarConnected
                    ? "Connected"
                    : "Disconnected"}
              </span>
            </span>
          )}
          description="Connect the approval flow to a shared workspace calendar."
          onClick={handleGoogleCalendarCardActivate}
          onKeyDown={handleGoogleCalendarCardKeyDown}
          role="button"
          tabIndex={0}
          aria-disabled={isGoogleCalendarStatusLoading || isGoogleCalendarConnecting || isGoogleCalendarDisconnecting}
          className={[
            "transition-all duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300",
            isGoogleCalendarStatusLoading || isGoogleCalendarConnecting || isGoogleCalendarDisconnecting
              ? "cursor-not-allowed opacity-75"
              : "cursor-pointer hover:-translate-y-0.5",
          ].join(" ")}
          contentClassName="space-y-3"
        >
          <div className={`rounded-xl border p-3 ${googleCalendarStatusTone}`}>
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-600">Connection status</p>
                <p className={`mt-1 text-sm font-medium ${
                  isGoogleCalendarStatusLoading
                    ? "text-blue-800"
                    : googleCalendarConnected
                      ? "text-emerald-800"
                      : "text-amber-800"
                }`}>
                  {isGoogleCalendarStatusLoading
                    ? "Checking connection..."
                    : googleCalendarConnected
                      ? "Connected"
                      : "Not connected"}
                </p>
              </div>
              <span className={`inline-flex rounded-full px-2.5 py-1 text-xs font-semibold ${googleCalendarStatusPillTone}`}>
                {isGoogleCalendarStatusLoading
                  ? "Loading"
                  : googleCalendarConnected
                    ? "Active"
                    : "Disconnected"}
              </span>
            </div>
            {googleCalendarConnectionStatus?.connectedAt ? (
              <p className="mt-3 text-xs text-slate-600">
                Connected at: {googleCalendarConnectionStatus.connectedAt}
              </p>
            ) : null}
            <p className="mt-3 text-xs font-medium text-slate-600">
              {googleCalendarCardHint}
            </p>
          </div>
          <p className="text-sm text-slate-600">
            LeaveSphere uses OAuth to create and update calendar events after an approval is submitted.
          </p>
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
          description="Admin overview of PTO activity, calendar, request queue, balances, and setup data."
          gradientVariant="workspace"
        />
      )}
      footer={cacheStatus && loadedYear !== null ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            setIsChipRefreshOverlayVisible(true);
            const loadYear = loadedYear ?? currentYear;
            const requestedMonthKey = buildMonthKeyForYear(currentMonthKey, loadYear) || `${loadYear}-01`;
            setCalendarMonth(requestedMonthKey);
            void (async () => {
              try {
                const refreshPlan = resolveCriteriaLoadPlan({
                  trigger: "cache-chip",
                  criteriaKey: loadYear,
                  loadedCriteriaKey: loadedYear,
                });
                await loadWorkspace(
                  loadYear,
                  refreshPlan.shouldIgnoreCache ? "network-only" : "cache-first",
                  buildInitialRequestLoadWindow(requestedMonthKey, loadYear),
                  refreshPlan.shouldIgnoreCache,
                );
              } finally {
                setIsChipRefreshOverlayVisible(false);
              }
            })();
          }}
          disabled={isInitializing || isRefreshing || isMutating || !isOnline}
          refreshing={isRefreshing || isChipRefreshOverlayVisible}
          refreshLabel="Hard refresh Leave Management workspace"
          tooltipText={isOnline ? "Click to hard refresh requests, balances, and setup data" : "Offline. Reconnect to hard refresh Leave Management workspace."}
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
          <div className="grid gap-3 sm:grid-cols-3">
            <article className="rounded-2xl border border-blue-200 bg-blue-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-blue-800">Total requests</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-blue-900">{overview.totalCount}</p>
            </article>
            <button
              type="button"
              onClick={() => {
                setIsPendingRequestsModalOpen(true);
              }}
              className={[
                "rounded-2xl border p-4 text-left transition-all duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-300",
                getLeaveSpherePtoRequestSurfaceClassName("pending", "tile"),
                "hover:-translate-y-0.5",
              ].join(" ")}
            >
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-amber-800">Pending requests</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-amber-900">{overview.pendingCount}</p>
            </button>
            <article className="rounded-2xl border border-indigo-200 bg-indigo-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-indigo-800">Upcoming requests</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-indigo-900">{overview.upcomingRequestCount}</p>
            </article>
          </div>

          <div className="grid w-full grid-cols-3 gap-1.5 rounded-xl border border-blue-200 bg-gradient-to-r from-blue-50/85 via-indigo-50/50 to-violet-50/70 p-1.5">
            {TAB_OPTIONS.map((item) => {
              const active = activeTab === item.id;
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
                  {item.id === "balances" ? <Users className="size-3.5" /> : null}
                  {item.id === "setup" ? <Settings2 className="size-3.5" /> : null}
                  {item.label}
                </button>
              );
            })}
          </div>

          <div className="relative">
            {activeTab === "calendar" ? renderCalendarTab() : null}
            {activeTab === "balances" ? renderBalancesTab() : null}
            {activeTab === "setup" ? renderSetupTab() : null}

            <SectionLoadingLayer
              active={loadingContract.sectionOverlayActive}
              message={loadingContract.sectionOverlayMessage}
            />
          </div>
        </>
      )}

      <ConfirmDialog
        open={pendingReloadYear !== null}
        title="Reload Leave Management?"
        description={
          pendingReloadYear !== null
            ? `Reload the loaded ${pendingReloadYear} workspace and reset current draft state to the last loaded baseline.`
            : ""
        }
        confirmLabel="Reload and reset"
        cancelLabel="Keep editing"
        onCancel={() => setPendingReloadYear(null)}
        onConfirm={() => {
          void handleConfirmReloadLoad();
        }}
      />

      <LeaveSpherePtoRequestDetailModal
        mode="create"
        open={isCreateModalOpen}
        request={null}
        initialForm={createForm as LeaveSpherePtoRequestFormState}
        title="Submit PTO Request"
        description="Enter request details. Your manager can approve or reject from the Manager PTO queue."
        ptoTypeOptions={createRequestPtoTypeOptions}
        saving={isMutating}
        calculateHours={(startDate, endDate) => resolvePtoHoursForEmployee(startDate, endDate, createForm.employeeId)}
        validateSubmit={validateCreateRequestHours}
        canSubmitOverride={canSubmitCreateRequestWithinBalance}
        saveLabel="Submit request"
        onOpenChange={setIsCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onFormChange={(nextForm) => setCreateForm((current) => ({ ...current, ...nextForm }))}
        onSubmit={handlePromptCreateRequest}
        allowedDateRange={loadedYearDateBounds ?? undefined}
        details={(
          <label className="block space-y-1 text-sm">
            <span className="text-slate-600">Employee</span>
            <AppDropdown
              value={createForm.employeeId}
              onValueChange={(value) => {
                createRequestEmployeeIdRef.current = value;
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
        <DialogContent className="max-w-md">
          <ModalHeaderRow
            actions={(
              <DialogClose asChild aria-label="Close submit choice dialog" disabled={isMutating}>
                <ModalCloseButton icon={<X className="size-4" />} />
              </DialogClose>
            )}
          >
            <DialogHeader>
              <DialogTitle>Submit PTO Request?</DialogTitle>
              <DialogDescription>
                Choose whether this admin-created request should be submitted as pending or submitted and approved immediately.
              </DialogDescription>
            </DialogHeader>
          </ModalHeaderRow>

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

      <Dialog
        open={isPendingRequestsModalOpen}
        onOpenChange={(open) => {
          setIsPendingRequestsModalOpen(open);
        }}
      >
        <DialogContent className="max-w-3xl flex max-h-[90vh] flex-col overflow-visible rounded-xl bg-white p-6">
          <ModalShell className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close pending requests modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>Pending requests</DialogTitle>
                <DialogDescription>
                  Select a pending PTO request to open its review modal.
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>

            <div className="mt-4 max-h-[60vh] space-y-4 overflow-y-auto pr-1 pt-1.5">
              <LeaveSpherePtoRequestList
                requests={pendingRequests}
                emptyMessage="No pending PTO requests."
                timeZone={tenantTimeZone}
                todayIsoDate={todayIsoDate}
                resolveEmployee={resolveRequestEmployee}
                getTypeLabel={(request) => requestTypeLabel(request.ptoTypeCode ?? request.type, ptoTypeMetaLookup)}
                getHoursLabel={(request) => formatHoursLabel(request.hours)}
                onRequestClick={(request) => {
                  setSelectedRequestId(request.id);
                  setReviewNote(request.approverNote || "");
                }}
              />
            </div>
          </ModalShell>
        </DialogContent>
      </Dialog>

      <LeaveSpherePtoRequestDetailModal
        open={Boolean(selectedRequest)}
        request={selectedRequest}
        layoutVariant="my-pto-detail"
        readOnly={!selectedRequestActionConfig?.canEditForm}
        title="Request Detail"
        description="Review and update employee PTO request details."
        ptoTypeOptions={selectedRequestPtoTypeOptions}
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
        allowedDateRange={loadedYearDateBounds ?? undefined}
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
                onClick={() => {
                  setReviewNote(selectedRequest.approverNote || "");
                  setPendingReviewAction("revert");
                }}
                disabled={isMutating}
              >
                Revert to pending
              </Button>
            ) : null}
            {selectedRequestActionConfig?.canCancel ? (
              <Button
                variant="outline"
                onClick={() => setPendingReviewAction("cancel")}
                disabled={isMutating}
              >
                Cancel request
              </Button>
            ) : null}
            {selectedRequestActionConfig?.canReject ? (
              <Button
                variant="outline"
                className="border-rose-200 text-rose-700 hover:bg-rose-50"
                onClick={() => setPendingReviewAction("reject")}
                disabled={isMutating}
              >
                Reject request
              </Button>
            ) : null}
            {selectedRequestActionConfig?.canApprove ? (
              <Button
                variant="outline"
                className="border-emerald-300 text-emerald-700 hover:bg-emerald-50"
                onClick={() => setPendingReviewAction("approve")}
                disabled={isMutating}
              >
                Approve request
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
        note={
          pendingReviewAction ? {
            label: pendingReviewActionLabel,
            value: reviewNote,
            onChange: setReviewNote,
            placeholder: "Add a note or reason for this decision",
            disabled: isMutating,
            required: pendingReviewActionRequiresNote,
            helpText: pendingReviewActionCopy?.noteHelpText,
          } : undefined
        }
      />

      <Dialog open={Boolean(selectedHoliday)} onOpenChange={(open) => {
        if (!open) {
          setSelectedHolidayId(null);
        }
      }}>
        <DialogContent className="max-w-xl">
          <ModalHeaderRow
            actions={(
              <DialogClose asChild aria-label="Close holiday detail modal">
                <ModalCloseButton icon={<X className="size-4" />} />
              </DialogClose>
            )}
          >
            <DialogHeader>
              <DialogTitle>Holiday Detail</DialogTitle>
              <DialogDescription>
                Team holiday information for the selected date.
              </DialogDescription>
            </DialogHeader>
          </ModalHeaderRow>

          {selectedHoliday ? (
            <div className="mt-4 rounded-xl border border-indigo-100 bg-indigo-50/40 p-3 text-sm text-slate-700">
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
        <DialogContent className="flex max-h-[90vh] max-w-md flex-col overflow-hidden rounded-xl bg-white p-6">
          <ModalShell className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close request selection modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>Choose Load Request</DialogTitle>
                <DialogDescription>
                  {adjustRequestPickerEmployeeName
                    ? `Select the approved load request for ${adjustRequestPickerEmployeeName}.`
                    : "Select the approved load request you want to adjust."}
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>

            <div className="mt-4 grid gap-2">
              {adjustRequestPickerRequests.map((request) => (
                <button
                  key={request.id}
                  type="button"
                  onClick={() => openAdjustRequestEditor(request)}
                  className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-left transition hover:border-blue-200 hover:bg-blue-50/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0 space-y-1">
                      <p className="text-sm font-semibold text-slate-900">{formatDateLabel(request.createdAt, tenantTimeZone)}</p>
                      {asString(request.description) ? (
                        <p className="text-xs leading-5 text-slate-500">{request.description}</p>
                      ) : null}
                    </div>
                    <div className="flex shrink-0 flex-col items-end gap-2">
                      <LeaveSpherePtoStatusChip
                        status={normalizeLeaveSpherePtoStatus(request.status)}
                        label={formatLeaveSpherePtoStatusLabel(normalizeLeaveSpherePtoStatus(request.status))}
                      />
                      <p className="text-sm font-semibold text-slate-900">{formatHoursLabel(request.hours)}</p>
                    </div>
                  </div>
                </button>
              ))}
              {adjustRequestPickerRequests.length === 0 ? (
                <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                  No approved load requests were found for this balance.
                </div>
              ) : null}
            </div>
          </ModalShell>
        </DialogContent>
      </Dialog>

      <LeaveSpherePtoLoadHoursModal
        mode={adjustModalRequest ? "edit" : "create"}
        open={isAdjustModalOpen}
        request={adjustModalRequest}
        initialForm={adjustForm}
        title={adjustModalRequest ? "Loaded PTO Hours Detail" : "Load PTO Hours"}
        description={adjustModalRequest
          ? "Update the selected approved PTO load request."
          : "Create a new approved PTO load request."}
        employeeOptions={employeeOptions}
        getPtoTypeOptions={getEmployeePtoTypeOptions}
        onOpenChange={handleAdjustModalOpenChange}
        onClose={closeAdjustModal}
        onFormChange={setAdjustForm}
        onSubmit={handleAdjustBalance}
        year={loadedYear}
        saving={isMutating}
        footerActions={adjustModalRequest ? (
          <Button
            variant="outline"
            className="border-rose-200 text-rose-700 hover:bg-rose-50"
            onClick={() => {
              setAdjustCancelNote(asString(adjustModalRequest.approverNote));
              setPendingAdjustAction("cancel");
            }}
            disabled={isMutating}
          >
            Cancel Transaction
          </Button>
        ) : null}
      />

      <LeaveSpherePtoDuplicateBalancesModal
        open={isDuplicateBalancesModalOpen}
        title="Duplicate Balances"
        description="Copy approved LOAD transactions from one year to another for selected employees."
        employeeOptions={activeEmployeeOptions}
        defaultEmployeeIds={duplicateBalancesDefaultEmployeeIds}
        defaultYearFrom={duplicateBalancesDefaultYearFrom}
        defaultYearTo={duplicateBalancesDefaultYearTo}
        onOpenChange={setIsDuplicateBalancesModalOpen}
        onClose={closeDuplicateBalancesModal}
        onSubmit={handleDuplicateBalances}
        saving={isMutating}
      />

      {pendingAdjustActionCopy ? (
        <ConfirmDialog
          open={Boolean(pendingAdjustAction)}
          title={pendingAdjustActionCopy.title}
          description={pendingAdjustActionCopy.description}
          confirmLabel={pendingAdjustActionCopy.confirmLabel}
          cancelLabel="Go back"
          onCancel={() => {
            setPendingAdjustAction(null);
            setAdjustCancelNote("");
          }}
          onConfirm={() => {
            void handleConfirmAdjustAction();
          }}
          note={pendingAdjustAction === "cancel" ? {
            label: "Admin note / reason",
            value: adjustCancelNote,
            onChange: setAdjustCancelNote,
            placeholder: "Add a reason for cancelling this load request",
            disabled: isMutating,
            helpText: pendingAdjustActionCopy.noteHelpText,
          } : undefined}
        />
      ) : null}

      <ConfirmDialog
        open={isGoogleCalendarDisconnectConfirmOpen}
        title="Disconnect Google Calendar?"
        description="LeaveSphere will remove the stored Google Calendar credentials for this tenant. Future approved requests will stop syncing until you connect again."
        confirmLabel={isGoogleCalendarDisconnecting ? "Disconnecting..." : "Disconnect"}
        cancelLabel="Keep connected"
        onCancel={() => setIsGoogleCalendarDisconnectConfirmOpen(false)}
        onConfirm={() => {
          void handleDisconnectGoogleCalendar();
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
