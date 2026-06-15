import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Plus,
  X,
} from "lucide-react";

import { PageBanner } from "@shell/components/layout/PageBanner";
import { Button } from "@tradsphere/components/ui/button";
import { ActionIconButton } from "@tradsphere/components/dashboard/ActionIconButton";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@tradsphere/components/ui/dialog";
import { AppDropdown } from "@tradsphere/components/ui/app-dropdown";
import { Input } from "@tradsphere/components/ui/input";
import { ConfirmDialog } from "@tradsphere/components/ui/confirm-dialog";
import { useToast } from "@shell/components/ui/toast";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { useOnlineStatus } from "@shared/hooks/useOnlineStatus";
import { useObservedElementHeight } from "@shared/hooks/useObservedElementHeight";
import { shouldFetchNetwork, type CachePolicy } from "@shared/cache";
import { useAuth } from "@shared/auth/useAuth";
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
import { DEFAULT_TIME_ZONE, formatDateInTimeZone, getCurrentMonthKeyInTimeZone, getCurrentYearInTimeZone, getTodayIsoDateInTimeZone } from "@shared/utils/time";
import { TooltipTarget } from "@shared/components/actions/TooltipTarget";
import { LeaveSpherePtoRequestCard } from "@leavesphere/components/LeaveSpherePtoRequestCard";
import { LeaveSphereMonthCalendar, type LeaveSphereMonthCalendarEvent } from "@leavesphere/components/MonthCalendar";
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
import { LeaveSpherePtoTypeChip } from "@leavesphere/components/PtoTypeChip";
import {
  LeaveSpherePtoRequestDetailModal,
  type LeaveSpherePtoRequestFormState,
} from "@leavesphere/components/PtoRequestDetailModal";
import {
  cancelLeaveSpherePtoRequest,
  loadLeaveSpherePtoWorkspace,
  reviewLeaveSpherePtoRequest,
  submitLeaveSpherePtoRequest,
  updateLeaveSpherePtoRequest,
  type LeaveSpherePtoRequest,
  type LeaveSpherePtoStatus,
  type LeaveSpherePtoType,
  type LeaveSpherePtoWorkspaceData,
} from "@leavesphere/lib/ptoMocks";
import { calculateLeaveSpherePtoHours } from "@leavesphere/lib/ptoHours";
import {
  buildLeaveSpherePtoTypeOptionsFromBalances,
  resolveLeaveSpherePtoAvailableHours,
  validateLeaveSpherePtoRequestedHours,
} from "@leavesphere/lib/ptoAvailability";
import { buildLeaveSphereHolidayDateSet } from "@leavesphere/lib/ptoHolidayScopes";
import {
  buildLeaveSpherePtoCalendarRequestChipLabel,
  buildLeaveSpherePtoCalendarRequestTooltipLabel,
} from "@leavesphere/lib/ptoCalendar";
import {
  formatMonthDayYearLabel,
  formatPtoRequestDateRangeLabel,
  getLeaveSpherePtoRequestCardTone,
} from "@leavesphere/lib/ptoDate";
import { getPtoRequestActionConfig } from "@leavesphere/lib/ptoRequestActionConfig";
import {
  getLeaveSphereReviewActionConfirmCopy,
  type LeaveSphereReviewAction,
} from "@leavesphere/lib/reviewActionConfirm";
import {
  readLeaveSpherePtoWorkspaceCacheSnapshot,
  writeLeaveSpherePtoWorkspaceCache,
} from "@leavesphere/lib/ptoWorkspaceCache";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type RequestFormState = {
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: string;
  description: string;
};

type PersistedLeaveSphereMyPtoPageState = {
  selectedYear: string;
  loadedYear: number | null;
  calendarMonth: string;
  requestForm: RequestFormState;
  isRequestDialogOpen: boolean;
  selectedMyRequestId: string | null;
  selectedHolidayId: string | null;
  reviewTargetId: string | null;
  reviewNote: string;
  managerSearchDraft?: string;
  managerSearchApplied?: string;
  scrollY: number;
};

const DEFAULT_PTO_TYPE_LABELS: Record<LeaveSpherePtoType, string> = {
  vacation: "Vacation",
  sick: "Sick",
  personal: "Personal",
};

const EMPTY_FORM: RequestFormState = {
  type: "vacation",
  startDate: "",
  endDate: "",
  hours: "",
  description: "",
};

const LEAVESPHERE_APP_CODE = "leavesphere";
const LEAVESPHERE_MY_PTO_PAGE_CODE = "my-pto";

const PTO_BALANCE_CARD_TONES = [
  {
    card: "border-sky-200 bg-gradient-to-br from-white to-sky-50/80",
    label: "text-sky-700/80",
    value: "text-sky-900",
  },
  {
    card: "border-rose-200 bg-gradient-to-br from-white to-rose-50/75",
    label: "text-rose-700/80",
    value: "text-rose-900",
  },
  {
    card: "border-violet-200 bg-gradient-to-br from-white to-violet-50/75",
    label: "text-violet-700/80",
    value: "text-violet-900",
  },
  {
    card: "border-emerald-200 bg-gradient-to-br from-white to-emerald-50/75",
    label: "text-emerald-700/80",
    value: "text-emerald-900",
  },
] as const;

function pickPtoBalanceTone(key: string): (typeof PTO_BALANCE_CARD_TONES)[number] {
  const normalized = asString(key).toLowerCase();
  if (!normalized) {
    return PTO_BALANCE_CARD_TONES[0];
  }
  if (normalized.includes("vac")) {
    return PTO_BALANCE_CARD_TONES[0];
  }
  if (normalized.includes("sick")) {
    return PTO_BALANCE_CARD_TONES[1];
  }
  if (normalized.includes("person")) {
    return PTO_BALANCE_CARD_TONES[2];
  }
  if (normalized.includes("float")) {
    return PTO_BALANCE_CARD_TONES[3];
  }
  let hash = 0;
  for (let index = 0; index < normalized.length; index += 1) {
    hash = ((hash * 31) + normalized.charCodeAt(index)) >>> 0;
  }
  return PTO_BALANCE_CARD_TONES[hash % PTO_BALANCE_CARD_TONES.length];
}

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeSearchKeyword(value: string): string {
  return asString(value).toLowerCase();
}

function formatRelativeTime(timestamp: number): string {
  const deltaMs = Math.max(0, Date.now() - timestamp);
  if (deltaMs < 60_000) {
    return "just now";
  }
  const minutes = Math.round(deltaMs / 60_000);
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
  if (Number.isInteger(hours)) {
    return `${hours}h`;
  }
  return `${hours.toFixed(1)}h`;
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

function requestTypeLabel(type: string): string {
  const normalized = asString(type).toLowerCase();
  if (!normalized) {
    return "PTO";
  }
  if (normalized in DEFAULT_PTO_TYPE_LABELS) {
    return DEFAULT_PTO_TYPE_LABELS[normalized as LeaveSpherePtoType] || type;
  }
  return asString(type).toUpperCase() || "PTO";
}

function buildYearDateBounds(year: number): { minDate: string; maxDate: string } {
  return {
    minDate: `${year}-01-01`,
    maxDate: `${year}-12-31`,
  };
}

function isDateWithinBounds(value: string, bounds: { minDate: string; maxDate: string }): boolean {
  return value >= bounds.minDate && value <= bounds.maxDate;
}

function isRangeWithinBounds(
  startDate: string,
  endDate: string,
  bounds: { minDate: string; maxDate: string },
): boolean {
  return isDateWithinBounds(startDate, bounds) && isDateWithinBounds(endDate, bounds);
}

function parseIsoDate(value: string): Date | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(asString(value));
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return null;
  }
  const date = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
  if (
    date.getUTCFullYear() !== year
    || date.getUTCMonth() + 1 !== month
    || date.getUTCDate() !== day
  ) {
    return null;
  }
  return date;
}

function buildDateSearchTokens(isoDate: string): string[] {
  const parsed = parseIsoDate(isoDate);
  if (!parsed) {
    return [asString(isoDate)];
  }
  const year = parsed.getUTCFullYear();
  const month = parsed.getUTCMonth() + 1;
  const day = parsed.getUTCDate();
  const monthPadded = String(month).padStart(2, "0");
  const dayPadded = String(day).padStart(2, "0");
  return [
    isoDate,
    `${monthPadded}/${dayPadded}/${year}`,
    `${month}/${day}/${year}`,
    `${monthPadded}-${dayPadded}-${year}`,
    `${month}-${day}-${year}`,
    formatDateLabel(isoDate, DEFAULT_TIME_ZONE),
  ];
}

function managerRequestMatchesSearch(request: LeaveSpherePtoRequest, keyword: string, employeeName: string): boolean {
  if (!keyword) {
    return true;
  }
  const tokens = [
    employeeName,
    requestTypeLabel(request.type),
    statusLabel(request.status),
    request.description,
    request.approverNote ?? "",
    ...buildDateSearchTokens(request.startDate),
    ...buildDateSearchTokens(request.endDate),
    ...buildDateSearchTokens(request.submittedAt),
    ...(request.reviewedAt ? buildDateSearchTokens(request.reviewedAt) : []),
  ];
  return tokens.some((item) => asString(item).toLowerCase().includes(keyword));
}

function requestIsWithinYear(request: LeaveSpherePtoRequest, year: number): boolean {
  const yearStart = `${year}-01-01`;
  const yearEnd = `${year}-12-31`;
  return request.startDate >= yearStart && request.endDate <= yearEnd;
}

function filterWorkspaceByYear(workspace: LeaveSpherePtoWorkspaceData, year: number): LeaveSpherePtoWorkspaceData {
  const yearPrefix = `${year}-`;
  return {
    ...workspace,
    requests: workspace.requests.filter((item) => requestIsWithinYear(item, year)),
    holidays: workspace.holidays.filter((item) => item.date.startsWith(yearPrefix)),
  };
}

function isPtoTypeValue(value: unknown): value is LeaveSpherePtoType {
  return value === "vacation" || value === "sick" || value === "personal";
}

function isRequestFormState(value: unknown): value is RequestFormState {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    isPtoTypeValue(record.type)
    && typeof record.startDate === "string"
    && typeof record.endDate === "string"
    && typeof record.hours === "string"
    && typeof record.description === "string"
  );
}

function isPersistedLeaveSphereMyPtoPageState(value: unknown): value is PersistedLeaveSphereMyPtoPageState {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  const loadedYear = record.loadedYear;
  const scrollY = record.scrollY;
  return (
    typeof record.selectedYear === "string"
    && (loadedYear === null || (typeof loadedYear === "number" && Number.isInteger(loadedYear)))
    && typeof record.calendarMonth === "string"
    && isRequestFormState(record.requestForm)
    && typeof record.isRequestDialogOpen === "boolean"
    && (record.selectedMyRequestId === null || typeof record.selectedMyRequestId === "string")
    && (record.selectedHolidayId === null || typeof record.selectedHolidayId === "string")
    && (record.reviewTargetId === null || typeof record.reviewTargetId === "string")
    && typeof record.reviewNote === "string"
    && (record.managerSearchDraft === undefined || typeof record.managerSearchDraft === "string")
    && (record.managerSearchApplied === undefined || typeof record.managerSearchApplied === "string")
    && typeof scrollY === "number"
    && Number.isFinite(scrollY)
  );
}

export default function LeaveSphereMyPtoPage() {
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const toast = useToast();
  const { isOnline } = useOnlineStatus();
  const tenantTimeZone = asString(auth.accessProfile?.tenant?.timezone) || DEFAULT_TIME_ZONE;
  const currentYear = useMemo(() => getCurrentYearInTimeZone(tenantTimeZone), [tenantTimeZone]);
  const currentMonthKey = useMemo(() => getCurrentMonthKeyInTimeZone(tenantTimeZone), [tenantTimeZone]);
  const todayIsoDate = useMemo(() => getTodayIsoDateInTimeZone(tenantTimeZone), [tenantTimeZone]);
  const [calendarSectionRef, calendarSectionHeight] = useObservedElementHeight<HTMLElement>();
  const initialTimeZoneRef = useRef(tenantTimeZone);
  const initialYearRef = useRef(currentYear);
  const initialMonthKeyRef = useRef(currentMonthKey);

  const currentUserId = asString(auth.user?.id) || "local-user";
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
      pageCode: LEAVESPHERE_MY_PTO_PAGE_CODE,
    };
  }, [canRestorePageState, currentUserId, tenantSlug]);
  const pageStateStorageKey = useMemo(() => {
    if (!pageStateScope) {
      return null;
    }
    return buildScopedPageStateStorageKey(pageStateScope);
  }, [pageStateScope]);
  const hydratedPageStateScopeRef = useRef<string | null>(null);
  const restoredScrollScopeRef = useRef<string | null>(null);
  const [hasHydratedPageState, setHasHydratedPageState] = useState(false);
  const [scrollY, setScrollY] = useState(0);

  const [workspace, setWorkspace] = useState<LeaveSpherePtoWorkspaceData | null>(null);
  const [selectedYear, setSelectedYear] = useState(() => String(currentYear));
  const [loadedYear, setLoadedYear] = useState<number | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [pageErrorMessage, setPageErrorMessage] = useState<string | null>(null);

  const [isInitializing, setIsInitializing] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isReviewing, setIsReviewing] = useState(false);
  const [isSavingRequestDetail, setIsSavingRequestDetail] = useState(false);

  const [calendarMonth, setCalendarMonth] = useState(() => currentMonthKey);
  const [isRequestDialogOpen, setIsRequestDialogOpen] = useState(false);
  const [requestForm, setRequestForm] = useState<RequestFormState>(EMPTY_FORM);
  const [selectedMyRequestId, setSelectedMyRequestId] = useState<string | null>(null);
  const [selectedHolidayId, setSelectedHolidayId] = useState<string | null>(null);
  const [isCancelMyRequestDialogOpen, setIsCancelMyRequestDialogOpen] = useState(false);

  const [reviewTargetId, setReviewTargetId] = useState<string | null>(null);
  const [reviewNote, setReviewNote] = useState("");
  const [pendingReviewAction, setPendingReviewAction] = useState<LeaveSphereReviewAction | null>(null);
  const [draftManagerSearch, setDraftManagerSearch] = useState("");
  const [appliedManagerSearch, setAppliedManagerSearch] = useState("");
  const workspaceLoadRequestTokenRef = useRef(0);
  const restoredWorkspaceScopeRef = useRef<string | null>(null);

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
    const persisted = readScopedPageState<PersistedLeaveSphereMyPtoPageState>(
      pageStateScope,
      isPersistedLeaveSphereMyPtoPageState,
    );
    if (persisted) {
      setSelectedYear(persisted.selectedYear);
      setCalendarMonth(persisted.calendarMonth);
      setRequestForm(persisted.requestForm);
      setIsRequestDialogOpen(persisted.isRequestDialogOpen);
      setSelectedMyRequestId(persisted.selectedMyRequestId);
      setSelectedHolidayId(persisted.selectedHolidayId);
      setReviewTargetId(persisted.reviewTargetId);
      setReviewNote(persisted.reviewNote);
      setDraftManagerSearch(asString(persisted.managerSearchDraft));
      setAppliedManagerSearch(asString(persisted.managerSearchApplied));
      setScrollY(Math.max(0, persisted.scrollY));
      const restoredYear = typeof persisted.loadedYear === "number" ? persisted.loadedYear : null;
      if (restoredYear !== null && restoredWorkspaceScopeRef.current !== pageStateStorageKey) {
        restoredWorkspaceScopeRef.current = pageStateStorageKey;
        const cacheSnapshot = readLeaveSpherePtoWorkspaceCacheSnapshot<LeaveSpherePtoWorkspaceData>({
          pageCode: LEAVESPHERE_MY_PTO_PAGE_CODE,
          tenantSlug,
          userId: currentUserId,
          year: restoredYear,
        });
        if (cacheSnapshot?.data) {
          setWorkspace(cacheSnapshot.data);
          setLoadedYear(restoredYear);
          setCacheStatus({
            source: cacheSnapshot.source,
            fetchedAt: cacheSnapshot.fetchedAt ?? Date.now(),
          });
        } else {
          setLoadedYear(null);
          setWorkspace(null);
          setCacheStatus(null);
        }
      } else {
        setLoadedYear(null);
        setWorkspace(null);
        setCacheStatus(null);
      }
    }
    setHasHydratedPageState(true);
  }, [canRestorePageState, currentUserId, pageStateScope, pageStateStorageKey, tenantSlug]);

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
    writeScopedPageState<PersistedLeaveSphereMyPtoPageState>(pageStateScope, {
      selectedYear,
      loadedYear,
      calendarMonth,
      requestForm,
      isRequestDialogOpen,
      selectedMyRequestId,
      selectedHolidayId,
      reviewTargetId,
      reviewNote,
      managerSearchDraft: draftManagerSearch,
      managerSearchApplied: appliedManagerSearch,
      scrollY,
    });
  }, [
    appliedManagerSearch,
    calendarMonth,
    canRestorePageState,
    draftManagerSearch,
    hasHydratedPageState,
    isRequestDialogOpen,
    loadedYear,
    pageStateScope,
    pageStateStorageKey,
    requestForm,
    reviewNote,
    reviewTargetId,
    scrollY,
    selectedHolidayId,
    selectedMyRequestId,
    selectedYear,
  ]);

  const yearOptions = useMemo(
    () => [
      { value: String(currentYear - 1), label: String(currentYear - 1) },
      { value: String(currentYear), label: String(currentYear) },
      { value: String(currentYear + 1), label: String(currentYear + 1) },
    ],
    [currentYear],
  );

  const workspaceForYear = useMemo(() => {
    if (!workspace || loadedYear === null) {
      return null;
    }
    return filterWorkspaceByYear(workspace, loadedYear);
  }, [loadedYear, workspace]);

  const isManager = Boolean(workspaceForYear?.isManager || workspace?.isManager);
  const selectedYearNumber = useMemo(() => Number(selectedYear), [selectedYear]);
  const loadedYearDateBounds = useMemo(
    () => (loadedYear === null ? null : buildYearDateBounds(loadedYear)),
    [loadedYear],
  );
  const loadedYearForRequests = loadedYear ?? (Number.isInteger(selectedYearNumber) ? selectedYearNumber : currentYear);
  const cachedEmployeeItems = useMemo(
    () => readLeaveSpherePtoEmployeeCacheSnapshot({ tenantSlug })?.data ?? [],
    [tenantSlug],
  );
  const mergedEmployeeItems = useMemo(
    () => mergeLeaveSpherePtoEmployeeCacheItems(workspaceForYear?.employees, cachedEmployeeItems),
    [cachedEmployeeItems, workspaceForYear?.employees],
  );
  const employeeLookupById = useMemo(
    () => buildLeaveSpherePtoEmployeeLookup(mergedEmployeeItems),
    [mergedEmployeeItems],
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
      mergedEmployeeItems,
      { fetchedAt: Date.now() },
    );
  }, [mergedEmployeeItems, tenantSlug, workspaceForYear]);

  const myRequests = useMemo(() => {
    if (!workspaceForYear) {
      return [];
    }
    return workspaceForYear.requests
      .filter((item) => item.employeeId === workspaceForYear.currentUserId)
      .sort((left, right) => right.submittedAt.localeCompare(left.submittedAt));
  }, [workspaceForYear]);

  const directReportRequests = useMemo(() => {
    if (!workspaceForYear || !isManager) {
      return [];
    }
    return workspaceForYear.requests
      .filter((item) => item.employeeId !== workspaceForYear.currentUserId && item.managerId === workspaceForYear.currentUserId)
      .sort((left, right) => right.submittedAt.localeCompare(left.submittedAt));
  }, [workspaceForYear]);

  const normalizedAppliedManagerSearch = useMemo(
    () => normalizeSearchKeyword(appliedManagerSearch),
    [appliedManagerSearch],
  );
  const filteredDirectReportRequests = useMemo(
    () => directReportRequests.filter((item) => managerRequestMatchesSearch(item, normalizedAppliedManagerSearch, resolveRequestEmployee(item).employeeName)),
    [directReportRequests, normalizedAppliedManagerSearch, resolveRequestEmployee],
  );

  const pendingDirectReportRequests = useMemo(
    () => directReportRequests.filter((item) => item.status === "pending"),
    [directReportRequests],
  );

  const requestStatusSummary = useMemo(() => {
    const summary = {
      pending: 0,
      approved: 0,
      rejected: 0,
    };
    for (const item of myRequests) {
      if (item.status === "approved") {
        summary.approved += 1;
      } else if (item.status === "rejected") {
        summary.rejected += 1;
      } else if (item.status === "pending") {
        summary.pending += 1;
      }
    }
    return summary;
  }, [myRequests]);

  const selectedReviewRequest = useMemo(
    () => directReportRequests.find((item) => item.id === reviewTargetId) || null,
    [directReportRequests, reviewTargetId],
  );
  const selectedMyRequest = useMemo(
    () => myRequests.find((item) => item.id === selectedMyRequestId) || null,
    [myRequests, selectedMyRequestId],
  );
  const selectedHoliday = useMemo(
    () => (workspaceForYear?.holidays ?? []).find((item) => item.id === selectedHolidayId) || null,
    [selectedHolidayId, workspaceForYear?.holidays],
  );

  useEffect(() => {
    if (!selectedReviewRequest) {
      return;
    }
    setReviewNote(selectedReviewRequest.approverNote || "");
  }, [selectedReviewRequest?.id]);
  useEffect(() => {
    if (!selectedMyRequest) {
      setIsCancelMyRequestDialogOpen(false);
    }
  }, [selectedMyRequest?.id]);
  const selectedMyRequestActionConfig = useMemo(
    () => (selectedMyRequest
      ? getPtoRequestActionConfig({
        role: "user",
        status: selectedMyRequest.status,
        startDate: selectedMyRequest.startDate,
        todayIsoDate,
      })
      : null),
    [selectedMyRequest, todayIsoDate],
  );
  const selectedReviewRequestActionConfig = useMemo(
    () => (selectedReviewRequest
      ? getPtoRequestActionConfig({
        role: "approver",
        status: selectedReviewRequest.status,
        startDate: selectedReviewRequest.startDate,
        todayIsoDate,
      })
      : null),
    [selectedReviewRequest, todayIsoDate],
  );
  const isReviewNoteDirty = useMemo(
    () => selectedReviewRequest
      ? normalizeOptionalNote(reviewNote) !== normalizeOptionalNote(selectedReviewRequest.approverNote)
      : false,
    [reviewNote, selectedReviewRequest],
  );
  const canSaveReviewNote = Boolean(selectedReviewRequestActionConfig?.canSubmit);
  const pendingReviewActionCopy = useMemo(
    () => (pendingReviewAction ? getLeaveSphereReviewActionConfirmCopy(pendingReviewAction, "manager") : null),
    [pendingReviewAction],
  );
  const canRequestPto = Boolean(
    Number.isInteger(selectedYearNumber)
    && selectedYearNumber >= currentYear
    && loadedYear === selectedYearNumber,
  );
  const calendarEvents = useMemo(() => {
    const events: LeaveSphereMonthCalendarEvent[] = [];
    for (const holiday of workspaceForYear?.holidays ?? []) {
      events.push({
        id: `holiday:${holiday.id}`,
        label: `${holiday.name} (${holiday.teamRegion})`,
        tone: mapLeaveSphereHolidayRegionToChipTone(holiday.teamRegion),
        startDate: holiday.date,
        endDate: holiday.date,
        title: `${holiday.name} · ${holiday.teamRegion}`,
      });
    }
    for (const request of myRequests) {
      const employee = resolveRequestEmployee(request);
      const requestType = requestTypeLabel(request.type);
      const hoursLabel = formatHoursLabel(request.hours);
      const chipLabel = buildLeaveSpherePtoCalendarRequestChipLabel(
        employee.employeeName,
        requestType,
        hoursLabel,
        request.description,
      );
      events.push({
        id: `request:${request.id}`,
        label: chipLabel,
        tone: mapLeaveSpherePtoStatusToChipTone(request.status),
        startDate: request.startDate,
        endDate: request.endDate,
        title: chipLabel,
      });
    }
    if (isManager) {
      for (const request of directReportRequests) {
        const employee = resolveRequestEmployee(request);
        const requestType = requestTypeLabel(request.type);
        const hoursLabel = formatHoursLabel(request.hours);
        const chipLabel = buildLeaveSpherePtoCalendarRequestChipLabel(
          employee.employeeName,
          requestType,
          hoursLabel,
          request.description,
        );
        events.push({
          id: `manager-request:${request.id}`,
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
    }
    return events;
  }, [directReportRequests, isManager, myRequests, workspaceForYear?.holidays]);
  const holidayDates = useMemo(
    () => buildLeaveSphereHolidayDateSet(workspaceForYear?.holidays, workspaceForYear?.currentUserTeamRegion),
    [workspaceForYear?.currentUserTeamRegion, workspaceForYear?.holidays],
  );
  const balanceRows = workspaceForYear?.balances ?? [];
  const ptoTypeOptionsWithAvailability = useMemo(
    () => buildLeaveSpherePtoTypeOptionsFromBalances(balanceRows),
    [balanceRows],
  );
  const requestAvailableHours = useMemo(
    () => resolveLeaveSpherePtoAvailableHours(balanceRows, requestForm.type),
    [balanceRows, requestForm.type],
  );
  const canSubmitRequestWithinBalance = useMemo(() => {
    const requestedHours = Number(requestForm.hours);
    if (!Number.isFinite(requestedHours) || requestedHours < 0) {
      return false;
    }
    if (requestAvailableHours == null) {
      return false;
    }
    return requestedHours <= requestAvailableHours;
  }, [requestAvailableHours, requestForm.hours]);
  const shouldCenterBalanceBlock = balanceRows.length > 0 && balanceRows.length <= 3;

  const commitWorkspace = useCallback((
    nextWorkspace: LeaveSpherePtoWorkspaceData,
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
        pageCode: "my-pto",
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

  const cacheStatusText = useMemo(() => {
    if (isRefreshing) {
      return "Refreshing LeaveSphere PTO workspace...";
    }
    if (loadedYear === null) {
      return "Select a year and click Load.";
    }
    if (!cacheStatus) {
      return "Loading LeaveSphere PTO workspace...";
    }
    if (!isOnline) {
      return `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
    }
    return `Year ${loadedYear}. Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [cacheStatus, isOnline, isRefreshing, loadedYear]);

  const applyManagerSearchKeyword = useCallback((rawValue: string) => {
    const normalized = asString(rawValue);
    setDraftManagerSearch(normalized);
    setAppliedManagerSearch(normalized);
  }, []);

  const applyManagerSearchFromDraft = useCallback(() => {
    const normalizedDraft = asString(draftManagerSearch);
    if (normalizedDraft !== draftManagerSearch) {
      setDraftManagerSearch(normalizedDraft);
    }
    if (normalizeSearchKeyword(normalizedDraft) === normalizeSearchKeyword(appliedManagerSearch)) {
      return;
    }
    applyManagerSearchKeyword(normalizedDraft);
  }, [appliedManagerSearch, applyManagerSearchKeyword, draftManagerSearch]);

  const handleManagerSearchInputChange = useCallback((nextValue: string) => {
    setDraftManagerSearch(nextValue);
    if (!asString(nextValue)) {
      applyManagerSearchKeyword("");
    }
  }, [applyManagerSearchKeyword]);

  const handleManagerSearchInputKeyDown = useCallback((event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") {
      event.preventDefault();
      applyManagerSearchFromDraft();
      return;
    }
    if (event.key === "Tab") {
      applyManagerSearchFromDraft();
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      if (draftManagerSearch) {
        applyManagerSearchKeyword("");
      }
      event.currentTarget.blur();
    }
  }, [applyManagerSearchFromDraft, applyManagerSearchKeyword, draftManagerSearch]);

  const handleClearManagerSearch = useCallback(() => {
    applyManagerSearchKeyword("");
  }, [applyManagerSearchKeyword]);

  useEffect(() => {
    const normalizedDraft = asString(draftManagerSearch);
    if (!normalizedDraft) {
      return;
    }
    if (normalizeSearchKeyword(normalizedDraft) === normalizeSearchKeyword(appliedManagerSearch)) {
      return;
    }
    const timer = window.setTimeout(() => {
      applyManagerSearchKeyword(normalizedDraft);
    }, 1000);
    return () => {
      window.clearTimeout(timer);
    };
  }, [appliedManagerSearch, applyManagerSearchKeyword, draftManagerSearch]);

  const pageMessages: StackMessage[] = [];
  if (refreshMessage) {
    pageMessages.push({
      id: "leavesphere-pto-refresh",
      variant: "info",
      message: refreshMessage,
    });
  }
  if (pageErrorMessage) {
    pageMessages.push({
      id: "leavesphere-pto-error",
      variant: "warning",
      message: pageErrorMessage,
    });
  }

  const loadingContract = resolveSharedLoadingContract(
    {
      pageInitializing: isInitializing,
      pageRefreshing: isRefreshing,
      cacheChipRefreshing: isChipRefreshOverlayVisible,
      sectionLoading: isSubmitting || isReviewing,
    },
    {
      pageInitializing: "Preparing LeaveSphere PTO workspace...",
      pageRefreshing: "Refreshing PTO workspace...",
      cacheChipRefreshing: "Refreshing PTO workspace...",
      sectionLoading: isSubmitting ? "Submitting PTO request..." : "Applying manager decision...",
    },
  );

  const loadWorkspace = useCallback(async (year: number, policy: CachePolicy = "stale-while-revalidate"): Promise<boolean> => {
    const requestToken = ++workspaceLoadRequestTokenRef.current;
    const cacheSnapshot = readLeaveSpherePtoWorkspaceCacheSnapshot<LeaveSpherePtoWorkspaceData>({
      pageCode: "my-pto",
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
        setRefreshMessage("You're offline. Showing cached PTO workspace.");
        return true;
      }
      setPageErrorMessage("You're offline. Connect to the internet to load PTO workspace.");
      return false;
    }

    setIsInitializing(!shouldShowRefreshing);
    setIsRefreshing(shouldShowRefreshing);
    setPageErrorMessage(null);
    if (shouldShowRefreshing && hasCachedWorkspace) {
      setRefreshMessage("Cached PTO workspace shown while refreshing.");
    } else {
      setRefreshMessage(null);
    }

    try {
      const result = await loadLeaveSpherePtoWorkspace({
        requestJson,
        year,
        timeZone: tenantTimeZone,
      });
      if (requestToken !== workspaceLoadRequestTokenRef.current) {
        return false;
      }
      commitWorkspace(result.workspace, "network", year, Date.now());
      setRefreshMessage(result.refreshMessage);
      return true;
    } catch {
      if (requestToken !== workspaceLoadRequestTokenRef.current) {
        return false;
      }
      if (hasCachedWorkspace && cachedWorkspace) {
        commitWorkspace(cachedWorkspace, "cache", year, cacheSnapshot?.fetchedAt ?? Date.now());
        setRefreshMessage("Showing cached PTO workspace. Could not refresh.");
        return true;
      }
      setPageErrorMessage("Unable to load PTO workspace right now. Please try again.");
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
    isOnline,
    loadedYear,
    requestJson,
    tenantSlug,
    tenantTimeZone,
    workspaceForYear,
  ]);

  const handleLoadByYear = useCallback(async () => {
    const parsedYear = Number(selectedYear);
    if (!Number.isInteger(parsedYear)) {
      return;
    }

    const didLoad = await loadWorkspace(parsedYear, loadedYear === parsedYear ? "network-only" : "cache-first");
    if (!didLoad) {
      return;
    }

    const currentMonthMatch = /^(\d{4})-(\d{2})$/.exec(calendarMonth);
    const defaultMonth = currentMonthMatch ? Number(currentMonthMatch[2]) : (parsedYear === currentYear ? Number(currentMonthKey.slice(5, 7)) : 1);
    setCalendarMonth(`${parsedYear}-${String(defaultMonth).padStart(2, "0")}`);
  }, [calendarMonth, currentMonthKey, currentYear, loadWorkspace, loadedYear, selectedYear]);

  const openSubmitDialog = useCallback(() => {
    if (!canRequestPto || !loadedYearDateBounds) {
      return;
    }
    const defaultIsoDate = isDateWithinBounds(todayIsoDate, loadedYearDateBounds)
      ? todayIsoDate
      : loadedYearDateBounds.minDate;
    const defaultType = ptoTypeOptionsWithAvailability[0]?.value || "";
    setRequestForm({
      type: defaultType as LeaveSpherePtoType,
      startDate: defaultIsoDate,
      endDate: defaultIsoDate,
      hours: calculateLeaveSpherePtoHours(defaultIsoDate, defaultIsoDate, holidayDates),
      description: "",
    });
    setIsRequestDialogOpen(true);
  }, [canRequestPto, holidayDates, loadedYearDateBounds, ptoTypeOptionsWithAvailability, todayIsoDate]);

  const openSubmitDialogForDate = useCallback((isoDate: string) => {
    if (!canRequestPto || !loadedYearDateBounds || !isDateWithinBounds(isoDate, loadedYearDateBounds)) {
      return;
    }
    const defaultType = ptoTypeOptionsWithAvailability[0]?.value || "";
    setRequestForm({
      type: defaultType as LeaveSpherePtoType,
      startDate: isoDate,
      endDate: isoDate,
      hours: calculateLeaveSpherePtoHours(isoDate, isoDate, holidayDates),
      description: "",
    });
    setIsRequestDialogOpen(true);
  }, [canRequestPto, holidayDates, loadedYearDateBounds, ptoTypeOptionsWithAvailability]);

  const handleSubmitRequest = useCallback(async (params: {
      payload: {
        type: LeaveSpherePtoType;
        startDate: string;
        endDate: string;
        hours: number;
        description: string;
      };
  }) => {
    if (!canRequestPto) {
      toast.error("Submit unavailable", "PTO requests are allowed only for a loaded current or future selected year.");
      return;
    }
    if (!loadedYearDateBounds || !isRangeWithinBounds(params.payload.startDate, params.payload.endDate, loadedYearDateBounds)) {
      toast.error("Invalid PTO dates", "Start and end dates must be within the loaded selected year.");
      return;
    }
    setIsSubmitting(true);

    try {
      const result = await submitLeaveSpherePtoRequest({
        requestJson,
        payload: {
          ...params.payload,
          year: loadedYearForRequests,
        },
      });
      commitWorkspace(result.workspace, result.source, loadedYearForRequests);
      setIsRequestDialogOpen(false);
      toast.success("PTO request submitted", "Your request is now pending manager review.");
    } catch {
      toast.error("Submit failed", "Unable to submit this PTO request right now.");
    } finally {
      setIsSubmitting(false);
    }
  }, [
    commitWorkspace,
    requestJson,
    toast,
    canRequestPto,
    loadedYearDateBounds,
    loadedYearForRequests,
  ]);
  const validateRequestHours = useCallback((form: LeaveSpherePtoRequestFormState) => {
    const availableHours = resolveLeaveSpherePtoAvailableHours(balanceRows, form.type);
    return validateLeaveSpherePtoRequestedHours({
      requestedHours: form.hours,
      availableHours,
    });
  }, [balanceRows]);

  const handleReviewRequest = useCallback(async (action: LeaveSphereReviewAction) => {
    if (!selectedReviewRequest) {
      return;
    }
    setIsReviewing(true);
    try {
      const result = await reviewLeaveSpherePtoRequest({
        requestJson,
        payload: {
          requestId: selectedReviewRequest.id,
          action,
          approverNote: reviewNote,
        },
      });
      commitWorkspace(result.workspace, result.source, loadedYearForRequests);
      setReviewTargetId(null);
      setReviewNote("");
      if (action === "approve") {
        toast.success("Request approved", "Employee will see the approved status.");
      } else if (action === "reject") {
        toast.success("Request rejected", "Employee will see the rejected status.");
      } else if (action === "cancel") {
        toast.success("Request cancelled", "Employee request status was updated to cancelled.");
      } else {
        toast.success("Decision reverted", "Request status was changed back to pending.");
      }
    } catch {
      toast.error("Decision failed", "Unable to save this manager decision right now.");
    } finally {
      setIsReviewing(false);
    }
  }, [
    commitWorkspace,
    loadedYearForRequests,
    requestJson,
    reviewNote,
    selectedReviewRequest,
    toast,
  ]);

  const handleCancelMyRequest = useCallback(async () => {
    if (!selectedMyRequest) {
      return;
    }
    setIsSavingRequestDetail(true);
    try {
      const result = await cancelLeaveSpherePtoRequest({
        requestJson,
        transactionId: selectedMyRequest.id,
      });
      commitWorkspace(result.workspace, result.source, loadedYearForRequests);
      setSelectedMyRequestId(null);
      toast.success("Request cancelled", "PTO request status was updated to cancelled.");
    } catch {
      toast.error("Cancel failed", "Unable to cancel this PTO request right now.");
    } finally {
      setIsSavingRequestDetail(false);
    }
  }, [commitWorkspace, loadedYearForRequests, requestJson, selectedMyRequest, toast]);

  const handleConfirmReviewAction = useCallback(async () => {
    if (!pendingReviewAction) {
      return;
    }
    const action = pendingReviewAction;
    setPendingReviewAction(null);
    await handleReviewRequest(action);
  }, [handleReviewRequest, pendingReviewAction]);

  const handleSaveMyRequestDetail = useCallback(async (params: {
    requestId: string | null;
      payload: {
        type: LeaveSpherePtoType;
        startDate: string;
        endDate: string;
        hours: number;
        description: string;
      };
  }) => {
    if (!workspace || !params.requestId) {
      return;
    }
    setIsSavingRequestDetail(true);
    try {
      const selectedRequestYear = selectedMyRequest?.year ?? loadedYearForRequests;
      const result = await updateLeaveSpherePtoRequest({
        requestJson,
        payload: {
          transactionId: params.requestId,
          type: params.payload.type,
          startDate: params.payload.startDate,
          endDate: params.payload.endDate,
          hours: params.payload.hours,
          description: params.payload.description,
          year: selectedRequestYear,
        },
      });
      commitWorkspace(result.workspace, result.source, selectedRequestYear);
      toast.success("Request updated", "PTO request details were updated.");
    } finally {
      setIsSavingRequestDetail(false);
    }
  }, [commitWorkspace, loadedYearForRequests, requestJson, selectedMyRequest?.year, toast, workspace]);
  const handleSaveReviewRequestDetail = useCallback(async (params: {
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
    setIsReviewing(true);
    try {
      const nextWorkspace = workspace ? {
        ...workspace,
        requests: workspace.requests.map((item) => {
          if (item.id !== params.requestId) {
            return item;
          }
          return {
            ...item,
            type: params.payload.type,
            startDate: params.payload.startDate,
            endDate: params.payload.endDate,
            hours: params.payload.hours,
            description: params.payload.description,
            approverNote: normalizeOptionalNote(reviewNote) || null,
          };
        }),
      } : null;
      if (!nextWorkspace) {
        return;
      }
      commitWorkspace(nextWorkspace, "cache", loadedYearForRequests);
      setReviewTargetId(null);
      setReviewNote("");
      toast.success("Request updated", "PTO request details were updated.");
    } finally {
      setIsReviewing(false);
    }
  }, [commitWorkspace, loadedYearForRequests, reviewNote, toast, workspace]);

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          eyebrow="LeaveSphere"
          title="My PTO"
          description={isManager
            ? "Track your PTO, submit requests, and review direct employee requests in one workspace."
            : "Track your PTO balance, submit requests, and monitor approvals in one workspace."}
          action={canRequestPto ? (
            <Button onClick={openSubmitDialog} disabled={isSubmitting || isReviewing}>
              <Plus className="size-4" />
              Submit PTO Request
            </Button>
          ) : null}
          gradientVariant="workspace"
        />
      )}
      footer={cacheStatus && loadedYear !== null ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            setIsChipRefreshOverlayVisible(true);
            void loadWorkspace(loadedYearForRequests, "network-only").finally(() => {
              setIsChipRefreshOverlayVisible(false);
            });
          }}
          disabled={isInitializing || isRefreshing || isSubmitting || isReviewing || !isOnline}
          refreshing={isRefreshing || isChipRefreshOverlayVisible}
          refreshLabel="Refresh PTO workspace"
          tooltipText={isOnline ? "Click to refresh PTO balances, requests, and calendar data" : "Offline. Reconnect to refresh PTO workspace."}
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
              disabled={isInitializing || isRefreshing || isSubmitting || isReviewing}
            />
          )}
          actions={(
            <>
              <Button onClick={() => void handleLoadByYear()} disabled={isInitializing || isRefreshing || isSubmitting || isReviewing}>
                {isInitializing ? "Loading..." : "Load"}
              </Button>
            </>
          )}
        />
      </SectionCard>

      {workspaceForYear && (
        <>
      <SectionCard
        title="PTO Balance Overview"
        description="Current totals by PTO type from the selected year's transactions. Remaining = total - used - scheduled."
      >
        {balanceRows.length === 0 ? (
          <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
            No PTO transactions were found for the selected year.
          </div>
        ) : (
          <div
            className={
              shouldCenterBalanceBlock
                ? "mx-auto grid w-full gap-3 sm:grid-cols-2 xl:grid-cols-3 xl:max-w-[72rem]"
                : "grid gap-3 sm:grid-cols-2 xl:grid-cols-4"
            }
          >
            {balanceRows.map((balance) => {
              const tone = pickPtoBalanceTone(balance.code || balance.type || balance.label);
              const remaining = balance.remainingHours ?? Math.max(0, (balance.totalHours || 0) - (balance.usedHours || 0) - (balance.scheduledHours || 0));
              return (
                <article
                  key={balance.code || balance.type}
                  className={`rounded-2xl border p-4 ${tone.card}`}
                >
                  <p className={`text-xs font-semibold uppercase tracking-[0.14em] ${tone.label}`}>{balance.label}</p>
                  <p className={`mt-2 text-2xl font-semibold tracking-[-0.03em] ${tone.value}`}>{formatHoursLabel(remaining)}</p>
                  <p className="mt-3 text-xs text-slate-700">
                    Total {formatHoursLabel(balance.totalHours)}
                    <span className="mx-1.5 text-slate-400">•</span>
                    Used {formatHoursLabel(balance.usedHours)}
                    <span className="mx-1.5 text-slate-400">•</span>
                    Scheduled {formatHoursLabel(balance.scheduledHours)}
                  </p>
                </article>
              );
            })}
          </div>
        )}
      </SectionCard>

      <div className="relative grid items-stretch gap-4 xl:grid-cols-[minmax(19rem,25rem)_minmax(0,1fr)]">
        <SectionCard
          title="My Requests"
          description={`${myRequests.length} request${myRequests.length === 1 ? "" : "s"} total`}
          actions={canRequestPto ? (
            <ActionIconButton
              aria-label="Submit PTO request"
              tooltip="Submit PTO request"
              onClick={openSubmitDialog}
              icon={<Plus className="size-4" />}
              className="h-9 w-9"
            />
          ) : null}
          style={calendarSectionHeight > 0 ? { height: `${calendarSectionHeight}px` } : undefined}
          contentClassName="flex min-h-0 flex-1 flex-col gap-3"
        >
          <div className="grid grid-cols-3 gap-2">
            <div className="rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-center">
              <p className="text-[11px] font-semibold uppercase tracking-[0.1em] text-amber-800">Pending</p>
              <p className="mt-1 text-lg font-semibold text-amber-900">{requestStatusSummary.pending}</p>
            </div>
            <div className="rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-center">
              <p className="text-[11px] font-semibold uppercase tracking-[0.1em] text-emerald-800">Approved</p>
              <p className="mt-1 text-lg font-semibold text-emerald-900">{requestStatusSummary.approved}</p>
            </div>
            <div className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-center">
              <p className="text-[11px] font-semibold uppercase tracking-[0.1em] text-rose-800">Rejected</p>
              <p className="mt-1 text-lg font-semibold text-rose-900">{requestStatusSummary.rejected}</p>
            </div>
          </div>

          <div className="min-h-0 flex-1 space-y-1.5 overflow-y-auto pr-1 pt-1.5">
            {myRequests.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                No PTO requests yet.
              </div>
            ) : (
              myRequests.map((request) => (
                <LeaveSpherePtoRequestCard
                  key={request.id}
                  onClick={() => setSelectedMyRequestId(request.id)}
                  tone={getLeaveSpherePtoRequestCardTone(request.startDate, request.endDate, todayIsoDate)}
                  employeeName={resolveRequestEmployee(request).employeeName}
                  title={resolveRequestEmployee(request).employeeName}
                  pictureUrl={resolveRequestEmployee(request).pictureUrl}
                  dateLabel={formatPtoRequestDateRangeLabel(request.startDate, request.endDate, tenantTimeZone)}
                  detailLabel={request.description}
                  hoursLabel={formatHoursLabel(request.hours)}
                  typeChip={<LeaveSpherePtoTypeChip type={request.type} label={requestTypeLabel(request.type)} />}
                  statusChip={<LeaveSpherePtoStatusChip status={request.status} label={statusLabel(request.status)} />}
                />
              ))
            )}
          </div>
        </SectionCard>

        <LeaveSphereMonthCalendar
          sectionRef={calendarSectionRef}
          title="PTO Calendar"
          description="Your PTO requests and holidays"
          monthKey={calendarMonth}
          className="self-start"
          onMonthChange={setCalendarMonth}
          onMonthHeadingClick={loadedYear === currentYear ? () => setCalendarMonth(currentMonthKey) : undefined}
          minMonthKey={loadedYear === null ? undefined : `${loadedYear}-01`}
          maxMonthKey={loadedYear === null ? undefined : `${loadedYear}-12`}
          todayIsoDate={todayIsoDate}
          events={calendarEvents}
          onDateClick={openSubmitDialogForDate}
          onEventClick={(event) => {
            if (event.id.startsWith("request:")) {
              setSelectedMyRequestId(event.id.slice("request:".length));
              setReviewTargetId(null);
              return;
            }
            if (event.id.startsWith("manager-request:")) {
              setReviewTargetId(event.id.slice("manager-request:".length));
              setSelectedMyRequestId(null);
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

        <SectionLoadingLayer
          active={loadingContract.sectionOverlayActive}
          message={loadingContract.sectionOverlayMessage}
        />
      </div>

      {isManager ? (
        <SectionCard
          title="Manager PTO"
          description="Direct employee requests and approval queue"
          contentClassName="space-y-4"
        >
          <div className="grid gap-3 md:grid-cols-3">
            <article className="rounded-2xl border border-amber-200 bg-amber-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-amber-800">Pending Approvals</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-amber-900">{pendingDirectReportRequests.length}</p>
            </article>
            <article className="rounded-2xl border border-emerald-200 bg-emerald-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-emerald-800">Reviewed</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-emerald-900">
                {Math.max(0, directReportRequests.length - pendingDirectReportRequests.length)}
              </p>
            </article>
            <article className="rounded-2xl border border-blue-100 bg-white p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Direct Reports</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-slate-900">{workspaceForYear.directReports.length}</p>
            </article>
          </div>

          <div className="flex justify-end">
            <div className="relative w-full max-w-[26rem]">
              <Input
                value={draftManagerSearch}
                onChange={(event) => handleManagerSearchInputChange(event.target.value)}
                onBlur={applyManagerSearchFromDraft}
                onKeyDown={handleManagerSearchInputKeyDown}
                placeholder="Filter by employee, type, date, or status"
                className="pr-9 transition !outline-none ![box-shadow:none] !focus:outline-none !focus:ring-0 !focus:ring-offset-0 !focus:border-slate-300 !focus:shadow-none !focus:[box-shadow:none] !focus-visible:outline-none !focus-visible:ring-0 !focus-visible:ring-offset-0 !focus-visible:border-slate-300 !focus-visible:shadow-none !focus-visible:[box-shadow:none]"
              />
              {draftManagerSearch ? (
                <TooltipTarget text="Clear manager request search">
                  <button
                    type="button"
                    onClick={handleClearManagerSearch}
                    className="absolute right-2 top-1/2 inline-flex size-6 -translate-y-1/2 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300"
                    aria-label="Clear manager request search"
                  >
                    <X className="size-3.5" />
                  </button>
                </TooltipTarget>
              ) : null}
            </div>
          </div>

          <LeaveSpherePtoRequestTable
            requests={filteredDirectReportRequests}
            emptyMessage={directReportRequests.length === 0
              ? "No direct employee requests yet."
              : "No manager requests match your keyword filter."}
            resolveEmployee={resolveRequestEmployee}
            requestTypeLabel={requestTypeLabel}
            statusLabel={statusLabel}
            formatSubmittedLabel={(request) => `Submitted ${formatDateInTimeZone(request.submittedAt, tenantTimeZone, {
              month: "numeric",
              day: "numeric",
              year: "numeric",
            })}`}
            formatDateRangeLabel={(request) => `${formatDateLabel(request.startDate, tenantTimeZone)} - ${formatDateLabel(request.endDate, tenantTimeZone)}`}
            formatHoursLabel={formatHoursLabel}
            onRequestClick={(request) => {
              setReviewTargetId(request.id);
              setReviewNote(request.approverNote || "");
            }}
            showDescription
            employeeColumnClassName="w-[19rem] px-3 py-2.5"
            dateRangeColumnClassName="w-[18rem] whitespace-nowrap px-3 py-2.5"
            descriptionColumnClassName="px-3 py-2.5 text-slate-600"
          />
        </SectionCard>
      ) : null}
        </>
      )}

      <LeaveSpherePtoRequestDetailModal
        mode="create"
        open={isRequestDialogOpen}
        request={null}
        initialForm={requestForm as LeaveSpherePtoRequestFormState}
        title="Submit PTO Request"
        description="Enter request details. Your manager can approve or reject from the Manager PTO queue."
        ptoTypeOptions={ptoTypeOptionsWithAvailability}
        statusLabel={statusLabel}
        saving={isSubmitting}
        calculateHours={(startDate, endDate) => calculateLeaveSpherePtoHours(startDate, endDate, holidayDates)}
        validateSubmit={validateRequestHours}
        canSubmitOverride={canSubmitRequestWithinBalance}
        saveLabel="Submit request"
        onOpenChange={setIsRequestDialogOpen}
        onClose={() => setIsRequestDialogOpen(false)}
        onFormChange={(nextForm) => setRequestForm(nextForm)}
        onSubmit={async ({ payload }) => {
          await handleSubmitRequest({ payload });
        }}
        allowedDateRange={loadedYearDateBounds ?? undefined}
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
              LeaveSphere holiday information for the selected date.
            </DialogDescription>
          </DialogHeader>

          {selectedHoliday ? (
            <div className="rounded-xl border border-indigo-100 bg-indigo-50/40 p-3 text-sm text-slate-700">
              <p><span className="font-semibold text-slate-900">Holiday:</span> {selectedHoliday.name}</p>
              <p><span className="font-semibold text-slate-900">Team region:</span> {selectedHoliday.teamRegion}</p>
              <p><span className="font-semibold text-slate-900">Date:</span> {formatDateLabel(selectedHoliday.date, tenantTimeZone)}</p>
            </div>
          ) : null}

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setSelectedHolidayId(null)}
            >
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <LeaveSpherePtoRequestDetailModal
        open={Boolean(selectedMyRequest)}
        request={selectedMyRequest}
        layoutVariant="my-pto-detail"
        readOnly={!selectedMyRequestActionConfig?.canEditForm}
        title="My PTO Request Detail"
        description="Review or update your PTO submission details."
        ptoTypeOptions={ptoTypeOptionsWithAvailability}
        statusLabel={statusLabel}
        saving={isSavingRequestDetail}
        calculateHours={(startDate, endDate) => calculateLeaveSpherePtoHours(startDate, endDate, holidayDates)}
        onOpenChange={(open) => {
          if (!open) {
            setSelectedMyRequestId(null);
          }
        }}
        onClose={() => setSelectedMyRequestId(null)}
        onSave={selectedMyRequestActionConfig?.canSubmit ? handleSaveMyRequestDetail : undefined}
        allowedDateRange={loadedYearDateBounds ?? undefined}
        details={selectedMyRequest ? (
          <div className="grid gap-2 text-sm text-slate-700 sm:grid-cols-2">
            <p>
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Submitted</span>
              <span className="mt-0.5 block font-semibold text-slate-900">{formatMonthDayYearLabel(selectedMyRequest.submittedAt, tenantTimeZone)}</span>
            </p>
            {selectedMyRequest.reviewerName ? (
              <p>
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Reviewed by</span>
              <span className="mt-0.5 block font-semibold text-slate-900">
                {selectedMyRequest.reviewerName}
                  {selectedMyRequest.reviewedAt ? ` · ${formatMonthDayYearLabel(selectedMyRequest.reviewedAt, tenantTimeZone)}` : ""}
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
        extraContent={selectedMyRequest?.approverNote ? (
          <div className="text-sm">
            <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Manager note</span>
            <p className="mt-1 text-slate-800">{selectedMyRequest.approverNote}</p>
          </div>
        ) : null}
        footerActions={selectedMyRequestActionConfig?.canCancel ? (
          <Button
            variant="outline"
            className="border-rose-200 text-rose-700 hover:bg-rose-50"
            onClick={() => setIsCancelMyRequestDialogOpen(true)}
            disabled={isSavingRequestDetail}
          >
            Cancel
          </Button>
        ) : null}
      />

      <ConfirmDialog
        open={isCancelMyRequestDialogOpen}
        title="Cancel PTO Request?"
        description="This will change the request status to cancelled. This action cannot be undone."
        confirmLabel="Cancel request"
        cancelLabel="Go back"
        onCancel={() => setIsCancelMyRequestDialogOpen(false)}
        onConfirm={() => {
          setIsCancelMyRequestDialogOpen(false);
          void handleCancelMyRequest();
        }}
      />

      <LeaveSpherePtoRequestDetailModal
        open={Boolean(selectedReviewRequest)}
        request={selectedReviewRequest}
        layoutVariant="my-pto-detail"
        readOnly={!selectedReviewRequestActionConfig?.canEditForm}
        title="Manager Request Preview"
        description="Review this direct employee PTO request and approve or reject."
        ptoTypeOptions={ptoTypeOptionsWithAvailability}
        statusLabel={statusLabel}
        saving={isReviewing}
        onOpenChange={(open) => {
          if (!open) {
            setReviewTargetId(null);
            setReviewNote("");
            setPendingReviewAction(null);
          }
        }}
        onClose={() => {
          setReviewTargetId(null);
          setReviewNote("");
          setPendingReviewAction(null);
        }}
        onSave={canSaveReviewNote ? handleSaveReviewRequestDetail : undefined}
        externalDirty={Boolean(selectedReviewRequest && isReviewNoteDirty)}
        details={selectedReviewRequest ? (
          <div className="grid gap-2 text-sm text-slate-700 sm:grid-cols-2">
            <p className="sm:col-span-2">
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Employee</span>
              <span className="mt-0.5 block font-semibold text-slate-900">
                {resolveRequestEmployee(selectedReviewRequest).employeeName}
              </span>
            </p>
            <p>
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Submitted</span>
              <span className="mt-0.5 block font-semibold text-slate-900">{formatDateLabel(selectedReviewRequest.submittedAt, tenantTimeZone)}</span>
            </p>
            {selectedReviewRequest.reviewerName ? (
              <p>
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Reviewed by</span>
              <span className="mt-0.5 block font-semibold text-slate-900">
                {selectedReviewRequest.reviewerName}
                  {selectedReviewRequest.reviewedAt ? ` · ${formatMonthDayYearLabel(selectedReviewRequest.reviewedAt, tenantTimeZone)}` : ""}
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
        extraContent={selectedReviewRequest ? (
          <div className="text-sm text-slate-700">
            <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Manager note</p>
            {selectedReviewRequest.approverNote ? (
              <p className="mt-1 whitespace-pre-wrap italic text-slate-400">
                {selectedReviewRequest.approverNote}
              </p>
            ) : (
              <p className="mt-1 whitespace-pre-wrap italic text-slate-400">
                No manager note.
              </p>
            )}
          </div>
        ) : null}
        footerActions={selectedReviewRequest ? (
          <>
            {selectedReviewRequestActionConfig?.canCancel ? (
              <Button
                variant="outline"
                onClick={() => setPendingReviewAction("cancel")}
                disabled={isReviewing}
              >
                Cancel
              </Button>
            ) : null}
            {selectedReviewRequestActionConfig?.canRevert ? (
              <Button
                variant="outline"
                onClick={() => setPendingReviewAction("revert")}
                disabled={isReviewing}
              >
                Revert
              </Button>
            ) : null}
            {selectedReviewRequestActionConfig?.canReject ? (
              <Button
                variant="outline"
                className="border-rose-200 text-rose-700 hover:bg-rose-50"
                onClick={() => setPendingReviewAction("reject")}
                disabled={isReviewing}
              >
                Reject
              </Button>
            ) : null}
            {selectedReviewRequestActionConfig?.canApprove ? (
              <Button
                variant="outline"
                className="border-emerald-300 text-emerald-700 hover:bg-emerald-50"
                onClick={() => setPendingReviewAction("approve")}
                disabled={isReviewing}
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
          label: "Manager note / reason",
          value: reviewNote,
          onChange: setReviewNote,
          placeholder: "Add a note or reason for this decision",
          disabled: isReviewing,
          helpText: pendingReviewActionCopy?.noteHelpText,
        }}
      />

      <PageLoadingLayer
        active={loadingContract.pageOverlayActive}
        message={loadingContract.pageOverlayMessage}
      />
    </AppPageLayout>
  );
}
