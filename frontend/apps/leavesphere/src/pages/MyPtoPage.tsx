import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Plus,
  UserRound,
  X,
} from "lucide-react";

import { PageBanner } from "@shell/components/layout/PageBanner";
import { Button } from "@tradsphere/components/ui/button";
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
import { Textarea } from "@tradsphere/components/ui/textarea";
import { ConfirmDialog } from "@tradsphere/components/ui/confirm-dialog";
import { useToast } from "@shell/components/ui/toast";
import { useApiRequest } from "@shared/hooks/useApiRequest";
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
import { TooltipTarget } from "@shared/components/actions/TooltipTarget";
import { LeaveSpherePtoRequestCard } from "@leavesphere/components/LeaveSpherePtoRequestCard";
import { LeaveSphereMonthCalendar, type LeaveSphereMonthCalendarEvent } from "@leavesphere/components/MonthCalendar";
import {
  mapLeaveSphereHolidayRegionToChipTone,
  mapLeaveSpherePtoStatusToChipTone,
  LeaveSpherePtoStatusChip,
  LeaveSpherePtoToneChip,
} from "@leavesphere/components/PtoStatusChip";
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
import { buildLeaveSpherePtoCalendarChipLabel } from "@leavesphere/lib/ptoCalendar";
import { formatPtoRequestDateRangeLabel } from "@leavesphere/lib/ptoDate";
import { getPtoRequestActionConfig } from "@leavesphere/lib/ptoRequestActionConfig";
import {
  getLeaveSphereReviewActionConfirmCopy,
  type LeaveSphereReviewAction,
} from "@leavesphere/lib/reviewActionConfirm";

type CacheStatus = {
  source: "network";
  fetchedAt: number;
};

type RequestFormState = {
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: string;
  reason: string;
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

const DEFAULT_PTO_TYPE_OPTIONS = [
  { value: "vacation", label: "Vacation" },
  { value: "sick", label: "Sick" },
  { value: "personal", label: "Personal" },
  { value: "floating", label: "Floating Holiday" },
] as const;

const DEFAULT_PTO_TYPE_LABELS: Record<LeaveSpherePtoType, string> = {
  vacation: "Vacation",
  sick: "Sick",
  personal: "Personal",
  floating: "Floating Holiday",
};

const EMPTY_FORM: RequestFormState = {
  type: "vacation",
  startDate: "",
  endDate: "",
  hours: "",
  reason: "",
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

function monthKeyFromDate(value: Date): string {
  return `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}`;
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

function formatDateLabel(isoDate: string): string {
  const value = new Date(`${isoDate}T00:00:00`);
  return value.toLocaleDateString(undefined, {
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

function requestTypeLabel(type: LeaveSpherePtoType): string {
  return DEFAULT_PTO_TYPE_LABELS[type] || "PTO";
}

function buildPtoTypeOptions(
  ptoTypes: LeaveSpherePtoWorkspaceData["ptoTypes"] | undefined,
): Array<{ value: LeaveSpherePtoType; label: string }> {
  const labels = new Map<LeaveSpherePtoType, string>(
    DEFAULT_PTO_TYPE_OPTIONS.map((item) => [item.value, item.label]),
  );
  for (const ptoType of ptoTypes ?? []) {
    if (ptoType.active && isPtoTypeValue(ptoType.type)) {
      labels.set(ptoType.type, ptoType.label || labels.get(ptoType.type) || requestTypeLabel(ptoType.type));
    }
  }
  return DEFAULT_PTO_TYPE_OPTIONS.map((item) => ({
    value: item.value,
    label: labels.get(item.value) || item.label,
  }));
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
    formatDateLabel(isoDate),
  ];
}

function managerRequestMatchesSearch(request: LeaveSpherePtoRequest, keyword: string): boolean {
  if (!keyword) {
    return true;
  }
  const tokens = [
    request.employeeName,
    requestTypeLabel(request.type),
    statusLabel(request.status),
    request.reason,
    request.managerNote ?? "",
    ...buildDateSearchTokens(request.startDate),
    ...buildDateSearchTokens(request.endDate),
    ...buildDateSearchTokens(request.submittedAt),
    ...(request.reviewedAt ? buildDateSearchTokens(request.reviewedAt) : []),
  ];
  return tokens.some((item) => asString(item).toLowerCase().includes(keyword));
}

function requestOverlapsYear(request: LeaveSpherePtoRequest, year: number): boolean {
  const yearStart = `${year}-01-01`;
  const yearEnd = `${year}-12-31`;
  return request.startDate <= yearEnd && request.endDate >= yearStart;
}

function filterWorkspaceByYear(workspace: LeaveSpherePtoWorkspaceData, year: number): LeaveSpherePtoWorkspaceData {
  const yearPrefix = `${year}-`;
  return {
    ...workspace,
    requests: workspace.requests.filter((item) => requestOverlapsYear(item, year)),
    holidays: workspace.holidays.filter((item) => item.date.startsWith(yearPrefix)),
  };
}

function isPtoTypeValue(value: unknown): value is LeaveSpherePtoType {
  return value === "vacation" || value === "sick" || value === "personal" || value === "floating";
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
    && typeof record.reason === "string"
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
  const currentYear = useMemo(() => new Date().getFullYear(), []);

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
  const restoredWorkspaceScopeRef = useRef<string | null>(null);
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

  const [calendarMonth, setCalendarMonth] = useState(() => monthKeyFromDate(new Date()));
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
      setLoadedYear(persisted.loadedYear);
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
  const ptoTypeOptions = useMemo(
    () => buildPtoTypeOptions(workspaceForYear?.ptoTypes),
    [workspaceForYear?.ptoTypes],
  );
  const selectedYearNumber = useMemo(() => Number(selectedYear), [selectedYear]);
  const loadedYearDateBounds = useMemo(
    () => (loadedYear === null ? null : buildYearDateBounds(loadedYear)),
    [loadedYear],
  );
  const loadedYearForRequests = loadedYear ?? (Number.isInteger(selectedYearNumber) ? selectedYearNumber : currentYear);

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
    () => directReportRequests.filter((item) => managerRequestMatchesSearch(item, normalizedAppliedManagerSearch)),
    [directReportRequests, normalizedAppliedManagerSearch],
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
    setReviewNote(selectedReviewRequest.managerNote || "");
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
      })
      : null),
    [selectedMyRequest],
  );
  const selectedReviewRequestActionConfig = useMemo(
    () => (selectedReviewRequest
      ? getPtoRequestActionConfig({
        role: "approver",
        status: selectedReviewRequest.status,
        startDate: selectedReviewRequest.startDate,
      })
      : null),
    [selectedReviewRequest],
  );
  const canEditReviewNote = Boolean(
    selectedReviewRequest?.status === "pending"
    && (
      selectedReviewRequestActionConfig?.canEditForm
      || selectedReviewRequestActionConfig?.canApprove
      || selectedReviewRequestActionConfig?.canReject
      || selectedReviewRequestActionConfig?.canCancel
    ),
  );
  const isReviewNoteDirty = useMemo(
    () => selectedReviewRequest
      ? normalizeOptionalNote(reviewNote) !== normalizeOptionalNote(selectedReviewRequest.managerNote)
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
      const requestType = requestTypeLabel(request.type);
      events.push({
        id: `request:${request.id}`,
        label: buildLeaveSpherePtoCalendarChipLabel(requestType, request.reason),
        tone: mapLeaveSpherePtoStatusToChipTone(request.status),
        startDate: request.startDate,
        endDate: request.endDate,
        title: [
          requestType,
          request.reason,
          `${formatDateLabel(request.startDate)} - ${formatDateLabel(request.endDate)}`,
        ].filter(Boolean).join(" · "),
      });
    }
    return events;
  }, [myRequests, workspaceForYear?.holidays]);
  const holidayDates = useMemo(() => {
    return new Set(
      (workspaceForYear?.holidays ?? [])
        .filter((item) => item.teamRegion === workspaceForYear?.currentUserTeamRegion)
        .map((item) => item.date),
    );
  }, [workspaceForYear?.currentUserTeamRegion, workspaceForYear?.holidays]);
  const balanceRows = workspaceForYear?.balances ?? [];
  const shouldCenterBalanceBlock = balanceRows.length > 0 && balanceRows.length <= 3;

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
    return `Year ${loadedYear}. Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [cacheStatus, isRefreshing, loadedYear]);

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

  const loadWorkspace = useCallback(async (year: number, freshData = false): Promise<boolean> => {
    if (freshData) {
      setIsRefreshing(true);
    } else {
      setIsInitializing(true);
    }
    setPageErrorMessage(null);

    try {
      const result = await loadLeaveSpherePtoWorkspace({
        requestJson,
        year,
      });
      setWorkspace(result.workspace);
      setLoadedYear(year);
      setSelectedYear(String(year));
      setCacheStatus({
        source: result.source,
        fetchedAt: Date.now(),
      });
      setRefreshMessage(result.refreshMessage);
      return true;
    } catch {
      setPageErrorMessage("Unable to load PTO workspace right now. Please try again.");
      return false;
    } finally {
      setIsInitializing(false);
      setIsRefreshing(false);
    }
  }, [requestJson]);

  useEffect(() => {
    if (!hasHydratedPageState || !pageStateStorageKey || loadedYear === null) {
      return;
    }
    if (restoredWorkspaceScopeRef.current === pageStateStorageKey) {
      return;
    }
    restoredWorkspaceScopeRef.current = pageStateStorageKey;
    void loadWorkspace(loadedYear, false);
  }, [hasHydratedPageState, loadedYear, loadWorkspace, pageStateStorageKey]);

  const handleLoadByYear = useCallback(async () => {
    const parsedYear = Number(selectedYear);
    if (!Number.isInteger(parsedYear)) {
      return;
    }

    const didLoad = await loadWorkspace(parsedYear, false);
    if (!didLoad) {
      return;
    }

    const today = new Date();
    const defaultMonth = parsedYear === today.getFullYear() ? today.getMonth() + 1 : 1;
    setCalendarMonth(`${parsedYear}-${String(defaultMonth).padStart(2, "0")}`);
  }, [loadWorkspace, selectedYear]);

  const openSubmitDialog = useCallback(() => {
    if (!canRequestPto || !loadedYearDateBounds) {
      return;
    }
    const today = new Date();
    const todayIsoDate = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;
    const defaultIsoDate = isDateWithinBounds(todayIsoDate, loadedYearDateBounds)
      ? todayIsoDate
      : loadedYearDateBounds.minDate;
    setRequestForm({
      type: "vacation",
      startDate: defaultIsoDate,
      endDate: defaultIsoDate,
      hours: calculateLeaveSpherePtoHours(defaultIsoDate, defaultIsoDate, holidayDates),
      reason: "",
    });
    setIsRequestDialogOpen(true);
  }, [canRequestPto, holidayDates, loadedYearDateBounds]);

  const openSubmitDialogForDate = useCallback((isoDate: string) => {
    if (!canRequestPto || !loadedYearDateBounds || !isDateWithinBounds(isoDate, loadedYearDateBounds)) {
      return;
    }
    setRequestForm({
      type: "vacation",
      startDate: isoDate,
      endDate: isoDate,
      hours: calculateLeaveSpherePtoHours(isoDate, isoDate, holidayDates),
      reason: "",
    });
    setIsRequestDialogOpen(true);
  }, [canRequestPto, holidayDates, loadedYearDateBounds]);

  const handleSubmitRequest = useCallback(async (params: {
    payload: {
      type: LeaveSpherePtoType;
      startDate: string;
      endDate: string;
      hours: number;
      reason: string;
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
      setWorkspace(result.workspace);
      setCacheStatus({ source: result.source, fetchedAt: Date.now() });
      setIsRequestDialogOpen(false);
      toast.success("PTO request submitted", "Your request is now pending manager review.");
    } catch {
      toast.error("Submit failed", "Unable to submit this PTO request right now.");
    } finally {
      setIsSubmitting(false);
    }
  }, [
    requestJson,
    toast,
    canRequestPto,
    loadedYearDateBounds,
    loadedYearForRequests,
  ]);

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
          note: reviewNote,
        },
      });
      setWorkspace(result.workspace);
      setCacheStatus({ source: result.source, fetchedAt: Date.now() });
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
      setWorkspace(result.workspace);
      setCacheStatus({ source: result.source, fetchedAt: Date.now() });
      setSelectedMyRequestId(null);
      toast.success("Request cancelled", "PTO request status was updated to cancelled.");
    } catch {
      toast.error("Cancel failed", "Unable to cancel this PTO request right now.");
    } finally {
      setIsSavingRequestDetail(false);
    }
  }, [requestJson, selectedMyRequest, toast]);

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
      reason: string;
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
          reason: params.payload.reason,
          year: selectedRequestYear,
        },
      });
      setWorkspace(result.workspace);
      setCacheStatus({ source: result.source, fetchedAt: Date.now() });
      toast.success("Request updated", "PTO request details were updated.");
    } finally {
      setIsSavingRequestDetail(false);
    }
  }, [loadedYearForRequests, requestJson, selectedMyRequest?.year, toast, workspace]);
  const handleSaveReviewRequestDetail = useCallback(async (params: {
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
    setIsReviewing(true);
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
              managerNote: normalizeOptionalNote(reviewNote) || null,
            };
          }),
        };
      });
      setReviewTargetId(null);
      setReviewNote("");
      toast.success("Request updated", "PTO request details were updated.");
    } finally {
      setIsReviewing(false);
    }
  }, [reviewNote, toast]);

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
          action={(
            <Button onClick={openSubmitDialog} disabled={!canRequestPto || isSubmitting || isReviewing}>
              <Plus className="size-4" />
              Submit PTO Request
            </Button>
          )}
          gradientVariant="workspace"
        />
      )}
      footer={cacheStatus && loadedYear !== null ? (
      <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            setIsChipRefreshOverlayVisible(true);
            void loadWorkspace(loadedYearForRequests, true).finally(() => {
              setIsChipRefreshOverlayVisible(false);
            });
          }}
          disabled={isInitializing || isRefreshing || isSubmitting || isReviewing}
          refreshing={isRefreshing || isChipRefreshOverlayVisible}
          refreshLabel="Refresh PTO workspace"
          tooltipText="Click to refresh PTO balances, requests, and calendar data"
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
        title="PTO balance overview"
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

      <div className="relative grid gap-4 xl:grid-cols-[minmax(18rem,23rem)_minmax(0,1fr)]">
        <SectionCard
          title="My Requets"
          description={`${myRequests.length} request${myRequests.length === 1 ? "" : "s"} total`}
          contentClassName="space-y-3"
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

          <div className="max-h-[38rem] space-y-2 overflow-y-auto pr-1">
            {myRequests.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                No PTO requests yet.
              </div>
            ) : (
              myRequests.map((request) => (
                <LeaveSpherePtoRequestCard
                  key={request.id}
                  onClick={() => setSelectedMyRequestId(request.id)}
                  title={requestTypeLabel(request.type)}
                  dateLabel={formatPtoRequestDateRangeLabel(request.startDate, request.endDate)}
                  detailLabel={request.reason}
                  hoursLabel={formatHoursLabel(request.hours)}
                  submittedLabel={`Submitted ${formatDateLabel(request.submittedAt)}`}
                  statusChip={<LeaveSpherePtoStatusChip status={request.status} label={statusLabel(request.status)} />}
                />
              ))
            )}
          </div>
        </SectionCard>

        <LeaveSphereMonthCalendar
          title="PTO calendar"
          description="Your PTO requests and holidays"
          monthKey={calendarMonth}
          onMonthChange={setCalendarMonth}
          events={calendarEvents}
          onDateClick={openSubmitDialogForDate}
          onEventClick={(event) => {
            if (event.id.startsWith("request:")) {
              setSelectedMyRequestId(event.id.slice("request:".length));
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
            <article className="rounded-2xl border border-blue-100 bg-white p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Direct reports</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-slate-900">{workspaceForYear.directReports.length}</p>
            </article>
            <article className="rounded-2xl border border-amber-200 bg-amber-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-amber-800">Pending approvals</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-amber-900">{pendingDirectReportRequests.length}</p>
            </article>
            <article className="rounded-2xl border border-emerald-200 bg-emerald-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-emerald-800">Reviewed</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-emerald-900">
                {Math.max(0, directReportRequests.length - pendingDirectReportRequests.length)}
              </p>
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
                {directReportRequests.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-3 py-6 text-center text-sm text-slate-600">
                      No direct employee requests yet.
                    </td>
                  </tr>
                ) : filteredDirectReportRequests.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-3 py-6 text-center text-sm text-slate-600">
                      No manager requests match your keyword filter.
                    </td>
                  </tr>
                ) : (
                  filteredDirectReportRequests.map((request) => (
                    <tr
                      key={request.id}
                      className="cursor-pointer border-t border-blue-100/80 bg-white text-slate-700 transition-colors hover:bg-blue-50/40"
                      onClick={() => {
                        setReviewTargetId(request.id);
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
      ) : null}
        </>
      )}

      <LeaveSpherePtoRequestDetailModal
        mode="create"
        open={isRequestDialogOpen}
        request={null}
        initialForm={requestForm as LeaveSpherePtoRequestFormState}
        title="Submit PTO request"
        description="Enter request details. Your manager can approve or reject from the Manager PTO queue."
        ptoTypeOptions={ptoTypeOptions}
        statusLabel={statusLabel}
        saving={isSubmitting}
        calculateHours={(startDate, endDate) => calculateLeaveSpherePtoHours(startDate, endDate, holidayDates)}
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
            <DialogTitle>Holiday detail</DialogTitle>
            <DialogDescription>
              LeaveSphere holiday information for the selected date.
            </DialogDescription>
          </DialogHeader>

          {selectedHoliday ? (
            <div className="rounded-xl border border-indigo-100 bg-indigo-50/40 p-3 text-sm text-slate-700">
              <p><span className="font-semibold text-slate-900">Holiday:</span> {selectedHoliday.name}</p>
              <p><span className="font-semibold text-slate-900">Team region:</span> {selectedHoliday.teamRegion}</p>
              <p><span className="font-semibold text-slate-900">Date:</span> {formatDateLabel(selectedHoliday.date)}</p>
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
        title="My PTO request detail"
        description="Review or update your PTO submission details."
        ptoTypeOptions={ptoTypeOptions}
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
        details={selectedMyRequest ? (
          <div className="grid gap-2 text-sm text-slate-700 sm:grid-cols-2">
            <p>
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Submitted</span>
              <span className="mt-0.5 block font-semibold text-slate-900">{formatDateLabel(selectedMyRequest.submittedAt)}</span>
            </p>
            {selectedMyRequest.reviewerName ? (
              <p>
                <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Reviewed by</span>
                <span className="mt-0.5 block font-semibold text-slate-900">
                  {selectedMyRequest.reviewerName}
                  {selectedMyRequest.reviewedAt ? ` · ${formatDateLabel(selectedMyRequest.reviewedAt)}` : ""}
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
        extraContent={selectedMyRequest?.managerNote ? (
          <div className="text-sm">
            <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Manager note</span>
            <p className="mt-1 text-slate-800">{selectedMyRequest.managerNote}</p>
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
        title="Cancel PTO request?"
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
        title="Manager request preview"
        description="Review this direct employee PTO request and approve or reject."
        ptoTypeOptions={ptoTypeOptions}
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
        externalDirty={Boolean(
          selectedReviewRequest
          && canEditReviewNote
          && isReviewNoteDirty
        )}
        details={selectedReviewRequest ? (
          <div className="grid gap-2 text-sm text-slate-700 sm:grid-cols-2">
            <p className="sm:col-span-2">
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Employee</span>
              <span className="mt-0.5 block font-semibold text-slate-900">{selectedReviewRequest.employeeName}</span>
            </p>
            <p>
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Submitted</span>
              <span className="mt-0.5 block font-semibold text-slate-900">{formatDateLabel(selectedReviewRequest.submittedAt)}</span>
            </p>
            {selectedReviewRequest.reviewerName ? (
              <p>
                <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Reviewed by</span>
                <span className="mt-0.5 block font-semibold text-slate-900">
                  {selectedReviewRequest.reviewerName}
                  {selectedReviewRequest.reviewedAt ? ` · ${formatDateLabel(selectedReviewRequest.reviewedAt)}` : ""}
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
          <label className="block space-y-1 text-sm">
            <span className="text-slate-600">Manager note</span>
            <Textarea
              value={reviewNote}
              onChange={(event) => setReviewNote(event.target.value)}
              className="min-h-[100px]"
              placeholder="Optional note for the employee"
              disabled={isReviewing || !canEditReviewNote}
            />
          </label>
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

      <PageLoadingLayer
        active={loadingContract.pageOverlayActive}
        message={loadingContract.pageOverlayMessage}
      />
    </AppPageLayout>
  );
}
