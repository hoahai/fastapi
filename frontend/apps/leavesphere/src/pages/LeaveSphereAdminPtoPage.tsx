import { useCallback, useEffect, useMemo, useState } from "react";
import {
  CalendarDays,
  Clock3,
  Plus,
  Settings2,
  Sparkles,
  Users,
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
import { useToast } from "@shell/components/ui/toast";
import { hasAppAdminAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
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
} from "@leavesphere/lib/leaveSphereAdminPtoMocks";
import type { LeaveSpherePtoRequest, LeaveSpherePtoStatus, LeaveSpherePtoType } from "@leavesphere/lib/leaveSpherePtoMocks";

type CacheStatus = {
  source: "mock" | "network";
  fetchedAt: number;
};

type CalendarDay = {
  isoDate: string;
  dayNumber: number;
  inCurrentMonth: boolean;
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
  active: boolean;
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
  active: true,
};

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
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

function statusChipClass(status: LeaveSpherePtoStatus): string {
  if (status === "approved") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (status === "rejected") {
    return "border-rose-200 bg-rose-50 text-rose-700";
  }
  if (status === "cancelled") {
    return "border-slate-300 bg-slate-100 text-slate-600";
  }
  return "border-amber-200 bg-amber-50 text-amber-700";
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

function requestTypeLabel(type: LeaveSpherePtoType): string {
  return PTO_TYPE_OPTIONS.find((item) => item.value === type)?.label || "PTO";
}

function monthKeyFromDate(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function parseMonthKey(monthKey: string): Date {
  const match = /^(\d{4})-(\d{2})$/.exec(monthKey);
  if (!match) {
    const now = new Date();
    return new Date(now.getFullYear(), now.getMonth(), 1);
  }
  return new Date(Number(match[1]), Number(match[2]) - 1, 1);
}

function formatMonthHeading(monthKey: string): string {
  return parseMonthKey(monthKey).toLocaleDateString(undefined, { month: "long", year: "numeric" });
}

function addMonths(monthKey: string, offset: number): string {
  const date = parseMonthKey(monthKey);
  date.setMonth(date.getMonth() + offset);
  return monthKeyFromDate(date);
}

function toIsoDate(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function buildCalendarDays(monthKey: string): CalendarDay[] {
  const monthStart = parseMonthKey(monthKey);
  const monthEnd = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0);
  const startWeekday = (monthStart.getDay() + 6) % 7;
  const gridStart = new Date(monthStart);
  gridStart.setDate(monthStart.getDate() - startWeekday);

  const days: CalendarDay[] = [];
  for (let i = 0; i < 42; i += 1) {
    const date = new Date(gridStart);
    date.setDate(gridStart.getDate() + i);
    days.push({
      isoDate: toIsoDate(date),
      dayNumber: date.getDate(),
      inCurrentMonth: date >= monthStart && date <= monthEnd,
    });
  }
  return days;
}

function requestIsOut(request: LeaveSpherePtoRequest): boolean {
  return request.status === "approved" || request.status === "pending";
}

function inRange(isoDate: string, start: string, end: string): boolean {
  return start <= isoDate && isoDate <= end;
}

function rangeIsValid(start: string, end: string): boolean {
  return Boolean(start) && Boolean(end) && start <= end;
}

function positiveNumber(value: string): boolean {
  return asNumber(value) > 0;
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

  const [workspace, setWorkspace] = useState<LeaveSphereAdminWorkspaceData | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [pageErrorMessage, setPageErrorMessage] = useState<string | null>(null);

  const [tab, setTab] = useState<AdminTab>("calendar");
  const [calendarMonth, setCalendarMonth] = useState(() => monthKeyFromDate(new Date()));

  const [isInitializing, setIsInitializing] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [isMutating, setIsMutating] = useState(false);

  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [createForm, setCreateForm] = useState<CreateRequestForm>(EMPTY_CREATE_FORM);
  const [createError, setCreateError] = useState<string | null>(null);

  const [selectedRequestId, setSelectedRequestId] = useState<string | null>(null);
  const [reviewNote, setReviewNote] = useState("");

  const [isAdjustModalOpen, setIsAdjustModalOpen] = useState(false);
  const [adjustForm, setAdjustForm] = useState<AdjustBalanceForm>(EMPTY_ADJUST_FORM);
  const [adjustError, setAdjustError] = useState<string | null>(null);

  const [isSetupModalOpen, setIsSetupModalOpen] = useState(false);
  const [setupForm, setSetupForm] = useState<SetupForm>(EMPTY_SETUP_FORM);
  const [setupError, setSetupError] = useState<string | null>(null);

  const requests = useMemo(
    () => [...(workspace?.requests ?? [])].sort((left, right) => right.submittedAt.localeCompare(left.submittedAt)),
    [workspace?.requests],
  );
  const pendingRequests = useMemo(
    () => requests.filter((item) => item.status === "pending"),
    [requests],
  );
  const recentRequests = useMemo(
    () => requests.slice(0, 10),
    [requests],
  );

  const selectedRequest = useMemo(
    () => requests.find((item) => item.id === selectedRequestId) || null,
    [requests, selectedRequestId],
  );

  const employeeById = useMemo(() => employeeNameById(workspace), [workspace]);

  const employeeOptions = useMemo(
    () => (workspace?.employees ?? []).map((item) => ({ value: item.employeeId, label: item.employeeName })),
    [workspace?.employees],
  );

  const ptoTypeOptions = useMemo(
    () => (workspace?.ptoTypes ?? []).filter((item) => item.active).map((item) => ({ value: item.code, label: item.label })),
    [workspace?.ptoTypes],
  );

  const calendarDays = useMemo(() => buildCalendarDays(calendarMonth), [calendarMonth]);

  const overview = useMemo(() => {
    return {
      pendingCount: pendingRequests.length,
      upcomingOutCount: countUpcomingOutRequests(requests),
      employeeCount: workspace?.employees.filter((item) => item.active).length ?? 0,
      holidayCount: workspace?.holidays.length ?? 0,
    };
  }, [pendingRequests.length, requests, workspace?.employees, workspace?.holidays.length]);

  const cacheStatusText = useMemo(() => {
    if (isRefreshing) {
      return "Refreshing LeaveSphere Admin PTO workspace...";
    }
    if (!cacheStatus) {
      return "Loading LeaveSphere Admin PTO workspace...";
    }
    return `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [cacheStatus, isRefreshing]);

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

  const loadWorkspace = useCallback(async (freshData = false) => {
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
    } catch {
      setPageErrorMessage("Unable to load Admin PTO workspace right now. Please try again.");
    } finally {
      setIsInitializing(false);
      setIsRefreshing(false);
    }
  }, [currentUserId, currentUserName, requestJson, workspaceKey]);

  useEffect(() => {
    void loadWorkspace(false);
  }, [loadWorkspace]);

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
    if (!positiveNumber(createForm.hours)) {
      setCreateError("Hours must be greater than 0.");
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

  const handleAdjustBalance = useCallback(async () => {
    if (!asString(adjustForm.employeeId)) {
      setAdjustError("Employee is required.");
      return;
    }
    if (!positiveNumber(adjustForm.hours)) {
      setAdjustError("Hours must be greater than 0.");
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
        managerId: asString(setupForm.managerId) || (workspace?.currentUserId ?? currentUserId),
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
    setupForm.title,
    toast,
    workspace?.currentUserId,
    workspaceKey,
  ]);

  const renderCalendarTab = () => {
    return (
      <SectionCard
        title="Month calendar"
        description="Who is out and company holidays"
        actions={(
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={() => setCalendarMonth((current) => addMonths(current, -1))}>Prev</Button>
            <p className="min-w-[10rem] text-center text-sm font-semibold text-slate-800">{formatMonthHeading(calendarMonth)}</p>
            <Button variant="outline" size="sm" onClick={() => setCalendarMonth((current) => addMonths(current, 1))}>Next</Button>
          </div>
        )}
        contentClassName="space-y-3"
      >
        <div className="grid grid-cols-7 gap-1 text-center text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-500">
          {["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].map((day) => (
            <div key={day}>{day}</div>
          ))}
        </div>

        <div className="grid grid-cols-7 gap-1.5">
          {calendarDays.map((day) => {
            const holidays = (workspace?.holidays ?? []).filter((item) => item.date === day.isoDate);
            const outRequests = requests.filter((item) => requestIsOut(item) && inRange(day.isoDate, item.startDate, item.endDate));
            return (
              <article
                key={day.isoDate}
                className={[
                  "min-h-[8.2rem] rounded-xl border p-2",
                  day.inCurrentMonth ? "border-blue-100 bg-white" : "border-slate-200 bg-slate-50/70 text-slate-400",
                ].join(" ")}
              >
                <p className="text-xs font-semibold">{day.dayNumber}</p>
                <div className="mt-1 space-y-1">
                  {holidays.slice(0, 1).map((holiday) => (
                    <div key={holiday.id} className="truncate rounded-full border border-indigo-200 bg-indigo-50 px-1.5 py-0.5 text-[10px] font-medium text-indigo-700" title={holiday.name}>
                      {holiday.name}
                    </div>
                  ))}
                  {outRequests.slice(0, 2).map((request) => (
                    <div key={`${request.id}-${day.isoDate}`} className="truncate rounded-full border border-blue-200 bg-blue-50 px-1.5 py-0.5 text-[10px] font-medium text-blue-700" title={`${request.employeeName} · ${requestTypeLabel(request.type)}`}>
                      {request.employeeName}
                    </div>
                  ))}
                  {outRequests.length > 2 ? (
                    <div className="text-[10px] font-medium text-slate-500">+{outRequests.length - 2} more</div>
                  ) : null}
                </div>
              </article>
            );
          })}
        </div>
      </SectionCard>
    );
  };

  const renderRequestsTab = () => {
    return (
      <div className="grid gap-4 xl:grid-cols-[minmax(18rem,22rem)_minmax(0,1fr)]">
        <SectionCard
          title="Pending requests"
          description={`${pendingRequests.length} waiting for decision`}
          contentClassName="space-y-2"
        >
          <div className="max-h-[40rem] space-y-2 overflow-y-auto pr-1">
            {pendingRequests.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                No pending PTO requests.
              </div>
            ) : (
              pendingRequests.map((request) => (
                <article key={request.id} className="rounded-xl border border-blue-100 bg-white px-3 py-3">
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <p className="text-sm font-semibold text-slate-900">{request.employeeName}</p>
                      <p className="text-xs text-slate-600">{requestTypeLabel(request.type)} · {formatDateLabel(request.startDate)} - {formatDateLabel(request.endDate)}</p>
                    </div>
                    <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold ${statusChipClass(request.status)}`}>
                      {statusLabel(request.status)}
                    </span>
                  </div>
                  <div className="mt-2 flex items-center justify-between">
                    <p className="text-xs text-slate-500">{formatHoursLabel(request.hours)}</p>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        setSelectedRequestId(request.id);
                        setReviewNote(request.managerNote || "");
                      }}
                    >
                      Preview
                    </Button>
                  </div>
                </article>
              ))
            )}
          </div>
        </SectionCard>

        <SectionCard
          title="Recent request history"
          description="All employee PTO request statuses"
          contentClassName="space-y-2"
        >
          <div className="overflow-x-auto rounded-xl border border-blue-100">
            <table className="min-w-full text-left text-sm">
              <thead className="bg-blue-50/70 text-xs uppercase tracking-[0.08em] text-slate-600">
                <tr>
                  <th className="px-3 py-2.5">Employee</th>
                  <th className="px-3 py-2.5">Type</th>
                  <th className="px-3 py-2.5">Range</th>
                  <th className="px-3 py-2.5">Status</th>
                  <th className="px-3 py-2.5">Hours</th>
                  <th className="px-3 py-2.5 text-right">Action</th>
                </tr>
              </thead>
              <tbody>
                {recentRequests.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="px-3 py-6 text-center text-sm text-slate-600">No requests found.</td>
                  </tr>
                ) : (
                  recentRequests.map((request) => (
                    <tr key={request.id} className="border-t border-blue-100/80 bg-white text-slate-700">
                      <td className="px-3 py-2.5">{request.employeeName}</td>
                      <td className="px-3 py-2.5">{requestTypeLabel(request.type)}</td>
                      <td className="px-3 py-2.5">{formatDateLabel(request.startDate)} - {formatDateLabel(request.endDate)}</td>
                      <td className="px-3 py-2.5">
                        <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold ${statusChipClass(request.status)}`}>
                          {statusLabel(request.status)}
                        </span>
                      </td>
                      <td className="px-3 py-2.5">{formatHoursLabel(request.hours)}</td>
                      <td className="px-3 py-2.5 text-right">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => {
                            setSelectedRequestId(request.id);
                            setReviewNote(request.managerNote || "");
                          }}
                        >
                          Review
                        </Button>
                      </td>
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
              {(workspace?.employeeBalances ?? []).map((row) => {
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
                  managerId: workspace?.currentUserId || currentUserId,
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
              {(workspace?.ptoTypes ?? []).map((item: LeaveSphereAdminPtoTypeConfig) => (
                <span key={item.code} className="inline-flex rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-700">
                  {item.label}
                </span>
              ))}
            </div>
          </div>

          <div className="rounded-xl border border-blue-100 bg-white p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">PTO actions</p>
            <ul className="mt-2 space-y-2 text-sm text-slate-700">
              {(workspace?.ptoActions ?? []).slice(0, 4).map((item) => (
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
              {(workspace?.employees ?? []).map((item: LeaveSphereAdminEmployee) => (
                <li key={item.employeeId} className="rounded-lg border border-slate-200 bg-slate-50 px-2.5 py-2">
                  <p className="font-medium text-slate-800">{item.employeeName}</p>
                  <p className="text-xs text-slate-600">{item.title} · Manager: {item.managerName}</p>
                </li>
              ))}
            </ul>
          </div>

          <div className="rounded-xl border border-blue-100 bg-white p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">Holidays</p>
            <ul className="mt-2 max-h-40 space-y-2 overflow-y-auto pr-1 text-sm text-slate-700">
              {(workspace?.holidays ?? []).map((item) => (
                <li key={item.id} className="rounded-lg border border-slate-200 bg-slate-50 px-2.5 py-2">
                  <p className="font-medium text-slate-800">{item.name}</p>
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
      footer={cacheStatus ? (
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

      <SectionCard
        title="Admin sections"
        description="Choose a focused workspace section"
        contentClassName="space-y-3"
      >
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
      </SectionCard>

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
            <Input type="number" min={1} step={0.5} value={createForm.hours} onChange={(event) => setCreateForm((current) => ({ ...current, hours: event.target.value }))} disabled={isMutating} />
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

      <Dialog open={Boolean(selectedRequest)} onOpenChange={(open) => {
        if (!open) {
          setSelectedRequestId(null);
          setReviewNote("");
        }
      }}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Request preview</DialogTitle>
            <DialogDescription>Review and approve or reject employee PTO requests.</DialogDescription>
          </DialogHeader>

          {selectedRequest ? (
            <div className="space-y-3">
              <div className="rounded-xl border border-blue-100 bg-blue-50/40 p-3 text-sm text-slate-700">
                <p><span className="font-semibold text-slate-900">Employee:</span> {selectedRequest.employeeName}</p>
                <p><span className="font-semibold text-slate-900">Manager:</span> {employeeById.get(selectedRequest.managerId) || selectedRequest.managerId}</p>
                <p><span className="font-semibold text-slate-900">Type:</span> {requestTypeLabel(selectedRequest.type)}</p>
                <p><span className="font-semibold text-slate-900">Range:</span> {formatDateLabel(selectedRequest.startDate)} - {formatDateLabel(selectedRequest.endDate)}</p>
                <p><span className="font-semibold text-slate-900">Hours:</span> {formatHoursLabel(selectedRequest.hours)}</p>
                <p><span className="font-semibold text-slate-900">Reason:</span> {selectedRequest.reason}</p>
                <p><span className="font-semibold text-slate-900">Status:</span> {statusLabel(selectedRequest.status)}</p>
              </div>

              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Admin note</span>
                <Textarea value={reviewNote} onChange={(event) => setReviewNote(event.target.value)} className="min-h-[100px]" disabled={isMutating} />
              </label>
            </div>
          ) : null}

          <DialogFooter>
            <Button variant="outline" onClick={() => setSelectedRequestId(null)} disabled={isMutating}>Close</Button>
            {selectedRequest?.status === "pending" ? (
              <>
                <Button variant="outline" className="border-rose-200 text-rose-700 hover:bg-rose-50" onClick={() => void handleReviewRequest(false)} disabled={isMutating}>Reject</Button>
                <Button className="bg-emerald-600 text-white hover:bg-emerald-700" onClick={() => void handleReviewRequest(true)} disabled={isMutating}>Approve</Button>
              </>
            ) : null}
          </DialogFooter>
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
              <Input type="number" min={0.5} step={0.5} value={adjustForm.hours} onChange={(event) => setAdjustForm((current) => ({ ...current, hours: event.target.value }))} disabled={isMutating} />
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
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Holiday name</span>
                  <Input value={setupForm.name} onChange={(event) => setSetupForm((current) => ({ ...current, name: event.target.value }))} disabled={isMutating} />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Date</span>
                  <Input type="date" value={setupForm.date} onChange={(event) => setSetupForm((current) => ({ ...current, date: event.target.value }))} disabled={isMutating} />
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
