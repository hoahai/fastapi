import { useCallback, useEffect, useMemo, useState } from "react";
import {
  CalendarDays,
  CheckCircle2,
  Clock3,
  Plus,
  ShieldCheck,
  UserRound,
  XCircle,
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
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { useAuth } from "@shared/auth/useAuth";
import { hasAppEditAccess } from "@shared/auth/permissions";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import {
  loadLeaveSpherePtoWorkspace,
  reviewLeaveSpherePtoRequest,
  submitLeaveSpherePtoRequest,
  type LeaveSphereHoliday,
  type LeaveSpherePtoRequest,
  type LeaveSpherePtoStatus,
  type LeaveSpherePtoType,
  type LeaveSpherePtoWorkspaceData,
} from "@leavesphere/lib/leaveSpherePtoMocks";

type CacheStatus = {
  source: "mock" | "network";
  fetchedAt: number;
};

type RequestFormState = {
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: string;
  reason: string;
};

type CalendarDay = {
  isoDate: string;
  dateNumber: number;
  isCurrentMonth: boolean;
};

type CalendarEvent = {
  kind: "holiday" | "request";
  label: string;
  tone: "holiday" | "approved" | "pending" | "rejected";
};

const PTO_TYPE_OPTIONS = [
  { value: "vacation", label: "Vacation" },
  { value: "sick", label: "Sick" },
  { value: "personal", label: "Personal" },
  { value: "floating", label: "Floating Holiday" },
] as const;

const EMPTY_FORM: RequestFormState = {
  type: "vacation",
  startDate: "",
  endDate: "",
  hours: "",
  reason: "",
};

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function toIsoDate(value: Date): string {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function monthKeyFromDate(value: Date): string {
  return `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}`;
}

function parseMonthKey(monthKey: string): Date {
  const match = /^(\d{4})-(\d{2})$/.exec(monthKey);
  if (!match) {
    const today = new Date();
    return new Date(today.getFullYear(), today.getMonth(), 1);
  }
  return new Date(Number(match[1]), Number(match[2]) - 1, 1);
}

function addMonths(monthKey: string, diff: number): string {
  const base = parseMonthKey(monthKey);
  base.setMonth(base.getMonth() + diff);
  return monthKeyFromDate(base);
}

function formatMonthHeading(monthKey: string): string {
  const date = parseMonthKey(monthKey);
  return date.toLocaleDateString(undefined, { month: "long", year: "numeric" });
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

function buildCalendarDays(monthKey: string): CalendarDay[] {
  const monthStart = parseMonthKey(monthKey);
  const monthEnd = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0);
  const startWeekday = (monthStart.getDay() + 6) % 7;
  const gridStart = new Date(monthStart);
  gridStart.setDate(monthStart.getDate() - startWeekday);

  const days: CalendarDay[] = [];
  for (let i = 0; i < 42; i += 1) {
    const next = new Date(gridStart);
    next.setDate(gridStart.getDate() + i);
    days.push({
      isoDate: toIsoDate(next),
      dateNumber: next.getDate(),
      isCurrentMonth: next >= monthStart && next <= monthEnd,
    });
  }
  return days;
}

function dayEvents(
  isoDate: string,
  myRequests: LeaveSpherePtoRequest[],
  holidays: LeaveSphereHoliday[],
): CalendarEvent[] {
  const events: CalendarEvent[] = [];
  for (const holiday of holidays) {
    if (holiday.date === isoDate) {
      events.push({ kind: "holiday", label: holiday.name, tone: "holiday" });
    }
  }
  for (const request of myRequests) {
    if (request.startDate <= isoDate && request.endDate >= isoDate) {
      const tone = request.status === "approved"
        ? "approved"
        : request.status === "rejected"
        ? "rejected"
        : "pending";
      events.push({
        kind: "request",
        label: `${requestTypeLabel(request.type)} (${statusLabel(request.status)})`,
        tone,
      });
    }
  }
  return events;
}

function toneClassName(tone: CalendarEvent["tone"]): string {
  if (tone === "holiday") {
    return "border-indigo-200 bg-indigo-50 text-indigo-700";
  }
  if (tone === "approved") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (tone === "rejected") {
    return "border-rose-200 bg-rose-50 text-rose-700";
  }
  return "border-amber-200 bg-amber-50 text-amber-700";
}

function isValidDateRange(startDate: string, endDate: string): boolean {
  if (!startDate || !endDate) {
    return false;
  }
  return startDate <= endDate;
}

function isPositiveNumber(value: string): boolean {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0;
}

export default function LeaveSphereMyPtoPage() {
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const toast = useToast();

  const currentUserId = asString(auth.user?.id) || "local-user";
  const currentUserName = asString(auth.user?.fullName) || asString(auth.user?.email) || "Workspace User";
  const isManager = useMemo(() => {
    const permissions = new Set(auth.accessProfile?.permissions ?? []);
    return hasAppEditAccess(auth.accessProfile, "leavesphere") || permissions.has("leavesphere.manager");
  }, [auth.accessProfile]);
  const managerId = isManager ? currentUserId : "mgr-alex-morgan";
  const workspaceKey = useMemo(() => {
    const tenant = asString(auth.tenantSlug) || "default";
    return `leavesphere:pto:${tenant}:${currentUserId}`;
  }, [auth.tenantSlug, currentUserId]);

  const [workspace, setWorkspace] = useState<LeaveSpherePtoWorkspaceData | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [pageErrorMessage, setPageErrorMessage] = useState<string | null>(null);

  const [isInitializing, setIsInitializing] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isReviewing, setIsReviewing] = useState(false);

  const [calendarMonth, setCalendarMonth] = useState(() => monthKeyFromDate(new Date()));
  const [isRequestDialogOpen, setIsRequestDialogOpen] = useState(false);
  const [requestForm, setRequestForm] = useState<RequestFormState>(EMPTY_FORM);
  const [requestFormError, setRequestFormError] = useState<string | null>(null);

  const [reviewTargetId, setReviewTargetId] = useState<string | null>(null);
  const [reviewNote, setReviewNote] = useState("");

  const myRequests = useMemo(() => {
    if (!workspace) {
      return [];
    }
    return workspace.requests
      .filter((item) => item.employeeId === workspace.currentUserId)
      .sort((left, right) => right.submittedAt.localeCompare(left.submittedAt));
  }, [workspace]);

  const directReportRequests = useMemo(() => {
    if (!workspace || !workspace.isManager) {
      return [];
    }
    return workspace.requests
      .filter((item) => item.employeeId !== workspace.currentUserId && item.managerId === workspace.currentUserId)
      .sort((left, right) => right.submittedAt.localeCompare(left.submittedAt));
  }, [workspace]);

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

  const calendarDays = useMemo(() => buildCalendarDays(calendarMonth), [calendarMonth]);

  const selectedReviewRequest = useMemo(
    () => directReportRequests.find((item) => item.id === reviewTargetId) || null,
    [directReportRequests, reviewTargetId],
  );

  const cacheStatusText = useMemo(() => {
    if (isRefreshing) {
      return "Refreshing LeaveSphere PTO workspace...";
    }
    if (!cacheStatus) {
      return "Loading LeaveSphere PTO workspace...";
    }
    return `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
  }, [cacheStatus, isRefreshing]);

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

  const loadWorkspace = useCallback(async (freshData = false) => {
    if (freshData) {
      setIsRefreshing(true);
    } else {
      setIsInitializing(true);
    }
    setPageErrorMessage(null);

    try {
      const result = await loadLeaveSpherePtoWorkspace({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        isManager,
        freshData,
      });
      setWorkspace(result.workspace);
      setCacheStatus({
        source: result.source,
        fetchedAt: Date.now(),
      });
      setRefreshMessage(result.refreshMessage);
    } catch {
      setPageErrorMessage("Unable to load PTO workspace right now. Please try again.");
    } finally {
      setIsInitializing(false);
      setIsRefreshing(false);
    }
  }, [currentUserId, currentUserName, isManager, requestJson, workspaceKey]);

  useEffect(() => {
    void loadWorkspace(false);
  }, [loadWorkspace]);

  const openSubmitDialog = useCallback(() => {
    setRequestForm({
      type: "vacation",
      startDate: "",
      endDate: "",
      hours: "",
      reason: "",
    });
    setRequestFormError(null);
    setIsRequestDialogOpen(true);
  }, []);

  const handleSubmitRequest = useCallback(async () => {
    const startDate = asString(requestForm.startDate);
    const endDate = asString(requestForm.endDate);
    const reason = asString(requestForm.reason);
    const hours = Number(requestForm.hours);

    if (!isValidDateRange(startDate, endDate)) {
      setRequestFormError("Start date must be on or before end date.");
      return;
    }
    if (!isPositiveNumber(requestForm.hours)) {
      setRequestFormError("Hours must be greater than 0.");
      return;
    }
    if (!reason) {
      setRequestFormError("Reason is required.");
      return;
    }

    setRequestFormError(null);
    setIsSubmitting(true);

    try {
      const result = await submitLeaveSpherePtoRequest({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        managerId,
        isManager,
        payload: {
          type: requestForm.type,
          startDate,
          endDate,
          hours,
          reason,
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
    currentUserId,
    currentUserName,
    isManager,
    managerId,
    requestForm.endDate,
    requestForm.hours,
    requestForm.reason,
    requestForm.startDate,
    requestForm.type,
    requestJson,
    toast,
    workspaceKey,
  ]);

  const handleReviewRequest = useCallback(async (approve: boolean) => {
    if (!selectedReviewRequest) {
      return;
    }
    setIsReviewing(true);
    try {
      const result = await reviewLeaveSpherePtoRequest({
        requestJson,
        workspaceKey,
        currentUserId,
        currentUserName,
        isManager,
        payload: {
          requestId: selectedReviewRequest.id,
          approve,
          note: reviewNote,
        },
      });
      setWorkspace(result.workspace);
      setCacheStatus({ source: result.source, fetchedAt: Date.now() });
      setReviewTargetId(null);
      setReviewNote("");
      toast.success(
        approve ? "Request approved" : "Request rejected",
        approve ? "Employee will see the approved status." : "Employee will see the rejected status.",
      );
    } catch {
      toast.error("Decision failed", "Unable to save this manager decision right now.");
    } finally {
      setIsReviewing(false);
    }
  }, [
    currentUserId,
    currentUserName,
    isManager,
    requestJson,
    reviewNote,
    selectedReviewRequest,
    toast,
    workspaceKey,
  ]);

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          eyebrow="LeaveSphere"
          title={isManager ? "My PTO / Manager PTO" : "My PTO"}
          description={isManager
            ? "Track your PTO, submit requests, and review direct employee requests in one workspace."
            : "Track your PTO balance, submit requests, and monitor approvals in one workspace."}
          action={(
            <Button onClick={openSubmitDialog} disabled={isSubmitting || isReviewing}>
              <Plus className="size-4" />
              Submit PTO Request
            </Button>
          )}
          gradientVariant="workspace"
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
          disabled={isInitializing || isRefreshing || isSubmitting || isReviewing}
          refreshing={isRefreshing || isChipRefreshOverlayVisible}
          refreshLabel="Refresh PTO workspace"
          tooltipText="Click to refresh PTO balances, requests, and calendar data"
          containerClassName="w-full"
        />
      ) : null}
    >
      <SectionCard
        title="PTO balance overview"
        description="Current totals by PTO type. Remaining = total - used - scheduled."
      >
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {(workspace?.balances ?? []).map((balance) => {
            const remaining = Math.max(0, balance.totalHours - balance.usedHours - balance.scheduledHours);
            return (
              <article
                key={balance.type}
                className="rounded-2xl border border-blue-100 bg-gradient-to-br from-white to-blue-50/60 p-4"
              >
                <p className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">{balance.label}</p>
                <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-slate-900">{formatHoursLabel(remaining)}</p>
                <div className="mt-3 space-y-1 text-xs text-slate-600">
                  <p>Total: {formatHoursLabel(balance.totalHours)}</p>
                  <p>Used: {formatHoursLabel(balance.usedHours)}</p>
                  <p>Scheduled: {formatHoursLabel(balance.scheduledHours)}</p>
                </div>
              </article>
            );
          })}
        </div>
      </SectionCard>

      <div className="relative grid gap-4 xl:grid-cols-[minmax(18rem,23rem)_minmax(0,1fr)]">
        <SectionCard
          title="My request status"
          description={`${myRequests.length} request${myRequests.length === 1 ? "" : "s"} total`}
          actions={(
            <Button variant="outline" onClick={openSubmitDialog} disabled={isSubmitting || isReviewing}>
              <Plus className="size-4" />
              Request
            </Button>
          )}
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
                <article key={request.id} className="rounded-xl border border-blue-100 bg-white px-3.5 py-3">
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <p className="text-sm font-semibold text-slate-900">{requestTypeLabel(request.type)}</p>
                      <p className="text-xs text-slate-600">
                        {formatDateLabel(request.startDate)} - {formatDateLabel(request.endDate)}
                      </p>
                    </div>
                    <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold ${statusChipClass(request.status)}`}>
                      {statusLabel(request.status)}
                    </span>
                  </div>
                  <p className="mt-2 text-xs text-slate-700">{request.reason}</p>
                  <div className="mt-2 flex items-center justify-between text-[11px] text-slate-500">
                    <span>{formatHoursLabel(request.hours)}</span>
                    <span>Submitted {formatDateLabel(request.submittedAt)}</span>
                  </div>
                </article>
              ))
            )}
          </div>
        </SectionCard>

        <SectionCard
          title="PTO calendar"
          description="Your PTO requests and holidays"
          actions={(
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={() => setCalendarMonth((current) => addMonths(current, -1))}>
                Prev
              </Button>
              <p className="min-w-[10rem] text-center text-sm font-semibold text-slate-800">{formatMonthHeading(calendarMonth)}</p>
              <Button variant="outline" size="sm" onClick={() => setCalendarMonth((current) => addMonths(current, 1))}>
                Next
              </Button>
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
              const events = dayEvents(day.isoDate, myRequests, workspace?.holidays ?? []);
              return (
                <div
                  key={day.isoDate}
                  className={[
                    "min-h-[7.6rem] rounded-xl border p-2",
                    day.isCurrentMonth
                      ? "border-blue-100 bg-white"
                      : "border-slate-200 bg-slate-50/75 text-slate-400",
                  ].join(" ")}
                >
                  <p className="text-xs font-semibold">{day.dateNumber}</p>
                  <div className="mt-1 space-y-1">
                    {events.slice(0, 2).map((event, index) => (
                      <div
                        key={`${day.isoDate}-${event.kind}-${index}`}
                        className={`truncate rounded-full border px-1.5 py-0.5 text-[10px] font-medium ${toneClassName(event.tone)}`}
                        title={event.label}
                      >
                        {event.label}
                      </div>
                    ))}
                    {events.length > 2 ? (
                      <div className="text-[10px] font-medium text-slate-500">+{events.length - 2} more</div>
                    ) : null}
                  </div>
                </div>
              );
            })}
          </div>

          <div className="flex flex-wrap gap-2 text-[11px] text-slate-600">
            <span className="inline-flex items-center gap-1 rounded-full border border-indigo-200 bg-indigo-50 px-2 py-0.5 text-indigo-700">
              <CalendarDays className="size-3" /> Holiday
            </span>
            <span className="inline-flex items-center gap-1 rounded-full border border-amber-200 bg-amber-50 px-2 py-0.5 text-amber-700">
              <Clock3 className="size-3" /> Pending PTO
            </span>
            <span className="inline-flex items-center gap-1 rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-emerald-700">
              <CheckCircle2 className="size-3" /> Approved PTO
            </span>
            <span className="inline-flex items-center gap-1 rounded-full border border-rose-200 bg-rose-50 px-2 py-0.5 text-rose-700">
              <XCircle className="size-3" /> Rejected PTO
            </span>
          </div>
        </SectionCard>

        <SectionLoadingLayer
          active={loadingContract.sectionOverlayActive}
          message={loadingContract.sectionOverlayMessage}
        />
      </div>

      {workspace?.isManager ? (
        <SectionCard
          title="Manager PTO"
          description="Direct employee requests and approval queue"
          contentClassName="space-y-4"
        >
          <div className="grid gap-3 md:grid-cols-3">
            <article className="rounded-2xl border border-blue-100 bg-white p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Direct reports</p>
              <p className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-slate-900">{workspace.directReports.length}</p>
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

          <div className="overflow-x-auto rounded-xl border border-blue-100">
            <table className="min-w-full text-left text-sm">
              <thead className="bg-blue-50/70 text-xs uppercase tracking-[0.08em] text-slate-600">
                <tr>
                  <th className="px-3 py-2.5">Employee</th>
                  <th className="px-3 py-2.5">Type</th>
                  <th className="px-3 py-2.5">Date range</th>
                  <th className="px-3 py-2.5">Status</th>
                  <th className="px-3 py-2.5">Hours</th>
                  <th className="px-3 py-2.5 text-right">Action</th>
                </tr>
              </thead>
              <tbody>
                {directReportRequests.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="px-3 py-6 text-center text-sm text-slate-600">
                      No direct employee requests yet.
                    </td>
                  </tr>
                ) : (
                  directReportRequests.map((request) => (
                    <tr key={request.id} className="border-t border-blue-100/80 bg-white text-slate-700">
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
                            setReviewTargetId(request.id);
                            setReviewNote(request.managerNote || "");
                          }}
                        >
                          <ShieldCheck className="size-4" />
                          Preview
                        </Button>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </SectionCard>
      ) : null}

      <Dialog open={isRequestDialogOpen} onOpenChange={setIsRequestDialogOpen}>
        <DialogContent className="max-w-2xl">
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label="Close submit PTO modal"
          />
          <DialogHeader>
            <DialogTitle>Submit PTO request</DialogTitle>
            <DialogDescription>
              Enter request details. Your manager can approve or reject from the Manager PTO queue.
            </DialogDescription>
          </DialogHeader>

          {requestFormError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
              {requestFormError}
            </div>
          ) : null}

          <div className="grid gap-3 sm:grid-cols-2">
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">PTO type</span>
              <AppDropdown
                value={requestForm.type}
                onValueChange={(value) => setRequestForm((current) => ({ ...current, type: value as LeaveSpherePtoType }))}
                options={PTO_TYPE_OPTIONS.map((item) => ({ value: item.value, label: item.label }))}
                searchable={false}
                disabled={isSubmitting}
              />
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Hours</span>
              <Input
                type="number"
                min={1}
                step={0.5}
                value={requestForm.hours}
                onChange={(event) => setRequestForm((current) => ({ ...current, hours: event.target.value }))}
                placeholder="8"
                disabled={isSubmitting}
              />
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Start date</span>
              <Input
                type="date"
                value={requestForm.startDate}
                onChange={(event) => setRequestForm((current) => ({ ...current, startDate: event.target.value }))}
                disabled={isSubmitting}
              />
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">End date</span>
              <Input
                type="date"
                value={requestForm.endDate}
                onChange={(event) => setRequestForm((current) => ({ ...current, endDate: event.target.value }))}
                disabled={isSubmitting}
              />
            </label>
          </div>

          <label className="space-y-1 text-sm">
            <span className="text-slate-600">Reason</span>
            <Textarea
              value={requestForm.reason}
              onChange={(event) => setRequestForm((current) => ({ ...current, reason: event.target.value }))}
              className="min-h-[120px]"
              placeholder="Add a short reason for this PTO request"
              disabled={isSubmitting}
            />
          </label>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setIsRequestDialogOpen(false)}
              disabled={isSubmitting}
            >
              Cancel
            </Button>
            <Button onClick={() => void handleSubmitRequest()} disabled={isSubmitting}>
              {isSubmitting ? "Submitting..." : "Submit request"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={Boolean(selectedReviewRequest)} onOpenChange={(open) => {
        if (!open) {
          setReviewTargetId(null);
          setReviewNote("");
        }
      }}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Manager request preview</DialogTitle>
            <DialogDescription>
              Review this direct employee PTO request and approve or reject.
            </DialogDescription>
          </DialogHeader>

          {selectedReviewRequest ? (
            <div className="space-y-3">
              <div className="rounded-xl border border-blue-100 bg-blue-50/40 p-3 text-sm text-slate-700">
                <p><span className="font-semibold text-slate-900">Employee:</span> {selectedReviewRequest.employeeName}</p>
                <p><span className="font-semibold text-slate-900">Type:</span> {requestTypeLabel(selectedReviewRequest.type)}</p>
                <p><span className="font-semibold text-slate-900">Range:</span> {formatDateLabel(selectedReviewRequest.startDate)} - {formatDateLabel(selectedReviewRequest.endDate)}</p>
                <p><span className="font-semibold text-slate-900">Hours:</span> {formatHoursLabel(selectedReviewRequest.hours)}</p>
                <p><span className="font-semibold text-slate-900">Reason:</span> {selectedReviewRequest.reason}</p>
                <p><span className="font-semibold text-slate-900">Status:</span> {statusLabel(selectedReviewRequest.status)}</p>
                {selectedReviewRequest.reviewerName ? (
                  <p>
                    <span className="font-semibold text-slate-900">Reviewed by:</span> {selectedReviewRequest.reviewerName}
                    {selectedReviewRequest.reviewedAt ? ` on ${formatDateLabel(selectedReviewRequest.reviewedAt)}` : ""}
                  </p>
                ) : null}
              </div>

              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Manager note</span>
                <Textarea
                  value={reviewNote}
                  onChange={(event) => setReviewNote(event.target.value)}
                  className="min-h-[100px]"
                  placeholder="Optional note for the employee"
                  disabled={isReviewing}
                />
              </label>
            </div>
          ) : null}

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setReviewTargetId(null);
                setReviewNote("");
              }}
              disabled={isReviewing}
            >
              Close
            </Button>
            {selectedReviewRequest?.status === "pending" ? (
              <>
                <Button
                  variant="outline"
                  className="border-rose-200 text-rose-700 hover:bg-rose-50"
                  onClick={() => void handleReviewRequest(false)}
                  disabled={isReviewing}
                >
                  Reject
                </Button>
                <Button
                  className="bg-emerald-600 text-white hover:bg-emerald-700"
                  onClick={() => void handleReviewRequest(true)}
                  disabled={isReviewing}
                >
                  Approve
                </Button>
              </>
            ) : null}
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
