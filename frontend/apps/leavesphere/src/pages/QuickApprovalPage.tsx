import { useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle,
  CalendarRange,
  CheckCircle2,
  Clock3,
  Loader2,
  ShieldCheck,
  UserRound,
  XCircle,
} from "lucide-react";

import { Button } from "@tradsphere/components/ui/button";
import { ConfirmDialog } from "@tradsphere/components/ui/confirm-dialog";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { PageLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { DEFAULT_TIME_ZONE, formatDateInTimeZone } from "@shared/utils/time";
import { LeaveSpherePtoEmployeeHeader } from "@leavesphere/components/LeaveSpherePtoEmployeeHeader";
import { LeaveSpherePtoTypeChip } from "@leavesphere/components/PtoTypeChip";
import { LeaveSpherePtoStatusChip } from "@leavesphere/components/PtoStatusChip";
import { formatLeaveSpherePtoStatusLabel } from "@leavesphere/lib/ptoStatus";
import { getLeaveSphereApprovalActionConfirmCopy } from "@leavesphere/lib/approvalActionConfirm";
import {
  loadLeaveSphereQuickApproval,
  refreshLeaveSphereQuickApproval,
  submitLeaveSphereQuickApprovalDecision,
  type LeaveSphereQuickApprovalDecision,
  type LeaveSphereQuickApprovalPreview,
} from "@leavesphere/lib/quickApproval";

type LeaveSphereQuickApprovalPageProps = {
  token: string;
};

type PageState = "loading" | "ready" | "success" | "invalid" | "expired" | "already_handled" | "error";

function formatDateLabel(value: string): string {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return "-";
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    return formatDateInTimeZone(normalized, DEFAULT_TIME_ZONE, {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  }
  const parsed = Date.parse(normalized);
  if (Number.isNaN(parsed)) {
    return normalized;
  }
  try {
    return new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      timeZone: DEFAULT_TIME_ZONE,
    }).format(new Date(parsed));
  } catch {
    return normalized;
  }
}

function decisionLabel(decision: LeaveSphereQuickApprovalDecision): string {
  return decision === "approved" ? "Approved" : "Rejected";
}

function formatHours(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "-";
  }
  if (Number.isInteger(value)) {
    return `${value}h`;
  }
  return `${value.toFixed(1)}h`;
}

function fallbackStateMessage(state: PageState): string {
  if (state === "loading") {
    return "Validating quick approval link...";
  }
  if (state === "invalid") {
    return "This quick approval link is invalid.";
  }
  if (state === "expired") {
    return "This quick approval link has expired.";
  }
  if (state === "already_handled") {
    return "This request has already been handled.";
  }
  if (state === "success") {
    return "Decision recorded successfully.";
  }
  return "We couldn't process this quick approval link right now.";
}

function recipientRoleLabel(role: "manager" | "admin" | null): string {
  if (role === "admin") {
    return "Admin";
  }
  if (role === "manager") {
    return "Manager";
  }
  return "Recipient";
}

function handledNoteLabel(role: "manager" | "admin" | null): string {
  if (role === "admin") {
    return "Admin note";
  }
  if (role === "manager") {
    return "Manager note";
  }
  return "Approver note";
}

function timeOfDayGreeting(date = new Date()): string {
  const hour = date.getHours();
  if (hour < 12) {
    return "Good morning";
  }
  if (hour < 18) {
    return "Good afternoon";
  }
  return "Good evening";
}

function handledStatusTheme(decision: LeaveSphereQuickApprovalDecision | null): {
  cardClassName: string;
  iconClassName: string;
  iconWrapperClassName: string;
  title: string;
  titleClassName: string;
  textClassName: string;
  decisionClassName: string;
} {
  if (decision === "approved") {
    return {
      cardClassName: "border-emerald-200 bg-emerald-50/80",
      iconClassName: "size-5 text-emerald-700",
      iconWrapperClassName: "border-emerald-100 bg-emerald-50",
      title: "Decision submitted",
      titleClassName: "text-emerald-950",
      textClassName: "text-emerald-900/90",
      decisionClassName: "text-emerald-900",
    };
  }
  if (decision === "rejected") {
    return {
      cardClassName: "border-rose-200 bg-rose-50/80",
      iconClassName: "size-5 text-rose-700",
      iconWrapperClassName: "border-rose-100 bg-rose-50",
      title: "Decision submitted",
      titleClassName: "text-rose-950",
      textClassName: "text-rose-900/90",
      decisionClassName: "text-rose-900",
    };
  }
  return {
    cardClassName: "border-blue-100/90 bg-white/95",
    iconClassName: "size-5 text-blue-700",
    iconWrapperClassName: "border-blue-100 bg-blue-50",
    title: "Quick approval status",
    titleClassName: "text-slate-900",
    textClassName: "text-slate-700",
    decisionClassName: "text-slate-800",
  };
}

export default function LeaveSphereQuickApprovalPage({ token }: LeaveSphereQuickApprovalPageProps) {
  const [pageState, setPageState] = useState<PageState>("loading");
  const [preview, setPreview] = useState<LeaveSphereQuickApprovalPreview | null>(null);
  const [handledDecision, setHandledDecision] = useState<LeaveSphereQuickApprovalDecision | null>(null);
  const [handledNote, setHandledNote] = useState<string | null>(null);
  const [handledAt, setHandledAt] = useState<string | null>(null);
  const [stateMessage, setStateMessage] = useState<string | null>(null);
  const [infoMessage, setInfoMessage] = useState<string | null>(null);
  const [canAct, setCanAct] = useState(true);
  const [recipientRole, setRecipientRole] = useState<"manager" | "admin" | null>(null);
  const [recipientEmail, setRecipientEmail] = useState<string | null>(null);
  const [recipientName, setRecipientName] = useState<string | null>(null);
  const [recipientPictureUrl, setRecipientPictureUrl] = useState<string | null>(null);
  const [recipientImageError, setRecipientImageError] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const isActionInFlightRef = useRef(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [pendingDecisionAction, setPendingDecisionAction] = useState<LeaveSphereQuickApprovalDecision | null>(null);
  const [decisionNote, setDecisionNote] = useState("");

  function closeQuickApprovalTab(): void {
    // Browsers only allow closing tabs that were opened in a closable context.
    // We still try the most compatible path first, then fall back to a blank page.
    window.open("", "_self", "");
    window.close();
    window.setTimeout(() => {
      if (!window.closed) {
        window.location.replace("about:blank");
      }
    }, 50);
  }

  useEffect(() => {
    let cancelled = false;
    async function run(): Promise<void> {
      setPageState("loading");
      setPreview(null);
      setStateMessage(null);
      setInfoMessage(null);
      setHandledNote(null);
      setHandledAt(null);
      setCanAct(true);
      setRecipientRole(null);
      setRecipientEmail(null);
      setRecipientName(null);
      setRecipientPictureUrl(null);
      setRecipientImageError(false);
      setIsRefreshing(false);
      try {
        const result = await loadLeaveSphereQuickApproval(token);
        if (cancelled) {
          return;
        }
        setInfoMessage(result.message);
        setHandledDecision(result.handledDecision);
        setHandledNote(result.handledNote);
        setHandledAt(result.handledAt ?? null);
        setCanAct(result.canAct);
        setRecipientRole(result.recipientRole);
        setRecipientEmail(result.recipientEmail);
        setRecipientName(result.recipientName);
        setRecipientPictureUrl(result.recipientPictureUrl);
        if (result.preview) {
          setPreview(result.preview);
        } else {
          setPreview(null);
        }
        setPageState(result.state);
        setStateMessage(result.message);
        if (result.state === "ready" || result.state === "already_handled") {
          setIsRefreshing(true);
          void refreshLiveRequest();
        }
      } catch {
        if (cancelled) {
          return;
        }
        setPageState("error");
        setStateMessage("We couldn't validate this link. Please try again in a moment.");
      }
    }

    async function refreshLiveRequest(): Promise<void> {
      try {
        const result = await refreshLeaveSphereQuickApproval(token);
        if (cancelled || isActionInFlightRef.current) {
          return;
        }
        setInfoMessage(result.message);
        setHandledDecision(result.handledDecision);
        setHandledNote(result.handledNote);
        setHandledAt(result.handledAt ?? null);
        setCanAct(result.canAct);
        setRecipientRole(result.recipientRole);
        setRecipientEmail(result.recipientEmail);
        setRecipientName(result.recipientName);
        setRecipientPictureUrl(result.recipientPictureUrl);
        if (result.preview) {
          setPreview(result.preview);
        } else {
          setPreview(null);
        }
        setPageState(result.state);
        if (result.state === "ready" || result.state === "already_handled") {
          setStateMessage(null);
          return;
        }
        setStateMessage(result.message);
      } catch {
        if (cancelled || isActionInFlightRef.current) {
          return;
        }
        setStateMessage("We couldn't refresh this request right now. The snapshot is still shown.");
      } finally {
        if (!cancelled) {
          setIsRefreshing(false);
        }
      }
    }

    void run();
    return () => {
      cancelled = true;
    };
  }, [token]);

  useEffect(() => {
    setRecipientImageError(false);
  }, [recipientPictureUrl]);

  async function submitDecision(decision: LeaveSphereQuickApprovalDecision, reason?: string): Promise<void> {
    isActionInFlightRef.current = true;
    setIsSubmitting(true);
    setStateMessage(null);
    try {
      const result = await submitLeaveSphereQuickApprovalDecision({
        token,
        decision,
        reason,
      });
      setInfoMessage(result.message);
      if (result.state === "success") {
        setPageState("success");
        return;
      }
      setPageState(result.state);
      setStateMessage(result.message);
      if (result.state === "already_handled") {
        setHandledDecision(null);
        setHandledNote(null);
      }
    } catch {
      setPageState("error");
      setStateMessage("We couldn't submit your decision right now. Please retry.");
    } finally {
      setIsSubmitting(false);
      isActionInFlightRef.current = false;
    }
  }

  const pageMessages = useMemo<StackMessage[]>(() => {
    const messages: StackMessage[] = [];
    if (infoMessage) {
      messages.push({
        variant: "info",
        title: "Quick approval preview mode",
        message: infoMessage,
      });
    }
    if (stateMessage && (pageState === "error" || pageState === "invalid" || pageState === "expired" || pageState === "already_handled")) {
      messages.push({
        variant: pageState === "error" ? "error" : "warning",
        message: stateMessage,
      });
    }
    return messages;
  }, [infoMessage, pageState, stateMessage]);
  const recipientGreetingLabel = recipientName || recipientRoleLabel(recipientRole);
  const greetingText = timeOfDayGreeting();
  const previewDescription =
    pageState === "already_handled"
      ? "This request has already been handled. Review the recorded details below."
      : "Validate details before approving or rejecting.";
  const handledTheme = handledStatusTheme(handledDecision);
  const handledDateLabel = handledAt ? formatDateLabel(handledAt) : null;
  const handledReviewerLabel = recipientName || recipientRoleLabel(recipientRole);
  const isHandled = pageState === "already_handled";
  const isReadyForReview = pageState === "ready";
  const statusPanelClassName = isHandled
    ? handledTheme.cardClassName
    : canAct
      ? "border-cyan-200/80 bg-cyan-50/85"
      : "border-amber-200/80 bg-amber-50/85";
  const statusIconClassName = isHandled
    ? handledTheme.iconClassName
    : canAct
      ? "size-5 text-cyan-700"
      : "size-5 text-amber-700";
  const statusIconWrapperClassName = isHandled
    ? handledTheme.iconWrapperClassName
    : canAct
      ? "border-cyan-100 bg-cyan-50"
      : "border-amber-100 bg-amber-50";
  const statusTitleClassName = isHandled ? handledTheme.titleClassName : "text-slate-900";
  const statusTextClassName = isHandled ? handledTheme.textClassName : "text-slate-700";
  const statusTitle = isHandled
    ? handledTheme.title
    : canAct
      ? "Ready for review"
      : "Preview only";
  const statusBody = isHandled
    ? "Processed successfully."
    : canAct
      ? "Review the details below and take action when you are ready."
      : "The request details are visible, but approval actions are disabled for this link.";
  const pendingDecisionCopy = pendingDecisionAction
    ? getLeaveSphereApprovalActionConfirmCopy(
      pendingDecisionAction === "approved" ? "approve" : "reject",
      recipientRole === "admin" ? "admin" : "manager",
    )
    : null;
  const pendingDecisionNoteRequired = Boolean(pendingDecisionCopy?.noteRequired);

  return (
    <div className="relative min-h-screen overflow-hidden bg-app-gradient text-foreground">
      <div className="pointer-events-none absolute -left-20 top-0 size-80 rounded-full bg-blue-200/40 blur-3xl" />
      <div className="pointer-events-none absolute right-0 top-16 size-80 rounded-full bg-cyan-200/30 blur-3xl" />
      <div className="pointer-events-none absolute left-[46%] top-5 size-72 rounded-full bg-indigo-200/30 blur-3xl" />
      <main className="relative mx-auto flex min-h-[100dvh] w-full max-w-6xl flex-col gap-5 px-4 py-8 sm:px-6 lg:px-8">
        <section className="relative overflow-hidden rounded-[1.7rem] border border-blue-100/90 bg-white/95 shadow-soft">
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_82%_24%,rgba(59,130,246,0.34),transparent_40%),radial-gradient(circle_at_56%_24%,rgba(129,140,248,0.38),transparent_46%),radial-gradient(circle_at_12%_30%,rgba(79,70,229,0.3),transparent_44%),linear-gradient(140deg,#f7faff_4%,#e9f1ff_45%,#f5f9ff_100%)]" />
          <div className="relative px-6 py-7 sm:px-8 sm:py-8">
            <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-indigo-700/85">LeaveSphere</p>
            <h1 className="mt-2 text-[1.8rem] font-semibold tracking-[-0.024em] text-slate-900 sm:text-[2.2rem]">
              Quick PTO approval
            </h1>
            <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-700">
              Review this PTO request and submit your decision securely from the email link.
            </p>
          </div>
        </section>

        {pageMessages.length ? <PageMessageStack messages={pageMessages} /> : null}
        <div className="grid gap-5 lg:grid-cols-[minmax(18rem,20rem)_minmax(0,1fr)]">
          <aside className="flex flex-col gap-4">
            {(recipientName || recipientRole || recipientPictureUrl) && (pageState === "ready" || pageState === "already_handled") ? (
              <section
                className="rounded-[1.35rem] border border-slate-200/90 bg-white/95 px-4 py-3.5 text-sm text-slate-700 shadow-sm"
                aria-label={recipientEmail ? `Quick approval for ${recipientEmail}` : "Quick approval recipient details"}
              >
                <div className="flex min-h-[4rem] items-center gap-3">
                  <div className="flex size-11 shrink-0 items-center justify-center overflow-hidden rounded-full border border-slate-200 bg-white shadow-[0_1px_2px_rgba(15,23,42,0.06)]">
                    {recipientPictureUrl && !recipientImageError ? (
                      <img
                        src={recipientPictureUrl}
                        alt={recipientGreetingLabel}
                        className="size-full object-cover"
                        loading="lazy"
                        onError={() => setRecipientImageError(true)}
                      />
                    ) : (
                      <UserRound className="size-5 text-slate-500" />
                    )}
                  </div>
                  <div className="min-w-0">
                    <p className="text-base leading-6 text-slate-900">
                      <span>{greetingText}, </span>
                      <span className="font-semibold text-slate-950">{recipientGreetingLabel}</span>
                    </p>
                  </div>
                </div>
              </section>
            ) : null}

            <section className={`rounded-[1.45rem] border p-5 shadow-soft ${statusPanelClassName}`}>
              <div className="flex items-start gap-3">
                <div className={`rounded-2xl border p-3 ${statusIconWrapperClassName}`}>
                  {isHandled ? <ShieldCheck className={statusIconClassName} /> : null}
                  {!isHandled && canAct ? <Clock3 className={statusIconClassName} /> : null}
                  {!isHandled && !canAct ? <AlertTriangle className={statusIconClassName} /> : null}
                </div>
                <div className="min-w-0">
                  <h2 className={`text-lg font-semibold ${statusTitleClassName}`}>{statusTitle}</h2>
                  <p className={`mt-1 text-sm leading-6 ${statusTextClassName}`}>
                    {isHandled
                      ? statusBody
                      : stateMessage || fallbackStateMessage(pageState)}
                  </p>
                </div>
              </div>

              {isHandled ? (
                <div className="mt-5 rounded-2xl border border-slate-200/80 bg-white/75 p-4">
                  <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Decision history</p>
                  <dl className="mt-4 space-y-3">
                    <div className="flex items-center justify-between gap-3 border-b border-slate-200/70 pb-3">
                      <dt className="text-sm text-slate-500">Action</dt>
                      <dd className={`text-sm font-semibold ${handledTheme.decisionClassName}`}>
                        {handledDecision ? decisionLabel(handledDecision) : "Handled"}
                      </dd>
                    </div>
                    <div className="flex items-center justify-between gap-3 border-b border-slate-200/70 pb-3">
                      <dt className="text-sm text-slate-500">Date</dt>
                      <dd className="text-sm font-medium text-slate-900">{handledDateLabel || "—"}</dd>
                    </div>
                    <div className="flex items-center justify-between gap-3">
                      <dt className="text-sm text-slate-500">Reviewer</dt>
                      <dd className="text-sm font-medium text-slate-900">{handledReviewerLabel}</dd>
                    </div>
                  </dl>
                </div>
              ) : null}

              {!isHandled && isReadyForReview ? (
                <div className="mt-5 rounded-2xl border border-slate-200/80 bg-white/75 p-4">
                  <p className="text-sm leading-6 text-slate-700">
                    This request is waiting for your decision.
                  </p>
                  {canAct ? (
                    <div className="mt-4 flex flex-col gap-2">
                      <Button
                        variant="outline"
                        className="justify-center border-emerald-300 text-emerald-700 hover:bg-emerald-50"
                        onClick={() => setPendingDecisionAction("approved")}
                        disabled={isSubmitting}
                      >
                        {isSubmitting ? <Loader2 className="size-4 animate-spin" /> : <CheckCircle2 className="size-4" />}
                        Approve request
                      </Button>
                      <Button
                        variant="outline"
                        className="justify-center border-rose-200 text-rose-700 hover:border-rose-300 hover:bg-rose-50/80 hover:text-rose-800"
                        onClick={() => setPendingDecisionAction("rejected")}
                        disabled={isSubmitting}
                      >
                        {isSubmitting ? <Loader2 className="size-4 animate-spin" /> : <XCircle className="size-4" />}
                        Reject request
                      </Button>
                    </div>
                  ) : (
                    <p className="mt-4 rounded-xl border border-amber-200 bg-amber-50/80 px-3 py-2 text-sm text-amber-900">
                      This link is preview-only. The request details are shown, but approval actions are disabled for this recipient.
                    </p>
                  )}
                </div>
              ) : null}

              {isHandled ? (
                <div className="mt-5 flex flex-col gap-3">
                  <p className="px-2 text-center text-sm leading-6 text-slate-600">
                    This request has already been handled. No further action is required from your side.
                  </p>
                  <Button
                    variant="outline"
                    onClick={closeQuickApprovalTab}
                    className="justify-center border-slate-300 text-slate-700 hover:bg-slate-50"
                  >
                    Close tab
                  </Button>
                </div>
              ) : null}
            </section>
          </aside>

          <SectionCard
            title="Request preview"
            description={previewDescription}
            className="rounded-[1.55rem] border-slate-200/80 bg-white/95 shadow-sm"
            contentClassName="mt-5"
          >
            {preview ? (
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="rounded-[1.15rem] border border-slate-200/80 bg-slate-50/80 p-4">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-blue-700">Employee</p>
                  <div className="mt-3">
                    <LeaveSpherePtoEmployeeHeader
                      employeeName={preview.employeeName}
                      pictureUrl={preview.pictureUrl}
                      title={preview.employeeName}
                      className="items-center"
                      titleClassName="truncate font-semibold"
                    />
                  </div>
                </div>
                <div className="rounded-[1.15rem] border border-slate-200/80 bg-slate-50/80 p-4">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-blue-700">PTO type</p>
                  <div className="mt-3">
                    <LeaveSpherePtoTypeChip type={preview.ptoTypeCode ?? preview.ptoTypeLabel} label={preview.ptoTypeLabel} />
                  </div>
                </div>
                <div className="rounded-[1.15rem] border border-slate-200/80 bg-slate-50/80 p-4 sm:col-span-2">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-blue-700">Date range</p>
                  <p className="mt-3 flex items-center gap-3 text-sm font-semibold text-slate-900">
                    <span className="inline-flex size-10 shrink-0 items-center justify-center rounded-xl bg-blue-50 text-blue-700">
                      <CalendarRange className="size-4" />
                    </span>
                    <span>
                      {formatDateLabel(preview.startDate)} to {formatDateLabel(preview.endDate)}
                    </span>
                  </p>
                </div>
                <div className="rounded-[1.15rem] border border-slate-200/80 bg-slate-50/80 p-4">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-blue-700">Requested time</p>
                  <div className="mt-3 flex items-end gap-2">
                    <span className="text-[2.15rem] font-semibold leading-none tracking-[-0.04em] text-slate-900">
                      {formatHours(preview.hoursRequested).replace(/h$/, "")}
                    </span>
                    <span className="pb-1 text-sm font-medium text-slate-500">hours</span>
                  </div>
                </div>
                <div className="rounded-[1.15rem] border border-slate-200/80 bg-slate-50/80 p-4">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-blue-700">Current status</p>
                  <div className="mt-3">
                    <LeaveSpherePtoStatusChip
                      status={preview.currentStatus}
                      label={formatLeaveSpherePtoStatusLabel(preview.currentStatus)}
                    />
                  </div>
                </div>
                <div className="rounded-[1.15rem] border border-slate-200/80 bg-slate-50/80 p-4 sm:col-span-2">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-blue-700">Description</p>
                  <div className="mt-3 rounded-[1rem] border border-slate-200/80 bg-white/90 p-4">
                    <p className="text-sm leading-6 text-slate-700 italic">
                      {preview.reason || "No reason provided."}
                    </p>
                  </div>
                </div>
                {isHandled ? (
                  <div className="rounded-[1.15rem] border border-slate-200/80 bg-slate-50/80 p-4 sm:col-span-2">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-blue-700">
                      {handledNoteLabel(recipientRole)}
                    </p>
                    <div className="mt-3 rounded-[1rem] border border-slate-200/80 bg-white/90 p-4">
                      <p className="whitespace-pre-wrap text-sm leading-6 text-slate-700 italic">
                        {handledNote || "No note was recorded."}
                      </p>
                    </div>
                  </div>
                ) : null}
              </div>
            ) : (
              <div className="rounded-[1.25rem] border border-slate-200/80 bg-slate-50/80 p-5">
                <p className="text-sm font-medium text-slate-900">
                  {pageState === "loading" ? "Request details are loading." : "Request details are unavailable."}
                </p>
                <p className="mt-1 text-sm leading-6 text-slate-600">
                  {pageState === "loading"
                    ? "We are still reconciling the live request. The snapshot should appear shortly."
                    : "We could not load the request snapshot for this link."}
                </p>
              </div>
            )}
          </SectionCard>
        </div>

        {pageState === "ready" && isRefreshing ? (
          <div className="flex items-start gap-3 rounded-xl border border-slate-200 bg-slate-50/90 px-4 py-3 text-sm text-slate-700">
            <Loader2 className="mt-0.5 size-4 shrink-0 animate-spin text-slate-500" />
            <div className="min-w-0">
              <p className="font-medium text-slate-900">Refreshing live request</p>
              <p className="mt-1 leading-6 text-slate-600">
                Showing the email snapshot now. We are rechecking the live request in the background.
              </p>
            </div>
          </div>
        ) : null}
      </main>

      <PageLoadingLayer active={pageState === "loading"} message="Validating quick approval link..." />

      <ConfirmDialog
        open={Boolean(pendingDecisionAction)}
        title={pendingDecisionCopy?.title ?? ""}
        description={pendingDecisionCopy?.description ?? ""}
        confirmLabel={pendingDecisionCopy?.confirmLabel ?? "Confirm"}
        cancelLabel="Go back"
        onCancel={() => {
          setPendingDecisionAction(null);
          setDecisionNote("");
        }}
        onConfirm={() => {
          if (!pendingDecisionAction) {
            return;
          }
          const note = decisionNote.trim() || undefined;
          void submitDecision(pendingDecisionAction, note);
          setPendingDecisionAction(null);
          setDecisionNote("");
        }}
        note={pendingDecisionAction ? {
          label: pendingDecisionCopy?.noteLabel ?? "Approver note / reason",
          value: decisionNote,
          onChange: setDecisionNote,
          placeholder: "Add context for the employee",
          disabled: isSubmitting,
          required: pendingDecisionNoteRequired,
          helpText: pendingDecisionCopy?.noteHelpText,
        } : undefined}
      />
    </div>
  );
}
