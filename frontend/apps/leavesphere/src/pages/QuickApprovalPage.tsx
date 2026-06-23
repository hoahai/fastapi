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
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return value;
  }
  return formatDateInTimeZone(value, DEFAULT_TIME_ZONE, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
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
  const [decisionSuccess, setDecisionSuccess] = useState<LeaveSphereQuickApprovalDecision | null>(null);
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
  const [closeCountdown, setCloseCountdown] = useState<number | null>(null);
  const [pendingDecisionAction, setPendingDecisionAction] = useState<LeaveSphereQuickApprovalDecision | null>(null);
  const [decisionNote, setDecisionNote] = useState("");

  useEffect(() => {
    let cancelled = false;
    async function run(): Promise<void> {
      setPageState("loading");
      setPreview(null);
      setDecisionSuccess(null);
      setStateMessage(null);
      setInfoMessage(null);
      setHandledNote(null);
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

  useEffect(() => {
    if (pageState !== "success" || !decisionSuccess) {
      setCloseCountdown(null);
      return;
    }
    let remaining = 5;
    setCloseCountdown(remaining);
    const intervalId = window.setInterval(() => {
      remaining -= 1;
      if (remaining <= 0) {
        setCloseCountdown(0);
        window.clearInterval(intervalId);
        window.close();
        return;
      }
      setCloseCountdown(remaining);
    }, 1000);
    return () => {
      window.clearInterval(intervalId);
    };
  }, [decisionSuccess, pageState]);

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
        setDecisionSuccess(decision);
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
      <main className="relative mx-auto flex min-h-[100dvh] w-full max-w-4xl flex-col justify-center gap-5 px-4 py-8 sm:px-6 lg:px-8">
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

        {(recipientName || recipientRole || recipientPictureUrl) && (pageState === "ready" || pageState === "already_handled") ? (
          <section
            className="rounded-[1.35rem] border border-slate-200/90 bg-slate-50/80 px-4 py-3.5 text-sm text-slate-700 shadow-sm"
            aria-label={recipientEmail ? `Quick approval for ${recipientEmail}` : "Quick approval recipient details"}
          >
            <div className="flex min-h-[4rem] items-center gap-3">
              <div className="flex size-10 shrink-0 items-center justify-center overflow-hidden rounded-full border border-slate-200 bg-white">
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
                <p className="mt-1 text-sm leading-6 text-slate-600">
                  Below is the request waiting for your approval.
                </p>
              </div>
            </div>
          </section>
        ) : null}

        {(pageState === "ready" || pageState === "already_handled") && preview ? (
          <SectionCard
            title="Request preview"
            description={previewDescription}
            className="rounded-[1.55rem]"
          >
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="rounded-xl border border-blue-100/90 bg-blue-50/55 p-4">
                <p className="text-xs font-semibold uppercase tracking-[0.14em] text-blue-700/85">Employee</p>
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
              <div className="rounded-xl border border-blue-100/90 bg-blue-50/55 p-4">
                <p className="text-xs font-semibold uppercase tracking-[0.14em] text-blue-700/85">PTO type</p>
                <div className="mt-2">
                  <LeaveSpherePtoTypeChip type={preview.ptoTypeCode ?? preview.ptoTypeLabel} label={preview.ptoTypeLabel} />
                </div>
              </div>
              <div className="rounded-xl border border-blue-100/90 bg-blue-50/55 p-4 sm:col-span-2">
                <p className="text-xs font-semibold uppercase tracking-[0.14em] text-blue-700/85">Date range</p>
                <p className="mt-2 flex items-center gap-2 text-sm font-semibold text-slate-900">
                  <CalendarRange className="size-4 text-blue-700" />
                  <span>
                    {formatDateLabel(preview.startDate)} to {formatDateLabel(preview.endDate)}
                  </span>
                </p>
              </div>
              <div className="rounded-xl border border-blue-100/90 bg-blue-50/55 p-4">
                <p className="text-xs font-semibold uppercase tracking-[0.14em] text-blue-700/85">Requested time</p>
                <p className="mt-2 text-sm font-semibold text-slate-900">
                  {formatHours(preview.hoursRequested)}
                </p>
              </div>
              <div className="rounded-xl border border-blue-100/90 bg-blue-50/55 p-4">
                <p className="text-xs font-semibold uppercase tracking-[0.14em] text-blue-700/85">Current status</p>
                <div className="mt-2">
                  <LeaveSpherePtoStatusChip
                    status={preview.currentStatus}
                    label={formatLeaveSpherePtoStatusLabel(preview.currentStatus)}
                  />
                </div>
              </div>
              <div className="rounded-xl border border-blue-100/90 bg-blue-50/55 p-4 sm:col-span-2">
                <p className="text-xs font-semibold uppercase tracking-[0.14em] text-blue-700/85">Reason or note</p>
                <p className="mt-2 text-sm text-slate-700">{preview.reason || "No reason provided."}</p>
              </div>
              {pageState === "already_handled" ? (
                <div className="rounded-xl border border-slate-200/90 bg-slate-50/70 p-4 sm:col-span-2">
                  <p className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-600">
                    {handledNoteLabel(recipientRole)}
                  </p>
                  <p className="mt-2 whitespace-pre-wrap text-sm leading-6 text-slate-700">
                    {handledNote || "No note was recorded."}
                  </p>
                </div>
              ) : null}
            </div>

            {pageState === "ready" && canAct ? (
              <div className="mt-6 flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
                <Button
                  variant="outline"
                  onClick={() => setPendingDecisionAction("rejected")}
                  disabled={isSubmitting}
                  className="border-rose-200 text-rose-700 hover:border-rose-300 hover:bg-rose-50/80 hover:text-rose-800"
                >
                  {isSubmitting ? <Loader2 className="size-4 animate-spin" /> : <XCircle className="size-4" />}
                  Reject request
                </Button>
                <Button
                  variant="outline"
                  className="border-emerald-300 text-emerald-700 hover:bg-emerald-50"
                  onClick={() => setPendingDecisionAction("approved")}
                  disabled={isSubmitting}
                >
                  {isSubmitting ? <Loader2 className="size-4 animate-spin" /> : <CheckCircle2 className="size-4" />}
                  Approve request
                </Button>
              </div>
            ) : pageState === "ready" ? (
              <div className="mt-6 rounded-xl border border-amber-200 bg-amber-50/80 px-4 py-3 text-sm text-amber-900">
                This link is preview-only. The request details are shown, but approval actions are disabled for this recipient.
              </div>
            ) : null}
          </SectionCard>
        ) : null}

        {pageState !== "ready" ? (
          <section className={`rounded-[1.55rem] p-6 shadow-soft sm:p-7 ${handledTheme.cardClassName}`}>
            <div className="flex items-start gap-3">
              <div className={`rounded-full p-2.5 ${handledTheme.iconWrapperClassName}`}>
                {pageState === "success" ? <CheckCircle2 className="size-5 text-emerald-600" /> : null}
                {pageState === "invalid" ? <XCircle className="size-5 text-rose-600" /> : null}
                {pageState === "expired" ? <Clock3 className="size-5 text-amber-600" /> : null}
                {pageState === "already_handled" ? <ShieldCheck className={handledTheme.iconClassName} /> : null}
                {pageState === "error" ? <AlertTriangle className="size-5 text-rose-700" /> : null}
                {pageState === "loading" ? <Loader2 className="size-5 animate-spin text-blue-700" /> : null}
              </div>
              <div className="min-w-0">
                <h2 className={`text-lg font-semibold ${handledTheme.titleClassName}`}>
                  {pageState === "success" ? handledTheme.title : "Quick approval status"}
                </h2>
                <p className={`mt-1 text-sm leading-6 ${handledTheme.textClassName}`}>
                  {pageState === "success" && decisionSuccess
                    ? `This PTO request is now ${decisionLabel(decisionSuccess)}.`
                    : stateMessage || fallbackStateMessage(pageState)}
                </p>
                {pageState === "already_handled" && handledDecision ? (
                  <p className="mt-2 text-xs font-medium text-slate-600">
                    Existing decision: <span className={`font-semibold ${handledTheme.decisionClassName}`}>{decisionLabel(handledDecision)}</span>
                  </p>
                ) : null}
                {pageState === "success" ? (
                  <p className="mt-3 text-xs text-slate-600">
                    This tab will close automatically in {closeCountdown ?? 5} second
                    {(closeCountdown ?? 5) === 1 ? "" : "s"}.
                  </p>
                ) : null}
              </div>
            </div>
          </section>
        ) : null}

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
