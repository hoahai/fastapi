import { useEffect, useMemo, useState } from "react";
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
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@tradsphere/components/ui/dialog";
import { Textarea } from "@tradsphere/components/ui/textarea";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { PageLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { ModalShell } from "@shared/components";
import { DEFAULT_TIME_ZONE, formatDateInTimeZone } from "@shared/utils/time";
import { LeaveSpherePtoTypeChip } from "@leavesphere/components/PtoTypeChip";
import { LeaveSpherePtoStatusChip } from "@leavesphere/components/PtoStatusChip";
import { formatLeaveSpherePtoStatusLabel } from "@leavesphere/lib/ptoStatus";
import {
  loadLeaveSphereQuickApproval,
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

function formatDays(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "-";
  }
  if (Number.isInteger(value)) {
    return `${value} day${value === 1 ? "" : "s"}`;
  }
  return `${value.toFixed(1)} days`;
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

export default function LeaveSphereQuickApprovalPage({ token }: LeaveSphereQuickApprovalPageProps) {
  const [pageState, setPageState] = useState<PageState>("loading");
  const [preview, setPreview] = useState<LeaveSphereQuickApprovalPreview | null>(null);
  const [handledDecision, setHandledDecision] = useState<LeaveSphereQuickApprovalDecision | null>(null);
  const [decisionSuccess, setDecisionSuccess] = useState<LeaveSphereQuickApprovalDecision | null>(null);
  const [stateMessage, setStateMessage] = useState<string | null>(null);
  const [infoMessage, setInfoMessage] = useState<string | null>(null);
  const [canAct, setCanAct] = useState(true);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [rejectDialogOpen, setRejectDialogOpen] = useState(false);
  const [rejectReason, setRejectReason] = useState("");

  useEffect(() => {
    let cancelled = false;
    async function run(): Promise<void> {
      setPageState("loading");
      setPreview(null);
      setDecisionSuccess(null);
      setStateMessage(null);
      setInfoMessage(null);
      setCanAct(true);
      try {
        const result = await loadLeaveSphereQuickApproval(token);
        if (cancelled) {
          return;
        }
        setInfoMessage(result.message);
        setHandledDecision(result.handledDecision);
        setCanAct(result.canAct);
        if (result.state === "ready" && result.preview) {
          setPreview(result.preview);
          setPageState("ready");
          return;
        }
        setPageState(result.state);
        setStateMessage(result.message);
      } catch {
        if (cancelled) {
          return;
        }
        setPageState("error");
        setStateMessage("We couldn't validate this link. Please try again in a moment.");
      }
    }
    void run();
    return () => {
      cancelled = true;
    };
  }, [token]);

  async function submitDecision(decision: LeaveSphereQuickApprovalDecision, reason?: string): Promise<void> {
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
      }
    } catch {
      setPageState("error");
      setStateMessage("We couldn't submit your decision right now. Please retry.");
    } finally {
      setIsSubmitting(false);
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

        {pageState === "ready" && preview ? (
          <SectionCard
            title="Request preview"
            description="Validate details before approving or rejecting."
            className="rounded-[1.55rem]"
          >
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="rounded-xl border border-blue-100/90 bg-blue-50/55 p-4">
                <p className="text-xs font-semibold uppercase tracking-[0.14em] text-blue-700/85">Employee</p>
                <p className="mt-2 flex items-center gap-2 text-sm font-semibold text-slate-900">
                  <UserRound className="size-4 text-blue-700" />
                  <span>{preview.employeeName}</span>
                </p>
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
                  {formatHours(preview.hoursRequested)} ({formatDays(preview.daysRequested)})
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
            </div>

            {canAct ? (
              <div className="mt-6 flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
                <Button
                  variant="outline"
                  onClick={() => setRejectDialogOpen(true)}
                  disabled={isSubmitting}
                  className="border-rose-200 text-rose-700 hover:border-rose-300 hover:bg-rose-50/80 hover:text-rose-800"
                >
                  {isSubmitting ? <Loader2 className="size-4 animate-spin" /> : <XCircle className="size-4" />}
                  Reject request
                </Button>
                <Button
                  onClick={() => void submitDecision("approved")}
                  disabled={isSubmitting}
                  className="bg-emerald-600 text-white hover:bg-emerald-600/95"
                >
                  {isSubmitting ? <Loader2 className="size-4 animate-spin" /> : <CheckCircle2 className="size-4" />}
                  Approve request
                </Button>
              </div>
            ) : (
              <div className="mt-6 rounded-xl border border-amber-200 bg-amber-50/80 px-4 py-3 text-sm text-amber-900">
                This link is preview-only. The request details are shown, but approval actions are disabled for this recipient.
              </div>
            )}
          </SectionCard>
        ) : null}

        {pageState !== "ready" ? (
          <section className="rounded-[1.55rem] border border-blue-100/90 bg-white/95 p-6 shadow-soft sm:p-7">
            <div className="flex items-start gap-3">
              <div className="rounded-full border border-blue-100 bg-blue-50 p-2.5">
                {pageState === "success" ? <CheckCircle2 className="size-5 text-emerald-600" /> : null}
                {pageState === "invalid" ? <XCircle className="size-5 text-rose-600" /> : null}
                {pageState === "expired" ? <Clock3 className="size-5 text-amber-600" /> : null}
                {pageState === "already_handled" ? <ShieldCheck className="size-5 text-blue-700" /> : null}
                {pageState === "error" ? <AlertTriangle className="size-5 text-rose-700" /> : null}
                {pageState === "loading" ? <Loader2 className="size-5 animate-spin text-blue-700" /> : null}
              </div>
              <div className="min-w-0">
                <h2 className="text-lg font-semibold text-slate-900">
                  {pageState === "success" ? "Decision submitted" : "Quick approval status"}
                </h2>
                <p className="mt-1 text-sm leading-6 text-slate-700">
                  {pageState === "success" && decisionSuccess
                    ? `This PTO request is now ${decisionLabel(decisionSuccess)}.`
                    : stateMessage || fallbackStateMessage(pageState)}
                </p>
                {pageState === "already_handled" && handledDecision ? (
                  <p className="mt-2 text-xs font-medium text-slate-600">
                    Existing decision: <span className="font-semibold text-slate-800">{decisionLabel(handledDecision)}</span>
                  </p>
                ) : null}
                {pageState === "success" ? (
                  <p className="mt-3 text-xs text-slate-600">You can close this tab now.</p>
                ) : null}
              </div>
            </div>
          </section>
        ) : null}
      </main>

      <PageLoadingLayer active={pageState === "loading"} message="Validating quick approval link..." />

      <Dialog open={rejectDialogOpen} onOpenChange={setRejectDialogOpen}>
        <DialogContent className="flex max-h-[90vh] max-w-xl flex-col overflow-hidden rounded-xl bg-white p-6">
          <ModalShell busy={isSubmitting} busyMessage="Submitting decision..." className="min-h-0 flex-1">
            <DialogHeader>
              <DialogTitle>Reject PTO request</DialogTitle>
              <DialogDescription>
                Optionally provide a note so the employee understands why this request was rejected.
              </DialogDescription>
            </DialogHeader>
            <div className="mt-4">
              <label htmlFor="quick-reject-note" className="mb-2 block text-xs font-semibold uppercase tracking-[0.12em] text-slate-600">
                Rejection note (optional)
              </label>
              <Textarea
                id="quick-reject-note"
                value={rejectReason}
                onChange={(event) => setRejectReason(event.target.value)}
                placeholder="Add context for the employee"
                maxLength={500}
              />
            </div>
            <DialogFooter>
              <DialogClose asChild>
                <Button variant="outline">Cancel</Button>
              </DialogClose>
              <Button
                disabled={isSubmitting}
                className="bg-rose-600 text-white hover:bg-rose-600/95"
                onClick={() => {
                  void submitDecision("rejected", rejectReason.trim() || undefined);
                  setRejectDialogOpen(false);
                  setRejectReason("");
                }}
              >
                {isSubmitting ? <Loader2 className="size-4 animate-spin" /> : <XCircle className="size-4" />}
                Confirm rejection
              </Button>
            </DialogFooter>
          </ModalShell>
        </DialogContent>
      </Dialog>
    </div>
  );
}
