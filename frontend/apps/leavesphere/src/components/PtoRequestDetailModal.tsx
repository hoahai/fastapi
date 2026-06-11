import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { X } from "lucide-react";

import { DateInputField } from "@tradsphere/components/dashboard/FlightDateRangeField";
import { AppDropdown } from "@tradsphere/components/ui/app-dropdown";
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
import { Input } from "@tradsphere/components/ui/input";
import { canModalClose, shouldBlockOutsideClose } from "@tradsphere/components/ui/modal-close-guard";
import { Textarea } from "@tradsphere/components/ui/textarea";
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";
import { LeaveSpherePtoStatusChip } from "@leavesphere/components/PtoStatusChip";
import { ModalShell } from "@shared/components";
import { useCommittedTextField } from "@shared/hooks/useCommittedTextField";

import type { LeaveSpherePtoRequest, LeaveSpherePtoStatus, LeaveSpherePtoType } from "@leavesphere/lib/ptoMocks";

export type LeaveSpherePtoRequestFormState = {
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: string;
  reason: string;
};

type LeaveSpherePtoRequestDetailModalProps = {
  mode?: "create" | "edit";
  layoutVariant?: "default" | "my-pto-detail";
  open: boolean;
  request: LeaveSpherePtoRequest | null;
  initialForm?: LeaveSpherePtoRequestFormState;
  title: string;
  description: string;
  ptoTypeOptions: Array<{ value: LeaveSpherePtoType; label: string }>;
  statusLabel: (status: LeaveSpherePtoStatus) => string;
  onOpenChange: (open: boolean) => void;
  onClose: () => void;
  onSubmit?: (params: {
    requestId: string | null;
    payload: {
      type: LeaveSpherePtoType;
      startDate: string;
      endDate: string;
      hours: number;
      reason: string;
    };
  }) => Promise<void | boolean> | void | boolean;
  onSave?: (params: {
    requestId: string | null;
    payload: {
      type: LeaveSpherePtoType;
      startDate: string;
      endDate: string;
      hours: number;
      reason: string;
    };
  }) => Promise<void | boolean> | void | boolean;
  onFormChange?: (form: LeaveSpherePtoRequestFormState) => void;
  calculateHours?: (startDate: string, endDate: string) => string;
  hoursHelperText?: string;
  readOnly?: boolean;
  canSubmitOverride?: boolean;
  saveLabel?: string;
  saving?: boolean;
  details?: ReactNode;
  extraContent?: ReactNode;
  footerActions?: ReactNode;
  externalDirty?: boolean;
  allowedDateRange?: {
    minDate: string;
    maxDate: string;
  };
};

const EMPTY_FORM: LeaveSpherePtoRequestFormState = {
  type: "vacation",
  startDate: "",
  endDate: "",
  hours: "",
  reason: "",
};

function createFormState(request: LeaveSpherePtoRequest | null): LeaveSpherePtoRequestFormState {
  const requestType = request?.type ?? "vacation";
  return {
    type: requestType === "vacation" || requestType === "sick" || requestType === "personal" || requestType === "floating"
      ? requestType
      : "vacation",
    startDate: request?.startDate ?? "",
    endDate: request?.endDate ?? "",
    hours: request ? String(request.hours) : "",
    reason: request?.reason ?? "",
  };
}

function isValidDateRange(startDate: string, endDate: string): boolean {
  return Boolean(startDate) && Boolean(endDate) && startDate <= endDate;
}

function isWithinRange(value: string, minDate?: string, maxDate?: string): boolean {
  if (!value) {
    return false;
  }
  if (minDate && value < minDate) {
    return false;
  }
  if (maxDate && value > maxDate) {
    return false;
  }
  return true;
}

function isNonNegativeNumber(value: string): boolean {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0;
}

function normalizeReason(value: string): string {
  return value.trim();
}

function formsEqual(left: LeaveSpherePtoRequestFormState, right: LeaveSpherePtoRequestFormState): boolean {
  return (
    left.type === right.type
    && left.startDate === right.startDate
    && left.endDate === right.endDate
    && Number(left.hours) === Number(right.hours)
    && normalizeReason(left.reason) === normalizeReason(right.reason)
  );
}

export function LeaveSpherePtoRequestDetailModal({
  mode = "edit",
  layoutVariant = "default",
  open,
  request,
  initialForm,
  title,
  description,
  ptoTypeOptions,
  statusLabel,
  onOpenChange,
  onClose,
  onSubmit,
  onSave,
  onFormChange,
  calculateHours,
  hoursHelperText,
  readOnly = false,
  canSubmitOverride,
  saveLabel = "Save changes",
  saving = false,
  details,
  extraContent,
  footerActions,
  externalDirty = false,
  allowedDateRange,
}: LeaveSpherePtoRequestDetailModalProps) {
  const isMyPtoDetailLayout = layoutVariant === "my-pto-detail";
  const sourceForm = useMemo(() => {
    if (mode === "edit" && request) {
      return createFormState(request);
    }
    if (initialForm) {
      return { ...initialForm };
    }
    return { ...EMPTY_FORM };
  }, [initialForm, mode, request]);

  const [form, setForm] = useState<LeaveSpherePtoRequestFormState>(sourceForm);
  const [baselineForm, setBaselineForm] = useState<LeaveSpherePtoRequestFormState>(sourceForm);
  const [formError, setFormError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const [openEndDatePickerSignal, setOpenEndDatePickerSignal] = useState(0);
  const wasOpenRef = useRef(false);
  const isHydratingFromOpenRef = useRef(false);
  const shouldAutoCalculateOnOpenRef = useRef(true);
  const hasHandledInitialAutoCalculateRef = useRef(false);
  const previousDateRangeRef = useRef<{ startDate: string; endDate: string } | null>(null);
  const reasonField = useCommittedTextField<HTMLTextAreaElement>(
    form.reason,
    (value) => setForm((current) => ({ ...current, reason: value })),
  );

  useEffect(() => {
    const didJustOpen = open && !wasOpenRef.current;
    wasOpenRef.current = open;
    if (!didJustOpen) {
      return;
    }
    isHydratingFromOpenRef.current = true;
    setForm(sourceForm);
    setBaselineForm(sourceForm);
    setFormError(null);
    shouldAutoCalculateOnOpenRef.current = mode === "create" || !sourceForm.hours.trim();
    hasHandledInitialAutoCalculateRef.current = false;
    previousDateRangeRef.current = {
      startDate: sourceForm.startDate,
      endDate: sourceForm.endDate,
    };
  }, [mode, open, sourceForm]);

  useEffect(() => {
    if (!calculateHours || !open) {
      return;
    }
    if (isHydratingFromOpenRef.current) {
      const hasHydratedFormValues = (
        form.type === sourceForm.type
        && form.startDate === sourceForm.startDate
        && form.endDate === sourceForm.endDate
        && form.hours === sourceForm.hours
        && form.reason === sourceForm.reason
      );
      if (!hasHydratedFormValues) {
        return;
      }
      isHydratingFromOpenRef.current = false;
    }
    const previousDateRange = previousDateRangeRef.current;
    const hasDateChanged = !previousDateRange
      || previousDateRange.startDate !== form.startDate
      || previousDateRange.endDate !== form.endDate;
    const shouldAutoCalculateOnFirstOpen = !hasHandledInitialAutoCalculateRef.current && shouldAutoCalculateOnOpenRef.current;

    hasHandledInitialAutoCalculateRef.current = true;
    previousDateRangeRef.current = {
      startDate: form.startDate,
      endDate: form.endDate,
    };

    if (!shouldAutoCalculateOnFirstOpen && !hasDateChanged) {
      return;
    }

    const nextHours = calculateHours(form.startDate, form.endDate);
    setForm((current) => {
      if (current.hours === nextHours) {
        return current;
      }
      return {
        ...current,
        hours: nextHours,
      };
    });
  }, [calculateHours, form, open, sourceForm]);

  useEffect(() => {
    onFormChange?.(form);
  }, [form, onFormChange]);

  const hasFormChanges = useMemo(
    () => !formsEqual(form, baselineForm),
    [baselineForm, form],
  );

  const hasUnsavedChanges = useMemo(
    () => hasFormChanges || externalDirty,
    [externalDirty, hasFormChanges],
  );

  const submitHandler = onSubmit ?? onSave;
  const hasValidCoreFields = Boolean(
    isValidDateRange(form.startDate, form.endDate)
    && isWithinRange(form.startDate, allowedDateRange?.minDate, allowedDateRange?.maxDate)
    && isWithinRange(form.endDate, allowedDateRange?.minDate, allowedDateRange?.maxDate)
    && isNonNegativeNumber(form.hours)
    && normalizeReason(form.reason),
  );
  const canSave = Boolean(
    submitHandler
    && hasValidCoreFields
    && hasFormChanges
    && (canSubmitOverride ?? true),
  );

  const shouldShowSubmitButton = Boolean(submitHandler && (canSave || saving));
  const endDateMin = form.startDate && allowedDateRange?.minDate
    ? (form.startDate > allowedDateRange.minDate ? form.startDate : allowedDateRange.minDate)
    : (form.startDate || allowedDateRange?.minDate);
  const reasonMinHeightClassName = isMyPtoDetailLayout ? "min-h-[110px]" : "min-h-[100px]";
  const formFields = (
    <>
      <div className="grid gap-3 sm:grid-cols-2">
        <label className="space-y-1 text-sm">
          <span className="text-slate-600">PTO type</span>
          <AppDropdown
            value={form.type}
            onValueChange={(value) => setForm((current) => ({ ...current, type: value as LeaveSpherePtoType }))}
            options={ptoTypeOptions}
            searchable={false}
            disabled={saving || readOnly}
          />
        </label>

        <label className="space-y-1 text-sm">
          <span className="text-slate-600">Hours</span>
          <Input
            type="number"
            min={0}
            step={0.5}
            value={form.hours}
            onChange={(event) => setForm((current) => ({ ...current, hours: event.target.value }))}
            disabled={saving || readOnly}
          />
          {hoursHelperText ? (
            <p className="text-[11px] text-slate-500">{hoursHelperText}</p>
          ) : null}
        </label>

        <label className="space-y-1 text-sm">
          <span className="text-slate-600">Start date</span>
          <DateInputField
            id="pto-detail-start-date"
            value={form.startDate}
            label="Start date"
            onChange={(value) => {
              setForm((current) => ({
                ...current,
                startDate: value,
                endDate: current.endDate && value > current.endDate ? value : current.endDate,
              }));
              setOpenEndDatePickerSignal((current) => current + 1);
            }}
            disabled={saving || readOnly}
            minDate={allowedDateRange?.minDate}
            maxDate={allowedDateRange?.maxDate}
          />
        </label>

        <label className="space-y-1 text-sm">
          <span className="text-slate-600">End date</span>
          <DateInputField
            id="pto-detail-end-date"
            value={form.endDate}
            label="End date"
            onChange={(value) => setForm((current) => ({ ...current, endDate: value }))}
            disabled={saving || readOnly}
            minDate={endDateMin}
            maxDate={allowedDateRange?.maxDate}
            openCalendarSignal={openEndDatePickerSignal}
          />
        </label>
      </div>

      <label className={isMyPtoDetailLayout ? "block space-y-1 text-sm" : "space-y-1 text-sm"}>
        <span className="text-slate-600">Reason</span>
        <Textarea
          value={reasonField.value}
          onChange={reasonField.onChange}
          onBlur={reasonField.onBlur}
          className={reasonMinHeightClassName}
          disabled={saving || readOnly}
        />
      </label>
    </>
  );

  function closeModal() {
    onOpenChange(false);
    onClose();
  }

  function handleDialogOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy: saving,
      hasUnsavedChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasUnsavedChanges && !saving) {
        setIsDiscardDialogOpen(true);
      }
      return;
    }
    if (!nextOpen) {
      closeModal();
      return;
    }
    onOpenChange(true);
  }

  async function handleSave() {
    if (!submitHandler) {
      return;
    }
    if (!isValidDateRange(form.startDate, form.endDate)) {
      setFormError("Start date must be on or before end date.");
      return;
    }
    if (
      !isWithinRange(form.startDate, allowedDateRange?.minDate, allowedDateRange?.maxDate)
      || !isWithinRange(form.endDate, allowedDateRange?.minDate, allowedDateRange?.maxDate)
    ) {
      setFormError("Start and end dates must be within the loaded year.");
      return;
    }
    if (!isNonNegativeNumber(form.hours)) {
      setFormError("Hours must be 0 or greater.");
      return;
    }
    if (!normalizeReason(form.reason)) {
      setFormError("Reason is required.");
      return;
    }

    const submittedForm: LeaveSpherePtoRequestFormState = {
      type: form.type,
      startDate: form.startDate,
      endDate: form.endDate,
      hours: String(Number(form.hours)),
      reason: normalizeReason(form.reason),
    };

    setFormError(null);
    const didSave = await submitHandler({
      requestId: request?.id ?? null,
      payload: {
        type: submittedForm.type,
        startDate: submittedForm.startDate,
        endDate: submittedForm.endDate,
        hours: Number(submittedForm.hours),
        reason: submittedForm.reason,
      },
    });
    if (didSave === false) {
      return;
    }
    setForm(submittedForm);
    setBaselineForm(submittedForm);
    closeModal();
  }

  function handleRevertChanges() {
    if (!hasUnsavedChanges || saving || readOnly) {
      return;
    }
    setForm(baselineForm);
    setFormError(null);
  }

  return (
    <Dialog open={open} onOpenChange={handleDialogOpenChange}>
      <DialogContent
        className={`${isMyPtoDetailLayout ? "max-w-3xl" : "max-w-2xl"} flex max-h-[90vh] flex-col overflow-hidden rounded-xl bg-white p-6`}
        onInteractOutside={(event) => {
          if (shouldBlockOutsideClose({ isBusy: saving, hasUnsavedChanges })) {
            event.preventDefault();
          }
        }}
      >
        <ModalShell busy={saving} busyMessage="Saving PTO request..." className="min-h-0 flex-1">
          <DialogClose
            className="absolute right-4 top-4 z-20 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close PTO request modal"
          >
            <X className="size-4" />
          </DialogClose>

          <DialogHeader>
            <DialogTitle>{title}</DialogTitle>
            <DialogDescription>{description}</DialogDescription>
          </DialogHeader>

          {isMyPtoDetailLayout ? (
            <div className="mt-4 space-y-4">
              {(details || request) ? (
                <div className="rounded-xl border border-slate-200 bg-slate-50/70 px-4 py-3">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div className="min-w-0 flex-1">
                      {details}
                    </div>
                    {request ? (
                      <LeaveSpherePtoStatusChip
                        status={request.status}
                        label={statusLabel(request.status)}
                        className="px-2.5 py-1 text-xs"
                      />
                    ) : null}
                  </div>
                </div>
              ) : null}

              <div className="space-y-3">
                {formFields}
              </div>

              {extraContent ? (
                <div className="border-t border-slate-200 pt-3">
                  {extraContent}
                </div>
              ) : null}
            </div>
          ) : (
            <div className="mt-4 space-y-4">
              {details}
              <div className="space-y-3">{formFields}</div>

              {request ? (
                <div className="rounded-xl border border-slate-200 bg-slate-50/70 px-3 py-2 text-xs text-slate-600">
                  Status:
                  {" "}
                  <LeaveSpherePtoStatusChip status={request.status} label={statusLabel(request.status)} className="font-semibold" />
                </div>
              ) : null}

              {extraContent}
            </div>
          )}

          {formError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
              {formError}
            </div>
          ) : null}

          <DialogFooter className="gap-2">
            {footerActions}
            <div className="flex items-center gap-2">
              {hasUnsavedChanges ? (
                <Button variant="outline" onClick={handleRevertChanges} disabled={saving}>
                  Revert
                </Button>
              ) : null}
              {shouldShowSubmitButton ? (
                <Button onClick={() => void handleSave()} disabled={saving || !canSave}>
                  {saving ? "Saving..." : saveLabel}
                </Button>
              ) : null}
            </div>
          </DialogFooter>
        </ModalShell>
      </DialogContent>

      <UnsavedChangesDialog
        open={isDiscardDialogOpen}
        onKeepEditing={() => {
          setIsDiscardDialogOpen(false);
        }}
        onDiscardChanges={() => {
          setIsDiscardDialogOpen(false);
          closeModal();
        }}
      />
    </Dialog>
  );
}
