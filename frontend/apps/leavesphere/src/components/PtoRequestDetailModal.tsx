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
  DialogHeader,
  DialogTitle,
} from "@tradsphere/components/ui/dialog";
import { Input } from "@tradsphere/components/ui/input";
import { Textarea } from "@tradsphere/components/ui/textarea";
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";
import { LeaveSpherePtoStatusChip } from "@leavesphere/components/PtoStatusChip";
import { ModalActionFooter, ModalCloseButton, ModalHeaderRow, ModalShell } from "@shared/components";
import { useCommittedTextField, useGuardedModalDialog, useModalDraftState } from "@shared/hooks";
import { formatLeaveSpherePtoStatusLabel } from "@leavesphere/lib/ptoStatus";

import type { LeaveSpherePtoRequest, LeaveSpherePtoType } from "@leavesphere/lib/ptoTypes";

export type LeaveSpherePtoRequestFormState = {
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: string;
  description: string;
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
  onOpenChange: (open: boolean) => void;
  onClose: () => void;
  onSubmit?: (params: {
    requestId: string | null;
      payload: {
        type: LeaveSpherePtoType;
        startDate: string;
        endDate: string;
        hours: number;
        description: string;
      };
  }) => Promise<void | boolean> | void | boolean;
  onSave?: (params: {
    requestId: string | null;
      payload: {
        type: LeaveSpherePtoType;
        startDate: string;
        endDate: string;
        hours: number;
        description: string;
      };
  }) => Promise<void | boolean> | void | boolean;
  onFormChange?: (form: LeaveSpherePtoRequestFormState) => void;
  calculateHours?: (startDate: string, endDate: string) => string;
  validateSubmit?: (form: LeaveSpherePtoRequestFormState) => string | null;
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
  description: "",
};

function createFormState(request: LeaveSpherePtoRequest | null): LeaveSpherePtoRequestFormState {
  return {
    type: request?.ptoTypeCode ?? request?.type ?? "vacation",
    startDate: request?.startDate ?? "",
    endDate: request?.endDate ?? "",
    hours: request ? String(request.hours) : "",
    description: request?.description ?? "",
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

function isPositiveNumber(value: string): boolean {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0;
}

function normalizeDescription(value: string): string {
  return value.trim();
}

function formsEqual(left: LeaveSpherePtoRequestFormState, right: LeaveSpherePtoRequestFormState): boolean {
  return (
    left.type === right.type
    && left.startDate === right.startDate
    && left.endDate === right.endDate
    && Number(left.hours) === Number(right.hours)
    && normalizeDescription(left.description) === normalizeDescription(right.description)
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
  onOpenChange,
  onClose,
  onSubmit,
  onSave,
  onFormChange,
  calculateHours,
  validateSubmit,
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

  const {
    draft: form,
    setDraft: setForm,
    hasChanges: hasFormChanges,
    resetDraft: revertDraft,
    commitDraft,
  } = useModalDraftState({
    open,
    sourceValue: sourceForm,
    isEqual: formsEqual,
    onOpen: (nextSourceForm) => {
      isHydratingFromOpenRef.current = true;
      setFormError(null);
      shouldAutoCalculateOnOpenRef.current = mode === "create" || !nextSourceForm.hours.trim();
      hasHandledInitialAutoCalculateRef.current = false;
      hasManualHoursOverrideRef.current = false;
      previousDateRangeRef.current = {
        startDate: nextSourceForm.startDate,
        endDate: nextSourceForm.endDate,
      };
    },
    onChange: onFormChange,
  });
  const hasUnsavedChanges = hasFormChanges || externalDirty;
  const [formError, setFormError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const [openEndDatePickerSignal, setOpenEndDatePickerSignal] = useState(0);
  const isHydratingFromOpenRef = useRef(false);
  const shouldAutoCalculateOnOpenRef = useRef(true);
  const hasHandledInitialAutoCalculateRef = useRef(false);
  const hasManualHoursOverrideRef = useRef(false);
  const previousDateRangeRef = useRef<{ startDate: string; endDate: string } | null>(null);
  const descriptionInput = useCommittedTextField<HTMLTextAreaElement>(
    form.description,
    (value) => setForm((current) => ({ ...current, description: value })),
  );
  const {
    closeModal,
    handleDialogOpenChange,
    handleInteractOutside,
  } = useGuardedModalDialog({
    isBusy: saving,
    hasUnsavedChanges,
    onOpenChange,
    onClose,
    onDiscardAttempt: () => {
      setIsDiscardDialogOpen(true);
    },
  });

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
        && form.description === sourceForm.description
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

    if (hasManualHoursOverrideRef.current || (!shouldAutoCalculateOnFirstOpen && !hasDateChanged)) {
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

  const submitHandler = onSubmit ?? onSave;
  const submitValidationError = validateSubmit?.(form) ?? null;
  const isHoursInvalid = Boolean(submitValidationError);
  const hasValidCoreFields = Boolean(
    isValidDateRange(form.startDate, form.endDate)
    && isWithinRange(form.startDate, allowedDateRange?.minDate, allowedDateRange?.maxDate)
    && isWithinRange(form.endDate, allowedDateRange?.minDate, allowedDateRange?.maxDate)
    && isPositiveNumber(form.hours)
    && normalizeDescription(form.description),
  );
  const canSave = Boolean(
    submitHandler
    && hasValidCoreFields
    && !submitValidationError
    && hasFormChanges
    && (canSubmitOverride ?? true),
  );

  const shouldShowSubmitButton = Boolean(submitHandler && (canSave || saving));
  const endDateMin = form.startDate && allowedDateRange?.minDate
    ? (form.startDate > allowedDateRange.minDate ? form.startDate : allowedDateRange.minDate)
    : (form.startDate || allowedDateRange?.minDate);
  const descriptionMinHeightClassName = isMyPtoDetailLayout ? "min-h-[110px]" : "min-h-[100px]";
  const requestStatusChip = request ? (
    <LeaveSpherePtoStatusChip
      status={request.status}
      label={formatLeaveSpherePtoStatusLabel(request.status)}
      className={isMyPtoDetailLayout ? "px-2.5 py-1 text-xs" : "font-semibold"}
    />
  ) : null;

  useEffect(() => {
    if (!formError) {
      return;
    }
    if (!hasValidCoreFields || submitValidationError) {
      return;
    }
    setFormError(null);
  }, [formError, hasValidCoreFields, submitValidationError]);

  const topFields = (
    <div className="grid gap-4 sm:grid-cols-2">
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
          onChange={(event) => {
            hasManualHoursOverrideRef.current = true;
            setForm((current) => ({ ...current, hours: event.target.value }));
          }}
          disabled={saving || readOnly}
          className={isHoursInvalid ? "border-rose-300 text-rose-900 focus-visible:border-rose-400 focus-visible:shadow-[inset_0_0_0_1px_rgba(244,63,94,0.15)]" : undefined}
        />
        {submitValidationError ? (
          <p className="text-[11px] font-medium text-rose-600">{submitValidationError}</p>
        ) : null}
      </label>

      <label className="space-y-1 text-sm">
        <span className="text-slate-600">Start date</span>
        <DateInputField
          id="pto-detail-start-date"
          value={form.startDate}
          label="Start date"
          onChange={(value) => {
            if (!isWithinRange(value, allowedDateRange?.minDate, allowedDateRange?.maxDate)) {
              return;
            }
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
          restrictMonthNavigation={Boolean(allowedDateRange)}
        />
      </label>

      <label className="space-y-1 text-sm">
        <span className="text-slate-600">End date</span>
        <DateInputField
          id="pto-detail-end-date"
          value={form.endDate}
          label="End date"
          onChange={(value) => {
            if (!isWithinRange(value, allowedDateRange?.minDate, allowedDateRange?.maxDate)) {
              return;
            }
            setForm((current) => ({ ...current, endDate: value }));
          }}
          disabled={saving || readOnly}
          minDate={endDateMin}
          maxDate={allowedDateRange?.maxDate}
          openCalendarSignal={openEndDatePickerSignal}
          restrictMonthNavigation={Boolean(allowedDateRange)}
        />
      </label>
    </div>
  );
  const descriptionField = (
    <label className={`${isMyPtoDetailLayout ? "block" : ""} space-y-1 text-sm`}>
      <span className="text-slate-600">Description</span>
      <Textarea
        value={descriptionInput.value}
        onChange={descriptionInput.onChange}
        onBlur={descriptionInput.onBlur}
        className={descriptionMinHeightClassName}
        disabled={saving || readOnly}
      />
    </label>
  );
  const detailsBlock = isMyPtoDetailLayout ? (
    <div className="space-y-4">
      {(details || request) ? (
        <div className="rounded-xl border border-slate-200 bg-slate-50/70 px-4 py-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0 flex-1">
              {details}
            </div>
            {requestStatusChip}
          </div>
        </div>
      ) : null}

      <div className="space-y-4">
        {topFields}
        <div className="mt-6">
          {descriptionField}
        </div>
      </div>

      {extraContent ? (
        <div className="border-t border-slate-200 pt-3">
          {extraContent}
        </div>
      ) : null}
    </div>
  ) : (
    <div className="space-y-4">
      {details}
      <div className="space-y-4">
        {topFields}
      </div>

      <div className="mt-6">
        {descriptionField}
      </div>

      {request ? (
        <div className="rounded-xl border border-slate-200 bg-slate-50/70 px-3 py-2 text-xs text-slate-600">
          Status:
          {" "}
          {requestStatusChip}
        </div>
      ) : null}

      {extraContent}
    </div>
  );

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
    if (!isPositiveNumber(form.hours)) {
      setFormError("Hours must be greater than zero.");
      return;
    }
    if (!normalizeDescription(form.description)) {
      setFormError("Description is required.");
      return;
    }
    if (submitValidationError) {
      setFormError(submitValidationError);
      return;
    }

    const submittedForm: LeaveSpherePtoRequestFormState = {
      type: form.type,
      startDate: form.startDate,
      endDate: form.endDate,
      hours: String(Number(form.hours)),
      description: normalizeDescription(form.description),
    };

    setFormError(null);
    const didSave = await submitHandler({
      requestId: request?.id ?? null,
      payload: {
        type: submittedForm.type,
        startDate: submittedForm.startDate,
        endDate: submittedForm.endDate,
        hours: Number(submittedForm.hours),
        description: submittedForm.description,
      },
    });
    if (didSave === false) {
      return;
    }
    commitDraft(submittedForm);
    closeModal();
  }

  function handleRevertChanges() {
    if (!hasUnsavedChanges || saving || readOnly) {
      return;
    }
    revertDraft();
    setFormError(null);
  }

  return (
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          className={`${isMyPtoDetailLayout ? "max-w-3xl" : "max-w-2xl"} flex max-h-[90vh] flex-col overflow-visible rounded-xl bg-white p-6`}
          onInteractOutside={handleInteractOutside}
        >
        <ModalShell
          busy={saving}
          busyMessage="Saving PTO request..."
          className="min-h-0 flex-1"
        >
          <ModalHeaderRow
            actions={(
              <DialogClose
                asChild
                aria-label="Close PTO request modal"
              >
                <ModalCloseButton icon={<X className="size-4" />} />
              </DialogClose>
            )}
          >
            <DialogHeader>
              <DialogTitle>{title}</DialogTitle>
              <DialogDescription>{description}</DialogDescription>
            </DialogHeader>
          </ModalHeaderRow>

          <div className="mt-4">
            {detailsBlock}
          </div>

          {formError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
              {formError}
            </div>
          ) : null}

          <ModalActionFooter className="mt-6 gap-2">
            {footerActions}
            <div className="flex items-center gap-2">
              {mode !== "create" && hasUnsavedChanges ? (
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
          </ModalActionFooter>
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
