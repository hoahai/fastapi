import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { X } from "lucide-react";

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
import { ConfirmDialog } from "@tradsphere/components/ui/confirm-dialog";
import { canModalClose, shouldBlockOutsideClose } from "@tradsphere/components/ui/modal-close-guard";
import { Textarea } from "@tradsphere/components/ui/textarea";
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";
import { ModalCloseButton, ModalHeaderRow, ModalShell } from "@shared/components";

import type { LeaveSpherePtoType } from "@leavesphere/lib/ptoTypes";
import type {
  LeaveManagementActionCode,
  LeaveManagementLoadRequest,
} from "@leavesphere/lib/leaveManagementBalanceLedger";

type PtoTypeOption = {
  value: LeaveSpherePtoType;
  label: string;
};

export type LeaveSpherePtoLoadHoursFormState = {
  employeeId: string;
  ptoTypeCode: LeaveSpherePtoType;
  ptoActionCode: LeaveManagementActionCode;
  transactionId: string;
  hours: string;
  description: string;
};

export type LeaveSpherePtoLoadHoursSubmitParams = {
  requestId: string | null;
  payload: {
    employeeId: string;
    ptoTypeCode: LeaveSpherePtoType;
    ptoActionCode: LeaveManagementActionCode;
    transactionId: string | null;
    hours: number;
    year: number;
    status: "Approved";
    description: string;
    approverNote: string;
  };
};

type LeaveSpherePtoLoadHoursModalProps = {
  mode?: "create" | "edit";
  open: boolean;
  request: LeaveManagementLoadRequest | null;
  initialForm?: Partial<LeaveSpherePtoLoadHoursFormState>;
  title: string;
  description: string;
  employeeOptions: Array<{ value: string; label: string }>;
  getPtoTypeOptions: (employeeId: string) => PtoTypeOption[];
  onOpenChange: (open: boolean) => void;
  onClose: () => void;
  onFormChange?: (form: LeaveSpherePtoLoadHoursFormState) => void;
  onSubmit?: (params: LeaveSpherePtoLoadHoursSubmitParams) => Promise<void | boolean> | void | boolean;
  year: number | null;
  saveLabel?: string;
  saving?: boolean;
  footerActions?: ReactNode;
};

const EMPTY_FORM: LeaveSpherePtoLoadHoursFormState = {
  employeeId: "",
  ptoTypeCode: "vacation",
  ptoActionCode: "load_grant",
  transactionId: "",
  hours: "",
  description: "",
};

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeOptionalValue(value: string): string {
  return value.trim();
}

function isPositiveOrZeroNumber(value: string): boolean {
  if (value === "") {
    return false;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0;
}

function formsEqual(
  left: LeaveSpherePtoLoadHoursFormState,
  right: LeaveSpherePtoLoadHoursFormState,
): boolean {
  return (
    left.employeeId === right.employeeId
    && left.ptoTypeCode === right.ptoTypeCode
    && left.ptoActionCode === right.ptoActionCode
    && left.transactionId === right.transactionId
    && Number(left.hours) === Number(right.hours)
    && normalizeOptionalValue(left.description) === normalizeOptionalValue(right.description)
  );
}

function buildFormState(params: {
  mode: "create" | "edit";
  request: LeaveManagementLoadRequest | null;
  initialForm?: Partial<LeaveSpherePtoLoadHoursFormState>;
  employeeOptions: Array<{ value: string; label: string }>;
  getPtoTypeOptions: (employeeId: string) => PtoTypeOption[];
}): LeaveSpherePtoLoadHoursFormState {
  if (params.mode === "edit" && params.request) {
    return {
      employeeId: params.request.employeeId,
      ptoTypeCode: params.request.ptoTypeCode,
      ptoActionCode: params.request.ptoActionCode,
      transactionId: params.request.id,
      hours: String(params.request.hours),
      description: params.request.description ?? "",
    };
  }

  const initialEmployeeId = asString(params.initialForm?.employeeId) || params.employeeOptions[0]?.value || "";
  const employeeTypeOptions = params.getPtoTypeOptions(initialEmployeeId);
  const ptoTypeCode = asString(params.initialForm?.ptoTypeCode) || employeeTypeOptions[0]?.value || "vacation";

  return {
    ...EMPTY_FORM,
    employeeId: initialEmployeeId,
    ptoTypeCode,
    ptoActionCode: params.initialForm?.ptoActionCode || "load_grant",
    transactionId: asString(params.initialForm?.transactionId),
    hours: asString(params.initialForm?.hours),
    description: asString(params.initialForm?.description),
  };
}

export function LeaveSpherePtoLoadHoursModal({
  mode = "create",
  open,
  request,
  initialForm,
  title,
  description,
  employeeOptions,
  getPtoTypeOptions,
  onOpenChange,
  onClose,
  onFormChange,
  onSubmit,
  year,
  saveLabel,
  saving = false,
  footerActions,
}: LeaveSpherePtoLoadHoursModalProps) {
  const sourceForm = useMemo(() => buildFormState({
    mode,
    request,
    initialForm,
    employeeOptions,
    getPtoTypeOptions,
  }), [employeeOptions, getPtoTypeOptions, initialForm, mode, request]);

  const [form, setForm] = useState<LeaveSpherePtoLoadHoursFormState>(sourceForm);
  const [baselineForm, setBaselineForm] = useState<LeaveSpherePtoLoadHoursFormState>(sourceForm);
  const [formError, setFormError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const [isSaveDialogOpen, setIsSaveDialogOpen] = useState(false);
  const [saveNote, setSaveNote] = useState("");
  const wasOpenRef = useRef(false);
  const lastEmittedFormRef = useRef<LeaveSpherePtoLoadHoursFormState | null>(null);

  useEffect(() => {
    const didJustOpen = open && !wasOpenRef.current;
    wasOpenRef.current = open;
    if (!didJustOpen) {
      return;
    }
    setForm(sourceForm);
    setBaselineForm(sourceForm);
    setFormError(null);
    setIsDiscardDialogOpen(false);
    setIsSaveDialogOpen(false);
    setSaveNote(asString(request?.approverNote));
    lastEmittedFormRef.current = sourceForm;
  }, [open, request?.approverNote, sourceForm]);

  useEffect(() => {
    if (!open) {
      return;
    }
    if (mode === "edit") {
      return;
    }
    setForm((current) => {
      const nextEmployeeId = current.employeeId || employeeOptions[0]?.value || "";
      const employeeTypeOptions = getPtoTypeOptions(nextEmployeeId);
      const nextPtoTypeCode = employeeTypeOptions.some((item) => item.value === current.ptoTypeCode)
        ? current.ptoTypeCode
        : (employeeTypeOptions[0]?.value || "vacation");
      if (current.employeeId === nextEmployeeId && current.ptoTypeCode === nextPtoTypeCode) {
        return current;
      }
      return {
        ...current,
        employeeId: nextEmployeeId,
        ptoTypeCode: nextPtoTypeCode,
      };
    });
  }, [employeeOptions, getPtoTypeOptions, mode, open]);

  const hasFormChanges = useMemo(
    () => !formsEqual(form, baselineForm),
    [baselineForm, form],
  );

  useEffect(() => {
    if (!open) {
      return;
    }
    if (lastEmittedFormRef.current && formsEqual(lastEmittedFormRef.current, form)) {
      return;
    }
    lastEmittedFormRef.current = { ...form };
    onFormChange?.(form);
  }, [form, onFormChange, open]);

  const submitValidationError = useMemo(() => {
    if (!form.employeeId) {
      return "Employee is required.";
    }
    if (!isPositiveOrZeroNumber(form.hours)) {
      return "Hours must be zero or greater.";
    }
    if (!normalizeOptionalValue(form.description)) {
      return "Description is required.";
    }
    if (!Number.isInteger(year ?? NaN)) {
      return "Load a year before recording PTO hours.";
    }
    if (mode === "edit" && !form.transactionId) {
      return "Select a load request to adjust.";
    }
    return null;
  }, [form.description, form.employeeId, form.hours, form.transactionId, mode, year]);

  const canSave = Boolean(onSubmit && !submitValidationError && hasFormChanges);
  const shouldShowSubmitButton = Boolean(canSave || saving);
  const isEditMode = mode === "edit";
  const submitButtonLabel = saveLabel ?? (isEditMode ? "Save changes" : "Load Hours");
  const saveConfirmCopy = useMemo(() => ({
    title: isEditMode ? "Save PTO Hours?" : "Load PTO Hours?",
    description: isEditMode
      ? "This will update the approved PTO load request and save the note below."
      : "This will create the approved PTO load request. Add an admin note below if needed.",
    confirmLabel: isEditMode ? "Save changes" : "Load Hours",
    noteHelpText: "Optional. Add a note or reason for this PTO hour change.",
    placeholder: isEditMode
      ? "Add a note or reason for this PTO hour change"
      : "Optional. Add a note or reason for this PTO hour change",
  }), [isEditMode]);
  const descriptionMinHeightClassName = "min-h-[88px]";

  useEffect(() => {
    if (!formError) {
      return;
    }
    if (!submitValidationError) {
      setFormError(null);
    }
  }, [formError, submitValidationError]);

  function closeModal() {
    onOpenChange(false);
    onClose();
  }

  function handleDialogOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy: saving,
      hasUnsavedChanges: hasFormChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasFormChanges && !saving) {
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
    if (!onSubmit) {
      return;
    }
    if (submitValidationError) {
      setFormError(submitValidationError);
      return;
    }
    setFormError(null);
    setIsSaveDialogOpen(true);
  }

  async function handleConfirmSave() {
    if (!onSubmit) {
      setIsSaveDialogOpen(false);
      return;
    }
    if (submitValidationError) {
      setFormError(submitValidationError);
      return;
    }

    const submittedForm = {
      employeeId: form.employeeId,
      ptoTypeCode: form.ptoTypeCode,
      ptoActionCode: form.ptoActionCode,
      transactionId: form.transactionId || null,
      hours: Number(form.hours),
      year: Number(year),
      status: "Approved" as const,
      description: normalizeOptionalValue(form.description),
      approverNote: normalizeOptionalValue(saveNote),
    };

    setFormError(null);
    setIsSaveDialogOpen(false);
    let didSave: void | boolean;
    try {
      didSave = await onSubmit({
        requestId: request?.id ?? null,
        payload: submittedForm,
      });
    } catch {
      didSave = false;
    }
    if (didSave === false) {
      return;
    }
    const committedForm: LeaveSpherePtoLoadHoursFormState = {
      employeeId: form.employeeId,
      ptoTypeCode: form.ptoTypeCode,
      ptoActionCode: form.ptoActionCode,
      transactionId: form.transactionId,
      hours: String(Number(form.hours)),
      description: normalizeOptionalValue(form.description),
    };
    setForm(committedForm);
    setBaselineForm(committedForm);
    closeModal();
  }

  function handleRevertChanges() {
    if (!hasFormChanges || saving) {
      return;
    }
    setForm(baselineForm);
    setFormError(null);
  }

  const employeeFieldOptions = employeeOptions;
  const ptoTypeFieldOptions = getPtoTypeOptions(form.employeeId);

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          className="flex max-h-[90vh] max-w-xl flex-col overflow-hidden rounded-xl bg-white p-6"
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy: saving, hasUnsavedChanges: hasFormChanges })) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={saving} busyMessage="Saving PTO hours..." className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close load PTO hours modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>{title}</DialogTitle>
                <DialogDescription>{description}</DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>

            {formError ? (
              <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{formError}</div>
            ) : null}

            <div className="mt-3 grid gap-3">
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Employee</span>
                <AppDropdown
                  value={form.employeeId}
                  onValueChange={(value) => {
                    const nextTypeOptions = getPtoTypeOptions(value);
                    const nextTypeCode = nextTypeOptions.some((item) => item.value === form.ptoTypeCode)
                      ? form.ptoTypeCode
                      : (nextTypeOptions[0]?.value || "vacation");
                    setForm((current) => ({
                      ...current,
                      employeeId: value,
                      ptoTypeCode: nextTypeCode,
                    }));
                  }}
                  options={employeeFieldOptions}
                  searchable
                  disabled={saving || isEditMode}
                />
              </label>
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">PTO type</span>
                  <AppDropdown
                    value={form.ptoTypeCode}
                    onValueChange={(value) => {
                      setForm((current) => ({ ...current, ptoTypeCode: value as LeaveSpherePtoType }));
                    }}
                    options={ptoTypeFieldOptions}
                    searchable={false}
                    disabled={saving || isEditMode}
                  />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Hours</span>
                  <Input
                    type="number"
                    min={0}
                    step={0.5}
                    value={form.hours}
                    onChange={(event) => setForm((current) => ({
                      ...current,
                      hours: event.target.value,
                    }))}
                    disabled={saving}
                  />
                </label>
              </div>
              <label className="block space-y-1 text-sm">
                <span className="text-slate-600">Description</span>
                <Textarea
                  value={form.description}
                  onChange={(event) => setForm((current) => ({
                    ...current,
                    description: event.target.value,
                  }))}
                  className={descriptionMinHeightClassName}
                  disabled={saving}
                  placeholder="Add a short description for this PTO hour change"
                />
              </label>
            </div>

            <div className="mt-3" />
            {(footerActions || hasFormChanges || shouldShowSubmitButton) ? (
              <DialogFooter className="gap-2">
                {footerActions}
                {isEditMode && hasFormChanges ? (
                  <Button
                    variant="outline"
                    onClick={handleRevertChanges}
                    disabled={saving}
                  >
                    Revert
                  </Button>
                ) : null}
                {shouldShowSubmitButton ? (
                  <Button onClick={() => void handleSave()} disabled={saving || !canSave}>
                    {saving ? "Saving..." : submitButtonLabel}
                  </Button>
                ) : null}
              </DialogFooter>
            ) : null}
          </ModalShell>
        </DialogContent>
      </Dialog>

      <ConfirmDialog
        open={isSaveDialogOpen}
        title={saveConfirmCopy.title}
        description={saveConfirmCopy.description}
        confirmLabel={saveConfirmCopy.confirmLabel}
        cancelLabel="Go back"
        onCancel={() => setIsSaveDialogOpen(false)}
        onConfirm={() => {
          void handleConfirmSave();
        }}
        note={{
          label: "Admin note / reason",
          value: saveNote,
          onChange: setSaveNote,
          placeholder: saveConfirmCopy.placeholder,
          disabled: saving,
          helpText: saveConfirmCopy.noteHelpText,
        }}
      />

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
    </>
  );
}
