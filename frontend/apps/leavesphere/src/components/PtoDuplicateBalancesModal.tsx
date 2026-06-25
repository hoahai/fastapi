import { useMemo, useState } from "react";
import { X } from "lucide-react";

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
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";
import { ModalActionFooter, ModalCloseButton, ModalHeaderRow, ModalShell } from "@shared/components";
import { useGuardedModalDialog, useModalDraftState } from "@shared/hooks";

type DuplicateBalancesFormState = {
  employeeIds: string[];
  yearFrom: string;
  yearTo: string;
};

export type LeaveSpherePtoDuplicateBalancesSubmitParams = {
  yearFrom: number;
  yearTo: number;
  employeeIds: string[];
};

type LeaveSpherePtoDuplicateBalancesModalProps = {
  open: boolean;
  title: string;
  description: string;
  employeeOptions: Array<{ value: string; label: string }>;
  defaultEmployeeIds: string[];
  defaultYearFrom: number;
  defaultYearTo: number;
  onOpenChange: (open: boolean) => void;
  onClose: () => void;
  onSubmit?: (params: LeaveSpherePtoDuplicateBalancesSubmitParams) => Promise<void | boolean> | void | boolean;
  saving?: boolean;
};

function normalizeEmployeeIds(values: string[]): string[] {
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const rawValue of values) {
    const value = String(rawValue || "").trim();
    if (!value || seen.has(value)) {
      continue;
    }
    seen.add(value);
    normalized.push(value);
  }
  return normalized;
}

function buildFormState(params: {
  defaultEmployeeIds: string[];
  defaultYearFrom: number;
  defaultYearTo: number;
}): DuplicateBalancesFormState {
  return {
    employeeIds: normalizeEmployeeIds(params.defaultEmployeeIds),
    yearFrom: Number.isFinite(params.defaultYearFrom) ? String(params.defaultYearFrom) : "",
    yearTo: Number.isFinite(params.defaultYearTo) ? String(params.defaultYearTo) : "",
  };
}

function formsEqual(left: DuplicateBalancesFormState, right: DuplicateBalancesFormState): boolean {
  return (
    left.yearFrom === right.yearFrom
    && left.yearTo === right.yearTo
    && JSON.stringify(normalizeEmployeeIds(left.employeeIds)) === JSON.stringify(normalizeEmployeeIds(right.employeeIds))
  );
}

function isPositiveIntegerYear(value: string): boolean {
  if (!value.trim()) {
    return false;
  }
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0;
}

export function LeaveSpherePtoDuplicateBalancesModal({
  open,
  title,
  description,
  employeeOptions,
  defaultEmployeeIds,
  defaultYearFrom,
  defaultYearTo,
  onOpenChange,
  onClose,
  onSubmit,
  saving = false,
}: LeaveSpherePtoDuplicateBalancesModalProps) {
  const sourceForm = useMemo(() => buildFormState({
    defaultEmployeeIds,
    defaultYearFrom,
    defaultYearTo,
  }), [defaultEmployeeIds, defaultYearFrom, defaultYearTo]);

  const {
    draft: form,
    setDraft: setForm,
    hasChanges,
  } = useModalDraftState({
    open,
    sourceValue: sourceForm,
    isEqual: formsEqual,
    onOpen: () => {
      setFormError(null);
    },
  });
  const [formError, setFormError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);

  const {
    closeModal,
    handleDialogOpenChange,
    handleInteractOutside,
  } = useGuardedModalDialog({
    isBusy: saving,
    hasUnsavedChanges: hasChanges,
    onOpenChange,
    onClose,
    onDiscardAttempt: () => {
      setIsDiscardDialogOpen(true);
    },
  });

  const submitValidationError = useMemo(() => {
    if (!isPositiveIntegerYear(form.yearFrom)) {
      return "From Year is required.";
    }
    if (!isPositiveIntegerYear(form.yearTo)) {
      return "Year To is required.";
    }
    return null;
  }, [form.yearFrom, form.yearTo]);

  const canSubmit = Boolean(onSubmit && !submitValidationError);
  const shouldShowSubmitButton = Boolean(canSubmit || saving);

  function handleSave() {
    if (!onSubmit) {
      return;
    }
    if (submitValidationError) {
      setFormError(submitValidationError);
      return;
    }
    setFormError(null);
    void handleConfirmSave();
  }

  async function handleConfirmSave() {
    if (!onSubmit) {
      return;
    }
    if (submitValidationError) {
      setFormError(submitValidationError);
      return;
    }

    const submittedForm: LeaveSpherePtoDuplicateBalancesSubmitParams = {
      yearFrom: Number(form.yearFrom),
      yearTo: Number(form.yearTo),
      employeeIds: normalizeEmployeeIds(form.employeeIds).filter((value) => employeeOptions.some((option) => option.value === value)),
    };

    setFormError(null);
    let didSave: void | boolean;
    try {
      didSave = await onSubmit(submittedForm);
    } catch {
      didSave = false;
    }
    if (didSave === false) {
      return;
    }
    closeModal();
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          className="flex max-h-[90vh] max-w-xl flex-col overflow-hidden rounded-xl bg-white p-6"
          onInteractOutside={handleInteractOutside}
        >
          <ModalShell busy={saving} busyMessage="Duplicating balances..." className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close duplicate balances modal">
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
                <span className="text-slate-600">Employees</span>
                <AppDropdown
                  value=""
                  values={form.employeeIds}
                  onValueChange={() => {}}
                  onValuesChange={(values) => setForm((current) => ({
                    ...current,
                    employeeIds: values,
                  }))}
                  ariaLabel="Employees"
                  options={employeeOptions}
                  multiple
                  searchable
                  placeholder="All employees"
                  emptyText="No active employees found."
                  disabled={saving}
                />
                <p className="text-xs text-slate-500">Leave empty to duplicate for all employees.</p>
              </label>

              <div className="grid gap-3 sm:grid-cols-2">
                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">From Year</span>
                  <Input
                    type="number"
                    min={0}
                    step={1}
                    value={form.yearFrom}
                    onChange={(event) => setForm((current) => ({
                      ...current,
                      yearFrom: event.target.value,
                    }))}
                    disabled={saving}
                  />
                </label>

                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Year To</span>
                  <Input
                    type="number"
                    min={0}
                    step={1}
                    value={form.yearTo}
                    onChange={(event) => setForm((current) => ({
                      ...current,
                      yearTo: event.target.value,
                    }))}
                    disabled={saving}
                  />
                </label>
              </div>
            </div>

            <div className="mt-3" />
            {shouldShowSubmitButton ? (
              <ModalActionFooter className="gap-2">
                {shouldShowSubmitButton ? (
                  <Button onClick={() => void handleSave()} disabled={saving || !canSubmit}>
                    {saving ? "Duplicating..." : "Duplicate Balances"}
                  </Button>
                ) : null}
              </ModalActionFooter>
            ) : null}
          </ModalShell>
        </DialogContent>
      </Dialog>

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
