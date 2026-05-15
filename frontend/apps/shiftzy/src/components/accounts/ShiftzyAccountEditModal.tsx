import { useEffect, useMemo, useState } from "react";
import { Loader2, X } from "lucide-react";

import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { canModalClose, shouldBlockOutsideClose } from "@/components/ui/modal-close-guard";
import { Textarea } from "@/components/ui/textarea";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { FormRow } from "@shared/components/form/FormRow";
import type { ShiftzyEmployee, ShiftzyPosition } from "@shiftzy/lib/shiftzyApi";

type ShiftzyAccountEditModalProps = {
  open: boolean;
  mode: "create" | "edit";
  employee: ShiftzyEmployee | null;
  positions: ShiftzyPosition[];
  canEdit: boolean;
  saving: boolean;
  onOpenChange: (open: boolean) => void;
  onSave: (payload: ShiftzyAccountFormPayload) => Promise<void>;
};

export type ShiftzyAccountFormPayload = {
  id?: string;
  name: string;
  scheduleSection: string;
  note: string;
  refPositionCode: string;
  active: boolean;
};

type FormState = {
  id: string;
  name: string;
  scheduleSection: string;
  note: string;
  refPositionCode: string;
  active: boolean;
};

const EMPTY_FORM: FormState = {
  id: "",
  name: "",
  scheduleSection: "",
  note: "",
  refPositionCode: "",
  active: true,
};

const AREA_OPTIONS = [
  { value: "Front", label: "Front" },
  { value: "Kitchen", label: "Kitchen" },
];

function toNameCase(value: string): string {
  const raw = value.trim().replace(/\s+/g, " ");
  if (!raw) {
    return "";
  }
  return raw.replace(/\S+/g, (word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase());
}

function formsEqual(left: FormState, right: FormState): boolean {
  return (
    left.id === right.id
    && left.name === right.name
    && left.scheduleSection === right.scheduleSection
    && left.note === right.note
    && left.refPositionCode === right.refPositionCode
    && left.active === right.active
  );
}

export function ShiftzyAccountEditModal({
  open,
  mode,
  employee,
  positions,
  canEdit,
  saving,
  onOpenChange,
  onSave,
}: ShiftzyAccountEditModalProps) {
  const [form, setForm] = useState<FormState>(EMPTY_FORM);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);

  const initialForm = useMemo<FormState>(() => {
    if (!employee) {
      return { ...EMPTY_FORM };
    }
    return {
      id: employee.id,
      name: employee.name,
      scheduleSection: employee.scheduleSection,
      note: employee.note || "",
      refPositionCode: employee.refPositionCode || "",
      active: employee.active,
    };
  }, [employee]);

  useEffect(() => {
    if (!open) {
      setForm(EMPTY_FORM);
      setIsDiscardDialogOpen(false);
      return;
    }
    setForm(initialForm);
    setIsDiscardDialogOpen(false);
  }, [initialForm, open]);

  const positionOptions = useMemo(
    () => [{ value: "", label: "" }, ...positions.map((item) => ({ value: item.code, label: item.name }))],
    [positions],
  );

  const hasUnsavedChanges = useMemo(() => !formsEqual(form, initialForm), [form, initialForm]);
  const canSave = Boolean(
    canEdit
    && form.name.trim().length > 0
    && form.scheduleSection.trim().length > 0,
  );

  const inputsDisabled = saving || !canEdit;
  const blockOutsideClose = shouldBlockOutsideClose({ isBusy: saving, hasUnsavedChanges });

  function handleOpenChange(nextOpen: boolean) {
    if (!canModalClose({ nextOpen, isBusy: saving, hasUnsavedChanges })) {
      if (!nextOpen && hasUnsavedChanges && !saving) {
        setIsDiscardDialogOpen(true);
      }
      return;
    }
    onOpenChange(nextOpen);
  }

  async function handleSubmit() {
    if (!canSave) {
      return;
    }
    await onSave({
      ...(form.id ? { id: form.id } : {}),
      name: form.name.trim(),
      scheduleSection: form.scheduleSection,
      note: form.note.trim(),
      refPositionCode: form.refPositionCode,
      active: form.active,
    });
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleOpenChange}>
        <DialogContent
          className="w-[calc(100%-1.5rem)] max-w-[680px] rounded-xl bg-white p-6"
          onPointerDownOutside={(event) => {
            if (blockOutsideClose) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70"
            aria-label="Close employee editor"
          >
            <X className="size-4" />
          </DialogClose>

          <DialogHeader>
            <DialogTitle>{mode === "create" ? "Add Employee" : "Edit Employee"}</DialogTitle>
            <DialogDescription>
              {canEdit
                ? "Create or update Shiftzy employee metadata used by schedules."
                : "You have view access only for Shiftzy employees."}
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 space-y-3">
          <FormRow label="Name">
            <Input
              value={form.name}
              onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
              onBlur={(event) => {
                const normalized = toNameCase(event.target.value);
                setForm((current) => ({ ...current, name: normalized }));
              }}
              onPaste={(event) => {
                const pasted = event.clipboardData.getData("text");
                if (!pasted.trim()) {
                  return;
                }
                event.preventDefault();
                setForm((current) => ({ ...current, name: toNameCase(pasted) }));
              }}
              disabled={inputsDisabled}
              autoComplete="off"
            />
          </FormRow>

          <FormRow label="Area">
            <AppDropdown
              value={form.scheduleSection}
              options={AREA_OPTIONS}
              placeholder=""
              searchable={false}
              onValueChange={(nextValue) => setForm((current) => ({ ...current, scheduleSection: nextValue }))}
              disabled={inputsDisabled}
            />
          </FormRow>

          <FormRow label="Ref Position">
            <AppDropdown
              value={form.refPositionCode}
              options={positionOptions}
              placeholder=""
              searchable={false}
              onValueChange={(nextValue) => setForm((current) => ({ ...current, refPositionCode: nextValue }))}
              disabled={inputsDisabled}
            />
          </FormRow>

          <FormRow label="Note" alignStart>
            <Textarea
              rows={3}
              value={form.note}
              onChange={(event) => setForm((current) => ({ ...current, note: event.target.value }))}
              disabled={inputsDisabled}
            />
          </FormRow>

          <FormRow label="Active">
            <button
              type="button"
              role="switch"
              aria-checked={form.active}
              aria-label="Active"
              onClick={() => {
                if (inputsDisabled) {
                  return;
                }
                setForm((current) => ({ ...current, active: !current.active }));
              }}
              disabled={inputsDisabled}
              aria-disabled={inputsDisabled}
              className={`inline-flex h-10 w-fit items-center gap-3 px-1 py-2 text-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                form.active
                  ? "text-blue-700"
                  : "text-slate-600"
              }`}
            >
              <span
                className={`relative inline-flex h-5 w-9 items-center rounded-full transition ${
                  form.active ? "bg-blue-500" : "bg-slate-300"
                }`}
              >
                <span
                  className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${
                    form.active ? "translate-x-[18px]" : "translate-x-[2px]"
                  }`}
                />
              </span>
            </button>
          </FormRow>
          </div>

          <DialogFooter>
            {hasUnsavedChanges && canSave ? (
              <Button onClick={() => void handleSubmit()} disabled={!canSave || saving}>
                {saving ? (
                  <>
                    <Loader2 className="size-4 animate-spin" />
                    Saving...
                  </>
                ) : mode === "create" ? (
                  "Add"
                ) : (
                  "Save"
                )}
              </Button>
            ) : null}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isDiscardDialogOpen}
        onKeepEditing={() => setIsDiscardDialogOpen(false)}
        onDiscardChanges={() => {
          setIsDiscardDialogOpen(false);
          onOpenChange(false);
        }}
      />
    </>
  );
}
