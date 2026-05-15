import { useEffect, useMemo, useState } from "react";
import { Clock3, Loader2, X } from "lucide-react";

import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { canModalClose, shouldBlockOutsideClose } from "@/components/ui/modal-close-guard";
import { Textarea } from "@/components/ui/textarea";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { FormRow } from "@shared/components/form/FormRow";
import type {
  ShiftzyEmployee,
  ShiftzyPosition,
  ShiftzySchedule,
  ShiftzyScheduleUpdateInput,
  ShiftzyShift,
} from "@shiftzy/lib/shiftzyApi";

type ShiftzyScheduleEditModalProps = {
  open: boolean;
  mode?: "create" | "edit";
  schedule: ShiftzySchedule | null;
  employees: ShiftzyEmployee[];
  positions: ShiftzyPosition[];
  shifts: ShiftzyShift[];
  canEdit: boolean;
  saving: boolean;
  onOpenChange: (open: boolean) => void;
  onSave: (payload: ShiftzyScheduleUpdateInput) => Promise<void>;
};

type FormState = {
  employeeId: string;
  positionCode: string;
  shiftId: string;
  date: string;
  startTime: string;
  endTime: string;
  note: string;
};

const EMPTY_FORM: FormState = {
  employeeId: "",
  positionCode: "",
  shiftId: "",
  date: "",
  startTime: "",
  endTime: "",
  note: "",
};

function formsEqual(left: FormState, right: FormState): boolean {
  return (
    left.employeeId === right.employeeId
    && left.positionCode === right.positionCode
    && left.shiftId === right.shiftId
    && left.date === right.date
    && left.startTime === right.startTime
    && left.endTime === right.endTime
    && left.note === right.note
  );
}

function toTimeInputValue(value: string): string {
  if (!value) {
    return "";
  }
  const normalized = value.trim();
  if (/^\d{2}:\d{2}$/.test(normalized)) {
    return normalized;
  }
  if (/^\d{2}:\d{2}:\d{2}$/.test(normalized)) {
    return normalized.slice(0, 5);
  }
  return normalized.slice(0, 5);
}

export function ShiftzyScheduleEditModal({
  open,
  mode = "edit",
  schedule,
  employees,
  positions,
  shifts,
  canEdit,
  saving,
  onOpenChange,
  onSave,
}: ShiftzyScheduleEditModalProps) {
  const [form, setForm] = useState<FormState>(EMPTY_FORM);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);

  const initialForm = useMemo<FormState>(() => {
    if (!schedule) {
      return EMPTY_FORM;
    }
    if (mode === "create") {
      return {
        ...EMPTY_FORM,
        date: schedule.date,
      };
    }
    return {
      employeeId: schedule.employeeId,
      positionCode: schedule.positionCode,
      shiftId: schedule.shiftId || "",
      date: schedule.date,
      startTime: toTimeInputValue(schedule.startTime),
      endTime: toTimeInputValue(schedule.endTime),
      note: schedule.note || "",
    };
  }, [mode, schedule]);

  useEffect(() => {
    if (!open) {
      setForm(EMPTY_FORM);
      setIsDiscardDialogOpen(false);
      return;
    }
    setForm(initialForm);
    setIsDiscardDialogOpen(false);
  }, [initialForm, open]);

  const employeeOptions = useMemo(
    () => employees.map((item) => ({
      label: item.name,
      value: item.id,
    })),
    [employees],
  );

  const positionOptions = useMemo(
    () => positions.map((item) => ({
      label: item.name,
      value: item.code,
    })),
    [positions],
  );

  const shiftOptions = useMemo(
    () => [
      { label: "Custom time", value: "" },
      ...shifts.map((item) => ({
        label: `${item.name} (${item.startTime}-${item.endTime})`,
        value: item.id,
      })),
    ],
    [shifts],
  );

  const hasUnsavedChanges = useMemo(() => !formsEqual(form, initialForm), [form, initialForm]);
  const canSave = Boolean(
    canEdit
    && schedule
    && form.employeeId
    && form.positionCode
    && form.date
    && form.startTime
    && form.endTime,
  );

  const inputsDisabled = saving || !canEdit;
  const blockOutsideClose = shouldBlockOutsideClose({
    isBusy: saving,
    hasUnsavedChanges,
  });

  function handleOpenChange(nextOpen: boolean) {
    if (!canModalClose({
      nextOpen,
      isBusy: saving,
      hasUnsavedChanges,
    })) {
      if (!nextOpen && hasUnsavedChanges && !saving) {
        setIsDiscardDialogOpen(true);
      }
      return;
    }
    onOpenChange(nextOpen);
  }

  async function handleSubmit() {
    if (!schedule || !canSave) {
      return;
    }
    await onSave({
      id: schedule.id,
      employeeId: form.employeeId,
      positionCode: form.positionCode,
      shiftId: form.shiftId || null,
      date: form.date,
      startTime: form.startTime,
      endTime: form.endTime,
      note: form.note.trim() || null,
    });
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleOpenChange}>
        <DialogContent
          className="w-[calc(100%-1.5rem)] max-w-[720px] rounded-xl bg-white p-6"
          onPointerDownOutside={(event) => {
            if (blockOutsideClose) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/70"
            aria-label="Close shift editor"
          >
            <X className="size-4" />
          </DialogClose>

          <DialogHeader>
            <DialogTitle>{mode === "create" ? "Add Shift" : "Edit Shift"}</DialogTitle>
            <DialogDescription>
              {canEdit
                ? mode === "create"
                  ? "Add a new shift card. Changes are staged locally until you click Save on the schedule board."
                  : "Update this card. Changes are staged locally until you click Save on the schedule board."
                : "You have view access only for this schedule card."}
            </DialogDescription>
          </DialogHeader>

          {schedule ? (
            <div className="mt-4 space-y-3">
            <FormRow label="Employee">
              <AppDropdown
                value={form.employeeId}
                options={employeeOptions}
                placeholder=""
                searchable={false}
                onValueChange={(nextValue) => setForm((current) => {
                  const employee = employees.find((item) => item.id === nextValue);
                  const preferredPosition = employee?.refPositionCode
                    && positions.some((item) => item.code === employee.refPositionCode)
                    ? employee.refPositionCode
                    : null;
                  return {
                    ...current,
                    employeeId: nextValue,
                    positionCode: preferredPosition || current.positionCode,
                  };
                })}
                disabled={inputsDisabled}
              />
            </FormRow>

            <FormRow label="Position">
              <AppDropdown
                value={form.positionCode}
                options={positionOptions}
                placeholder=""
                searchable={positions.length > 8}
                onValueChange={(nextValue) => setForm((current) => ({ ...current, positionCode: nextValue }))}
                disabled={inputsDisabled}
              />
            </FormRow>

            <FormRow label="Shift">
              <AppDropdown
                value={form.shiftId}
                options={shiftOptions}
                placeholder=""
                searchable={shifts.length > 8}
                onValueChange={(nextValue) => {
                  const nextShift = shifts.find((item) => item.id === nextValue);
                  setForm((current) => ({
                    ...current,
                    shiftId: nextValue,
                    startTime: nextShift ? toTimeInputValue(nextShift.startTime) : current.startTime,
                    endTime: nextShift ? toTimeInputValue(nextShift.endTime) : current.endTime,
                  }));
                }}
                disabled={inputsDisabled}
              />
            </FormRow>

            <FormRow label="Start / End">
              <div className="grid gap-2 sm:grid-cols-2">
                <div className="relative">
                  <Input
                    type="time"
                    value={form.startTime}
                    onChange={(event) => setForm((current) => ({ ...current, startTime: event.target.value }))}
                    disabled={inputsDisabled}
                    className="pr-10"
                  />
                  <Clock3 className="pointer-events-none absolute right-3 top-1/2 size-4 -translate-y-1/2 text-slate-500" />
                </div>
                <div className="relative">
                  <Input
                    type="time"
                    value={form.endTime}
                    onChange={(event) => setForm((current) => ({ ...current, endTime: event.target.value }))}
                    disabled={inputsDisabled}
                    className="pr-10"
                  />
                  <Clock3 className="pointer-events-none absolute right-3 top-1/2 size-4 -translate-y-1/2 text-slate-500" />
                </div>
              </div>
            </FormRow>

            <FormRow label="Note" alignStart>
              <Textarea
                rows={3}
                value={form.note}
                onChange={(event) => setForm((current) => ({ ...current, note: event.target.value }))}
                disabled={inputsDisabled}
              />
            </FormRow>
            </div>
          ) : null}

          <DialogFooter>
            {canEdit && hasUnsavedChanges && canSave ? (
              <Button onClick={() => void handleSubmit()} disabled={!canSave || saving}>
                {saving ? (
                  <>
                    <Loader2 className="size-4 animate-spin" />
                    Saving...
                  </>
                ) : (
                  mode === "create" ? "Add" : "Edit"
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
