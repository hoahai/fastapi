import type { ReactNode } from "react";
import { X } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { cn } from "@/lib/utils";
import { ModalCloseButton, ModalHeaderRow, ModalShell } from "@shared/components";

type ConfirmDialogNoteProps = {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
  helpText?: string;
  required?: boolean;
};

type ConfirmDialogProps = {
  open: boolean;
  title: string;
  description: string;
  confirmLabel?: string;
  cancelLabel?: string;
  onConfirm: () => void;
  onCancel: () => void;
  note?: ConfirmDialogNoteProps;
  children?: ReactNode;
};

export function ConfirmDialog({
  open,
  title,
  description,
  confirmLabel = "Confirm",
  cancelLabel = "Cancel",
  onConfirm,
  onCancel,
  note,
  children,
}: ConfirmDialogProps) {
  const noteValue = note?.value.trim() ?? "";
  const isNoteMissing = Boolean(note?.required) && noteValue.length === 0;

  return (
    <Dialog
      open={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) {
          onCancel();
        }
      }}
    >
      <DialogContent className={cn("!z-[60]", note || children ? "max-w-lg" : "max-w-md")}>
        <ModalShell className="min-h-0 flex-1">
          <ModalHeaderRow
            actions={(
              <DialogClose asChild aria-label="Close confirm dialog">
                <ModalCloseButton icon={<X className="size-4" />} />
              </DialogClose>
            )}
          >
            <DialogHeader>
              <DialogTitle>{title}</DialogTitle>
              <DialogDescription>{description}</DialogDescription>
            </DialogHeader>
          </ModalHeaderRow>
          {note ? (
            <label className="mt-4 block space-y-1 text-sm">
              <span className="text-slate-600">
                {note.label}
                {note.required ? " *" : ""}
              </span>
              <Textarea
                value={note.value}
                onChange={(event) => note.onChange(event.target.value)}
                className={cn("min-h-[120px]")}
                placeholder={note.placeholder ?? "Add a note or reason for this decision"}
                disabled={note.disabled}
                required={note.required}
              />
              {note.helpText ? (
                <p className={cn("text-xs leading-5", isNoteMissing ? "text-rose-600" : "text-slate-500")}>
                  {note.helpText}
                </p>
              ) : null}
            </label>
          ) : null}
          {children ? <div className="mt-4">{children}</div> : null}
          <DialogFooter>
            <Button variant="outline" onClick={onCancel}>
              {cancelLabel}
            </Button>
            <Button onClick={onConfirm} disabled={isNoteMissing || note?.disabled}>
              {confirmLabel}
            </Button>
          </DialogFooter>
        </ModalShell>
      </DialogContent>
    </Dialog>
  );
}
