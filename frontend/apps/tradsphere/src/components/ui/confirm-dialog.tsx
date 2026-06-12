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
import { ModalCloseButton } from "@shared/components";

type ConfirmDialogNoteProps = {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
  helpText?: string;
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
        <DialogClose asChild aria-label="Close confirm dialog">
          <ModalCloseButton icon={<X className="size-4" />} className="absolute right-0 top-0 z-20" />
        </DialogClose>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        {note ? (
          <label className="mt-4 block space-y-1 text-sm">
            <span className="text-slate-600">{note.label}</span>
            <Textarea
              value={note.value}
              onChange={(event) => note.onChange(event.target.value)}
              className={cn("min-h-[120px]")}
              placeholder={note.placeholder ?? "Add a note or reason for this decision"}
              disabled={note.disabled}
            />
            {note.helpText ? (
              <p className="text-xs leading-5 text-slate-500">{note.helpText}</p>
            ) : null}
          </label>
        ) : null}
        {children ? <div className="mt-4">{children}</div> : null}
        <DialogFooter>
          <Button variant="outline" onClick={onCancel}>
            {cancelLabel}
          </Button>
          <Button onClick={onConfirm}>
            {confirmLabel}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
