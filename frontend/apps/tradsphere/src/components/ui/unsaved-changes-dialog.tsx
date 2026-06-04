import { AlertTriangle } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

interface UnsavedChangesDialogProps {
  open: boolean;
  onKeepEditing: () => void;
  onDiscardChanges: () => void;
}

export function UnsavedChangesDialog({
  open,
  onKeepEditing,
  onDiscardChanges,
}: UnsavedChangesDialogProps) {
  return (
    <Dialog
      open={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) {
          onKeepEditing();
        }
      }}
    >
      <DialogContent
        className="max-w-md rounded-2xl border border-amber-100 bg-white p-6 shadow-[0_20px_60px_-24px_rgba(15,23,42,0.45)]"
        onEscapeKeyDown={(event) => {
          event.preventDefault();
          onDiscardChanges();
        }}
        onInteractOutside={(event) => {
          event.preventDefault();
        }}
      >
        <DialogHeader className="space-y-3">
          <div className="inline-flex size-10 items-center justify-center rounded-full border border-amber-200 bg-amber-100 text-amber-700">
            <AlertTriangle className="size-5" aria-hidden="true" />
          </div>
          <DialogTitle className="text-slate-900">Discard unsaved changes?</DialogTitle>
          <DialogDescription className="text-slate-600">
            You have unsaved changes in this form. If you close now, your changes will be lost.
          </DialogDescription>
        </DialogHeader>

        <DialogFooter className="mt-7">
          <Button
            variant="outline"
            onClick={onDiscardChanges}
            className="min-w-32 border-rose-300 text-rose-700 hover:bg-rose-50"
          >
            Discard changes
          </Button>
          <Button onClick={onKeepEditing} className="min-w-32">
            Keep editing
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
