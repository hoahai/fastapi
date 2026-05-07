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
        className="max-w-md rounded-xl bg-white p-6"
        onInteractOutside={(event) => {
          event.preventDefault();
        }}
      >
        <DialogHeader className="space-y-2">
          <div className="inline-flex size-10 items-center justify-center rounded-full bg-amber-100 text-amber-700">
            <AlertTriangle className="size-5" />
          </div>
          <DialogTitle>Discard unsaved changes?</DialogTitle>
          <DialogDescription>
            You have unsaved changes in this form. If you close now, your changes will be lost.
          </DialogDescription>
        </DialogHeader>

        <DialogFooter className="mt-7">
          <Button onClick={onKeepEditing}>Keep editing</Button>
          <Button
            variant="outline"
            onClick={onDiscardChanges}
            className="border-rose-200 text-rose-700 hover:bg-rose-50"
          >
            Discard changes
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
