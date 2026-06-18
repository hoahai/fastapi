import { useCallback } from "react";

type OutsideClickEvent = {
  preventDefault: () => void;
};

export type UseGuardedModalDialogOptions = {
  isBusy?: boolean;
  hasUnsavedChanges?: boolean;
  onOpenChange: (open: boolean) => void;
  onClose: () => void;
  onDiscardAttempt?: () => void;
};

export function useGuardedModalDialog({
  isBusy = false,
  hasUnsavedChanges = false,
  onOpenChange,
  onClose,
  onDiscardAttempt,
}: UseGuardedModalDialogOptions) {
  const closeModal = useCallback(() => {
    onOpenChange(false);
    onClose();
  }, [onClose, onOpenChange]);

  const handleDialogOpenChange = useCallback((nextOpen: boolean) => {
    if (nextOpen) {
      onOpenChange(true);
      return;
    }

    if (hasUnsavedChanges) {
      if (!isBusy) {
        onDiscardAttempt?.();
      }
      return;
    }

    closeModal();
  }, [closeModal, hasUnsavedChanges, isBusy, onDiscardAttempt, onOpenChange]);

  const handleInteractOutside = useCallback((event: OutsideClickEvent) => {
    if (isBusy) {
      return;
    }
    if (hasUnsavedChanges) {
      event.preventDefault();
    }
  }, [hasUnsavedChanges, isBusy]);

  return {
    closeModal,
    handleDialogOpenChange,
    handleInteractOutside,
  };
}
