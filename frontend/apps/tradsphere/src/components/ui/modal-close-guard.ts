type ModalCloseGuardParams = {
  nextOpen: boolean;
  isBusy?: boolean;
  hasUnsavedChanges?: boolean;
};

export function canModalClose({
  nextOpen,
  hasUnsavedChanges = false,
}: ModalCloseGuardParams): boolean {
  if (nextOpen) {
    return true;
  }

  if (!hasUnsavedChanges) {
    return true;
  }

  return false;
}

export function shouldBlockOutsideClose({
  isBusy = false,
  hasUnsavedChanges = false,
}: {
  isBusy?: boolean;
  hasUnsavedChanges?: boolean;
}): boolean {
  if (isBusy) {
    return false;
  }

  return hasUnsavedChanges;
}
