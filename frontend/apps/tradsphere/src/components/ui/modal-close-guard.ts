type ModalCloseGuardParams = {
  nextOpen: boolean;
  isBusy?: boolean;
  hasUnsavedChanges?: boolean;
};

export function canModalClose({
  nextOpen,
  isBusy = false,
  hasUnsavedChanges = false,
}: ModalCloseGuardParams): boolean {
  if (nextOpen) {
    return true;
  }

  if (isBusy) {
    return false;
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
  return isBusy || hasUnsavedChanges;
}
