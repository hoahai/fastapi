import { useCallback, useEffect, useRef, useState, type Dispatch, type SetStateAction } from "react";

export type UseModalDraftStateOptions<T> = {
  open: boolean;
  sourceValue: T;
  isEqual: (left: T, right: T) => boolean;
  onOpen?: (sourceValue: T) => void;
  onChange?: (value: T) => void;
};

export type UseModalDraftStateResult<T> = {
  draft: T;
  setDraft: Dispatch<SetStateAction<T>>;
  baseline: T;
  hasChanges: boolean;
  resetDraft: () => void;
  commitDraft: (value: T) => void;
};

export function useModalDraftState<T>({
  open,
  sourceValue,
  isEqual,
  onOpen,
  onChange,
}: UseModalDraftStateOptions<T>): UseModalDraftStateResult<T> {
  const [draft, setDraft] = useState(sourceValue);
  const [baseline, setBaseline] = useState(sourceValue);
  const wasOpenRef = useRef(false);
  const lastEmittedValueRef = useRef(sourceValue);

  useEffect(() => {
    const didJustOpen = open && !wasOpenRef.current;
    wasOpenRef.current = open;
    if (!didJustOpen) {
      return;
    }
    setDraft(sourceValue);
    setBaseline(sourceValue);
    lastEmittedValueRef.current = sourceValue;
    onOpen?.(sourceValue);
  }, [onOpen, open, sourceValue]);

  useEffect(() => {
    if (!open) {
      return;
    }
    if (isEqual(lastEmittedValueRef.current, draft)) {
      return;
    }
    lastEmittedValueRef.current = draft;
    onChange?.(draft);
  }, [draft, isEqual, onChange, open]);

  const resetDraft = useCallback(() => {
    setDraft(baseline);
  }, [baseline]);

  const commitDraft = useCallback((value: T) => {
    setDraft(value);
    setBaseline(value);
    lastEmittedValueRef.current = value;
  }, []);

  return {
    draft,
    setDraft,
    baseline,
    hasChanges: !isEqual(draft, baseline),
    resetDraft,
    commitDraft,
  };
}
