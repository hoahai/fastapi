import { useCallback, type ChangeEvent, type FocusEvent } from "react";

export type CommittedTextNormalizer = (value: string) => string;

export type UseCommittedTextFieldOptions<TElement extends HTMLInputElement | HTMLTextAreaElement> = {
  normalizeOnBlur?: CommittedTextNormalizer;
  onCommit?: (value: string, rawValue: string, event: FocusEvent<TElement>) => void;
};

function defaultNormalizeCommittedText(value: string): string {
  return value.trim();
}

export function useCommittedTextField<TElement extends HTMLInputElement | HTMLTextAreaElement>(
  value: string,
  onValueChange: (value: string) => void,
  options: UseCommittedTextFieldOptions<TElement> = {},
) {
  const normalizeOnBlur = options.normalizeOnBlur ?? defaultNormalizeCommittedText;
  const onCommit = options.onCommit;

  const onChange = useCallback((event: ChangeEvent<TElement>) => {
    onValueChange(event.target.value);
  }, [onValueChange]);

  const onBlur = useCallback((event: FocusEvent<TElement>) => {
    const rawValue = event.target.value;
    const normalizedValue = normalizeOnBlur(rawValue);
    if (normalizedValue !== rawValue) {
      onValueChange(normalizedValue);
    }
    onCommit?.(normalizedValue, rawValue, event);
  }, [normalizeOnBlur, onCommit, onValueChange]);

  return {
    value,
    onChange,
    onBlur,
  };
}
