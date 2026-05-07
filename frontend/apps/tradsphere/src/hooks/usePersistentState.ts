import { type Dispatch, type SetStateAction, useEffect, useState } from "react";

type StorageKind = "local" | "session";

type PersistentStateOptions<T> = {
  storage?: StorageKind;
  validate?: (value: unknown) => value is T;
};

function getStorage(kind: StorageKind): Storage | null {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    return kind === "local" ? window.localStorage : window.sessionStorage;
  } catch {
    return null;
  }
}

function resolveDefaultValue<T>(defaultValue: T | (() => T)): T {
  return defaultValue instanceof Function ? defaultValue() : defaultValue;
}

export function usePersistentState<T>(
  key: string,
  defaultValue: T | (() => T),
  options: PersistentStateOptions<T> = {},
): [T, Dispatch<SetStateAction<T>>] {
  const storageKind = options.storage ?? "session";

  const [state, setState] = useState<T>(() => {
    const fallback = resolveDefaultValue(defaultValue);
    const storage = getStorage(storageKind);
    if (!storage) {
      return fallback;
    }

    const rawValue = storage.getItem(key);
    if (!rawValue) {
      return fallback;
    }

    try {
      const parsedValue = JSON.parse(rawValue) as unknown;
      if (options.validate && !options.validate(parsedValue)) {
        storage.removeItem(key);
        return fallback;
      }
      return parsedValue as T;
    } catch {
      storage.removeItem(key);
      return fallback;
    }
  });

  useEffect(() => {
    const storage = getStorage(storageKind);
    if (!storage) {
      return;
    }

    try {
      storage.setItem(key, JSON.stringify(state));
    } catch {
      // Ignore persistence write errors.
    }
  }, [key, state, storageKind]);

  return [state, setState];
}
