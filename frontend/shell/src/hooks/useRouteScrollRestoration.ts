import { useEffect } from "react";

type ScrollRestorationOptions = {
  throttleMs?: number;
  maxRestoreAttempts?: number;
  restoreRetryDelayMs?: number;
};

function setScrollY(storageKey: string, scrollY: number): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.sessionStorage.setItem(storageKey, String(Math.max(0, Math.floor(scrollY))));
  } catch {
    // Ignore persistence write errors.
  }
}

function getScrollY(storageKey: string): number | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    const rawValue = window.sessionStorage.getItem(storageKey);
    if (!rawValue) {
      return null;
    }
    const parsedValue = Number(rawValue);
    if (!Number.isFinite(parsedValue) || parsedValue < 0) {
      window.sessionStorage.removeItem(storageKey);
      return null;
    }
    return parsedValue;
  } catch {
    return null;
  }
}

export function useRouteScrollRestoration(
  storageKey: string,
  options: ScrollRestorationOptions = {},
): void {
  const throttleMs = options.throttleMs ?? 150;
  const maxRestoreAttempts = options.maxRestoreAttempts ?? 20;
  const restoreRetryDelayMs = options.restoreRetryDelayMs ?? 80;

  useEffect(() => {
    const targetScrollY = getScrollY(storageKey);
    if (targetScrollY === null) {
      return;
    }

    let canceled = false;

    const restoreScroll = (attempt: number) => {
      if (canceled) {
        return;
      }

      window.scrollTo({ top: targetScrollY, left: 0, behavior: "auto" });

      if (Math.abs(window.scrollY - targetScrollY) <= 2) {
        return;
      }

      const pageCanStillGrow = document.documentElement.scrollHeight - window.innerHeight < targetScrollY;
      if (attempt < maxRestoreAttempts && pageCanStillGrow) {
        window.setTimeout(() => {
          window.requestAnimationFrame(() => restoreScroll(attempt + 1));
        }, restoreRetryDelayMs);
      }
    };

    window.requestAnimationFrame(() => restoreScroll(0));

    return () => {
      canceled = true;
    };
  }, [maxRestoreAttempts, restoreRetryDelayMs, storageKey]);

  useEffect(() => {
    let timeoutId: number | null = null;

    const flush = () => {
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
        timeoutId = null;
      }
      setScrollY(storageKey, window.scrollY);
    };

    const handleScroll = () => {
      if (timeoutId !== null) {
        return;
      }
      timeoutId = window.setTimeout(() => {
        timeoutId = null;
        setScrollY(storageKey, window.scrollY);
      }, throttleMs);
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === "hidden") {
        flush();
      }
    };

    window.addEventListener("scroll", handleScroll, { passive: true });
    window.addEventListener("beforeunload", flush);
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      flush();
      window.removeEventListener("scroll", handleScroll);
      window.removeEventListener("beforeunload", flush);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [storageKey, throttleMs]);
}
