import { AlertCircle, CheckCircle2, Info, X } from "lucide-react";
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import { cn } from "@tradsphere/lib/utils";

type ToastKind = "success" | "error" | "info";

type ToastItem = {
  id: string;
  kind: ToastKind;
  title: string;
  message?: string;
};

type ToastContextValue = {
  showToast: (input: { kind: ToastKind; title: string; message?: string; durationMs?: number }) => void;
  success: (title: string, message?: string, durationMs?: number) => void;
  error: (title: string, message?: string, durationMs?: number) => void;
  info: (title: string, message?: string, durationMs?: number) => void;
};

const ToastContext = createContext<ToastContextValue | null>(null);

const DEFAULT_DURATION_MS = 3800;
const ERROR_DURATION_MS = 7000;

function createToastId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([]);

  const dismiss = useCallback((id: string) => {
    setItems((current) => current.filter((item) => item.id !== id));
  }, []);

  const showToast = useCallback(
    (input: { kind: ToastKind; title: string; message?: string; durationMs?: number }) => {
      const id = createToastId();
      setItems((current) => [
        ...current,
        {
          id,
          kind: input.kind,
          title: input.title,
          message: input.message,
        },
      ]);

      const durationMs = input.durationMs ?? (input.kind === "error" ? ERROR_DURATION_MS : DEFAULT_DURATION_MS);
      window.setTimeout(() => {
        dismiss(id);
      }, durationMs);
    },
    [dismiss],
  );

  const contextValue = useMemo<ToastContextValue>(() => {
    return {
      showToast,
      success: (title, message, durationMs) =>
        showToast({ kind: "success", title, message, durationMs }),
      error: (title, message, durationMs) =>
        showToast({ kind: "error", title, message, durationMs }),
      info: (title, message, durationMs) =>
        showToast({ kind: "info", title, message, durationMs }),
    };
  }, [showToast]);

  return (
    <ToastContext.Provider value={contextValue}>
      {children}
      <div
        className="pointer-events-none fixed left-1/2 top-4 z-[120] flex w-[min(92vw,28rem)] -translate-x-1/2 flex-col gap-2"
        aria-live="polite"
      >
        {items.map((item) => {
          const Icon = item.kind === "success" ? CheckCircle2 : item.kind === "error" ? AlertCircle : Info;
          return (
            <div
              key={item.id}
              className={cn(
                "pointer-events-auto rounded-xl border px-3 py-2.5 shadow-2xl ring-1 ring-black/5",
                item.kind === "success" && "border-emerald-700 bg-emerald-600 text-white",
                item.kind === "error" && "border-rose-700 bg-rose-600 text-white",
                item.kind === "info" && "border-blue-700 bg-blue-600 text-white",
              )}
            >
              <div className="flex items-start gap-2">
                <Icon
                  className={cn(
                    "mt-0.5 size-4 shrink-0",
                    item.kind === "success" && "text-emerald-100",
                    item.kind === "error" && "text-rose-100",
                    item.kind === "info" && "text-blue-100",
                  )}
                />
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-semibold text-white">{item.title}</p>
                  {item.message ? <p className="text-sm text-white/90">{item.message}</p> : null}
                </div>
                <button
                  type="button"
                  className="rounded p-0.5 text-white/70 transition-colors hover:text-white"
                  onClick={() => dismiss(item.id)}
                  aria-label="Dismiss notification"
                >
                  <X className="size-3.5" />
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastContextValue {
  const value = useContext(ToastContext);
  if (!value) {
    throw new Error("useToast must be used within ToastProvider");
  }
  return value;
}
