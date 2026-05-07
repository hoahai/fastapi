import { useEffect, useRef } from "react";
import { CheckCircle2, Loader2, RefreshCcw, Search, X } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";

export type EstimateNumberSearchInterpretationOption = {
  id: string;
  label: string;
  description?: string | null;
};

type EstimateNumberSearchProps = {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  onRefresh: () => void;
  searching: boolean;
  refreshing: boolean;
  disabled?: boolean;
  resultText?: string | null;
  interpretationOptions?: EstimateNumberSearchInterpretationOption[];
  onSelectInterpretation?: (id: string) => void;
  onDismissInterpretation?: () => void;
};

export function EstimateNumberSearch({
  value,
  onChange,
  onSubmit,
  onRefresh,
  searching,
  refreshing,
  disabled,
  resultText,
  interpretationOptions,
  onSelectInterpretation,
  onDismissInterpretation,
}: EstimateNumberSearchProps) {
  const firstOptionButtonRef = useRef<HTMLButtonElement | null>(null);
  const isInterpretationOpen = Boolean(interpretationOptions?.length);

  useEffect(() => {
    if (!isInterpretationOpen) {
      return;
    }
    firstOptionButtonRef.current?.focus();
  }, [isInterpretationOpen]);

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-soft">
      <form
        className="flex flex-col gap-3 md:flex-row md:items-center"
        onSubmit={(event) => {
          event.preventDefault();
          if (disabled || searching || refreshing) {
            return;
          }
          onSubmit();
        }}
      >
        <label htmlFor="estnum-search" className="relative block flex-1">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-400" />
          <Input
            id="estnum-search"
            value={value}
            onChange={(event) => {
              onChange(event.target.value);
            }}
            disabled={disabled}
            autoComplete="off"
            spellCheck={false}
            placeholder="Search estimate number, account, buyer, media, note..."
            className="h-11 pl-9"
            onKeyDown={(event) => {
              if (event.key === "Escape" && isInterpretationOpen) {
                event.preventDefault();
                onDismissInterpretation?.();
              }
            }}
          />
        </label>

        <Button
          type="submit"
          variant="outline"
          disabled={disabled || searching || refreshing}
          aria-label="Submit estimate-number search"
        >
          Search
        </Button>

        <Button
          variant="outline"
          onClick={onRefresh}
          disabled={disabled || searching || refreshing}
          aria-label="Refresh current estimate-number search"
        >
          {refreshing ? <Loader2 className="size-4 animate-spin" /> : <RefreshCcw className="size-4" />}
          Refresh
        </Button>
      </form>

      <Dialog
        open={isInterpretationOpen}
        onOpenChange={(open) => {
          if (!open) {
            onDismissInterpretation?.();
          }
        }}
      >
        <DialogContent
          className="max-w-xl p-0"
          onEscapeKeyDown={(event) => {
            event.preventDefault();
            onDismissInterpretation?.();
          }}
        >
          <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-soft">
            <div className="mb-2 flex items-start justify-between gap-2">
              <DialogHeader className="space-y-1 text-left">
                <DialogTitle className="text-base text-slate-900">Choose Search Type</DialogTitle>
                <DialogDescription className="text-xs text-slate-500">
                  This term can be interpreted in multiple ways. Select one option to continue.
                </DialogDescription>
              </DialogHeader>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="size-7 text-slate-500"
                aria-label="Dismiss search interpretation dialog"
                onClick={onDismissInterpretation}
              >
                <X className="size-4" />
              </Button>
            </div>
            <div className="grid gap-2" role="listbox" aria-label="Search interpretation options">
              {interpretationOptions?.map((option, index) => (
                <button
                  key={option.id}
                  ref={index === 0 ? firstOptionButtonRef : undefined}
                  type="button"
                  role="option"
                  className="flex w-full items-start gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-left transition hover:border-slate-300 hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-400"
                  onClick={() => onSelectInterpretation?.(option.id)}
                >
                  <CheckCircle2 className="mt-0.5 size-4 text-slate-500" />
                  <span className="min-w-0">
                    <span className="block text-sm font-medium text-slate-800">{option.label}</span>
                    {option.description ? <span className="block text-xs text-slate-500">{option.description}</span> : null}
                  </span>
                </button>
              ))}
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <p className="mt-2 text-xs text-slate-500">
        Search by estimate number, account code, account name, buyer, media type, month/note, or year.
      </p>
      <p className="mt-1 text-xs text-slate-400">Ambiguous examples: "2026", "Q1'26", "6/26".</p>
      <p className="mt-1 text-xs text-slate-400">Tip: type &quot;today&quot; to find estimate numbers created today.</p>
      {resultText ? <p className="mt-2 text-xs text-slate-500">{resultText}</p> : null}
    </section>
  );
}
