import { Loader2, RefreshCcw, Search } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

type EstimateNumberSearchProps = {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  onRefresh: () => void;
  searching: boolean;
  refreshing: boolean;
  disabled?: boolean;
  resultText?: string | null;
  contextualHint?: string | null;
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
  contextualHint,
}: EstimateNumberSearchProps) {
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
            onChange={(event) => onChange(event.target.value)}
            disabled={disabled}
            autoComplete="off"
            spellCheck={false}
            placeholder="Search estimate number, account, buyer, media, note..."
            className="h-11 pl-9"
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

      <p className="mt-2 text-xs text-slate-500">
        Search by estimate number, account code, account name, buyer, media type, month/note, or year.
      </p>
      <p className="mt-1 text-xs text-slate-400">
        Tip: type &quot;today&quot; to find estimate numbers created today. Use &quot;year:2026&quot; for year search.
      </p>
      {contextualHint ? <p className="mt-1 text-xs text-slate-500">{contextualHint}</p> : null}
      {resultText ? <p className="mt-2 text-xs text-slate-500">{resultText}</p> : null}
    </section>
  );
}
