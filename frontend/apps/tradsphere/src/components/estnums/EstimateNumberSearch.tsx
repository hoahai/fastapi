import { useMemo, type ReactNode } from "react";
import { Loader2, Search, X } from "lucide-react";

import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Section, SectionHeader } from "@shared/components";

export type EstimateNumberSearchFormValues = {
  estimateNumber: string;
  account: string;
  buyer: string;
  note: string;
  months: string[];
  year: string;
  quarter: string;
  createdToday: boolean;
};

type EstimateNumberSearchProps = {
  value: EstimateNumberSearchFormValues;
  onChange: <K extends keyof EstimateNumberSearchFormValues>(field: K, nextValue: EstimateNumberSearchFormValues[K]) => void;
  onSubmit: () => void;
  onClear: () => void;
  canSubmit: boolean;
  canClear: boolean;
  searching: boolean;
  disabled?: boolean;
  resultText?: string | null;
  message?: string | null;
};

const MONTH_OPTIONS = [
  { value: "1", label: "JAN" },
  { value: "2", label: "FEB" },
  { value: "3", label: "MAR" },
  { value: "4", label: "APR" },
  { value: "5", label: "MAY" },
  { value: "6", label: "JUN" },
  { value: "7", label: "JUL" },
  { value: "8", label: "AUG" },
  { value: "9", label: "SEP" },
  { value: "10", label: "OCT" },
  { value: "11", label: "NOV" },
  { value: "12", label: "DEC" },
];

const QUARTER_OPTIONS = [
  { value: "", label: "" },
  { value: "Q1", label: "Q1" },
  { value: "Q2", label: "Q2" },
  { value: "Q3", label: "Q3" },
  { value: "Q4", label: "Q4" },
];

export function EstimateNumberSearch({
  value,
  onChange,
  onSubmit,
  onClear,
  canSubmit,
  canClear,
  searching,
  disabled,
  resultText,
  message,
}: EstimateNumberSearchProps) {
  const yearOptions = useMemo(() => {
    const nowYear = new Date().getFullYear();
    const years: string[] = [];
    for (let year = nowYear - 3; year <= nowYear + 2; year += 1) {
      years.push(String(year));
    }
    return [{ value: "", label: "" }, ...years.map((year) => ({ value: year, label: year }))];
  }, []);

  return (
    <Section className="rounded-[1.45rem] border border-blue-100/90 bg-white/95 p-5 shadow-soft">
      <SectionHeader
        title="Estimate Numbers Search"
        description="Search by one or more fields. Results load only after you click Search."
      />

      <form
        className="space-y-5 px-1.5"
        onSubmit={(event) => {
          event.preventDefault();
          if (disabled || searching || !canSubmit) {
            return;
          }
          onSubmit();
        }}
      >
        <div className="grid grid-cols-1 gap-3.5 md:grid-cols-2 xl:grid-cols-4">
          <Field label="Estimate Number">
            <ClearableInput
              id="estnum-search-estimate-number"
              value={value.estimateNumber}
              onChange={(nextValue) => {
                onChange("estimateNumber", nextValue.replace(/\D+/g, ""));
              }}
              inputMode="numeric"
            />
          </Field>

          <Field label="Account">
            <ClearableInput
              id="estnum-search-account"
              value={value.account}
              onChange={(nextValue) => onChange("account", nextValue)}
            />
          </Field>

          <Field label="Buyer">
            <ClearableInput
              id="estnum-search-buyer"
              value={value.buyer}
              onChange={(nextValue) => onChange("buyer", nextValue)}
            />
          </Field>

          <Field label="Note">
            <ClearableInput
              id="estnum-search-note"
              value={value.note}
              onChange={(nextValue) => onChange("note", nextValue)}
            />
          </Field>

          <Field label="Months">
            <ClearableDropdown
              canClear={value.months.length > 0}
              disabled={disabled || searching}
              onClear={() => onChange("months", [])}
            >
              <AppDropdown
                ariaLabel="Months"
                value=""
                values={value.months}
                onValueChange={() => {}}
                onValuesChange={(nextValues) => onChange("months", nextValues)}
                options={MONTH_OPTIONS}
                placeholder=""
                searchable={false}
                multiple
                disabled={disabled || searching}
                emptyText="No month found."
              />
            </ClearableDropdown>
          </Field>

          <Field label="Quarter">
            <ClearableDropdown
              canClear={value.quarter.trim().length > 0}
              disabled={disabled || searching}
              onClear={() => onChange("quarter", "")}
            >
              <AppDropdown
                ariaLabel="Quarter"
                value={value.quarter}
                onValueChange={(nextValue) => onChange("quarter", nextValue)}
                options={QUARTER_OPTIONS}
                placeholder=""
                searchable={false}
                disabled={disabled || searching}
                emptyText="No quarter found."
              />
            </ClearableDropdown>
          </Field>

          <Field label="Year">
            <ClearableDropdown
              canClear={value.year.trim().length > 0}
              disabled={disabled || searching}
              onClear={() => onChange("year", "")}
            >
              <AppDropdown
                ariaLabel="Year"
                value={value.year}
                onValueChange={(nextValue) => onChange("year", nextValue)}
                options={yearOptions}
                placeholder=""
                searchable={false}
                disabled={disabled || searching}
                emptyText="No year found."
              />
            </ClearableDropdown>
          </Field>

          <div className="flex items-end">
            <label htmlFor="estnum-search-created-today" className="flex h-10 cursor-pointer items-center gap-2 rounded-lg border border-blue-100/85 bg-blue-50/50 px-3 text-sm text-slate-700 shadow-[0_8px_16px_-14px_rgba(37,99,235,0.45)]">
              <input
                id="estnum-search-created-today"
                type="checkbox"
                checked={value.createdToday}
                onChange={(event) => onChange("createdToday", event.target.checked)}
                className="size-4 rounded border-slate-300 text-blue-600 focus-visible:ring-2 focus-visible:ring-blue-300"
              />
              <span>Created today</span>
            </label>
          </div>
        </div>

        <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-blue-100/70 bg-blue-50/40 px-3 py-2">
          <p className="text-xs font-medium text-slate-500">{resultText ?? ""}</p>
          <div className="flex items-center gap-2">
            {canClear ? (
              <Button type="button" variant="outline" onClick={onClear} disabled={disabled || searching}>
                Clear
              </Button>
            ) : null}
            {canSubmit || searching ? (
              <Button type="submit" disabled={disabled || searching || !canSubmit}>
                {searching ? <Loader2 className="size-4 animate-spin" /> : <Search className="size-4" />}
                Search
              </Button>
            ) : null}
          </div>
        </div>
      </form>

      {message ? <p className="mt-3 rounded-lg border border-amber-200/85 bg-amber-50/90 px-3 py-2 text-xs text-amber-700">{message}</p> : null}
    </Section>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="block min-w-0">
      <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">{label}</span>
      {children}
    </label>
  );
}

function ClearableInput({
  id,
  value,
  onChange,
  inputMode,
}: {
  id: string;
  value: string;
  onChange: (nextValue: string) => void;
  inputMode?: "text" | "numeric" | "decimal" | "email" | "tel" | "search" | "url";
}) {
  const hasValue = value.trim().length > 0;

  return (
    <div className="group relative">
      <Input
        id={id}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        autoComplete="off"
        spellCheck={false}
        inputMode={inputMode}
        className="pr-9"
      />
      <button
        type="button"
        aria-label="Clear input"
        onClick={() => onChange("")}
        className="absolute right-2 top-1/2 -translate-y-1/2 rounded p-1 text-slate-400 opacity-0 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:opacity-100 focus-visible:outline-none group-hover:opacity-100 group-focus-within:opacity-100"
        style={{ visibility: hasValue ? "visible" : "hidden" }}
        tabIndex={hasValue ? 0 : -1}
      >
        <X className="size-3.5" />
      </button>
    </div>
  );
}

function ClearableDropdown({
  children,
  canClear,
  disabled,
  onClear,
}: {
  children: ReactNode;
  canClear: boolean;
  disabled?: boolean;
  onClear: () => void;
}) {
  const canInteract = canClear && !disabled;
  return (
    <div className="group relative">
      <div className="pr-6">{children}</div>
      <button
        type="button"
        aria-label="Clear field"
        disabled={!canInteract}
        onClick={(event) => {
          event.preventDefault();
          event.stopPropagation();
          if (!canInteract) {
            return;
          }
          onClear();
        }}
        className="absolute right-8 top-1/2 -translate-y-1/2 rounded p-1 text-slate-400 opacity-0 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:opacity-100 focus-visible:outline-none group-hover:opacity-100 group-focus-within:opacity-100"
        style={{ visibility: canInteract ? "visible" : "hidden" }}
        tabIndex={canInteract ? 0 : -1}
      >
        <X className="size-3.5" />
      </button>
    </div>
  );
}
