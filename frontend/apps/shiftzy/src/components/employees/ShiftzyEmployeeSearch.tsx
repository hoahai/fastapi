import { type ReactNode } from "react";
import { Loader2, Search, X } from "lucide-react";

import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Section, SectionHeader } from "@shared/components";

import type { ShiftzyEmployeeSearchFormValues } from "./types";

type ShiftzyEmployeeSearchProps = {
  value: ShiftzyEmployeeSearchFormValues;
  onChange: <K extends keyof ShiftzyEmployeeSearchFormValues>(
    field: K,
    nextValue: ShiftzyEmployeeSearchFormValues[K],
  ) => void;
  onSubmit: () => void;
  onClear: () => void;
  canSubmit: boolean;
  canClear: boolean;
  searching: boolean;
  disabled?: boolean;
  resultText?: string | null;
  sectionOptions: { value: string; label: string }[];
  positionOptions: { value: string; label: string }[];
};

const STATUS_OPTIONS = [
  { value: "", label: "" },
  { value: "active", label: "Active" },
  { value: "inactive", label: "Inactive" },
];

export function ShiftzyEmployeeSearch({
  value,
  onChange,
  onSubmit,
  onClear,
  canSubmit,
  canClear,
  searching,
  disabled,
  resultText,
  sectionOptions,
  positionOptions,
}: ShiftzyEmployeeSearchProps) {
  return (
    <Section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-soft">
      <SectionHeader
        title="Shiftzy Employees Search"
        description="Search by one or more employee fields. Results refresh when you click Search."
      />

      <form
        className="space-y-4 px-1"
        onSubmit={(event) => {
          event.preventDefault();
          if (disabled || searching || !canSubmit) {
            return;
          }
          onSubmit();
        }}
      >
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
          <Field label="Name">
            <ClearableInput
              id="shiftzy-account-search-name"
              value={value.name}
              onChange={(nextValue) => onChange("name", nextValue)}
            />
          </Field>

          <Field label="Area">
            <ClearableDropdown
              canClear={value.scheduleSection.trim().length > 0}
              disabled={disabled || searching}
              onClear={() => onChange("scheduleSection", "")}
            >
              <AppDropdown
                ariaLabel="Schedule section"
                value={value.scheduleSection}
                onValueChange={(nextValue) => onChange("scheduleSection", nextValue)}
                options={sectionOptions}
                placeholder=""
                searchable={false}
                disabled={disabled || searching}
                emptyText="No section found."
              />
            </ClearableDropdown>
          </Field>

          <Field label="Reference Position">
            <ClearableDropdown
              canClear={value.positionCode.trim().length > 0}
              disabled={disabled || searching}
              onClear={() => onChange("positionCode", "")}
            >
              <AppDropdown
                ariaLabel="Position"
                value={value.positionCode}
                onValueChange={(nextValue) => onChange("positionCode", nextValue)}
                options={positionOptions}
                placeholder=""
                searchable={false}
                disabled={disabled || searching}
                emptyText="No position found."
              />
            </ClearableDropdown>
          </Field>

          <Field label="Active">
            <AppDropdown
              ariaLabel="Active"
              value={value.status}
              onValueChange={(nextValue) => onChange("status", nextValue as ShiftzyEmployeeSearchFormValues["status"])}
              options={STATUS_OPTIONS}
              placeholder=""
              searchable={false}
              disabled={disabled || searching}
            />
          </Field>
        </div>

        <div className="flex flex-wrap items-center justify-between gap-2">
          <p className="text-xs text-slate-500">{resultText ?? ""}</p>
          <div className="flex items-center gap-2">
            {canClear ? (
              <Button type="button" variant="outline" onClick={onClear} disabled={disabled || searching}>
                Clear
              </Button>
            ) : null}
            {canSubmit ? (
              <Button type="submit" disabled={disabled || searching || !canSubmit}>
                {searching ? <Loader2 className="size-4 animate-spin" /> : <Search className="size-4" />}
                Search
              </Button>
            ) : null}
          </div>
        </div>
      </form>
    </Section>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="block min-w-0">
      <span className="mb-1 block text-xs font-medium uppercase tracking-[0.08em] text-slate-500">{label}</span>
      {children}
    </label>
  );
}

function ClearableInput({
  id,
  value,
  onChange,
}: {
  id: string;
  value: string;
  onChange: (nextValue: string) => void;
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
        className="pr-9"
      />
      <button
        type="button"
        aria-label="Clear input"
        onClick={() => onChange("")}
        className="absolute right-2 top-1/2 -translate-y-1/2 rounded p-1 text-slate-400 opacity-0 transition hover:text-slate-600 focus-visible:opacity-100 focus-visible:outline-none group-hover:opacity-100 group-focus-within:opacity-100"
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
  return (
    <div className="group relative">
      {children}
      {canClear ? (
        <button
          type="button"
          aria-label="Clear dropdown"
          onClick={onClear}
          className="absolute right-10 top-1/2 -translate-y-1/2 rounded p-1 text-slate-400 opacity-0 transition hover:text-slate-600 focus-visible:opacity-100 focus-visible:outline-none group-hover:opacity-100 group-focus-within:opacity-100"
          disabled={disabled}
        >
          <X className="size-3.5" />
        </button>
      ) : null}
    </div>
  );
}
