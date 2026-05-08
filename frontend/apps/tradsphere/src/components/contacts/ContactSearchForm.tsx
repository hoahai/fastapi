import { type ReactNode } from "react";
import { Loader2, Search, X } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Section, SectionHeader } from "@shared/components";

import type { ContactSearchFormValues } from "./types";

type ContactSearchFormProps = {
  value: ContactSearchFormValues;
  onChange: <K extends keyof ContactSearchFormValues>(field: K, nextValue: ContactSearchFormValues[K]) => void;
  onSubmit: () => void;
  onClear: () => void;
  canSubmit: boolean;
  canClear: boolean;
  searching: boolean;
  disabled?: boolean;
  resultText?: string | null;
  message?: string | null;
};

export function ContactSearchForm({
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
}: ContactSearchFormProps) {
  return (
    <Section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-soft">
      <SectionHeader
        title="Contacts Search"
        description="Search by one or more fields. Results load only after you click Search."
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
              id="contacts-search-name"
              value={value.name}
              onChange={(nextValue) => onChange("name", nextValue)}
            />
          </Field>

          <Field label="Email">
            <ClearableInput
              id="contacts-search-email"
              value={value.email}
              onChange={(nextValue) => onChange("email", nextValue)}
              inputMode="email"
            />
          </Field>

          <Field label="Phone">
            <ClearableInput
              id="contacts-search-phone"
              value={value.phone}
              onChange={(nextValue) => onChange("phone", nextValue)}
              inputMode="tel"
            />
          </Field>

          <Field label="Station">
            <ClearableInput
              id="contacts-search-station"
              value={value.station}
              onChange={(nextValue) => onChange("station", nextValue)}
            />
          </Field>
        </div>

        <div className="flex flex-wrap items-center justify-end gap-2">
          <p className="text-xs text-slate-500">{resultText ?? ""}</p>
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
      </form>

      {message ? <p className="mt-2 text-xs text-amber-700">{message}</p> : null}
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
        className="absolute right-2 top-1/2 -translate-y-1/2 rounded p-1 text-slate-400 opacity-0 transition hover:text-slate-600 focus-visible:opacity-100 focus-visible:outline-none group-hover:opacity-100 group-focus-within:opacity-100"
        style={{ visibility: hasValue ? "visible" : "hidden" }}
        tabIndex={hasValue ? 0 : -1}
      >
        <X className="size-3.5" />
      </button>
    </div>
  );
}
