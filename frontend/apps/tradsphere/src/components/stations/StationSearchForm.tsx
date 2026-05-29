import { type ReactNode } from "react";
import { Loader2, Search, X } from "lucide-react";

import { AppDropdown, type AppDropdownOption } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Section, SectionHeader } from "@shared/components";

import type { StationSearchFormValues } from "./types";

type StationSearchFormProps = {
  value: StationSearchFormValues;
  onChange: <K extends keyof StationSearchFormValues>(field: K, nextValue: StationSearchFormValues[K]) => void;
  onSubmit: () => void;
  onClear: () => void;
  canSubmit: boolean;
  canClear: boolean;
  searching: boolean;
  disabled?: boolean;
  resultText?: string | null;
  message?: string | null;
};

export function StationSearchForm({
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
}: StationSearchFormProps) {
  const mediaTypeOptions: AppDropdownOption[] = [
    { label: "", value: "" },
    { label: "TV", value: "TV" },
    { label: "RA", value: "RA" },
    { label: "CA", value: "CA" },
  ];
  const languageOptions: AppDropdownOption[] = [
    { label: "", value: "" },
    { label: "English", value: "English" },
    { label: "Spanish", value: "Spanish" },
  ];

  return (
    <Section className="rounded-[1.45rem] border border-blue-100/90 bg-white/95 p-5 shadow-soft">
      <SectionHeader
        title="Stations Search"
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
          <Field label="Station Code">
            <ClearableInput
              id="stations-search-code"
              value={value.stationCode}
              onChange={(nextValue) => onChange("stationCode", nextValue)}
            />
          </Field>

          <Field label="Station Name">
            <ClearableInput
              id="stations-search-name"
              value={value.stationName}
              onChange={(nextValue) => onChange("stationName", nextValue)}
            />
          </Field>

          <Field label="Media Type">
            <AppDropdown
              ariaLabel="Stations search media type"
              options={mediaTypeOptions}
              value={value.mediaType}
              onValueChange={(nextValue) => onChange("mediaType", nextValue)}
              placeholder=""
              searchable={false}
              className="w-full"
              emptyText="No media type found."
            />
          </Field>

          <Field label="Language">
            <AppDropdown
              ariaLabel="Stations search language"
              options={languageOptions}
              value={value.language}
              onValueChange={(nextValue) => onChange("language", nextValue)}
              placeholder=""
              searchable={false}
              className="w-full"
              emptyText="No language found."
            />
          </Field>

          <Field label="Affiliation">
            <ClearableInput
              id="stations-search-affiliation"
              value={value.affiliation}
              onChange={(nextValue) => onChange("affiliation", nextValue)}
            />
          </Field>

          <Field label="Contact Name or Email">
            <ClearableInput
              id="stations-search-contact"
              value={value.contact}
              onChange={(nextValue) => onChange("contact", nextValue)}
              inputMode="email"
            />
          </Field>

        </div>

        <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-blue-100/70 bg-blue-50/40 px-3 py-2">
          <p className="text-xs font-medium text-slate-500">{resultText ?? ""}</p>
          <div className="flex flex-wrap items-center justify-end gap-2">
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
