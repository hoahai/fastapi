import { useMemo } from "react";

import { getBroadcastMonthRange, getBroadcastQuarterRange, getBroadcastYearRange, buildBroadcastYearOptions } from "@/lib/broadcastCalendar";

import { AppDropdown, type AppDropdownOption } from "@/components/ui/app-dropdown";

export type FlightRangeType = "MONTH" | "QUARTER" | "YEAR" | "CUSTOM";

export type FlightRangePresetState = {
  rangeType: FlightRangeType;
  rangeValue: string;
  year: string;
};

const RANGE_TYPE_OPTIONS: AppDropdownOption[] = [
  { value: "MONTH", label: "Month" },
  { value: "QUARTER", label: "Quarter" },
  { value: "YEAR", label: "Year" },
  { value: "CUSTOM", label: "Custom" },
];

const MONTH_OPTIONS: AppDropdownOption[] = [
  { value: "1", label: "January" },
  { value: "2", label: "February" },
  { value: "3", label: "March" },
  { value: "4", label: "April" },
  { value: "5", label: "May" },
  { value: "6", label: "June" },
  { value: "7", label: "July" },
  { value: "8", label: "August" },
  { value: "9", label: "September" },
  { value: "10", label: "October" },
  { value: "11", label: "November" },
  { value: "12", label: "December" },
];

const QUARTER_OPTIONS: AppDropdownOption[] = [
  { value: "1", label: "Q1" },
  { value: "2", label: "Q2" },
  { value: "3", label: "Q3" },
  { value: "4", label: "Q4" },
];

function getDefaultRangeValue(rangeType: FlightRangeType, fallbackMonth: number): string {
  if (rangeType === "MONTH") {
    return String(fallbackMonth);
  }
  if (rangeType === "QUARTER") {
    return String(Math.floor((fallbackMonth - 1) / 3) + 1);
  }
  return "";
}

function parseYear(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const year = Number(trimmed);
  if (!Number.isInteger(year)) {
    return null;
  }

  return year;
}

function parseRangeValue(value: string, minimum: number, maximum: number): number | null {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    return null;
  }
  return parsed;
}

function resolvePresetRange(value: FlightRangePresetState): { flightStart: string; flightEnd: string } | null {
  const selectedYear = parseYear(value.year);
  if (selectedYear === null) {
    throw new Error("Select a valid year.");
  }

  if (value.rangeType === "MONTH") {
    const month = parseRangeValue(value.rangeValue, 1, 12);
    if (month === null) {
      throw new Error("Select a valid month.");
    }
    return getBroadcastMonthRange(month, selectedYear);
  }

  if (value.rangeType === "QUARTER") {
    const quarter = parseRangeValue(value.rangeValue, 1, 4);
    if (quarter === null) {
      throw new Error("Select a valid quarter.");
    }
    return getBroadcastQuarterRange(quarter as 1 | 2 | 3 | 4, selectedYear);
  }

  if (value.rangeType === "YEAR") {
    return getBroadcastYearRange(selectedYear);
  }

  return null;
}

export function FlightRangeSelector({
  value,
  onChange,
  onApply,
  onError,
  disabled,
  defaultMonth,
  defaultYear,
  variant = "card",
}: {
  value: FlightRangePresetState;
  onChange: (nextValue: FlightRangePresetState) => void;
  onApply: (range: { flightStart: string; flightEnd: string }) => void;
  onError: (message: string | null) => void;
  disabled: boolean;
  defaultMonth: number;
  defaultYear: number;
  variant?: "card" | "compact-inline";
}) {
  const yearOptions = useMemo(() => {
    const selectedYear = parseYear(value.year) ?? defaultYear;
    return buildBroadcastYearOptions(selectedYear, 11).map((year) => ({
      value: String(year),
      label: String(year),
    }));
  }, [defaultYear, value.year]);

  const rangeValueOptions = value.rangeType === "MONTH" ? MONTH_OPTIONS : value.rangeType === "QUARTER" ? QUARTER_OPTIONS : [];
  const isRangeValueDisabled = disabled || value.rangeType === "YEAR" || value.rangeType === "CUSTOM";
  const shouldShowRangeValue = value.rangeType !== "CUSTOM" && value.rangeType !== "YEAR";
  const shouldShowYear = value.rangeType !== "CUSTOM";

  function maybeAutoApply(nextValue: FlightRangePresetState) {
    if (disabled) {
      return;
    }

    onError(null);
    try {
      const range = resolvePresetRange(nextValue);
      if (range) {
        onApply(range);
      }
    } catch (error) {
      onError(error instanceof Error ? error.message : "Unable to apply quick range.");
    }
  }

  function handleRangeTypeChange(nextType: string) {
    if (nextType !== "MONTH" && nextType !== "QUARTER" && nextType !== "YEAR" && nextType !== "CUSTOM") {
      return;
    }

    onError(null);
    const nextValue: FlightRangePresetState = {
      rangeType: nextType,
      rangeValue:
        value.rangeType === nextType && value.rangeValue
          ? value.rangeValue
          : getDefaultRangeValue(nextType, defaultMonth),
      year: value.year || String(defaultYear),
    };
    onChange(nextValue);
    maybeAutoApply(nextValue);
  }

  if (variant === "compact-inline") {
    return (
      <div className="w-full">
        <p className="text-[11px] font-medium text-slate-500">Quick Range</p>
        <div className="mt-1.5 flex flex-wrap items-center gap-1.5 sm:flex-nowrap">
          <AppDropdown
            ariaLabel="Flight range type"
            value={value.rangeType}
            options={RANGE_TYPE_OPTIONS}
            onValueChange={handleRangeTypeChange}
            disabled={disabled}
            searchable={false}
            size="sm"
            className="w-[6.75rem] min-w-[6.75rem]"
          />

          {shouldShowRangeValue ? (
            <AppDropdown
              ariaLabel="Flight range value"
              value={value.rangeValue}
              options={rangeValueOptions}
              onValueChange={(nextValue) => {
                onError(null);
                const updated = { ...value, rangeValue: nextValue };
                onChange(updated);
                maybeAutoApply(updated);
              }}
              placeholder={
                value.rangeType === "MONTH"
                  ? "Month"
                  : value.rangeType === "QUARTER"
                    ? "Quarter"
                    : "N/A"
              }
              disabled={isRangeValueDisabled}
              searchable={false}
              size="sm"
              className="w-[5.5rem] min-w-[5.5rem] flex-1"
            />
          ) : null}

          {shouldShowYear ? (
            <AppDropdown
              ariaLabel="Flight range year"
              value={value.year}
              options={yearOptions}
              onValueChange={(nextValue) => {
                onError(null);
                const updated = { ...value, year: nextValue };
                onChange(updated);
                maybeAutoApply(updated);
              }}
              placeholder="Year"
              disabled={disabled}
              searchable={false}
              size="sm"
              className="w-[5.5rem] min-w-[5.5rem]"
            />
          ) : null}
        </div>
      </div>
    );
  }

  return (
    <div className="w-full rounded-md border border-slate-200/80 bg-white/80 px-2 py-2">
      <div className="flex flex-wrap items-center gap-1.5">
        <span className="mr-0.5 shrink-0 text-[11px] font-medium text-slate-500">Quick Range</span>
        <AppDropdown
          ariaLabel="Flight range type"
          value={value.rangeType}
          options={RANGE_TYPE_OPTIONS}
          onValueChange={handleRangeTypeChange}
          disabled={disabled}
          searchable={false}
          size="sm"
          className="min-w-[8.25rem] flex-1"
        />

        <AppDropdown
          ariaLabel="Flight range value"
          value={value.rangeValue}
          options={rangeValueOptions}
          onValueChange={(nextValue) => {
            onError(null);
            const updated = { ...value, rangeValue: nextValue };
            onChange(updated);
            maybeAutoApply(updated);
          }}
          placeholder={value.rangeType === "MONTH" ? "Select month" : value.rangeType === "QUARTER" ? "Select quarter" : "N/A"}
          disabled={isRangeValueDisabled}
          searchable={false}
          size="sm"
          className="min-w-[7.25rem] flex-1"
        />

        <AppDropdown
          ariaLabel="Flight range year"
          value={value.year}
          options={yearOptions}
          onValueChange={(nextValue) => {
            onError(null);
            const updated = { ...value, year: nextValue };
            onChange(updated);
            maybeAutoApply(updated);
          }}
          placeholder="Year"
          disabled={disabled}
          searchable={false}
          size="sm"
          className="w-[5.5rem] min-w-[5.5rem]"
        />
      </div>
    </div>
  );
}
