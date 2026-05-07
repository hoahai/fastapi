import { useRef, useState } from "react";
import { CheckCircle2, ChevronDown, Circle } from "lucide-react";
import { Tooltip } from "@shared/components/actions/Tooltip";
import { cn } from "@/lib/utils";

import type { EstimateAccountGroup, EstimateSearchItem, EstimateYearGroup } from "./types";

type EstimateNumberResultGroupProps = {
  group: EstimateAccountGroup;
  disabled?: boolean;
  onViewSchedule: (item: EstimateSearchItem) => void;
  onEditEstimate: (item: EstimateSearchItem) => void;
};

export function EstimateNumberResultGroup({
  group,
  disabled,
  onViewSchedule,
  onEditEstimate,
}: EstimateNumberResultGroupProps) {
  const title = group.accountName?.trim() ? `${group.accountName} (${group.accountCode})` : group.accountCode;
  const totalItems = group.years.reduce((sum, yearGroup) => sum + yearGroup.items.length, 0);

  return (
    <details open className="group rounded-2xl border border-blue-100 bg-white shadow-soft">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-3 rounded-2xl px-5 py-4">
        <div>
          <h3 className="text-lg font-bold text-blue-900 md:text-xl">{title}</h3>
          <p className="mt-1 text-xs text-slate-500">{totalItems} matches</p>
        </div>
        <ChevronDown className="size-4 text-slate-500 transition-transform group-open:rotate-180" />
      </summary>

      <div className="space-y-3 px-3 pb-4">
        {group.years.map((yearGroup) => (
          <YearBucket
            key={yearGroup.key}
            yearGroup={yearGroup}
            disabled={disabled}
            onViewSchedule={onViewSchedule}
            onEditEstimate={onEditEstimate}
          />
        ))}
      </div>
    </details>
  );
}

type YearBucketProps = {
  yearGroup: EstimateYearGroup;
  disabled?: boolean;
  onViewSchedule: (item: EstimateSearchItem) => void;
  onEditEstimate: (item: EstimateSearchItem) => void;
};

function YearBucket({
  yearGroup,
  disabled,
  onViewSchedule,
  onEditEstimate,
}: YearBucketProps) {
  return (
    <section className="overflow-hidden rounded-xl border border-slate-100 bg-slate-50/70">
      <header className="border-b border-blue-100 bg-blue-50/70 px-4 py-2.5">
        <p className="text-sm font-bold uppercase tracking-[0.14em] text-blue-800">{yearGroup.label}</p>
      </header>

      <div className="grid gap-3 p-3 sm:grid-cols-2 xl:grid-cols-3">
        {yearGroup.items.map((item) => {
          const canOpenEdit = !disabled;
          const canOpenSchedule = Boolean(item.hasSchedule && !disabled);
          const flightDates = formatFlightDates(item.flightStart, item.flightEnd);
          const periodLabel = buildPeriodLabel(item);
          const noteText = toText(item.note);
          return (
            <article
              key={`${item.accountCode}:${item.estNum}`}
              role="button"
              tabIndex={canOpenEdit ? 0 : -1}
              onClick={() => {
                if (canOpenEdit) {
                  onEditEstimate(item);
                }
              }}
              onKeyDown={(event) => {
                if (!canOpenEdit) {
                  return;
                }
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  onEditEstimate(item);
                }
              }}
              className={cn(
                "rounded-xl p-4 shadow-sm transition",
                item.hasSchedule
                  ? "border border-emerald-200/90 bg-emerald-50/35"
                  : "border border-slate-200 bg-white",
                canOpenEdit ? "cursor-pointer hover:border-blue-200 hover:bg-blue-50/40" : "cursor-default",
              )}
            >
              <div className="min-w-0">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <p className="text-base font-semibold text-slate-900">EstNum {item.estNum}</p>
                  <StatusPill
                    hasSchedule={item.hasSchedule}
                    clickable={canOpenSchedule}
                    onClick={() => onViewSchedule(item)}
                  />
                </div>

                <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-2 text-sm text-slate-700">
                  <MetaLine label="Account Code" value={item.accountCode} />
                  <MetaLine label="Media" value={toText(item.mediaType)} />
                  <MetaLine className="col-span-2" label="Flight Dates" value={flightDates} />
                  <MetaLine className="col-span-2" label="Month, Quarter, Year" value={periodLabel} />
                  <MetaLine label="Buyer" value={toText(item.buyer)} />
                  <MetaLine label="Note" value={noteText} />
                </div>
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}

function StatusPill({
  hasSchedule,
  clickable,
  onClick,
}: {
  hasSchedule: boolean;
  clickable: boolean;
  onClick: () => void;
}) {
  const anchorRef = useRef<HTMLButtonElement | null>(null);
  const [isTooltipOpen, setIsTooltipOpen] = useState(false);

  if (hasSchedule) {
    return (
      <>
        <button
          ref={anchorRef}
          type="button"
          onClick={(event) => {
            event.stopPropagation();
            if (!clickable) {
              return;
            }
            onClick();
          }}
          onMouseEnter={() => setIsTooltipOpen(true)}
          onMouseLeave={() => setIsTooltipOpen(false)}
          onFocus={() => setIsTooltipOpen(true)}
          onBlur={() => setIsTooltipOpen(false)}
          disabled={!clickable}
          className={cn(
            "inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-xs font-semibold transition-transform",
            "border border-emerald-200 bg-emerald-50 text-emerald-700",
            clickable
              ? "cursor-pointer hover:scale-105 hover:bg-emerald-100 focus-visible:scale-105 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400"
              : "cursor-default",
          )}
          aria-label="View schedule"
        >
          <CheckCircle2 className="size-3" />
          Scheduled
        </button>
        <Tooltip open={isTooltipOpen && clickable} anchorRef={anchorRef} text="Click to view schedule" />
      </>
    );
  }

  return (
    <span
      onClick={(event) => {
        event.stopPropagation();
      }}
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-xs font-semibold",
        "cursor-default border border-slate-300 bg-slate-200/95 text-slate-700",
      )}
    >
      <Circle className="size-3" />
      No Schedule
    </span>
  );
}

function MetaLine({ label, value, className }: { label: string; value: string; className?: string }) {
  return (
    <p className={cn("truncate", className)}>
      <span className="text-slate-500">{label}: </span>
      <span className="text-slate-800">{value || "-"}</span>
    </p>
  );
}

function toText(value: string | number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }
  const text = String(value).trim();
  return text || "-";
}

function formatFlightDates(start?: string | null, end?: string | null): string {
  const normalizedStart = formatIsoDateForDisplay(start);
  const normalizedEnd = formatIsoDateForDisplay(end);
  if (!normalizedStart && !normalizedEnd) {
    return "-";
  }
  if (normalizedStart && normalizedEnd) {
    return `${normalizedStart} \u2192 ${normalizedEnd}`;
  }
  return normalizedStart || normalizedEnd || "-";
}

function formatIsoDateForDisplay(value?: string | null): string {
  const text = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return "";
  }
  const year = text.slice(0, 4);
  const month = text.slice(5, 7);
  const day = text.slice(8, 10);
  return `${month}/${day}/${year}`;
}

function buildMonthLabel(
  month: number,
): string {
  const monthNames = [
    "",
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
  ];
  if (month >= 1 && month <= 12) {
    return monthNames[month] || "-";
  }
  return "-";
}

function isValidMonth(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 1 && value <= 12;
}

function isValidYear(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 1;
}

function parseIsoMonthYear(value?: string | null): { month: number; year: number } | null {
  const text = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return null;
  }
  const year = Number(text.slice(0, 4));
  const month = Number(text.slice(5, 7));
  if (!isValidYear(year) || !isValidMonth(month)) {
    return null;
  }
  return { month, year };
}

function deriveQuarter(month: number): number {
  return Math.floor((month - 1) / 3) + 1;
}

function buildPeriodLabel(item: EstimateSearchItem): string {
  const broadcastMonths = (item.broadcastMonths || []).filter((candidate) => isValidMonth(candidate));
  const broadcastYears = (item.broadcastYears || []).filter((candidate) => isValidYear(candidate));

  const flightStart = parseIsoMonthYear(item.flightStart);
  const flightEnd = parseIsoMonthYear(item.flightEnd);

  const startMonth = broadcastMonths[0] ?? (isValidMonth(item.month) ? item.month : flightStart?.month ?? null);
  const endMonth =
    broadcastMonths.length > 0
      ? (broadcastMonths[broadcastMonths.length - 1] ?? null)
      : (flightEnd?.month ?? startMonth);

  const startYear = broadcastYears[0] ?? (isValidYear(item.year) ? item.year : flightStart?.year ?? null);
  const endYear =
    broadcastYears.length > 0 ? (broadcastYears[broadcastYears.length - 1] ?? null) : (flightEnd?.year ?? startYear);

  const monthLabel =
    isValidMonth(startMonth) && isValidMonth(endMonth)
      ? startMonth === endMonth
        ? buildMonthLabel(startMonth)
        : `${buildMonthLabel(startMonth)}-${buildMonthLabel(endMonth)}`
      : "-";

  const quarterLabel =
    isValidMonth(startMonth) && isValidMonth(endMonth)
      ? deriveQuarter(startMonth) === deriveQuarter(endMonth)
        ? `Q${deriveQuarter(startMonth)}`
        : `Q${deriveQuarter(startMonth)}-Q${deriveQuarter(endMonth)}`
      : "-";

  const yearLabel =
    isValidYear(startYear) && isValidYear(endYear)
      ? startYear === endYear
        ? String(startYear)
        : `${startYear}-${endYear}`
      : "-";

  if (monthLabel === "-" && quarterLabel === "-" && yearLabel === "-") {
    return "-";
  }

  return `${monthLabel}, ${quarterLabel}, ${yearLabel}`;
}
