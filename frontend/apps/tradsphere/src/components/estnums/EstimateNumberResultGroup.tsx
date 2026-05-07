import { ChevronDown } from "lucide-react";

import { EstimateNumberResultActions } from "@/components/estnums/EstimateNumberResultActions";
import { cn } from "@/lib/utils";

import type { EstimateAccountGroup, EstimateSearchItem, EstimateYearGroup } from "./types";

type EstimateNumberResultGroupProps = {
  group: EstimateAccountGroup;
  disabled?: boolean;
  onViewSchedule: (item: EstimateSearchItem) => void;
  onEditEstimate: (item: EstimateSearchItem) => void;
  onCopyEstNum: (item: EstimateSearchItem) => void;
};

export function EstimateNumberResultGroup({
  group,
  disabled,
  onViewSchedule,
  onEditEstimate,
  onCopyEstNum,
}: EstimateNumberResultGroupProps) {
  const title = group.accountName?.trim() ? `${group.accountName} (${group.accountCode})` : group.accountCode;
  const totalItems = group.years.reduce((sum, yearGroup) => sum + yearGroup.items.length, 0);

  return (
    <details open className="group rounded-2xl border border-blue-100 bg-white shadow-soft">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-3 rounded-2xl px-5 py-4">
        <div>
          <h3 className="text-base font-semibold text-slate-900">{title}</h3>
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
            onCopyEstNum={onCopyEstNum}
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
  onCopyEstNum: (item: EstimateSearchItem) => void;
};

function YearBucket({
  yearGroup,
  disabled,
  onViewSchedule,
  onEditEstimate,
  onCopyEstNum,
}: YearBucketProps) {
  return (
    <section className="overflow-hidden rounded-xl border border-slate-100 bg-slate-50/70">
      <header className="border-b border-slate-100 px-4 py-2">
        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-600">{yearGroup.label}</p>
      </header>

      <div className="divide-y divide-slate-100">
        {yearGroup.items.map((item) => {
          const canOpenSchedule = item.hasSchedule && !disabled;
          const flightDates = formatFlightDates(item.flightStart, item.flightEnd);
          const monthOrNote = buildMonthOrNote(item.month, item.note, item.broadcastMonths);
          return (
            <article
              key={`${item.accountCode}:${item.estNum}`}
              role="button"
              tabIndex={canOpenSchedule ? 0 : -1}
              onClick={() => {
                if (canOpenSchedule) {
                  onViewSchedule(item);
                }
              }}
              onKeyDown={(event) => {
                if (!canOpenSchedule) {
                  return;
                }
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  onViewSchedule(item);
                }
              }}
              className={cn(
                "grid gap-3 px-4 py-3",
                "md:grid-cols-[minmax(0,1fr)_minmax(0,220px)]",
                canOpenSchedule ? "cursor-pointer hover:bg-blue-50/45" : "cursor-default",
              )}
            >
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <p className="text-base font-semibold text-slate-900">EstNum {item.estNum}</p>
                  <StatusPill hasSchedule={item.hasSchedule} />
                </div>

                <div className="mt-2 grid gap-2 text-sm text-slate-700 sm:grid-cols-2 lg:grid-cols-3">
                  <MetaLine label="Account" value={item.accountCode} />
                  <MetaLine label="Year" value={toText(item.year)} />
                  <MetaLine label="Quarter" value={item.quarter ? `Q${item.quarter}` : "-"} />
                  <MetaLine label="Month / Note" value={monthOrNote} />
                  <MetaLine label="Media" value={toText(item.mediaType)} />
                  <MetaLine label="Buyer" value={toText(item.buyer)} />
                  <MetaLine label="Flight Dates" value={flightDates} />
                </div>
              </div>

              <EstimateNumberResultActions
                item={item}
                disabled={disabled}
                onViewSchedule={onViewSchedule}
                onEditEstimate={onEditEstimate}
                onCopyEstNum={onCopyEstNum}
              />
            </article>
          );
        })}
      </div>
    </section>
  );
}

function StatusPill({ hasSchedule }: { hasSchedule: boolean }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2.5 py-1 text-xs font-semibold",
        hasSchedule
          ? "border border-emerald-200 bg-emerald-50 text-emerald-700"
          : "border border-amber-200 bg-amber-50 text-amber-700",
      )}
    >
      {hasSchedule ? "Scheduled" : "No schedule"}
    </span>
  );
}

function MetaLine({ label, value }: { label: string; value: string }) {
  return (
    <p className="truncate">
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
  const normalizedStart = normalizeIsoDate(start);
  const normalizedEnd = normalizeIsoDate(end);
  if (!normalizedStart && !normalizedEnd) {
    return "-";
  }
  if (normalizedStart && normalizedEnd) {
    return `${normalizedStart} to ${normalizedEnd}`;
  }
  return normalizedStart || normalizedEnd || "-";
}

function normalizeIsoDate(value?: string | null): string {
  const text = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return "";
  }
  return text;
}

function buildMonthOrNote(
  month: number | null | undefined,
  note: string | null | undefined,
  broadcastMonths: number[] | undefined,
): string {
  if (typeof month === "number" && month >= 1 && month <= 12) {
    return `M${month}`;
  }
  const firstBroadcastMonth = (broadcastMonths || []).find((candidate) => candidate >= 1 && candidate <= 12);
  if (typeof firstBroadcastMonth === "number") {
    return `M${firstBroadcastMonth}`;
  }
  const normalizedNote = String(note || "").trim();
  return normalizedNote || "-";
}
