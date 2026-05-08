import { ReadOnlyValue } from "@/components/dashboard/FormFieldRow";
import { Section, SectionHeader } from "@shared/components";

import type { ContactEstNumUsage } from "./types";

type UsedByEstNumsSectionProps = {
  estNums: ContactEstNumUsage[];
  className?: string;
};

const MONTH_ABBREVIATIONS = [
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

function toPeriodLabel(item: ContactEstNumUsage): string {
  const explicitPeriodLabel = item.periodLabel.trim();
  if (explicitPeriodLabel) {
    return explicitPeriodLabel;
  }
  const year = item.year ?? (item.broadcastYears.length === 1 ? item.broadcastYears[0] : null);
  if (year !== null) {
    const yearSuffix = String(year).slice(-2);
    if (item.quarter !== null && item.quarter >= 1 && item.quarter <= 4) {
      return `Q${item.quarter}'${yearSuffix}`;
    }
    if (item.month !== null && item.month >= 1 && item.month <= 12) {
      const monthLabel = MONTH_ABBREVIATIONS[item.month] || "-";
      return `${monthLabel}'${yearSuffix}`;
    }

    const validMonths = item.broadcastMonths.filter((month) => month >= 1 && month <= 12);
    if (!validMonths.length) {
      return `Q?'${yearSuffix}`;
    }
    const uniqueMonths = [...new Set(validMonths)].sort((left, right) => left - right);
    const monthSet = new Set(uniqueMonths);
    const quarters = [...new Set(uniqueMonths.map((month) => Math.floor((month - 1) / 3) + 1))].sort(
      (left, right) => left - right,
    );
    const fullQuarters = quarters.every((quarter) =>
      [quarter * 3 - 2, quarter * 3 - 1, quarter * 3].every((month) => monthSet.has(month)),
    );
    if (fullQuarters && uniqueMonths.length === quarters.length * 3) {
      return `Q${quarters.join(",")}'${yearSuffix}`;
    }
    return `${uniqueMonths.map((month) => MONTH_ABBREVIATIONS[month] || "").filter(Boolean).join(",")}'${yearSuffix}`;
  }
  return "-";
}

function buildEstNumDescription(item: ContactEstNumUsage): string {
  return [
    item.accountCode || "-",
    toPeriodLabel(item),
    item.mediaType || "-",
  ]
    .map((part) => part.trim())
    .filter(Boolean)
    .join(" ");
}

export function UsedByEstNumsSection({ estNums, className }: UsedByEstNumsSectionProps) {
  return (
    <Section className={className}>
      <SectionHeader
        title="Used by EstNums"
        description="Read-only estimate-number relationships derived from linked stations."
      />

      {estNums.length ? (
        <div className="grid max-h-[240px] grid-cols-1 gap-1.5 overflow-y-auto pr-1 sm:grid-cols-2">
          {estNums.map((item) => (
            <article
              key={`${item.estNum}:${item.accountCode}`}
              className="rounded-md border border-slate-200/90 bg-white px-2.5 py-2 shadow-[0_1px_2px_rgba(15,23,42,0.05)] transition hover:border-blue-200 hover:shadow-sm"
            >
              <p className="text-sm font-semibold text-slate-800">{item.estNum}</p>
              <p className="truncate text-[11px] text-slate-500">{buildEstNumDescription(item)}</p>
            </article>
          ))}
        </div>
      ) : (
        <ReadOnlyValue value="No linked EstNums." className="rounded-md border border-dashed border-slate-300 bg-slate-50" />
      )}
    </Section>
  );
}
