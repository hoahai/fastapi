import { ReadOnlyValue } from "@/components/dashboard/FormFieldRow";
import { cn } from "@/lib/utils";
import { Section, SectionHeader } from "@shared/components";

import type { ContactUsageRow } from "./types";

type UsedByStationsSectionProps = {
  usage: ContactUsageRow[];
  className?: string;
};

export function UsedByStationsSection({ usage, className }: UsedByStationsSectionProps) {
  return (
    <Section className={className}>
      <SectionHeader
        title="Used by Stations"
        description="Read-only station relationship data for this contact."
      />

      {usage.length ? (
        <div className="grid max-h-[380px] grid-cols-1 gap-1.5 overflow-y-auto pr-1 sm:grid-cols-2">
          {usage.map((item) => (
            <article
              key={`${item.linkId ?? "link"}:${item.stationCode}`}
              className="rounded-md border border-slate-200/90 bg-white px-2.5 py-2 shadow-[0_1px_2px_rgba(15,23,42,0.05)] transition hover:border-blue-200 hover:shadow-sm"
            >
              <header className="flex items-start justify-between gap-2 border-b border-slate-100 pb-1.5">
                <div className="min-w-0">
                  <p className="text-sm font-semibold leading-5 text-slate-800">{item.stationCode || "-"}</p>
                  <p className="truncate text-[11px] leading-4 text-slate-500">{item.stationName || "-"}</p>
                </div>
                <div className="flex shrink-0 items-center gap-1">
                  <StatusBadge
                    tone={item.active ? "success" : "muted"}
                    label={item.active ? "Active" : "Inactive"}
                  />
                  {item.primaryContact ? <StatusBadge tone="info" label="Primary" /> : null}
                </div>
              </header>
              <dl className="mt-1.5 grid grid-cols-1 gap-x-3 gap-y-1 text-xs sm:grid-cols-2">
                <UsageField label="Media Type" value={item.mediaType || "-"} />
                <UsageField label="Language" value={item.language || "-"} />
                <UsageField label="Affiliation" value={item.affiliation || "-"} />
                {item.syscode ? <UsageField label="Syscode" value={item.syscode} /> : null}
              </dl>
            </article>
          ))}
        </div>
      ) : (
        <ReadOnlyValue value="No linked stations." className="rounded-md border border-dashed border-slate-300 bg-slate-50" />
      )}
    </Section>
  );
}

function UsageField({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="text-[10px] font-medium uppercase tracking-[0.08em] text-slate-400">{label}</dt>
      <dd className="truncate text-[12px] font-medium leading-4 text-slate-700">{value || "-"}</dd>
    </div>
  );
}

function StatusBadge({ label, tone }: { label: string; tone: "success" | "muted" | "info" }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold leading-4",
        tone === "success" && "bg-emerald-50 text-emerald-700 ring-1 ring-emerald-100",
        tone === "muted" && "bg-slate-100 text-slate-600 ring-1 ring-slate-200",
        tone === "info" && "bg-blue-50 text-blue-700 ring-1 ring-blue-100",
      )}
    >
      {label}
    </span>
  );
}
