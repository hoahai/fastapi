import { ReadOnlyValue } from "@/components/dashboard/FormFieldRow";
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
        <div className="max-h-[240px] overflow-auto rounded-lg border border-slate-200">
          <table className="min-w-full border-collapse text-sm">
            <thead className="sticky top-0 bg-slate-50 text-left text-xs uppercase tracking-[0.08em] text-slate-500">
              <tr>
                <Th>Station Code</Th>
                <Th>Station Name</Th>
                <Th>Media</Th>
                <Th>Market</Th>
                <Th>Contact Type</Th>
                <Th>Primary</Th>
                <Th>Active</Th>
              </tr>
            </thead>
            <tbody>
              {usage.map((item) => (
                <tr key={`${item.linkId ?? "link"}:${item.stationCode}:${item.contactType}`} className="border-t border-slate-100">
                  <Td>{item.stationCode || "-"}</Td>
                  <Td>{item.stationName || "-"}</Td>
                  <Td>{item.mediaType || "-"}</Td>
                  <Td>{item.market || "-"}</Td>
                  <Td>{item.contactType || "-"}</Td>
                  <Td>{item.primaryContact ? "Yes" : "No"}</Td>
                  <Td>{item.active ? "Active" : "Inactive"}</Td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <ReadOnlyValue value="No linked stations." className="rounded-md border border-dashed border-slate-300 bg-slate-50" />
      )}
    </Section>
  );
}

function Th({ children }: { children: string }) {
  return <th className="px-3 py-2 font-semibold">{children}</th>;
}

function Td({ children }: { children: string }) {
  return <td className="px-3 py-2 align-top text-slate-700">{children}</td>;
}
