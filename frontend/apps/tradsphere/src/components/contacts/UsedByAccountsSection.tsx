import { ReadOnlyValue } from "@/components/dashboard/FormFieldRow";
import { Section, SectionHeader } from "@shared/components";

import type { ContactAccountUsage } from "./types";

type UsedByAccountsSectionProps = {
  accounts: ContactAccountUsage[];
  className?: string;
};

export function UsedByAccountsSection({ accounts, className }: UsedByAccountsSectionProps) {
  return (
    <Section className={className}>
      <SectionHeader
        title="Used by Accounts"
        description="Read-only account relationships derived from linked stations."
      />

      {accounts.length ? (
        <div className="grid max-h-[220px] grid-cols-1 gap-1.5 overflow-y-auto pr-1 sm:grid-cols-2">
          {accounts.map((item) => (
            <article
              key={item.accountCode}
              className="rounded-md border border-slate-200/90 bg-white px-2.5 py-2 shadow-[0_1px_2px_rgba(15,23,42,0.05)] transition hover:border-blue-200 hover:shadow-sm"
            >
              <p className="text-sm font-semibold text-slate-800">{item.accountCode || "-"}</p>
              <p className="truncate text-[11px] text-slate-500">{item.accountName || "-"}</p>
            </article>
          ))}
        </div>
      ) : (
        <ReadOnlyValue value="No linked accounts." className="rounded-md border border-dashed border-slate-300 bg-slate-50" />
      )}
    </Section>
  );
}
