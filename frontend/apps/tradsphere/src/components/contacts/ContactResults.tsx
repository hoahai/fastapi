import { AlertCircle, ChevronDown, Search } from "lucide-react";

import { SectionCard } from "@shared/components/layout/SectionCard";

import { ContactResultCard } from "./ContactResultCard";
import type { ContactGroup, ContactRecord } from "./types";

type ContactResultsState = "idle" | "loading" | "error" | "empty" | "ready";

type ContactResultsProps = {
  state: ContactResultsState;
  groups: ContactGroup[];
  error?: string | null;
  disabled?: boolean;
  onCopy: (contact: ContactRecord) => void;
  onEdit: (contact: ContactRecord) => void;
  onViewUsage: (contact: ContactRecord) => void;
};

export function ContactResults({
  state,
  groups,
  error,
  disabled,
  onCopy,
  onEdit,
  onViewUsage,
}: ContactResultsProps) {
  if (state === "idle") {
    return (
      <EmptyPanel
        icon={<Search className="size-5 text-slate-400" />}
        message="Use the search form above to find contacts."
      />
    );
  }

  if (state === "error") {
    return (
      <EmptyPanel
        icon={<AlertCircle className="size-5 text-rose-500" />}
        message={error || "Could not load contacts. Please try again."}
      />
    );
  }

  if (state === "empty") {
    return <EmptyPanel icon={<Search className="size-5 text-slate-400" />} message="No contacts found." />;
  }

  if (state === "loading" && !groups.length) {
    return (
      <EmptyPanel
        icon={<Search className="size-5 text-slate-400" />}
        message="Searching contacts..."
      />
    );
  }

  return (
    <SectionCard title="Results" contentClassName="space-y-4">
        {groups.map((group) => (
          <details key={group.key} open className="group overflow-hidden rounded-2xl border border-blue-100 bg-slate-50/70">
            <summary className="flex cursor-pointer list-none items-center justify-between gap-2 border-b border-blue-100 bg-blue-50/70 px-4 py-3">
              <p className="text-sm font-bold uppercase tracking-[0.14em] text-blue-800">{group.label}</p>
              <span className="inline-flex items-center gap-2 text-slate-500" aria-hidden="true">
                <span className="text-xs">{group.items.length} contacts</span>
                <ChevronDown className="size-4 transition-transform group-open:rotate-180" />
              </span>
            </summary>

            <div className="p-3">
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                {group.items.map((contact) => (
                  <ContactResultCard
                    key={contact.id}
                    contact={contact}
                    disabled={disabled}
                    onCopy={onCopy}
                    onEdit={onEdit}
                    onViewUsage={onViewUsage}
                  />
                ))}
              </div>
            </div>
          </details>
        ))}
    </SectionCard>
  );
}

function EmptyPanel({ icon, message }: { icon: JSX.Element; message: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-6 py-10 text-center">
      <div className="inline-flex items-center gap-2 text-sm text-slate-600">
        {icon}
        <span>{message}</span>
      </div>
    </div>
  );
}
