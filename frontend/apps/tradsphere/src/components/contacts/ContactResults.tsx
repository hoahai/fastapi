import { AlertCircle, Search } from "lucide-react";

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
    <div className="space-y-4">
      {groups.map((group) => (
        <details key={group.key} open className="group rounded-2xl border border-blue-100 bg-white shadow-soft">
          <summary className="flex cursor-pointer list-none items-center justify-between gap-3 rounded-2xl px-5 py-3">
            <div>
              <h3 className="text-base font-bold text-blue-900">{group.label}</h3>
              <p className="mt-0.5 text-xs text-slate-500">{group.items.length} contacts</p>
            </div>
            <span className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500 group-open:hidden">Expand</span>
            <span className="hidden text-xs font-semibold uppercase tracking-[0.08em] text-slate-500 group-open:inline">Collapse</span>
          </summary>

          <div className="grid gap-3 px-3 pb-4 sm:grid-cols-2 xl:grid-cols-3">
            {group.items.map((contact) => (
              <ContactResultCard
                key={contact.id}
                contact={contact}
                disabled={disabled || state === "loading"}
                onCopy={onCopy}
                onEdit={onEdit}
                onViewUsage={onViewUsage}
              />
            ))}
          </div>
        </details>
      ))}
    </div>
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
