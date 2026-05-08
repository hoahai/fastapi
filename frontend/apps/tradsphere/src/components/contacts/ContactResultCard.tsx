import { Copy, Waypoints } from "lucide-react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";

import type { ContactRecord } from "./types";

type ContactResultCardProps = {
  contact: ContactRecord;
  disabled?: boolean;
  onCopy: (contact: ContactRecord) => void;
  onEdit: (contact: ContactRecord) => void;
  onViewUsage: (contact: ContactRecord) => void;
};

export function ContactResultCard({
  contact,
  disabled,
  onCopy,
  onEdit,
  onViewUsage,
}: ContactResultCardProps) {
  const name = contact.fullName || "-";
  const email = contact.email || "-";

  return (
    <article
      role="button"
      tabIndex={disabled ? -1 : 0}
      onClick={() => {
        if (!disabled) {
          onEdit(contact);
        }
      }}
      onKeyDown={(event) => {
        if (disabled) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onEdit(contact);
        }
      }}
      className="space-y-3 rounded-xl border border-slate-200 bg-white p-4 shadow-sm transition hover:border-blue-300 hover:shadow focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="truncate text-base font-semibold text-slate-900">{name}</p>
          <p className="mt-0.5 break-all text-sm text-slate-700">{email}</p>
        </div>

        <div className="flex items-center gap-0.5">
          <ActionIconButton
            icon={<Waypoints />}
            tooltip="View details"
            onClick={(event) => {
              event.stopPropagation();
              onViewUsage(contact);
            }}
            disabled={disabled}
          />
          <ActionIconButton
            icon={<Copy />}
            tooltip="Copy contact"
            onClick={(event) => {
              event.stopPropagation();
              onCopy(contact);
            }}
            disabled={disabled || !contact.email}
          />
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-1.5">
        <span
          className={`rounded-full px-2 py-0.5 text-xs font-medium ${
            contact.active ? "bg-emerald-100 text-emerald-700" : "bg-rose-100 text-rose-700"
          }`}
        >
          {contact.active ? "Active" : "Inactive"}
        </span>
      </div>

      <div className="grid gap-1.5 text-sm text-slate-700 sm:grid-cols-2">
        <Meta label="Company" value={contact.company} />
        <Meta label="Job Title" value={contact.jobTitle} />
        <Meta label="Office" value={contact.office} />
        <Meta label="Cell" value={contact.cell} />
      </div>
    </article>
  );
}

function Meta({ label, value }: { label: string; value: string }) {
  const display = value.trim() || "-";
  return (
    <p className="truncate">
      <span className="text-slate-500">{label}: </span>
      <span className="text-slate-800">{display}</span>
    </p>
  );
}
