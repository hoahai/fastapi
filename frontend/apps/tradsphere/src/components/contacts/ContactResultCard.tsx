import { Copy, Pencil, Waypoints } from "lucide-react";

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
    <article className="space-y-3 rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="truncate text-base font-semibold text-slate-900">{name}</p>
          <p className="mt-0.5 break-all text-sm text-slate-700">{email}</p>
        </div>

        <div className="flex items-center gap-0.5">
          <ActionIconButton
            icon={<Copy />}
            tooltip="Copy contact"
            onClick={() => onCopy(contact)}
            disabled={disabled || !contact.email}
          />
          <ActionIconButton
            icon={<Pencil />}
            tooltip="Edit contact"
            onClick={() => onEdit(contact)}
            disabled={disabled}
          />
          <ActionIconButton
            icon={<Waypoints />}
            tooltip="View used by stations"
            onClick={() => onViewUsage(contact)}
            disabled={disabled}
          />
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-1.5">
        {contact.contactTypes.length ? (
          contact.contactTypes.map((type) => (
            <span key={`${contact.id}:${type}`} className="rounded-full bg-blue-100 px-2 py-0.5 text-xs font-semibold text-blue-700">
              {type}
            </span>
          ))
        ) : (
          <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-600">No type</span>
        )}

        {contact.isPrimaryContact ? (
          <span className="rounded-full bg-emerald-100 px-2 py-0.5 text-xs font-medium text-emerald-700">Primary</span>
        ) : null}

        <span
          className={`rounded-full px-2 py-0.5 text-xs font-medium ${
            contact.active ? "bg-slate-100 text-slate-700" : "bg-rose-100 text-rose-700"
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
        <Meta label="Used by stations" value={String(contact.usedByStationCount)} />
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
