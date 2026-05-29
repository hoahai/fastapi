import { Copy, Waypoints } from "lucide-react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";
import { cn } from "@/lib/utils";

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

  const canOpen = !disabled;

  return (
    <article
      role="button"
      tabIndex={canOpen ? 0 : -1}
      onClick={() => {
        if (canOpen) {
          onEdit(contact);
        }
      }}
      onKeyDown={(event) => {
        if (!canOpen) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onEdit(contact);
        }
      }}
      className={cn(
        "space-y-3.5 rounded-xl border p-4 shadow-[0_18px_30px_-24px_rgba(37,99,235,0.45)] transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
        canOpen ? "cursor-pointer" : "cursor-default",
        contact.active
          ? "border-blue-100/90 bg-white hover:-translate-y-0.5 hover:border-blue-300 hover:shadow-[0_20px_34px_-24px_rgba(37,99,235,0.5)]"
          : "border-slate-300 bg-slate-100/90 hover:border-slate-300 hover:shadow-sm",
        !canOpen && "hover:border-slate-200 hover:translate-y-0 hover:shadow-sm",
      )}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="truncate text-base font-semibold tracking-[-0.01em] text-slate-900">{name}</p>
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
            contact.active ? "border border-emerald-200 bg-emerald-100/80 text-emerald-700" : "border border-slate-300 bg-slate-200/90 text-slate-700"
          }`}
        >
          {contact.active ? "Active" : "Inactive"}
        </span>
      </div>

      <div className="grid gap-2 text-sm text-slate-700 sm:grid-cols-2">
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
