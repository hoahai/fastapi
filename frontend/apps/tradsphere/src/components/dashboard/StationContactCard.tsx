import { Copy, Pencil, Trash2 } from "lucide-react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";

export type StationDraftContact = {
  clientKey?: string | null;
  linkId?: number | null;
  linkNote?: string | null;
  contactType: string;
  contactId?: number | null;
  firstName?: string | null;
  lastName?: string | null;
  fullName?: string | null;
  email?: string | null;
  office?: string | null;
  cell?: string | null;
  company?: string | null;
  jobTitle?: string | null;
  note?: string | null;
  primaryContact?: boolean | null;
};

interface StationContactCardProps {
  contact: StationDraftContact;
  isSubmitting: boolean;
  onEdit: () => void;
  onRemove: () => void;
  onCopy: () => void;
}

function displayValue(value?: string | null): string {
  const text = String(value ?? "").trim();
  return text;
}

function displayContactType(value?: string | null): string {
  const text = String(value ?? "").trim().toUpperCase();
  return text || "REP";
}

function contactTypeChipClass(contactType: string): string {
  switch (contactType) {
    case "REP":
      return "bg-indigo-100 text-indigo-700";
    case "TRAFFIC":
      return "bg-emerald-100 text-emerald-700";
    case "BILLING":
      return "bg-amber-100 text-amber-800";
    case "SALES":
      return "bg-cyan-100 text-cyan-700";
    case "PROGRAMMING":
      return "bg-fuchsia-100 text-fuchsia-700";
    default:
      return "bg-slate-100 text-slate-700";
  }
}

function Field({
  label,
  value,
  wrapValue = false,
}: {
  label: string;
  value: string;
  wrapValue?: boolean;
}) {
  return (
    <div className="flex min-w-0 items-start gap-2">
      <p className="shrink-0 text-sm font-medium leading-5 text-slate-600">{label}</p>
      <p className={`${wrapValue ? "break-all" : "truncate"} min-w-0 text-sm text-slate-800`}>{value}</p>
    </div>
  );
}

export function StationContactCard({
  contact,
  isSubmitting,
  onEdit,
  onRemove,
  onCopy,
}: StationContactCardProps) {
  const fullName = displayValue(contact.fullName);
  const email = displayValue(contact.email);
  const office = displayValue(contact.office);
  const cell = displayValue(contact.cell);
  const contactType = displayContactType(contact.contactType);
  const phoneFields = [
    office ? { label: "Office", value: office } : null,
    cell ? { label: "Cell", value: cell } : null,
  ].filter((item): item is { label: "Office" | "Cell"; value: string } => item !== null);
  const hasDetails = Boolean(fullName || email || phoneFields.length);

  return (
    <article className="space-y-2 rounded-md border border-slate-200/80 bg-white/70 p-2.5">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5">
          <span
            className={`rounded-full px-1.5 py-0.5 text-[10px] font-medium ${contactTypeChipClass(contactType)}`}
          >
            {contactType}
          </span>
          {contact.primaryContact ? (
            <span className="rounded-full bg-blue-100 px-1.5 py-0.5 text-[10px] font-medium text-blue-700">Primary</span>
          ) : null}
        </div>
        <div className="flex items-center gap-0.5">
          <ActionIconButton
            icon={<Trash2 />}
            tooltip="Remove contact from station"
            onClick={onRemove}
            disabled={isSubmitting}
            className="!h-6 !w-6 !p-0 text-rose-500 hover:text-rose-600 focus-visible:text-rose-600 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:text-rose-500 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110 hover:[&_svg]:text-rose-600 focus-visible:[&_svg]:text-rose-600"
            aria-label="Remove contact from station"
          />
          <ActionIconButton
            icon={<Pencil />}
            tooltip="Edit contact"
            onClick={onEdit}
            disabled={isSubmitting}
            className="!h-6 !w-6 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
          />
          <ActionIconButton
            icon={<Copy />}
            tooltip="Copy contact"
            onClick={onCopy}
            disabled={isSubmitting}
            className="!h-6 !w-6 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
          />
        </div>
      </div>

      {hasDetails ? (
        <div className="space-y-1.5">
          {fullName ? <Field label="Name" value={fullName} /> : null}
          {email ? <Field label="Email" value={email} wrapValue /> : null}
          {phoneFields.length ? (
            <div className="grid gap-1.5 sm:grid-cols-2">
              {phoneFields.map((field) => (
                <Field key={field.label} label={field.label} value={field.value} />
              ))}
            </div>
          ) : null}
        </div>
      ) : null}
    </article>
  );
}
