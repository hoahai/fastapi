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

function getTypeTone(type: string): {
  cardClassName: string;
  badgeClassName: string;
} {
  if (type === "REP") {
    return {
      cardClassName: "border-blue-200/90 bg-blue-50/35",
      badgeClassName: "bg-blue-100 text-blue-700",
    };
  }
  if (type === "TRAFFIC") {
    return {
      cardClassName: "border-emerald-200/90 bg-emerald-50/35",
      badgeClassName: "bg-emerald-100 text-emerald-700",
    };
  }
  if (type === "BILLING") {
    return {
      cardClassName: "border-amber-200/90 bg-amber-50/35",
      badgeClassName: "bg-amber-100 text-amber-700",
    };
  }
  return {
    cardClassName: "border-slate-200/80 bg-white/70",
    badgeClassName: "bg-slate-100 text-slate-700",
  };
}

export function StationContactCard({
  contact,
  isSubmitting,
  onEdit,
  onRemove,
  onCopy,
}: StationContactCardProps) {
  const type = String(contact.contactType || "").trim().toUpperCase() || "UNKNOWN";
  const fullName = displayValue(contact.fullName);
  const email = displayValue(contact.email);
  const office = displayValue(contact.office);
  const cell = displayValue(contact.cell);
  const phoneFields = [
    office ? { label: "Office", value: office } : null,
    cell ? { label: "Cell", value: cell } : null,
  ].filter((item): item is { label: "Office" | "Cell"; value: string } => item !== null);
  const hasDetails = Boolean(fullName || email || phoneFields.length);
  const tone = getTypeTone(type);

  return (
    <article className={`space-y-2 rounded-md border p-2.5 ${tone.cardClassName}`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <p className={`rounded-full px-2 py-0.5 text-xs font-semibold tracking-wide ${tone.badgeClassName}`}>{type}</p>
          {contact.primaryContact ? (
            <span className="rounded-full bg-blue-100 px-1.5 py-0.5 text-[10px] font-medium text-blue-700">
              Primary
            </span>
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
