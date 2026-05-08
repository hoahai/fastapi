import type { StationRecord } from "./types";

type StationResultCardProps = {
  station: StationRecord;
  disabled?: boolean;
  onEdit: (station: StationRecord) => void;
};

export function StationResultCard({
  station,
  disabled,
  onEdit,
}: StationResultCardProps) {
  const deliverySummary = station.deliveryMethod?.name?.trim() || "-";

  const repSummary = station.repContacts
    .sort((left, right) => {
      if (left.primaryContact !== right.primaryContact) {
        return left.primaryContact ? -1 : 1;
      }
      return `${left.fullName} ${left.email}`.localeCompare(`${right.fullName} ${right.email}`);
    })
    .map((contact) => {
      const name = contact.fullName || "";
      const email = contact.email || "";
      return name || email;
    })
    .filter(Boolean)
    .join(", ") || "-";

  return (
    <article
      role="button"
      tabIndex={disabled ? -1 : 0}
      onClick={() => {
        if (!disabled) {
          onEdit(station);
        }
      }}
      onKeyDown={(event) => {
        if (disabled) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onEdit(station);
        }
      }}
      className="space-y-3 rounded-xl border border-slate-200 bg-white p-4 shadow-sm transition hover:border-blue-300 hover:shadow focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="truncate text-base font-semibold text-slate-900">{station.code || "-"}</p>
          <p className="mt-0.5 truncate text-sm text-slate-700">{station.name || "-"}</p>
        </div>
      </div>

      <div className="grid gap-1.5 text-sm text-slate-700 sm:grid-cols-2">
        <Meta label="Media Type" value={station.mediaType} />
        <Meta label="Language" value={station.language} />
        <Meta label="Affiliation" value={station.affiliation} />
        {station.syscode ? <Meta label="Syscode" value={station.syscode} /> : null}
        <Meta label="Delivery Method" value={deliverySummary} className="sm:col-span-2" />
        <Meta label="Rep Contact" value={repSummary} className="sm:col-span-2" />
      </div>
    </article>
  );
}

function Meta({ label, value, className }: { label: string; value: string; className?: string }) {
  const display = value.trim() || "-";
  return (
    <p className={`truncate ${className || ""}`.trim()}>
      <span className="text-slate-500">{label}: </span>
      <span className="text-slate-800">{display}</span>
    </p>
  );
}
