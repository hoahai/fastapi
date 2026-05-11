import { CirclePlus, ListCheck, Pencil } from "lucide-react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";

export type StationDraftDeliveryMethod = {
  id?: number | null;
  name: string;
  url: string;
  username: string;
  password: string;
  passwordStatus?: string;
  deadline: string;
  note: string;
};

interface StationDeliveryMethodSectionProps {
  deliveryMethod: StationDraftDeliveryMethod;
  isSubmitting: boolean;
  isReadOnly?: boolean;
  onSelectDeliveryMethod: () => void;
  onAddDeliveryMethod: () => void;
  onEditDeliveryMethod: () => void;
}

function SummaryRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="grid grid-cols-[88px_minmax(0,1fr)] items-start gap-2">
      <p className="text-sm font-medium leading-5 text-slate-600">{label}</p>
      <p className="break-words text-sm leading-5 text-slate-800">{value || "-"}</p>
    </div>
  );
}

export function StationDeliveryMethodSection({
  deliveryMethod,
  isSubmitting,
  isReadOnly = false,
  onSelectDeliveryMethod,
  onAddDeliveryMethod,
  onEditDeliveryMethod,
}: StationDeliveryMethodSectionProps) {
  const hasSelectedMethod = Boolean(deliveryMethod.name.trim() || deliveryMethod.id !== null);
  const isActionDisabled = isSubmitting || isReadOnly;
  const resolvedPassword = asString(deliveryMethod.password);
  const passwordDisplay = resolvedPassword || (asString(deliveryMethod.passwordStatus) === "Stored" ? "Stored (hidden)" : "-");

  return (
    <section className="min-w-0 space-y-3">
      <div className="flex min-h-10 items-center justify-between gap-3 border-b border-slate-200 pb-2">
        <h3 className="text-sm font-semibold text-slate-800">Traffic Delivery Method</h3>
        <div className="flex flex-wrap items-center justify-end gap-1">
          <ActionIconButton
            icon={<CirclePlus />}
            tooltip="Add delivery method"
            aria-label="Add delivery method"
            onClick={onAddDeliveryMethod}
            disabled={isActionDisabled}
          />
          <ActionIconButton
            icon={<Pencil />}
            tooltip="Edit delivery method"
            aria-label="Edit delivery method"
            onClick={onEditDeliveryMethod}
            disabled={isActionDisabled || !hasSelectedMethod}
          />
          <ActionIconButton
            icon={<ListCheck />}
            tooltip="Select delivery method"
            aria-label="Select delivery method"
            onClick={onSelectDeliveryMethod}
            disabled={isActionDisabled}
          />
        </div>
      </div>

      {hasSelectedMethod ? (
        <div className="space-y-2">
          <SummaryRow label="Name" value={deliveryMethod.name} />
          <SummaryRow label="URL" value={deliveryMethod.url} />
          <SummaryRow label="Username" value={deliveryMethod.username} />
          <SummaryRow label="Password" value={passwordDisplay} />
          <SummaryRow label="Deadline" value={deliveryMethod.deadline} />
          <SummaryRow label="Note" value={deliveryMethod.note} />
        </div>
      ) : (
        <p className="rounded-md bg-slate-50 px-3 py-2 text-sm text-slate-600">
          No delivery method selected yet.
        </p>
      )}
    </section>
  );
}

function asString(value: string | null | undefined): string {
  return (value ?? "").trim();
}
