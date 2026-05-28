import { AppDropdown } from "@/components/ui/app-dropdown";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";

import { LabeledField } from "./FormFieldRow";

export const ACCOUNT_BILLING_OPTIONS = [
  { label: "Calendar", value: "Calendar" },
  { label: "Broadcast", value: "Broadcast" },
];

type AccountEditableFieldsProps = {
  billingType: string;
  market: string;
  note: string;
  active?: boolean;
  onBillingTypeChange: (value: string) => void;
  onMarketChange: (value: string) => void;
  onNoteChange: (value: string) => void;
  onActiveChange?: (value: boolean) => void;
  disabled?: boolean;
  billingAriaLabel?: string;
  activeAriaLabel?: string;
  marketPlaceholder?: string;
  notePlaceholder?: string;
};

export function AccountEditableFields({
  billingType,
  market,
  note,
  active,
  onBillingTypeChange,
  onMarketChange,
  onNoteChange,
  onActiveChange,
  disabled = false,
  billingAriaLabel = "Billing type",
  activeAriaLabel = "Active",
  marketPlaceholder,
  notePlaceholder,
}: AccountEditableFieldsProps) {
  return (
    <>
      <LabeledField label="Billing Type">
        <AppDropdown
          ariaLabel={billingAriaLabel}
          value={billingType || ACCOUNT_BILLING_OPTIONS[0].value}
          onValueChange={onBillingTypeChange}
          options={ACCOUNT_BILLING_OPTIONS}
          disabled={disabled}
          searchable={false}
          className="w-full"
          placeholder="Select billing type"
          emptyText="No billing type found."
        />
      </LabeledField>

      <LabeledField label="Market">
        <Input
          value={market}
          disabled={disabled}
          placeholder={marketPlaceholder}
          onChange={(event) => onMarketChange(event.target.value)}
        />
      </LabeledField>

      <LabeledField label="Note" alignStart>
        <Textarea
          value={note}
          disabled={disabled}
          placeholder={notePlaceholder}
          onChange={(event) => onNoteChange(event.target.value)}
        />
      </LabeledField>

      {typeof active === "boolean" && onActiveChange ? (
        <LabeledField label="Active">
          <button
            type="button"
            role="switch"
            aria-checked={active}
            aria-label={activeAriaLabel}
            onClick={() => onActiveChange(!active)}
            disabled={disabled}
            aria-disabled={disabled}
            className={`inline-flex h-10 w-fit items-center gap-3 px-1 py-2 text-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
              active ? "text-blue-700" : "text-slate-600"
            }`}
          >
            <span
              className={`relative inline-flex h-5 w-9 items-center rounded-full transition ${
                active ? "bg-blue-500" : "bg-slate-300"
              }`}
            >
              <span
                className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${
                  active ? "translate-x-[18px]" : "translate-x-[2px]"
                }`}
              />
            </span>
          </button>
        </LabeledField>
      ) : null}
    </>
  );
}
