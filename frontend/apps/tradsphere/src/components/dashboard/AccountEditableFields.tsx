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
  onBillingTypeChange: (value: string) => void;
  onMarketChange: (value: string) => void;
  onNoteChange: (value: string) => void;
  disabled?: boolean;
  billingAriaLabel?: string;
  marketPlaceholder?: string;
  notePlaceholder?: string;
};

export function AccountEditableFields({
  billingType,
  market,
  note,
  onBillingTypeChange,
  onMarketChange,
  onNoteChange,
  disabled = false,
  billingAriaLabel = "Billing type",
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
    </>
  );
}
