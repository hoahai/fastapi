import { AppDropdown } from "@/components/ui/app-dropdown";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";

import { LabeledField, ReadOnlyValue } from "./FormFieldRow";

const MEDIA_TYPE_OPTIONS = ["TV", "RA", "CA", "OD", "NP", "CINE", "OTT"].map((value) => ({
  value,
  label: value,
}));

const LANGUAGE_OPTIONS = [
  { value: "English", label: "English" },
  { value: "Spanish", label: "Spanish" },
];

export type StationDraftStation = {
  code: string;
  name: string;
  affiliation: string;
  mediaType: string;
  syscode: string;
  language: string;
  ownership: string;
  note: string;
  deliveryMethodId?: number | null;
};

interface StationBasicInfoSectionProps {
  station: StationDraftStation;
  isEditMode: boolean;
  isSubmitting: boolean;
  onChange: (next: Partial<StationDraftStation>) => void;
}

function RequiredMark() {
  return <span className="ml-1 text-rose-600">*</span>;
}

export function StationBasicInfoSection({
  station,
  isEditMode,
  isSubmitting,
  onChange,
}: StationBasicInfoSectionProps) {
  return (
    <section className="min-w-0 space-y-3">
      <div className="flex min-h-10 items-center justify-between gap-3 border-b border-slate-200 pb-2">
        <h3 className="text-sm font-semibold text-slate-800">Basic Information</h3>
        <div aria-hidden className="h-8 w-[76px]" />
      </div>
      <div className="space-y-3">
        <LabeledField
          label={
            <>
              Code<RequiredMark />
            </>
          }
        >
          {isEditMode ? (
            <ReadOnlyValue value={station.code} />
          ) : (
            <Input
              value={station.code}
              onChange={(event) => onChange({ code: event.target.value.toUpperCase() })}
              placeholder="e.g. KABC"
              maxLength={50}
              disabled={isSubmitting}
            />
          )}
        </LabeledField>

        <LabeledField
          label={
            <>
              Name<RequiredMark />
            </>
          }
        >
          <Input
            value={station.name}
            onChange={(event) => onChange({ name: event.target.value })}
            placeholder="Station name"
            maxLength={255}
            disabled={isSubmitting}
          />
        </LabeledField>

        <LabeledField label="Affiliation">
          <Input
            value={station.affiliation}
            onChange={(event) => onChange({ affiliation: event.target.value })}
            placeholder="Optional"
            maxLength={255}
            disabled={isSubmitting}
          />
        </LabeledField>

        <LabeledField
          label={
            <>
              Media Type<RequiredMark />
            </>
          }
        >
          <AppDropdown
            ariaLabel="Station media type"
            value={station.mediaType}
            options={MEDIA_TYPE_OPTIONS}
            onValueChange={(value) => onChange({ mediaType: value })}
            placeholder="Select media type"
            disabled={isSubmitting}
            searchable={false}
            className="w-full"
            emptyText="No media type found."
          />
        </LabeledField>

        {station.mediaType.trim().toUpperCase() === "CA" ? (
          <LabeledField
            label={
              <>
                Syscode<RequiredMark />
              </>
            }
          >
            <Input
              type="number"
              min={0}
              step={1}
              value={station.syscode}
              onChange={(event) => onChange({ syscode: event.target.value })}
              placeholder="e.g. 1001"
              disabled={isSubmitting}
            />
          </LabeledField>
        ) : null}

        <LabeledField
          label={
            <>
              Language<RequiredMark />
            </>
          }
        >
          <AppDropdown
            ariaLabel="Station language"
            value={station.language}
            options={LANGUAGE_OPTIONS}
            onValueChange={(value) => onChange({ language: value })}
            placeholder="Select language"
            disabled={isSubmitting}
            searchable={false}
            className="w-full"
            emptyText="No language found."
          />
        </LabeledField>

        <LabeledField label="Ownership">
          <Input
            value={station.ownership}
            onChange={(event) => onChange({ ownership: event.target.value })}
            placeholder="Optional"
            maxLength={255}
            disabled={isSubmitting}
          />
        </LabeledField>

        <LabeledField label="Note" alignStart>
          <Textarea
            value={station.note}
            onChange={(event) => onChange({ note: event.target.value })}
            placeholder="Optional note"
            maxLength={2048}
            disabled={isSubmitting}
          />
        </LabeledField>
      </div>
    </section>
  );
}
