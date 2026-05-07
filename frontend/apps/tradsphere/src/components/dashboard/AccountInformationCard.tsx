import { useState } from "react";
import { Save, X } from "lucide-react";

import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogClose,
  DialogContent,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { Textarea } from "@/components/ui/textarea";

import { LabeledField, ReadOnlyValue } from "./FormFieldRow";
import type { AccountInfo } from "./types";

interface AccountInformationCardProps {
  account: AccountInfo | null;
  isSaving: boolean;
  hasEditableChanges: boolean;
  saveError: string | null;
  onBillingTypeChange: (value: string) => void;
  onMarketChange: (value: string) => void;
  onNoteChange: (value: string) => void;
  onSave: () => void;
}

const BILLING_OPTIONS = [
  { label: "Calendar", value: "Calendar" },
  { label: "Broadcast", value: "Broadcast" },
];

export function AccountInformationCard({
  account,
  isSaving,
  hasEditableChanges,
  saveError,
  onBillingTypeChange,
  onMarketChange,
  onNoteChange,
  onSave,
}: AccountInformationCardProps) {
  const editableDisabled = !account || isSaving;

  return (
    <Card className="h-fit border-blue-100 shadow-[0_14px_30px_-18px_rgba(37,99,235,0.45)]">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-semibold text-blue-700">Account Information</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6 pb-6">
        <LogoBlock account={account} />

        <div className="space-y-4">
          <LabeledField label="Code">
            <ReadOnlyValue value={account?.code} />
          </LabeledField>

          <LabeledField label="Name">
            <ReadOnlyValue value={account?.name} />
          </LabeledField>

          <LabeledField label="Billing Type">
            <AppDropdown
              ariaLabel="Billing type"
              value={account?.billingType || BILLING_OPTIONS[0].value}
              onValueChange={onBillingTypeChange}
              options={BILLING_OPTIONS.map((option) => ({ value: option.value, label: option.label }))}
              disabled={editableDisabled}
              searchable={false}
              className="w-full"
              placeholder="Select billing type"
              emptyText="No billing type found."
            />
          </LabeledField>

          <LabeledField label="Market">
            <Input
              value={account?.market ?? ""}
              disabled={editableDisabled}
              onChange={(event) => onMarketChange(event.target.value)}
            />
          </LabeledField>

          <LabeledField label="Note" alignStart>
            <Textarea
              value={account?.note ?? ""}
              disabled={editableDisabled}
              onChange={(event) => onNoteChange(event.target.value)}
            />
          </LabeledField>
        </div>

        {saveError ? <p className="text-sm text-rose-600">{saveError}</p> : null}

        {hasEditableChanges ? (
          <div className="flex justify-end">
            <Button onClick={onSave} disabled={editableDisabled} className="min-w-28">
              {isSaving ? (
                <>
                  <Spinner />
                  Saving
                </>
              ) : (
                <>
                  <Save className="size-4" />
                  Save
                </>
              )}
            </Button>
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}

function LogoBlock({ account }: { account: AccountInfo | null }) {
  const hasLogo = Boolean(account?.logoUrl);
  const [isZoomOpen, setIsZoomOpen] = useState(false);

  return (
    <>
      <div className="relative h-40 overflow-hidden rounded-xl border border-slate-200 bg-slate-50">
        {hasLogo ? (
          <button
            type="button"
            className="flex h-full w-full cursor-zoom-in items-center justify-center overflow-hidden p-4"
            onClick={() => setIsZoomOpen(true)}
            aria-label="Open logo preview"
          >
            <img
              src={account?.logoUrl ?? undefined}
              alt={`${account?.name ?? "Account"} logo`}
              className="block h-auto w-auto max-h-full max-w-full object-contain object-center"
            />
          </button>
        ) : (
          <p className="text-center text-sm font-semibold text-slate-500">
            {account?.name ? `${account.name} Logo` : "No Logo"}
          </p>
        )}
      </div>

      {hasLogo ? (
        <Dialog open={isZoomOpen} onOpenChange={setIsZoomOpen}>
          <DialogContent
            className="w-[calc(100vw-2.5rem)] max-w-3xl border-none bg-transparent p-0 shadow-none"
            aria-describedby={undefined}
          >
            <div className="relative flex w-full items-center justify-center overflow-hidden rounded-2xl bg-white px-6 pb-6 pt-14 shadow-2xl sm:px-8 sm:pb-8 sm:pt-16">
              <DialogClose
                className="absolute right-4 top-4 z-10 rounded-md bg-slate-900/85 p-1.5 text-white transition-colors hover:bg-slate-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                aria-label="Close logo preview"
              >
                <X className="size-4" />
              </DialogClose>
              <img
                src={account?.logoUrl ?? undefined}
                alt={`${account?.name ?? "Account"} logo enlarged`}
                className="mx-auto block h-auto max-h-[70vh] w-auto max-w-full object-contain"
              />
            </div>
          </DialogContent>
        </Dialog>
      ) : null}
    </>
  );
}
