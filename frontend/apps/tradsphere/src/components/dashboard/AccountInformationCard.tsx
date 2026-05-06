import { type ReactNode, useState } from "react";
import { createPortal } from "react-dom";
import { Save } from "lucide-react";

import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { Textarea } from "@/components/ui/textarea";

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

function LabeledField({
  label,
  children,
  alignStart = false,
}: {
  label: string;
  children: ReactNode;
  alignStart?: boolean;
}) {
  return (
    <div className={`grid grid-cols-[110px_1fr] gap-4 ${alignStart ? "items-start" : "items-center"}`}>
      <p className={`text-sm text-slate-600 ${alignStart ? "pt-2" : ""}`}>{label}</p>
      <div>{children}</div>
    </div>
  );
}

function ReadOnlyValue({ value }: { value?: string }) {
  const displayValue = value?.trim() ? value : "-";
  return <div className="min-h-10 py-2 text-sm text-slate-800 cursor-default select-text">{displayValue}</div>;
}

function LogoBlock({ account }: { account: AccountInfo | null }) {
  const hasLogo = Boolean(account?.logoUrl);
  const [isZoomOpen, setIsZoomOpen] = useState(false);
  const canRenderPortal = typeof document !== "undefined";

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

      {hasLogo && isZoomOpen && canRenderPortal
        ? createPortal(
            <div
              className="fixed inset-0 z-[9999] grid h-[100dvh] w-screen place-items-center bg-slate-900/75 p-6"
              onClick={() => setIsZoomOpen(false)}
            >
              <button
                type="button"
                className="absolute right-5 top-5 rounded-md bg-white/90 px-3 py-1 text-sm font-medium text-slate-900"
                onClick={(event) => {
                  event.stopPropagation();
                  setIsZoomOpen(false);
                }}
                aria-label="Close logo preview"
              >
                Close
              </button>
              <img
                src={account?.logoUrl ?? undefined}
                alt={`${account?.name ?? "Account"} logo enlarged`}
                className="max-h-[90vh] max-w-[90vw] rounded-xl bg-white object-contain p-4 shadow-2xl"
                onClick={(event) => event.stopPropagation()}
              />
            </div>,
            document.body,
          )
        : null}
    </>
  );
}
