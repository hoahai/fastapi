import { AlertCircle } from "lucide-react";

import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";

import type { AccountSelection } from "./types";

interface AccountSelectorProps {
  selectedAccountCode: string;
  options: AccountSelection[];
  isLoadingSelections: boolean;
  selectionsError: string | null;
  isLoadingAccount: boolean;
  isRefreshingAccount?: boolean;
  isSavingAccount: boolean;
  onAccountChange: (value: string) => void;
  onLoad: () => void;
}

export function AccountSelector({
  selectedAccountCode,
  options,
  isLoadingSelections,
  selectionsError,
  isLoadingAccount,
  isRefreshingAccount = false,
  isSavingAccount,
  onAccountChange,
  onLoad,
}: AccountSelectorProps) {
  const isControlLocked = isLoadingSelections || isLoadingAccount || isRefreshingAccount || isSavingAccount;
  const hasValidSelection = options.some((option) => option.accountCode === selectedAccountCode);

  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-center gap-3">
        <AppDropdown
          ariaLabel="Account selector"
          className="max-w-sm"
          value={selectedAccountCode}
          onValueChange={onAccountChange}
          options={options.map((option) => ({
            value: option.accountCode,
            label: option.label,
            keywords: option.name,
            muted: option.active === false,
          }))}
          placeholder={isLoadingSelections ? "Loading accounts..." : "Select account"}
          searchable
          loading={isLoadingSelections}
          disabled={isControlLocked}
          emptyText="No account found."
        />

        {isLoadingSelections ? <Spinner className="text-blue-600" /> : null}

        <Button
          className="min-w-28 rounded-lg"
          disabled={!hasValidSelection || isControlLocked}
          onClick={onLoad}
        >
          {isLoadingAccount ? (
            <>
              <Spinner />
              Loading
            </>
          ) : (
            "Load"
          )}
        </Button>
      </div>

      {selectionsError ? (
        <p className="flex items-center gap-2 text-sm text-rose-600">
          <AlertCircle className="size-4" />
          {selectionsError}
        </p>
      ) : null}
    </section>
  );
}
