import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import {
  CheckCircle2,
  ChevronDown,
  Plus,
  RefreshCw,
  Search,
  X,
} from "lucide-react";

import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { Section, SectionHeader } from "@shared/components";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { ModalCloseButton, ModalFooter, ModalHeaderRow, ModalShell } from "@shared/components";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { useCommittedTextField } from "@shared/hooks/useCommittedTextField";
import { useOnlineStatus } from "@shared/hooks/useOnlineStatus";
import { useScopedPersistentState } from "@shared/hooks/useScopedPersistentState";
import { shouldFetchNetwork, type CachePolicy } from "@shared/cache";
import { shouldProtectFrontendAuth } from "@shared/auth/guards";
import { hasAppEditAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { cn } from "@shared/components/utils/cn";
import { TooltipTarget } from "@shared/components/actions/TooltipTarget";
import { PageBanner } from "@shell/components/layout/PageBanner";
import { Button } from "@tradsphere/components/ui/button";
import { AppDropdown, type AppDropdownOption } from "@tradsphere/components/ui/app-dropdown";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@tradsphere/components/ui/dialog";
import { Input } from "@tradsphere/components/ui/input";
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";
import { FormRow } from "@shared/components/form/FormRow";
import { ReadOnlyField } from "@shared/components/form/ReadOnlyField";

import {
  createFundsphereAccount,
  loadFundsphereAccounts,
  normalizeFundsphereAccountForm,
  updateFundsphereAccount,
  type FundsphereAccount,
  type FundsphereAccountFormState,
  type FundsphereRequestJson,
} from "@fundsphere/lib/accountsApi";
import {
  buildFundsphereAccountsCacheKey,
  readFundsphereAccountsCacheSnapshot,
  syncFundsphereAccountsCache,
  FUNDSPHERE_ACCOUNTS_PAGE_CODE,
  type FundsphereAccountsCacheContext,
} from "@fundsphere/lib/accountsCache";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type AccountMode = "create" | "edit";
type AccountGroupKey = "active" | "inactive";
type AccountStatusFilter = "" | "active" | "inactive";
type AccountSearchCriteria = {
  code: string;
  name: string;
  aeName: string;
  statusFilter: AccountStatusFilter;
};

type PersistedAccountsPageState = {
  searchDraft: AccountSearchCriteria;
  searchCriteria: AccountSearchCriteria;
  hasSearched: boolean;
};

type AccountModalProps = {
  open: boolean;
  mode: AccountMode;
  account: FundsphereAccount | null;
  canEdit: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (payload: {
    mode: AccountMode;
    accountCode: string | null;
    form: FundsphereAccountFormState;
  }) => Promise<void>;
};

type AccountGroup = {
  key: AccountGroupKey;
  label: string;
  items: FundsphereAccount[];
};

type FundsphereAccountRep = {
  accountCode: string;
  employeeId: string;
};

type LeaveSphereEmployeeLookup = {
  id: string;
  fullName: string;
};

const FUNDSPHERE_APP_CODE = "fundsphere";
const DEFAULT_ACCOUNT_STATUS_FILTER: AccountStatusFilter = "active";
const EMPTY_ACCOUNT_STATUS_FILTER: AccountStatusFilter = "";
const DEFAULT_SEARCH_CRITERIA: AccountSearchCriteria = {
  code: "",
  name: "",
  aeName: "",
  statusFilter: DEFAULT_ACCOUNT_STATUS_FILTER,
};
const EMPTY_SEARCH_CRITERIA: AccountSearchCriteria = {
  code: "",
  name: "",
  aeName: "",
  statusFilter: EMPTY_ACCOUNT_STATUS_FILTER,
};
const EMPTY_PAGE_STATE: PersistedAccountsPageState = {
  searchDraft: { ...DEFAULT_SEARCH_CRITERIA },
  searchCriteria: { ...DEFAULT_SEARCH_CRITERIA },
  hasSearched: false,
};
const STATUS_OPTIONS: AppDropdownOption[] = [
  { value: "", label: "" },
  { value: "active", label: "Active" },
  { value: "inactive", label: "Inactive" },
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return "";
}

function formatRelativeTime(timestamp: number): string {
  const ageMs = Math.max(0, Date.now() - timestamp);
  if (ageMs < 60_000) {
    return "just now";
  }
  const minutes = Math.floor(ageMs / 60_000);
  if (minutes < 60) {
    return `${minutes} min ago`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours} hr ago`;
  }
  const days = Math.floor(hours / 24);
  return `${days} day${days === 1 ? "" : "s"} ago`;
}

function getTodayIsoDate(): string {
  const today = new Date();
  const year = today.getFullYear();
  const month = String(today.getMonth() + 1).padStart(2, "0");
  const day = String(today.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function normalizeStatusFilterValue(value: string): AccountStatusFilter {
  if (value === "active" || value === "inactive") {
    return value;
  }
  return "";
}

function hasAccountSearchCriteria(criteria: AccountSearchCriteria): boolean {
  return (
    Boolean(criteria.code.trim()) ||
    Boolean(criteria.name.trim()) ||
    Boolean(criteria.aeName.trim()) ||
    Boolean(criteria.statusFilter)
  );
}

function areAccountSearchCriteriaEqual(left: AccountSearchCriteria, right: AccountSearchCriteria): boolean {
  return (
    left.code.trim() === right.code.trim() &&
    left.name.trim() === right.name.trim() &&
    left.aeName.trim() === right.aeName.trim() &&
    left.statusFilter === right.statusFilter
  );
}

function sortAccounts(accounts: FundsphereAccount[]): FundsphereAccount[] {
  return [...accounts].sort((left, right) => left.code.localeCompare(right.code));
}

function buildEmployeeFullName(firstName: string, lastName: string): string {
  return [firstName, lastName].map((part) => part.trim()).filter(Boolean).join(" ");
}

function normalizeFundsphereAccountRep(value: unknown): FundsphereAccountRep | null {
  if (!isRecord(value)) {
    return null;
  }

  const accountCode = asString(value.accountCode).toUpperCase();
  const employeeId = asString(value.employeeId);
  if (!accountCode || !employeeId) {
    return null;
  }

  return {
    accountCode,
    employeeId,
  };
}

function normalizeLeaveSphereEmployeeLookup(value: unknown): LeaveSphereEmployeeLookup | null {
  if (!isRecord(value)) {
    return null;
  }

  const id = asString(value.id);
  const fullName = buildEmployeeFullName(asString(value.firstName), asString(value.lastName));
  if (!id || !fullName) {
    return null;
  }

  return {
    id,
    fullName,
  };
}

async function loadFundsphereAccountRepMetadata(requestJson: FundsphereRequestJson): Promise<{
  accountReps: FundsphereAccountRep[];
  employeeLookup: Record<string, string>;
}> {
  const [accountRepsResult, employeesResult] = await Promise.allSettled([
    requestJson("/api/fundsphere/v1/accountReps", { errorToast: false }),
    requestJson("/api/leavesphere/v1/employees", { errorToast: false }),
  ]);

  const accountReps =
    accountRepsResult.status === "fulfilled"
      ? (() => {
          const payload = unwrapResponseData(accountRepsResult.value);
          return Array.isArray(payload)
            ? payload
              .map((item) => normalizeFundsphereAccountRep(item))
              .filter((item): item is FundsphereAccountRep => item !== null)
            : [];
        })()
      : [];

  const employeeLookupEntries =
    employeesResult.status === "fulfilled"
      ? (() => {
          const payload = unwrapResponseData(employeesResult.value);
          return Array.isArray(payload)
            ? payload
              .map((item) => normalizeLeaveSphereEmployeeLookup(item))
              .filter((item): item is LeaveSphereEmployeeLookup => item !== null)
            : [];
        })()
      : [];

  return {
    accountReps,
    employeeLookup: Object.fromEntries(
      employeeLookupEntries.map((item) => [item.id, item.fullName] as const),
    ),
  };
}

function unwrapResponseData(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

function buildAccountGroups(accounts: FundsphereAccount[]): AccountGroup[] {
  const activeAccounts = accounts.filter((account) => account.active);
  const inactiveAccounts = accounts.filter((account) => !account.active);

  const groups: AccountGroup[] = [
    { key: "active", label: "Active Accounts", items: activeAccounts },
    { key: "inactive", label: "Inactive Accounts", items: inactiveAccounts },
  ];

  return groups.filter((group) => group.items.length > 0);
}

function buildEmptyMessage(params: {
  hasSearched: boolean;
  hasFilters: boolean;
  hasAccounts: boolean;
  hasMatches: boolean;
  statusFilter: AccountStatusFilter;
}): { title: string; description: string } {
  if (!params.hasSearched) {
    return {
      title: "Search accounts",
      description: "Load the account list, then use the filters to narrow results.",
    };
  }
  if (!params.hasAccounts) {
    return {
      title: "No accounts yet",
      description: "Create the first FundSphere account to start managing budgets.",
    };
  }
  if (!params.hasMatches) {
    if (params.statusFilter === "active") {
      return {
        title: "No active accounts",
        description: "No active accounts matched the current filters. Switch Status to Inactive or clear the filters to see inactive accounts.",
      };
    }
    if (params.hasFilters) {
      return {
        title: "No matches",
        description: "No accounts matched the current filters. Adjust the filters and search again.",
      };
    }
  }
  if (params.hasFilters) {
    return {
      title: "No matches",
      description: "No accounts match the current filters. Clear the filters to see the full list.",
    };
  }
  return {
    title: "No accounts found",
    description: "The workspace did not return any account rows.",
  };
}

function isValidIsoDate(value: string): boolean {
  if (!value) {
    return true;
  }
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function isPersistedAccountsPageState(value: unknown): value is PersistedAccountsPageState {
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.hasSearched !== "boolean" || !isRecord(value.searchDraft) || !isRecord(value.searchCriteria)) {
    return false;
  }
  return (
    typeof value.searchDraft.code === "string" &&
    typeof value.searchDraft.name === "string" &&
    typeof value.searchDraft.aeName === "string" &&
    typeof value.searchDraft.statusFilter === "string" &&
    typeof value.searchCriteria.code === "string" &&
    typeof value.searchCriteria.name === "string" &&
    typeof value.searchCriteria.aeName === "string" &&
    typeof value.searchCriteria.statusFilter === "string"
  );
}

function toAccountForm(account: FundsphereAccount | null): FundsphereAccountFormState {
  return normalizeFundsphereAccountForm(account);
}

function toUiAccount(account: FundsphereAccount): FundsphereAccount {
  return {
    ...account,
    code: account.code.toUpperCase(),
    name: account.name,
    logoUrl: account.logoUrl ? asString(account.logoUrl) : null,
    conseroId: account.conseroId ? asString(account.conseroId) : null,
    conseroName: account.conseroName ? asString(account.conseroName) : null,
    strataName: account.strataName ? asString(account.strataName) : null,
    endDate: account.endDate ? account.endDate.slice(0, 10) : null,
  };
}

function buildAccountPayloadFromForm(form: FundsphereAccountFormState): FundsphereAccount {
  return {
    code: asString(form.code).toUpperCase(),
    name: asString(form.name),
    logoUrl: asString(form.logoUrl) || null,
    conseroId: asString(form.conseroId) || null,
    conseroName: asString(form.conseroName) || null,
    strataName: asString(form.strataName) || null,
    active: Boolean(form.active),
    endDate: asString(form.endDate) || null,
  };
}

function validateAccountForm(form: FundsphereAccountFormState, mode: AccountMode): {
  code: string | null;
  name: string | null;
  logoUrl: string | null;
  conseroId: string | null;
  conseroName: string | null;
  strataName: string | null;
  endDate: string | null;
} {
  const code = asString(form.code).toUpperCase();
  const name = asString(form.name);
  const logoUrl = asString(form.logoUrl);
  const conseroId = asString(form.conseroId);
  const conseroName = asString(form.conseroName);
  const strataName = asString(form.strataName);
  const endDate = asString(form.endDate);

  return {
    code: mode === "create" && !code ? "Code is required." : code.length > 10 ? "Code must be 10 characters or fewer." : null,
    name: !name ? "Name is required." : name.length > 255 ? "Name must be 255 characters or fewer." : null,
    logoUrl: logoUrl.length > 2048 ? "Logo URL must be 2048 characters or fewer." : null,
    conseroId: conseroId.length > 10 ? "Consero ID must be 10 characters or fewer." : null,
    conseroName: conseroName.length > 255 ? "Consero name must be 255 characters or fewer." : null,
    strataName: strataName.length > 255 ? "Strata name must be 255 characters or fewer." : null,
    endDate: !isValidIsoDate(endDate) ? "End date must be a date." : null,
  };
}

function AccountModal({
  open,
  mode,
  account,
  canEdit,
  onOpenChange,
  onSubmit,
}: AccountModalProps) {
  const [form, setForm] = useState<FundsphereAccountFormState>(() => toAccountForm(account));
  const [baseline, setBaseline] = useState<FundsphereAccountFormState>(() => toAccountForm(account));
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);

  const formErrors = useMemo(() => validateAccountForm(form, mode), [form, mode]);
  const formIsValid = useMemo(() => Object.values(formErrors).every((item) => item === null), [formErrors]);
  const hasUnsavedChanges = useMemo(() => JSON.stringify(form) !== JSON.stringify(baseline), [baseline, form]);
  const canSubmit = canEdit && hasUnsavedChanges && formIsValid && !isSubmitting;
  const primaryActionLabel = mode === "create" ? "Create Account" : "Save Changes";

  const codeField = useCommittedTextField<HTMLInputElement>(
    form.code,
    (value) => {
      setForm((current) => ({ ...current, code: value.toUpperCase() }));
      if (submitError) {
        setSubmitError(null);
      }
    },
    { normalizeOnBlur: (value) => value.trim().toUpperCase() },
  );
  const nameField = useCommittedTextField<HTMLInputElement>(form.name, (value) => {
    setForm((current) => ({ ...current, name: value }));
    if (submitError) {
      setSubmitError(null);
    }
  });
  const logoUrlField = useCommittedTextField<HTMLInputElement>(form.logoUrl, (value) => {
    setForm((current) => ({ ...current, logoUrl: value }));
    if (submitError) {
      setSubmitError(null);
    }
  });
  const conseroIdField = useCommittedTextField<HTMLInputElement>(form.conseroId, (value) => {
    setForm((current) => ({ ...current, conseroId: value }));
    if (submitError) {
      setSubmitError(null);
    }
  });
  const conseroNameField = useCommittedTextField<HTMLInputElement>(form.conseroName, (value) => {
    setForm((current) => ({ ...current, conseroName: value }));
    if (submitError) {
      setSubmitError(null);
    }
  });
  const strataNameField = useCommittedTextField<HTMLInputElement>(form.strataName, (value) => {
    setForm((current) => ({ ...current, strataName: value }));
    if (submitError) {
      setSubmitError(null);
    }
  });

  useEffect(() => {
    if (!open) {
      setForm(toAccountForm(null));
      setBaseline(toAccountForm(null));
      setIsSubmitting(false);
      setSubmitError(null);
      setIsDiscardDialogOpen(false);
      return;
    }

    const nextForm = toAccountForm(account);
    setForm(nextForm);
    setBaseline(nextForm);
    setSubmitError(null);
    setIsSubmitting(false);
    setIsDiscardDialogOpen(false);
  }, [account, open]);

  function updateForm<K extends keyof FundsphereAccountFormState>(
    field: K,
    value: FundsphereAccountFormState[K],
  ) {
    setForm((current) => ({
      ...current,
      [field]: value,
    }));
    if (submitError) {
      setSubmitError(null);
    }
  }

  function setActive(nextActive: boolean) {
    setForm((current) => ({
      ...current,
      active: nextActive,
      endDate: nextActive ? "" : current.endDate || getTodayIsoDate(),
    }));
    if (submitError) {
      setSubmitError(null);
    }
  }

  function restoreBaseline() {
    setForm(baseline);
    setSubmitError(null);
  }

  function handleDialogOpenChange(nextOpen: boolean) {
    if (nextOpen) {
      onOpenChange(true);
      return;
    }
    if (isSubmitting) {
      return;
    }
    if (hasUnsavedChanges) {
      setIsDiscardDialogOpen(true);
      return;
    }
    onOpenChange(false);
  }

  async function handleSubmit() {
    if (!canSubmit) {
      return;
    }

    setIsSubmitting(true);
    setSubmitError(null);
    try {
      await onSubmit({
        mode,
        accountCode: account?.code ?? null,
        form: {
          ...form,
          code: asString(form.code).toUpperCase(),
          name: asString(form.name),
          logoUrl: asString(form.logoUrl),
          conseroId: asString(form.conseroId),
          conseroName: asString(form.conseroName),
          strataName: asString(form.strataName),
          endDate: asString(form.endDate),
        },
      });
      onOpenChange(false);
    } catch (error) {
      setSubmitError(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not save account.");
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          className="max-h-[92vh] w-[min(94vw,1040px)] max-w-none overflow-hidden rounded-[1.6rem] bg-white px-7 py-6"
          onInteractOutside={(event) => {
            if (isSubmitting || hasUnsavedChanges) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={isSubmitting} busyMessage={mode === "create" ? "Creating account..." : "Saving account..."} className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close account modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>{mode === "create" ? "Add Account" : "Edit Account"}</DialogTitle>
                <DialogDescription>
                  {mode === "create"
                    ? "Create a FundSphere account record and make it available for budgeting."
                    : "Update the account record and keep the reference data current."}
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>

            <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1">
              <div className="grid grid-cols-1 gap-8 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                <Section className="space-y-3">
                  <SectionHeader
                    title="Identity"
                    description="Core account information and status."
                  />

                  <FormRow
                    label={(
                      <>
                        Code<span className="ml-1 text-rose-600">*</span>
                      </>
                    )}
                  >
                    {mode === "create" ? (
                      <Input
                        {...codeField}
                        disabled={isSubmitting || !canEdit}
                        maxLength={10}
                        autoComplete="off"
                        spellCheck={false}
                        className="uppercase"
                      />
                    ) : (
                      <ReadOnlyField value={account?.code ?? baseline.code} />
                    )}
                  </FormRow>
                  {formErrors.code ? <p className="text-sm text-rose-600">{formErrors.code}</p> : null}

                  <FormRow
                    label={(
                      <>
                        Name<span className="ml-1 text-rose-600">*</span>
                      </>
                    )}
                  >
                    <Input
                      {...nameField}
                      disabled={isSubmitting || !canEdit}
                      maxLength={255}
                      autoComplete="off"
                    />
                  </FormRow>
                  {formErrors.name ? <p className="text-sm text-rose-600">{formErrors.name}</p> : null}

                  <FormRow label="Active">
                    <button
                      type="button"
                      role="switch"
                      aria-checked={form.active}
                      aria-label="Active"
                      onClick={() => setActive(!form.active)}
                      disabled={isSubmitting || !canEdit}
                      aria-disabled={isSubmitting || !canEdit}
                      className={cn(
                        "inline-flex h-10 w-fit items-center gap-3 px-1 py-2 text-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                        form.active ? "text-emerald-700" : "text-slate-600",
                      )}
                    >
                      <span className={cn("relative inline-flex h-5 w-9 items-center rounded-full transition", form.active ? "bg-emerald-500" : "bg-slate-300")}>
                        <span className={cn("inline-block h-4 w-4 rounded-full bg-white shadow transition", form.active ? "translate-x-[18px]" : "translate-x-[2px]")} />
                      </span>
                    </button>
                  </FormRow>

                  <FormRow label="End Date">
                    <Input
                      type="date"
                      value={form.endDate}
                      onChange={(event) => updateForm("endDate", event.target.value)}
                      disabled={isSubmitting || !canEdit}
                      maxLength={10}
                    />
                  </FormRow>
                  {formErrors.endDate ? <p className="text-sm text-rose-600">{formErrors.endDate}</p> : null}
                  <p className="text-xs leading-5 text-slate-500">
                    Leave blank for active accounts. Setting an end date is useful when deactivating an account.
                  </p>
                </Section>

                <Section className="space-y-3">
                  <SectionHeader
                    title="Reference Data"
                    description="Optional fields used by other FundSphere workflows."
                  />

                  <FormRow label="Logo URL">
                    <Input
                      {...logoUrlField}
                      disabled={isSubmitting || !canEdit}
                      maxLength={2048}
                      autoComplete="off"
                      spellCheck={false}
                    />
                  </FormRow>
                  {formErrors.logoUrl ? <p className="text-sm text-rose-600">{formErrors.logoUrl}</p> : null}

                  <FormRow label="Consero ID">
                    <Input
                      {...conseroIdField}
                      disabled={isSubmitting || !canEdit}
                      maxLength={10}
                      autoComplete="off"
                      spellCheck={false}
                    />
                  </FormRow>
                  {formErrors.conseroId ? <p className="text-sm text-rose-600">{formErrors.conseroId}</p> : null}

                  <FormRow label="Consero Name">
                    <Input
                      {...conseroNameField}
                      disabled={isSubmitting || !canEdit}
                      maxLength={255}
                      autoComplete="off"
                      spellCheck={false}
                    />
                  </FormRow>
                  {formErrors.conseroName ? <p className="text-sm text-rose-600">{formErrors.conseroName}</p> : null}

                  <FormRow label="Strata Name">
                    <Input
                      {...strataNameField}
                      disabled={isSubmitting || !canEdit}
                      maxLength={255}
                      autoComplete="off"
                      spellCheck={false}
                    />
                  </FormRow>
                  {formErrors.strataName ? <p className="text-sm text-rose-600">{formErrors.strataName}</p> : null}
                </Section>
              </div>

              {submitError ? <p className="mt-4 text-sm text-rose-600">{submitError}</p> : null}
            </div>

            <ModalFooter className="mt-4 flex-col gap-2 border-t border-slate-100 pt-3 sm:flex-row sm:items-center sm:justify-between">
              <span className="text-xs text-slate-500">
                {form.active ? "Active accounts remain available for budget planning." : "Inactive accounts are soft-disabled, not deleted."}
              </span>
              <div className="flex items-center gap-2">
                {canEdit && hasUnsavedChanges ? (
                  <Button
                    variant="outline"
                    type="button"
                    onClick={restoreBaseline}
                    disabled={isSubmitting}
                  >
                    Revert
                  </Button>
                ) : null}
                {canEdit && hasUnsavedChanges ? (
                  <Button type="button" onClick={handleSubmit} disabled={!canSubmit}>
                    {isSubmitting ? (
                      <>
                        <RefreshCw className="size-4 animate-spin" />
                        {mode === "create" ? "Creating..." : "Saving..."}
                      </>
                    ) : (
                      primaryActionLabel
                    )}
                  </Button>
                ) : null}
              </div>
            </ModalFooter>
          </ModalShell>
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isDiscardDialogOpen}
        onKeepEditing={() => setIsDiscardDialogOpen(false)}
        onDiscardChanges={() => {
          setIsDiscardDialogOpen(false);
          onOpenChange(false);
        }}
      />
    </>
  );
}

function buildAccountStatusLabel(active: boolean): string {
  return active ? "Active" : "Inactive";
}

function buildAccountStatusClass(active: boolean): string {
  return active
    ? "border-emerald-200 bg-emerald-50/90 text-emerald-800"
    : "border-slate-300 bg-slate-100/90 text-slate-700";
}

function stripRolePrefix(value: string): string {
  return value.replace(/^(?:AR|AE)\s*:\s*/i, "").trim();
}

function AccountCard({
  account,
  aeNames,
  disabled,
  canEdit,
  onEdit,
}: {
  account: FundsphereAccount;
  aeNames?: string | null;
  disabled?: boolean;
  canEdit: boolean;
  onEdit: (account: FundsphereAccount) => void;
}) {
  const [logoError, setLogoError] = useState(false);
  const [logoPreviewOpen, setLogoPreviewOpen] = useState(false);
  useEffect(() => {
    setLogoError(false);
    setLogoPreviewOpen(false);
  }, [account.logoUrl]);

  const showLogo = Boolean(account.logoUrl) && !logoError;
  const logoFallback = (account.name || account.code || "AC").slice(0, 2).toUpperCase();
  const displayAeNames = aeNames ? stripRolePrefix(aeNames) : "";

  return (
    <article
      role="button"
      tabIndex={disabled || !canEdit ? -1 : 0}
      onClick={() => {
        if (!disabled && canEdit) {
          onEdit(account);
        }
      }}
      onKeyDown={(event) => {
        if (disabled || !canEdit) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onEdit(account);
        }
      }}
      className={cn(
        "rounded-[1.35rem] border p-4 shadow-[0_18px_30px_-24px_rgba(37,99,235,0.42)] transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
        disabled ? "cursor-default" : "cursor-pointer",
        account.active
          ? "border-blue-100/90 bg-white hover:-translate-y-0.5 hover:border-blue-200 hover:shadow-[0_20px_34px_-24px_rgba(37,99,235,0.5)]"
          : "border-slate-200 bg-slate-50/90 hover:-translate-y-0.5 hover:border-slate-300 hover:shadow-[0_18px_28px_-26px_rgba(15,23,42,0.22)]",
      )}
    >
      <div className="flex items-start gap-4">
        <button
          type="button"
          className="flex size-14 shrink-0 items-center justify-center overflow-hidden rounded-2xl bg-gradient-to-br from-slate-50 to-blue-50 shadow-[0_10px_20px_-16px_rgba(37,99,235,0.45)] transition hover:-translate-y-0.5 hover:shadow-[0_14px_24px_-18px_rgba(37,99,235,0.55)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
          onClick={(event) => {
            event.stopPropagation();
            if (showLogo) {
              setLogoPreviewOpen(true);
            }
          }}
          disabled={!showLogo}
          aria-label={showLogo ? `Preview ${account.name} logo` : `${account.name} has no logo`}
        >
          {showLogo ? (
            <img
              src={account.logoUrl ?? undefined}
              alt={`${account.name} logo`}
              className="h-full w-full object-contain p-2"
              loading="lazy"
              onError={() => setLogoError(true)}
            />
          ) : (
            <span className="text-sm font-semibold uppercase tracking-[0.16em] text-blue-800">
              {logoFallback}
            </span>
          )}
        </button>

        <div className="min-w-0 flex-1">
          <TooltipTarget text={account.name}>
            <p className="truncate text-lg font-semibold tracking-[-0.02em] text-slate-900">{account.name}</p>
          </TooltipTarget>

          <div className="mt-2 flex flex-wrap items-center gap-2">
            <span className="inline-flex items-center rounded-full border border-blue-100 bg-blue-50 px-2.5 py-1 text-xs font-semibold uppercase tracking-[0.12em] text-blue-800">
              {account.code}
            </span>
            <span
              className={cn(
                "inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium",
                buildAccountStatusClass(account.active),
              )}
            >
              {buildAccountStatusLabel(account.active)}
            </span>
          </div>

          {displayAeNames ? (
            <p className="mt-3 truncate text-sm text-slate-700">{displayAeNames}</p>
          ) : null}
        </div>
      </div>

      {showLogo ? (
        <Dialog open={logoPreviewOpen} onOpenChange={setLogoPreviewOpen}>
          <DialogContent
            className="w-[calc(100vw-2.5rem)] max-w-3xl border-none bg-transparent p-0 shadow-none"
            aria-describedby={undefined}
          >
            <div className="relative flex w-full items-center justify-center overflow-hidden rounded-2xl bg-white px-6 pb-6 pt-14 shadow-2xl sm:px-8 sm:pb-8 sm:pt-16">
              <DialogClose asChild aria-label="Close logo preview">
                <ModalCloseButton
                  icon={<X className="size-4" />}
                  className="absolute right-0 top-0 z-10 rounded-md bg-slate-900/85 p-1.5 text-white transition-colors hover:bg-slate-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                />
              </DialogClose>
              <img
                src={account.logoUrl ?? undefined}
                alt={`${account.name} logo enlarged`}
                className="mx-auto block h-auto max-h-[70vh] w-auto max-w-full object-contain"
              />
            </div>
          </DialogContent>
        </Dialog>
      ) : null}
    </article>
  );
}

function FundsphereAccountsPage() {
  const { requestJson } = useApiRequest();
  const { isOnline } = useOnlineStatus();
  const auth = useAuth();

  const cacheContext = useMemo<FundsphereAccountsCacheContext>(
    () => ({
      tenantSlug: auth.tenantSlug || "",
      userKey: auth.user?.id || auth.user?.email || "",
    }),
    [auth.tenantSlug, auth.user?.email, auth.user?.id],
  );
  const cacheKey = useMemo(() => buildFundsphereAccountsCacheKey(cacheContext), [cacheContext]);
  const canEditFundsphere = useMemo(() => {
    if (!shouldProtectFrontendAuth()) {
      return true;
    }
    return hasAppEditAccess(auth.accessProfile, FUNDSPHERE_APP_CODE);
  }, [auth.accessProfile]);

  const [pageState, setPageState, pageStateControls] = useScopedPersistentState<PersistedAccountsPageState>(
    {
      appCode: FUNDSPHERE_APP_CODE,
      pageCode: FUNDSPHERE_ACCOUNTS_PAGE_CODE,
      stateKey: "filters",
    },
    EMPTY_PAGE_STATE,
    { validate: isPersistedAccountsPageState },
  );

  const [accounts, setAccounts] = useState<FundsphereAccount[] | null>(null);
  const accountsRef = useRef<FundsphereAccount[] | null>(null);
  const [accountReps, setAccountReps] = useState<FundsphereAccountRep[] | null>(null);
  const [employeeLookup, setEmployeeLookup] = useState<Record<string, string>>({});
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const requestTokenRef = useRef(0);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState<AccountMode>("create");
  const [modalAccount, setModalAccount] = useState<FundsphereAccount | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [accountGroupOpenState, setAccountGroupOpenState] = useState<Record<AccountGroupKey, boolean>>({
    active: true,
    inactive: true,
  });

  useEffect(() => {
    accountsRef.current = accounts;
  }, [accounts]);

  const searchDraft = pageState.searchDraft;
  const searchCriteria = pageState.searchCriteria;
  const hasSearched = pageState.hasSearched;

  function commitAccounts(
    nextAccounts: FundsphereAccount[] | null,
    source: "cache" | "network",
    criteria: AccountSearchCriteria,
  ) {
    accountsRef.current = nextAccounts;
    setAccounts(nextAccounts);
    if (!nextAccounts) {
      return;
    }
    const fetchedAt = Date.now();
    setCacheStatus({ source, fetchedAt });
    setPageState((current) => ({
      ...current,
      hasSearched: true,
    }));
    syncFundsphereAccountsCache(cacheContext, nextAccounts, criteria, { source, fetchedAt });
  }

  async function refreshAccounts(policy: CachePolicy, criteria: AccountSearchCriteria = searchCriteria): Promise<void> {
    const requestToken = ++requestTokenRef.current;
    const snapshot = readFundsphereAccountsCacheSnapshot(cacheContext, criteria);
    const cachedAccounts = snapshot?.data ? sortAccounts(snapshot.data.map((item) => toUiAccount(item))) : null;
    const shouldFetch = shouldFetchNetwork(policy, snapshot);

    if (cachedAccounts) {
      commitAccounts(cachedAccounts, "cache", criteria);
      setErrorMessage(null);
      setRefreshMessage(null);
    }

    if (!shouldFetch) {
      setIsLoading(false);
      setIsRefreshing(false);
      return;
    }

    if (!isOnline) {
      if (cachedAccounts) {
        setRefreshMessage("You're offline. Showing cached accounts.");
      } else {
        setErrorMessage("You're offline. Connect to the internet to load accounts.");
      }
      setIsLoading(false);
      setIsRefreshing(false);
      return;
    }

    if (cachedAccounts) {
      setIsRefreshing(true);
    } else {
      setIsLoading(true);
      setRefreshMessage(null);
      setErrorMessage(null);
    }

    const metadataPromise = loadFundsphereAccountRepMetadata(requestJson);
    void metadataPromise
      .then((metadata) => {
        if (requestToken !== requestTokenRef.current) {
          return;
        }
        setAccountReps(metadata.accountReps);
        setEmployeeLookup(metadata.employeeLookup);
      })
      .catch(() => {
        // AE search can still use the previous metadata state when refresh fails.
      });

    try {
      const nextAccounts = sortAccounts(
        (await loadFundsphereAccounts({
          requestJson,
          criteria,
        })).map((item) => toUiAccount(item)),
      );
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      commitAccounts(nextAccounts, "network", criteria);
      setRefreshMessage(null);
      setErrorMessage(null);
    } catch (error) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      if (accountsRef.current || cachedAccounts) {
        setRefreshMessage("Showing cached accounts. Could not refresh.");
        setErrorMessage(null);
      } else {
        setErrorMessage(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not load accounts.");
      }
    } finally {
      if (requestToken === requestTokenRef.current) {
        setIsLoading(false);
        setIsRefreshing(false);
      }
    }
  }

  useEffect(() => {
    accountsRef.current = null;
    setAccounts(null);
    setCacheStatus(null);
    setRefreshMessage(null);
    setErrorMessage(null);
    setIsLoading(false);
    setIsRefreshing(false);
    setIsModalOpen(false);
    setModalMode("create");
    setModalAccount(null);
    setIsSaving(false);
    setAccountReps(null);
    setEmployeeLookup({});
    setAccountGroupOpenState({
      active: true,
      inactive: true,
    });
    ++requestTokenRef.current;
  }, [cacheKey]);

  useEffect(() => {
    if (!pageStateControls.hydrated) {
      return;
    }
    void refreshAccounts("cache-first", searchCriteria);
  }, [pageStateControls.hydrated, cacheKey]);

  const accountRepNamesByAccountCode = useMemo(() => {
    const grouped = new Map<string, Set<string>>();

    for (const rep of accountReps ?? []) {
      const employeeName = employeeLookup[rep.employeeId];
      if (!employeeName) {
        continue;
      }
      const accountCode = rep.accountCode.toUpperCase();
      if (!grouped.has(accountCode)) {
        grouped.set(accountCode, new Set());
      }
      grouped.get(accountCode)?.add(employeeName);
    }

    return Object.fromEntries(
      Array.from(grouped.entries()).map(([accountCode, names]) => [
        accountCode,
        Array.from(names).sort((left, right) => left.localeCompare(right)).join(", "),
      ]),
    ) as Record<string, string>;
  }, [accountReps, employeeLookup]);

  const filteredAccounts = useMemo(() => {
    if (!hasSearched || !accounts) {
      return [];
    }
    return sortAccounts(accounts);
  }, [accounts, hasSearched]);

  const accountGroups = useMemo(() => buildAccountGroups(filteredAccounts), [filteredAccounts]);

  const hasAccounts = Boolean(accounts && accounts.length > 0);
  const hasMatches = Boolean(hasSearched && filteredAccounts.length > 0);
  const hasFilters =
    Boolean(searchCriteria.code.trim()) ||
    Boolean(searchCriteria.name.trim()) ||
    Boolean(searchCriteria.aeName.trim()) ||
    searchCriteria.statusFilter !== DEFAULT_ACCOUNT_STATUS_FILTER;
  const hasDraftFilters = hasAccountSearchCriteria(searchDraft);
  const emptyMessage = buildEmptyMessage({
    hasSearched,
    hasFilters,
    hasAccounts,
    hasMatches,
    statusFilter: searchCriteria.statusFilter,
  });

  const searchResultText = !hasSearched
    ? "Search by one or more fields. Results load only after you click Search."
    : !accounts
      ? "Loading accounts..."
      : filteredAccounts.length === 0
        ? "No accounts matched your search."
        : `${filteredAccounts.length} account${filteredAccounts.length === 1 ? "" : "s"} shown.`;

  const pageMessages: StackMessage[] = [];
  if (refreshMessage) {
    pageMessages.push({
      id: "fundsphere-refresh",
      variant: refreshMessage.toLowerCase().includes("offline") ? "info" : "warning",
      message: refreshMessage,
    });
  }
  if (errorMessage) {
    pageMessages.push({
      id: "fundsphere-error",
      variant: "error",
      message: errorMessage,
    });
  }

  const cacheStatusText = isRefreshing
    ? "Refreshing accounts..."
    : !isOnline && cacheStatus
      ? `Offline. Showing cached accounts from ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : cacheStatus
        ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
        : "No cached accounts yet";

  function handleSearchSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const nextSearchCriteria = { ...pageState.searchDraft };
    const shouldForceRefresh =
      Boolean(accountsRef.current) && areAccountSearchCriteriaEqual(nextSearchCriteria, pageState.searchCriteria);

    setPageState((current) => ({
      ...current,
      searchCriteria: nextSearchCriteria,
      hasSearched: true,
    }));

    if (shouldForceRefresh) {
      void refreshAccounts("network-only", nextSearchCriteria);
      return;
    }

    if (!accountsRef.current) {
      void refreshAccounts("cache-first", nextSearchCriteria);
      return;
    }

    void refreshAccounts("cache-first", nextSearchCriteria);
  }

  function resetSearchCriteria() {
    setPageState((current) => ({
      ...current,
      searchDraft: { ...EMPTY_SEARCH_CRITERIA },
      searchCriteria: { ...EMPTY_SEARCH_CRITERIA },
      hasSearched: false,
    }));
  }

  function openCreateModal() {
    if (!canEditFundsphere) {
      return;
    }
    setModalMode("create");
    setModalAccount(null);
    setIsModalOpen(true);
  }

  function openEditModal(account: FundsphereAccount) {
    setModalMode("edit");
    setModalAccount(account);
    setIsModalOpen(true);
  }

  async function handleAccountSubmit(payload: {
    mode: AccountMode;
    accountCode: string | null;
    form: FundsphereAccountFormState;
  }) {
    if (!canEditFundsphere || isSaving) {
      return;
    }

    const normalizedForm = {
      ...payload.form,
      code: asString(payload.form.code).toUpperCase(),
      name: asString(payload.form.name),
      logoUrl: asString(payload.form.logoUrl),
      conseroId: asString(payload.form.conseroId),
      conseroName: asString(payload.form.conseroName),
      strataName: asString(payload.form.strataName),
      endDate: asString(payload.form.endDate),
    };

    setIsSaving(true);
    setErrorMessage(null);
    try {
      if (payload.mode === "create") {
        await createFundsphereAccount({
          requestJson,
          form: normalizedForm,
        });
      } else {
        if (!payload.accountCode) {
          throw new Error("Account code is required.");
        }
        await updateFundsphereAccount({
          requestJson,
          code: payload.accountCode,
          form: normalizedForm,
        });
      }

      const nextAccount = buildAccountPayloadFromForm(normalizedForm);
      const nextAccounts = sortAccounts([
        ...(accountsRef.current ?? []).filter((item) => item.code !== nextAccount.code),
        nextAccount,
      ]);
      accountsRef.current = nextAccounts;
      setAccounts(nextAccounts);
      syncFundsphereAccountsCache(
        cacheContext,
        nextAccounts,
        searchCriteria,
        { source: "network", fetchedAt: Date.now() },
      );
      setPageState((current) => ({
        ...current,
        hasSearched: true,
      }));
      void refreshAccounts("network-only", searchCriteria);
    } catch (error) {
      throw error instanceof Error ? error : new Error("Could not save account.");
    } finally {
      setIsSaving(false);
    }
  }

  const activeCount = accounts?.filter((account) => account.active).length ?? 0;
  const inactiveCount = (accounts?.length ?? 0) - activeCount;
  const canRefresh = Boolean(isOnline && !isLoading && !isRefreshing && !isSaving && !isModalOpen);

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          gradientVariant="workspace"
          eyebrow="FundSphere"
          title="Accounts"
          description="Manage client account records used by budgets, services, and account assignments."
          action={(
            <Button onClick={openCreateModal} disabled={!canEditFundsphere}>
              <Plus className="size-4" />
              Add Account
            </Button>
          )}
        />
      )}
      footer={accounts || cacheStatus ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            void refreshAccounts("network-only", searchCriteria);
          }}
          disabled={!canRefresh}
          refreshing={isRefreshing}
          refreshLabel="Refresh accounts"
          tooltipText={isOnline ? "Click to refresh the account list" : "Offline. Reconnect to refresh accounts."}
          containerClassName="w-full"
        />
      ) : null}
    >
      <Section className="rounded-[1.45rem] border border-blue-100/90 bg-white/95 p-5 shadow-soft">
        <SectionHeader
          title="Accounts Search"
          description="Select search criteria, then click Search to load matching accounts."
        />

        <form
          className="space-y-5 px-1.5"
          onSubmit={handleSearchSubmit}
        >
          <div className="grid grid-cols-1 gap-3.5 md:grid-cols-2 xl:grid-cols-4">
            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Code
              </span>
              <div className="relative">
                <Input
                  value={searchDraft.code}
                  onChange={(event) =>
                    setPageState((current) => ({
                      ...current,
                      searchDraft: {
                        ...current.searchDraft,
                        code: event.target.value,
                      },
                    }))
                  }
                  placeholder="Search code"
                  autoComplete="off"
                  spellCheck={false}
                  className="pl-10"
                />
                <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-400" />
              </div>
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Name
              </span>
              <Input
                value={searchDraft.name}
                onChange={(event) =>
                  setPageState((current) => ({
                    ...current,
                    searchDraft: {
                      ...current.searchDraft,
                      name: event.target.value,
                    },
                  }))
                }
                placeholder="Search name"
                autoComplete="off"
                spellCheck={false}
              />
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                AE Name
              </span>
              <Input
                value={searchDraft.aeName}
                onChange={(event) =>
                  setPageState((current) => ({
                    ...current,
                    searchDraft: {
                      ...current.searchDraft,
                      aeName: event.target.value,
                    },
                  }))
                }
                placeholder="Search AE name"
                autoComplete="off"
                spellCheck={false}
              />
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Status
              </span>
              <AppDropdown
                value={searchDraft.statusFilter}
                options={STATUS_OPTIONS}
                onValueChange={(value) =>
                  setPageState((current) => ({
                    ...current,
                    searchDraft: {
                      ...current.searchDraft,
                      statusFilter: normalizeStatusFilterValue(value),
                    },
                  }))
                }
                searchable={false}
                allowCustomValue={false}
                ariaLabel="Account status filter"
                placeholder="Active"
              />
            </label>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-blue-100/70 bg-blue-50/40 px-3 py-2">
            <p className="text-xs font-medium text-slate-500">{searchResultText}</p>
            <div className="flex flex-wrap items-center justify-end gap-2">
              {hasDraftFilters ? (
                <Button variant="outline" type="button" onClick={resetSearchCriteria} disabled={isLoading || isRefreshing}>
                  Clear
                </Button>
              ) : null}
              {hasDraftFilters ? (
                <Button type="submit" disabled={isLoading || isRefreshing}>
                  <Search className="size-4" />
                  Search
                </Button>
              ) : null}
            </div>
          </div>
        </form>
      </Section>

      <div className="relative">
        <SectionCard
          title="Accounts"
          description={
            accounts
              ? hasSearched
                ? `${filteredAccounts.length} of ${accounts.length} accounts shown.`
                : "Search results will appear after you run a search."
              : "Search results will appear after you run a search."
          }
          contentClassName="space-y-4"
        >
          {accounts && filteredAccounts.length > 0 ? (
            <div className="space-y-4">
              <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
                <span className="inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 font-medium text-emerald-700">
                  {activeCount} active
                </span>
                <span className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 font-medium text-slate-700">
                  {inactiveCount} inactive
                </span>
              </div>

              {accountGroups.map((group) => (
                <details
                  key={group.key}
                  open={accountGroupOpenState[group.key] ?? true}
                  onToggle={(event) => {
                    const nextOpen = event.currentTarget.open;
                    setAccountGroupOpenState((current) => ({ ...current, [group.key]: nextOpen }));
                  }}
                  className="group overflow-hidden rounded-2xl border border-blue-100/90 bg-slate-50/75 shadow-[0_18px_30px_-26px_rgba(37,99,235,0.5)]"
                >
                  <summary className="flex cursor-pointer list-none items-center justify-between gap-2 border-b border-blue-100/90 bg-gradient-to-r from-blue-50/85 to-indigo-50/45 px-4 py-3.5">
                    <p className="text-sm font-semibold uppercase tracking-[0.12em] text-blue-800">{group.label}</p>
                    <span className="inline-flex items-center gap-2 text-slate-500" aria-hidden="true">
                      <span className="text-xs">{group.items.length} accounts</span>
                      <ChevronDown className="size-4 transition-transform group-open:rotate-180" />
                    </span>
                  </summary>

                  <div className="p-3">
                    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                      {group.items.map((account) => (
                        <AccountCard
                          key={account.code}
                          account={account}
                          aeNames={accountRepNamesByAccountCode[account.code] ?? ""}
                          disabled={isLoading || isRefreshing || isSaving}
                          canEdit={canEditFundsphere}
                          onEdit={openEditModal}
                        />
                      ))}
                    </div>
                  </div>
                </details>
              ))}
            </div>
          ) : (
            <div className="relative overflow-hidden rounded-2xl border border-blue-100/90 bg-slate-50/75 p-8 text-center shadow-[0_18px_30px_-26px_rgba(37,99,235,0.5)]">
              <div className="mx-auto flex size-12 items-center justify-center rounded-full border border-blue-100 bg-blue-50 text-blue-700">
                <CheckCircle2 className="size-5" />
              </div>
              <div className="mt-4 space-y-1">
                <p className="text-base font-semibold text-slate-800">{emptyMessage.title}</p>
                <p className="text-sm leading-6 text-slate-500">{emptyMessage.description}</p>
              </div>
            </div>
          )}

          <SectionLoadingLayer
            active={Boolean(isRefreshing && accounts)}
            message="Refreshing accounts..."
          />
        </SectionCard>
      </div>

      <PageLoadingLayer active={Boolean(isLoading && !accounts)} message="Loading accounts..." />

      <AccountModal
        open={isModalOpen}
        mode={modalMode}
        account={modalAccount}
        canEdit={canEditFundsphere}
        onOpenChange={setIsModalOpen}
        onSubmit={handleAccountSubmit}
      />
    </AppPageLayout>
  );
}

export default FundsphereAccountsPage;
