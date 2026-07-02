import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import {
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
import { ImageUploadField } from "@shared/components/form/ImageUploadField";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { ModalCacheFooter, ModalCloseButton, ModalFooter, ModalHeaderRow, ModalShell } from "@shared/components";
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
import { SearchEmptyStatePanel } from "@shared/components/status/SearchEmptyStatePanel";
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
  loadFundsphereAccount,
  loadFundsphereAccounts,
  normalizeFundsphereAccountForm,
  uploadFundsphereAccountLogo,
  updateFundsphereAccount,
  type FundsphereAccount,
  type FundsphereAccountFormState,
  type FundsphereRequestJson,
} from "@fundsphere/lib/accountsApi";
import {
  buildFundsphereAccountsCacheKey,
  readFundsphereAccountDetailCacheSnapshot,
  readFundsphereAccountsCacheSnapshot,
  syncFundsphereAccountDetailCache,
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
  requestJson: FundsphereRequestJson;
  onUploadLogo: (file: File, account: { code: string; name: string }) => Promise<string>;
  onSubmit: (payload: {
    mode: AccountMode;
    accountCode: string | null;
    form: FundsphereAccountFormState;
  }) => Promise<void>;
  cacheContext: FundsphereAccountsCacheContext;
  isOnline: boolean;
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
  statusFilter: DEFAULT_ACCOUNT_STATUS_FILTER,
};
const EMPTY_PAGE_STATE: PersistedAccountsPageState = {
  searchDraft: { ...DEFAULT_SEARCH_CRITERIA },
  searchCriteria: { ...DEFAULT_SEARCH_CRITERIA },
  hasSearched: false,
};
const STATUS_OPTIONS: AppDropdownOption[] = [
  { value: "active", label: "Active" },
  { value: "inactive", label: "Inactive" },
];
const ACCOUNT_CARD_ACTIVATION_SUPPRESSION_MS = 250;

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
  return DEFAULT_ACCOUNT_STATUS_FILTER;
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
      description: "Use the search form above to find accounts.",
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

function ClearableInput({
  id,
  value,
  onChange,
}: {
  id: string;
  value: string;
  onChange: (nextValue: string) => void;
}) {
  const hasValue = value.trim().length > 0;

  return (
    <div className="group relative">
      <Input
        id={id}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        autoComplete="off"
        spellCheck={false}
        className="pr-9"
      />
      <button
        type="button"
        aria-label="Clear input"
        onClick={() => onChange("")}
        className="absolute right-2 top-1/2 -translate-y-1/2 rounded p-1 text-slate-400 opacity-0 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:opacity-100 focus-visible:outline-none group-hover:opacity-100 group-focus-within:opacity-100"
        style={{ visibility: hasValue ? "visible" : "hidden" }}
        tabIndex={hasValue ? 0 : -1}
      >
        <X className="size-3.5" />
      </button>
    </div>
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
} {
  const code = asString(form.code).toUpperCase();
  const name = asString(form.name);
  const logoUrl = asString(form.logoUrl);
  const conseroId = asString(form.conseroId);
  const conseroName = asString(form.conseroName);
  const strataName = asString(form.strataName);

  return {
    code: mode === "create" && !code ? "Code is required." : code.length > 10 ? "Code must be 10 characters or fewer." : null,
    name: !name ? "Name is required." : name.length > 255 ? "Name must be 255 characters or fewer." : null,
    logoUrl: logoUrl.length > 2048 ? "Logo URL must be 2048 characters or fewer." : null,
    conseroId: conseroId.length > 10 ? "Consero ID must be 10 characters or fewer." : null,
    conseroName: conseroName.length > 255 ? "Consero name must be 255 characters or fewer." : null,
    strataName: strataName.length > 255 ? "Strata name must be 255 characters or fewer." : null,
  };
}

function createEmptyAccountFieldErrors(): ReturnType<typeof validateAccountForm> {
  return {
    code: null,
    name: null,
    logoUrl: null,
    conseroId: null,
    conseroName: null,
    strataName: null,
  };
}

function validateAccountField(
  field: keyof ReturnType<typeof validateAccountForm>,
  form: FundsphereAccountFormState,
  mode: AccountMode,
): string | null {
  return validateAccountForm(form, mode)[field];
}

function AccountModal({
  open,
  mode,
  account,
  canEdit,
  onOpenChange,
  requestJson,
  onUploadLogo,
  onSubmit,
  cacheContext,
  isOnline,
}: AccountModalProps) {
  const [form, setForm] = useState<FundsphereAccountFormState>(() => toAccountForm(account));
  const [baseline, setBaseline] = useState<FundsphereAccountFormState>(() => toAccountForm(account));
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isLogoUploading, setIsLogoUploading] = useState(false);
  const [detailCacheStatus, setDetailCacheStatus] = useState<CacheStatus | null>(null);
  const [isDetailRefreshing, setIsDetailRefreshing] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [logoUploadError, setLogoUploadError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const [fieldErrors, setFieldErrors] = useState(() => createEmptyAccountFieldErrors());
  const [logoPreviewError, setLogoPreviewError] = useState(false);
  const [logoPreviewOpen, setLogoPreviewOpen] = useState(false);
  const [logoDraftFile, setLogoDraftFile] = useState<File | null>(null);
  const [logoDraftObjectUrl, setLogoDraftObjectUrl] = useState<string | null>(null);
  const detailRequestTokenRef = useRef(0);

  const currentFormErrors = useMemo(() => validateAccountForm(form, mode), [form, mode]);
  const formIsValid = useMemo(() => Object.values(currentFormErrors).every((item) => item === null), [currentFormErrors]);
  const hasUnsavedChanges = useMemo(() => JSON.stringify(form) !== JSON.stringify(baseline) || Boolean(logoDraftFile), [baseline, form, logoDraftFile]);
  const canSubmit = canEdit && hasUnsavedChanges && formIsValid && !isSubmitting && !isLogoUploading;
  const showPrimaryAction = canEdit && hasUnsavedChanges && formIsValid && !isSubmitting && !isLogoUploading;
  const primaryActionLabel = mode === "create" ? "Create Account" : "Save Changes";
  const logoPreviewSrc = logoDraftObjectUrl || asString(form.logoUrl);
  const hasLogoPreview = Boolean(logoPreviewSrc) && !logoPreviewError;
  const logoUploadItem = useMemo(() => {
    const previewLabel = "Logo preview";
    const placeholderText = (form.name || form.code || "AC").slice(0, 2).toUpperCase();
    return {
      key: logoDraftObjectUrl ? `draft:${logoDraftObjectUrl}` : `logo:${form.code || "new"}`,
      label: previewLabel,
      meta: logoDraftFile
        ? "Previewing selected logo image."
        : form.logoUrl
          ? "Preview from the current logo."
          : "Upload a logo image to preview it.",
      previewSrc: logoPreviewSrc || null,
      placeholder: (
        <span className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
          {placeholderText}
        </span>
      ),
      previewAriaLabel: "Preview logo",
      removeAriaLabel: logoDraftFile ? "Revert logo" : "Remove logo",
    };
  }, [form.code, form.logoUrl, form.name, logoDraftFile, logoDraftObjectUrl, logoPreviewSrc]);

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

  function markFieldBlurred(
    field: keyof ReturnType<typeof validateAccountForm>,
    nextForm: FundsphereAccountFormState = form,
  ) {
    setFieldErrors((current) => ({
      ...current,
      [field]: validateAccountField(field, nextForm, mode),
    }));
  }

  useEffect(() => {
    return () => {
      if (logoDraftObjectUrl) {
        URL.revokeObjectURL(logoDraftObjectUrl);
      }
    };
  }, [logoDraftObjectUrl]);

  useEffect(() => {
    setLogoPreviewError(false);
    if (!logoPreviewSrc) {
      setLogoPreviewOpen(false);
    }
  }, [logoPreviewSrc]);

  useEffect(() => {
    if (!open) {
      setForm(toAccountForm(null));
      setBaseline(toAccountForm(null));
      setIsSubmitting(false);
      setIsLogoUploading(false);
      setDetailCacheStatus(null);
      setIsDetailRefreshing(false);
      setSubmitError(null);
      setLogoUploadError(null);
      setIsDiscardDialogOpen(false);
      setFieldErrors(createEmptyAccountFieldErrors());
      setLogoPreviewError(false);
      setLogoPreviewOpen(false);
      clearLogoDraftAttachment();
      return;
    }

    const nextForm = toAccountForm(account);
    setForm(nextForm);
    setBaseline(nextForm);
    setDetailCacheStatus(null);
    setIsDetailRefreshing(false);
    setSubmitError(null);
    setIsSubmitting(false);
    setIsLogoUploading(false);
    setLogoUploadError(null);
    setIsDiscardDialogOpen(false);
    setFieldErrors(createEmptyAccountFieldErrors());
    setLogoPreviewError(false);
    setLogoPreviewOpen(false);
    clearLogoDraftAttachment();
  }, [account, open]);

  async function refreshAccountDetail(policy: CachePolicy): Promise<void> {
    const accountCode = asString(account?.code).toUpperCase();
    if (mode !== "edit" || !accountCode) {
      setDetailCacheStatus(null);
      setIsDetailRefreshing(false);
      return;
    }

    const requestToken = ++detailRequestTokenRef.current;
    const snapshot = readFundsphereAccountDetailCacheSnapshot(cacheContext, accountCode);
    const cachedAccount = snapshot?.data ? toUiAccount(snapshot.data) : null;
    const shouldFetch = shouldFetchNetwork(policy, snapshot);

    if (cachedAccount) {
      const cachedForm = toAccountForm(cachedAccount);
      setForm(cachedForm);
      setBaseline(cachedForm);
      setDetailCacheStatus({
        source: "cache",
        fetchedAt: snapshot?.fetchedAt ?? Date.now(),
      });
    }

    if (!shouldFetch) {
      if (requestToken === detailRequestTokenRef.current) {
        setIsDetailRefreshing(false);
      }
      return;
    }

    if (!isOnline) {
      if (requestToken === detailRequestTokenRef.current) {
        setIsDetailRefreshing(false);
      }
      return;
    }

    setIsDetailRefreshing(true);
    try {
      const nextAccount = toUiAccount(await loadFundsphereAccount({
        requestJson,
        code: accountCode,
      }));
      if (requestToken !== detailRequestTokenRef.current) {
        return;
      }
      const nextForm = toAccountForm(nextAccount);
      setForm(nextForm);
      setBaseline(nextForm);
      const fetchedAt = Date.now();
      setDetailCacheStatus({
        source: "network",
        fetchedAt,
      });
      syncFundsphereAccountDetailCache(cacheContext, nextAccount, { source: "network", fetchedAt });
    } finally {
      if (requestToken === detailRequestTokenRef.current) {
        setIsDetailRefreshing(false);
      }
    }
  }

  useEffect(() => {
    if (!open || mode !== "edit" || !account?.code) {
      return;
    }
    void refreshAccountDetail("cache-first");
  }, [account?.code, mode, open]);

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

  function clearLogoDraftAttachment() {
    if (logoDraftObjectUrl) {
      URL.revokeObjectURL(logoDraftObjectUrl);
    }
    setLogoDraftFile(null);
    setLogoDraftObjectUrl(null);
  }

  function restoreOriginalLogoAttachment() {
    clearLogoDraftAttachment();
    setForm((current) => ({
      ...current,
      logoUrl: baseline.logoUrl,
    }));
    setLogoPreviewError(false);
    setLogoUploadError(null);
  }

  async function handleLogoFilesSelected(files: File[]) {
    if (!files.length || isSubmitting || isLogoUploading || !canEdit) {
      return;
    }
    const file = files[0];
    const normalizedMimeType = asString(file.type).toLowerCase();
    const normalizedFileName = asString(file.name).toLowerCase();
    const validByMimeType = normalizedMimeType.startsWith("image/");
    const validByExtension = (
      normalizedFileName.endsWith(".png")
      || normalizedFileName.endsWith(".jpg")
      || normalizedFileName.endsWith(".jpeg")
      || normalizedFileName.endsWith(".webp")
    );
    if (!validByMimeType && !validByExtension) {
      setLogoUploadError("Only PNG, JPG, JPEG, and WEBP files are allowed.");
      return;
    }

    setLogoUploadError(null);
    setLogoPreviewError(false);
    clearLogoDraftAttachment();
    const nextObjectUrl = URL.createObjectURL(file);
    setLogoDraftFile(file);
    setLogoDraftObjectUrl(nextObjectUrl);
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
    clearLogoDraftAttachment();
    setForm(baseline);
    setSubmitError(null);
    setLogoUploadError(null);
    setFieldErrors(createEmptyAccountFieldErrors());
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
      setFieldErrors(currentFormErrors);
      return;
    }

    setIsSubmitting(true);
    setSubmitError(null);
    try {
      let nextForm = {
        ...form,
        code: asString(form.code).toUpperCase(),
        name: asString(form.name),
        logoUrl: asString(form.logoUrl),
        conseroId: asString(form.conseroId),
        conseroName: asString(form.conseroName),
        strataName: asString(form.strataName),
      };

      if (logoDraftFile) {
        setIsLogoUploading(true);
        try {
          const logoUrl = await onUploadLogo(logoDraftFile, {
            code: asString(form.code).toUpperCase(),
            name: asString(form.name),
          });
          nextForm = {
            ...nextForm,
            logoUrl,
          };
          setForm(nextForm);
          clearLogoDraftAttachment();
        } catch (error) {
          setLogoUploadError(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not upload logo.");
          return;
        } finally {
          setIsLogoUploading(false);
        }
      }

      await onSubmit({
        mode,
        accountCode: account?.code ?? null,
        form: nextForm,
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
          className="max-h-[92vh] max-w-[620px] overflow-hidden rounded-xl bg-white p-6"
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

            <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1 space-y-4">
              <Section className="space-y-3">
                <SectionHeader
                  title="Add Account"
                  description="Core account information, logo, and status."
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
                      onBlur={(event) => {
                        codeField.onBlur(event);
                        markFieldBlurred("code", {
                          ...form,
                          code: event.target.value.trim().toUpperCase(),
                        });
                      }}
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
                {fieldErrors.code ? <p className="text-sm text-rose-600">{fieldErrors.code}</p> : null}

                <FormRow
                  label={(
                    <>
                      Name<span className="ml-1 text-rose-600">*</span>
                    </>
                  )}
                >
                  <Input
                    {...nameField}
                    onBlur={(event) => {
                      nameField.onBlur(event);
                      markFieldBlurred("name");
                    }}
                    disabled={isSubmitting || !canEdit}
                    maxLength={255}
                    autoComplete="off"
                  />
                </FormRow>
                {fieldErrors.name ? <p className="text-sm text-rose-600">{fieldErrors.name}</p> : null}

                <FormRow label="Logo" alignStart>
                  <ImageUploadField
                    buttonLabel="Select image or paste screenshot"
                    helperText="PNG, JPG, JPEG, or WEBP. Up to 10 MB."
                    items={[logoUploadItem]}
                    disabled={isSubmitting || !canEdit}
                    isBusy={isLogoUploading}
                    onFilesSelected={(files) => {
                      void handleLogoFilesSelected(files);
                    }}
                    onPreviewItem={() => {
                      if (logoPreviewSrc) {
                        setLogoPreviewOpen(true);
                      }
                    }}
                    onRemoveItem={
                      logoDraftFile || form.logoUrl
                        ? () => {
                            if (logoDraftFile) {
                              restoreOriginalLogoAttachment();
                              return;
                            }
                            updateForm("logoUrl", "");
                            setLogoPreviewError(false);
                            setLogoUploadError(null);
                            if (submitError) {
                              setSubmitError(null);
                            }
                          }
                        : undefined
                    }
                    errorText={logoUploadError || fieldErrors.logoUrl || undefined}
                    statusText={isLogoUploading ? "Uploading logo..." : null}
                  />
                </FormRow>

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
              </Section>

              <Section className="space-y-3">
                <SectionHeader
                  title="Reference Data"
                  description="Optional fields used by other FundSphere workflows."
                />

                <FormRow label="Consero ID">
                  <Input
                    {...conseroIdField}
                    onBlur={(event) => {
                      conseroIdField.onBlur(event);
                      markFieldBlurred("conseroId");
                    }}
                    disabled={isSubmitting || !canEdit}
                    maxLength={10}
                    autoComplete="off"
                    spellCheck={false}
                  />
                </FormRow>
                {fieldErrors.conseroId ? <p className="text-sm text-rose-600">{fieldErrors.conseroId}</p> : null}

                <FormRow label="Consero Name">
                  <Input
                    {...conseroNameField}
                    onBlur={(event) => {
                      conseroNameField.onBlur(event);
                      markFieldBlurred("conseroName");
                    }}
                    disabled={isSubmitting || !canEdit}
                    maxLength={255}
                    autoComplete="off"
                    spellCheck={false}
                  />
                </FormRow>
                {fieldErrors.conseroName ? <p className="text-sm text-rose-600">{fieldErrors.conseroName}</p> : null}

                <FormRow label="Strata Name">
                  <Input
                    {...strataNameField}
                    onBlur={(event) => {
                      strataNameField.onBlur(event);
                      markFieldBlurred("strataName");
                    }}
                    disabled={isSubmitting || !canEdit}
                    maxLength={255}
                    autoComplete="off"
                    spellCheck={false}
                  />
                </FormRow>
                {fieldErrors.strataName ? <p className="text-sm text-rose-600">{fieldErrors.strataName}</p> : null}
              </Section>

              {submitError ? <p className="mt-4 text-sm text-rose-600">{submitError}</p> : null}
            </div>

            {mode === "edit" ? (
              <ModalCacheFooter
                text={
                  isDetailRefreshing
                    ? "Refreshing account data..."
                    : detailCacheStatus
                      ? `Data source: ${detailCacheStatus.source}. Last updated ${formatRelativeTime(detailCacheStatus.fetchedAt)}.`
                      : "No cached account data yet"
                }
                onRefresh={() => {
                  if (!isDetailRefreshing && !hasUnsavedChanges && !isSubmitting) {
                    void refreshAccountDetail("network-only");
                  }
                }}
                disabled={isDetailRefreshing || hasUnsavedChanges || isSubmitting}
                refreshing={isDetailRefreshing}
                refreshLabel="Refresh account data"
                tooltipText={
                  hasUnsavedChanges
                    ? "Save or discard your edits before refreshing account data."
                    : "Click to refresh this account data"
                }
                actions={(
                  <>
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
                    {showPrimaryAction ? (
                      <Button type="button" onClick={handleSubmit} disabled={!canSubmit}>
                        {isSubmitting ? (
                          <>
                            <RefreshCw className="size-4 animate-spin" />
                            Saving...
                          </>
                        ) : (
                          primaryActionLabel
                        )}
                      </Button>
                    ) : null}
                  </>
                )}
              />
            ) : (
              <ModalFooter className="mt-4 flex-col items-end gap-2 border-t border-slate-100 pt-3 sm:flex-row sm:items-center sm:justify-end">
                <div className="flex items-center justify-end gap-2">
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
                  {showPrimaryAction ? (
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
            )}
          </ModalShell>
        </DialogContent>
      </Dialog>

      {hasLogoPreview ? (
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
                src={logoPreviewSrc}
                alt={`${form.name || "Account"} logo enlarged`}
                className="mx-auto block h-auto max-h-[70vh] w-auto max-w-full object-contain"
              />
            </div>
          </DialogContent>
        </Dialog>
      ) : null}

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
  const suppressNextCardActivationRef = useRef(false);
  useEffect(() => {
    setLogoError(false);
    setLogoPreviewOpen(false);
    suppressNextCardActivationRef.current = false;
  }, [account.logoUrl]);

  function handleLogoPreviewOpenChange(nextOpen: boolean): void {
    if (!nextOpen) {
      suppressNextCardActivationRef.current = true;
      window.setTimeout(() => {
        suppressNextCardActivationRef.current = false;
      }, ACCOUNT_CARD_ACTIVATION_SUPPRESSION_MS);
    }
    setLogoPreviewOpen(nextOpen);
  }

  const showLogo = Boolean(account.logoUrl) && !logoError;
  const logoFallback = (account.name || account.code || "AC").slice(0, 2).toUpperCase();
  const displayAeNames = aeNames ? stripRolePrefix(aeNames) : "";

  return (
    <article
      role="button"
      tabIndex={disabled || !canEdit || logoPreviewOpen ? -1 : 0}
      onClick={() => {
        if (!disabled && canEdit && !logoPreviewOpen && !suppressNextCardActivationRef.current) {
          onEdit(account);
        }
      }}
      onKeyDown={(event) => {
        if (disabled || !canEdit || logoPreviewOpen || suppressNextCardActivationRef.current) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onEdit(account);
        }
      }}
      className={cn(
        "rounded-[1.35rem] border p-4 shadow-[0_18px_30px_-24px_rgba(37,99,235,0.42)] transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
        disabled || logoPreviewOpen ? "pointer-events-none cursor-default" : "cursor-pointer",
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
        <Dialog open={logoPreviewOpen} onOpenChange={handleLogoPreviewOpenChange}>
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
  const didRestoreSearchStateRef = useRef<string | null>(null);
  const searchDraft = pageState.searchDraft;
  const searchCriteria = pageState.searchCriteria;
  const hasSearched = pageState.hasSearched;

  useEffect(() => {
    accountsRef.current = accounts;
  }, [accounts]);

  useEffect(() => {
    if (!pageStateControls.hydrated) {
      return;
    }
    setPageState((current) => {
      const nextSearchDraftStatus = normalizeStatusFilterValue(current.searchDraft.statusFilter);
      const nextSearchCriteriaStatus = normalizeStatusFilterValue(current.searchCriteria.statusFilter);
      if (
        nextSearchDraftStatus === current.searchDraft.statusFilter &&
        nextSearchCriteriaStatus === current.searchCriteria.statusFilter
      ) {
        return current;
      }
      return {
        ...current,
        searchDraft: {
          ...current.searchDraft,
          statusFilter: nextSearchDraftStatus,
        },
        searchCriteria: {
          ...current.searchCriteria,
          statusFilter: nextSearchCriteriaStatus,
        },
      };
    });
  }, [pageStateControls.hydrated]);

  useEffect(() => {
    if (!pageStateControls.hydrated || !hasSearched) {
      return;
    }
    if (didRestoreSearchStateRef.current === cacheKey) {
      return;
    }
    didRestoreSearchStateRef.current = cacheKey;
    void refreshAccounts("cache-first", searchCriteria);
  }, [cacheKey, hasSearched, pageStateControls.hydrated, searchCriteria]);

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
      syncFundsphereAccountDetailCache(cacheContext, nextAccount, { source: "network", fetchedAt: Date.now() });
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

  async function handleAccountLogoUpload(file: File, account: { code: string; name: string }): Promise<string> {
    return uploadFundsphereAccountLogo({
      requestJson,
      accountCode: account.code,
      file,
    });
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
          gradientVariant="fundsphere"
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
              <ClearableInput
                id="fundsphere-accounts-search-code"
                value={searchDraft.code}
                onChange={(nextValue) =>
                  setPageState((current) => ({
                    ...current,
                    searchDraft: {
                      ...current.searchDraft,
                      code: nextValue.toUpperCase(),
                    },
                  }))
                }
              />
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Name
              </span>
              <ClearableInput
                id="fundsphere-accounts-search-name"
                value={searchDraft.name}
                onChange={(nextValue) =>
                  setPageState((current) => ({
                    ...current,
                    searchDraft: {
                      ...current.searchDraft,
                      name: nextValue,
                    },
                  }))
                }
              />
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                AE Name
              </span>
              <ClearableInput
                id="fundsphere-accounts-search-ae-name"
                value={searchDraft.aeName}
                onChange={(nextValue) =>
                  setPageState((current) => ({
                    ...current,
                    searchDraft: {
                      ...current.searchDraft,
                      aeName: nextValue,
                    },
                  }))
                }
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
        {accounts && filteredAccounts.length > 0 ? (
          <SectionCard
            title="Accounts"
            description={`${filteredAccounts.length} of ${accounts.length} accounts shown.`}
            contentClassName="space-y-4"
          >
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
          </SectionCard>
        ) : (
          <SearchEmptyStatePanel
            icon={<Search className="size-7" />}
            message={emptyMessage.title}
            description={emptyMessage.description}
          />
        )}

        <SectionLoadingLayer
          active={Boolean(isRefreshing && accounts)}
          message="Refreshing accounts..."
        />
      </div>

      <PageLoadingLayer active={Boolean(isLoading && !accounts)} message="Loading accounts..." />

      <AccountModal
        open={isModalOpen}
        mode={modalMode}
        account={modalAccount}
        canEdit={canEditFundsphere}
        onOpenChange={setIsModalOpen}
        requestJson={requestJson}
        onUploadLogo={handleAccountLogoUpload}
        onSubmit={handleAccountSubmit}
        cacheContext={cacheContext}
        isOnline={isOnline}
      />
    </AppPageLayout>
  );
}

export default FundsphereAccountsPage;
