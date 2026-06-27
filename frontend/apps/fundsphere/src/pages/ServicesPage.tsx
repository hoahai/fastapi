import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { ChevronDown, Plus, RefreshCw, Search, X } from "lucide-react";

import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { Section, SectionHeader } from "@shared/components";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { FormRow, ModalCacheFooter, ModalCloseButton, ModalFooter, ModalHeaderRow, ModalShell, NumberUnitField } from "@shared/components";
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
import { Textarea } from "@tradsphere/components/ui/textarea";
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";

import {
  createFundsphereService,
  loadFundsphereDepartments,
  loadFundsphereService,
  loadFundsphereServices,
  normalizeFundsphereServiceForm,
  updateFundsphereService,
  type FundsphereDepartment,
  type FundsphereService,
  type FundsphereServiceFormState,
  type FundsphereServiceSearchCriteria,
  type FundsphereRequestJson,
} from "@fundsphere/lib/servicesApi";
import {
  buildFundsphereServicesCacheKey,
  readFundsphereServicesDepartmentsCacheSnapshot,
  readFundsphereServiceDetailCacheSnapshot,
  FUNDSPHERE_SERVICES_PAGE_CODE,
  readFundsphereServicesCacheSnapshot,
  syncFundsphereServiceDetailCache,
  syncFundsphereServicesDepartmentsCache,
  syncFundsphereServicesCache,
  type FundsphereServicesCacheContext,
  type FundsphereServicesDepartmentsCacheContext,
} from "@fundsphere/lib/servicesCache";
import { buildFundsphereDepartmentColorStyles } from "@fundsphere/lib/departmentColor";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type ServiceMode = "create" | "edit";
type ServiceStatusFilter = "" | "active" | "inactive";
type PersistedServicesPageState = {
  searchDraft: FundsphereServiceSearchCriteria;
  searchCriteria: FundsphereServiceSearchCriteria;
  hasSearched: boolean;
};

type ServiceModalProps = {
  open: boolean;
  mode: ServiceMode;
  service: FundsphereService | null;
  departmentOptions: AppDropdownOption[];
  departmentLoading: boolean;
  canEdit: boolean;
  onOpenChange: (open: boolean) => void;
  requestJson: FundsphereRequestJson;
  onSubmit: (payload: {
    mode: ServiceMode;
    serviceId: string | null;
    form: FundsphereServiceFormState;
  }) => Promise<void>;
  cacheContext: FundsphereServicesCacheContext;
  isOnline: boolean;
};

type ServiceGroup = {
  key: string;
  label: string;
  departmentColor: string | null;
  items: FundsphereService[];
};

const FUNDSPHERE_APP_CODE = "fundsphere";
const DEFAULT_SERVICE_STATUS_FILTER: ServiceStatusFilter = "active";
const DEFAULT_SEARCH_CRITERIA: FundsphereServiceSearchCriteria = {
  name: "",
  departmentCode: "",
  statusFilter: DEFAULT_SERVICE_STATUS_FILTER,
};
const EMPTY_SEARCH_CRITERIA: FundsphereServiceSearchCriteria = {
  name: "",
  departmentCode: "",
  statusFilter: DEFAULT_SERVICE_STATUS_FILTER,
};
const EMPTY_PAGE_STATE: PersistedServicesPageState = {
  searchDraft: { ...DEFAULT_SEARCH_CRITERIA },
  searchCriteria: { ...DEFAULT_SEARCH_CRITERIA },
  hasSearched: false,
};
const STATUS_OPTIONS: AppDropdownOption[] = [
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

function normalizeStatusFilterValue(value: string): ServiceStatusFilter {
  if (value === "active" || value === "inactive") {
    return value;
  }
  return DEFAULT_SERVICE_STATUS_FILTER;
}

function hasServiceSearchCriteria(criteria: FundsphereServiceSearchCriteria): boolean {
  return Boolean(criteria.name.trim()) || Boolean(criteria.departmentCode.trim()) || Boolean(criteria.statusFilter);
}

function areServiceSearchCriteriaEqual(
  left: FundsphereServiceSearchCriteria,
  right: FundsphereServiceSearchCriteria,
): boolean {
  return (
    left.name.trim() === right.name.trim() &&
    left.departmentCode.trim() === right.departmentCode.trim() &&
    left.statusFilter === right.statusFilter
  );
}

function sortServices(services: FundsphereService[]): FundsphereService[] {
  return [...services].sort((left, right) => {
    const leftOrder = left.departmentListingOrder ?? Number.MAX_SAFE_INTEGER;
    const rightOrder = right.departmentListingOrder ?? Number.MAX_SAFE_INTEGER;
    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder;
    }
    const departmentComparison = left.departmentName.localeCompare(right.departmentName);
    if (departmentComparison !== 0) {
      return departmentComparison;
    }
    const serviceComparison = left.name.localeCompare(right.name);
    if (serviceComparison !== 0) {
      return serviceComparison;
    }
    return left.id.localeCompare(right.id);
  });
}

function sortDepartments<T extends { code: string; name: string; listingOrder: number | null }>(departments: T[]): T[] {
  return [...departments].sort((left, right) => {
    const leftOrder = left.listingOrder ?? Number.MAX_SAFE_INTEGER;
    const rightOrder = right.listingOrder ?? Number.MAX_SAFE_INTEGER;
    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder;
    }
    const nameComparison = left.name.localeCompare(right.name);
    if (nameComparison !== 0) {
      return nameComparison;
    }
    return left.code.localeCompare(right.code);
  });
}

function buildDepartmentOptions(
  departments: FundsphereDepartment[],
  services: FundsphereService[] | null,
): AppDropdownOption[] {
  const source: Array<{ code: string; name: string; listingOrder: number | null }> = departments.length
    ? departments
    : Array.from(
        new Map(
          (services ?? []).map((service) => [
            service.departmentCode,
            {
              code: service.departmentCode,
              name: service.departmentName,
              listingOrder: service.departmentListingOrder,
            },
          ]),
        ).values(),
      );

  return sortDepartments(
    source.map((department) => ({
      code: department.code,
      name: department.name,
      listingOrder: department.listingOrder ?? null,
    })),
  ).map((department) => ({
    value: department.code.toUpperCase(),
    label: department.name,
  }));
}

function buildServiceGroups(
  services: FundsphereService[],
  departmentColors: Map<string, string | null>,
): ServiceGroup[] {
  const grouped = new Map<
    string,
    { label: string; listingOrder: number; departmentColor: string | null; items: FundsphereService[] }
  >();

  for (const service of sortServices(services)) {
    const groupKey = service.departmentCode;
    const group = grouped.get(groupKey);
    if (group) {
      group.items.push(service);
      continue;
    }

    grouped.set(groupKey, {
      label: service.departmentName,
      listingOrder: service.departmentListingOrder ?? Number.MAX_SAFE_INTEGER,
      departmentColor: departmentColors.get(groupKey) ?? null,
      items: [service],
    });
  }

  return Array.from(grouped.entries())
    .sort((left, right) => {
      const leftGroup = left[1];
      const rightGroup = right[1];
      if (leftGroup.listingOrder !== rightGroup.listingOrder) {
        return leftGroup.listingOrder - rightGroup.listingOrder;
      }
      const labelComparison = leftGroup.label.localeCompare(rightGroup.label);
      if (labelComparison !== 0) {
        return labelComparison;
      }
      return left[0].localeCompare(right[0]);
    })
    .map(([key, group]) => ({
      key,
      label: group.label,
      departmentColor: group.departmentColor,
      items: group.items,
    }));
}

function buildEmptyMessage(params: {
  hasSearched: boolean;
  hasFilters: boolean;
  hasServices: boolean;
  hasMatches: boolean;
  statusFilter: ServiceStatusFilter;
}): { title: string; description: string } {
  if (!params.hasSearched) {
    return {
      title: "Search services",
      description: "Use the search form above to find services.",
    };
  }
  if (!params.hasServices) {
    return {
      title: "No services yet",
      description: "Create the first FundSphere service to start linking budgets and departments.",
    };
  }
  if (!params.hasMatches) {
    if (params.statusFilter === "active") {
      return {
        title: "No active services",
        description: "No active services matched the current filters. Switch Status to Inactive or clear the filters to see inactive services.",
      };
    }
    if (params.hasFilters) {
      return {
        title: "No matches",
        description: "No services matched the current filters. Adjust the filters and search again.",
      };
    }
  }
  if (params.hasFilters) {
    return {
      title: "No matches",
      description: "No services match the current filters. Clear the filters to see the full list.",
    };
  }
  return {
    title: "No services found",
    description: "The workspace did not return any service rows.",
  };
}

function isPersistedServicesPageState(value: unknown): value is PersistedServicesPageState {
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.hasSearched !== "boolean" || !isRecord(value.searchDraft) || !isRecord(value.searchCriteria)) {
    return false;
  }
  return (
    typeof value.searchDraft.name === "string" &&
    typeof value.searchDraft.departmentCode === "string" &&
    typeof value.searchDraft.statusFilter === "string" &&
    typeof value.searchCriteria.name === "string" &&
    typeof value.searchCriteria.departmentCode === "string" &&
    typeof value.searchCriteria.statusFilter === "string"
  );
}

function toServiceForm(service: FundsphereService | null): FundsphereServiceFormState {
  return normalizeFundsphereServiceForm(service);
}

function toUiService(service: FundsphereService): FundsphereService {
  return {
    ...service,
    id: asString(service.id),
    name: asString(service.name),
    conseroId: service.conseroId ? asString(service.conseroId) : null,
    departmentCode: asString(service.departmentCode).toUpperCase(),
    departmentName: asString(service.departmentName),
    departmentListingOrder: service.departmentListingOrder ?? null,
    description: service.description ? asString(service.description) : null,
    commission: asString(service.commission) || "0.00",
    netAdjustment: asString(service.netAdjustment) || "0.00",
    active: Boolean(service.active),
    dateCreated: service.dateCreated ? asString(service.dateCreated) : null,
    dateUpdated: service.dateUpdated ? asString(service.dateUpdated) : null,
  };
}

function buildServicePayloadFromForm(form: FundsphereServiceFormState): FundsphereServiceFormState {
  return {
    name: asString(form.name),
    departmentCode: asString(form.departmentCode).toUpperCase(),
    description: asString(form.description),
    commission: asString(form.commission),
    netAdjustment: asString(form.netAdjustment),
    active: Boolean(form.active),
  };
}

function validateDecimalValue(value: string, label: string): string | null {
  const trimmed = asString(value);
  if (!trimmed) {
    return null;
  }
  return /^[-+]?\d+(?:\.\d+)?$/.test(trimmed) ? null : `${label} must be a valid number.`;
}

function validateServiceForm(
  form: FundsphereServiceFormState,
): {
  name: string | null;
  departmentCode: string | null;
  description: string | null;
  commission: string | null;
  netAdjustment: string | null;
} {
  const name = asString(form.name);
  const departmentCode = asString(form.departmentCode).toUpperCase();
  const description = asString(form.description);
  const commission = asString(form.commission);
  const netAdjustment = asString(form.netAdjustment);

  return {
    name: !name ? "Service name is required." : name.length > 255 ? "Service name must be 255 characters or fewer." : null,
    departmentCode: !departmentCode ? "Department is required." : null,
    description: description.length > 2048 ? "Description must be 2048 characters or fewer." : null,
    commission: validateDecimalValue(commission, "Commission"),
    netAdjustment: validateDecimalValue(netAdjustment, "Net adjustment"),
  };
}

function createEmptyServiceFieldErrors(): ReturnType<typeof validateServiceForm> {
  return {
    name: null,
    departmentCode: null,
    description: null,
    commission: null,
    netAdjustment: null,
  };
}

function validateServiceField(
  field: keyof ReturnType<typeof validateServiceForm>,
  form: FundsphereServiceFormState,
): string | null {
  return validateServiceForm(form)[field];
}

function buildServiceStatusLabel(active: boolean): string {
  return active ? "Active" : "Inactive";
}

function buildServiceStatusClass(active: boolean): string {
  return active
    ? "border-emerald-200 bg-emerald-50/90 text-emerald-800"
    : "border-slate-300 bg-slate-100/90 text-slate-700";
}

function ServiceModal({
  open,
  mode,
  service,
  departmentOptions,
  departmentLoading,
  canEdit,
  onOpenChange,
  requestJson,
  onSubmit,
  cacheContext,
  isOnline,
}: ServiceModalProps) {
  const [form, setForm] = useState<FundsphereServiceFormState>(() => toServiceForm(service));
  const [baseline, setBaseline] = useState<FundsphereServiceFormState>(() => toServiceForm(service));
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [detailCacheStatus, setDetailCacheStatus] = useState<CacheStatus | null>(null);
  const [isDetailRefreshing, setIsDetailRefreshing] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const [fieldErrors, setFieldErrors] = useState(() => createEmptyServiceFieldErrors());
  const detailRequestTokenRef = useRef(0);

  const currentFormErrors = useMemo(() => validateServiceForm(form), [form]);
  const formIsValid = useMemo(() => Object.values(currentFormErrors).every((item) => item === null), [currentFormErrors]);
  const hasUnsavedChanges = useMemo(
    () => JSON.stringify(form) !== JSON.stringify(baseline),
    [baseline, form],
  );
  const canSubmit = canEdit && hasUnsavedChanges && formIsValid && !isSubmitting;
  const showPrimaryAction = canEdit && hasUnsavedChanges && formIsValid && !isSubmitting;
  const primaryActionLabel = mode === "create" ? "Create Service" : "Save Changes";

  const nameField = useCommittedTextField<HTMLInputElement>(form.name, (value) => {
    setForm((current) => ({ ...current, name: value }));
    if (submitError) {
      setSubmitError(null);
    }
  });

  function markFieldBlurred(field: keyof ReturnType<typeof validateServiceForm>, nextForm: FundsphereServiceFormState = form) {
    setFieldErrors((current) => ({
      ...current,
      [field]: validateServiceField(field, nextForm),
    }));
  }

  useEffect(() => {
    if (!open) {
      setForm(toServiceForm(null));
      setBaseline(toServiceForm(null));
      setIsSubmitting(false);
      setDetailCacheStatus(null);
      setIsDetailRefreshing(false);
      setSubmitError(null);
      setIsDiscardDialogOpen(false);
      setFieldErrors(createEmptyServiceFieldErrors());
      return;
    }

    const nextForm = toServiceForm(service);
    setForm(nextForm);
    setBaseline(nextForm);
    setDetailCacheStatus(null);
    setIsDetailRefreshing(false);
    setSubmitError(null);
    setIsSubmitting(false);
    setIsDiscardDialogOpen(false);
    setFieldErrors(createEmptyServiceFieldErrors());
  }, [open, service]);

  async function refreshServiceDetail(policy: CachePolicy): Promise<void> {
    const serviceId = asString(service?.id);
    if (mode !== "edit" || !serviceId) {
      setDetailCacheStatus(null);
      setIsDetailRefreshing(false);
      return;
    }

    const requestToken = ++detailRequestTokenRef.current;
    const snapshot = readFundsphereServiceDetailCacheSnapshot(cacheContext, serviceId);
    const cachedService = snapshot?.data ? toUiService(snapshot.data) : null;
    const shouldFetch = shouldFetchNetwork(policy, snapshot);

    if (cachedService) {
      const cachedForm = toServiceForm(cachedService);
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
      const nextService = toUiService(await loadFundsphereService({
        requestJson,
        id: serviceId,
      }));
      if (requestToken !== detailRequestTokenRef.current) {
        return;
      }
      const nextForm = toServiceForm(nextService);
      setForm(nextForm);
      setBaseline(nextForm);
      const fetchedAt = Date.now();
      setDetailCacheStatus({
        source: "network",
        fetchedAt,
      });
      syncFundsphereServiceDetailCache(cacheContext, nextService, { source: "network", fetchedAt });
    } finally {
      if (requestToken === detailRequestTokenRef.current) {
        setIsDetailRefreshing(false);
      }
    }
  }

  useEffect(() => {
    if (!open || mode !== "edit" || !service?.id) {
      return;
    }
    void refreshServiceDetail("cache-first");
  }, [mode, open, service?.id]);

  function updateForm<K extends keyof FundsphereServiceFormState>(field: K, value: FundsphereServiceFormState[K]) {
    setForm((current) => ({
      ...current,
      [field]: value,
    }));
    if (submitError) {
      setSubmitError(null);
    }
  }

  function restoreBaseline() {
    setForm(baseline);
    setSubmitError(null);
    setFieldErrors(createEmptyServiceFieldErrors());
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
      const nextForm = buildServicePayloadFromForm(form);
      await onSubmit({
        mode,
        serviceId: service?.id ?? null,
        form: nextForm,
      });
      onOpenChange(false);
    } catch (error) {
      setSubmitError(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not save service.");
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          className="max-h-[92vh] max-w-[680px] overflow-hidden rounded-xl bg-white p-6"
          onInteractOutside={(event) => {
            if (isSubmitting || hasUnsavedChanges) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={isSubmitting} busyMessage={mode === "create" ? "Creating service..." : "Saving service..."} className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close service modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>{mode === "create" ? "Add Service" : "Edit Service"}</DialogTitle>
                <DialogDescription>
                  {mode === "create"
                    ? "Create a FundSphere service record for budgeting and department mapping."
                    : "Update the service record and keep the reference data current."}
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>

            <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1 space-y-4">
              <Section className="space-y-3">
                <FormRow
                  label={(
                    <>
                      Service Name<span className="ml-1 text-rose-600">*</span>
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

                <FormRow
                  label={(
                    <>
                      Department<span className="ml-1 text-rose-600">*</span>
                    </>
                  )}
                >
                  <AppDropdown
                    value={form.departmentCode}
                    options={departmentOptions}
                    onValueChange={(value) => {
                      updateForm("departmentCode", value.toUpperCase());
                      if (submitError) {
                        setSubmitError(null);
                      }
                    }}
                    loading={departmentLoading}
                    searchable
                    allowCustomValue={false}
                    ariaLabel="Department filter"
                    placeholder="Select department"
                    disabled={isSubmitting || !canEdit || departmentLoading}
                  />
                </FormRow>
                {fieldErrors.departmentCode ? <p className="text-sm text-rose-600">{fieldErrors.departmentCode}</p> : null}

                <FormRow label="Description" alignStart>
                  <Textarea
                    value={form.description}
                    onChange={(event) => updateForm("description", event.target.value)}
                    onBlur={() => markFieldBlurred("description")}
                    disabled={isSubmitting || !canEdit}
                    maxLength={2048}
                    rows={4}
                    placeholder="Optional service description"
                  />
                </FormRow>
                {fieldErrors.description ? <p className="text-sm text-rose-600">{fieldErrors.description}</p> : null}

                <div className="space-y-4">
                  <FormRow label="Commission">
                    <NumberUnitField
                      value={form.commission}
                      unit="%"
                      unitPosition="left"
                      onValueChange={(value) => updateForm("commission", value)}
                      onBlurValue={(value) => markFieldBlurred("commission", { ...form, commission: value })}
                      disabled={isSubmitting || !canEdit}
                      placeholder="0.00"
                      shellClassName="max-w-[18rem]"
                    />
                  </FormRow>
                  {fieldErrors.commission ? <p className="text-sm text-rose-600">{fieldErrors.commission}</p> : null}

                  <FormRow label="Net Adjustment">
                    <NumberUnitField
                      value={form.netAdjustment}
                      unit="$"
                      unitPosition="left"
                      onValueChange={(value) => updateForm("netAdjustment", value)}
                      onBlurValue={(value) => markFieldBlurred("netAdjustment", { ...form, netAdjustment: value })}
                      disabled={isSubmitting || !canEdit}
                      placeholder="0.00"
                      shellClassName="max-w-[18rem]"
                    />
                  </FormRow>
                  {fieldErrors.netAdjustment ? <p className="text-sm text-rose-600">{fieldErrors.netAdjustment}</p> : null}
                </div>

                <FormRow label="Active">
                  <button
                    type="button"
                    role="switch"
                    aria-checked={form.active}
                    aria-label="Active"
                    onClick={() => updateForm("active", !form.active)}
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

              {submitError ? <p className="mt-4 text-sm text-rose-600">{submitError}</p> : null}
            </div>

            {mode === "edit" ? (
              <ModalCacheFooter
                text={
                  isDetailRefreshing
                    ? "Refreshing service data..."
                    : detailCacheStatus
                      ? `Data source: ${detailCacheStatus.source}. Last updated ${formatRelativeTime(detailCacheStatus.fetchedAt)}.`
                      : "No cached service data yet"
                }
                onRefresh={() => {
                  if (!isDetailRefreshing && !hasUnsavedChanges && !isSubmitting) {
                    void refreshServiceDetail("network-only");
                  }
                }}
                disabled={isDetailRefreshing || hasUnsavedChanges || isSubmitting}
                refreshing={isDetailRefreshing}
                refreshLabel="Refresh service data"
                tooltipText={
                  hasUnsavedChanges
                    ? "Save or discard your edits before refreshing service data."
                    : "Click to refresh this service data"
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

function ServiceCard({
  service,
  departmentColor,
  departmentIdentity,
  disabled,
  canEdit,
  onEdit,
}: {
  service: FundsphereService;
  departmentColor: string | null;
  departmentIdentity: string;
  disabled?: boolean;
  canEdit: boolean;
  onEdit: (service: FundsphereService) => void;
}) {
  const departmentStyles = buildFundsphereDepartmentColorStyles({
    color: departmentColor,
    identity: departmentIdentity,
  });

  return (
    <article
      role="button"
      tabIndex={disabled || !canEdit ? -1 : 0}
      onClick={() => {
        if (!disabled && canEdit) {
          onEdit(service);
        }
      }}
      onKeyDown={(event) => {
        if (disabled || !canEdit) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onEdit(service);
        }
      }}
      className={cn(
        "rounded-[1.35rem] border p-4 shadow-[0_18px_30px_-24px_rgba(37,99,235,0.42)] transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
        disabled ? "pointer-events-none cursor-default" : "cursor-pointer",
        service.active
          ? "border-blue-100/90 bg-white hover:-translate-y-0.5 hover:border-blue-200 hover:shadow-[0_20px_34px_-24px_rgba(37,99,235,0.5)]"
          : "border-slate-200 bg-slate-50/90 hover:-translate-y-0.5 hover:border-slate-300 hover:shadow-[0_18px_28px_-26px_rgba(15,23,42,0.22)]",
      )}
      style={departmentStyles.cardStyle}
    >
      <div className="min-w-0">
        <TooltipTarget text={service.name}>
          <p className="truncate text-lg font-semibold tracking-[-0.02em] text-slate-900">{service.name}</p>
        </TooltipTarget>

        <div className="mt-2 flex flex-wrap items-center gap-2">
          <span
            className="inline-flex max-w-full items-center rounded-full border px-2.5 py-1 text-xs font-semibold"
            style={departmentStyles.chipStyle}
          >
            <span className="truncate">{service.departmentName}</span>
          </span>
          <span
            className={cn(
              "inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium",
              buildServiceStatusClass(service.active),
            )}
          >
            {buildServiceStatusLabel(service.active)}
          </span>
        </div>

        {service.description ? (
          <p className="mt-3 line-clamp-2 text-sm leading-6 text-slate-600">{service.description}</p>
        ) : null}
      </div>
    </article>
  );
}

function FundsphereServicesPage() {
  const { requestJson } = useApiRequest();
  const { isOnline } = useOnlineStatus();
  const auth = useAuth();

  const cacheContext = useMemo<FundsphereServicesCacheContext>(
    () => ({
      tenantSlug: auth.tenantSlug || "",
      userKey: auth.user?.id || auth.user?.email || "",
    }),
    [auth.tenantSlug, auth.user?.email, auth.user?.id],
  );
  const cacheKey = useMemo(() => buildFundsphereServicesCacheKey(cacheContext), [cacheContext]);
  const departmentsCacheContext = useMemo<FundsphereServicesDepartmentsCacheContext>(
    () => ({
      tenantSlug: auth.tenantSlug || "",
    }),
    [auth.tenantSlug],
  );
  const canEditFundsphere = useMemo(() => {
    if (!shouldProtectFrontendAuth()) {
      return true;
    }
    return hasAppEditAccess(auth.accessProfile, FUNDSPHERE_APP_CODE);
  }, [auth.accessProfile]);

  const [pageState, setPageState, pageStateControls] = useScopedPersistentState<PersistedServicesPageState>(
    {
      appCode: FUNDSPHERE_APP_CODE,
      pageCode: FUNDSPHERE_SERVICES_PAGE_CODE,
      stateKey: "filters",
    },
    EMPTY_PAGE_STATE,
    { validate: isPersistedServicesPageState },
  );

  const [services, setServices] = useState<FundsphereService[] | null>(null);
  const servicesRef = useRef<FundsphereService[] | null>(null);
  const [departments, setDepartments] = useState<FundsphereDepartment[]>([]);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const requestTokenRef = useRef(0);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState<ServiceMode>("create");
  const [modalService, setModalService] = useState<FundsphereService | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [isLoadingDepartments, setIsLoadingDepartments] = useState(true);
  const didRestoreSearchStateRef = useRef<string | null>(null);
  const departmentLoadTokenRef = useRef(0);
  const [serviceGroupOpenState, setServiceGroupOpenState] = useState<Record<string, boolean>>({});
  const searchDraft = pageState.searchDraft;
  const searchCriteria = pageState.searchCriteria;
  const hasSearched = pageState.hasSearched;

  useEffect(() => {
    servicesRef.current = services;
  }, [services]);

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
    void refreshServices("cache-first", searchCriteria);
  }, [cacheKey, hasSearched, pageStateControls.hydrated, searchCriteria]);

  function commitServices(
    nextServices: FundsphereService[] | null,
    source: "cache" | "network",
    criteria: FundsphereServiceSearchCriteria,
  ) {
    servicesRef.current = nextServices;
    setServices(nextServices);
    if (!nextServices) {
      return;
    }
    const fetchedAt = Date.now();
    setCacheStatus({ source, fetchedAt });
    setPageState((current) => ({
      ...current,
      hasSearched: true,
    }));
    syncFundsphereServicesCache(cacheContext, nextServices, criteria, { source, fetchedAt });
  }

  async function refreshServices(
    policy: CachePolicy,
    criteria: FundsphereServiceSearchCriteria = searchCriteria,
  ): Promise<void> {
    const requestToken = ++requestTokenRef.current;
    const snapshot = readFundsphereServicesCacheSnapshot(cacheContext, criteria);
    const cachedServices = snapshot?.data ? sortServices(snapshot.data) : null;
    const shouldFetch = shouldFetchNetwork(policy, snapshot);

    if (cachedServices) {
      commitServices(cachedServices, "cache", criteria);
      setErrorMessage(null);
      setRefreshMessage(null);
    }

    if (!shouldFetch) {
      setIsLoading(false);
      setIsRefreshing(false);
      return;
    }

    if (!isOnline) {
      if (cachedServices) {
        setRefreshMessage("You're offline. Showing cached services.");
      } else {
        setErrorMessage("You're offline. Connect to the internet to load services.");
      }
      setIsLoading(false);
      setIsRefreshing(false);
      return;
    }

    if (cachedServices) {
      setIsRefreshing(true);
    } else {
      setIsLoading(true);
      setRefreshMessage(null);
      setErrorMessage(null);
    }

    try {
      const nextServices = sortServices(
        await loadFundsphereServices({
          requestJson,
          criteria,
        }),
      );
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      commitServices(nextServices, "network", criteria);
      setRefreshMessage(null);
      setErrorMessage(null);
    } catch (error) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      if (servicesRef.current || cachedServices) {
        setRefreshMessage("Showing cached services. Could not refresh.");
        setErrorMessage(null);
      } else {
        setErrorMessage(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not load services.");
      }
    } finally {
      if (requestToken === requestTokenRef.current) {
        setIsLoading(false);
        setIsRefreshing(false);
      }
    }
  }

  async function refreshDepartments(policy: CachePolicy): Promise<void> {
    const requestToken = ++departmentLoadTokenRef.current;
    const snapshot = readFundsphereServicesDepartmentsCacheSnapshot(departmentsCacheContext);
    const cachedDepartments = snapshot?.data ? sortDepartments(snapshot.data) : null;
    const shouldFetch = shouldFetchNetwork(policy, snapshot);

    if (cachedDepartments) {
      setDepartments(cachedDepartments);
    }

    if (!shouldFetch) {
      setIsLoadingDepartments(false);
      return;
    }

    if (!isOnline) {
      if (!cachedDepartments) {
        setDepartments([]);
      }
      setIsLoadingDepartments(false);
      return;
    }

    setIsLoadingDepartments(true);

    try {
      const nextDepartments = sortDepartments(await loadFundsphereDepartments({ requestJson }));
      if (requestToken !== departmentLoadTokenRef.current) {
        return;
      }
      setDepartments(nextDepartments);
      syncFundsphereServicesDepartmentsCache(departmentsCacheContext, nextDepartments, {
        source: "network",
        fetchedAt: Date.now(),
      });
    } catch {
      if (requestToken !== departmentLoadTokenRef.current) {
        return;
      }
      if (!cachedDepartments) {
        setDepartments([]);
      }
    } finally {
      if (requestToken === departmentLoadTokenRef.current) {
        setIsLoadingDepartments(false);
      }
    }
  }

  useEffect(() => {
    servicesRef.current = null;
    setServices(null);
    setCacheStatus(null);
    setRefreshMessage(null);
    setErrorMessage(null);
    setIsLoading(false);
    setIsRefreshing(false);
    setIsModalOpen(false);
    setModalMode("create");
    setModalService(null);
    setIsSaving(false);
    setDepartments([]);
    setIsLoadingDepartments(true);
    ++requestTokenRef.current;
  }, [cacheKey]);

  useEffect(() => {
    if (!pageStateControls.hydrated) {
      return;
    }
    void refreshDepartments("cache-first");
  }, [departmentsCacheContext, isOnline, pageStateControls.hydrated, requestJson]);

  const departmentOptions = useMemo(() => {
    return buildDepartmentOptions(departments, services);
  }, [departments, services]);

  const departmentColorsByCode = useMemo(
    () =>
      new Map(
        departments.map((department) => [
          department.code.toUpperCase(),
          department.color,
        ]),
      ),
    [departments],
  );

  const departmentSearchOptions = useMemo<AppDropdownOption[]>(
    () => [{ value: "", label: "" }, ...departmentOptions],
    [departmentOptions],
  );

  const filteredServices = useMemo(() => {
    if (!hasSearched || !services) {
      return [];
    }
    return sortServices(services);
  }, [hasSearched, services]);

  const serviceGroups = useMemo(
    () => buildServiceGroups(filteredServices, departmentColorsByCode),
    [departmentColorsByCode, filteredServices],
  );

  const hasServices = Boolean(services && services.length > 0);
  const hasMatches = Boolean(hasSearched && filteredServices.length > 0);
  const hasFilters = Boolean(searchCriteria.name.trim()) || Boolean(searchCriteria.departmentCode.trim()) || searchCriteria.statusFilter !== DEFAULT_SERVICE_STATUS_FILTER;
  const hasDraftFilters = hasServiceSearchCriteria(searchDraft);
  const emptyMessage = buildEmptyMessage({
    hasSearched,
    hasFilters,
    hasServices,
    hasMatches,
    statusFilter: searchCriteria.statusFilter,
  });

  const searchResultText = !hasSearched
    ? "Search by one or more fields. Results load only after you click Search."
    : !services
      ? "Searching services..."
      : filteredServices.length === 0
        ? "No services matched your search."
        : `${filteredServices.length} service${filteredServices.length === 1 ? "" : "s"} shown.`;

  const pageMessages: StackMessage[] = [];
  if (refreshMessage) {
    pageMessages.push({
      id: "fundsphere-services-refresh",
      variant: refreshMessage.toLowerCase().includes("offline") ? "info" : "warning",
      message: refreshMessage,
    });
  }
  if (errorMessage) {
    pageMessages.push({
      id: "fundsphere-services-error",
      variant: "error",
      message: errorMessage,
    });
  }

  const cacheStatusText = isRefreshing
    ? "Refreshing services..."
    : !isOnline && cacheStatus
      ? `Offline. Showing cached services from ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : cacheStatus
        ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
        : "No cached services yet";

  function handleSearchSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const nextSearchCriteria = { ...pageState.searchDraft };
    const shouldForceRefresh =
      Boolean(servicesRef.current) && areServiceSearchCriteriaEqual(nextSearchCriteria, pageState.searchCriteria);

    setPageState((current) => ({
      ...current,
      searchCriteria: nextSearchCriteria,
      hasSearched: true,
    }));

    if (shouldForceRefresh) {
      void refreshServices("network-only", nextSearchCriteria);
      return;
    }

    if (!servicesRef.current) {
      void refreshServices("cache-first", nextSearchCriteria);
      return;
    }

    void refreshServices("cache-first", nextSearchCriteria);
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
    setModalService(null);
    setIsModalOpen(true);
  }

  function openEditModal(service: FundsphereService) {
    setModalMode("edit");
    setModalService(service);
    setIsModalOpen(true);
  }

  async function handleServiceSubmit(payload: {
    mode: ServiceMode;
    serviceId: string | null;
    form: FundsphereServiceFormState;
  }) {
    if (!canEditFundsphere || isSaving) {
      return;
    }

    setIsSaving(true);
    setErrorMessage(null);
    try {
      if (payload.mode === "create") {
        await createFundsphereService({
          requestJson,
          form: payload.form,
        });
      } else {
        if (!payload.serviceId) {
          throw new Error("Service ID is required.");
        }
        await updateFundsphereService({
          requestJson,
          id: payload.serviceId,
          form: payload.form,
        });

        const department = departments.find((item) => item.code === asString(payload.form.departmentCode).toUpperCase());
        if (modalService) {
          syncFundsphereServiceDetailCache(
            cacheContext,
            {
              ...modalService,
              name: asString(payload.form.name),
              departmentCode: asString(payload.form.departmentCode).toUpperCase(),
              departmentName: department?.name ?? modalService.departmentName,
              departmentListingOrder: department?.listingOrder ?? modalService.departmentListingOrder ?? null,
              description: asString(payload.form.description) || null,
              commission: asString(payload.form.commission) || "0.00",
              netAdjustment: asString(payload.form.netAdjustment) || "0.00",
              active: Boolean(payload.form.active),
            },
            { source: "network", fetchedAt: Date.now() },
          );
        }
      }

      void refreshServices("network-only", searchCriteria);
    } catch (error) {
      throw error instanceof Error ? error : new Error("Could not save service.");
    } finally {
      setIsSaving(false);
    }
  }

  const activeCount = services?.filter((service) => service.active).length ?? 0;
  const inactiveCount = (services?.length ?? 0) - activeCount;
  const canRefresh = Boolean(isOnline && !isLoading && !isRefreshing && !isSaving && !isModalOpen);
  const canRefreshDepartments = Boolean(isOnline && !isLoadingDepartments);

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          gradientVariant="fundsphere"
          eyebrow="FundSphere"
          title="Services"
          description="Manage service records used by budgets, departments, and operational reporting."
          action={(
            <Button onClick={openCreateModal} disabled={!canEditFundsphere}>
              <Plus className="size-4" />
              Add Service
            </Button>
          )}
        />
      )}
      footer={services || cacheStatus ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            void Promise.all([
              refreshServices("network-only", searchCriteria),
              refreshDepartments("network-only"),
            ]);
          }}
          disabled={!canRefresh || !canRefreshDepartments}
          refreshing={isRefreshing}
          refreshLabel="Refresh page"
          tooltipText={isOnline ? "Click to refresh services and departments" : "Offline. Reconnect to refresh the page."}
          containerClassName="w-full"
        />
      ) : null}
    >
      <Section className="rounded-[1.45rem] border border-blue-100/90 bg-white/95 p-5 shadow-soft">
        <SectionHeader
          title="Services Search"
          description="Select search criteria, then click Search to load matching services."
        />

        <form className="space-y-5 px-1.5" onSubmit={handleSearchSubmit}>
          <div className="grid grid-cols-1 gap-3.5 md:grid-cols-2 xl:grid-cols-4">
            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Service Name
              </span>
              <div className="relative">
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
                  placeholder="Search service name"
                  autoComplete="off"
                  spellCheck={false}
                  className="pl-10"
                />
                <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-400" />
              </div>
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Department
              </span>
              <AppDropdown
                value={searchDraft.departmentCode}
                options={departmentSearchOptions}
                onValueChange={(value) =>
                  setPageState((current) => ({
                    ...current,
                    searchDraft: {
                      ...current.searchDraft,
                      departmentCode: value.toUpperCase(),
                    },
                  }))
                }
                searchable={false}
                loading={isLoadingDepartments}
                allowCustomValue={false}
                ariaLabel="Department filter"
                placeholder=""
              />
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Active
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
                ariaLabel="Service status filter"
                placeholder="Active"
              />
            </label>

            <div className="hidden xl:block" aria-hidden="true" />
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
        {services && filteredServices.length > 0 ? (
          <SectionCard
            title="Services"
            description={`${filteredServices.length} service${filteredServices.length === 1 ? "" : "s"} matched the current filters.`}
            actions={(
              <div className="flex items-center gap-2 text-xs font-medium text-slate-500">
                <span className="rounded-full bg-emerald-50 px-2.5 py-1 text-emerald-700">Active {activeCount}</span>
                <span className="rounded-full bg-slate-100 px-2.5 py-1 text-slate-600">Inactive {inactiveCount}</span>
              </div>
            )}
            contentClassName="space-y-4"
          >
            <div className="space-y-4">
              {serviceGroups.map((group) => {
                const departmentStyles = buildFundsphereDepartmentColorStyles({
                  color: group.departmentColor,
                  identity: `${group.key}:${group.label}`,
                });

                return (
                <details
                  key={group.key}
                  open={serviceGroupOpenState[group.key] ?? true}
                  onToggle={(event) => {
                    const nextOpen = event.currentTarget.open;
                    setServiceGroupOpenState((current) => ({ ...current, [group.key]: nextOpen }));
                  }}
                  className="group overflow-hidden rounded-2xl border shadow-[0_18px_30px_-26px_rgba(37,99,235,0.24)]"
                >
                  <summary
                    className="flex cursor-pointer list-none items-center justify-between gap-2 border-b px-4 py-3.5"
                    style={departmentStyles.headerStyle}
                  >
                    <p className="text-sm font-semibold uppercase tracking-[0.12em]">{group.label}</p>
                    <span className="inline-flex items-center gap-2 text-slate-500" aria-hidden="true">
                      <span className="text-xs">{group.items.length} services</span>
                      <ChevronDown className="size-4 transition-transform group-open:rotate-180" />
                    </span>
                  </summary>

                  <div className="p-3">
                    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                      {group.items.map((service) => (
                        <ServiceCard
                          key={service.id}
                          service={service}
                          departmentColor={group.departmentColor}
                          departmentIdentity={`${group.key}:${group.label}`}
                          canEdit={canEditFundsphere}
                          onEdit={openEditModal}
                        />
                      ))}
                    </div>
                  </div>
                </details>
                );
              })}
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
          active={Boolean(isRefreshing && services)}
          message="Refreshing services..."
          className="rounded-[1.1rem]"
        />
      </div>

      <PageLoadingLayer active={Boolean(isLoading && !services)} message="Loading services..." />

      <ServiceModal
        open={isModalOpen}
        mode={modalMode}
        service={modalService}
        departmentOptions={departmentOptions}
        departmentLoading={isLoadingDepartments}
        canEdit={canEditFundsphere}
        onOpenChange={setIsModalOpen}
        requestJson={requestJson}
        onSubmit={handleServiceSubmit}
        cacheContext={cacheContext}
        isOnline={isOnline}
      />
    </AppPageLayout>
  );
}

export default FundsphereServicesPage;
