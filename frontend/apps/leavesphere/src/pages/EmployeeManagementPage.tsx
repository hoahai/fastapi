import { useEffect, useMemo, useRef, useState, type ClipboardEvent } from "react";
import {
  ChevronDown,
  RefreshCw,
  UploadCloud,
  Search,
  UserCheck,
  UserRound,
  UserX,
  Users,
  Trash2,
  X,
} from "lucide-react";

import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { Section, SectionHeader } from "@shared/components";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { DateInputField } from "@tradsphere/components/dashboard/FlightDateRangeField";
import { LabeledField } from "@tradsphere/components/dashboard/FormFieldRow";
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
import { ConfirmDialog } from "@tradsphere/components/ui/confirm-dialog";
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { useOnlineStatus } from "@shared/hooks/useOnlineStatus";
import { shouldProtectFrontendAuth } from "@shared/auth/guards";
import { hasAppAdminAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { PageBanner } from "@shell/components/layout/PageBanner";
import { ActionIconButton } from "@tradsphere/components/dashboard/ActionIconButton";
import { ModalCacheFooter, ModalCloseButton, ModalFooter, ModalHeaderRow, ModalShell } from "@shared/components";
import { cn } from "@shared/components/utils/cn";
import {
  normalizeUsPhoneDisplay,
  normalizeUsPhoneOnInput,
  validateUsPhoneField,
} from "@shared/utils/phone";
import {
  buildScopedPageStateStorageKey,
  readScopedPageState,
  shouldFetchNetwork,
  writeScopedPageState,
  type CachePolicy,
  type ScopedPageState,
} from "@shared/cache";
import {
  activateLeaveSphereEmployeeManagementEmployee,
  createLeaveSphereEmployeeManagementEmployee,
  createLeaveSphereEmployeeManagementManager,
  deactivateLeaveSphereEmployeeManagementEmployee,
  deleteLeaveSphereEmployeeManagementManager,
  extractLeaveSphereEmployeeManagementCreatedEmployeeId,
  extractLeaveSphereEmployeeManagementEmployeeFromPayload,
  loadLeaveSphereEmployeeManagementWorkspace,
  loadLeaveSphereEmployeeManagementManagers,
  normalizeLeaveSphereEmployeeManagementEmployee,
  normalizeLeaveSphereEmployeeManagementForm,
  normalizeLeaveSphereEmployeeManagementWorkspace,
  updateLeaveSphereEmployeeManagementEmployee,
  uploadLeaveSphereEmployeeManagementPicture,
  sortLeaveSphereEmployeeManagementEmployees,
  type LeaveSphereEmployeeManagementManager,
  type LeaveSphereEmployeeManagementEmployee,
  type LeaveSphereEmployeeManagementFormState,
  type LeaveSphereEmployeeManagementRequestJson,
  type LeaveSphereEmployeeManagementWorkspace,
} from "@leavesphere/lib/employeeManagementApi";
import {
  LEAVESPHERE_EMPLOYEE_MANAGEMENT_PAGE_CODE,
  type LeaveSphereEmployeeManagementManagerCacheItem,
  readLeaveSphereEmployeeManagementManagerCacheSnapshot,
  buildLeaveSphereEmployeeManagementWorkspaceCacheKey,
  readLeaveSphereEmployeeManagementWorkspaceCacheSnapshot,
  syncLeaveSphereEmployeeManagementManagerCache,
  syncLeaveSphereEmployeeManagementWorkspaceCache,
  type LeaveSphereEmployeeManagementWorkspaceCacheContext,
  type LeaveSphereEmployeeManagementManagerCacheContext,
} from "@leavesphere/lib/employeeManagementCache";

type EmployeeMode = "create" | "edit";
type EmployeeGroupOpenState = Record<string, boolean>;
type EmployeePictureAttachment = {
  name: string;
  meta: string;
  previewSrc: string;
};

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type EmployeeStatusFilter = "" | "active" | "inactive";
type EmployeeRegionFilter = "" | "US" | "Mexico" | "Philippines";
type EmployeeSearchCriteria = {
  nameOrTitle: string;
  email: string;
  statusFilter: EmployeeStatusFilter;
  regionFilter: EmployeeRegionFilter;
};

type PersistedEmployeeManagementPageState = {
  searchDraft: EmployeeSearchCriteria;
  searchCriteria: EmployeeSearchCriteria;
  hasSearched: boolean;
  groupOpenState: EmployeeGroupOpenState;
};

type EmployeeModalProps = {
  open: boolean;
  mode: EmployeeMode;
  employee: LeaveSphereEmployeeManagementEmployee | null;
  canEdit: boolean;
  onOpenChange: (open: boolean) => void;
  requestJson: LeaveSphereEmployeeManagementRequestJson;
  employeeOptions: AppDropdownOption[];
  onUploadPicture: (file: File) => Promise<string>;
  onSubmit: (payload: { mode: EmployeeMode; id: string | null; form: LeaveSphereEmployeeManagementFormState }) => Promise<void>;
  detailCacheStatusText?: string | null;
  detailCacheRefreshing?: boolean;
  detailCacheRefreshDisabled?: boolean;
  onRefreshDetailCache?: () => void;
  managerCacheContext: LeaveSphereEmployeeManagementManagerCacheContext;
};

const EMPLOYEE_REGION_OPTIONS: AppDropdownOption[] = [
  { value: "US", label: "🇺🇸 U.S." },
  { value: "Mexico", label: "🇲🇽 Mexico" },
  { value: "Philippines", label: "🇵🇭 Philippines" },
];

const EMPLOYEE_STATUS_OPTIONS: AppDropdownOption[] = [
  { value: "", label: "" },
  { value: "active", label: "Active" },
  { value: "inactive", label: "Inactive" },
];

const EMPLOYEE_REGION_FILTER_OPTIONS: AppDropdownOption[] = [
  { value: "", label: "" },
  { value: "US", label: "🇺🇸 U.S." },
  { value: "Mexico", label: "🇲🇽 Mexico" },
  { value: "Philippines", label: "🇵🇭 Philippines" },
];

const DEFAULT_SEARCH_CRITERIA: EmployeeSearchCriteria = {
  nameOrTitle: "",
  email: "",
  statusFilter: "",
  regionFilter: "",
};

const EMPTY_SEARCH_CRITERIA: EmployeeSearchCriteria = {
  nameOrTitle: "",
  email: "",
  statusFilter: "",
  regionFilter: "",
};

const EMPTY_FORM: LeaveSphereEmployeeManagementFormState = {
  identityKey: "",
  firstName: "",
  lastName: "",
  email: "",
  phone: "",
  dob: "",
  pictureUrl: "",
  region: "US",
  startDate: "",
  title: "",
  isAE: false,
  active: true,
  managerIds: [],
};

const LEAVESPHERE_APP_CODE = "leavesphere";

function getTodayIsoDate(): string {
  const today = new Date();
  const year = today.getFullYear();
  const month = String(today.getMonth() + 1).padStart(2, "0");
  const day = String(today.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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

function formatFileSize(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const kilobytes = bytes / 1024;
  if (kilobytes < 1024) {
    return `${kilobytes.toFixed(1)} KB`;
  }
  return `${(kilobytes / 1024).toFixed(1)} MB`;
}

function buildEmployeeFullName(employee: LeaveSphereEmployeeManagementEmployee): string {
  return [employee.firstName, employee.lastName].filter(Boolean).join(" ").trim() || employee.email;
}

function buildEmployeeNameOrTitleSearchText(employee: LeaveSphereEmployeeManagementEmployee): string {
  return [
    employee.firstName,
    employee.lastName,
    buildEmployeeFullName(employee),
    employee.title,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function buildEmployeeOptionLabel(employee: LeaveSphereEmployeeManagementEmployee): string {
  return buildEmployeeFullName(employee);
}

function buildEmployeeOptions(employees: LeaveSphereEmployeeManagementEmployee[], excludeEmployeeId?: string | null): AppDropdownOption[] {
  return sortLeaveSphereEmployeeManagementEmployees(employees)
    .filter((employee) => employee.id !== excludeEmployeeId)
    .map((employee) => ({
      value: employee.id,
      label: buildEmployeeOptionLabel(employee),
      keywords: [buildEmployeeFullName(employee), employee.email, employee.title || ""].filter(Boolean).join(" "),
      muted: !employee.active,
    }));
}

function normalizeEmployeeForm(employee: LeaveSphereEmployeeManagementEmployee | null): LeaveSphereEmployeeManagementFormState {
  if (!employee) {
    return {
      ...EMPTY_FORM,
      startDate: getTodayIsoDate(),
    };
  }
  return {
    identityKey: employee.identityKey,
    firstName: employee.firstName,
    lastName: employee.lastName,
    email: employee.email,
    phone: employee.phone ?? "",
    dob: employee.dob ?? "",
    pictureUrl: employee.pictureUrl ?? "",
    region: employee.region,
    startDate: employee.startDate ?? "",
    title: employee.title ?? "",
    isAE: employee.isAE,
    active: employee.active,
    managerIds: [],
  };
}

function getFileNameFromUrl(url: string): string {
  const text = asString(url);
  if (!text) {
    return "Profile picture";
  }
  const fallback = "Profile picture";
  const decode = (value: string) => {
    try {
      return decodeURIComponent(value);
    } catch {
      return value;
    }
  };
  try {
    const parsed = new URL(text);
    const segments = parsed.pathname.split("/").filter(Boolean);
    return decode(segments[segments.length - 1] || fallback);
  } catch {
    const segments = text.split("/").filter(Boolean);
    return decode(segments[segments.length - 1] || fallback);
  }
}


function buildEmployeePictureAttachmentFromPictureUrl(pictureUrl: string): EmployeePictureAttachment | null {
  const normalizedPictureUrl = asString(pictureUrl);
  if (!normalizedPictureUrl) {
    return null;
  }
  return {
    name: getFileNameFromUrl(normalizedPictureUrl),
    meta: "Stored image",
    previewSrc: normalizedPictureUrl,
  };
}

function buildEmployeePictureAttachmentFromFile(file: File, pictureUrl: string): EmployeePictureAttachment {
  return {
    name: asString(file.name) || "profile-picture",
    meta: `${asString(file.type) || "Unknown type"} • ${formatFileSize(file.size)}`,
    previewSrc: pictureUrl,
  };
}

function normalizeManagerIds(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const item of value) {
    const candidate = asString(item);
    if (!candidate || seen.has(candidate)) {
      continue;
    }
    seen.add(candidate);
    normalized.push(candidate);
  }
  normalized.sort((left, right) => left.localeCompare(right));
  return normalized;
}

function normalizeEmployeeSearchCriteria(value: unknown, fallback: EmployeeSearchCriteria): EmployeeSearchCriteria {
  if (!isRecord(value)) {
    return { ...fallback };
  }
  return {
    nameOrTitle: asString(value.nameOrTitle),
    email: asString(value.email),
    statusFilter: coerceEmployeeStatusFilter(asString(value.statusFilter), fallback.statusFilter),
    regionFilter: coerceEmployeeRegionFilter(asString(value.regionFilter), fallback.regionFilter),
  };
}

function formsEqual(left: LeaveSphereEmployeeManagementFormState, right: LeaveSphereEmployeeManagementFormState): boolean {
  return JSON.stringify(normalizeLeaveSphereEmployeeManagementForm(left)) === JSON.stringify(normalizeLeaveSphereEmployeeManagementForm(right));
}

function validateEmail(value: string): string | null {
  const email = asString(value).toLowerCase();
  if (!email) {
    return "Email is required.";
  }
  const valid = /^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$/.test(email);
  return valid ? null : "Email must be valid.";
}

function validateOptionalEmail(value: string): string | null {
  const email = asString(value).toLowerCase();
  if (!email) {
    return null;
  }
  const valid = /^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$/.test(email);
  return valid ? null : "Email must be valid.";
}

function validateIsoDate(value: string, field: string): string | null {
  const text = asString(value);
  if (!text) {
    return null;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return `${field} must be a date.`;
  }
  return null;
}

function validateForm(form: LeaveSphereEmployeeManagementFormState): {
  firstName: string | null;
  lastName: string | null;
  email: string | null;
  phone: string | null;
  dob: string | null;
  pictureUrl: string | null;
  startDate: string | null;
} {
  const firstName = asString(form.firstName);
  const lastName = asString(form.lastName);
  const phone = asString(form.phone);
  const pictureUrl = asString(form.pictureUrl);
  return {
    firstName: !firstName ? "First name is required." : null,
    lastName: !lastName ? "Last name is required." : null,
    email: validateEmail(form.email),
    phone: validateUsPhoneField(phone, {
      field: "Phone",
      maxLength: 20,
      allowExtension: false,
    }),
    dob: validateIsoDate(form.dob, "Date of birth"),
    pictureUrl: pictureUrl.length > 2048 ? "Picture URL must be 2048 characters or fewer." : null,
    startDate: validateIsoDate(form.startDate, "Start date"),
  };
}

function isFormValid(errors: ReturnType<typeof validateForm>): boolean {
  return Object.values(errors).every((item) => item === null);
}

function createEmptyFieldErrors(): ReturnType<typeof validateForm> {
  return {
    firstName: null,
    lastName: null,
    email: null,
    phone: null,
    dob: null,
    pictureUrl: null,
    startDate: null,
  };
}

function validateEmployeeField(
  field: keyof ReturnType<typeof validateForm>,
  form: LeaveSphereEmployeeManagementFormState,
): string | null {
  switch (field) {
    case "firstName":
      return asString(form.firstName) ? null : "First name is required.";
    case "lastName":
      return asString(form.lastName) ? null : "Last name is required.";
    case "email":
      return validateEmail(form.email);
    case "phone": {
      return validateUsPhoneField(form.phone, {
        field: "Phone",
        maxLength: 20,
        allowExtension: false,
      });
    }
    case "dob":
      return validateIsoDate(form.dob, "Date of birth");
    case "pictureUrl": {
      const pictureUrl = asString(form.pictureUrl);
      return pictureUrl.length > 2048 ? "Picture URL must be 2048 characters or fewer." : null;
    }
    case "startDate":
      return validateIsoDate(form.startDate, "Start date");
    default:
      return null;
  }
}

function buildEmployeeStatusLabel(employee: LeaveSphereEmployeeManagementEmployee): string {
  return employee.active ? "Active" : "Inactive";
}

function buildEmployeeRegionLabel(region: string): string {
  if (region === "US") {
    return "U.S.";
  }
  return region;
}

function coerceEmployeeStatusFilter(value: string, fallback: EmployeeStatusFilter): EmployeeStatusFilter {
  if (value === "" || value === "active" || value === "inactive") {
    return value;
  }
  return fallback;
}

function coerceEmployeeRegionFilter(value: string, fallback: EmployeeRegionFilter): EmployeeRegionFilter {
  if (value === "" || value === "US" || value === "Mexico" || value === "Philippines") {
    return value;
  }
  return fallback;
}

function getEmployeeRegionSortOrder(region: string): number {
  if (region === "US") {
    return 0;
  }
  if (region === "Mexico") {
    return 1;
  }
  if (region === "Philippines") {
    return 2;
  }
  return 3;
}

function isPersistedEmployeeManagementPageState(value: unknown): value is PersistedEmployeeManagementPageState {
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.hasSearched !== "boolean") {
    return false;
  }
  if (!isRecord(value.searchDraft) || !isRecord(value.searchCriteria) || !isRecord(value.groupOpenState)) {
    return false;
  }

  const searchDraft = value.searchDraft;
  const searchCriteria = value.searchCriteria;
  const groupOpenState = value.groupOpenState;

  return (
    typeof searchDraft.nameOrTitle === "string" &&
    typeof searchDraft.email === "string" &&
    typeof searchDraft.statusFilter === "string" &&
    typeof searchDraft.regionFilter === "string" &&
    typeof searchCriteria.nameOrTitle === "string" &&
    typeof searchCriteria.email === "string" &&
    typeof searchCriteria.statusFilter === "string" &&
    typeof searchCriteria.regionFilter === "string" &&
    Object.values(groupOpenState).every((item) => typeof item === "boolean")
  );
}

function hasEmployeeSearchCriteria(criteria: EmployeeSearchCriteria): boolean {
  return Boolean(criteria.nameOrTitle.trim()) || Boolean(criteria.email.trim()) || Boolean(criteria.statusFilter) || Boolean(criteria.regionFilter);
}

function areEmployeeSearchCriteriaEqual(left: EmployeeSearchCriteria, right: EmployeeSearchCriteria): boolean {
  return (
    left.nameOrTitle.trim() === right.nameOrTitle.trim() &&
    left.email.trim() === right.email.trim() &&
    left.statusFilter === right.statusFilter &&
    left.regionFilter === right.regionFilter
  );
}

function getEmptyMessage(
  hasSearched: boolean,
  hasFilters: boolean,
  hasEmployees: boolean,
  hasMatches: boolean,
): { title: string; description: string } {
  if (!hasSearched) {
    return {
      title: "Search employees",
      description: "Select a search criterion, then click Search to load matching employees.",
    };
  }
  if (!hasMatches && hasFilters) {
    return {
      title: "No matches",
      description: "No employees matched the current search criteria. Adjust the criteria and search again.",
    };
  }
  if (!hasEmployees) {
    return {
      title: "No employees yet",
      description: "Add the first employee record to start managing the LeaveSphere workspace.",
    };
  }
  if (hasFilters) {
    return {
      title: "No matches",
      description: "No employees match the current filters. Clear the filters to see the full list.",
    };
  }
  return {
    title: "No employees found",
    description: "The workspace did not return any employee rows.",
  };
}

function toEmployeeCardStatusClass(active: boolean): string {
  return active
    ? "border-emerald-200 bg-emerald-50/80 text-emerald-800"
    : "border-slate-300 bg-slate-100/90 text-slate-700";
}

function normalizeStatusFilterValue(value: string): EmployeeStatusFilter {
  if (value === "" || value === "active" || value === "inactive") {
    return value;
  }
  return "active";
}

function normalizeRegionFilterValue(value: string): EmployeeRegionFilter {
  if (value === "US" || value === "Mexico" || value === "Philippines") {
    return value;
  }
  return "";
}

function EmployeeManagementModal({
  open,
  mode,
  employee,
  canEdit,
  onOpenChange,
  requestJson,
  employeeOptions,
  onUploadPicture,
  onSubmit,
  detailCacheStatusText,
  detailCacheRefreshing = false,
  detailCacheRefreshDisabled = false,
  onRefreshDetailCache,
  managerCacheContext,
}: EmployeeModalProps) {
  const [form, setForm] = useState<LeaveSphereEmployeeManagementFormState>(() => normalizeEmployeeForm(employee));
  const [baseline, setBaseline] = useState<LeaveSphereEmployeeManagementFormState>(() => normalizeEmployeeForm(employee));
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const [isPicturePreviewOpen, setIsPicturePreviewOpen] = useState(false);
  const [picturePreviewError, setPicturePreviewError] = useState(false);
  const [isPictureUploading, setIsPictureUploading] = useState(false);
  const [pictureUploadError, setPictureUploadError] = useState<string | null>(null);
  const [pictureDraftFile, setPictureDraftFile] = useState<File | null>(null);
  const [pictureDraftObjectUrl, setPictureDraftObjectUrl] = useState<string | null>(null);
  const [isManagersLoading, setIsManagersLoading] = useState(false);
  const [managerLoadError, setManagerLoadError] = useState<string | null>(null);
  const [fieldErrors, setFieldErrors] = useState(() => createEmptyFieldErrors());
  const pictureFileInputRef = useRef<HTMLInputElement | null>(null);
  const originalPictureUrlRef = useRef<string>(asString(employee?.pictureUrl));
  const managerOptions = useMemo(
    () => employeeOptions.filter((option) => (
      option.value !== employee?.id && (!option.muted || form.managerIds.includes(option.value))
    )),
    [employee?.id, employeeOptions, form.managerIds],
  );
  const pictureAttachment = useMemo(() => {
    if (pictureDraftFile && pictureDraftObjectUrl) {
      return buildEmployeePictureAttachmentFromFile(pictureDraftFile, pictureDraftObjectUrl);
    }
    return buildEmployeePictureAttachmentFromPictureUrl(asString(form.pictureUrl) || originalPictureUrlRef.current);
  }, [form.pictureUrl, pictureDraftFile, pictureDraftObjectUrl]);
  const hasUnsavedChanges = useMemo(() => !formsEqual(form, baseline) || Boolean(pictureDraftFile), [baseline, form, pictureDraftFile]);
  const currentFormErrors = useMemo(() => validateForm(form), [form]);
  const formIsValid = useMemo(() => isFormValid(currentFormErrors), [currentFormErrors]);
  const canSubmit = canEdit && hasUnsavedChanges && formIsValid && !isSubmitting && !isManagersLoading && !managerLoadError;
  const showPrimaryAction = canEdit && hasUnsavedChanges && formIsValid && !isManagersLoading && !managerLoadError;
  const primaryActionLabel = mode === "create" ? "Create Employee" : "Save Changes";
  const picturePreviewSrc = pictureAttachment?.previewSrc || asString(form.pictureUrl) || originalPictureUrlRef.current;
  const hasPicturePreview = Boolean(picturePreviewSrc) && !picturePreviewError;

  useEffect(() => {
    return () => {
      if (pictureDraftObjectUrl) {
        URL.revokeObjectURL(pictureDraftObjectUrl);
      }
    };
  }, [pictureDraftObjectUrl]);

  useEffect(() => {
    if (!open) {
      originalPictureUrlRef.current = "";
      setForm(normalizeEmployeeForm(null));
      setBaseline(normalizeEmployeeForm(null));
      setIsSubmitting(false);
      setSubmitError(null);
      setIsDiscardDialogOpen(false);
      setIsPicturePreviewOpen(false);
      setPicturePreviewError(false);
      setIsPictureUploading(false);
      setPictureUploadError(null);
      clearPictureDraftAttachment();
      setIsManagersLoading(false);
      setManagerLoadError(null);
      if (pictureFileInputRef.current) {
        pictureFileInputRef.current.value = "";
      }
      setFieldErrors(createEmptyFieldErrors());
      return;
    }

    const nextForm = normalizeEmployeeForm(employee);
    originalPictureUrlRef.current = nextForm.pictureUrl;
    setForm(nextForm);
    setBaseline(nextForm);
    setSubmitError(null);
    setIsSubmitting(false);
    setIsDiscardDialogOpen(false);
    setIsPicturePreviewOpen(false);
    setPicturePreviewError(false);
    setIsPictureUploading(false);
    setPictureUploadError(null);
    clearPictureDraftAttachment();
    setIsManagersLoading(mode === "edit" && Boolean(employee?.id));
    setManagerLoadError(null);
    if (pictureFileInputRef.current) {
      pictureFileInputRef.current.value = "";
    }
    setFieldErrors(createEmptyFieldErrors());
  }, [employee, mode, open]);

  useEffect(() => {
    let cancelled = false;
    if (!open || mode !== "edit" || !employee?.id) {
      return () => {
        cancelled = true;
      };
    }

    const cacheSnapshot = readLeaveSphereEmployeeManagementManagerCacheSnapshot({
      ...managerCacheContext,
    });
    const cachedManagerIds = cacheSnapshot?.data
      ? normalizeManagerIds(
          cacheSnapshot.data
            .filter((item) => item.employeeId === employee.id)
            .map((item) => item.managerId)
            .filter((managerId) => managerId && managerId !== employee.id),
        )
      : [];
    if (cacheSnapshot) {
      setForm((current) => ({ ...current, managerIds: cachedManagerIds }));
      setBaseline((current) => ({ ...current, managerIds: cachedManagerIds }));
    }
    setManagerLoadError(null);
    if (cacheSnapshot && !cacheSnapshot.isExpired) {
      setIsManagersLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setIsManagersLoading(true);

    void (async () => {
      try {
        const mappings = await loadLeaveSphereEmployeeManagementManagers({
          requestJson,
        });
        if (cancelled) {
          return;
        }
        const nextManagerLinks = mappings
          .map((item) => ({
            employeeId: asString(item.employeeId),
            managerId: asString(item.managerId),
          }))
          .filter((item) => item.employeeId && item.managerId) as LeaveSphereEmployeeManagementManagerCacheItem[];
        const nextManagerIds = normalizeManagerIds(
          nextManagerLinks
            .filter((item) => item.employeeId === employee.id)
            .map((item) => item.managerId)
            .filter((managerId) => managerId && managerId !== employee.id),
        );
        setForm((current) => ({ ...current, managerIds: nextManagerIds }));
        setBaseline((current) => ({ ...current, managerIds: nextManagerIds }));
        syncLeaveSphereEmployeeManagementManagerCache(
          {
            ...managerCacheContext,
          },
          nextManagerLinks,
          { source: "network", fetchedAt: Date.now() },
        );
      } catch (error) {
        if (cancelled) {
          return;
        }
        setManagerLoadError(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not load managers.");
      } finally {
        if (!cancelled) {
          setIsManagersLoading(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [employee?.id, managerCacheContext, mode, open, requestJson]);

  useEffect(() => {
    setPicturePreviewError(false);
    if (!picturePreviewSrc) {
      setIsPicturePreviewOpen(false);
    }
  }, [picturePreviewSrc]);

  function updateForm<K extends keyof LeaveSphereEmployeeManagementFormState>(
    field: K,
    value: LeaveSphereEmployeeManagementFormState[K],
  ) {
    const next = {
      ...form,
      [field]: value,
    };
    setForm(next);
    if (submitError) {
      setSubmitError(null);
    }
  }

  function markFieldBlurred(
    field: keyof ReturnType<typeof validateForm>,
    nextForm: LeaveSphereEmployeeManagementFormState = form,
  ) {
    setFieldErrors((current) => ({
      ...current,
      [field]: validateEmployeeField(field, nextForm),
    }));
  }

  function openPicturePicker() {
    if (isSubmitting || isPictureUploading || !canEdit) {
      return;
    }
    setPictureUploadError(null);
    if (pictureFileInputRef.current) {
      pictureFileInputRef.current.value = "";
      pictureFileInputRef.current.click();
    }
  }

  function clearPictureDraftAttachment() {
    if (pictureDraftObjectUrl) {
      URL.revokeObjectURL(pictureDraftObjectUrl);
    }
    setPictureDraftFile(null);
    setPictureDraftObjectUrl(null);
  }

  async function processPictureFile(file: File) {
    if (isSubmitting || isPictureUploading || !canEdit) {
      return;
    }

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
      setPictureUploadError("Only PNG, JPG, JPEG, and WEBP files are allowed.");
      return;
    }

    setPictureUploadError(null);
    setPicturePreviewError(false);
    clearPictureDraftAttachment();
    const nextObjectUrl = URL.createObjectURL(file);
    setPictureDraftFile(file);
    setPictureDraftObjectUrl(nextObjectUrl);
  }

  async function handlePictureFileChange(fileList: FileList | null) {
    const file = fileList?.[0] ?? null;
    if (pictureFileInputRef.current) {
      pictureFileInputRef.current.value = "";
    }
    if (!file) {
      return;
    }
    await processPictureFile(file);
  }

  async function handlePicturePaste(event: ClipboardEvent<HTMLDivElement>) {
    if (isSubmitting || isPictureUploading || !canEdit) {
      return;
    }
    const clipboardFiles = Array.from(event.clipboardData?.items ?? [])
      .filter((item) => item.kind === "file")
      .map((item) => item.getAsFile())
      .filter((file): file is File => file instanceof File);
    if (!clipboardFiles.length) {
      return;
    }
    event.preventDefault();
    await processPictureFile(clipboardFiles[0]);
  }

  function restoreOriginalPictureAttachment() {
    const originalPictureUrl = originalPictureUrlRef.current;
    clearPictureDraftAttachment();
    setForm((current) => ({
      ...current,
      pictureUrl: originalPictureUrl,
    }));
    setPicturePreviewError(false);
    setPictureUploadError(null);
  }

  function handleDialogOpenChange(nextOpen: boolean) {
    if (nextOpen) {
      onOpenChange(true);
      return;
    }
    if (isSubmitting || isPictureUploading) {
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

    setSubmitError(null);
    setIsSubmitting(true);
    try {
      let nextForm = form;
      if (pictureDraftFile) {
        setIsPictureUploading(true);
        try {
          const pictureUrl = await onUploadPicture(pictureDraftFile);
          nextForm = {
            ...nextForm,
            pictureUrl,
          };
          setForm(nextForm);
        } catch (error) {
          setPictureUploadError(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not upload picture.");
          return;
        } finally {
          setIsPictureUploading(false);
        }
      }
      await onSubmit({
        mode,
        id: employee?.id ?? null,
        form: normalizeLeaveSphereEmployeeManagementForm(nextForm),
      });
      onOpenChange(false);
    } catch (error) {
      setSubmitError(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not save employee.");
    } finally {
      setIsSubmitting(false);
    }
  }

  const errors = fieldErrors;

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          className="max-h-[92vh] w-[min(94vw,940px)] max-w-none overflow-hidden rounded-[1.6rem] bg-white px-7 py-6"
          onInteractOutside={(event) => {
            const target = event.target;
            if (target instanceof Element && target.closest("[data-app-dropdown-root='true'], [data-app-dropdown-menu='true']")) {
              event.preventDefault();
              return;
            }
            if (isSubmitting || isPictureUploading || hasUnsavedChanges) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={isSubmitting} busyMessage={mode === "create" ? "Creating employee..." : "Saving employee..."} className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close employee modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>{mode === "create" ? "Add Employee" : "Edit Employee"}</DialogTitle>
                <DialogDescription>
                  {mode === "create"
                    ? "Add a LeaveSphere employee record and make it available for PTO workflows."
                    : "Update the employee profile and account status."}
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>

            <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1">
              <div className="grid grid-cols-1 items-start gap-8 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                <Section className="space-y-3">
                  <SectionHeader
                    title="Personal Information"
                    description="Core contact details, profile image, and account status."
                  />
                  <LabeledField
                    label={
                      <>
                        First Name<span className="ml-1 text-rose-600">*</span>
                      </>
                    }
                  >
                    <Input
                      value={form.firstName}
                      onChange={(event) => updateForm("firstName", event.target.value)}
                      onBlur={() => markFieldBlurred("firstName")}
                      disabled={isSubmitting || !canEdit}
                      maxLength={255}
                      autoComplete="off"
                    />
                  </LabeledField>
                  {errors.firstName ? <p className="text-sm text-rose-600">{errors.firstName}</p> : null}

                  <LabeledField
                    label={
                      <>
                        Last Name<span className="ml-1 text-rose-600">*</span>
                      </>
                    }
                  >
                    <Input
                      value={form.lastName}
                      onChange={(event) => updateForm("lastName", event.target.value)}
                      onBlur={() => markFieldBlurred("lastName")}
                      disabled={isSubmitting || !canEdit}
                      maxLength={255}
                      autoComplete="off"
                    />
                  </LabeledField>
                  {errors.lastName ? <p className="text-sm text-rose-600">{errors.lastName}</p> : null}

                  <LabeledField
                    label={
                      <>
                        Email<span className="ml-1 text-rose-600">*</span>
                      </>
                    }
                  >
                    <Input
                      value={form.email}
                      onChange={(event) => updateForm("email", event.target.value)}
                      onBlur={() => markFieldBlurred("email")}
                      disabled={isSubmitting || !canEdit}
                      maxLength={255}
                      autoComplete="off"
                      spellCheck={false}
                      inputMode="email"
                    />
                  </LabeledField>
                  {errors.email ? <p className="text-sm text-rose-600">{errors.email}</p> : null}

                  <LabeledField label="Phone">
                    <Input
                      value={form.phone}
                      onChange={(event) => updateForm("phone", normalizeUsPhoneOnInput(event.target.value, false))}
                      onBlur={(event) => {
                        const normalized = normalizeUsPhoneDisplay(event.target.value, false);
                        updateForm("phone", normalized);
                        markFieldBlurred("phone", {
                          ...form,
                          phone: normalized,
                        });
                      }}
                      disabled={isSubmitting || !canEdit}
                      maxLength={20}
                      autoComplete="off"
                      inputMode="tel"
                    />
                  </LabeledField>
                  {errors.phone ? <p className="text-sm text-rose-600">{errors.phone}</p> : null}

                  <LabeledField label="Birthday">
                    <DateInputField
                      id="employee-dob"
                      value={form.dob}
                      onChange={(value) => updateForm("dob", value)}
                      disabled={isSubmitting || !canEdit}
                      label="birthday"
                    />
                  </LabeledField>

                  <LabeledField label="Active">
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
                  </LabeledField>

                  <LabeledField label="Profile Picture" alignStart>
                    <div className="space-y-3" onPaste={(event) => { void handlePicturePaste(event); }}>
                      <input
                        ref={pictureFileInputRef}
                        type="file"
                        accept=".png,.jpg,.jpeg,.webp,image/png,image/jpeg,image/webp"
                        className="hidden"
                        onChange={(event) => {
                          void handlePictureFileChange(event.target.files);
                        }}
                        disabled={isSubmitting || !canEdit || isPictureUploading}
                      />
                      <button
                        type="button"
                        className={cn(
                          "flex w-full items-center justify-center gap-2 rounded-lg border border-dashed px-3 py-4 text-sm transition",
                          isSubmitting || !canEdit || isPictureUploading
                            ? "cursor-not-allowed border-blue-100 bg-blue-50/20 text-slate-400"
                            : "cursor-pointer border-blue-200 bg-blue-50/30 text-slate-700 hover:border-blue-300 hover:bg-blue-50/50",
                        )}
                        onClick={openPicturePicker}
                        disabled={isSubmitting || !canEdit || isPictureUploading}
                      >
                        <UploadCloud className="size-4 text-blue-600" />
                        Select image or paste screenshot
                      </button>

                      {pictureAttachment ? (
                        <div className="flex items-center justify-between gap-3 rounded-md border border-slate-200 bg-white px-3 py-2 text-xs">
                          <div className="flex min-w-0 items-center gap-3">
                            {hasPicturePreview ? (
                              <button
                                type="button"
                                className="flex h-12 w-12 shrink-0 cursor-zoom-in items-center justify-center overflow-hidden rounded-md border border-slate-200 bg-slate-50"
                                onClick={() => setIsPicturePreviewOpen(true)}
                                disabled={isSubmitting || !canEdit}
                                aria-label="Preview profile picture"
                              >
                                <img
                                  key={picturePreviewSrc || "picture-preview-empty"}
                                  src={picturePreviewSrc}
                                  alt={pictureAttachment.name}
                                  className="h-full w-full object-cover"
                                  loading="lazy"
                                  onError={() => setPicturePreviewError(true)}
                                />
                              </button>
                            ) : (
                              <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-md border border-slate-200 bg-slate-50 text-slate-500">
                                <UserRound className="size-4" aria-hidden="true" />
                              </div>
                            )}
                            <div className="min-w-0">
                              <p className="truncate font-medium text-slate-800">{pictureAttachment.name}</p>
                              <p className="text-slate-500">{pictureAttachment.meta}</p>
                            </div>
                          </div>
                          <button
                            type="button"
                            className="inline-flex size-6 items-center justify-center rounded-md text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-70"
                            onClick={() => {
                              if (pictureDraftFile) {
                                restoreOriginalPictureAttachment();
                              } else {
                                updateForm("pictureUrl", "");
                                setPictureDraftFile(null);
                                setPictureDraftObjectUrl(null);
                                setPicturePreviewError(false);
                                setPictureUploadError(null);
                              }
                              if (pictureFileInputRef.current) {
                                pictureFileInputRef.current.value = "";
                              }
                            }}
                            disabled={isSubmitting || !canEdit || isPictureUploading}
                            aria-label={pictureDraftFile ? "Revert profile picture" : "Remove profile picture"}
                          >
                            <Trash2 className="size-4" aria-hidden="true" />
                          </button>
                        </div>
                      ) : null}

                      <p className="text-xs text-slate-500">PNG, JPG, JPEG, or WEBP. Up to 10 MB.</p>
                      {isPictureUploading ? <p className="text-xs text-slate-500">Uploading picture...</p> : null}
                      {pictureUploadError ? <p className="text-sm text-rose-600">{pictureUploadError}</p> : null}
                      {errors.pictureUrl ? <p className="text-sm text-rose-600">{errors.pictureUrl}</p> : null}
                    </div>
                  </LabeledField>
                </Section>

                <div className="space-y-8">
                  <Section className="space-y-3">
                    <SectionHeader
                      title="Company Information"
                      description="Employment details used throughout LeaveSphere."
                    />
                    <LabeledField label="Title">
                      <Input
                        value={form.title}
                        onChange={(event) => updateForm("title", event.target.value)}
                        disabled={isSubmitting || !canEdit}
                        maxLength={255}
                        autoComplete="off"
                      />
                    </LabeledField>
                    <LabeledField label="Start Date">
                      <DateInputField
                        id="employee-start-date"
                        value={form.startDate}
                        onChange={(value) => updateForm("startDate", value)}
                        disabled={isSubmitting || !canEdit}
                        label="start date"
                      />
                    </LabeledField>

                    <LabeledField label="Region">
                      <AppDropdown
                        value={form.region || "US"}
                        options={EMPLOYEE_REGION_OPTIONS}
                        onValueChange={(value) => updateForm("region", value as LeaveSphereEmployeeManagementFormState["region"])}
                        searchable={false}
                        allowCustomValue={false}
                        ariaLabel="Employee region"
                        placeholder="Select region"
                        disabled={isSubmitting || !canEdit}
                      />
                    </LabeledField>

                    <LabeledField label="isAE">
                      <button
                        type="button"
                        role="switch"
                        aria-checked={form.isAE}
                        aria-label="AE status"
                        onClick={() => updateForm("isAE", !form.isAE)}
                        disabled={isSubmitting || !canEdit}
                        aria-disabled={isSubmitting || !canEdit}
                        className={cn(
                          "inline-flex h-10 w-fit items-center gap-3 px-1 py-2 text-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                          form.isAE ? "text-blue-700" : "text-slate-600",
                        )}
                      >
                        <span className={cn("relative inline-flex h-5 w-9 items-center rounded-full transition", form.isAE ? "bg-blue-500" : "bg-slate-300")}>
                          <span className={cn("inline-block h-4 w-4 rounded-full bg-white shadow transition", form.isAE ? "translate-x-[18px]" : "translate-x-[2px]")} />
                        </span>
                      </button>
                    </LabeledField>
                  </Section>

                  <Section className="space-y-3">
                    <SectionHeader
                      title="Relationship"
                      description="Pick one or more managers for this employee."
                    />
                    <LabeledField label="Managers" alignStart>
                      <div className="space-y-2">
                        <AppDropdown
                          value=""
                          values={form.managerIds}
                          onValueChange={() => {}}
                          onValuesChange={(values) => updateForm("managerIds", normalizeManagerIds(values))}
                          options={managerOptions}
                          multiple
                          searchable
                          allowCustomValue={false}
                          ariaLabel="Employee managers"
                          placeholder={managerOptions.length ? "Select managers" : "No other employees available"}
                          disabled={
                            isSubmitting
                            || !canEdit
                            || isManagersLoading
                            || Boolean(managerLoadError)
                            || !form.active
                            || managerOptions.length === 0
                          }
                        />
                        {!form.active ? (
                          <p className="text-xs leading-5 text-slate-500">
                            Activate this employee to edit manager assignments.
                          </p>
                        ) : null}
                        {isManagersLoading ? (
                          <p className="text-xs leading-5 text-slate-500">Loading manager assignments...</p>
                        ) : null}
                        {managerLoadError ? <p className="text-sm text-rose-600">{managerLoadError}</p> : null}
                      </div>
                    </LabeledField>
                  </Section>
                </div>
              </div>
              {submitError ? <p className="mt-4 text-sm text-rose-600">{submitError}</p> : null}
            </div>

            {mode === "edit" && detailCacheStatusText && onRefreshDetailCache ? (
              <ModalCacheFooter
                text={detailCacheStatusText}
                onRefresh={() => {
                  if (!detailCacheRefreshDisabled && !detailCacheRefreshing && !hasUnsavedChanges && !isSubmitting) {
                    onRefreshDetailCache();
                  }
                }}
                disabled={detailCacheRefreshDisabled || detailCacheRefreshing || hasUnsavedChanges || isSubmitting}
                refreshing={detailCacheRefreshing}
                refreshLabel="Refresh employee workspace"
                tooltipText={
                  hasUnsavedChanges
                    ? "Save or discard your edits before refreshing employee data."
                    : "Click to refresh this employee data"
                }
                actions={
                  <>
                    {canEdit && hasUnsavedChanges ? (
                      <Button
                        variant="outline"
                        onClick={() => {
                          const next = baseline;
                          setForm(next);
                          restoreOriginalPictureAttachment();
                          setFieldErrors(createEmptyFieldErrors());
                          setSubmitError(null);
                        }}
                        disabled={isSubmitting}
                      >
                        Revert
                      </Button>
                    ) : null}
                    {showPrimaryAction ? (
                      <Button onClick={handleSubmit} disabled={!canSubmit}>
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
                }
              />
            ) : (
              <ModalFooter className="mt-4 flex-col gap-2 border-t border-slate-100 pt-3 sm:flex-row sm:items-center sm:justify-between">
                <span className="text-xs text-slate-500" />
                <div className="flex items-center gap-2">
                  {canEdit && hasUnsavedChanges ? (
                    <Button
                      variant="outline"
                      onClick={() => {
                        const next = baseline;
                        setForm(next);
                        restoreOriginalPictureAttachment();
                        setFieldErrors(createEmptyFieldErrors());
                        setSubmitError(null);
                      }}
                      disabled={isSubmitting}
                    >
                      Revert
                    </Button>
                  ) : null}
                  {showPrimaryAction ? (
                    <Button onClick={handleSubmit} disabled={!canSubmit}>
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

      <Dialog open={isPicturePreviewOpen} onOpenChange={setIsPicturePreviewOpen}>
        <DialogContent
          className="w-[calc(100vw-2.5rem)] max-w-3xl border-none bg-transparent p-0 shadow-none"
          aria-describedby={undefined}
        >
          <div className="relative flex w-full items-center justify-center overflow-hidden rounded-2xl bg-white px-6 pb-6 pt-14 shadow-2xl sm:px-8 sm:pb-8 sm:pt-16">
            <DialogClose asChild aria-label="Close picture preview">
              <ModalCloseButton
                icon={<X className="size-4" />}
                className="absolute right-0 top-0 z-10 rounded-md bg-slate-900/85 p-1.5 text-white transition-colors hover:bg-slate-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              />
            </DialogClose>
            {hasPicturePreview ? (
              <img
                key={picturePreviewSrc || "picture-preview-empty-large"}
                src={picturePreviewSrc}
                alt={`${asString(form.firstName) || "Employee"} picture enlarged`}
                className="mx-auto block h-auto max-h-[70vh] w-auto max-w-full object-contain"
                loading="lazy"
                onError={() => setPicturePreviewError(true)}
              />
            ) : (
              <div className="flex min-h-[18rem] w-full items-center justify-center text-center text-slate-500">
                <div className="flex flex-col items-center gap-2">
                  <UserRound className="size-8" />
                  <p className="text-sm font-medium">Picture preview unavailable</p>
                </div>
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}

function EmployeeCard({
  employee,
  disabled,
  canActivate,
  canDeactivate,
  canEdit,
  onEdit,
  onToggleActive,
}: {
  employee: LeaveSphereEmployeeManagementEmployee;
  disabled?: boolean;
  canActivate: boolean;
  canDeactivate: boolean;
  canEdit: boolean;
  onEdit: (employee: LeaveSphereEmployeeManagementEmployee) => void;
  onToggleActive: (employee: LeaveSphereEmployeeManagementEmployee, nextActive: boolean) => void;
}) {
  const [imageError, setImageError] = useState(false);
  const employeeName = buildEmployeeFullName(employee);
  const employeeTitle = asString(employee.title) || "Title not set";
  const employeeInitials = buildEmployeeInitials(employee);
  const showPicture = Boolean(employee.pictureUrl) && !imageError;

  useEffect(() => {
    setImageError(false);
  }, [employee.pictureUrl]);

  return (
    <article
      role="button"
      tabIndex={disabled ? -1 : 0}
      onClick={() => {
        if (!disabled && canEdit) {
          onEdit(employee);
        }
      }}
      onKeyDown={(event) => {
        if (disabled || !canEdit) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onEdit(employee);
        }
      }}
      className={cn(
        "space-y-4 rounded-[1.35rem] border p-5 shadow-[0_18px_30px_-24px_rgba(37,99,235,0.42)] transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
        disabled ? "cursor-default" : "cursor-pointer",
        employee.active
          ? "border-blue-100/90 bg-white hover:-translate-y-0.5 hover:border-blue-200 hover:shadow-[0_20px_34px_-24px_rgba(37,99,235,0.5)]"
          : "border-slate-200 bg-slate-50/90 hover:-translate-y-0.5 hover:border-slate-300 hover:shadow-[0_18px_28px_-26px_rgba(15,23,42,0.22)]",
      )}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="flex min-w-0 gap-4">
          <span className="inline-flex size-16 shrink-0 items-center justify-center overflow-hidden rounded-full border border-blue-100 bg-gradient-to-br from-blue-50 to-white text-blue-700 shadow-[0_10px_20px_-16px_rgba(37,99,235,0.55)]">
            {showPicture ? (
              <img
                src={employee.pictureUrl || undefined}
                alt={employeeName}
                className="size-full object-cover"
                loading="lazy"
                onError={() => setImageError(true)}
              />
            ) : employeeInitials ? (
              <span className="text-sm font-semibold tracking-[0.08em]">{employeeInitials}</span>
            ) : (
              <UserRound className="size-7" />
            )}
          </span>

          <div className="min-w-0 pt-0.5">
            <p className="truncate text-lg font-semibold tracking-[-0.02em] text-slate-900">{employeeName}</p>
            <p className="truncate text-sm text-slate-500">{employeeTitle}</p>

            <div className="mt-3 flex flex-wrap items-center gap-2">
              <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium", toEmployeeCardStatusClass(employee.active))}>
                {buildEmployeeStatusLabel(employee)}
              </span>
              <span className="inline-flex items-center rounded-full border border-blue-100 bg-blue-50 px-2.5 py-1 text-xs font-medium text-blue-700">
                {buildEmployeeRegionLabel(employee.region)}
              </span>
              {employee.isAE ? (
                <span className="inline-flex items-center rounded-full border border-violet-200 bg-violet-50 px-2.5 py-1 text-xs font-medium text-violet-700">
                  AE
                </span>
              ) : null}
            </div>
          </div>
        </div>

        <div className="flex items-center gap-1.5">
          {employee.active && canDeactivate ? (
            <ActionIconButton
              icon={<UserX />}
              tooltip="Deactivate employee"
              onClick={(event) => {
                event.stopPropagation();
                onToggleActive(employee, false);
              }}
              disabled={disabled}
            />
          ) : null}
          {!employee.active && canActivate ? (
            <ActionIconButton
              icon={<UserCheck />}
              tooltip="Activate employee"
              onClick={(event) => {
                event.stopPropagation();
                onToggleActive(employee, true);
              }}
              disabled={disabled}
            />
          ) : null}
        </div>
      </div>
    </article>
  );
}

function buildEmployeeInitials(employee: LeaveSphereEmployeeManagementEmployee): string {
  return buildEmployeeInitialsFromName(buildEmployeeFullName(employee));
}

function buildEmployeeInitialsFromName(name: string): string {
  const parts = asString(name)
    .split(/\s+/)
    .filter(Boolean);

  if (parts.length === 0) {
    return "";
  }

  if (parts.length === 1) {
    return parts[0].slice(0, 2).toUpperCase();
  }

  return `${parts[0][0]}${parts[parts.length - 1][0]}`.toUpperCase();
}

function EmptyEmployeesPanel({
  hasSearched,
  hasFilters,
  hasEmployees,
  hasMatches,
  onClearFilters,
}: {
  hasSearched: boolean;
  hasFilters: boolean;
  hasEmployees: boolean;
  hasMatches: boolean;
  onClearFilters: () => void;
}) {
  const message = getEmptyMessage(hasSearched, hasFilters, hasEmployees, hasMatches);

  return (
    <div className="rounded-[1.35rem] border border-dashed border-blue-200/90 bg-gradient-to-b from-blue-50/45 to-white px-6 py-11 text-center">
      <div className="mx-auto flex max-w-xl flex-col items-center gap-3">
        <div className="inline-flex size-14 items-center justify-center rounded-full border border-blue-100 bg-blue-50 text-blue-700">
          {hasEmployees ? <Search className="size-7" /> : <Users className="size-7" />}
        </div>
        <div className="space-y-1">
          <p className="text-base font-semibold text-slate-900">{message.title}</p>
          <p className="text-sm leading-6 text-slate-600">{message.description}</p>
        </div>
        {hasFilters ? (
          <Button variant="outline" onClick={onClearFilters}>
            Clear filters
          </Button>
        ) : null}
      </div>
    </div>
  );
}

function buildWorkspaceFromPatch(
  currentWorkspace: LeaveSphereEmployeeManagementWorkspace | null,
  nextEmployee: LeaveSphereEmployeeManagementEmployee,
): LeaveSphereEmployeeManagementWorkspace {
  if (!currentWorkspace) {
    const activeEmployees = nextEmployee.active ? 1 : 0;
    return {
      pageCode: LEAVESPHERE_EMPLOYEE_MANAGEMENT_PAGE_CODE,
      pageTitle: "Employee Management",
      summary: {
        totalEmployees: 1,
        activeEmployees,
        inactiveEmployees: 1 - activeEmployees,
      },
      capabilities: {
        canCreate: true,
        canUpdate: true,
        canActivate: true,
        canDeactivate: true,
        canArchive: false,
      },
      employees: [nextEmployee],
    };
  }

  const withoutCurrent = currentWorkspace.employees.filter((employee) => employee.id !== nextEmployee.id);
  const nextEmployees = sortLeaveSphereEmployeeManagementEmployees([...withoutCurrent, nextEmployee]);
  const activeEmployees = nextEmployees.filter((employee) => employee.active).length;
  return {
    ...currentWorkspace,
    employees: nextEmployees,
    summary: {
      totalEmployees: nextEmployees.length,
      activeEmployees,
      inactiveEmployees: nextEmployees.length - activeEmployees,
    },
  };
}

export default function EmployeeManagementPage() {
  const { requestJson } = useApiRequest();
  const { isOnline } = useOnlineStatus();
  const auth = useAuth();
  const pageStateUserKey = asString(auth.user?.id || auth.user?.email);
  const tenantSlug = asString(auth.tenantSlug);
  const cacheContext = useMemo<LeaveSphereEmployeeManagementWorkspaceCacheContext>(
    () => ({
      tenantSlug: auth.tenantSlug || "",
      userKey: auth.user?.id || auth.user?.email || "",
    }),
    [auth.tenantSlug, auth.user?.email, auth.user?.id],
  );
  const cacheKey = useMemo(() => buildLeaveSphereEmployeeManagementWorkspaceCacheKey(cacheContext), [cacheContext]);
  const managerCacheBaseContext = useMemo<LeaveSphereEmployeeManagementManagerCacheContext>(
    () => ({
      ...cacheContext,
    }),
    [cacheContext],
  );
  const canManage = useMemo(() => {
    if (!shouldProtectFrontendAuth()) {
      return true;
    }
    return hasAppAdminAccess(auth.accessProfile, "leavesphere");
  }, [auth.accessProfile]);
  const canRestorePageState = auth.status === "authenticated" && Boolean(tenantSlug) && Boolean(pageStateUserKey);
  const pageStateScope = useMemo<ScopedPageState | null>(() => {
    if (!canRestorePageState) {
      return null;
    }
    return {
      userKey: pageStateUserKey,
      tenantSlug,
      appCode: LEAVESPHERE_APP_CODE,
      pageCode: LEAVESPHERE_EMPLOYEE_MANAGEMENT_PAGE_CODE,
    };
  }, [canRestorePageState, pageStateUserKey, tenantSlug]);
  const pageStateStorageKey = useMemo(() => {
    if (!pageStateScope) {
      return null;
    }
    return buildScopedPageStateStorageKey(pageStateScope);
  }, [pageStateScope]);

  const [workspace, setWorkspace] = useState<LeaveSphereEmployeeManagementWorkspace | null>(null);
  const workspaceRef = useRef<LeaveSphereEmployeeManagementWorkspace | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const requestTokenRef = useRef(0);

  const [searchDraft, setSearchDraft] = useState<EmployeeSearchCriteria>({ ...DEFAULT_SEARCH_CRITERIA });
  const [searchCriteria, setSearchCriteria] = useState<EmployeeSearchCriteria>({ ...DEFAULT_SEARCH_CRITERIA });
  const [hasSearched, setHasSearched] = useState(false);
  const [searchEmailTouched, setSearchEmailTouched] = useState(false);
  const [searchEmailError, setSearchEmailError] = useState<string | null>(null);
  const [employeeGroupOpenState, setEmployeeGroupOpenState] = useState<EmployeeGroupOpenState>({});
  const [hasHydratedPageState, setHasHydratedPageState] = useState(false);

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState<EmployeeMode>("create");
  const [modalEmployee, setModalEmployee] = useState<LeaveSphereEmployeeManagementEmployee | null>(null);
  const [pendingAction, setPendingAction] = useState<{ employee: LeaveSphereEmployeeManagementEmployee; nextActive: boolean } | null>(null);
  const [isMutationInFlight, setIsMutationInFlight] = useState(false);
  const hydratedPageStateScopeRef = useRef<string | null>(null);

  useEffect(() => {
    workspaceRef.current = workspace;
  }, [workspace]);

  function commitWorkspace(nextWorkspace: LeaveSphereEmployeeManagementWorkspace | null, source: "cache" | "network") {
    workspaceRef.current = nextWorkspace;
    setWorkspace(nextWorkspace);
    if (!nextWorkspace) {
      return;
    }
    setHasSearched(true);
    const fetchedAt = Date.now();
    setCacheStatus({ source, fetchedAt });
    syncLeaveSphereEmployeeManagementWorkspaceCache(cacheContext, nextWorkspace, { source, fetchedAt });
  }

  async function refreshWorkspace(policy: CachePolicy): Promise<void> {
    const requestToken = ++requestTokenRef.current;
    const snapshot = readLeaveSphereEmployeeManagementWorkspaceCacheSnapshot(cacheContext);
    const cachedWorkspace = snapshot?.data ? normalizeLeaveSphereEmployeeManagementWorkspace(snapshot.data) : null;
    const shouldFetch = shouldFetchNetwork(policy, snapshot);

    if (cachedWorkspace) {
      commitWorkspace(cachedWorkspace, "cache");
      setErrorMessage(null);
      setRefreshMessage(null);
    }

    if (!shouldFetch) {
      setIsLoading(false);
      setIsRefreshing(false);
      return;
    }

    if (!isOnline) {
      if (cachedWorkspace) {
        setRefreshMessage("You're offline. Showing cached employees.");
      } else {
        setErrorMessage("You're offline. Connect to the internet to load employees.");
      }
      setIsLoading(false);
      setIsRefreshing(false);
      return;
    }

    if (cachedWorkspace) {
      setIsRefreshing(true);
    } else {
      setIsLoading(true);
      setRefreshMessage(null);
      setErrorMessage(null);
    }

    try {
      const nextWorkspace = await loadLeaveSphereEmployeeManagementWorkspace({
        requestJson,
        freshData: policy === "network-only",
      });
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      commitWorkspace(nextWorkspace, "network");
      setRefreshMessage(null);
      setErrorMessage(null);
    } catch (error) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      if (workspaceRef.current || cachedWorkspace) {
        setRefreshMessage("Showing cached employees. Could not refresh.");
        setErrorMessage(null);
      } else {
        setErrorMessage(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not load employees.");
      }
    } finally {
      if (requestToken === requestTokenRef.current) {
        setIsLoading(false);
        setIsRefreshing(false);
      }
    }
  }

  useEffect(() => {
    setWorkspace(null);
    setCacheStatus(null);
    setRefreshMessage(null);
    setErrorMessage(null);
    setIsLoading(false);
    setIsRefreshing(false);
    setSearchDraft({ ...DEFAULT_SEARCH_CRITERIA });
    setSearchCriteria({ ...DEFAULT_SEARCH_CRITERIA });
    setHasSearched(false);
    setSearchEmailTouched(false);
    setSearchEmailError(null);
    setEmployeeGroupOpenState({});
    hydratedPageStateScopeRef.current = null;
    setHasHydratedPageState(false);
  }, [cacheKey]);

  useEffect(() => {
    if (!canRestorePageState || !pageStateScope || !pageStateStorageKey) {
      hydratedPageStateScopeRef.current = null;
      setHasHydratedPageState(false);
      return;
    }
    if (hydratedPageStateScopeRef.current === pageStateStorageKey) {
      setHasHydratedPageState(true);
      return;
    }

    hydratedPageStateScopeRef.current = pageStateStorageKey;
    const persisted = readScopedPageState<PersistedEmployeeManagementPageState>(pageStateScope, isPersistedEmployeeManagementPageState);
    if (persisted) {
      setSearchDraft(normalizeEmployeeSearchCriteria(persisted.searchDraft, DEFAULT_SEARCH_CRITERIA));
      setSearchCriteria(normalizeEmployeeSearchCriteria(persisted.searchCriteria, DEFAULT_SEARCH_CRITERIA));
      setHasSearched(persisted.hasSearched);
      setEmployeeGroupOpenState(persisted.groupOpenState);
    }

    setHasHydratedPageState(true);
  }, [canRestorePageState, pageStateScope, pageStateStorageKey]);

  useEffect(() => {
    if (!hasHydratedPageState) {
      return;
    }
    void refreshWorkspace("cache-first");
  }, [hasHydratedPageState]);

  useEffect(() => {
    if (
      !canRestorePageState ||
      !hasHydratedPageState ||
      !pageStateScope ||
      !pageStateStorageKey ||
      hydratedPageStateScopeRef.current !== pageStateStorageKey
    ) {
      return;
    }
    writeScopedPageState<PersistedEmployeeManagementPageState>(pageStateScope, {
      searchDraft,
      searchCriteria,
      hasSearched,
      groupOpenState: employeeGroupOpenState,
    });
  }, [
    canRestorePageState,
    employeeGroupOpenState,
    hasHydratedPageState,
    hasSearched,
    pageStateScope,
    pageStateStorageKey,
    searchCriteria,
    searchDraft,
  ]);

  const allEmployees = workspace?.employees ?? [];
  const employeeOptions = useMemo(() => buildEmployeeOptions(allEmployees), [allEmployees]);
  const filteredEmployees = useMemo(() => {
    if (!hasSearched || !workspace) {
      return [];
    }
    const normalizedNameOrTitle = searchCriteria.nameOrTitle.trim().toLowerCase();
    const normalizedEmail = searchCriteria.email.trim().toLowerCase();
    return sortLeaveSphereEmployeeManagementEmployees(
      allEmployees.filter((employee) => {
        if (searchCriteria.statusFilter === "active" && !employee.active) {
          return false;
        }
        if (searchCriteria.statusFilter === "inactive" && employee.active) {
          return false;
        }
        if (searchCriteria.regionFilter && employee.region !== searchCriteria.regionFilter) {
          return false;
        }
        if (normalizedNameOrTitle) {
          const nameOrTitleText = buildEmployeeNameOrTitleSearchText(employee);
          if (!nameOrTitleText.includes(normalizedNameOrTitle)) {
            return false;
          }
        }
        if (normalizedEmail) {
          if (!employee.email.toLowerCase().includes(normalizedEmail)) {
            return false;
          }
        }
        return true;
      }),
    );
  }, [allEmployees, hasSearched, searchCriteria.email, searchCriteria.nameOrTitle, searchCriteria.regionFilter, searchCriteria.statusFilter, workspace]);

  const hasFilters = hasEmployeeSearchCriteria(searchCriteria);
  const hasDraftFilters = hasEmployeeSearchCriteria(searchDraft);
  const hasEmployees = allEmployees.length > 0;
  const hasMatches = Boolean(workspace && filteredEmployees.length > 0);
  const employeeResultGroups = useMemo(() => {
    const grouped = new Map<string, LeaveSphereEmployeeManagementEmployee[]>();
    for (const employee of filteredEmployees) {
      const key = asString(employee.region) || "Other";
      const current = grouped.get(key) ?? [];
      current.push(employee);
      grouped.set(key, current);
    }

    return Array.from(grouped.entries())
      .map(([key, items]) => ({
        key,
        label: buildEmployeeRegionLabel(key),
        items,
      }))
      .sort((left, right) => {
        const leftOrder = getEmployeeRegionSortOrder(left.key);
        const rightOrder = getEmployeeRegionSortOrder(right.key);
        if (leftOrder !== rightOrder) {
          return leftOrder - rightOrder;
        }
        return left.label.localeCompare(right.label);
      });
  }, [filteredEmployees]);

  useEffect(() => {
    if (!employeeResultGroups.length) {
      return;
    }
    setEmployeeGroupOpenState((current) => {
      let changed = false;
      const next = { ...current };
      for (const group of employeeResultGroups) {
        if (next[group.key] === undefined) {
          next[group.key] = true;
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, [employeeResultGroups]);
  const searchResultText = !hasSearched
    ? "Search by one or more fields. Results load only after you click Search."
    : workspace
      ? filteredEmployees.length === 0
        ? "No employees matched your search."
        : `${filteredEmployees.length} employee${filteredEmployees.length === 1 ? "" : "s"} found.`
      : "Searching employees...";
  const pageMessages: StackMessage[] = [];
  if (refreshMessage) {
    pageMessages.push({
      id: "employee-refresh",
      variant: refreshMessage.toLowerCase().includes("offline") ? "info" : "warning",
      message: refreshMessage,
    });
  }
  if (errorMessage) {
    pageMessages.push({
      id: "employee-error",
      variant: "error",
      message: errorMessage,
    });
  }

  const cacheStatusText = isRefreshing
    ? "Refreshing employees..."
    : !isOnline && cacheStatus
      ? `Offline. Showing cached employees from ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : cacheStatus
        ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
        : "No cached employees yet";

  function resetSearchCriteria() {
    ++requestTokenRef.current;
    setWorkspace(null);
    setCacheStatus(null);
    setRefreshMessage(null);
    setErrorMessage(null);
    setIsLoading(false);
    setIsRefreshing(false);
    setSearchDraft({ ...EMPTY_SEARCH_CRITERIA });
    setSearchCriteria({ ...EMPTY_SEARCH_CRITERIA });
    setHasSearched(false);
    setSearchEmailTouched(false);
    setSearchEmailError(null);
  }

  function handleSearch() {
    if (!hasDraftFilters) {
      return;
    }
    const nextEmailError = validateOptionalEmail(searchDraft.email);
    setSearchEmailTouched(true);
    setSearchEmailError(nextEmailError);
    if (nextEmailError) {
      return;
    }
    const nextSearchCriteria = { ...searchDraft };
    const shouldForceRefresh =
      Boolean(workspaceRef.current) && areEmployeeSearchCriteriaEqual(nextSearchCriteria, searchCriteria);

    setSearchCriteria(nextSearchCriteria);
    setHasSearched(true);

    if (shouldForceRefresh) {
      void refreshWorkspace("network-only");
      return;
    }

    if (!workspaceRef.current) {
      void refreshWorkspace("cache-first");
    }
  }

  function handleSearchEmailBlur() {
    setSearchEmailTouched(true);
    setSearchEmailError(validateOptionalEmail(searchDraft.email));
  }

  function openCreateModal() {
    if (!canManage) {
      return;
    }
    setModalMode("create");
    setModalEmployee(null);
    setIsModalOpen(true);
  }

  function openEditModal(employee: LeaveSphereEmployeeManagementEmployee) {
    setModalMode("edit");
    setModalEmployee(employee);
    setIsModalOpen(true);
  }

  function startToggleEmployeeActive(employee: LeaveSphereEmployeeManagementEmployee, nextActive: boolean) {
    setPendingAction({ employee, nextActive });
  }

  function updateWorkspaceWithEmployee(nextEmployee: LeaveSphereEmployeeManagementEmployee) {
    const currentWorkspace = workspaceRef.current;
    const nextWorkspace = buildWorkspaceFromPatch(currentWorkspace, nextEmployee);
    commitWorkspace(nextWorkspace, "cache");
  }

  async function syncEmployeeManagers(employeeId: string, nextManagerIds: string[], employeeActive: boolean): Promise<void> {
    if (!employeeActive) {
      return;
    }

    const normalizedEmployeeId = asString(employeeId);
    const normalizedNextManagerIds = normalizeManagerIds(
      nextManagerIds.filter((managerId) => asString(managerId) && asString(managerId) !== normalizedEmployeeId),
    );
    const activeEmployeeIds = new Set(
      allEmployees
        .filter((employee) => employee.active)
        .map((employee) => employee.id),
    );
    const invalidManagerIds = normalizedNextManagerIds.filter((managerId) => !activeEmployeeIds.has(managerId));
    if (invalidManagerIds.length) {
      throw new Error("Managers must be active employees.");
    }

    const currentMappings = await loadLeaveSphereEmployeeManagementManagers({ requestJson });
    const currentEmployeeMappings = currentMappings.filter((mapping) => mapping.employeeId === normalizedEmployeeId);
    const currentMappingByManagerId = new Map<string, LeaveSphereEmployeeManagementManager>(
      currentEmployeeMappings.map((mapping) => [mapping.managerId, mapping]),
    );
    const currentManagerIds = new Set(currentEmployeeMappings.map((mapping) => mapping.managerId));
    const removedMappings = currentEmployeeMappings.filter((mapping) => !normalizedNextManagerIds.includes(mapping.managerId));
    const addedManagerIds = normalizedNextManagerIds.filter((managerId) => !currentManagerIds.has(managerId));

    for (const mapping of removedMappings) {
      await deleteLeaveSphereEmployeeManagementManager({
        requestJson,
        mappingId: mapping.id,
      });
    }

    for (const managerId of addedManagerIds) {
      if (currentMappingByManagerId.has(managerId)) {
        continue;
      }
      await createLeaveSphereEmployeeManagementManager({
        requestJson,
        employeeId: normalizedEmployeeId,
        managerId,
      });
    }

    syncLeaveSphereEmployeeManagementManagerCache(
      managerCacheBaseContext,
      [
        ...currentMappings
          .filter((mapping) => mapping.employeeId !== normalizedEmployeeId)
          .map((mapping) => ({
            employeeId: mapping.employeeId,
            managerId: mapping.managerId,
          })),
        ...normalizedNextManagerIds.map((managerId) => ({
          employeeId: normalizedEmployeeId,
          managerId,
        })),
      ],
      { source: "network", fetchedAt: Date.now() },
    );
  }

  async function handleEmployeeSubmit(payload: { mode: EmployeeMode; id: string | null; form: LeaveSphereEmployeeManagementFormState }) {
    setIsMutationInFlight(true);
    try {
      if (payload.form.active) {
        const employeeLookup = new Map(allEmployees.map((employee) => [employee.id, employee]));
        const invalidManagerIds = normalizeManagerIds(payload.form.managerIds).filter((managerId) => {
          if (!managerId || managerId === payload.id) {
            return false;
          }
          return !employeeLookup.get(managerId)?.active;
        });
        if (invalidManagerIds.length) {
          throw new Error("Managers must be active employees.");
        }
      }

      if (payload.mode === "create") {
        const response = await createLeaveSphereEmployeeManagementEmployee({
          requestJson,
          payload: payload.form,
        });
        const createdId = extractLeaveSphereEmployeeManagementCreatedEmployeeId(response);
        const createdEmployee = normalizeLeaveSphereEmployeeManagementEmployee({
          id: createdId || `temp-${Date.now()}`,
          identityKey: payload.form.identityKey,
          firstName: payload.form.firstName,
          lastName: payload.form.lastName,
          email: payload.form.email,
          phone: payload.form.phone || null,
          dob: payload.form.dob || null,
          pictureUrl: payload.form.pictureUrl || null,
          region: payload.form.region || "US",
          startDate: payload.form.startDate || null,
          title: payload.form.title || null,
          isAE: payload.form.isAE,
          active: payload.form.active,
        });
        if (createdEmployee) {
          updateWorkspaceWithEmployee(createdEmployee);
          if (createdId) {
            await syncEmployeeManagers(createdId, payload.form.managerIds, payload.form.active);
          }
        }
      } else {
        if (!payload.id) {
          throw new Error("Missing employee id.");
        }
        const response = await updateLeaveSphereEmployeeManagementEmployee({
          requestJson,
          employeeId: payload.id,
          payload: payload.form,
        });
        const responseEmployee = extractLeaveSphereEmployeeManagementEmployeeFromPayload(response);
        const nextEmployee = responseEmployee || normalizeLeaveSphereEmployeeManagementEmployee({
          id: payload.id,
          identityKey: payload.form.identityKey,
          firstName: payload.form.firstName,
          lastName: payload.form.lastName,
          email: payload.form.email,
          phone: payload.form.phone || null,
          dob: payload.form.dob || null,
          pictureUrl: payload.form.pictureUrl || null,
          region: payload.form.region || "US",
          startDate: payload.form.startDate || null,
          title: payload.form.title || null,
          isAE: payload.form.isAE,
          active: payload.form.active,
        });
        if (nextEmployee) {
          updateWorkspaceWithEmployee(nextEmployee);
        }
        await syncEmployeeManagers(payload.id, payload.form.managerIds, payload.form.active);
      }
      void refreshWorkspace("network-only");
    } finally {
      setIsMutationInFlight(false);
    }
  }

  async function handlePictureUpload(file: File): Promise<string> {
    return uploadLeaveSphereEmployeeManagementPicture({
      requestJson,
      file,
    });
  }

  async function handleLifecycleAction(): Promise<void> {
    const target = pendingAction;
    if (!target) {
      return;
    }
    setPendingAction(null);
    setIsMutationInFlight(true);
    try {
      const response = target.nextActive
        ? await activateLeaveSphereEmployeeManagementEmployee({
            requestJson,
            employeeId: target.employee.id,
          })
        : await deactivateLeaveSphereEmployeeManagementEmployee({
            requestJson,
            employeeId: target.employee.id,
          });
      const nextEmployee = extractLeaveSphereEmployeeManagementEmployeeFromPayload(response) || {
        ...target.employee,
        active: target.nextActive,
      };
      if (nextEmployee) {
        updateWorkspaceWithEmployee(nextEmployee);
      }
      void refreshWorkspace("network-only");
    } finally {
      setIsMutationInFlight(false);
    }
  }

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          gradientVariant="workspace"
          eyebrow="LeaveSphere"
          title="Employees"
          description="Manage employee records, activate or deactivate accounts, and keep the LeaveSphere workspace current."
          action={(
            <Button onClick={openCreateModal} disabled={!canManage}>
              Add Employee
            </Button>
          )}
        />
      )}
      footer={workspace || cacheStatus ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            void refreshWorkspace("network-only");
          }}
          disabled={isLoading || isRefreshing || !isOnline}
          refreshing={isRefreshing}
          refreshLabel="Refresh employees"
          tooltipText={isOnline ? "Click to refresh the employee workspace" : "Offline. Reconnect to refresh employees."}
          containerClassName="w-full"
        />
      ) : null}
    >
      <Section className="rounded-[1.45rem] border border-blue-100/90 bg-white/95 p-5 shadow-soft">
        <SectionHeader
          title="Employees Search"
          description="Select search criteria, then click Search to load matching employees."
        />

        <form
          className="space-y-5 px-1.5"
          onSubmit={(event) => {
            event.preventDefault();
            handleSearch();
          }}
        >
          <div className="grid grid-cols-1 gap-3.5 md:grid-cols-2 xl:grid-cols-4">
            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Name or title
              </span>
              <div className="relative">
                <Input
                  value={searchDraft.nameOrTitle}
                  onChange={(event) => setSearchDraft((current) => ({ ...current, nameOrTitle: event.target.value }))}
                  placeholder="Search name or title"
                  autoComplete="off"
                  spellCheck={false}
                  className="pl-10"
                />
                <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-400" />
              </div>
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Email
              </span>
              <Input
                value={searchDraft.email}
                onChange={(event) => setSearchDraft((current) => ({ ...current, email: event.target.value }))}
                onBlur={handleSearchEmailBlur}
                placeholder="Search email"
                autoComplete="off"
                spellCheck={false}
                inputMode="email"
              />
              {searchEmailTouched && searchEmailError ? <p className="mt-1.5 text-xs text-rose-600">{searchEmailError}</p> : null}
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Region
              </span>
              <AppDropdown
                value={searchDraft.regionFilter}
                options={EMPLOYEE_REGION_FILTER_OPTIONS}
                onValueChange={(value) => setSearchDraft((current) => ({ ...current, regionFilter: normalizeRegionFilterValue(value) }))}
                searchable={false}
                allowCustomValue={false}
                ariaLabel="Employee region filter"
                placeholder=""
              />
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Status
              </span>
              <AppDropdown
                value={searchDraft.statusFilter}
                options={EMPLOYEE_STATUS_OPTIONS}
                onValueChange={(value) => setSearchDraft((current) => ({ ...current, statusFilter: normalizeStatusFilterValue(value) }))}
                searchable={false}
                allowCustomValue={false}
                ariaLabel="Employee status filter"
                placeholder=""
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
                <Button type="submit" disabled={Boolean(searchEmailError) || isLoading || isRefreshing}>
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
          title="Results"
          description={
            workspace && hasSearched
              ? `${filteredEmployees.length} of ${workspace.summary.totalEmployees} employees shown.`
              : "Search results will appear after you run a search."
          }
          contentClassName="space-y-4"
        >
          {workspace && filteredEmployees.length > 0 ? (
            <div className="space-y-4">
              <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
                <span className="inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 font-medium text-emerald-700">
                  {workspace.summary.activeEmployees} active
                </span>
                <span className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 font-medium text-slate-700">
                  {workspace.summary.inactiveEmployees} inactive
                </span>
              </div>

              {employeeResultGroups.map((group) => (
                <details
                  key={group.key}
                  open={employeeGroupOpenState[group.key] ?? true}
                  onToggle={(event) => {
                    const nextOpen = event.currentTarget.open;
                    setEmployeeGroupOpenState((current) => ({ ...current, [group.key]: nextOpen }));
                  }}
                  className="group overflow-hidden rounded-2xl border border-blue-100/90 bg-slate-50/75 shadow-[0_18px_30px_-26px_rgba(37,99,235,0.5)]"
                >
                  <summary className="flex cursor-pointer list-none items-center justify-between gap-2 border-b border-blue-100/90 bg-gradient-to-r from-blue-50/85 to-indigo-50/45 px-4 py-3.5">
                    <p className="text-sm font-semibold uppercase tracking-[0.12em] text-blue-800">{group.label}</p>
                    <span className="inline-flex items-center gap-2 text-slate-500" aria-hidden="true">
                      <span className="text-xs">{group.items.length} employees</span>
                      <ChevronDown className="size-4 transition-transform group-open:rotate-180" />
                    </span>
                  </summary>

                  <div className="p-3">
                    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                      {group.items.map((employee) => (
                        <EmployeeCard
                          key={employee.id}
                          employee={employee}
                          disabled={isLoading || isRefreshing || isMutationInFlight}
                          canActivate={Boolean(workspace.capabilities.canActivate)}
                          canDeactivate={Boolean(workspace.capabilities.canDeactivate)}
                          canEdit={Boolean(workspace.capabilities.canUpdate)}
                          onEdit={openEditModal}
                          onToggleActive={startToggleEmployeeActive}
                        />
                      ))}
                    </div>
                  </div>
                </details>
              ))}
            </div>
          ) : (
            <EmptyEmployeesPanel
              hasSearched={hasSearched}
              hasFilters={hasFilters}
              hasEmployees={hasEmployees}
              hasMatches={hasMatches}
              onClearFilters={resetSearchCriteria}
            />
          )}

          <SectionLoadingLayer
            active={Boolean(isRefreshing && workspace)}
            message="Refreshing employees..."
          />
        </SectionCard>
      </div>

      <EmployeeManagementModal
        open={isModalOpen}
        mode={modalMode}
        employee={modalEmployee}
        canEdit={canManage && Boolean(workspace?.capabilities.canUpdate)}
        onOpenChange={setIsModalOpen}
        requestJson={requestJson}
        employeeOptions={employeeOptions}
        onUploadPicture={handlePictureUpload}
        onSubmit={handleEmployeeSubmit}
        detailCacheStatusText={modalMode === "edit" ? cacheStatusText : null}
        detailCacheRefreshing={isRefreshing}
        detailCacheRefreshDisabled={isLoading || isRefreshing || !isOnline}
        onRefreshDetailCache={() => {
          void refreshWorkspace("network-only");
        }}
        managerCacheContext={managerCacheBaseContext}
      />

      <ConfirmDialog
        open={Boolean(pendingAction)}
        title={pendingAction?.nextActive ? "Activate employee?" : "Deactivate employee?"}
        description={
          pendingAction?.nextActive
            ? `Activate ${pendingAction ? buildEmployeeFullName(pendingAction.employee) : "this employee"} so they can participate in PTO workflows.`
            : `Deactivate ${pendingAction ? buildEmployeeFullName(pendingAction.employee) : "this employee"} to pause PTO activity.`
        }
        confirmLabel={pendingAction?.nextActive ? "Activate" : "Deactivate"}
        cancelLabel="Cancel"
        onConfirm={() => {
          void handleLifecycleAction();
        }}
        onCancel={() => setPendingAction(null)}
      />

    </AppPageLayout>
  );
}
