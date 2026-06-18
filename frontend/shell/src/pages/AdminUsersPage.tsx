import { ChevronDown, Copy, Filter, Plus, ShieldCheck, Trash2, UserCheck, UserMinus, X } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { ActionIconButton } from "@tradsphere/components/dashboard/ActionIconButton";
import { LabeledField, ReadOnlyValue } from "@tradsphere/components/dashboard/FormFieldRow";
import { PageBanner } from "@shell/components/layout/PageBanner";
import { AppDropdown } from "@tradsphere/components/ui/app-dropdown";
import { Button } from "@tradsphere/components/ui/button";
import {
  DialogClose,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@tradsphere/components/ui/dialog";
import { Input } from "@tradsphere/components/ui/input";
import { canModalClose, shouldBlockOutsideClose } from "@tradsphere/components/ui/modal-close-guard";
import { Spinner } from "@tradsphere/components/ui/spinner";
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { useOnlineStatus } from "@shared/hooks/useOnlineStatus";
import { readBrowserCacheSnapshot, writeBrowserCache } from "@shared/lib/browserCache";
import { SectionCard } from "@shared/components";
import { Tooltip } from "@shared/components/actions/Tooltip";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { PageLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import { FRONTEND_CACHE_TTL_MS } from "@shared/cache";
import { resolveCriteriaLoadPlan } from "@shared/hooks/useCriteriaLoadPolicy";
import { useAuth } from "@shared/auth/useAuth";

type RoleItem = {
  key: string;
  label: string;
  assignable: boolean;
  requiresSchemaChange?: boolean;
};

type TenantItem = {
  id: string;
  slug: string;
  name: string;
  active: boolean;
};

type AppItem = {
  id: string;
  code: string;
  name: string;
  active: boolean;
};

type TenantMembership = {
  tenantId: string;
  tenantSlug: string | null;
  tenantName: string | null;
  status: "active" | "pending" | "disabled";
  createdAt: string | null;
  updatedAt: string | null;
};

type AppAssignment = {
  tenantId: string;
  tenantSlug: string | null;
  tenantName: string | null;
  appId: string;
  appCode: string | null;
  appName: string | null;
  role: string | null;
  createdAt: string | null;
  updatedAt: string | null;
};

type AdminUser = {
  userId: string;
  email: string | null;
  fullName: string | null;
  status: "active" | "pending" | "disabled";
  role: string | null;
  createdAt: string | null;
  updatedAt: string | null;
  roleUpdatedAt: string | null;
  tenantMemberships: TenantMembership[];
  appAssignments: AppAssignment[];
  globalRoles?: string[];
  isSuperAdmin?: boolean;
};

type AdminInvitation = {
  id: string;
  email: string;
  tenantId: string;
  tenantSlug: string | null;
  tenantName: string | null;
  appId: string;
  appCode: string | null;
  appName: string | null;
  role: string | null;
  status: "pending" | "accepted" | "revoked" | "expired";
  expiresAt: string | null;
  createdAt: string | null;
  acceptedAt: string | null;
  inviteUrl: string | null;
  assignmentCount?: number;
  assignments?: Array<{
    tenantId: string;
    tenantSlug: string | null;
    tenantName: string | null;
    appId: string;
    appCode: string | null;
    appName: string | null;
    role: string | null;
  }>;
};

type DisableTarget = {
  userId: string;
  email: string | null;
  status: AdminUser["status"];
};

type PasswordResetTarget = {
  userId: string;
  email: string | null;
};

type EditAssignmentRow = {
  id: string;
  tenantId: string;
  appId: string;
  role: string;
};

type InviteAssignmentRow = {
  id: string;
  tenantId: string;
  appId: string;
  role: string;
};

type EditDraft = {
  fullName: string;
  assignments: EditAssignmentRow[];
};

type AdminPageCacheSnapshot = {
  users: AdminUser[];
  invitations: AdminInvitation[];
  roles: RoleItem[];
  tenants: TenantItem[];
  apps: AppItem[];
  scope: { isSuperAdmin?: boolean } | null;
};

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type AdminUsersLoadResponse = {
  users?: AdminUser[];
  invitations?: AdminInvitation[];
  roles?: RoleItem[];
  tenants?: TenantItem[];
  apps?: AppItem[];
  scope?: { isSuperAdmin?: boolean } | null;
};

const ROLE_GROUP_ORDER = [
  "super_admin",
  "admin",
  "editor",
  "viewer",
  "other",
] as const;

const ROLE_GROUP_LABEL: Record<(typeof ROLE_GROUP_ORDER)[number], string> = {
  super_admin: "Super Admin",
  admin: "Admin",
  editor: "Editor",
  viewer: "Viewer",
  other: "Other / Unknown",
};

const ADMIN_PAGE_CACHE_KEY = "admin-users:page:v1";
const ADMIN_PAGE_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;
const EDIT_MODAL_CLEAR_DELAY_MS = 360;

let adminUsersLoadInFlight: Promise<AdminUsersLoadResponse> | null = null;

function unwrap<T>(payload: unknown, fallback: T): T {
  if (payload && typeof payload === "object" && "data" in (payload as Record<string, unknown>)) {
    return ((payload as { data?: T }).data ?? fallback);
  }
  return (payload as T) ?? fallback;
}

function formatDate(value: string | null): string {
  if (!value) {
    return "-";
  }
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(timestamp));
}

function formatRelativeTime(timestamp: number): string {
  const ageMs = Math.max(0, Date.now() - timestamp);
  if (ageMs < 30_000) {
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
  return `${days} day${days > 1 ? "s" : ""} ago`;
}

function unique(values: string[]): string[] {
  const seen = new Set<string>();
  const results: string[] = [];
  for (const value of values) {
    const normalized = String(value || "").trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    results.push(normalized);
  }
  return results;
}

const APP_CHIP_STYLES = [
  "border-blue-200 bg-blue-50 text-blue-700",
  "border-fuchsia-200 bg-fuchsia-50 text-fuchsia-700",
  "border-teal-200 bg-teal-50 text-teal-700",
  "border-orange-200 bg-orange-50 text-orange-700",
] as const;
const APP_OPTION_CODE_PREFIX = "app-code::";
const EXTRA_APP_CODES = ["fundsphere", "leavesphere", "opssphere"] as const;
const APP_NAME_BY_CODE: Record<string, string> = {
  tradsphere: "TradSphere",
  spendsphere: "SpendSphere",
  fundsphere: "FundSphere",
  leavesphere: "LeaveSphere",
  opssphere: "OpsSphere",
  shiftzy: "Shiftzy",
};

function assignmentIdentity(item: { tenantId: string; appId: string; role: string }) {
  return `${item.tenantId}::${item.appId}::${item.role}`;
}

function normalizeRoleKey(roleKey: string | null | undefined): string {
  const normalized = String(roleKey || "").trim();
  if (!normalized) {
    return "";
  }
  if (normalized === "workspace.super_admin") {
    return "super_admin";
  }
  if (normalized === "tradsphere.admin") {
    return "admin";
  }
  if (normalized === "tradsphere.editor") {
    return "editor";
  }
  if (normalized === "tradsphere.viewer" || normalized === "user") {
    return "viewer";
  }
  return normalized;
}

function normalizeDraftComparable(draft: EditDraft): {
  fullName: string;
  assignments: string[];
} {
  const assignments = draft.assignments
    .map((item) => ({
      tenantId: String(item.tenantId || "").trim(),
      appId: String(item.appId || "").trim(),
      role: String(item.role || "").trim(),
    }))
    .filter((item) => item.tenantId && item.appId && item.role)
    .map((item) => assignmentIdentity(item))
    .sort((a, b) => a.localeCompare(b));
  return {
    fullName: draft.fullName.trim(),
    assignments,
  };
}

function statusChipClass(status: AdminUser["status"]): string {
  if (status === "active") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (status === "pending") {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  return "border-slate-300 bg-slate-100 text-slate-700";
}

function roleChipClass(roleKey: string | null): string {
  const normalized = normalizeRoleKey(roleKey);
  if (normalized === "super_admin") {
    return "border-fuchsia-200 bg-fuchsia-50 text-fuchsia-700";
  }
  if (normalized === "admin") {
    return "border-blue-200 bg-blue-50 text-blue-700";
  }
  if (normalized === "editor") {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  if (normalized === "viewer") {
    return "border-slate-300 bg-slate-100 text-slate-700";
  }
  return "border-violet-200 bg-violet-50 text-violet-700";
}

function invitationStatusChipClass(status: AdminInvitation["status"]): string {
  if (status === "pending") {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  if (status === "accepted") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (status === "revoked") {
    return "border-rose-200 bg-rose-50 text-rose-700";
  }
  return "border-slate-300 bg-slate-100 text-slate-700";
}

function DisableMemberIconButton({
  status,
  onToggle,
}: {
  status: AdminUser["status"];
  onToggle: () => void;
}) {
  const anchorRef = useRef<HTMLButtonElement | null>(null);
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const isDisabled = status === "disabled";
  const actionLabel = isDisabled ? "Enable access" : "Disable access";
  const tooltipText = isDisabled ? "Enable access" : "Disable access";

  return (
    <>
      <button
        ref={anchorRef}
        type="button"
        onClick={(event) => {
          event.stopPropagation();
          onToggle();
        }}
        onMouseEnter={() => setTooltipOpen(true)}
        onMouseLeave={() => setTooltipOpen(false)}
        onFocus={() => setTooltipOpen(true)}
        onBlur={() => setTooltipOpen(false)}
        className={`inline-flex h-7 w-7 items-center justify-center rounded-full border bg-white transition-colors focus-visible:outline-none focus-visible:ring-2 ${
          isDisabled
            ? "border-emerald-300 text-emerald-700 hover:border-emerald-400 hover:bg-emerald-50 focus-visible:ring-emerald-300"
            : "border-slate-300 text-slate-600 hover:border-rose-300 hover:bg-rose-50 hover:text-rose-700 focus-visible:ring-rose-300"
        }`}
        aria-label={actionLabel}
      >
        {isDisabled ? <UserCheck className="size-3.5" /> : <UserMinus className="size-3.5" />}
      </button>
      <Tooltip
        open={tooltipOpen}
        anchorRef={anchorRef}
        text={tooltipText}
      />
    </>
  );
}

function roleRank(roleKey: string | null): number {
  const normalized = normalizeRoleKey(roleKey);
  if (normalized === "super_admin") {
    return 0;
  }
  if (normalized === "admin") {
    return 1;
  }
  if (normalized === "editor") {
    return 2;
  }
  if (normalized === "viewer") {
    return 3;
  }
  return 4;
}

function roleGroupKey(roleKey: string | null): (typeof ROLE_GROUP_ORDER)[number] {
  const normalized = normalizeRoleKey(roleKey);
  if (normalized === "super_admin") {
    return "super_admin";
  }
  if (normalized === "admin") {
    return "admin";
  }
  if (normalized === "editor") {
    return "editor";
  }
  if (normalized === "viewer") {
    return "viewer";
  }
  return "other";
}

function displayRole(roleKey: string | null, roleLabels: Record<string, string>): string {
  const normalized = normalizeRoleKey(roleKey);
  if (!normalized) {
    return "Unknown";
  }
  return roleLabels[normalized]
    || roleLabels[String(roleKey || "").trim()]
    || (normalized === "super_admin"
      ? "Super Admin"
      : normalized === "admin"
        ? "Admin"
        : normalized === "editor"
          ? "Editor"
          : normalized === "viewer"
            ? "Viewer"
            : normalized);
}

function summarizeUserRole(user: AdminUser): string | null {
  if (user.isSuperAdmin || (Array.isArray(user.globalRoles) && user.globalRoles.some((role) => normalizeRoleKey(role) === "super_admin"))) {
    return "super_admin";
  }
  const roles = unique(
    user.appAssignments
      .map((item) => normalizeRoleKey(String(item.role || "").trim()))
      .filter((item) => Boolean(item)),
  );
  if (roles.length === 0) {
    return normalizeRoleKey(user.role);
  }
  return roles.sort((a, b) => roleRank(a) - roleRank(b))[0] || normalizeRoleKey(user.role);
}

function buildInitialDraft(user: AdminUser): EditDraft {
  const assignments = user.appAssignments
    .filter((item) => item.role && item.tenantId && item.appId)
    .map((item, index) => ({
      id: `${user.userId}-${index}-${item.tenantId}-${item.appId}`,
      tenantId: item.tenantId,
      appId: item.appId,
      role: normalizeRoleKey(String(item.role || "").trim()),
    }));
  return {
    fullName: user.fullName || "",
    assignments,
  };
}

function buildEmptyInviteAssignmentRow(id: string): InviteAssignmentRow {
  return {
    id,
    tenantId: "",
    appId: "",
    role: "",
  };
}

function normalizeInviteAssignmentsComparable(assignments: InviteAssignmentRow[]): string[] {
  return assignments
    .map((item) => ({
      tenantId: String(item.tenantId || "").trim(),
      appId: String(item.appId || "").trim(),
      role: normalizeRoleKey(String(item.role || "").trim()),
    }))
    .filter((item) => item.tenantId || item.appId || item.role)
    .map((item) => `${item.tenantId}::${item.appId}::${item.role}`)
    .sort((a, b) => a.localeCompare(b));
}

function tenantDisplayName(tenant: TenantItem): string {
  return tenant.name || tenant.slug || tenant.id;
}

function appDisplayName(app: AppItem): string {
  const normalizedCode = String(app.code || "").trim().toLowerCase();
  return APP_NAME_BY_CODE[normalizedCode] || app.name || app.code || app.id;
}

function isRoleItem(value: unknown): value is RoleItem {
  if (!value || typeof value !== "object") {
    return false;
  }
  const row = value as Record<string, unknown>;
  return typeof row.key === "string" && typeof row.label === "string" && typeof row.assignable === "boolean";
}

function isTenantItem(value: unknown): value is TenantItem {
  if (!value || typeof value !== "object") {
    return false;
  }
  const row = value as Record<string, unknown>;
  return typeof row.id === "string" && typeof row.slug === "string";
}

function isAppItem(value: unknown): value is AppItem {
  if (!value || typeof value !== "object") {
    return false;
  }
  const row = value as Record<string, unknown>;
  return typeof row.id === "string" && typeof row.code === "string";
}

function isAdminUser(value: unknown): value is AdminUser {
  if (!value || typeof value !== "object") {
    return false;
  }
  const row = value as Record<string, unknown>;
  return typeof row.userId === "string";
}

function isAdminInvitation(value: unknown): value is AdminInvitation {
  if (!value || typeof value !== "object") {
    return false;
  }
  const row = value as Record<string, unknown>;
  return typeof row.id === "string" && typeof row.email === "string";
}

function normalizeRoles(items: RoleItem[]): RoleItem[] {
  if (items.length > 0) {
    return items;
  }
  return [
    { key: "viewer", label: "Viewer", assignable: true },
    { key: "editor", label: "Editor", assignable: true },
    { key: "admin", label: "Admin", assignable: true },
  ];
}

function buildTenantDropdownOptions(tenants: TenantItem[]): Array<{ value: string; label: string; keywords?: string }> {
  return tenants
    .map((tenant) => ({
      value: tenant.id,
      label: tenantDisplayName(tenant),
      keywords: `${tenant.slug} ${tenant.id}`,
    }))
    .sort((a, b) => a.label.localeCompare(b.label));
}

function buildAppDropdownOptions(apps: AppItem[]): Array<{ value: string; label: string; keywords?: string }> {
  const options = apps
    .map((app) => ({
      value: app.id,
      label: appDisplayName(app),
      keywords: `${app.code} ${app.id} ${appDisplayName(app)}`,
    }))
    .sort((a, b) => a.label.localeCompare(b.label));

  const existingCodes = new Set(
    apps.map((app) => String(app.code || "").trim().toLowerCase()).filter(Boolean),
  );
  for (const appCode of EXTRA_APP_CODES) {
    if (existingCodes.has(appCode)) {
      continue;
    }
    options.push({
      value: `${APP_OPTION_CODE_PREFIX}${appCode}`,
      label: APP_NAME_BY_CODE[appCode] || appCode,
      keywords: appCode,
    });
  }

  options.sort((a, b) => a.label.localeCompare(b.label));
  return options;
}

export default function AdminUsersPage() {
  const auth = useAuth();
  const currentUserId = String(auth.user?.id || "").trim();
  const { requestJson } = useApiRequest();
  const { isOnline } = useOnlineStatus();
  const loadDataRef = useRef<(trigger?: "load-button" | "cache-chip") => Promise<void>>(async () => undefined);
  const loadInvocationRef = useRef(0);

  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [backgroundRefreshing, setBackgroundRefreshing] = useState(false);
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);

  const [users, setUsers] = useState<AdminUser[]>([]);
  const [invitations, setInvitations] = useState<AdminInvitation[]>([]);
  const [roles, setRoles] = useState<RoleItem[]>([]);
  const [tenants, setTenants] = useState<TenantItem[]>([]);
  const [apps, setApps] = useState<AppItem[]>([]);
  const [scope, setScope] = useState<{ isSuperAdmin?: boolean } | null>(null);

  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteAssignments, setInviteAssignments] = useState<InviteAssignmentRow[]>([]);
  const [inviteExpirationHours, setInviteExpirationHours] = useState("72");
  const [creatingInvite, setCreatingInvite] = useState(false);
  const [inviteAssignmentsTouched, setInviteAssignmentsTouched] = useState(false);
  const [isInviteModalOpen, setIsInviteModalOpen] = useState(false);
  const [isInviteUnsavedDialogOpen, setIsInviteUnsavedDialogOpen] = useState(false);
  const [memberSearch, setMemberSearch] = useState("");
  const [memberRoleFilter, setMemberRoleFilter] = useState<string>("all");
  const [memberStatusFilter, setMemberStatusFilter] = useState<string>("all");
  const [memberTenantFilter, setMemberTenantFilter] = useState<string>("all");

  const [editingUser, setEditingUser] = useState<AdminUser | null>(null);
  const [editDraft, setEditDraft] = useState<EditDraft | null>(null);
  const [originalEditDraft, setOriginalEditDraft] = useState<EditDraft | null>(null);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [isEditUnsavedDialogOpen, setIsEditUnsavedDialogOpen] = useState(false);
  const [savingEdit, setSavingEdit] = useState(false);
  const [passwordResetTarget, setPasswordResetTarget] = useState<PasswordResetTarget | null>(null);
  const [sendingPasswordReset, setSendingPasswordReset] = useState(false);
  const editModalClearTimerRef = useRef<number | null>(null);

  const [disableTarget, setDisableTarget] = useState<DisableTarget | null>(null);
  const [processingDisable, setProcessingDisable] = useState(false);

  const applyLoadedData = useCallback((
    payload: {
      users?: AdminUser[];
      invitations?: AdminInvitation[];
      roles?: RoleItem[];
      tenants?: TenantItem[];
      apps?: AppItem[];
      scope?: { isSuperAdmin?: boolean } | null;
    },
  ) => {
    const nextUsers = Array.isArray(payload.users) ? payload.users.filter(isAdminUser) : [];
    const nextInvites = Array.isArray(payload.invitations) ? payload.invitations.filter(isAdminInvitation) : [];
    const nextTenants = Array.isArray(payload.tenants) ? payload.tenants.filter(isTenantItem) : [];
    const nextApps = Array.isArray(payload.apps) ? payload.apps.filter(isAppItem) : [];
    const nextRoles = normalizeRoles(Array.isArray(payload.roles) ? payload.roles.filter(isRoleItem) : []);

    setUsers(nextUsers);
    setInvitations(nextInvites);
    setTenants(nextTenants);
    setApps(nextApps);
    setRoles(nextRoles);
    setScope(payload.scope ?? null);

    if (nextTenants.length > 0 && nextApps.length > 0 && nextRoles.length > 0) {
      setInviteAssignments((current) => {
        if (current.length > 0) {
          return current;
        }
        return [buildEmptyInviteAssignmentRow("invite-row-initial")];
      });
    }
  }, []);

  const requestBundledAdminLoad = useCallback(async (): Promise<AdminUsersLoadResponse> => {
    if (!adminUsersLoadInFlight) {
      adminUsersLoadInFlight = requestJson("/api/auth/v1/admin/users/load", { successToast: false, errorToast: false })
        .then((payload) => unwrap<AdminUsersLoadResponse>(payload, {}))
        .finally(() => {
          adminUsersLoadInFlight = null;
        });
    }
    return adminUsersLoadInFlight;
  }, [requestJson]);

  const roleLabels = useMemo(() => {
    const map: Record<string, string> = {};
    for (const role of roles) {
      const normalizedKey = normalizeRoleKey(role.key);
      map[role.key] = role.label;
      if (normalizedKey && !(normalizedKey in map)) {
        map[normalizedKey] = role.label;
      }
    }
    map.super_admin = map.super_admin || "Super Admin";
    map.admin = map.admin || "Admin";
    map.editor = map.editor || "Editor";
    map.viewer = map.viewer || "Viewer";
    return map;
  }, [roles]);

  const assignableRoles = useMemo(() => roles.filter((role) => role.assignable), [roles]);
  const pendingInvitations = useMemo(() => invitations.filter((item) => item.status === "pending"), [invitations]);

  const tenantNameById = useMemo(() => {
    const map: Record<string, string> = {};
    for (const tenant of tenants) {
      map[tenant.id] = tenantDisplayName(tenant);
    }
    return map;
  }, [tenants]);

  const appNameById = useMemo(() => {
    const map: Record<string, string> = {};
    for (const app of apps) {
      map[app.id] = appDisplayName(app);
    }
    return map;
  }, [apps]);
  const appCodeById = useMemo(() => {
    const map: Record<string, string> = {};
    for (const app of apps) {
      map[app.id] = String(app.code || "").trim().toLowerCase();
    }
    return map;
  }, [apps]);

  const tenantDropdownOptions = useMemo(
    () => buildTenantDropdownOptions(tenants),
    [tenants],
  );

  const appDropdownOptions = useMemo(
    () => buildAppDropdownOptions(apps),
    [apps],
  );

  const roleDropdownOptions = useMemo(
    () => assignableRoles.map((role) => ({ value: role.key, label: role.label })),
    [assignableRoles],
  );

  const roleFilterOptions = useMemo(
    () => [
      { value: "all", label: "All roles" },
      ...ROLE_GROUP_ORDER.map((groupKey) => ({
        value: groupKey,
        label: ROLE_GROUP_LABEL[groupKey],
      })),
    ],
    [],
  );

  const statusFilterOptions = useMemo(
    () => [
      { value: "all", label: "All statuses" },
      { value: "active", label: "Active" },
      { value: "pending", label: "Pending" },
      { value: "disabled", label: "Disabled" },
    ],
    [],
  );

  const memberTenantFilterOptions = useMemo(
    () => [
      { value: "all", label: "All tenants" },
      ...tenantDropdownOptions,
    ],
    [tenantDropdownOptions],
  );

  const filteredGroupedMembers = useMemo(() => {
    const searchValue = memberSearch.trim().toLowerCase();
    const filteredUsers = users.filter((user) => {
      const resolvedRole = summarizeUserRole(user);
      const groupKey = roleGroupKey(resolvedRole);
      if (memberRoleFilter !== "all" && memberRoleFilter !== groupKey) {
        return false;
      }
      if (memberStatusFilter !== "all" && memberStatusFilter !== user.status) {
        return false;
      }
      if (memberTenantFilter !== "all") {
        const hasTenant = user.tenantMemberships.some((membership) => membership.tenantId === memberTenantFilter);
        if (!hasTenant) {
          return false;
        }
      }
      if (!searchValue) {
        return true;
      }

      const tenantLabels = user.tenantMemberships
        .map((item) => item.tenantName || item.tenantSlug || tenantNameById[item.tenantId] || item.tenantId)
        .join(" ");
      const assignmentLabels = user.appAssignments
        .map((item) => {
          const appLabel = appNameById[item.appId] || item.appName || item.appCode || item.appId;
          const tenantLabel = item.tenantName || item.tenantSlug || tenantNameById[item.tenantId] || item.tenantId;
          const roleLabel = displayRole(item.role, roleLabels);
          return `${tenantLabel} ${appLabel} ${roleLabel}`;
        })
        .join(" ");
      const haystack = [
        user.fullName || "",
        user.email || "",
        user.userId,
        tenantLabels,
        assignmentLabels,
      ].join(" ").toLowerCase();
      return haystack.includes(searchValue);
    });

    const groups: Record<(typeof ROLE_GROUP_ORDER)[number], AdminUser[]> = {
      super_admin: [],
      admin: [],
      editor: [],
      viewer: [],
      other: [],
    };
    for (const user of filteredUsers) {
      const role = summarizeUserRole(user);
      const key = roleGroupKey(role);
      groups[key].push(user);
    }
    for (const key of ROLE_GROUP_ORDER) {
      groups[key].sort((a, b) => String(a.email || a.userId).localeCompare(String(b.email || b.userId)));
    }
    return groups;
  }, [
    appNameById,
    memberRoleFilter,
    memberSearch,
    memberStatusFilter,
    memberTenantFilter,
    roleLabels,
    tenantNameById,
    users,
  ]);

  const filteredUserCount = useMemo(
    () => ROLE_GROUP_ORDER.reduce((count, key) => count + filteredGroupedMembers[key].length, 0),
    [filteredGroupedMembers],
  );

  const loadData = useCallback(async (trigger: "load-button" | "cache-chip" = "load-button") => {
    const loadInvocationId = loadInvocationRef.current + 1;
    loadInvocationRef.current = loadInvocationId;
    const loadPlan = resolveCriteriaLoadPlan({
      trigger,
      criteriaKey: ADMIN_PAGE_CACHE_KEY,
      loadedCriteriaKey: trigger === "cache-chip" ? ADMIN_PAGE_CACHE_KEY : null,
    });

    const cachedSnapshot = !loadPlan.shouldIgnoreCache
      ? readBrowserCacheSnapshot<AdminPageCacheSnapshot>(ADMIN_PAGE_CACHE_KEY)
      : null;
    const cachedData = cachedSnapshot?.data;
    const canUseCachedData = Boolean(
      cachedData
      && Array.isArray(cachedData.users)
      && Array.isArray(cachedData.invitations)
      && Array.isArray(cachedData.roles)
      && Array.isArray(cachedData.tenants)
      && Array.isArray(cachedData.apps),
    );

    if (loadPlan.shouldIgnoreCache) {
      setRefreshing(true);
      setBackgroundRefreshing(false);
    } else {
      setLoading(true);
      setBackgroundRefreshing(false);
      if (canUseCachedData && cachedData) {
        applyLoadedData(cachedData);
        setCacheStatus({
          source: "cache",
          fetchedAt: cachedSnapshot?.fetchedAt ?? Date.now(),
        });
        setLoading(false);
        setBackgroundRefreshing(true);
      }
    }
    setError(null);

    if (!isOnline) {
      if (!canUseCachedData) {
        if (loadInvocationRef.current !== loadInvocationId) {
          return;
        }
        setError("You're offline. Admin data is unavailable until connection is restored.");
        setCacheStatus(null);
      }
      if (loadInvocationRef.current === loadInvocationId) {
        setLoading(false);
        setRefreshing(false);
        setBackgroundRefreshing(false);
      }
      return;
    }

    try {
      const bundledData = await requestBundledAdminLoad();
      const nextRoles = Array.isArray(bundledData.roles) ? bundledData.roles : [];

      const nextData: AdminPageCacheSnapshot = {
        users: Array.isArray(bundledData.users) ? bundledData.users : [],
        invitations: Array.isArray(bundledData.invitations) ? bundledData.invitations : [],
        roles: nextRoles,
        tenants: Array.isArray(bundledData.tenants) ? bundledData.tenants : [],
        apps: Array.isArray(bundledData.apps) ? bundledData.apps : [],
        scope: bundledData.scope || null,
      };
      if (loadInvocationRef.current !== loadInvocationId) {
        return;
      }
      applyLoadedData(nextData);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });
      writeBrowserCache(ADMIN_PAGE_CACHE_KEY, nextData, ADMIN_PAGE_CACHE_TTL_MS, {
        source: "network",
        fetchedAt: Date.now(),
      });
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "Unable to load admin data.";
      if (!canUseCachedData) {
        if (loadInvocationRef.current !== loadInvocationId) {
          return;
        }
        setError(message);
        setCacheStatus(null);
      }
    } finally {
      if (loadInvocationRef.current !== loadInvocationId) {
        return;
      }
      setLoading(false);
      setRefreshing(false);
      setBackgroundRefreshing(false);
    }
  }, [applyLoadedData, isOnline, requestBundledAdminLoad]);

  useEffect(() => {
    loadDataRef.current = loadData;
  }, [loadData]);

  useEffect(() => {
    void loadDataRef.current("load-button");
  }, []);

  function resetInviteDraft() {
    setInviteEmail("");
    setInviteExpirationHours("72");
    setInviteAssignmentsTouched(false);
    setInviteAssignments([buildEmptyInviteAssignmentRow("invite-row-initial")]);
  }

  function openEditUser(user: AdminUser) {
    const isSuperAdminMember = summarizeUserRole(user) === "super_admin" || Boolean(user.isSuperAdmin);
    if (isSuperAdminMember && !scope?.isSuperAdmin) {
      return;
    }
    if (editModalClearTimerRef.current !== null && typeof window !== "undefined") {
      window.clearTimeout(editModalClearTimerRef.current);
      editModalClearTimerRef.current = null;
    }
    const draft = buildInitialDraft(user);
    setEditingUser(user);
    setEditDraft(draft);
    setOriginalEditDraft(draft);
    setIsEditUnsavedDialogOpen(false);
    setIsEditModalOpen(true);
  }

  function closeEditUser() {
    if (savingEdit || sendingPasswordReset) {
      return;
    }
    setPasswordResetTarget(null);
    setIsEditUnsavedDialogOpen(false);
    setIsEditModalOpen(false);
  }

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    if (isEditModalOpen) {
      if (editModalClearTimerRef.current !== null) {
        window.clearTimeout(editModalClearTimerRef.current);
        editModalClearTimerRef.current = null;
      }
      return;
    }

    if (!editingUser && !editDraft && !originalEditDraft) {
      return;
    }

    editModalClearTimerRef.current = window.setTimeout(() => {
      setEditingUser(null);
      setEditDraft(null);
      setOriginalEditDraft(null);
      editModalClearTimerRef.current = null;
    }, EDIT_MODAL_CLEAR_DELAY_MS);

    return () => {
      if (editModalClearTimerRef.current !== null) {
        window.clearTimeout(editModalClearTimerRef.current);
        editModalClearTimerRef.current = null;
      }
    };
  }, [editDraft, editingUser, isEditModalOpen, originalEditDraft]);

  function updateEditAssignmentRow(rowId: string, patch: Partial<EditAssignmentRow>) {
    setEditDraft((current) => {
      if (!current) {
        return current;
      }
      return {
        ...current,
        assignments: current.assignments.map((item) => (item.id === rowId ? { ...item, ...patch } : item)),
      };
    });
  }

  function addEditAssignmentRow() {
    setEditDraft((current) => {
      if (!current) {
        return current;
      }
      return {
        ...current,
        assignments: [
          ...current.assignments,
          {
            id: `row-${Date.now()}-${Math.random().toString(36).slice(2)}`,
            tenantId: "",
            appId: "",
            role: "",
          },
        ],
      };
    });
  }

  function removeEditAssignmentRow(rowId: string) {
    setEditDraft((current) => {
      if (!current) {
        return current;
      }
      return {
        ...current,
        assignments: current.assignments.filter((item) => item.id !== rowId),
      };
    });
  }

  function updateInviteAssignmentRow(rowId: string, patch: Partial<InviteAssignmentRow>) {
    setInviteAssignments((current) =>
      current.map((item) => (item.id === rowId ? { ...item, ...patch } : item)),
    );
  }

  function addInviteAssignmentRow() {
    setInviteAssignments((current) => [
      ...current,
      {
        id: `invite-row-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        tenantId: "",
        appId: "",
        role: "",
      },
    ]);
  }

  function removeInviteAssignmentRow(rowId: string) {
    setInviteAssignments((current) => current.filter((item) => item.id !== rowId));
  }

  async function handleSaveUserEdit() {
    if (!editingUser || !editDraft) {
      return;
    }
    const validAssignments = editDraft.assignments
      .map((item) => {
        const tenantId = String(item.tenantId || "").trim();
        const appSelection = String(item.appId || "").trim();
        const appCode = appSelection.startsWith(APP_OPTION_CODE_PREFIX)
          ? appSelection.slice(APP_OPTION_CODE_PREFIX.length).trim().toLowerCase()
          : "";
        const appId = appCode ? "" : appSelection;
        const mappedAppCode = appCode || appCodeById[appId] || "";
        return {
          tenantId,
          appId,
          appCode: mappedAppCode || undefined,
          role: String(item.role || "").trim(),
        };
      })
      .filter((item) => item.tenantId && (item.appId || item.appCode) && item.role);

    setSavingEdit(true);
    try {
      await requestJson(`/api/auth/v1/admin/users/${encodeURIComponent(editingUser.userId)}/access`, {
        method: "PATCH",
        body: {
          fullName: editDraft.fullName.trim() || undefined,
          email: editingUser.email || undefined,
          appAssignments: validAssignments,
        },
        successToast: {
          title: "Member updated",
          message: `${editingUser.email || editingUser.userId} access has been updated.`,
        },
      });
      closeEditUser();
      await loadData("cache-chip");
    } finally {
      setSavingEdit(false);
    }
  }

  async function handleCreateInvite() {
    setInviteAssignmentsTouched(true);
    if (inviteValidationError) {
      return;
    }
    const normalizedEmail = inviteEmail.trim().toLowerCase();
    const normalizedAssignments = inviteAssignments
      .map((item) => {
        const tenantId = String(item.tenantId || "").trim();
        const appSelection = String(item.appId || "").trim();
        const appCode = appSelection.startsWith(APP_OPTION_CODE_PREFIX)
          ? appSelection.slice(APP_OPTION_CODE_PREFIX.length).trim().toLowerCase()
          : "";
        const appId = appCode ? "" : appSelection;
        const mappedAppCode = appCode || appCodeById[appId] || "";
        return {
          tenantId,
          appId,
          appCode: mappedAppCode || undefined,
          role: normalizeRoleKey(String(item.role || "").trim()),
        };
      })
      .filter((item) => item.tenantId && (item.appId || item.appCode) && item.role);
    if (!normalizedEmail || normalizedAssignments.length === 0) {
      return;
    }
    const seenAssignments = new Set<string>();
    for (const item of normalizedAssignments) {
      const appIdentity = item.appId || `${APP_OPTION_CODE_PREFIX}${item.appCode || ""}`;
      const key = `${item.tenantId}::${appIdentity}`;
      if (seenAssignments.has(key)) {
        return;
      }
      seenAssignments.add(key);
      if (item.role === "super_admin") {
        return;
      }
    }
    if (!normalizedAssignments.every((item) => item.role === "admin" || item.role === "editor" || item.role === "viewer")) {
      return;
    }

    const expiresHours = Number(inviteExpirationHours);
    if (!Number.isFinite(expiresHours) || expiresHours < 1) {
      return;
    }

    setCreatingInvite(true);
    try {
      const payload = await requestJson("/api/auth/v1/admin/invitations", {
        method: "POST",
        body: {
          email: normalizedEmail,
          assignments: normalizedAssignments,
          expirationHours: Math.floor(expiresHours),
        },
        successToast: {
          title: "Invitation created",
          message: `Invite package for ${normalizedEmail} is ready.`,
        },
      });
      const data = unwrap<{ inviteUrl?: string; items?: Array<{ inviteUrl?: string }> }>(payload, {});
      const firstInviteUrl = String(data.inviteUrl || data.items?.[0]?.inviteUrl || "").trim();
      if (firstInviteUrl) {
        await navigator.clipboard.writeText(firstInviteUrl).catch(() => undefined);
      }
      resetInviteDraft();
      setIsInviteUnsavedDialogOpen(false);
      setIsInviteModalOpen(false);
      await loadData("cache-chip");
    } finally {
      setCreatingInvite(false);
    }
  }

  async function handleRevokeInvite(invitationId: string, email: string) {
    await requestJson(`/api/auth/v1/admin/invitations/${encodeURIComponent(invitationId)}/revoke`, {
      method: "POST",
      successToast: {
        title: "Invitation revoked",
        message: `Pending invite for ${email} has been revoked.`,
      },
    });
    await loadData("cache-chip");
  }

  async function handleDisableConfirmed() {
    if (!disableTarget) {
      return;
    }
    const nextStatus: AdminUser["status"] = disableTarget.status === "disabled" ? "active" : "disabled";
    const isEnableAction = nextStatus === "active";

    setProcessingDisable(true);
    try {
      await requestJson(`/api/auth/v1/admin/users/${encodeURIComponent(disableTarget.userId)}`, {
        method: "PATCH",
        body: {
          status: nextStatus,
        },
        successToast: {
          title: isEnableAction ? "Account enabled" : "Account disabled",
          message: isEnableAction
            ? `${disableTarget.email || disableTarget.userId} can sign in again.`
            : `${disableTarget.email || disableTarget.userId} is now banned from sign-in.`,
        },
      });
      setDisableTarget(null);
      closeEditUser();
      await loadData("cache-chip");
    } finally {
      setProcessingDisable(false);
    }
  }

  async function handleSendPasswordResetConfirmed() {
    if (!passwordResetTarget) {
      return;
    }
    setSendingPasswordReset(true);
    try {
      await requestJson(`/api/auth/v1/admin/users/${encodeURIComponent(passwordResetTarget.userId)}/password-reset`, {
        method: "POST",
        successToast: {
          title: "Password reset sent",
          message: `A secure password-reset email was sent to ${passwordResetTarget.email || passwordResetTarget.userId}.`,
        },
      });
      setPasswordResetTarget(null);
    } finally {
      setSendingPasswordReset(false);
    }
  }

  const cacheStatusText = (refreshing || backgroundRefreshing)
    ? "Refreshing..."
    : !isOnline && cacheStatus
      ? `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`
    : cacheStatus
      ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : "No cached data yet";
  const hasLoadedAnyData = users.length > 0 || pendingInvitations.length > 0 || tenants.length > 0 || apps.length > 0;
  const showCacheChip = !loading && !error && !isEditModalOpen && !disableTarget;
  const loadingContract = resolveSharedLoadingContract(
    {
      pageInitializing: loading && !hasLoadedAnyData,
      pageRefreshing: refreshing || backgroundRefreshing,
      cacheChipRefreshing: isChipRefreshOverlayVisible,
    },
    {
      pageInitializing: "Preparing admin workspace...",
      pageRefreshing: "Refreshing admin data...",
      cacheChipRefreshing: "Refreshing admin data...",
    },
  );
  const pageMessages: StackMessage[] = [];
  if (error) {
    pageMessages.push({
      id: "admin-users-load-error",
      variant: "error",
      message: error,
    });
  }
  const inviteValidationError = useMemo(() => {
    const seen = new Set<string>();
    for (const row of inviteAssignments) {
      const tenantId = String(row.tenantId || "").trim();
      const appId = String(row.appId || "").trim();
      const role = normalizeRoleKey(String(row.role || "").trim());
      if (!tenantId || !appId || !role) {
        return "Complete all invite assignment rows before creating invitation.";
      }
      if (role === "super_admin") {
        return "Super Admin cannot be assigned from this page.";
      }
      const key = `${tenantId}::${appId}`;
      if (seen.has(key)) {
        return "Duplicate tenant/app assignment rows are not allowed.";
      }
      seen.add(key);
    }
    return null;
  }, [inviteAssignments]);
  const editValidationError = useMemo(() => {
    if (!editDraft) {
      return null;
    }
    const seen = new Set<string>();
    for (const row of editDraft.assignments) {
      const tenantId = String(row.tenantId || "").trim();
      const appId = String(row.appId || "").trim();
      const role = String(row.role || "").trim();
      const isBlankRow = !tenantId && !appId && role.length === 0;
      if (isBlankRow) {
        continue;
      }
      if (!tenantId || !appId || role.length === 0) {
        return "Complete all app assignment fields before saving.";
      }
      const key = `${tenantId}::${appId}`;
      if (seen.has(key)) {
        return "Duplicate tenant/app assignments are not allowed.";
      }
      seen.add(key);
    }
    return null;
  }, [editDraft]);

  const hasEditChanges = useMemo(() => {
    if (!editDraft || !originalEditDraft) {
      return false;
    }
    const current = normalizeDraftComparable(editDraft);
    const original = normalizeDraftComparable(originalEditDraft);
    return JSON.stringify(current) !== JSON.stringify(original);
  }, [editDraft, originalEditDraft]);

  const hasInviteChanges = useMemo(() => {
    const normalizedEmail = inviteEmail.trim().toLowerCase();
    const normalizedExpirationHours = inviteExpirationHours.trim();
    const normalizedAssignments = normalizeInviteAssignmentsComparable(inviteAssignments);
    return Boolean(normalizedEmail || normalizedExpirationHours !== "72" || normalizedAssignments.length > 0);
  }, [inviteAssignments, inviteEmail, inviteExpirationHours]);

  const canSubmitEdit = Boolean(editingUser && editDraft && hasEditChanges && !editValidationError);
  const shouldShowSaveButton = canSubmitEdit || savingEdit;
  const canSubmitInvite = Boolean(
    hasInviteChanges &&
      !creatingInvite &&
      inviteEmail.trim() &&
      inviteAssignments.length > 0 &&
      !inviteValidationError,
  );
  const shouldShowCreateInviteButton = creatingInvite || canSubmitInvite;

  function handleEditModalOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy: savingEdit || sendingPasswordReset,
      hasUnsavedChanges: hasEditChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasEditChanges && !savingEdit && !sendingPasswordReset) {
        setIsEditUnsavedDialogOpen(true);
      }
      return;
    }
    if (nextOpen) {
      setIsEditModalOpen(true);
      return;
    }
    closeEditUser();
  }

  function handleInviteModalOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy: creatingInvite,
      hasUnsavedChanges: hasInviteChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasInviteChanges && !creatingInvite) {
        setIsInviteUnsavedDialogOpen(true);
      }
      return;
    }
    setIsInviteModalOpen(nextOpen);
  }

  async function handleRefreshFromChip() {
    setIsChipRefreshOverlayVisible(true);
    try {
      await loadData("cache-chip");
    } finally {
      setIsChipRefreshOverlayVisible(false);
    }
  }

  return (
    <AppPageLayout
      className="max-w-[1680px] gap-6 pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          eyebrow="TheSphereWorks"
          title="Admin Users"
          description="Manage app assignments, roles, and invitations."
          gradientVariant="admin"
          className="[&>div.relative]:min-h-[136px] [&>div.relative]:py-6 md:[&>div.relative]:min-h-[168px] md:[&>div.relative]:py-8"
          action={(
            <Button
              className="min-w-36"
              onClick={() => {
                setIsInviteUnsavedDialogOpen(false);
                if (inviteAssignments.length === 0) {
                  setInviteAssignments([buildEmptyInviteAssignmentRow("invite-row-initial")]);
                }
                setIsInviteModalOpen(true);
              }}
            >
              Invite User
            </Button>
          )}
        />
      )}
      footer={showCacheChip ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={handleRefreshFromChip}
          disabled={refreshing || backgroundRefreshing || loading || isChipRefreshOverlayVisible || !isOnline}
          refreshing={refreshing || backgroundRefreshing || isChipRefreshOverlayVisible}
          refreshLabel="Refresh admin data"
          tooltipText={
            isOnline
              ? "Click to refresh admin users, invitations, tenants, apps, and roles"
              : "Offline. Reconnect to refresh admin data."
          }
          containerClassName="w-full"
        />
      ) : null}
    >

      <SectionCard
        title="Invitations"
        description="Manage pending invitation links and invite status."
        actions={<p className="text-sm text-slate-500">{pendingInvitations.length} pending</p>}
        className="p-5"
      >
        <div className="mt-4 flex items-center justify-between gap-2">
          <h3 className="text-sm font-semibold text-slate-800">Pending invitations</h3>
          <p className="text-xs uppercase tracking-[0.14em] text-slate-500">{pendingInvitations.length}</p>
        </div>

        <div className="mt-3 space-y-2">
          {loading && pendingInvitations.length === 0 ? (
            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
              <div className="flex items-center gap-2">
                <Spinner className="size-4" />
                <span>Loading invitations...</span>
              </div>
            </div>
          ) : null}
          {!loading && pendingInvitations.length === 0 ? (
            <p className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
              No pending invitations.
            </p>
          ) : null}
          {pendingInvitations.map((invite) => (
            <article key={invite.id} className="space-y-2 rounded-lg border border-slate-200 bg-slate-50/70 p-3">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div className="min-w-0">
                  <p className="truncate text-sm font-semibold text-slate-900">{invite.email}</p>
                  <p className="mt-0.5 text-xs text-slate-500">
                    Created: {formatDate(invite.createdAt)} · Expires: {formatDate(invite.expiresAt)}
                  </p>
                </div>
                <div className="flex items-center gap-1.5">
                  <span className={`inline-flex rounded-full border px-2 py-0.5 text-xs font-semibold capitalize ${invitationStatusChipClass(invite.status)}`}>
                    {invite.status}
                  </span>
                  <Button
                    variant="outline"
                    className="h-8 px-2.5 text-xs"
                    onClick={() => void navigator.clipboard.writeText(String(invite.inviteUrl || "")).catch(() => undefined)}
                    disabled={!invite.inviteUrl}
                  >
                    <Copy className="size-3.5" />
                    Copy link
                  </Button>
                  <Button
                    variant="secondary"
                    className="h-8 px-2.5 text-xs"
                    onClick={() => void handleRevokeInvite(invite.id, invite.email)}
                  >
                    Revoke
                  </Button>
                </div>
              </div>
              <div className="flex flex-wrap gap-1">
                {Array.isArray(invite.assignments) && invite.assignments.length > 0 ? (
                  invite.assignments.map((assignment) => {
                    const tenantLabel = assignment.tenantName || assignment.tenantSlug || assignment.tenantId || "-";
                    const appLabel = appNameById[assignment.appId] || assignment.appName || assignment.appCode || assignment.appId || "-";
                    const roleLabel = displayRole(assignment.role || null, roleLabels);
                    return (
                      <span key={`${invite.id}-${tenantLabel}-${appLabel}-${roleLabel}`} className="rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-700">
                        {tenantLabel}: {appLabel} ({roleLabel})
                      </span>
                    );
                  })
                ) : (
                  <span className="rounded-full border border-slate-200 bg-white px-2 py-0.5 text-xs text-slate-600">
                    {(invite.tenantName || invite.tenantSlug || invite.tenantId)}: {(appNameById[invite.appId] || invite.appName || invite.appCode || invite.appId)} ({displayRole(invite.role, roleLabels)})
                  </span>
                )}
              </div>
            </article>
          ))}
        </div>
      </SectionCard>

      <SectionCard
        title="Members"
        description="Grouped by access role with tenant/app assignment visibility and in-place admin actions."
        actions={(
          <p className="text-sm text-slate-500">
            {filteredUserCount}/{users.length} users
            {scope?.isSuperAdmin ? " · super-admin scope" : ""}
          </p>
        )}
        className="p-5"
      >

        <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50/70 p-3">
          <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
            <Filter className="size-3.5" />
            <span>Filters</span>
          </div>
          <div className="mt-2 grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
            <Input
              value={memberSearch}
              onChange={(event) => setMemberSearch(event.target.value)}
              placeholder="Search name, email, tenant, app"
            />
            <AppDropdown
              value={memberRoleFilter}
              onValueChange={(value) => setMemberRoleFilter(value)}
              options={roleFilterOptions}
              searchable={false}
            />
            <AppDropdown
              value={memberStatusFilter}
              onValueChange={(value) => setMemberStatusFilter(value)}
              options={statusFilterOptions}
              searchable={false}
            />
            <AppDropdown
              value={memberTenantFilter}
              onValueChange={(value) => setMemberTenantFilter(value)}
              options={memberTenantFilterOptions}
              searchable={false}
            />
          </div>
        </div>

        <div className="mt-4">
          {loading && users.length === 0 ? (
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
              <div className="flex items-center gap-2">
                <Spinner className="size-4" />
                <span>Loading members...</span>
              </div>
            </div>
          ) : filteredUserCount === 0 ? (
            <p className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
              No members match the current filters.
            </p>
          ) : (
            <div className="space-y-3">
              {ROLE_GROUP_ORDER.map((groupKey) => {
                const groupUsers = filteredGroupedMembers[groupKey] || [];
                if (groupUsers.length === 0) {
                  return null;
                }
                return (
                  <details key={groupKey} open className="group overflow-hidden rounded-2xl border border-blue-100 bg-slate-50/70">
                    <summary className="flex cursor-pointer list-none items-center justify-between gap-2 border-b border-blue-100 bg-blue-50/70 px-4 py-3">
                      <p className="text-sm font-bold uppercase tracking-[0.14em] text-blue-800">
                        {ROLE_GROUP_LABEL[groupKey]} Members
                      </p>
                      <div className="flex items-center gap-2">
                        <p className="text-xs text-slate-500">{groupUsers.length} members</p>
                        <ChevronDown className="size-4 text-slate-500 transition-transform group-open:rotate-180" />
                      </div>
                    </summary>
                    <div className="p-3">
                      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                        {groupUsers.map((user) => {
                          const resolvedRole = summarizeUserRole(user);
                          const isSuperAdminMember = resolvedRole === "super_admin" || Boolean(user.isSuperAdmin);
                          const isSelfMember = Boolean(currentUserId && user.userId === currentUserId);
                          const canEditMember = !isSuperAdminMember || Boolean(scope?.isSuperAdmin);
                          const canEditMemberAccess = canEditMember;
                          const assignmentLabels = unique(
                            user.appAssignments.map((item) => {
                              const appLabel = appNameById[item.appId] || item.appName || item.appCode || item.appId;
                              const roleLabel = displayRole(item.role, roleLabels);
                              const tenantLabel = item.tenantName || item.tenantSlug || tenantNameById[item.tenantId] || item.tenantId;
                              return `${tenantLabel}: ${appLabel} (${roleLabel})`;
                            }),
                          );

                          return (
                            <article
                              key={user.userId}
                              role={canEditMemberAccess ? "button" : undefined}
                              tabIndex={canEditMemberAccess ? 0 : -1}
                              onClick={() => {
                                if (!canEditMemberAccess) {
                                  return;
                                }
                                openEditUser(user);
                              }}
                              onKeyDown={(event) => {
                                if (!canEditMemberAccess) {
                                  return;
                                }
                                if (event.key === "Enter" || event.key === " ") {
                                  event.preventDefault();
                                  openEditUser(user);
                                }
                              }}
                              className={`rounded-xl p-4 shadow-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300 ${
                                canEditMemberAccess
                                  ? "cursor-pointer border border-slate-200 bg-white hover:border-blue-200 hover:bg-blue-50/40"
                                  : "cursor-default border border-slate-200 bg-white"
                              }`}
                            >
                              <div className="flex items-start justify-between gap-2">
                                <div className="min-w-0">
                                  <p className="truncate text-sm font-semibold text-slate-900">{user.fullName || "-"}</p>
                                  <p className="mt-0.5 break-all text-xs text-slate-600">{user.email || user.userId}</p>
                                </div>
                                <div className="flex items-center gap-2">
                                  {!isSuperAdminMember && !isSelfMember ? (
                                    <DisableMemberIconButton
                                      status={user.status}
                                      onToggle={() => setDisableTarget({
                                        userId: user.userId,
                                        email: user.email,
                                        status: user.status,
                                      })}
                                    />
                                  ) : null}
                                  <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold ${roleChipClass(resolvedRole)}`}>
                                    {displayRole(resolvedRole, roleLabels)}
                                  </span>
                                </div>
                              </div>

                              <div className="mt-2 flex flex-wrap items-center gap-1">
                                <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold capitalize ${statusChipClass(user.status)}`}>
                                  {user.status}
                                </span>
                              </div>

                              <div className="mt-3 space-y-2">
                                <div>
                                  <p className="mb-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">App assignments</p>
                                  {assignmentLabels.length ? (
                                    <div className="flex flex-wrap gap-1">
                                      {assignmentLabels.map((label, index) => (
                                        <span
                                          key={label}
                                          className={`rounded-full border px-2 py-0.5 text-[11px] font-medium ${APP_CHIP_STYLES[index % APP_CHIP_STYLES.length]}`}
                                        >
                                          {label}
                                        </span>
                                      ))}
                                    </div>
                                  ) : (
                                    <p className="text-xs text-slate-500">No app assignments.</p>
                                  )}
                                </div>
                              </div>

                              <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-slate-500">
                                <span>Created: {formatDate(user.createdAt)}</span>
                                <span>Updated: {formatDate(user.updatedAt || user.roleUpdatedAt)}</span>
                              </div>
                            </article>
                          );
                        })}
                      </div>
                    </div>
                  </details>
                );
              })}
            </div>
          )}
        </div>
      </SectionCard>

      <Dialog
        open={isEditModalOpen}
        onOpenChange={handleEditModalOpenChange}
      >
        <DialogContent
          className="max-w-[860px] rounded-xl bg-white p-6"
          onEscapeKeyDown={(event) => {
            if (savingEdit || sendingPasswordReset) {
              event.preventDefault();
            }
          }}
          onInteractOutside={(event) => {
            if (
              shouldBlockOutsideClose({
                isBusy: savingEdit || sendingPasswordReset,
                hasUnsavedChanges: hasEditChanges,
              })
            ) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close edit user modal"
            disabled={savingEdit}
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader>
            <DialogTitle>Edit member access</DialogTitle>
            <DialogDescription>
              Access is assigned by tenant, app, and role. Removing an assignment revokes access for that app scope.
            </DialogDescription>
          </DialogHeader>

          {editingUser && editDraft ? (
            <div className="mt-4 space-y-4">
              <LabeledField label="User">
                <ReadOnlyValue
                  value={editingUser.email || editingUser.userId}
                  className="rounded-md border border-slate-200 bg-slate-50"
                />
              </LabeledField>

              <LabeledField label="Full Name">
                <Input
                  value={editDraft.fullName}
                  onChange={(event) => setEditDraft((current) => (current ? { ...current, fullName: event.target.value } : current))}
                  placeholder="Full name"
                  maxLength={255}
                />
              </LabeledField>

              <div className="space-y-2 rounded-lg border border-slate-200 bg-slate-50 p-3">
                <div className="flex items-center justify-between">
                  <p className="text-xs uppercase tracking-[0.16em] text-slate-500">App assignments</p>
                  <ActionIconButton
                    icon={<Plus />}
                    tooltip="Add assignment"
                    onClick={addEditAssignmentRow}
                    disabled={savingEdit}
                  />
                </div>
                {editDraft.assignments.length === 0 ? (
                  <p className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-600">
                    No app assignments selected.
                  </p>
                ) : (
                  <div className="space-y-2">
                    {editDraft.assignments.map((row) => (
                      <div key={row.id} className="grid grid-cols-1 items-center gap-2 rounded-md border border-slate-200 bg-white p-2 sm:grid-cols-2 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(0,0.9fr)_auto]">
                        <div className="min-w-0">
                          <AppDropdown
                            value={row.tenantId}
                            onValueChange={(value) => updateEditAssignmentRow(row.id, { tenantId: value })}
                            options={tenantDropdownOptions}
                            placeholder="Tenant"
                            searchable={false}
                            size="sm"
                          />
                        </div>
                        <div className="min-w-0">
                          <AppDropdown
                            value={row.appId}
                            onValueChange={(value) => updateEditAssignmentRow(row.id, { appId: value })}
                            options={appDropdownOptions}
                            placeholder="App"
                            searchable={false}
                            size="sm"
                          />
                        </div>
                        <div className="min-w-0">
                          <AppDropdown
                            value={row.role}
                            onValueChange={(value) => updateEditAssignmentRow(row.id, { role: value })}
                            options={roleDropdownOptions}
                            placeholder="Role"
                            searchable={false}
                            size="sm"
                          />
                        </div>
                        <ActionIconButton
                          icon={<Trash2 />}
                          tooltip="Remove assignment"
                          onClick={() => removeEditAssignmentRow(row.id)}
                          disabled={savingEdit}
                          className="h-8 w-8 [&_svg]:h-4 [&_svg]:w-4"
                        />
                      </div>
                    ))}
                  </div>
                )}
                <p className="text-xs text-slate-500">
                  Access is assigned by tenant, app, and role. Removing an assignment revokes access for that app scope.
                </p>
              </div>
            </div>
          ) : null}

          {editValidationError ? <p className="mt-3 text-sm text-rose-600">{editValidationError}</p> : null}

          <DialogFooter>
            {editingUser ? (
              <Button
                variant="outline"
                onClick={() => setPasswordResetTarget({ userId: editingUser.userId, email: editingUser.email })}
                disabled={savingEdit || sendingPasswordReset}
              >
                {sendingPasswordReset ? <Spinner className="size-4" /> : <ShieldCheck className="size-4" />}
                {sendingPasswordReset ? "Sending..." : "Send password reset"}
              </Button>
            ) : null}
            {shouldShowSaveButton ? (
              <Button onClick={() => void handleSaveUserEdit()} disabled={savingEdit || !canSubmitEdit}>
                {savingEdit ? <Spinner className="size-4" /> : null}
                {savingEdit ? "Saving..." : "Save Changes"}
              </Button>
            ) : null}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={Boolean(disableTarget)} onOpenChange={(open) => !open && !processingDisable && setDisableTarget(null)}>
        <DialogContent
          className="max-w-[560px]"
          onEscapeKeyDown={(event) => {
            if (processingDisable) {
              event.preventDefault();
            }
          }}
          onInteractOutside={(event) => {
            if (processingDisable) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close account status modal"
            disabled={processingDisable}
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader>
            <DialogTitle>{disableTarget?.status === "disabled" ? "Enable account" : "Disable account"}</DialogTitle>
          </DialogHeader>

          <p className="text-sm text-slate-700">
            {disableTarget?.status === "disabled" ? "Enable" : "Disable"} account for{" "}
            <span className="font-semibold">{disableTarget?.email || disableTarget?.userId}</span>?
          </p>

          <DialogFooter>
            <Button
              className={
                disableTarget?.status === "disabled"
                  ? "border border-emerald-700 bg-emerald-600 text-white hover:bg-emerald-700"
                  : "border border-rose-700 bg-rose-600 text-white hover:bg-rose-700"
              }
              onClick={() => void handleDisableConfirmed()}
              disabled={processingDisable}
            >
              {processingDisable ? <Spinner className="size-4" /> : null}
              {processingDisable
                ? disableTarget?.status === "disabled"
                  ? "Enabling..."
                  : "Disabling..."
                : disableTarget?.status === "disabled"
                  ? "Enable account"
                  : "Disable account"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={Boolean(passwordResetTarget)} onOpenChange={(open) => !open && !sendingPasswordReset && setPasswordResetTarget(null)}>
        <DialogContent
          className="max-w-[560px]"
          onEscapeKeyDown={(event) => {
            if (sendingPasswordReset) {
              event.preventDefault();
            }
          }}
          onInteractOutside={(event) => {
            if (sendingPasswordReset) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close password reset modal"
            disabled={sendingPasswordReset}
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader>
            <DialogTitle>Send password reset</DialogTitle>
          </DialogHeader>

          <p className="text-sm text-slate-700">
            Send a secure password-reset email to <span className="font-semibold">{passwordResetTarget?.email || passwordResetTarget?.userId}</span>?
          </p>
          <p className="text-xs text-slate-500">
            This sends a Supabase recovery email. No password or reset token is exposed in the admin UI.
          </p>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => void handleSendPasswordResetConfirmed()}
              disabled={sendingPasswordReset}
            >
              {sendingPasswordReset ? <Spinner className="size-4" /> : <ShieldCheck className="size-4" />}
              {sendingPasswordReset ? "Sending..." : "Send password reset"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isInviteModalOpen} onOpenChange={handleInviteModalOpenChange}>
        <DialogContent
          className="max-w-[920px] rounded-xl bg-white p-6"
          onEscapeKeyDown={(event) => {
            if (creatingInvite) {
              event.preventDefault();
            }
          }}
          onInteractOutside={(event) => {
            if (
              shouldBlockOutsideClose({
                isBusy: creatingInvite,
                hasUnsavedChanges: hasInviteChanges,
              })
            ) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close create invitation modal"
            disabled={creatingInvite}
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader>
            <DialogTitle>Create invitation</DialogTitle>
            <DialogDescription>
              Create invite access by tenant, app, and role. First invite URL is auto-copied when available.
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 space-y-4">
            <div className="grid gap-3 sm:grid-cols-[minmax(0,1.6fr)_120px]">
              <div>
                <p className="mb-1 text-xs uppercase tracking-[0.16em] text-slate-500">Invite email</p>
                <Input
                  value={inviteEmail}
                  onChange={(event) => setInviteEmail(event.target.value)}
                  placeholder="new.user@company.com"
                  type="email"
                  autoComplete="email"
                />
              </div>
              <div>
                <p className="mb-1 text-xs uppercase tracking-[0.16em] text-slate-500">Expiry hrs</p>
                <Input
                  value={inviteExpirationHours}
                  onChange={(event) => setInviteExpirationHours(event.target.value)}
                  placeholder="72"
                  inputMode="numeric"
                />
              </div>
            </div>

            <div className="space-y-2 rounded-lg border border-slate-200 bg-slate-50 p-3">
              <div className="flex items-center justify-between">
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Invite assignments</p>
                <ActionIconButton
                  icon={<Plus />}
                  tooltip="Add assignment"
                  onClick={addInviteAssignmentRow}
                  disabled={creatingInvite}
                />
              </div>
              {inviteAssignments.length === 0 ? (
                <p className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-600">
                  Add at least one tenant/app/role assignment.
                </p>
              ) : (
                <div className="space-y-2">
                  {inviteAssignments.map((row) => (
                    <div key={row.id} className="grid grid-cols-1 items-center gap-2 rounded-md border border-slate-200 bg-white p-2 sm:grid-cols-2 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(0,0.9fr)_auto]">
                      <AppDropdown
                        value={row.tenantId}
                        onValueChange={(value) => updateInviteAssignmentRow(row.id, { tenantId: value })}
                        options={tenantDropdownOptions}
                        searchable={false}
                        size="sm"
                      />
                      <AppDropdown
                        value={row.appId}
                        onValueChange={(value) => updateInviteAssignmentRow(row.id, { appId: value })}
                        options={appDropdownOptions}
                        searchable={false}
                        size="sm"
                      />
                      <AppDropdown
                        value={row.role}
                        onValueChange={(value) => updateInviteAssignmentRow(row.id, { role: normalizeRoleKey(value) })}
                        options={roleDropdownOptions}
                        searchable={false}
                        size="sm"
                      />
                      <ActionIconButton
                        icon={<Trash2 />}
                        tooltip="Remove assignment"
                        onClick={() => removeInviteAssignmentRow(row.id)}
                        disabled={creatingInvite}
                        className="h-8 w-8 [&_svg]:h-4 [&_svg]:w-4"
                      />
                    </div>
                  ))}
                </div>
              )}
            </div>
            {inviteAssignmentsTouched && inviteValidationError ? <p className="text-sm text-rose-600">{inviteValidationError}</p> : null}
          </div>

          {shouldShowCreateInviteButton ? (
            <DialogFooter>
              <Button
                onClick={() => void handleCreateInvite()}
                disabled={creatingInvite || !inviteEmail.trim() || inviteAssignments.length === 0 || Boolean(inviteValidationError)}
              >
                {creatingInvite ? <Spinner className="size-4" /> : <ShieldCheck className="size-4" />}
                {creatingInvite ? "Creating..." : "Create invite"}
              </Button>
            </DialogFooter>
          ) : null}
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isEditUnsavedDialogOpen}
        onKeepEditing={() => setIsEditUnsavedDialogOpen(false)}
        onDiscardChanges={() => {
          setIsEditUnsavedDialogOpen(false);
          closeEditUser();
        }}
      />

      <UnsavedChangesDialog
        open={isInviteUnsavedDialogOpen}
        onKeepEditing={() => setIsInviteUnsavedDialogOpen(false)}
        onDiscardChanges={() => {
          setIsInviteUnsavedDialogOpen(false);
          setIsInviteModalOpen(false);
          resetInviteDraft();
        }}
      />

      <PageLoadingLayer
        active={loadingContract.pageOverlayActive}
        message={loadingContract.pageOverlayMessage}
      />
    </AppPageLayout>
  );
}
