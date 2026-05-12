import { Copy, Loader2, Plus, ShieldCheck, Trash2, UserMinus, X } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";
import { LabeledField, ReadOnlyValue } from "@/components/dashboard/FormFieldRow";
import { PageBanner } from "@/components/layout/PageBanner";
import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import {
  DialogClose,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { useApiRequest } from "@/hooks/useApiRequest";
import { readBrowserCacheSnapshot, writeBrowserCache } from "@/lib/browserCache";
import { Tooltip } from "@shared/components/actions/Tooltip";

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
};

type DisableTarget = {
  userId: string;
  email: string | null;
};

type EditAssignmentRow = {
  id: string;
  tenantId: string;
  appId: string;
  role: string;
};

type EditDraft = {
  fullName: string;
  tenantIds: string[];
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

const ROLE_GROUP_ORDER = [
  "workspace.super_admin",
  "tradsphere.admin",
  "tradsphere.editor",
  "tradsphere.viewer",
  "other",
] as const;

const ROLE_GROUP_LABEL: Record<(typeof ROLE_GROUP_ORDER)[number], string> = {
  "workspace.super_admin": "Super Admin",
  "tradsphere.admin": "Admin",
  "tradsphere.editor": "Editor",
  "tradsphere.viewer": "Viewer",
  other: "Other / Unknown",
};

const ADMIN_PAGE_CACHE_KEY = "admin-users:page:v1";
const ADMIN_PAGE_CACHE_TTL_MS = 2 * 60 * 1000;
const EDIT_MODAL_CLEAR_DELAY_MS = 360;
const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";

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

function readSidebarCollapsedState(): boolean {
  if (typeof window === "undefined") {
    return false;
  }

  try {
    const nextValue = window.localStorage.getItem(SIDEBAR_COLLAPSED_STORAGE_KEY);
    if (nextValue !== null) {
      return nextValue === "1";
    }
    return window.localStorage.getItem(LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

const TENANT_CHIP_STYLES = [
  "border-indigo-200 bg-indigo-50 text-indigo-700",
  "border-emerald-200 bg-emerald-50 text-emerald-700",
  "border-amber-200 bg-amber-50 text-amber-700",
  "border-cyan-200 bg-cyan-50 text-cyan-700",
] as const;

const APP_CHIP_STYLES = [
  "border-blue-200 bg-blue-50 text-blue-700",
  "border-fuchsia-200 bg-fuchsia-50 text-fuchsia-700",
  "border-teal-200 bg-teal-50 text-teal-700",
  "border-orange-200 bg-orange-50 text-orange-700",
] as const;

function assignmentIdentity(item: { tenantId: string; appId: string; role: string }) {
  return `${item.tenantId}::${item.appId}::${item.role}`;
}

function normalizeDraftComparable(draft: EditDraft): {
  fullName: string;
  tenantIds: string[];
  assignments: string[];
} {
  const tenantIds = unique(draft.tenantIds).sort((a, b) => a.localeCompare(b));
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
    tenantIds,
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

function DisableMemberIconButton({
  disabled,
  onDisable,
}: {
  disabled: boolean;
  onDisable: () => void;
}) {
  const anchorRef = useRef<HTMLButtonElement | null>(null);
  const [tooltipOpen, setTooltipOpen] = useState(false);

  return (
    <>
      <button
        ref={anchorRef}
        type="button"
        onClick={(event) => {
          if (disabled) {
            return;
          }
          event.stopPropagation();
          onDisable();
        }}
        onMouseEnter={() => setTooltipOpen(true)}
        onMouseLeave={() => setTooltipOpen(false)}
        onFocus={() => setTooltipOpen(true)}
        onBlur={() => setTooltipOpen(false)}
        className={`inline-flex h-7 w-7 items-center justify-center rounded-full border border-slate-300 bg-white text-slate-600 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-rose-300 ${
          disabled
            ? "pointer-events-none cursor-default opacity-50"
            : "hover:border-rose-300 hover:bg-rose-50 hover:text-rose-700"
        }`}
        aria-label={disabled ? "Access already disabled" : "Disable access"}
      >
        <UserMinus className="size-3.5" />
      </button>
      <Tooltip
        open={tooltipOpen}
        anchorRef={anchorRef}
        text={disabled ? "Already disabled" : "Disable access"}
      />
    </>
  );
}

function roleRank(roleKey: string | null): number {
  const normalized = String(roleKey || "").trim();
  if (normalized === "workspace.super_admin") {
    return 0;
  }
  if (normalized === "tradsphere.admin") {
    return 1;
  }
  if (normalized === "tradsphere.editor") {
    return 2;
  }
  if (normalized === "tradsphere.viewer") {
    return 3;
  }
  return 4;
}

function roleGroupKey(roleKey: string | null): (typeof ROLE_GROUP_ORDER)[number] {
  const normalized = String(roleKey || "").trim();
  if (normalized === "workspace.super_admin") {
    return "workspace.super_admin";
  }
  if (normalized === "tradsphere.admin") {
    return "tradsphere.admin";
  }
  if (normalized === "tradsphere.editor") {
    return "tradsphere.editor";
  }
  if (normalized === "tradsphere.viewer") {
    return "tradsphere.viewer";
  }
  return "other";
}

function displayRole(roleKey: string | null, roleLabels: Record<string, string>): string {
  const normalized = String(roleKey || "").trim();
  if (!normalized) {
    return "Unknown";
  }
  return roleLabels[normalized] || normalized;
}

function summarizeUserRole(user: AdminUser): string | null {
  const roles = unique(
    user.appAssignments
      .map((item) => String(item.role || "").trim())
      .filter((item) => Boolean(item)),
  );
  if (roles.length === 0) {
    return user.role;
  }
  return roles.sort((a, b) => roleRank(a) - roleRank(b))[0] || user.role;
}

function buildInitialDraft(user: AdminUser): EditDraft {
  const tenantIds = unique(
    user.tenantMemberships
      .filter((item) => item.status !== "disabled")
      .map((item) => item.tenantId),
  );
  const assignments = user.appAssignments
    .filter((item) => item.role && item.tenantId && item.appId)
    .map((item, index) => ({
      id: `${user.userId}-${index}-${item.tenantId}-${item.appId}`,
      tenantId: item.tenantId,
      appId: item.appId,
      role: String(item.role || "").trim(),
    }));
  return {
    fullName: user.fullName || "",
    tenantIds,
    assignments,
  };
}

function tenantDisplayName(tenant: TenantItem): string {
  return tenant.name || tenant.slug || tenant.id;
}

function appDisplayName(app: AppItem): string {
  return app.name || app.code || app.id;
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
    { key: "tradsphere.viewer", label: "Viewer", assignable: true },
    { key: "tradsphere.editor", label: "Editor", assignable: true },
    { key: "tradsphere.admin", label: "Admin", assignable: true },
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
  return apps
    .map((app) => ({
      value: app.id,
      label: appDisplayName(app),
      keywords: `${app.code} ${app.id}`,
    }))
    .sort((a, b) => a.label.localeCompare(b.label));
}

export default function AdminUsersPage() {
  const { requestJson } = useApiRequest();

  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);

  const [users, setUsers] = useState<AdminUser[]>([]);
  const [invitations, setInvitations] = useState<AdminInvitation[]>([]);
  const [roles, setRoles] = useState<RoleItem[]>([]);
  const [tenants, setTenants] = useState<TenantItem[]>([]);
  const [apps, setApps] = useState<AppItem[]>([]);
  const [scope, setScope] = useState<{ isSuperAdmin?: boolean } | null>(null);

  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState("tradsphere.viewer");
  const [inviteTenantIds, setInviteTenantIds] = useState<string[]>([]);
  const [inviteAppIds, setInviteAppIds] = useState<string[]>([]);
  const [inviteExpirationHours, setInviteExpirationHours] = useState("72");
  const [creatingInvite, setCreatingInvite] = useState(false);

  const [editingUser, setEditingUser] = useState<AdminUser | null>(null);
  const [editDraft, setEditDraft] = useState<EditDraft | null>(null);
  const [originalEditDraft, setOriginalEditDraft] = useState<EditDraft | null>(null);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [savingEdit, setSavingEdit] = useState(false);
  const editModalClearTimerRef = useRef<number | null>(null);

  const [disableTarget, setDisableTarget] = useState<DisableTarget | null>(null);
  const [processingDisable, setProcessingDisable] = useState(false);
  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());

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

    if (nextTenants.length && inviteTenantIds.length === 0) {
      setInviteTenantIds([nextTenants[0].id]);
    }
    if (nextApps.length && inviteAppIds.length === 0) {
      const tradsphereApp = nextApps.find((item) => String(item.code || "").trim().toLowerCase() === "tradsphere");
      setInviteAppIds([tradsphereApp?.id || nextApps[0].id]);
    }
    if (!nextRoles.some((role) => role.key === inviteRole)) {
      const fallback = nextRoles.find((role) => role.assignable);
      if (fallback) {
        setInviteRole(fallback.key);
      }
    }
  }, [inviteAppIds.length, inviteRole, inviteTenantIds.length]);

  const roleLabels = useMemo(() => {
    const map: Record<string, string> = {};
    for (const role of roles) {
      map[role.key] = role.label;
    }
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

  const groupedMembers = useMemo(() => {
    const groups: Record<(typeof ROLE_GROUP_ORDER)[number], AdminUser[]> = {
      "workspace.super_admin": [],
      "tradsphere.admin": [],
      "tradsphere.editor": [],
      "tradsphere.viewer": [],
      other: [],
    };
    for (const user of users) {
      const role = summarizeUserRole(user);
      const key = roleGroupKey(role);
      groups[key].push(user);
    }
    for (const key of ROLE_GROUP_ORDER) {
      groups[key].sort((a, b) => String(a.email || a.userId).localeCompare(String(b.email || b.userId)));
    }
    return groups;
  }, [users]);

  const loadData = useCallback(async (showRefreshing: boolean) => {
    const cachedSnapshot = !showRefreshing
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

    if (showRefreshing) {
      setRefreshing(true);
    } else {
      setLoading(true);
      if (canUseCachedData && cachedData) {
        applyLoadedData(cachedData);
        setCacheStatus({
          source: "cache",
          fetchedAt: cachedSnapshot?.fetchedAt ?? Date.now(),
        });
        setLoading(false);
      }
    }
    setError(null);

    try {
      const [usersPayload, invitesPayload, rolesPayload, tenantsPayload, appsPayload] = await Promise.all([
        requestJson("/api/auth/v1/admin/users", { successToast: false, errorToast: false }),
        requestJson("/api/auth/v1/admin/invitations?status=pending", { successToast: false, errorToast: false }),
        requestJson("/api/auth/v1/admin/roles", { successToast: false, errorToast: false }),
        requestJson("/api/auth/v1/admin/tenants", { successToast: false, errorToast: false }),
        requestJson("/api/auth/v1/admin/apps", { successToast: false, errorToast: false }),
      ]);

      const usersData = unwrap<{ items?: AdminUser[]; scope?: { isSuperAdmin?: boolean } }>(usersPayload, {});
      const invitesData = unwrap<{ items?: AdminInvitation[] }>(invitesPayload, {});
      const rolesData = unwrap<{ items?: RoleItem[]; roles?: string[] }>(rolesPayload, {});
      const tenantsData = unwrap<{ items?: TenantItem[] }>(tenantsPayload, {});
      const appsData = unwrap<{ items?: AppItem[] }>(appsPayload, {});
      const nextRoles = Array.isArray(rolesData.items)
        ? rolesData.items
        : (Array.isArray(rolesData.roles) ? rolesData.roles.map((key) => ({ key, label: key, assignable: true })) : []);

      const nextData: AdminPageCacheSnapshot = {
        users: Array.isArray(usersData.items) ? usersData.items : [],
        invitations: Array.isArray(invitesData.items) ? invitesData.items : [],
        roles: nextRoles,
        tenants: Array.isArray(tenantsData.items) ? tenantsData.items : [],
        apps: Array.isArray(appsData.items) ? appsData.items : [],
        scope: usersData.scope || null,
      };
      applyLoadedData(nextData);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });
      writeBrowserCache(ADMIN_PAGE_CACHE_KEY, nextData, ADMIN_PAGE_CACHE_TTL_MS, {
        source: "network",
        fetchedAt: Date.now(),
      });
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "Unable to load admin data.";
      if (!canUseCachedData) {
        setError(message);
        setCacheStatus(null);
      }
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [applyLoadedData, requestJson]);

  useEffect(() => {
    void loadData(false);
  }, [loadData]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleStorage = () => {
      const collapsed = readSidebarCollapsedState();
      setSidebarVisuallyExpanded(!collapsed);
    };
    const handleSidebarEvent = (event: Event) => {
      const customEvent = event as CustomEvent<{ collapsed?: boolean; visuallyExpanded?: boolean }>;
      if (typeof customEvent.detail?.visuallyExpanded === "boolean") {
        setSidebarVisuallyExpanded(customEvent.detail.visuallyExpanded);
        return;
      }
      if (typeof customEvent.detail?.collapsed === "boolean") {
        setSidebarVisuallyExpanded(!customEvent.detail.collapsed);
        return;
      }
      const collapsed = readSidebarCollapsedState();
      setSidebarVisuallyExpanded(!collapsed);
    };

    window.addEventListener("storage", handleStorage);
    window.addEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    return () => {
      window.removeEventListener("storage", handleStorage);
      window.removeEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    };
  }, []);

  function openEditUser(user: AdminUser) {
    if (editModalClearTimerRef.current !== null && typeof window !== "undefined") {
      window.clearTimeout(editModalClearTimerRef.current);
      editModalClearTimerRef.current = null;
    }
    const draft = buildInitialDraft(user);
    setEditingUser(user);
    setEditDraft(draft);
    setOriginalEditDraft(draft);
    setIsEditModalOpen(true);
  }

  function closeEditUser() {
    if (savingEdit) {
      return;
    }
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
      const tenantId = current.tenantIds[0] || tenants[0]?.id || "";
      const appId = inviteAppIds[0] || apps[0]?.id || "";
      const role = assignableRoles.find((item) => item.key === "tradsphere.viewer")?.key
        || assignableRoles[0]?.key
        || "tradsphere.viewer";
      return {
        ...current,
        assignments: [
          ...current.assignments,
          {
            id: `row-${Date.now()}-${Math.random().toString(36).slice(2)}`,
            tenantId,
            appId,
            role,
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

  async function handleSaveUserEdit() {
    if (!editingUser || !editDraft) {
      return;
    }
    const normalizedTenantIds = unique(editDraft.tenantIds.map((tenantId) => String(tenantId || "").trim()).filter((tenantId) => Boolean(tenantId)));
    const validAssignments = editDraft.assignments
      .map((item) => {
        const tenantId = String(item.tenantId || "").trim();
        const appId = String(item.appId || "").trim();
        return {
          tenantId,
          appId,
          role: String(item.role || "").trim(),
        };
      })
      .filter((item) => item.tenantId && item.appId && item.role);

    setSavingEdit(true);
    try {
      await requestJson(`/api/auth/v1/admin/users/${encodeURIComponent(editingUser.userId)}/access`, {
        method: "PATCH",
        body: {
          fullName: editDraft.fullName.trim() || undefined,
          email: editingUser.email || undefined,
          tenantMemberships: normalizedTenantIds.map((tenantId) => ({
            tenantId,
            status: "active",
          })),
          appAssignments: validAssignments,
        },
        successToast: {
          title: "Member updated",
          message: `${editingUser.email || editingUser.userId} access has been updated.`,
        },
      });
      closeEditUser();
      await loadData(true);
    } finally {
      setSavingEdit(false);
    }
  }

  async function handleCreateInvite() {
    const normalizedEmail = inviteEmail.trim().toLowerCase();
    const normalizedTenantIds = unique(inviteTenantIds.map((tenantId) => String(tenantId || "").trim()).filter((tenantId) => Boolean(tenantId)));
    const normalizedAppIds = unique(inviteAppIds.map((appId) => String(appId || "").trim()).filter((appId) => Boolean(appId)));
    if (!normalizedEmail || normalizedTenantIds.length === 0 || normalizedAppIds.length === 0) {
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
          tenantIds: normalizedTenantIds,
          appIds: normalizedAppIds,
          role: inviteRole,
          expirationHours: Math.floor(expiresHours),
        },
        successToast: {
          title: "Invitation created",
          message: `Invite package for ${normalizedEmail} is ready.`,
        },
      });
      const data = unwrap<{ items?: Array<{ inviteUrl?: string }> }>(payload, {});
      const firstInviteUrl = String(data.items?.[0]?.inviteUrl || "").trim();
      if (firstInviteUrl) {
        await navigator.clipboard.writeText(firstInviteUrl).catch(() => undefined);
      }
      setInviteEmail("");
      await loadData(true);
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
    await loadData(true);
  }

  async function handleDisableConfirmed() {
    if (!disableTarget) {
      return;
    }

    setProcessingDisable(true);
    try {
      await requestJson(`/api/auth/v1/admin/users/${encodeURIComponent(disableTarget.userId)}`, {
        method: "PATCH",
        body: {
          status: "disabled",
        },
        successToast: {
          title: "Access disabled",
          message: `${disableTarget.email || disableTarget.userId} no longer has active tenant access in this scope.`,
        },
      });
      setDisableTarget(null);
      closeEditUser();
      await loadData(true);
    } finally {
      setProcessingDisable(false);
    }
  }

  const cacheStatusText = refreshing
    ? "Refreshing..."
    : cacheStatus
      ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : "No cached data yet";
  const showCacheChip = !loading && !error && !isEditModalOpen && !disableTarget;
  const editValidationError = useMemo(() => {
    if (!editDraft) {
      return null;
    }
    const normalizedTenantIds = unique(editDraft.tenantIds.map((tenantId) => String(tenantId || "").trim()));
    if (normalizedTenantIds.some((tenantId) => !tenantId)) {
      return "One or more selected tenants are unavailable in current scope.";
    }
    const seen = new Set<string>();
    for (const row of editDraft.assignments) {
      const tenantId = String(row.tenantId || "").trim();
      const appId = String(row.appId || "").trim();
      const role = String(row.role || "").trim();
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

  const canSubmitEdit = Boolean(editingUser && editDraft && hasEditChanges && !editValidationError);
  const shouldShowSaveButton = canSubmitEdit || savingEdit;

  return (
    <div className="mx-auto flex min-h-[100dvh] w-full max-w-[1680px] flex-col gap-6">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="Admin Users"
        description="Manage tenant memberships, app assignments, roles, and invitations."
        gradientVariant="admin"
      />

      {error ? (
        <section className="rounded-2xl border border-rose-200 bg-rose-50 px-5 py-4 text-sm text-rose-700">
          {error}
        </section>
      ) : null}

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <h2 className="text-base font-semibold text-slate-900">Invitations</h2>
            <p className="text-sm text-slate-600">Create invites and manage pending invitation links in one place.</p>
          </div>
        </div>

        <div className="mt-4 grid gap-3 lg:grid-cols-[minmax(220px,1.4fr)_minmax(170px,0.8fr)_minmax(220px,1fr)_minmax(220px,1fr)_130px_auto]">
          <div>
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Invite email</p>
            <Input
              value={inviteEmail}
              onChange={(event) => setInviteEmail(event.target.value)}
              placeholder="new.user@company.com"
              type="email"
              autoComplete="email"
            />
          </div>
          <div>
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Role</p>
            <AppDropdown
              value={inviteRole}
              onValueChange={setInviteRole}
              options={roleDropdownOptions}
              searchable={false}
            />
          </div>
          <div>
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Tenants</p>
            <AppDropdown
              value=""
              onValueChange={() => undefined}
              multiple
              values={inviteTenantIds}
              onValuesChange={setInviteTenantIds}
              options={tenantDropdownOptions}
              placeholder="Select tenants"
              searchable={false}
            />
          </div>
          <div>
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Apps</p>
            <AppDropdown
              value=""
              onValueChange={() => undefined}
              multiple
              values={inviteAppIds}
              onValuesChange={setInviteAppIds}
              options={appDropdownOptions}
              placeholder="Select apps"
              searchable={false}
            />
          </div>
          <div>
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Expiry hrs</p>
            <Input
              value={inviteExpirationHours}
              onChange={(event) => setInviteExpirationHours(event.target.value)}
              placeholder="72"
              inputMode="numeric"
            />
          </div>
          <div className="flex items-end">
            <Button
              onClick={() => void handleCreateInvite()}
              disabled={creatingInvite || !inviteEmail.trim() || inviteTenantIds.length === 0 || inviteAppIds.length === 0}
            >
              {creatingInvite ? <Spinner className="size-4" /> : <ShieldCheck className="size-4" />}
              {creatingInvite ? "Creating..." : "Create invite"}
            </Button>
          </div>
        </div>
        <p className="mt-3 text-sm text-slate-600">First invite URL is auto-copied when available. Current schema creates one token per tenant/app assignment.</p>
        <div className="mt-8 flex items-center justify-between gap-2">
          <h3 className="text-base font-semibold text-slate-900">Pending invitations</h3>
          <p className="text-sm text-slate-500">{pendingInvitations.length} pending</p>
        </div>

        {pendingInvitations.length === 0 ? (
          <p className="mt-2 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
            No pending invitations.
          </p>
        ) : (
          <div className="mt-2 overflow-x-auto">
            <table className="w-full min-w-[1040px] border-separate border-spacing-y-2 text-sm">
              <thead>
                <tr className="text-left text-xs uppercase tracking-[0.14em] text-slate-500">
                  <th className="px-3 py-2">Email</th>
                  <th className="px-3 py-2">Tenant</th>
                  <th className="px-3 py-2">App</th>
                  <th className="px-3 py-2">Role</th>
                  <th className="px-3 py-2">Status</th>
                  <th className="px-3 py-2">Expires</th>
                  <th className="px-3 py-2">Actions</th>
                </tr>
              </thead>
              <tbody>
                {pendingInvitations.map((invite) => (
                  <tr key={invite.id} className="bg-slate-50 text-slate-700">
                    <td className="rounded-l-xl border border-r-0 border-slate-200 px-3 py-3">{invite.email}</td>
                    <td className="border border-l-0 border-r-0 border-slate-200 px-3 py-3">
                      {invite.tenantName || invite.tenantSlug || invite.tenantId}
                    </td>
                    <td className="border border-l-0 border-r-0 border-slate-200 px-3 py-3">
                      {invite.appName || invite.appCode || invite.appId}
                    </td>
                    <td className="border border-l-0 border-r-0 border-slate-200 px-3 py-3">
                      {displayRole(invite.role, roleLabels)}
                    </td>
                    <td className="border border-l-0 border-r-0 border-slate-200 px-3 py-3 capitalize">{invite.status}</td>
                    <td className="border border-l-0 border-r-0 border-slate-200 px-3 py-3">{formatDate(invite.expiresAt)}</td>
                    <td className="rounded-r-xl border border-l-0 border-slate-200 px-3 py-3">
                      <div className="flex gap-2">
                        <Button
                          variant="outline"
                          onClick={() => void navigator.clipboard.writeText(String(invite.inviteUrl || "")).catch(() => undefined)}
                          disabled={!invite.inviteUrl}
                        >
                          <Copy className="size-4" />
                          Copy
                        </Button>
                        <Button
                          variant="secondary"
                          onClick={() => void handleRevokeInvite(invite.id, invite.email)}
                        >
                          Revoke
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <h2 className="text-base font-semibold text-slate-900">Members</h2>
            <p className="text-sm text-slate-600">
              Members are grouped by role type. Edit assignments by tenant/app and disable access without deleting auth users.
            </p>
          </div>
          <p className="text-sm text-slate-500">
            {users.length} users
            {scope?.isSuperAdmin ? " · super-admin scope" : ""}
          </p>
        </div>

        <div className="relative mt-4">
          {users.length === 0 ? (
            <p className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
              No users found in this admin scope.
            </p>
          ) : (
            <div className="space-y-5">
              {ROLE_GROUP_ORDER.map((groupKey) => {
                const groupUsers = groupedMembers[groupKey] || [];
                if (groupUsers.length === 0) {
                  return null;
                }
                return (
                  <div key={groupKey}>
                    <div className="mb-2 flex items-center justify-between">
                      <h3 className="text-sm font-semibold text-slate-800">{ROLE_GROUP_LABEL[groupKey]}</h3>
                      <p className="text-xs uppercase tracking-[0.14em] text-slate-500">{groupUsers.length}</p>
                    </div>
                    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                      {groupUsers.map((user) => {
                        const resolvedRole = summarizeUserRole(user);
                        const tenantLabels = unique(
                          user.tenantMemberships
                            .filter((item) => item.status !== "disabled")
                            .map((item) => item.tenantName || item.tenantSlug || tenantNameById[item.tenantId] || item.tenantId),
                        );
                        const assignmentLabels = unique(
                          user.appAssignments.map((item) => {
                            const appLabel = item.appName || item.appCode || appNameById[item.appId] || item.appId;
                            const roleLabel = displayRole(item.role, roleLabels);
                            const tenantLabel = item.tenantName || item.tenantSlug || tenantNameById[item.tenantId] || item.tenantId;
                            return `${tenantLabel}: ${appLabel} (${roleLabel})`;
                          }),
                        );
                        return (
                          <div
                            key={user.userId}
                            role="button"
                            tabIndex={0}
                            onClick={() => openEditUser(user)}
                            onKeyDown={(event) => {
                              if (event.key === "Enter" || event.key === " ") {
                                event.preventDefault();
                                openEditUser(user);
                              }
                            }}
                            className="cursor-pointer rounded-xl border border-slate-200 bg-white p-4 shadow-sm transition hover:border-blue-200 hover:bg-blue-50/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300"
                          >
                              <div className="flex flex-wrap items-start justify-between gap-2">
                                <div>
                                  <p className="text-base font-semibold text-slate-900">{user.fullName || "-"}</p>
                                  <p className="text-sm text-slate-500">{user.email || user.userId}</p>
                                </div>
                              <div className="flex items-center gap-2">
                                {resolvedRole !== "workspace.super_admin" ? (
                                  <DisableMemberIconButton
                                    disabled={user.status === "disabled"}
                                    onDisable={() => setDisableTarget({ userId: user.userId, email: user.email })}
                                  />
                                ) : null}
                                <span className="rounded-full border border-blue-200 bg-blue-50 px-2 py-1 text-xs font-medium text-blue-800">
                                  {displayRole(resolvedRole, roleLabels)}
                                </span>
                              </div>
                              </div>

                              <div className="mt-2 grid gap-x-3 gap-y-2 text-sm text-slate-700 sm:grid-cols-2">
                                <div>
                                  <p className="mb-1 text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Tenants</p>
                                  {tenantLabels.length ? (
                                    <div className="flex flex-wrap gap-1">
                                      {tenantLabels.map((label, index) => (
                                        <span
                                          key={label}
                                          className={`rounded-full border px-2 py-0.5 text-xs font-medium ${TENANT_CHIP_STYLES[index % TENANT_CHIP_STYLES.length]}`}
                                        >
                                          {label}
                                        </span>
                                      ))}
                                    </div>
                                  ) : (
                                    <p>-</p>
                                  )}
                                </div>
                                <div>
                                  <p className="mb-1 text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Status</p>
                                  <span className={`inline-flex rounded-full border px-2 py-0.5 text-xs font-semibold capitalize ${statusChipClass(user.status)}`}>
                                    {user.status}
                                  </span>
                                  <p className="mt-1 text-xs text-slate-500">Created: {formatDate(user.createdAt)}</p>
                                  <p className="text-xs text-slate-500">Updated: {formatDate(user.updatedAt || user.roleUpdatedAt)}</p>
                                </div>
                              </div>

                              <div className="mt-3">
                                <p className="mb-1 text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Apps</p>
                                {assignmentLabels.length ? (
                                  <div className="flex flex-wrap gap-1">
                                    {assignmentLabels.map((label, index) => (
                                      <span
                                        key={label}
                                        className={`rounded-full border px-2 py-0.5 text-xs font-medium ${APP_CHIP_STYLES[index % APP_CHIP_STYLES.length]}`}
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
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {loading ? (
            <div className="absolute inset-0 z-20 flex items-center justify-center rounded-xl bg-white/70 backdrop-blur-[1px]">
              <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft">
                <Loader2 className="size-4 animate-spin text-blue-600" />
                <span>Loading access...</span>
              </div>
            </div>
          ) : null}
        </div>
      </section>

      <Dialog
        open={isEditModalOpen}
        onOpenChange={(open) => {
          if (!open) {
            closeEditUser();
          } else {
            setIsEditModalOpen(true);
          }
        }}
      >
        <DialogContent className="max-w-[860px] rounded-xl bg-white p-6">
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
              Assign tenants, apps, and roles for this user. Saving applies tenant/app access updates in backend scope.
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

              <LabeledField label="Tenants" alignStart>
                <AppDropdown
                  value=""
                  onValueChange={() => undefined}
                  multiple
                  values={editDraft.tenantIds}
                  onValuesChange={(values) => setEditDraft((current) => (current ? { ...current, tenantIds: unique(values) } : current))}
                  options={tenantDropdownOptions}
                  placeholder="Select tenant memberships"
                  searchable={false}
                />
                <p className="mt-1 text-xs text-slate-500">Removing a tenant here disables membership for that tenant in managed scope.</p>
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
                            searchable={false}
                            size="sm"
                          />
                        </div>
                        <div className="min-w-0">
                          <AppDropdown
                            value={row.appId}
                            onValueChange={(value) => updateEditAssignmentRow(row.id, { appId: value })}
                            options={appDropdownOptions}
                            searchable={false}
                            size="sm"
                          />
                        </div>
                        <div className="min-w-0">
                          <AppDropdown
                            value={row.role}
                            onValueChange={(value) => updateEditAssignmentRow(row.id, { role: value })}
                            options={roleDropdownOptions}
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
              </div>
            </div>
          ) : null}

          {editValidationError ? <p className="mt-3 text-sm text-rose-600">{editValidationError}</p> : null}

          {shouldShowSaveButton ? (
            <DialogFooter>
              <Button onClick={() => void handleSaveUserEdit()} disabled={savingEdit || !canSubmitEdit}>
                {savingEdit ? <Spinner className="size-4" /> : null}
                {savingEdit ? "Saving..." : "Save Changes"}
              </Button>
            </DialogFooter>
          ) : null}
        </DialogContent>
      </Dialog>

      <Dialog open={Boolean(disableTarget)} onOpenChange={(open) => !open && !processingDisable && setDisableTarget(null)}>
        <DialogContent className="max-w-[560px]">
          <DialogHeader>
            <DialogTitle>Disable tenant access</DialogTitle>
          </DialogHeader>

          <p className="text-sm text-slate-700">
            Disable access for <span className="font-semibold">{disableTarget?.email || disableTarget?.userId}</span>?
          </p>

          <DialogFooter>
            <Button
              className="bg-rose-600 text-white hover:bg-rose-700 border border-rose-700"
              onClick={() => void handleDisableConfirmed()}
              disabled={processingDisable}
            >
              {processingDisable ? <Spinner className="size-4" /> : null}
              {processingDisable ? "Disabling..." : "Disable access"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {showCacheChip ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40">
          <div className={`mx-4 sm:mx-6 lg:mr-6 ${sidebarVisuallyExpanded ? "lg:ml-[18.75rem]" : "lg:ml-[6.5rem]"}`}>
            <div className="mx-auto w-full max-w-[1600px]">
              <CacheStatusChip
                text={cacheStatusText}
                onRefresh={() => void loadData(true)}
                disabled={refreshing || loading}
                refreshing={refreshing}
                refreshLabel="Refresh admin data"
                tooltipText="Click to refresh admin users, invitations, tenants, apps, and roles"
                containerClassName="pointer-events-auto"
                className="max-w-[min(90vw,38rem)]"
              />
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
