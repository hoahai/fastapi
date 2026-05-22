import { RefreshCw, Search, UserMinus, UserPlus, X } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { PageBanner } from "@shell/components/layout/PageBanner";
import { AppDropdown } from "@tradsphere/components/ui/app-dropdown";
import { Button } from "@tradsphere/components/ui/button";
import { CacheStatusChip } from "@tradsphere/components/ui/cache-status-chip";
import {
  DialogClose,
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@tradsphere/components/ui/dialog";
import { Input } from "@tradsphere/components/ui/input";
import { Spinner } from "@tradsphere/components/ui/spinner";
import { useApiRequest } from "@tradsphere/hooks/useApiRequest";
import { useOnlineStatus } from "@tradsphere/hooks/useOnlineStatus";
import { readBrowserCacheSnapshot, writeBrowserCache } from "@tradsphere/lib/browserCache";
import { TradspherePermissionDetailsSection } from "@tradsphere/components/admin/TradspherePermissionDetailsSection";
import { resolveAppScopedAdminSections } from "@shell/admin/appScopedAdminConfig";
import { SectionCard } from "@shared/components";
import { Tooltip } from "@shared/components/actions/Tooltip";
import { roleLabel } from "@shared/auth/accessAssignments";
import { useAuth } from "@shared/auth/useAuth";

type ScopedUser = {
  userId: string;
  email: string | null;
  fullName: string | null;
  firstName: string | null;
  lastName: string | null;
  status: string;
  role: string | null;
  isSuperAdmin?: boolean;
  assignedInScope: boolean;
};

type ScopedUsersResponse = {
  items?: ScopedUser[];
};

type ScopedUsersCacheSnapshot = {
  items: ScopedUser[];
};

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type RoleOption = {
  value: "viewer" | "editor" | "admin";
  label: string;
};

type AppScopedAdminPageProps = {
  appCode: string;
  appName: string;
};

type RemoveAccessTarget = {
  userId: string;
  email: string | null;
  fullName: string | null;
};

const ROLE_OPTIONS: RoleOption[] = [
  { value: "viewer", label: "Viewer" },
  { value: "editor", label: "Editor" },
  { value: "admin", label: "Admin" },
];
const APP_SCOPED_ADMIN_CACHE_TTL_MS = 2 * 60 * 1000;
const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";

function unwrap<T>(payload: unknown, fallback: T): T {
  if (payload && typeof payload === "object" && "data" in (payload as Record<string, unknown>)) {
    return ((payload as { data?: T }).data ?? fallback);
  }
  return (payload as T) ?? fallback;
}

function isEmailValid(value: string): boolean {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return false;
  }
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized);
}

function statusChipClass(status: string): string {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "disabled") {
    return "border-rose-200 bg-rose-50 text-rose-700";
  }
  if (normalized === "pending") {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  return "border-emerald-200 bg-emerald-50 text-emerald-700";
}

function formatStatusChipLabel(status: string): string {
  const normalized = String(status || "").trim().toLowerCase();
  if (!normalized) {
    return "Unknown";
  }
  return normalized
    .replace(/[_-]+/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
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

function normalizeRoleKey(role: string | null | undefined): string {
  return String(role || "").trim().toLowerCase();
}

function roleRank(role: string | null | undefined): number {
  const normalized = normalizeRoleKey(role);
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
  return 99;
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

function buildScopedAdminCacheKey(params: { userId: string; appCode: string; tenantSlug: string }): string {
  const userKey = String(params.userId || "anonymous").trim() || "anonymous";
  const appKey = String(params.appCode || "app").trim().toLowerCase() || "app";
  const tenantKey = String(params.tenantSlug || "default").trim().toLowerCase() || "default";
  return `app-scoped-admin:${tenantKey}:${appKey}:${userKey}:v1`;
}

function RemoveAccessIconButton({
  onRemove,
  disabled,
}: {
  onRemove: () => void;
  disabled: boolean;
}) {
  const anchorRef = useRef<HTMLButtonElement | null>(null);
  const [tooltipOpen, setTooltipOpen] = useState(false);

  return (
    <>
      <button
        ref={anchorRef}
        type="button"
        onClick={(event) => {
          event.stopPropagation();
          onRemove();
        }}
        onMouseEnter={() => setTooltipOpen(true)}
        onMouseLeave={() => setTooltipOpen(false)}
        onFocus={() => setTooltipOpen(true)}
        onBlur={() => setTooltipOpen(false)}
        disabled={disabled}
        className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-slate-300 bg-white text-slate-600 transition-colors hover:border-rose-300 hover:bg-rose-50 hover:text-rose-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-rose-300 disabled:pointer-events-none disabled:opacity-60"
        aria-label="Remove access"
      >
        <UserMinus className="size-3.5" />
      </button>
      <Tooltip
        open={tooltipOpen}
        anchorRef={anchorRef}
        text="Remove access"
      />
    </>
  );
}

export default function AppScopedAdminPage({ appCode, appName }: AppScopedAdminPageProps) {
  const auth = useAuth();
  const { requestJson } = useApiRequest();
  const { isOnline } = useOnlineStatus();
  const loadInvocationRef = useRef(0);

  const [users, setUsers] = useState<ScopedUser[]>([]);
  const [loadingUsers, setLoadingUsers] = useState(true);
  const [refreshingUsers, setRefreshingUsers] = useState(false);
  const [backgroundRefreshingUsers, setBackgroundRefreshingUsers] = useState(false);
  const [usersError, setUsersError] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());

  const [lookupEmail, setLookupEmail] = useState("");
  const [selectedRole, setSelectedRole] = useState<RoleOption["value"]>("viewer");
  const [lookupResult, setLookupResult] = useState<ScopedUser | null>(null);
  const [lookupMessage, setLookupMessage] = useState<string | null>(null);
  const [lookingUp, setLookingUp] = useState(false);
  const [addingAccess, setAddingAccess] = useState(false);
  const [memberSearch, setMemberSearch] = useState("");
  const [memberRoleFilter, setMemberRoleFilter] = useState<string>("all");
  const [memberStatusFilter, setMemberStatusFilter] = useState<string>("all");
  const [removingUserId, setRemovingUserId] = useState<string | null>(null);
  const [removeTarget, setRemoveTarget] = useState<RemoveAccessTarget | null>(null);

  const normalizedAppCode = String(appCode || "").trim().toLowerCase();
  const enabledSections = useMemo(() => resolveAppScopedAdminSections(normalizedAppCode), [normalizedAppCode]);
  const showUserAccessSection = enabledSections.includes("user_access");
  const showTradspherePermissionDetails = enabledSections.includes("tradsphere_permission_details");
  const needsUserDirectory = showUserAccessSection;
  const appLabel = String(appName || normalizedAppCode || "App").trim() || "App";
  const currentTenantSlug = String(auth.accessProfile?.tenant?.slug || auth.tenantSlug || "-").trim() || "-";
  const actorUserId = String(auth.user?.id || "").trim();
  const actorPermissions = useMemo(() => new Set(auth.accessProfile?.permissions ?? []), [auth.accessProfile?.permissions]);
  const actorRole = normalizeRoleKey(auth.accessProfile?.role || "");
  const actorIsSuperAdmin = actorPermissions.has("workspace.super_admin") || actorRole === "super_admin";
  const scopedAdminCacheKey = useMemo(
    () => buildScopedAdminCacheKey({
      userId: String(auth.user?.id || "").trim(),
      appCode: normalizedAppCode,
      tenantSlug: currentTenantSlug,
    }),
    [auth.user?.id, currentTenantSlug, normalizedAppCode],
  );

  const persistScopedUsersCache = useCallback((nextItems: ScopedUser[]) => {
    const fetchedAt = Date.now();
    setUsers(nextItems);
    setCacheStatus({ source: "network", fetchedAt });
    writeBrowserCache(
      scopedAdminCacheKey,
      { items: nextItems },
      APP_SCOPED_ADMIN_CACHE_TTL_MS,
      { source: "network", fetchedAt },
    );
  }, [scopedAdminCacheKey]);

  const loadScopedUsers = useCallback(async (showRefreshing: boolean) => {
    const loadInvocationId = loadInvocationRef.current + 1;
    loadInvocationRef.current = loadInvocationId;

    const cachedSnapshot = !showRefreshing
      ? readBrowserCacheSnapshot<ScopedUsersCacheSnapshot>(scopedAdminCacheKey)
      : null;
    const cachedData = cachedSnapshot?.data;
    const canUseCachedData = Boolean(cachedData && Array.isArray(cachedData.items));

    if (showRefreshing) {
      setRefreshingUsers(true);
      setBackgroundRefreshingUsers(false);
    } else {
      setLoadingUsers(true);
      setBackgroundRefreshingUsers(false);
      if (canUseCachedData && cachedData) {
        setUsers(cachedData.items);
        setCacheStatus({
          source: "cache",
          fetchedAt: cachedSnapshot?.fetchedAt ?? Date.now(),
        });
        setLoadingUsers(false);
        setBackgroundRefreshingUsers(true);
      }
    }
    setUsersError(null);

    if (!isOnline) {
      if (!canUseCachedData) {
        if (loadInvocationRef.current !== loadInvocationId) {
          return;
        }
        setUsersError(`You're offline. ${appLabel} users are unavailable until connection is restored.`);
        setCacheStatus(null);
      }
      if (loadInvocationRef.current === loadInvocationId) {
        setLoadingUsers(false);
        setRefreshingUsers(false);
        setBackgroundRefreshingUsers(false);
      }
      return;
    }

    try {
      const payload = await requestJson("/api/auth/v1/invitations/users", {
        headers: {
          "X-App-Code": normalizedAppCode,
        },
        successToast: false,
        errorToast: false,
      });
      const data = unwrap<ScopedUsersResponse>(payload, {});
      const nextItems = Array.isArray(data.items) ? data.items : [];
      if (loadInvocationRef.current !== loadInvocationId) {
        return;
      }
      persistScopedUsersCache(nextItems);
    } catch (error) {
      const message = error instanceof Error ? error.message : `Unable to load current ${appLabel} users.`;
      if (!canUseCachedData) {
        if (loadInvocationRef.current !== loadInvocationId) {
          return;
        }
        setUsersError(message);
        setCacheStatus(null);
      }
    } finally {
      if (loadInvocationRef.current !== loadInvocationId) {
        return;
      }
      setLoadingUsers(false);
      setRefreshingUsers(false);
      setBackgroundRefreshingUsers(false);
    }
  }, [appLabel, isOnline, normalizedAppCode, persistScopedUsersCache, requestJson, scopedAdminCacheKey]);

  useEffect(() => {
    if (!needsUserDirectory) {
      setUsers([]);
      setLoadingUsers(false);
      setRefreshingUsers(false);
      setBackgroundRefreshingUsers(false);
      setUsersError(null);
      setCacheStatus(null);
      return;
    }
    void loadScopedUsers(false);
  }, [loadScopedUsers, needsUserDirectory]);

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

  const normalizedLookupEmail = lookupEmail.trim().toLowerCase();
  const canLookup = isEmailValid(normalizedLookupEmail) && !lookingUp && !addingAccess;
  const canAdd = Boolean(lookupResult && !lookupResult.assignedInScope && !addingAccess && !lookingUp);
  const roleFilterOptions = useMemo(
    () => [
      { value: "all", label: "All roles" },
      ...ROLE_OPTIONS.map((role) => ({ value: role.value, label: role.label })),
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
  const filteredUsers = useMemo(() => {
    const searchValue = memberSearch.trim().toLowerCase();
    return users.filter((user) => {
      const role = String(user.role || "viewer").trim().toLowerCase() || "viewer";
      const status = String(user.status || "active").trim().toLowerCase() || "active";
      if (memberRoleFilter !== "all" && role !== memberRoleFilter) {
        return false;
      }
      if (memberStatusFilter !== "all" && status !== memberStatusFilter) {
        return false;
      }
      if (!searchValue) {
        return true;
      }
      const haystack = [
        user.fullName || "",
        user.email || "",
        user.userId,
        role,
        status,
      ].join(" ").toLowerCase();
      return haystack.includes(searchValue);
    });
  }, [memberRoleFilter, memberSearch, memberStatusFilter, users]);
  const cacheStatusText = (refreshingUsers || backgroundRefreshingUsers)
    ? "Refreshing..."
    : !isOnline && cacheStatus
      ? `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : cacheStatus
        ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
        : "No cached data yet";
  const showCacheChip = needsUserDirectory && !loadingUsers && !usersError;

  async function handleLookup() {
    if (!canLookup) {
      return;
    }

    setLookingUp(true);
    setLookupMessage(null);
    try {
      const payload = await requestJson(
        `/api/auth/v1/invitations/users/lookup?email=${encodeURIComponent(normalizedLookupEmail)}`,
        {
          headers: {
            "X-App-Code": normalizedAppCode,
          },
          successToast: false,
          errorToast: false,
        },
      );
      const data = unwrap<ScopedUsersResponse>(payload, {});
      const first = Array.isArray(data.items) && data.items.length > 0 ? data.items[0] : null;
      if (!first) {
        setLookupResult(null);
        setLookupMessage("No active existing user found for that email.");
        return;
      }
      setLookupResult(first);
      setLookupMessage(first.assignedInScope
        ? `This user already has ${appLabel} access in the current tenant.`
        : `Active existing user found. Choose a role to add ${appLabel} access.`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to lookup user.";
      setLookupResult(null);
      setLookupMessage(message);
    } finally {
      setLookingUp(false);
    }
  }

  async function handleAddAccess() {
    if (!lookupResult || !canAdd) {
      return;
    }
    const targetEmail = String(lookupResult.email || "").trim().toLowerCase();
    if (!targetEmail) {
      setLookupMessage("Selected user email is unavailable.");
      return;
    }

    setAddingAccess(true);
    try {
      await requestJson("/api/auth/v1/invitations", {
        method: "POST",
        headers: {
          "X-App-Code": normalizedAppCode,
        },
        body: {
          email: targetEmail,
          appCode: normalizedAppCode,
          role: selectedRole,
        },
        successToast: {
          title: "Access updated",
          message: `${appLabel} access was added for this tenant.`,
        },
      });
      setLookupEmail("");
      setSelectedRole("viewer");
      setLookupResult(null);
      setLookupMessage(`${appLabel} access added successfully.`);
      const normalizedLookupUserId = String(lookupResult.userId || "").trim();
      if (normalizedLookupUserId) {
        const optimisticUser: ScopedUser = {
          ...lookupResult,
          role: selectedRole,
          assignedInScope: true,
        };
        const nextUsers = users.some((item) => String(item.userId || "").trim() === normalizedLookupUserId)
          ? users.map((item) => (String(item.userId || "").trim() === normalizedLookupUserId
            ? {
              ...item,
              ...optimisticUser,
            }
            : item))
          : [optimisticUser, ...users];
        persistScopedUsersCache(nextUsers);
      }
      await loadScopedUsers(true);
    } finally {
      setAddingAccess(false);
    }
  }

  function canManageUserAccess(user: ScopedUser): boolean {
    const targetUserId = String(user.userId || "").trim();
    if (!targetUserId) {
      return false;
    }
    if (targetUserId === actorUserId) {
      return false;
    }
    if (Boolean(user.isSuperAdmin) && !actorIsSuperAdmin) {
      return false;
    }
    if (!actorIsSuperAdmin && roleRank(user.role) < roleRank(actorRole)) {
      return false;
    }
    return true;
  }

  function handleRemoveAccess(user: ScopedUser) {
    if (!canManageUserAccess(user)) {
      return;
    }
    setRemoveTarget({
      userId: user.userId,
      email: user.email || null,
      fullName: user.fullName || null,
    });
  }

  async function handleRemoveAccessConfirmed() {
    if (!removeTarget) {
      return;
    }
    setRemovingUserId(removeTarget.userId);
    try {
      await requestJson(`/api/auth/v1/invitations/users/${encodeURIComponent(removeTarget.userId)}/access`, {
        method: "DELETE",
        headers: {
          "X-App-Code": normalizedAppCode,
        },
        successToast: {
          title: "Access removed",
          message: `${appLabel} access was removed from this user.`,
        },
      });
      const normalizedTargetUserId = String(removeTarget.userId || "").trim();
      if (normalizedTargetUserId) {
        const nextUsers = users.filter((item) => String(item.userId || "").trim() !== normalizedTargetUserId);
        persistScopedUsersCache(nextUsers);
      }
      setRemoveTarget(null);
      await loadScopedUsers(true);
    } finally {
      setRemovingUserId(null);
    }
  }

  return (
    <div className="mx-auto flex min-h-[100dvh] w-full max-w-[1680px] flex-col gap-6">
      <PageBanner
        eyebrow={appLabel}
        title="Admin"
        description={`Manage who can access ${appLabel} for the current tenant.`}
        gradientVariant="admin"
        className="[&>div.relative]:min-h-[136px] [&>div.relative]:py-6 md:[&>div.relative]:min-h-[164px] md:[&>div.relative]:py-8"
      />

      {showUserAccessSection ? (
        <SectionCard
          title="Add Existing User"
          description="Tenant/app admins can add active existing users only. Brand-new user invites remain Super Admin-only from global admin tools."
          className="p-5"
        >

          <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50/70 p-3">
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
              <Search className="size-3.5" />
              <span>Lookup Existing User</span>
            </div>
            <div className="mt-2 grid gap-2 sm:grid-cols-[minmax(0,1fr)_auto]">
              <Input
                value={lookupEmail}
                onChange={(event) => {
                  setLookupEmail(event.target.value);
                  setLookupResult(null);
                  setLookupMessage(null);
                }}
                placeholder="Enter user email"
                type="email"
                autoComplete="off"
              />
              <Button
                type="button"
                variant="outline"
                disabled={!canLookup}
                onClick={() => void handleLookup()}
                className="min-w-32"
              >
                {lookingUp ? <Spinner className="size-4" /> : <Search className="size-4" />}
                Find user
              </Button>
            </div>
          </div>

          {lookupMessage ? (
            <p className="mt-3 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-600">{lookupMessage}</p>
          ) : null}

          {lookupResult ? (
            <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50/70 p-3">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div>
                  <p className="text-sm font-semibold text-slate-900">{lookupResult.fullName || lookupResult.email || lookupResult.userId}</p>
                  <p className="text-xs text-slate-600">{lookupResult.email || lookupResult.userId}</p>
                </div>
                <span className={`inline-flex rounded-full border px-2 py-0.5 text-xs font-semibold ${statusChipClass(lookupResult.status)}`}>
                  {formatStatusChipLabel(lookupResult.status)}
                </span>
              </div>

              <div className="mt-3 grid gap-2 sm:grid-cols-[minmax(0,220px)_auto] sm:items-center">
                <AppDropdown
                  value={selectedRole}
                  onValueChange={(value) => setSelectedRole(value as RoleOption["value"])}
                  options={ROLE_OPTIONS.map((role) => ({ value: role.value, label: role.label }))}
                  searchable={false}
                  ariaLabel={`Select ${appLabel} role`}
                />
                {canAdd ? (
                  <Button
                    type="button"
                    disabled={addingAccess}
                    onClick={() => void handleAddAccess()}
                    className="sm:justify-self-end"
                  >
                    {addingAccess ? <Spinner className="size-4" /> : <UserPlus className="size-4" />}
                    Add user access
                  </Button>
                ) : null}
              </div>
            </div>
          ) : null}
        </SectionCard>
      ) : null}

      {showUserAccessSection ? (
        <SectionCard
          title={`Current ${appLabel} Users`}
          description={`Users currently assigned to this tenant + ${appLabel} scope.`}
          actions={(
            <div className="flex items-center gap-2">
              <p className="text-sm text-slate-500">
                {filteredUsers.length}/{users.length} users
              </p>
              <Button
                type="button"
                variant="outline"
                disabled={refreshingUsers || backgroundRefreshingUsers || loadingUsers || !isOnline}
                onClick={() => void loadScopedUsers(true)}
                className="h-8 px-2.5 text-xs"
              >
                {refreshingUsers ? <Spinner className="size-3.5" /> : <RefreshCw className="size-3.5" />}
                Refresh
              </Button>
            </div>
          )}
          className="p-5"
        >

        <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50/70 p-3">
          <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
            <Search className="size-3.5" />
            <span>Filters</span>
          </div>
          <div className="mt-2 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
            <Input
              value={memberSearch}
              onChange={(event) => setMemberSearch(event.target.value)}
              placeholder="Search name, email, role"
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
          </div>
        </div>

        {usersError ? (
          <p className="mt-4 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{usersError}</p>
        ) : null}

        {loadingUsers && users.length === 0 ? (
          <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
            <div className="flex items-center gap-2">
              <Spinner className="size-4" />
              <span>Loading users...</span>
            </div>
          </div>
        ) : null}

        {!loadingUsers && users.length === 0 ? (
          <p className="mt-4 rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
            No users are currently assigned to this tenant + {appLabel} scope.
          </p>
        ) : null}

        {!loadingUsers && users.length > 0 && filteredUsers.length === 0 ? (
          <p className="mt-4 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
            No users match the current filters.
          </p>
        ) : null}

          {filteredUsers.length > 0 ? (
            <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
              {filteredUsers.map((user) => (
                <article key={user.userId} className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <p className="truncate text-sm font-semibold text-slate-900">{user.fullName || user.email || user.userId}</p>
                      <p className="truncate text-xs text-slate-600">{user.email || user.userId}</p>
                    </div>
                    <div className="flex items-center gap-2">
                      {canManageUserAccess(user) ? (
                        <RemoveAccessIconButton
                          onRemove={() => handleRemoveAccess(user)}
                          disabled={removingUserId === user.userId}
                        />
                      ) : null}
                      <span className={`inline-flex rounded-full border px-2 py-0.5 text-xs font-semibold ${statusChipClass(user.status)}`}>
                        {formatStatusChipLabel(user.status)}
                      </span>
                    </div>
                  </div>
                  <div className="mt-3 space-y-2">
                    <div>
                      <p className="mb-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">Assignment</p>
                      <div className="flex flex-wrap gap-1">
                        <span className="rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-[11px] font-medium text-blue-700">
                          {currentTenantSlug}: {appLabel} ({roleLabel(user.role || "viewer")})
                        </span>
                      </div>
                    </div>
                    {!canManageUserAccess(user) ? (
                      <div className="flex items-center justify-end">
                        <span className="text-[11px] text-slate-500">Protected user access</span>
                      </div>
                    ) : null}
                  </div>
                </article>
              ))}
            </div>
          ) : null}
        </SectionCard>
      ) : null}

      {showTradspherePermissionDetails ? (
        <TradspherePermissionDetailsSection
          appCode={normalizedAppCode}
          appLabel={appLabel}
        />
      ) : null}

      <Dialog open={Boolean(removeTarget)} onOpenChange={(open) => !open && !removingUserId && setRemoveTarget(null)}>
        <DialogContent
          className="max-w-[560px]"
          onEscapeKeyDown={(event) => {
            if (Boolean(removingUserId)) {
              event.preventDefault();
            }
          }}
          onInteractOutside={(event) => {
            if (Boolean(removingUserId)) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close remove access modal"
            disabled={Boolean(removingUserId)}
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader>
            <DialogTitle>Remove app access</DialogTitle>
          </DialogHeader>

          <p className="text-sm text-slate-700">
            Remove {appLabel} access for{" "}
            <span className="font-semibold">{removeTarget?.fullName || removeTarget?.email || removeTarget?.userId}</span>?
          </p>

          <DialogFooter>
            <Button
              className="border border-rose-700 bg-rose-600 text-white hover:bg-rose-700"
              onClick={() => void handleRemoveAccessConfirmed()}
              disabled={Boolean(removingUserId)}
            >
              {Boolean(removingUserId) ? <Spinner className="size-4" /> : null}
              {Boolean(removingUserId) ? "Removing..." : "Remove access"}
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
                onRefresh={() => void loadScopedUsers(true)}
                disabled={refreshingUsers || backgroundRefreshingUsers || loadingUsers || !isOnline}
                refreshing={refreshingUsers || backgroundRefreshingUsers}
                refreshLabel={`Refresh ${appLabel} users`}
                tooltipText={isOnline ? "Click to refresh current scoped users" : "Offline. Reconnect to refresh users."}
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
