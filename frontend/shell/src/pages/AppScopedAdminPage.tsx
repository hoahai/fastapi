import { RefreshCw, Search, UserMinus, UserPlus, X } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { PageBanner } from "@shell/components/layout/PageBanner";
import { AppDropdown } from "@tradsphere/components/ui/app-dropdown";
import { Button } from "@tradsphere/components/ui/button";
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
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { useOnlineStatus } from "@shared/hooks/useOnlineStatus";
import { readBrowserCacheSnapshot, writeBrowserCache } from "@shared/lib/browserCache";
import { PagePermissionsSection } from "@shared/components/admin/PagePermissionsSection";
import { resolveAppScopedAdminSections } from "@shell/admin/appScopedAdminConfig";
import { SectionCard } from "@shared/components";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { PageLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import { FRONTEND_CACHE_TTL_MS } from "@shared/cache";
import { Tooltip } from "@shared/components/actions/Tooltip";
import { roleLabel } from "@shared/auth/accessAssignments";
import { useAuth } from "@shared/auth/useAuth";
import { useScopedPersistentState } from "@shared/hooks/useScopedPersistentState";
import { resolveCriteriaLoadPlan } from "@shared/hooks/useCriteriaLoadPolicy";

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

type GrantScopedAccessResponse = {
  status?: string;
  user?: ScopedUser;
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

type RoleChangeTarget = {
  userId: string;
  fullName: string | null;
  email: string | null;
  currentRole: RoleOption["value"];
  nextRole: RoleOption["value"];
};

const ROLE_OPTIONS: RoleOption[] = [
  { value: "viewer", label: "Viewer" },
  { value: "editor", label: "Editor" },
  { value: "admin", label: "Admin" },
];
const APP_SCOPED_ADMIN_CACHE_TTL_MS = FRONTEND_CACHE_TTL_MS.DEFAULT;
const APP_SCOPED_ADMIN_PAGE_CODE = "admin";
const APP_SCOPED_ADMIN_LOOKUP_QUERY_STATE_KEY = "lookupQuery";
const APP_SCOPED_ADMIN_MEMBER_SEARCH_STATE_KEY = "memberSearch";
const APP_SCOPED_ADMIN_MEMBER_ROLE_FILTER_STATE_KEY = "memberRoleFilter";
const APP_SCOPED_ADMIN_MEMBER_STATUS_FILTER_STATE_KEY = "memberStatusFilter";

function unwrap<T>(payload: unknown, fallback: T): T {
  if (payload && typeof payload === "object" && "data" in (payload as Record<string, unknown>)) {
    return ((payload as { data?: T }).data ?? fallback);
  }
  return (payload as T) ?? fallback;
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

function normalizeScopedRole(role: string | null | undefined): RoleOption["value"] {
  const normalized = normalizeRoleKey(role);
  if (normalized === "admin") {
    return "admin";
  }
  if (normalized === "editor") {
    return "editor";
  }
  return "viewer";
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
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [usersError, setUsersError] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);

  const [lookupQuery, setLookupQuery] = useScopedPersistentState<string>(
    {
      appCode: String(appCode || "").trim().toLowerCase() || "workspace",
      pageCode: APP_SCOPED_ADMIN_PAGE_CODE,
      stateKey: APP_SCOPED_ADMIN_LOOKUP_QUERY_STATE_KEY,
    },
    "",
    { validate: (value: unknown): value is string => typeof value === "string" },
  );
  const [lookupResults, setLookupResults] = useState<ScopedUser[]>([]);
  const [lookupRoleByUserId, setLookupRoleByUserId] = useState<Record<string, RoleOption["value"]>>({});
  const [lookupMessage, setLookupMessage] = useState<string | null>(null);
  const [lookingUp, setLookingUp] = useState(false);
  const [addingAccessByUserId, setAddingAccessByUserId] = useState<Record<string, boolean>>({});
  const [savingUserRoleByUserId, setSavingUserRoleByUserId] = useState<Record<string, boolean>>({});
  const [memberSearch, setMemberSearch] = useScopedPersistentState<string>(
    {
      appCode: String(appCode || "").trim().toLowerCase() || "workspace",
      pageCode: APP_SCOPED_ADMIN_PAGE_CODE,
      stateKey: APP_SCOPED_ADMIN_MEMBER_SEARCH_STATE_KEY,
    },
    "",
    { validate: (value: unknown): value is string => typeof value === "string" },
  );
  const [memberRoleFilter, setMemberRoleFilter] = useScopedPersistentState<string>(
    {
      appCode: String(appCode || "").trim().toLowerCase() || "workspace",
      pageCode: APP_SCOPED_ADMIN_PAGE_CODE,
      stateKey: APP_SCOPED_ADMIN_MEMBER_ROLE_FILTER_STATE_KEY,
    },
    "all",
    {
      validate: (value: unknown): value is string =>
        typeof value === "string" && ["all", "viewer", "editor", "admin"].includes(value),
    },
  );
  const [memberStatusFilter, setMemberStatusFilter] = useScopedPersistentState<string>(
    {
      appCode: String(appCode || "").trim().toLowerCase() || "workspace",
      pageCode: APP_SCOPED_ADMIN_PAGE_CODE,
      stateKey: APP_SCOPED_ADMIN_MEMBER_STATUS_FILTER_STATE_KEY,
    },
    "all",
    {
      validate: (value: unknown): value is string =>
        typeof value === "string" && ["all", "active", "pending", "disabled"].includes(value),
    },
  );
  const [removingUserId, setRemovingUserId] = useState<string | null>(null);
  const [removeTarget, setRemoveTarget] = useState<RemoveAccessTarget | null>(null);
  const [roleChangeTarget, setRoleChangeTarget] = useState<RoleChangeTarget | null>(null);
  const [permissionSyncNonce, setPermissionSyncNonce] = useState(0);
  const [lastRemovedUserId, setLastRemovedUserId] = useState<string | null>(null);

  const normalizedAppCode = String(appCode || "").trim().toLowerCase();
  const enabledSections = useMemo(() => resolveAppScopedAdminSections(normalizedAppCode), [normalizedAppCode]);
  const showUserAccessSection = enabledSections.includes("user_access");
  const showPagePermissionsSection = enabledSections.includes("page_permissions");
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

  const loadScopedUsers = useCallback(async (trigger: "load-button" | "cache-chip" = "load-button") => {
    const loadInvocationId = loadInvocationRef.current + 1;
    loadInvocationRef.current = loadInvocationId;
    const loadPlan = resolveCriteriaLoadPlan({
      trigger,
      criteriaKey: scopedAdminCacheKey,
      loadedCriteriaKey: trigger === "cache-chip" ? scopedAdminCacheKey : null,
    });

    const cachedSnapshot = !loadPlan.shouldIgnoreCache
      ? readBrowserCacheSnapshot<ScopedUsersCacheSnapshot>(scopedAdminCacheKey)
      : null;
    const cachedData = cachedSnapshot?.data;
    const canUseCachedData = Boolean(cachedData && Array.isArray(cachedData.items));

    if (loadPlan.shouldIgnoreCache) {
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
    void loadScopedUsers("load-button");
  }, [loadScopedUsers, needsUserDirectory]);

  const normalizedLookupQuery = lookupQuery.trim();
  const canLookup = normalizedLookupQuery.length >= 2 && !lookingUp;
  const assignableRoleOptions = useMemo(
    () => ROLE_OPTIONS.filter((option) => (
      actorIsSuperAdmin || roleRank(option.value) >= roleRank(actorRole)
    )),
    [actorIsSuperAdmin, actorRole],
  );
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
  const loadingContract = resolveSharedLoadingContract(
    {
      pageInitializing: needsUserDirectory && loadingUsers && users.length === 0,
      pageRefreshing: needsUserDirectory && (refreshingUsers || backgroundRefreshingUsers),
      cacheChipRefreshing: needsUserDirectory && isChipRefreshOverlayVisible,
    },
    {
      pageInitializing: `Preparing ${appLabel} admin workspace...`,
      pageRefreshing: `Refreshing ${appLabel} users...`,
      cacheChipRefreshing: `Refreshing ${appLabel} users...`,
    },
  );
  const pageMessages: StackMessage[] = [];
  if (usersError) {
    pageMessages.push({
      id: "app-scoped-admin-load-error",
      variant: "error",
      message: usersError,
    });
  }

  async function handleLookup() {
    if (!canLookup) {
      return;
    }

    setLookingUp(true);
    setLookupMessage(null);
    try {
      const payload = await requestJson(
        `/api/auth/v1/invitations/users/lookup?query=${encodeURIComponent(normalizedLookupQuery)}`,
        {
          headers: {
            "X-App-Code": normalizedAppCode,
          },
          successToast: false,
          errorToast: false,
        },
      );
      const data = unwrap<ScopedUsersResponse>(payload, {});
      const matchedItems = (Array.isArray(data.items) ? data.items : [])
        .filter((item) => !item.assignedInScope)
        .filter((item) => !item.isSuperAdmin || actorIsSuperAdmin);
      setLookupResults(matchedItems);
      setLookupRoleByUserId((current) => matchedItems.reduce<Record<string, RoleOption["value"]>>((acc, item) => {
        const userId = String(item.userId || "").trim();
        if (!userId) {
          return acc;
        }
        acc[userId] = current[userId] || normalizeScopedRole(item.role);
        return acc;
      }, {}));
      if (matchedItems.length === 0) {
        setLookupMessage("No active existing user found for that search.");
        return;
      }
      setLookupMessage(`Found ${matchedItems.length} active user(s).`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to lookup user.";
      setLookupResults([]);
      setLookupMessage(message);
    } finally {
      setLookingUp(false);
    }
  }

  async function handleAddAccess(targetUser: ScopedUser) {
    if (lookingUp) {
      return;
    }
    const targetUserId = String(targetUser.userId || "").trim();
    if (!targetUserId) {
      setLookupMessage("Selected user is unavailable.");
      return;
    }
    const roleForUser = lookupRoleByUserId[targetUserId] || normalizeScopedRole(targetUser.role);
    if (addingAccessByUserId[targetUserId]) {
      return;
    }
    if (targetUser.assignedInScope) {
      setLookupMessage("This user already has access in the current tenant.");
      return;
    }

    setAddingAccessByUserId((current) => ({ ...current, [targetUserId]: true }));
    try {
      const payload = await requestJson("/api/auth/v1/invitations/users/access", {
        method: "POST",
        headers: {
          "X-App-Code": normalizedAppCode,
        },
        body: {
          userId: targetUserId,
          role: roleForUser,
        },
        successToast: {
          title: "Access granted",
          message: `${appLabel} access was granted for this tenant.`,
        },
      });
      const data = unwrap<GrantScopedAccessResponse>(payload, {});
      const grantedUser = data.user && typeof data.user === "object" ? data.user : null;
      setLookupMessage(`${appLabel} access granted successfully.`);
      if (grantedUser && String(grantedUser.userId || "").trim()) {
        const normalizedGrantedUserId = String(grantedUser.userId || "").trim();
        setLookupResults((current) => current.filter((item) => String(item.userId || "").trim() !== normalizedGrantedUserId));
        setLookupRoleByUserId((current) => ({
          ...current,
          [normalizedGrantedUserId]: normalizeScopedRole(grantedUser.role),
        }));
        const nextUsers = users.some((item) => String(item.userId || "").trim() === normalizedGrantedUserId)
          ? users.map((item) => (String(item.userId || "").trim() === normalizedGrantedUserId
            ? {
              ...item,
              ...grantedUser,
              assignedInScope: true,
            }
            : item))
          : [{ ...grantedUser, assignedInScope: true }, ...users];
        persistScopedUsersCache(nextUsers);
      } else {
        await loadScopedUsers("cache-chip");
      }
      setLastRemovedUserId(null);
      setPermissionSyncNonce((current) => current + 1);
    } finally {
      setAddingAccessByUserId((current) => ({ ...current, [targetUserId]: false }));
    }
  }

  async function handleUpdateUserRole(user: ScopedUser, nextRole: RoleOption["value"]) {
    const userId = String(user.userId || "").trim();
    if (!userId) {
      return;
    }
    if (!canManageUserAccess(user)) {
      return;
    }
    const currentRole = normalizeScopedRole(user.role);
    if (nextRole === currentRole) {
      return;
    }
    if (savingUserRoleByUserId[userId]) {
      return;
    }

    setSavingUserRoleByUserId((current) => ({ ...current, [userId]: true }));
    try {
      const payload = await requestJson("/api/auth/v1/invitations/users/access", {
        method: "POST",
        headers: {
          "X-App-Code": normalizedAppCode,
        },
        body: {
          userId,
          role: nextRole,
        },
        successToast: {
          title: "Role updated",
          message: `${appLabel} role was updated for this user.`,
        },
      });
      const data = unwrap<GrantScopedAccessResponse>(payload, {});
      const updatedUser = data.user && typeof data.user === "object" ? data.user : null;
      if (updatedUser && String(updatedUser.userId || "").trim() === userId) {
        const nextUsers = users.map((item) => (String(item.userId || "").trim() === userId
          ? {
            ...item,
            ...updatedUser,
            assignedInScope: true,
          }
          : item));
        persistScopedUsersCache(nextUsers);
      } else {
        await loadScopedUsers("cache-chip");
      }
      setPermissionSyncNonce((current) => current + 1);
    } finally {
      setSavingUserRoleByUserId((current) => ({ ...current, [userId]: false }));
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

  function handleRequestRoleChange(user: ScopedUser, nextRole: RoleOption["value"]) {
    const userId = String(user.userId || "").trim();
    if (!userId || !canManageUserAccess(user) || savingUserRoleByUserId[userId]) {
      return;
    }
    const currentRole = normalizeScopedRole(user.role);
    if (currentRole === nextRole) {
      return;
    }
    setRoleChangeTarget({
      userId,
      fullName: user.fullName || null,
      email: user.email || null,
      currentRole,
      nextRole,
    });
  }

  async function handleConfirmRoleChange() {
    if (!roleChangeTarget) {
      return;
    }
    const target = users.find((item) => String(item.userId || "").trim() === roleChangeTarget.userId);
    if (!target) {
      setRoleChangeTarget(null);
      return;
    }
    await handleUpdateUserRole(target, roleChangeTarget.nextRole);
    setRoleChangeTarget(null);
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
        setLastRemovedUserId(normalizedTargetUserId);
      }
      setPermissionSyncNonce((current) => current + 1);
      setRemoveTarget(null);
      await loadScopedUsers("cache-chip");
    } finally {
      setRemovingUserId(null);
    }
  }

  const roleChangeSaving = Boolean(
    roleChangeTarget?.userId && savingUserRoleByUserId[roleChangeTarget.userId],
  );

  async function handleRefreshFromChip() {
    setIsChipRefreshOverlayVisible(true);
    try {
      await loadScopedUsers("cache-chip");
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
          eyebrow={appLabel}
          title="Admin"
          description={`Manage who can access ${appLabel} for the current tenant.`}
          gradientVariant="admin"
          className="[&>div.relative]:min-h-[136px] [&>div.relative]:py-6 md:[&>div.relative]:min-h-[164px] md:[&>div.relative]:py-8"
        />
      )}
      footer={showCacheChip ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={handleRefreshFromChip}
          disabled={refreshingUsers || backgroundRefreshingUsers || loadingUsers || isChipRefreshOverlayVisible || !isOnline}
          refreshing={refreshingUsers || backgroundRefreshingUsers || isChipRefreshOverlayVisible}
          refreshLabel={`Refresh ${appLabel} users`}
          tooltipText={isOnline ? "Click to refresh current scoped users" : "Offline. Reconnect to refresh users."}
          containerClassName="w-full"
        />
      ) : null}
    >

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
                value={lookupQuery}
                disabled={lookingUp}
                onChange={(event) => {
                  setLookupQuery(event.target.value);
                  setLookupResults([]);
                  setLookupMessage(null);
                }}
                onKeyDown={(event) => {
                  if (event.key !== "Enter") {
                    return;
                  }
                  event.preventDefault();
                  if (!canLookup) {
                    return;
                  }
                  void handleLookup();
                }}
                placeholder="Enter user email or name"
                type="text"
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
            <p className="mt-3 text-sm text-slate-600">{lookupMessage}</p>
          ) : null}

          {lookupResults.length > 0 ? (
              <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {lookupResults.map((user) => {
                  const userId = String(user.userId || "").trim();
                  const selectedCardRole = lookupRoleByUserId[userId] || normalizeScopedRole(user.role);
                  const saving = Boolean(addingAccessByUserId[userId]);
                  return (
                    <article key={user.userId} className="rounded-xl border border-slate-200 bg-white p-3">
                      <div className="flex items-start justify-between gap-2">
                        <div>
                          <p className="text-sm font-semibold text-slate-900">{user.fullName || user.email || user.userId}</p>
                          <p className="text-xs text-slate-600">{user.email || user.userId}</p>
                        </div>
                        <span className={`inline-flex rounded-full border px-2 py-0.5 text-xs font-semibold ${statusChipClass(user.status)}`}>
                          {formatStatusChipLabel(user.status)}
                        </span>
                      </div>
                      <div className="mt-2 flex flex-wrap items-center gap-2">
                        {user.isSuperAdmin ? (
                          <span className="inline-flex rounded-full border border-indigo-200 bg-indigo-50 px-2 py-0.5 text-[11px] font-medium text-indigo-700">
                            Super Admin
                          </span>
                        ) : null}
                      </div>
                      <div className="mt-3">
                        <AppDropdown
                          value={selectedCardRole}
                          onValueChange={(value) => setLookupRoleByUserId((current) => ({
                            ...current,
                            [userId]: value as RoleOption["value"],
                          }))}
                          options={assignableRoleOptions.map((role) => ({ value: role.value, label: role.label }))}
                          searchable={false}
                          ariaLabel={`Select ${appLabel} role for ${user.fullName || user.email || user.userId}`}
                        />
                      </div>
                      <div className="mt-3 flex justify-end">
                        <Button
                          type="button"
                          disabled={saving || lookingUp}
                          onClick={() => void handleAddAccess(user)}
                          className="h-8 px-3 text-xs"
                        >
                          {saving ? <Spinner className="size-3.5" /> : <UserPlus className="size-3.5" />}
                          Add user access
                        </Button>
                      </div>
                    </article>
                  );
                })}
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
                onClick={() => void loadScopedUsers("cache-chip")}
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
              {filteredUsers.map((user) => {
                const userId = String(user.userId || "").trim();
                const canManage = canManageUserAccess(user);
                const persistedRole = normalizeScopedRole(user.role);
                const savingRole = Boolean(savingUserRoleByUserId[userId]);
                return (
                  <article key={user.userId} className="flex h-full flex-col overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
                    <div className="flex-1 p-4">
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0">
                          <p className="truncate text-sm font-semibold text-slate-900">{user.fullName || user.email || user.userId}</p>
                          <p className="truncate text-xs text-slate-600">{user.email || user.userId}</p>
                        </div>
                        <div className="flex items-center gap-2">
                          {canManage ? (
                            <RemoveAccessIconButton
                              onRemove={() => handleRemoveAccess(user)}
                              disabled={removingUserId === user.userId || savingRole}
                            />
                          ) : null}
                          <span className={`inline-flex rounded-full border px-2 py-0.5 text-xs font-semibold ${statusChipClass(user.status)}`}>
                            {formatStatusChipLabel(user.status)}
                          </span>
                        </div>
                      </div>
                    </div>
                    {canManage ? (
                      <div className="flex h-[56px] items-center px-3">
                        <div
                          className="inline-flex w-full flex-wrap items-center gap-1 rounded-lg border border-slate-200 bg-white p-0.5"
                          role="group"
                          aria-label={`Change ${appLabel} role for ${user.fullName || user.email || user.userId}`}
                        >
                          {assignableRoleOptions.map((roleOption) => {
                            const isActive = persistedRole === roleOption.value;
                            return (
                              <button
                                key={`${userId}-${roleOption.value}`}
                                type="button"
                                disabled={savingRole}
                                onClick={() => handleRequestRoleChange(user, roleOption.value)}
                                className={
                                  `h-7 flex-1 rounded-md px-2 text-xs font-semibold transition-colors ${
                                    isActive
                                      ? "border border-blue-200 bg-blue-50 text-blue-800"
                                      : "border border-transparent bg-transparent text-slate-400 hover:bg-slate-50 hover:text-slate-700"
                                  }`
                                }
                                aria-pressed={isActive}
                              >
                                {roleOption.label}
                              </button>
                            );
                          })}
                        </div>
                      </div>
                    ) : (
                      <div className="mt-auto flex h-[56px] items-center justify-end px-3">
                        <p className="text-right text-[11px] leading-none text-slate-500">Protected user access</p>
                      </div>
                    )}
                  </article>
                );
              })}
            </div>
          ) : null}
        </SectionCard>
      ) : null}

      {showPagePermissionsSection ? (
        <PagePermissionsSection
          appCode={normalizedAppCode}
          appLabel={appLabel}
          removedUserId={lastRemovedUserId}
          syncNonce={permissionSyncNonce}
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

      <Dialog open={Boolean(roleChangeTarget)} onOpenChange={(open) => !open && !roleChangeSaving && setRoleChangeTarget(null)}>
        <DialogContent
          className="max-w-[560px]"
          onEscapeKeyDown={(event) => {
            if (roleChangeSaving) {
              event.preventDefault();
            }
          }}
          onInteractOutside={(event) => {
            if (roleChangeSaving) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close role change modal"
            disabled={roleChangeSaving}
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader>
            <DialogTitle>Confirm role change</DialogTitle>
          </DialogHeader>

          <p className="text-sm text-slate-700">
            Change role for{" "}
            <span className="font-semibold">
              {roleChangeTarget?.fullName || roleChangeTarget?.email || roleChangeTarget?.userId}
            </span>{" "}
            from{" "}
            <span className="font-semibold">{roleLabel(roleChangeTarget?.currentRole || "viewer")}</span>{" "}
            to{" "}
            <span className="font-semibold">{roleLabel(roleChangeTarget?.nextRole || "viewer")}</span>?
          </p>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setRoleChangeTarget(null)}
              disabled={roleChangeSaving}
            >
              Cancel
            </Button>
            <Button
              onClick={() => void handleConfirmRoleChange()}
              disabled={roleChangeSaving}
            >
              {roleChangeSaving ? (
                <Spinner className="size-4" />
              ) : null}
              {roleChangeSaving ? "Updating..." : "Confirm"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <PageLoadingLayer
        active={loadingContract.pageOverlayActive}
        message={loadingContract.pageOverlayMessage}
      />
    </AppPageLayout>
  );
}
