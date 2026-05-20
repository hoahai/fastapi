import { RefreshCw } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { useApiRequest } from "@/hooks/useApiRequest";
import { useOnlineStatus } from "@/hooks/useOnlineStatus";
import { readBrowserCacheSnapshot, writeBrowserCache } from "@/lib/browserCache";
import { SectionCard } from "@shared/components";
import { roleLabel } from "@shared/auth/accessAssignments";
import { useAuth } from "@shared/auth/useAuth";

type AppPageOption = {
  key: string;
  label: string;
  route: string;
};

type PagePermissionUser = {
  userId: string;
  email: string | null;
  fullName: string | null;
  status: string | null;
  role: string | null;
  isSuperAdmin?: boolean;
  pageKeys: string[];
  hasRestrictions: boolean;
};

type PagePermissionsLoadResponse = {
  availablePages?: AppPageOption[];
  items?: PagePermissionUser[];
};

type PagePermissionsCacheSnapshot = {
  availablePages: AppPageOption[];
  items: PagePermissionUser[];
};

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type TradspherePermissionDetailsSectionProps = {
  appCode: string;
  appLabel: string;
};

function unwrap<T>(payload: unknown, fallback: T): T {
  if (payload && typeof payload === "object" && "data" in (payload as Record<string, unknown>)) {
    return ((payload as { data?: T }).data ?? fallback);
  }
  return (payload as T) ?? fallback;
}

function normalizeRole(role: string | null | undefined): "viewer" | "editor" | "admin" {
  const normalized = String(role || "").trim().toLowerCase();
  if (normalized === "admin") {
    return "admin";
  }
  if (normalized === "editor") {
    return "editor";
  }
  return "viewer";
}

function normalizeRoleKey(role: string | null | undefined): string {
  return String(role || "").trim().toLowerCase();
}

function rolePriority(role: "viewer" | "editor" | "admin"): number {
  if (role === "admin") {
    return 0;
  }
  if (role === "editor") {
    return 1;
  }
  return 2;
}

function normalizeStatus(status: string | null | undefined): string {
  const normalized = String(status || "").trim().toLowerCase();
  if (!normalized) {
    return "Unknown";
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function statusChipClass(status: string | null | undefined): string {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "active") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (normalized === "pending") {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  if (normalized === "disabled") {
    return "border-rose-200 bg-rose-50 text-rose-700";
  }
  return "border-slate-200 bg-slate-50 text-slate-700";
}

function normalizedPageKeys(keys: string[]): string[] {
  const set = new Set(
    (Array.isArray(keys) ? keys : [])
      .map((value) => String(value || "").trim().toLowerCase())
      .filter(Boolean),
  );
  return Array.from(set).sort();
}

function areSameKeys(left: string[], right: string[]): boolean {
  const a = normalizedPageKeys(left);
  const b = normalizedPageKeys(right);
  if (a.length !== b.length) {
    return false;
  }
  return a.every((value, index) => value === b[index]);
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

const PAGE_PERMISSIONS_CACHE_TTL_MS = 2 * 60 * 1000;

export function TradspherePermissionDetailsSection({
  appCode,
  appLabel,
}: TradspherePermissionDetailsSectionProps) {
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const { isOnline } = useOnlineStatus();
  const loadInvocationRef = useRef(0);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [backgroundRefreshing, setBackgroundRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [availablePages, setAvailablePages] = useState<AppPageOption[]>([]);
  const [users, setUsers] = useState<PagePermissionUser[]>([]);
  const [draftsByUserId, setDraftsByUserId] = useState<Record<string, string[]>>({});
  const [savingByUserId, setSavingByUserId] = useState<Record<string, boolean>>({});
  const actorUserId = String(auth.user?.id || "").trim();
  const actorPermissions = useMemo(() => new Set(auth.accessProfile?.permissions ?? []), [auth.accessProfile?.permissions]);
  const actorRoleKey = normalizeRoleKey(auth.accessProfile?.role || "");
  const actorIsSuperAdmin = actorPermissions.has("workspace.super_admin") || actorRoleKey === "super_admin";

  const cacheKey = useMemo(() => {
    const userKey = String(auth.user?.id || "").trim().toLowerCase() || "anonymous";
    const tenantKey = String(auth.tenantSlug || "").trim().toLowerCase() || "default";
    const appKey = String(appCode || "").trim().toLowerCase() || "app";
    return `app-scoped-admin:page-permissions:${tenantKey}:${appKey}:${userKey}:v1`;
  }, [appCode, auth.tenantSlug, auth.user?.id]);

  const applyPayloadToState = useCallback((payload: PagePermissionsLoadResponse) => {
    const nextAvailablePages = Array.isArray(payload.availablePages) ? payload.availablePages : [];
    const nextUsers = Array.isArray(payload.items) ? payload.items : [];
    setAvailablePages(nextAvailablePages);
    setUsers(nextUsers);
    setDraftsByUserId(
      nextUsers.reduce<Record<string, string[]>>((acc, user) => {
        acc[user.userId] = normalizedPageKeys(user.pageKeys);
        return acc;
      }, {}),
    );
  }, []);

  const loadPagePermissions = useCallback(async (showRefreshing: boolean) => {
    const loadInvocationId = loadInvocationRef.current + 1;
    loadInvocationRef.current = loadInvocationId;
    const cachedSnapshot = !showRefreshing
      ? readBrowserCacheSnapshot<PagePermissionsCacheSnapshot>(cacheKey)
      : null;
    const cachedData = cachedSnapshot?.data;
    const canUseCached = Boolean(
      cachedData
      && Array.isArray(cachedData.availablePages)
      && Array.isArray(cachedData.items),
    );

    if (showRefreshing) {
      setRefreshing(true);
      setBackgroundRefreshing(false);
    } else {
      setLoading(true);
      setBackgroundRefreshing(false);
      if (canUseCached && cachedData) {
        applyPayloadToState(cachedData);
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
      if (!canUseCached) {
        if (loadInvocationRef.current !== loadInvocationId) {
          return;
        }
        setError("You're offline. Page permissions are unavailable until connection is restored.");
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
      const payload = await requestJson("/api/auth/v1/invitations/users/page-permissions", {
        headers: {
          "X-App-Code": appCode,
        },
        successToast: false,
        errorToast: false,
      });
      const data = unwrap<PagePermissionsLoadResponse>(payload, {});
      if (loadInvocationRef.current !== loadInvocationId) {
        return;
      }
      applyPayloadToState(data);
      const fetchedAt = Date.now();
      setCacheStatus({
        source: "network",
        fetchedAt,
      });
      writeBrowserCache<PagePermissionsCacheSnapshot>(
        cacheKey,
        {
          availablePages: Array.isArray(data.availablePages) ? data.availablePages : [],
          items: Array.isArray(data.items) ? data.items : [],
        },
        PAGE_PERMISSIONS_CACHE_TTL_MS,
        {
          source: "network",
          fetchedAt,
        },
      );
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "Unable to load page permissions.";
      if (!canUseCached) {
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
  }, [appCode, applyPayloadToState, cacheKey, isOnline, requestJson]);

  useEffect(() => {
    void loadPagePermissions(false);
  }, [loadPagePermissions]);

  const sortedUsers = useMemo(() => {
    return [...users].sort((left, right) => {
      const roleCmp = rolePriority(normalizeRole(left.role)) - rolePriority(normalizeRole(right.role));
      if (roleCmp !== 0) {
        return roleCmp;
      }
      const leftName = String(left.fullName || left.email || left.userId).trim().toLowerCase();
      const rightName = String(right.fullName || right.email || right.userId).trim().toLowerCase();
      return leftName.localeCompare(rightName);
    });
  }, [users]);

  function togglePageKey(userId: string, pageKey: string) {
    const normalizedPageKey = String(pageKey || "").trim().toLowerCase();
    if (!normalizedPageKey) {
      return;
    }
    setDraftsByUserId((current) => {
      const existing = normalizedPageKeys(current[userId] ?? []);
      const set = new Set(existing);
      if (set.has(normalizedPageKey)) {
        set.delete(normalizedPageKey);
      } else {
        set.add(normalizedPageKey);
      }
      return { ...current, [userId]: Array.from(set).sort() };
    });
  }

  async function saveUserPagePermissions(user: PagePermissionUser) {
    const draftPageKeys = normalizedPageKeys(draftsByUserId[user.userId] ?? []);
    setSavingByUserId((current) => ({ ...current, [user.userId]: true }));
    try {
      const payload = await requestJson(
        `/api/auth/v1/invitations/users/${encodeURIComponent(user.userId)}/page-permissions`,
        {
          method: "PUT",
          headers: {
            "X-App-Code": appCode,
          },
          body: {
            pageKeys: draftPageKeys,
          },
          successToast: {
            title: "Page permissions updated",
            message: `${appLabel} page restrictions were updated.`,
          },
        },
      );
      const data = unwrap<{ pageKeys?: string[] }>(payload, {});
      const savedPageKeys = normalizedPageKeys(Array.isArray(data.pageKeys) ? data.pageKeys : draftPageKeys);
      setUsers((current) => current.map((item) => (item.userId === user.userId
        ? {
          ...item,
          pageKeys: savedPageKeys,
          hasRestrictions: savedPageKeys.length > 0,
        }
        : item)));
      setDraftsByUserId((current) => ({ ...current, [user.userId]: savedPageKeys }));
      const currentSnapshot = readBrowserCacheSnapshot<PagePermissionsCacheSnapshot>(cacheKey);
      const cachedUsers = Array.isArray(currentSnapshot?.data?.items) ? currentSnapshot.data.items : users;
      const cachedPages = Array.isArray(currentSnapshot?.data?.availablePages)
        ? currentSnapshot.data.availablePages
        : availablePages;
      const nextCachedUsers = cachedUsers.map((item) => (item.userId === user.userId
        ? {
          ...item,
          pageKeys: savedPageKeys,
          hasRestrictions: savedPageKeys.length > 0,
        }
        : item));
      const fetchedAt = Date.now();
      writeBrowserCache<PagePermissionsCacheSnapshot>(
        cacheKey,
        {
          availablePages: cachedPages,
          items: nextCachedUsers,
        },
        PAGE_PERMISSIONS_CACHE_TTL_MS,
        {
          source: "network",
          fetchedAt,
        },
      );
      setCacheStatus({
        source: "network",
        fetchedAt,
      });
      if (auth.user?.id === user.userId) {
        auth.refreshAccessProfile();
      }
    } finally {
      setSavingByUserId((current) => ({ ...current, [user.userId]: false }));
    }
  }

  function clearUserRestrictions(userId: string) {
    setDraftsByUserId((current) => ({ ...current, [userId]: [] }));
  }

  function canManagePagePermissions(user: PagePermissionUser): boolean {
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
    return true;
  }

  return (
    <SectionCard
      title="Page Permissions"
      description="Default behavior allows all app pages. Once one or more page permissions are set for a user, that user can access only selected pages."
      className="p-5"
      actions={(
        <Button
          type="button"
          variant="outline"
          onClick={() => void loadPagePermissions(true)}
          disabled={loading || refreshing || backgroundRefreshing || !isOnline}
          className="h-8 px-2.5 text-xs"
        >
          {refreshing || backgroundRefreshing ? <Spinner className="size-3.5" /> : <RefreshCw className="size-3.5" />}
          Refresh
        </Button>
      )}
    >
      {cacheStatus ? (
        <p className="mt-3 text-xs text-slate-500">
          {!isOnline
            ? `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`
            : `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`}
        </p>
      ) : null}

      {error ? (
        <p className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</p>
      ) : null}

      {loading ? (
        <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
          <div className="flex items-center gap-2">
            <Spinner className="size-4" />
            <span>Loading page permissions...</span>
          </div>
        </div>
      ) : null}

      {!loading && availablePages.length === 0 ? (
        <p className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          No app-page catalog is configured for this app yet.
        </p>
      ) : null}

      {!loading && availablePages.length > 0 ? (
        <div className="mt-3">
          {sortedUsers.length === 0 ? (
            <p className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
              No users found in this app scope.
            </p>
          ) : (
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
              {sortedUsers.map((user) => {
                const saving = Boolean(savingByUserId[user.userId]);
                const draftPageKeys = normalizedPageKeys(draftsByUserId[user.userId] ?? []);
                const persistedPageKeys = normalizedPageKeys(user.pageKeys);
                const dirty = !areSameKeys(draftPageKeys, persistedPageKeys);
                const canManage = canManagePagePermissions(user);
                const pageCountText = draftPageKeys.length > 0
                  ? `${draftPageKeys.length} page(s) selected`
                  : "No restrictions (all pages allowed)";
                return (
                  <article key={user.userId} aria-busy={saving} className="relative rounded-xl border border-slate-200 bg-white p-3">
                    {saving ? (
                      <div className="absolute inset-0 z-10 flex items-center justify-center rounded-xl bg-white/70">
                        <div className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 shadow-sm">
                          <Spinner className="size-3.5" />
                          <span>Saving...</span>
                        </div>
                      </div>
                    ) : null}
                    <div className="flex flex-wrap items-start justify-between gap-2">
                      <div>
                        <p className="text-sm font-semibold text-slate-900">{user.fullName || user.email || user.userId}</p>
                        <p className="text-xs text-slate-600">{user.email || user.userId}</p>
                      </div>
                      <div className="flex flex-wrap items-center gap-1">
                        <span className="rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-[11px] font-medium text-blue-700">
                          {roleLabel(normalizeRole(user.role))}
                        </span>
                        <span className={`rounded-full border px-2 py-0.5 text-[11px] font-medium ${statusChipClass(user.status)}`}>
                          {normalizeStatus(user.status)}
                        </span>
                      </div>
                    </div>

                    <p className="mt-2 text-xs text-slate-600">{pageCountText}</p>
                    <div className="mt-2 grid gap-1 sm:grid-cols-2">
                      {availablePages.map((page) => {
                        const checked = draftPageKeys.includes(String(page.key || "").trim().toLowerCase());
                        const inputId = `page-permission-${user.userId}-${page.key}`;
                        return (
                          <label
                            key={page.key}
                            htmlFor={inputId}
                            title={page.route}
                            className="flex cursor-pointer items-center gap-2 rounded-md px-1 py-1 text-xs text-slate-700 hover:bg-slate-50"
                          >
                            <input
                              id={inputId}
                              type="checkbox"
                              checked={checked}
                              disabled={!canManage || saving}
                              onChange={() => togglePageKey(user.userId, page.key)}
                              className="size-3.5 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                            />
                            <span className="font-medium text-slate-800">{page.label}</span>
                          </label>
                        );
                      })}
                    </div>

                    {!canManage ? (
                      <p className="mt-3 text-[11px] text-slate-500">
                        You cannot edit page permissions for this user.
                      </p>
                    ) : null}

                    {dirty && canManage ? (
                      <div className="mt-3 flex flex-wrap items-center justify-end gap-2">
                        <Button
                          type="button"
                          variant="outline"
                          onClick={() => clearUserRestrictions(user.userId)}
                          disabled={saving || draftPageKeys.length === 0}
                          className="h-8 px-3 text-xs"
                        >
                          Clear restrictions
                        </Button>
                        <Button
                          type="button"
                          onClick={() => void saveUserPagePermissions(user)}
                          disabled={saving}
                          className="h-8 px-3 text-xs"
                        >
                          {saving ? <Spinner className="size-3.5" /> : null}
                          Save
                        </Button>
                      </div>
                    ) : null}
                  </article>
                );
              })}
            </div>
          )}
        </div>
      ) : null}
    </SectionCard>
  );
}
