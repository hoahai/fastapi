import { RefreshCw } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { useApiRequest } from "@/hooks/useApiRequest";
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
  pageKeys: string[];
  hasRestrictions: boolean;
};

type PagePermissionsLoadResponse = {
  availablePages?: AppPageOption[];
  items?: PagePermissionUser[];
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
    return "unknown";
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
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

export function TradspherePermissionDetailsSection({
  appCode,
  appLabel,
}: TradspherePermissionDetailsSectionProps) {
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [availablePages, setAvailablePages] = useState<AppPageOption[]>([]);
  const [users, setUsers] = useState<PagePermissionUser[]>([]);
  const [draftsByUserId, setDraftsByUserId] = useState<Record<string, string[]>>({});
  const [savingByUserId, setSavingByUserId] = useState<Record<string, boolean>>({});

  const loadPagePermissions = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const payload = await requestJson("/api/auth/v1/invitations/users/page-permissions", {
        headers: {
          "X-App-Code": appCode,
        },
        successToast: false,
        errorToast: false,
      });
      const data = unwrap<PagePermissionsLoadResponse>(payload, {});
      const nextAvailablePages = Array.isArray(data.availablePages) ? data.availablePages : [];
      const nextUsers = Array.isArray(data.items) ? data.items : [];

      setAvailablePages(nextAvailablePages);
      setUsers(nextUsers);
      setDraftsByUserId(
        nextUsers.reduce<Record<string, string[]>>((acc, user) => {
          acc[user.userId] = normalizedPageKeys(user.pageKeys);
          return acc;
        }, {}),
      );
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "Unable to load page permissions.";
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [appCode, requestJson]);

  useEffect(() => {
    void loadPagePermissions();
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

  return (
    <SectionCard
      title={`${appLabel} Page Permissions`}
      description="Default behavior allows all app pages. Once one or more page permissions are set for a user, that user can access only selected pages."
      className="p-5"
      actions={(
        <Button
          type="button"
          variant="outline"
          onClick={() => void loadPagePermissions()}
          disabled={loading}
          className="h-8 px-2.5 text-xs"
        >
          {loading ? <Spinner className="size-3.5" /> : <RefreshCw className="size-3.5" />}
          Refresh
        </Button>
      )}
    >
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
        <div className="mt-3 space-y-3">
          {sortedUsers.length === 0 ? (
            <p className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
              No users found in this app scope.
            </p>
          ) : (
            sortedUsers.map((user) => {
              const saving = Boolean(savingByUserId[user.userId]);
              const draftPageKeys = normalizedPageKeys(draftsByUserId[user.userId] ?? []);
              const persistedPageKeys = normalizedPageKeys(user.pageKeys);
              const dirty = !areSameKeys(draftPageKeys, persistedPageKeys);
              const pageCountText = draftPageKeys.length > 0
                ? `${draftPageKeys.length} page(s) selected`
                : "No restrictions (all pages allowed)";
              return (
                <article key={user.userId} className="rounded-xl border border-slate-200 bg-white p-3">
                  <div className="flex flex-wrap items-start justify-between gap-2">
                    <div>
                      <p className="text-sm font-semibold text-slate-900">{user.fullName || user.email || user.userId}</p>
                      <p className="text-xs text-slate-600">{user.email || user.userId}</p>
                    </div>
                    <div className="flex flex-wrap items-center gap-1">
                      <span className="rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-[11px] font-medium text-blue-700">
                        {roleLabel(normalizeRole(user.role))}
                      </span>
                      <span className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[11px] font-medium text-slate-700">
                        {normalizeStatus(user.status)}
                      </span>
                    </div>
                  </div>

                  <p className="mt-2 text-xs text-slate-600">{pageCountText}</p>
                  <div className="mt-2 grid gap-1 sm:grid-cols-2 xl:grid-cols-3">
                    {availablePages.map((page) => {
                      const checked = draftPageKeys.includes(String(page.key || "").trim().toLowerCase());
                      const inputId = `page-permission-${user.userId}-${page.key}`;
                      return (
                        <label
                          key={page.key}
                          htmlFor={inputId}
                          className="flex cursor-pointer items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-2 py-1.5 text-xs text-slate-700 hover:border-blue-200 hover:bg-blue-50/50"
                        >
                          <input
                            id={inputId}
                            type="checkbox"
                            checked={checked}
                            onChange={() => togglePageKey(user.userId, page.key)}
                            className="size-3.5 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                          />
                          <span className="font-medium text-slate-800">{page.label}</span>
                          <span className="truncate text-[10px] text-slate-500">{page.route}</span>
                        </label>
                      );
                    })}
                  </div>

                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <Button
                      type="button"
                      onClick={() => void saveUserPagePermissions(user)}
                      disabled={!dirty || saving}
                      className="h-8 px-3 text-xs"
                    >
                      {saving ? <Spinner className="size-3.5" /> : null}
                      Save
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() => clearUserRestrictions(user.userId)}
                      disabled={saving || draftPageKeys.length === 0}
                      className="h-8 px-3 text-xs"
                    >
                      Clear restrictions
                    </Button>
                  </div>
                </article>
              );
            })
          )}
        </div>
      ) : null}
    </SectionCard>
  );
}
