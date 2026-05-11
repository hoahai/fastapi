import { RefreshCw, ShieldCheck, UserMinus } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";

import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Spinner } from "@/components/ui/spinner";
import { useApiRequest } from "@/hooks/useApiRequest";

type AdminUser = {
  userId: string;
  email: string | null;
  fullName: string | null;
  status: "active" | "pending" | "disabled";
  role: "tradsphere.viewer" | "tradsphere.editor" | "tradsphere.admin" | null;
  createdAt: string | null;
  updatedAt: string | null;
  roleUpdatedAt: string | null;
};

type AdminInvitation = {
  id: string;
  email: string;
  role: "tradsphere.viewer" | "tradsphere.editor" | "tradsphere.admin";
  status: "pending" | "accepted" | "revoked" | "expired";
  expiresAt: string | null;
  createdAt: string | null;
  acceptedAt: string | null;
  inviteUrl: string;
};

type DisableTarget = {
  userId: string;
  email: string | null;
};

const STATUS_OPTIONS = [
  { value: "active", label: "Active" },
  { value: "pending", label: "Pending" },
  { value: "disabled", label: "Disabled" },
];

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

export default function AdminUsersPage() {
  const { requestJson } = useApiRequest();

  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [roles, setRoles] = useState<string[]>(["tradsphere.viewer", "tradsphere.editor", "tradsphere.admin"]);
  const [invitations, setInvitations] = useState<AdminInvitation[]>([]);
  const [error, setError] = useState<string | null>(null);

  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState("tradsphere.viewer");
  const [inviteExpirationHours, setInviteExpirationHours] = useState("72");
  const [creatingInvite, setCreatingInvite] = useState(false);

  const [pendingEdits, setPendingEdits] = useState<Record<string, { status: string; role: string }>>({});
  const [savingUserId, setSavingUserId] = useState<string | null>(null);

  const [disableTarget, setDisableTarget] = useState<DisableTarget | null>(null);
  const [processingDisable, setProcessingDisable] = useState(false);

  const pendingInvitations = useMemo(
    () => invitations.filter((item) => item.status === "pending"),
    [invitations],
  );

  const loadData = useCallback(async (showRefreshing: boolean) => {
    if (showRefreshing) {
      setRefreshing(true);
    } else {
      setLoading(true);
    }
    setError(null);

    try {
      const [usersPayload, invitesPayload, rolesPayload] = await Promise.all([
        requestJson("/api/auth/v1/admin/users", { successToast: false, errorToast: false }),
        requestJson("/api/auth/v1/admin/invitations?status=pending", { successToast: false, errorToast: false }),
        requestJson("/api/auth/v1/admin/roles", { successToast: false, errorToast: false }),
      ]);

      const usersData = unwrap<{ items?: AdminUser[] }>(usersPayload, { items: [] });
      const invitesData = unwrap<{ items?: AdminInvitation[] }>(invitesPayload, { items: [] });
      const rolesData = unwrap<{ roles?: string[] }>(rolesPayload, { roles: [] });

      setUsers(Array.isArray(usersData.items) ? usersData.items : []);
      setInvitations(Array.isArray(invitesData.items) ? invitesData.items : []);
      const nextRoles = Array.isArray(rolesData.roles) && rolesData.roles.length
        ? rolesData.roles
        : ["tradsphere.viewer", "tradsphere.editor", "tradsphere.admin"];
      setRoles(nextRoles);
      setPendingEdits({});
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "Unable to load admin data.";
      setError(message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [requestJson]);

  useEffect(() => {
    void loadData(false);
  }, [loadData]);

  function draftForUser(user: AdminUser): { status: string; role: string } {
    const current = pendingEdits[user.userId];
    if (current) {
      return current;
    }
    return {
      status: user.status,
      role: user.role || "tradsphere.viewer",
    };
  }

  function hasUserChanges(user: AdminUser): boolean {
    const draft = draftForUser(user);
    return draft.status !== user.status || draft.role !== (user.role || "tradsphere.viewer");
  }

  async function handleSaveUser(user: AdminUser) {
    const draft = draftForUser(user);
    if (!hasUserChanges(user)) {
      return;
    }

    setSavingUserId(user.userId);
    try {
      await requestJson(`/api/auth/v1/admin/users/${encodeURIComponent(user.userId)}`, {
        method: "PATCH",
        body: {
          status: draft.status,
          role: draft.role,
        },
        successToast: {
          title: "User access updated",
          message: `${user.email || user.userId} has updated membership settings.`,
        },
      });
      await loadData(true);
    } finally {
      setSavingUserId(null);
    }
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
          message: `${disableTarget.email || disableTarget.userId} no longer has active tenant access.`,
        },
      });
      setDisableTarget(null);
      await loadData(true);
    } finally {
      setProcessingDisable(false);
    }
  }

  async function handleCreateInvite() {
    const normalizedEmail = inviteEmail.trim().toLowerCase();
    if (!normalizedEmail) {
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
          appCode: "tradsphere",
          role: inviteRole,
          expirationHours: Math.floor(expiresHours),
        },
        successToast: {
          title: "Invitation created",
          message: `Invite for ${normalizedEmail} is ready.`,
        },
      });
      const data = unwrap<Record<string, unknown>>(payload, {});
      const inviteUrl = String(data.inviteUrl || "").trim();
      if (inviteUrl) {
        await navigator.clipboard.writeText(inviteUrl).catch(() => undefined);
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

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-6">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="Admin: Users & Access"
        description="Manage tenant membership, app roles, and invitations for TradSphere."
        gradientVariant="workspace"
        action={
          <Button variant="secondary" onClick={() => void loadData(true)} disabled={refreshing || loading}>
            {refreshing ? <Spinner className="size-4" /> : <RefreshCw className="size-4" />}
            {refreshing ? "Refreshing..." : "Refresh"}
          </Button>
        }
      />

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft">
        <div className="flex flex-wrap items-end gap-3">
          <div className="min-w-[220px] flex-1">
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Invite email</p>
            <Input
              value={inviteEmail}
              onChange={(event) => setInviteEmail(event.target.value)}
              placeholder="new.user@company.com"
              type="email"
              autoComplete="email"
            />
          </div>
          <div className="w-[220px]">
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Role</p>
            <Select
              value={inviteRole}
              onValueChange={setInviteRole}
              options={roles.map((role) => ({ value: role, label: role }))}
            />
          </div>
          <div className="w-[160px]">
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Expiry hours</p>
            <Input
              value={inviteExpirationHours}
              onChange={(event) => setInviteExpirationHours(event.target.value)}
              placeholder="72"
              inputMode="numeric"
            />
          </div>
          <Button onClick={() => void handleCreateInvite()} disabled={creatingInvite || !inviteEmail.trim()}>
            {creatingInvite ? <Spinner className="size-4" /> : <ShieldCheck className="size-4" />}
            {creatingInvite ? "Creating..." : "Create invite"}
          </Button>
        </div>
        <p className="mt-3 text-sm text-slate-600">Invite URL is generated server-side and copied to clipboard when available.</p>
      </section>

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft">
        <div className="flex items-center justify-between">
          <h2 className="text-base font-semibold text-slate-900">Pending invitations</h2>
          <p className="text-sm text-slate-500">{pendingInvitations.length} pending</p>
        </div>
        {pendingInvitations.length === 0 ? (
          <p className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">No pending invitations.</p>
        ) : (
          <div className="mt-3 space-y-3">
            {pendingInvitations.map((invite) => (
              <div key={invite.id} className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <p className="text-sm font-semibold text-slate-900">{invite.email}</p>
                    <p className="text-xs text-slate-500">Role: {invite.role} · Expires: {formatDate(invite.expiresAt)}</p>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      onClick={() => void navigator.clipboard.writeText(invite.inviteUrl).catch(() => undefined)}
                    >
                      Copy link
                    </Button>
                    <Button
                      variant="secondary"
                      onClick={() => void handleRevokeInvite(invite.id, invite.email)}
                    >
                      Revoke
                    </Button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft">
        <div className="flex items-center justify-between">
          <h2 className="text-base font-semibold text-slate-900">Members</h2>
          <p className="text-sm text-slate-500">{users.length} users</p>
        </div>

        {loading ? (
          <div className="mt-3 flex items-center gap-2 text-sm text-slate-600">
            <Spinner className="size-4" /> Loading members...
          </div>
        ) : error ? (
          <p className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</p>
        ) : users.length === 0 ? (
          <p className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">No users found for this tenant.</p>
        ) : (
          <div className="mt-3 overflow-x-auto">
            <table className="w-full min-w-[920px] border-separate border-spacing-y-2">
              <thead>
                <tr className="text-left text-xs uppercase tracking-[0.14em] text-slate-500">
                  <th className="px-3 py-2">User</th>
                  <th className="px-3 py-2">Status</th>
                  <th className="px-3 py-2">Role</th>
                  <th className="px-3 py-2">Created</th>
                  <th className="px-3 py-2">Updated</th>
                  <th className="px-3 py-2">Actions</th>
                </tr>
              </thead>
              <tbody>
                {users.map((user) => {
                  const draft = draftForUser(user);
                  const changed = hasUserChanges(user);
                  const isSaving = savingUserId === user.userId;
                  return (
                    <tr key={user.userId} className="rounded-xl bg-slate-50 text-sm text-slate-700">
                      <td className="rounded-l-xl border border-r-0 border-slate-200 px-3 py-3 align-top">
                        <p className="font-semibold text-slate-900">{user.email || user.userId}</p>
                        <p className="text-xs text-slate-500">{user.fullName || "-"}</p>
                        <p className="text-xs text-slate-500">{user.userId}</p>
                      </td>
                      <td className="border border-l-0 border-r-0 border-slate-200 px-3 py-3 align-top">
                        <Select
                          value={draft.status}
                          onValueChange={(value) => {
                            setPendingEdits((current) => ({
                              ...current,
                              [user.userId]: {
                                status: value,
                                role: (current[user.userId]?.role || draft.role),
                              },
                            }));
                          }}
                          options={STATUS_OPTIONS}
                        />
                      </td>
                      <td className="border border-l-0 border-r-0 border-slate-200 px-3 py-3 align-top">
                        <Select
                          value={draft.role}
                          onValueChange={(value) => {
                            setPendingEdits((current) => ({
                              ...current,
                              [user.userId]: {
                                status: current[user.userId]?.status || draft.status,
                                role: value,
                              },
                            }));
                          }}
                          options={roles.map((role) => ({ value: role, label: role }))}
                        />
                      </td>
                      <td className="border border-l-0 border-r-0 border-slate-200 px-3 py-3 align-top text-xs text-slate-500">
                        {formatDate(user.createdAt)}
                      </td>
                      <td className="border border-l-0 border-r-0 border-slate-200 px-3 py-3 align-top text-xs text-slate-500">
                        {formatDate(user.updatedAt || user.roleUpdatedAt)}
                      </td>
                      <td className="rounded-r-xl border border-l-0 border-slate-200 px-3 py-3 align-top">
                        <div className="flex flex-wrap gap-2">
                          <Button
                            onClick={() => void handleSaveUser(user)}
                            disabled={!changed || isSaving}
                          >
                            {isSaving ? <Spinner className="size-4" /> : null}
                            {isSaving ? "Saving..." : "Save"}
                          </Button>
                          <Button
                            variant="outline"
                            onClick={() => setDisableTarget({ userId: user.userId, email: user.email })}
                            disabled={user.status === "disabled" || isSaving}
                          >
                            <UserMinus className="size-4" />
                            Disable
                          </Button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <Dialog open={Boolean(disableTarget)} onOpenChange={(open) => !open && !processingDisable && setDisableTarget(null)}>
        <DialogContent className="max-w-[560px]">
          <DialogHeader>
            <DialogTitle>Disable tenant access</DialogTitle>
            <DialogDescription>
              This action disables active tenant membership for the selected user. It does not delete the Supabase auth user.
            </DialogDescription>
          </DialogHeader>

          <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
            Disable access for <span className="font-semibold">{disableTarget?.email || disableTarget?.userId}</span>?
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setDisableTarget(null)} disabled={processingDisable}>Keep access</Button>
            <Button variant="secondary" onClick={() => void handleDisableConfirmed()} disabled={processingDisable}>
              {processingDisable ? <Spinner className="size-4" /> : null}
              {processingDisable ? "Disabling..." : "Disable access"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
