import { CheckCircle2, KeyRound, Lightbulb, Save, ShieldCheck, UserCircle2, UserRound } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { PageBanner } from "@shell/components/layout/PageBanner";
import { Button } from "@tradsphere/components/ui/button";
import { Input } from "@tradsphere/components/ui/input";
import { Spinner } from "@tradsphere/components/ui/spinner";
import { getAccessAssignments, roleLabel } from "@shared/auth/accessAssignments";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { MIN_PASSWORD_LENGTH, PASSWORD_RULE_MESSAGE, validatePasswordAgainstPolicy } from "@shared/auth/passwordRules";
import { useAuth } from "@shared/auth/useAuth";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";

type ProfileDraft = {
  firstName: string;
  lastName: string;
};

type PasswordDraft = {
  newPassword: string;
  confirmPassword: string;
};

function splitFullName(value: string | null | undefined): ProfileDraft {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return { firstName: "", lastName: "" };
  }
  const parts = normalized.split(/\s+/);
  if (parts.length === 1) {
    return { firstName: parts[0], lastName: "" };
  }
  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(" "),
  };
}

function composeFullName(firstName: string, lastName: string): string {
  return [firstName.trim(), lastName.trim()].filter(Boolean).join(" ").trim();
}

function readUserName(auth: ReturnType<typeof useAuth>): ProfileDraft {
  return splitFullName(auth.accessProfile?.user?.fullName || auth.user?.fullName || "");
}

function validatePasswordDraft(draft: PasswordDraft): string | null {
  const password = String(draft.newPassword || "");
  const confirm = String(draft.confirmPassword || "");
  const policyError = validatePasswordAgainstPolicy(password);
  if (policyError) {
    return policyError;
  }
  if (!confirm) {
    return "Confirm your new password.";
  }
  if (password !== confirm) {
    return "New password and confirmation must match.";
  }
  return null;
}

function mapPasswordError(error: unknown): string {
  const text = String(error instanceof Error ? error.message : "").trim().toLowerCase();
  if (!text) {
    return "Unable to update password right now. Please try again.";
  }
  if (text.includes("expired") || text.includes("sign in again")) {
    return "Your session expired. Sign in again, then retry password update.";
  }
  if (text.includes("weak") || text.includes("password should")) {
    return "Password does not meet security requirements. Use a stronger password.";
  }
  if (text.includes("same") && text.includes("password")) {
    return "Choose a new password that is different from your current password.";
  }
  if (text.includes("reauthentication") || text.includes("nonce")) {
    return "For security, sign in again before changing your password.";
  }
  return "Unable to update password right now. Please try again.";
}

export default function ProfilePage() {
  const auth = useAuth();
  const { requestJson } = useApiRequest();
  const [draft, setDraft] = useState<ProfileDraft>(() => readUserName(auth));
  const [baseline, setBaseline] = useState<ProfileDraft>(() => readUserName(auth));
  const [saving, setSaving] = useState(false);
  const [profileError, setProfileError] = useState<string | null>(null);
  const [profileNotice, setProfileNotice] = useState<string | null>(null);

  const [passwordDraft, setPasswordDraft] = useState<PasswordDraft>({ newPassword: "", confirmPassword: "" });
  const [updatingPassword, setUpdatingPassword] = useState(false);
  const [passwordError, setPasswordError] = useState<string | null>(null);
  const [passwordNotice, setPasswordNotice] = useState<string | null>(null);

  useEffect(() => {
    const next = readUserName(auth);
    setDraft(next);
    setBaseline(next);
  }, [auth.accessProfile?.user?.fullName, auth.user?.fullName]);

  const normalizedFirstName = draft.firstName.trim();
  const normalizedLastName = draft.lastName.trim();
  const isProfileChanged = normalizedFirstName !== baseline.firstName.trim() || normalizedLastName !== baseline.lastName.trim();
  const canSaveProfile = Boolean(normalizedFirstName && normalizedLastName && isProfileChanged);

  const passwordValidationError = useMemo(() => validatePasswordDraft(passwordDraft), [passwordDraft]);
  const hasPasswordInput = Boolean(passwordDraft.newPassword || passwordDraft.confirmPassword);
  const canSubmitPassword = Boolean(hasPasswordInput && !passwordValidationError && !updatingPassword);
  const shouldShowPasswordAction = hasPasswordInput || updatingPassword;

  const permissions = auth.accessProfile?.permissions ?? [];
  const currentAppCode = String(auth.accessProfile?.app?.code || "").trim().toLowerCase() || "tradsphere";
  const roleKey = String(auth.accessProfile?.role || "").trim().toLowerCase();
  const isSuperAdmin = roleKey === "super_admin" || permissions.includes("workspace.super_admin");

  const highestRoleLabel = useMemo(() => {
    if (isSuperAdmin) {
      return "Super Admin";
    }
    if (roleKey === "admin") {
      return "Admin";
    }
    if (roleKey === "editor") {
      return "Editor";
    }
    return "Viewer";
  }, [isSuperAdmin, roleKey]);

  const appsAvailable = useMemo(() => {
    const apps = new Set<string>();
    for (const permission of permissions) {
      const prefix = String(permission || "").split(".")[0]?.trim().toLowerCase();
      if (prefix && prefix !== "workspace") {
        apps.add(prefix);
      }
    }
    if (currentAppCode) {
      apps.add(currentAppCode);
    }
    return Array.from(apps).sort((a, b) => a.localeCompare(b));
  }, [currentAppCode, permissions]);

  const appAccessRows = useMemo(() => {
    const assignments = getAccessAssignments(auth.accessProfile);
    if (assignments.length > 0) {
      return assignments.map((assignment) => ({
        id: `${assignment.appCode}::${assignment.tenantSlug}`,
        appName: assignment.appName || assignment.appCode[0].toUpperCase() + assignment.appCode.slice(1),
        role: roleLabel(assignment.role),
        scope: assignment.tenantName ? `${assignment.tenantName} (${assignment.tenantSlug})` : assignment.tenantSlug,
        note: "Tenant-scoped app access",
      }));
    }
    if (isSuperAdmin) {
      return [
        {
          id: "workspace::super_admin",
          appName: "Workspace",
          role: "Super Admin",
          scope: "Global access",
          note: "Global workspace access",
        },
      ];
    }
    return [
      {
        id: `${currentAppCode}::${highestRoleLabel.toLowerCase()}`,
        appName: currentAppCode ? currentAppCode[0].toUpperCase() + currentAppCode.slice(1) : "Tradsphere",
        role: highestRoleLabel,
        scope: auth.tenantSlug ? `Tenant: ${auth.tenantSlug}` : "Tenant scoped",
        note: "Tenant-scoped app access",
      },
    ];
  }, [auth.accessProfile, auth.tenantSlug, currentAppCode, highestRoleLabel, isSuperAdmin]);

  async function handleSaveProfile() {
    if (!canSaveProfile) {
      return;
    }
    const normalizedFullName = composeFullName(normalizedFirstName, normalizedLastName);
    if (!normalizedFullName) {
      setProfileError("First name and last name are required.");
      return;
    }

    setSaving(true);
    setProfileError(null);
    setProfileNotice(null);
    try {
      const payload = await requestJson("/api/auth/v1/session/me/profile", {
        method: "PATCH",
        body: {
          profile: {
            firstName: normalizedFirstName,
            lastName: normalizedLastName,
            fullName: normalizedFullName,
          },
        },
        successToast: {
          title: "Profile updated",
          message: "Your profile details are saved.",
        },
      });
      const resolvedData =
        payload && typeof payload === "object" && "data" in (payload as Record<string, unknown>)
          ? ((payload as { data?: unknown }).data as Record<string, unknown> | undefined)
          : ((payload as Record<string, unknown> | null) ?? {});
      const userRecord = (resolvedData && typeof resolvedData.user === "object" ? resolvedData.user : null) as Record<string, unknown> | null;
      const fullName = String(userRecord?.fullName || normalizedFullName).trim();
      const next = splitFullName(fullName);
      setDraft(next);
      setBaseline(next);
      setProfileNotice("Profile details were updated successfully.");
      auth.refreshAccessProfile({ force: true });
    } catch (error) {
      const message = String(error instanceof Error ? error.message : "").trim();
      setProfileError(message || "Unable to update profile right now. Please try again.");
    } finally {
      setSaving(false);
    }
  }

  async function handleUpdatePassword() {
    if (!canSubmitPassword) {
      return;
    }

    setUpdatingPassword(true);
    setPasswordError(null);
    setPasswordNotice(null);
    try {
      await auth.updatePassword(passwordDraft.newPassword);
      setPasswordDraft({ newPassword: "", confirmPassword: "" });
      setPasswordNotice("Password updated successfully.");
    } catch (error) {
      setPasswordError(mapPasswordError(error));
    } finally {
      setUpdatingPassword(false);
    }
  }

  return (
    <AppPageLayout
      className="max-w-[1040px] gap-6 pb-5"
      banner={(
        <PageBanner
          eyebrow="TheSphereWorks"
          title="My Profile"
          description="Manage your personal profile and account security for workspace access."
          gradientVariant="workspace"
          className="[&>div.relative]:min-h-[132px] [&>div.relative]:py-6 md:[&>div.relative]:min-h-[156px] md:[&>div.relative]:py-8"
        />
      )}
    >

      <section className="rounded-3xl border border-blue-100/90 bg-white/95 p-5 shadow-soft md:p-6">
        <div className="flex items-center gap-2">
          <UserCircle2 className="size-4 text-blue-700" />
          <h2 className="text-base font-semibold text-slate-900">Account summary</h2>
        </div>
        <p className="mt-1 text-sm text-slate-600">Workspace identity and access summary.</p>

        <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <div className="rounded-2xl border border-slate-200/90 bg-slate-50/80 px-4 py-3">
            <p className="text-[11px] uppercase tracking-[0.14em] text-slate-500">Signed-in Email</p>
            <p className="mt-1 break-all text-sm font-semibold text-slate-800">{auth.user?.email || "-"}</p>
          </div>
          <div className="rounded-2xl border border-blue-200/90 bg-blue-50/70 px-4 py-3">
            <p className="text-[11px] uppercase tracking-[0.14em] text-blue-700/75">Access Level</p>
            <p className="mt-1 text-sm font-semibold text-slate-900">{highestRoleLabel}</p>
            <p className="mt-1 text-xs text-slate-600">{isSuperAdmin ? "Global workspace access" : "Scoped app access"}</p>
          </div>
          <div className="rounded-2xl border border-indigo-200/90 bg-indigo-50/70 px-4 py-3">
            <p className="text-[11px] uppercase tracking-[0.14em] text-indigo-700/75">Apps Available</p>
            <p className="mt-1 text-sm font-semibold text-slate-900">{appsAvailable.length || 1}</p>
            <p className="mt-1 text-xs text-slate-600">{appsAvailable.map((item) => item[0].toUpperCase() + item.slice(1)).join(", ") || "Tradsphere"}</p>
          </div>
          <div className="rounded-2xl border border-emerald-200/90 bg-emerald-50/70 px-4 py-3">
            <p className="text-[11px] uppercase tracking-[0.14em] text-emerald-700/75">Account Status</p>
            <p className="mt-1 text-sm font-semibold text-slate-900">{auth.status === "authenticated" ? "Active" : "Unavailable"}</p>
            <p className="mt-1 text-xs text-slate-600">Authenticated workspace account</p>
          </div>
        </div>

        <div className="mt-4">
          <p className="text-xs uppercase tracking-[0.14em] text-slate-500">App Access</p>
          <div className="mt-2 space-y-2">
            {appAccessRows.map((row) => (
              <div key={row.id} className="rounded-xl border border-slate-200/80 bg-slate-50/55 px-3 py-2">
                <div className="flex flex-wrap items-center gap-2">
                  <p className="text-sm font-semibold text-slate-900">{row.appName}</p>
                  <span className="inline-flex items-center rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-[11px] font-medium text-blue-700">
                    {row.role}
                  </span>
                  <span className="inline-flex items-center rounded-full border border-slate-200 bg-white px-2 py-0.5 text-[11px] text-slate-600">
                    {row.scope}
                  </span>
                </div>
                <p className="mt-1 text-xs text-slate-600">{row.note}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <div className="grid gap-6 lg:grid-cols-2">
        <section className="rounded-3xl border border-blue-100/90 bg-white/95 p-5 shadow-soft md:p-6">
          <div className="flex items-center gap-2">
            <UserRound className="size-4 text-blue-700" />
            <h2 className="text-base font-semibold text-slate-900">Profile details</h2>
          </div>
          <p className="mt-1 text-sm text-slate-600">Keep your profile up to date so teammates see accurate account details.</p>

          <div className="mt-4 grid gap-4 sm:grid-cols-2">
            <div className="sm:col-span-2">
              <p className="mb-1 text-xs uppercase tracking-[0.16em] text-slate-500">Email</p>
              <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">{auth.user?.email || "-"}</div>
            </div>
            <div>
              <p className="mb-1 text-xs uppercase tracking-[0.16em] text-slate-500">First name</p>
              <Input
                value={draft.firstName}
                onChange={(event) => {
                  setDraft((current) => ({ ...current, firstName: event.target.value }));
                  setProfileError(null);
                  setProfileNotice(null);
                }}
                autoComplete="given-name"
                maxLength={120}
              />
            </div>
            <div>
              <p className="mb-1 text-xs uppercase tracking-[0.16em] text-slate-500">Last name</p>
              <Input
                value={draft.lastName}
                onChange={(event) => {
                  setDraft((current) => ({ ...current, lastName: event.target.value }));
                  setProfileError(null);
                  setProfileNotice(null);
                }}
                autoComplete="family-name"
                maxLength={120}
              />
            </div>
          </div>

          {profileError ? (
            <p className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{profileError}</p>
          ) : null}
          {profileNotice ? (
            <p className="mt-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{profileNotice}</p>
          ) : null}

          {canSaveProfile || saving ? (
            <div className="mt-4">
              <Button onClick={() => void handleSaveProfile()} disabled={!canSaveProfile || saving}>
                {saving ? <Spinner className="size-4" /> : <Save className="size-4" />}
                {saving ? "Saving..." : "Save profile"}
              </Button>
            </div>
          ) : null}
        </section>

        <section className="rounded-3xl border border-blue-100/90 bg-white/95 p-5 shadow-soft md:p-6">
          <div className="flex items-center gap-2">
            <ShieldCheck className="size-4 text-blue-700" />
            <h2 className="text-base font-semibold text-slate-900">Security</h2>
          </div>
          <p className="mt-1 text-sm text-slate-600">Change your password to keep your workspace account secure.</p>

          <div className="mt-4 grid gap-4 sm:grid-cols-2">
            <div className="sm:col-span-2">
              <p className="mb-1 text-xs uppercase tracking-[0.16em] text-slate-500">New password</p>
              <Input
                type="password"
                value={passwordDraft.newPassword}
                onChange={(event) => {
                  setPasswordDraft((current) => ({ ...current, newPassword: event.target.value }));
                  setPasswordError(null);
                  setPasswordNotice(null);
                }}
                autoComplete="new-password"
                placeholder={`At least ${MIN_PASSWORD_LENGTH} characters`}
              />
            </div>
            <div className="sm:col-span-2">
              <p className="mb-1 text-xs uppercase tracking-[0.16em] text-slate-500">Confirm new password</p>
              <Input
                type="password"
                value={passwordDraft.confirmPassword}
                onChange={(event) => {
                  setPasswordDraft((current) => ({ ...current, confirmPassword: event.target.value }));
                  setPasswordError(null);
                  setPasswordNotice(null);
                }}
                autoComplete="new-password"
                placeholder="Re-enter new password"
              />
            </div>
          </div>

          <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
            <p className="flex items-start gap-1.5">
              <Lightbulb className="mt-0.5 size-3.5 text-amber-600" />
              <span>{PASSWORD_RULE_MESSAGE}</span>
            </p>
          </div>

          {hasPasswordInput && passwordValidationError ? (
            <p className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{passwordValidationError}</p>
          ) : null}
          {passwordError ? (
            <p className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{passwordError}</p>
          ) : null}
          {passwordNotice ? (
            <p className="mt-3 inline-flex items-center gap-2 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
              <CheckCircle2 className="size-4" />
              {passwordNotice}
            </p>
          ) : null}

          {shouldShowPasswordAction ? (
            <div className="mt-4">
              <Button onClick={() => void handleUpdatePassword()} disabled={!canSubmitPassword || updatingPassword}>
                {updatingPassword ? <Spinner className="size-4" /> : <KeyRound className="size-4" />}
                {updatingPassword ? "Updating..." : "Update password"}
              </Button>
            </div>
          ) : null}
        </section>
      </div>
    </AppPageLayout>
  );
}
