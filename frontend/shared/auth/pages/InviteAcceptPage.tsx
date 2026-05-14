import { useEffect, useMemo, useRef, useState } from "react";

import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { PASSWORD_RULE_MESSAGE, validatePasswordAgainstPolicy } from "@shared/auth/passwordRules";
import { useAuth } from "@shared/auth/useAuth";

type InviteAcceptPageProps = {
  token: string;
};

type InvitePayload = {
  id?: string;
  email?: string;
  tenantId?: string;
  appId?: string;
  role?: string;
  status?: string;
  expiresAt?: string;
  acceptedAt?: string;
  createdAt?: string;
};

type InviteViewState =
  | "loading"
  | "load_error"
  | "needs_sign_in"
  | "email_mismatch"
  | "ready"
  | "accepting"
  | "accepted"
  | "closed";

function composeFullName(firstName: string, lastName: string): string {
  return [firstName.trim(), lastName.trim()].filter(Boolean).join(" ").trim();
}

function splitFullName(value: string | null | undefined): { firstName: string; lastName: string } {
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

function unwrapResponse(payload: unknown): Record<string, unknown> | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }
  const asRecord = payload as Record<string, unknown>;
  const candidate = "data" in asRecord ? asRecord.data : payload;
  if (!candidate || typeof candidate !== "object") {
    return null;
  }
  return candidate as Record<string, unknown>;
}

function normalizeInvitePayload(input: unknown): InvitePayload | null {
  const record = unwrapResponse(input);
  if (!record) {
    return null;
  }
  return {
    id: typeof record.id === "string" ? record.id : undefined,
    email: typeof record.email === "string" ? record.email : undefined,
    tenantId: typeof record.tenantId === "string" ? record.tenantId : undefined,
    appId: typeof record.appId === "string" ? record.appId : undefined,
    role: typeof record.role === "string" ? record.role : undefined,
    status: typeof record.status === "string" ? record.status : undefined,
    expiresAt: typeof record.expiresAt === "string" ? record.expiresAt : undefined,
    acceptedAt: typeof record.acceptedAt === "string" ? record.acceptedAt : undefined,
    createdAt: typeof record.createdAt === "string" ? record.createdAt : undefined,
  };
}

function toDisplayDate(value?: string): string {
  if (!value) {
    return "-";
  }
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(timestamp));
}

function normalizeAuthError(error: unknown): string {
  const raw = error instanceof Error ? error.message : "";
  const text = raw.trim().toLowerCase();
  if (!text) {
    return "Could not accept this invitation right now.";
  }
  if (text.includes("email does not match")) {
    return "This invitation was issued for a different email account.";
  }
  if (text.includes("expired")) {
    return "This invitation has expired.";
  }
  if (text.includes("not pending")) {
    return "This invitation is no longer pending.";
  }
  if (text.includes("not found")) {
    return "Invitation not found.";
  }
  if (text.includes("network") || text.includes("failed to fetch")) {
    return "Network issue detected. Please retry.";
  }
  return "Could not accept this invitation right now.";
}

function normalizeStatus(status?: string): string {
  const value = String(status || "unknown").trim().toLowerCase();
  if (!value) {
    return "unknown";
  }
  return value;
}

export function InviteAcceptPage({ token }: InviteAcceptPageProps) {
  const auth = useAuth();
  const [state, setState] = useState<InviteViewState>("loading");
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<InvitePayload | null>(null);
  const [registerPassword, setRegisterPassword] = useState("");
  const [registerConfirmPassword, setRegisterConfirmPassword] = useState("");
  const [registering, setRegistering] = useState(false);
  const [registerMessage, setRegisterMessage] = useState<string | null>(null);
  const [profileFirstName, setProfileFirstName] = useState("");
  const [profileLastName, setProfileLastName] = useState("");
  const [profileError, setProfileError] = useState<string | null>(null);
  const autoAcceptAttemptedRef = useRef(false);

  const inviteStatus = normalizeStatus(payload?.status);
  const signedInEmail = String(auth.user?.email || "").trim().toLowerCase();
  const inviteEmail = String(payload?.email || "").trim().toLowerCase();
  const workspaceLabel = auth.tenantSlug || String(import.meta.env.VITE_DEFAULT_TENANT_SLUG || "").trim().toLowerCase();
  const canCreatePassword = Boolean(inviteEmail);
  const registerPasswordValidationError = useMemo(() => {
    const policyError = validatePasswordAgainstPolicy(registerPassword);
    if (policyError) {
      return policyError;
    }
    if (!registerConfirmPassword) {
      return "Confirm your password.";
    }
    if (registerPassword !== registerConfirmPassword) {
      return "Passwords do not match.";
    }
    return null;
  }, [registerConfirmPassword, registerPassword]);
  const canSubmitCreatePassword = Boolean(registerPassword && registerConfirmPassword && !registerPasswordValidationError && !registering);
  const loginHref = useMemo(
    () => `/auth/login?return_to=${encodeURIComponent(`/auth/invite/${token}?auto_accept=1`)}`,
    [token],
  );
  const autoAcceptRequested = useMemo(() => {
    if (typeof window === "undefined") {
      return false;
    }
    return String(new URLSearchParams(window.location.search).get("auto_accept") || "").trim() === "1";
  }, []);

  const inviteStateMessage = useMemo(() => {
    if (inviteStatus === "accepted") {
      return "This invitation has already been accepted.";
    }
    if (inviteStatus === "revoked") {
      return "This invitation has been revoked by an administrator.";
    }
    if (inviteStatus === "expired") {
      return "This invitation has expired.";
    }
    return "This invitation is no longer available.";
  }, [inviteStatus]);

  useEffect(() => {
    if (profileFirstName.trim() || profileLastName.trim()) {
      return;
    }
    const seeded = splitFullName(auth.accessProfile?.user?.fullName || auth.user?.fullName || "");
    if (!seeded.firstName && !seeded.lastName) {
      return;
    }
    setProfileFirstName(seeded.firstName);
    setProfileLastName(seeded.lastName);
  }, [auth.accessProfile?.user?.fullName, auth.user?.fullName, profileFirstName, profileLastName]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      setState("loading");
      setError(null);
      setRegisterMessage(null);

      try {
        const response = await fetch(`/api/auth/v1/invitations/${encodeURIComponent(token)}`);
        const json = await response.json().catch(() => null);

        if (!response.ok) {
          throw new Error("Invitation not found or unavailable.");
        }

        const nextPayload = normalizeInvitePayload(json);
        if (!nextPayload) {
          throw new Error("Invitation is unavailable.");
        }

        if (cancelled) {
          return;
        }

        setPayload(nextPayload);

        const status = normalizeStatus(nextPayload.status);
        if (status !== "pending") {
          setState("closed");
          return;
        }

        if (auth.status !== "authenticated" || !auth.session?.accessToken) {
          setState("needs_sign_in");
          return;
        }

        const normalizedInviteEmail = String(nextPayload.email || "").trim().toLowerCase();
        const normalizedUserEmail = String(auth.user?.email || "").trim().toLowerCase();
        if (normalizedInviteEmail && normalizedUserEmail && normalizedInviteEmail !== normalizedUserEmail) {
          setState("email_mismatch");
          return;
        }

        setState("ready");
      } catch (err) {
        if (cancelled) {
          return;
        }
        setError(normalizeAuthError(err));
        setState("load_error");
      }
    }

    void run();
    return () => {
      cancelled = true;
    };
  }, [token, auth.status, auth.session?.accessToken, auth.user?.email]);

  async function handleCreatePasswordAndSignIn() {
    const normalizedInviteEmail = String(payload?.email || "").trim().toLowerCase();
    if (!normalizedInviteEmail) {
      setRegisterMessage("This invitation is missing an email address. Contact your administrator.");
      return;
    }

    const passwordPolicyError = validatePasswordAgainstPolicy(registerPassword);
    if (passwordPolicyError) {
      setRegisterMessage(passwordPolicyError);
      return;
    }

    if (registerPassword !== registerConfirmPassword) {
      setRegisterMessage("Passwords do not match.");
      return;
    }

    setRegisterMessage(null);
    setProfileError(null);
    setRegistering(true);
    try {
      const signUpResult = await auth.signUpWithPassword(normalizedInviteEmail, registerPassword);
      if (signUpResult.status === "confirm_email") {
        setRegisterMessage("Account created. Please verify your email, then sign in to accept this invitation.");
        return;
      }
      setRegisterPassword("");
      setRegisterConfirmPassword("");
      setRegisterMessage("Account created and signed in. Continue below to complete your profile and accept invitation.");
    } catch (err) {
      const raw = err instanceof Error ? err.message : "";
      const text = raw.trim().toLowerCase();
      if (text.includes("already registered") || text.includes("already exists")) {
        setRegisterMessage("This email already has an account. Sign in to continue.");
        return;
      }
      if (text.includes("password")) {
        setRegisterMessage(PASSWORD_RULE_MESSAGE);
        return;
      }
      setRegisterMessage(normalizeAuthError(err));
    } finally {
      setRegistering(false);
    }
  }

  async function handleAccept() {
    if (!auth.session?.accessToken) {
      setError("Please sign in before accepting this invitation.");
      setState("needs_sign_in");
      return;
    }
    const normalizedFirstName = profileFirstName.trim();
    const normalizedLastName = profileLastName.trim();
    const normalizedFullName = composeFullName(normalizedFirstName, normalizedLastName);
    if (!normalizedFirstName || !normalizedLastName || !normalizedFullName) {
      setProfileError("First name and last name are required.");
      return;
    }

    setError(null);
    setProfileError(null);
    setState("accepting");

    try {
      const response = await fetch(`/api/auth/v1/invitations/${encodeURIComponent(token)}/accept`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${auth.session.accessToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          profile: {
            firstName: normalizedFirstName,
            lastName: normalizedLastName,
            fullName: normalizedFullName,
          },
        }),
      });

      const json = await response.json().catch(() => null);
      if (!response.ok) {
        const unwrapped = unwrapResponse(json) ?? ((json && typeof json === "object" ? json : {}) as Record<string, unknown>);
        const detail =
          typeof unwrapped.detail === "string"
            ? unwrapped.detail
            : typeof unwrapped.message === "string"
              ? unwrapped.message
              : "Could not accept this invitation right now.";
        throw new Error(detail);
      }

      setState("accepted");
      window.setTimeout(() => {
        window.location.replace("/tradsphere/home");
      }, 650);
    } catch (err) {
      setError(normalizeAuthError(err));
      setState("ready");
    }
  }

  useEffect(() => {
    if (state !== "ready" || !autoAcceptRequested || autoAcceptAttemptedRef.current) {
      return;
    }
    if (!profileFirstName.trim() || !profileLastName.trim()) {
      return;
    }
    autoAcceptAttemptedRef.current = true;
    void handleAccept();
  }, [state, autoAcceptRequested, profileFirstName, profileLastName]);

  return (
    <div className="mx-auto flex w-full max-w-[860px] flex-col gap-6">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="Accept invitation"
        description="Review your invitation details and confirm workspace access."
        gradientVariant="workspace"
      />

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft">
        {state === "loading" ? (
          <div className="flex items-center gap-3 text-sm text-slate-700">
            <Spinner className="size-4 text-blue-700" />
            <p>Loading invitation details...</p>
          </div>
        ) : null}

        {state === "load_error" ? (
          <div className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
            {error || "Unable to load invitation."}
          </div>
        ) : null}

        {state === "closed" ? (
          <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
            {inviteStateMessage}
          </div>
        ) : null}

        {state === "needs_sign_in" ? (
          <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
            <p className="font-medium text-slate-800">Sign in first to accept this invitation.</p>
            {canCreatePassword ? (
              <div className="mt-4 space-y-3 rounded-xl border border-slate-200 bg-white px-3 py-3">
                <p className="text-sm text-slate-700">
                  First time here? Create a password for <span className="font-semibold">{inviteEmail}</span>.
                </p>
                <Input
                  type="password"
                  value={registerPassword}
                  onChange={(event) => {
                    setRegisterPassword(event.target.value);
                    if (registerMessage) {
                      setRegisterMessage(null);
                    }
                  }}
                  placeholder="Create password"
                  autoComplete="new-password"
                />
                <Input
                  type="password"
                  value={registerConfirmPassword}
                  onChange={(event) => {
                    setRegisterConfirmPassword(event.target.value);
                    if (registerMessage) {
                      setRegisterMessage(null);
                    }
                  }}
                  placeholder="Confirm password"
                  autoComplete="new-password"
                />
                <p className="text-xs text-slate-500">{PASSWORD_RULE_MESSAGE}</p>
                {(registerPassword || registerConfirmPassword) && registerPasswordValidationError ? (
                  <p className="text-xs text-rose-700">{registerPasswordValidationError}</p>
                ) : null}
                {registerMessage ? (
                  <p className="text-xs text-slate-600">{registerMessage}</p>
                ) : null}
                <Button onClick={handleCreatePasswordAndSignIn} disabled={!canSubmitCreatePassword}>
                  {registering ? <Spinner className="size-4" /> : null}
                  {registering ? "Creating account..." : "Create password and sign in"}
                </Button>
              </div>
            ) : null}
            <Button className="mt-3" variant="secondary" onClick={() => window.location.assign(loginHref)}>Go to login</Button>
          </div>
        ) : null}

        {state === "email_mismatch" ? (
          <div className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
            <p>This invitation is for {payload?.email || "another email"}, but you are signed in as {auth.user?.email || "a different user"}.</p>
            <Button className="mt-3" variant="secondary" onClick={() => void auth.signOut()}>
              Sign out
            </Button>
          </div>
        ) : null}

        {state === "ready" || state === "accepting" || state === "accepted" ? (
            <div className="space-y-4">
              <div className="grid gap-3 sm:grid-cols-2">
              <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
                <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Invited email</p>
                <p className="mt-1 text-sm font-medium text-slate-900">{payload?.email || "-"}</p>
              </div>
              <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
                <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Workspace</p>
                <p className="mt-1 text-sm font-medium text-slate-900">{workspaceLabel || payload?.tenantId || "-"}</p>
              </div>
              <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
                <p className="text-xs uppercase tracking-[0.16em] text-slate-500">App</p>
                <p className="mt-1 text-sm font-medium text-slate-900">Tradsphere</p>
              </div>
              <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
                <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Role</p>
                <p className="mt-1 text-sm font-medium text-slate-900">{payload?.role || "-"}</p>
              </div>
              <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
                <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Status</p>
                <p className="mt-1 text-sm font-medium capitalize text-slate-900">{inviteStatus}</p>
              </div>
              <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
                <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Expiration</p>
                <p className="mt-1 text-sm font-medium text-slate-900">{toDisplayDate(payload?.expiresAt)}</p>
              </div>
              </div>

              <div className="rounded-xl border border-slate-200 bg-white px-3 py-3">
                <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Profile setup</p>
                <div className="mt-2 space-y-2">
                  <div className="grid gap-2 sm:grid-cols-2">
                    <Input
                      value={profileFirstName}
                      onChange={(event) => {
                        setProfileFirstName(event.target.value);
                        if (profileError) {
                          setProfileError(null);
                        }
                      }}
                      placeholder="First name"
                      autoComplete="given-name"
                      maxLength={120}
                      disabled={state === "accepting"}
                    />
                    <Input
                      value={profileLastName}
                      onChange={(event) => {
                        setProfileLastName(event.target.value);
                        if (profileError) {
                          setProfileError(null);
                        }
                      }}
                      placeholder="Last name"
                      autoComplete="family-name"
                      maxLength={120}
                      disabled={state === "accepting"}
                    />
                  </div>
                  {profileError ? <p className="text-xs text-rose-700">{profileError}</p> : null}
                </div>
              </div>

              {error ? (
                <p className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</p>
            ) : null}

            {state === "accepted" ? (
              <p className="rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
                Invitation accepted. Redirecting to Tradsphere...
              </p>
            ) : (
              <Button onClick={handleAccept} disabled={state === "accepting" || !profileFirstName.trim() || !profileLastName.trim()}>
                {state === "accepting" ? <Spinner className="size-4" /> : null}
                {state === "accepting" ? "Accepting invitation..." : "Accept invitation"}
              </Button>
            )}
          </div>
        ) : null}

        {state === "load_error" || state === "closed" ? (
          <div className="mt-4 flex flex-wrap gap-2">
            <Button variant="secondary" onClick={() => window.location.replace("/auth/login")}>Back to login</Button>
            <Button variant="outline" onClick={() => window.location.replace("/")}>Back to Workspace Home</Button>
          </div>
        ) : null}

        {state !== "loading" ? (
          <p className="mt-4 text-xs text-slate-500">Signed-in account: {signedInEmail || "Not signed in"}</p>
        ) : null}
        {inviteEmail && signedInEmail && inviteEmail !== signedInEmail ? (
          <p className="mt-1 text-xs text-rose-600">Signed-in email does not match the invited email.</p>
        ) : null}
      </section>
    </div>
  );
}
