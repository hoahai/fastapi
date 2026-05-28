import { ShieldCheck } from "lucide-react";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";

import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { shouldProtectFrontendAuth } from "@shared/auth/guards";
import { useAuth } from "@shared/auth/useAuth";

function normalizeAuthError(error: unknown, fallback: string): string {
  const raw = error instanceof Error ? error.message : "";
  const message = raw.trim().toLowerCase();
  if (!message) {
    return fallback;
  }
  if (message.includes("invalid login") || message.includes("invalid credentials")) {
    return "Email or password is incorrect.";
  }
  if (message.includes("email not confirmed")) {
    return "Please verify your email first, then try again.";
  }
  if (message.includes("too many") || message.includes("rate limit")) {
    return "Too many attempts. Please wait a minute and try again.";
  }
  if (message.includes("network") || message.includes("failed to fetch")) {
    return "Network issue detected. Check your connection and try again.";
  }
  if (message.includes("disabled")) {
    return "Your account has been disabled. Contact your workspace administrator.";
  }
  if (message.includes("pending activation")) {
    return "Your account is pending activation. Contact your workspace administrator.";
  }
  if (message.includes("supabase frontend env vars are not configured")) {
    return "Sign-in is not configured for this environment yet.";
  }
  return fallback;
}

function normalizeAccessErrorMessage(message: string | null | undefined): string | null {
  const raw = String(message || "").trim();
  if (!raw) {
    return null;
  }
  const normalized = raw.toLowerCase();
  if (normalized.includes("disabled")) {
    return "Your account has been disabled. Contact your workspace administrator.";
  }
  if (normalized.includes("pending activation")) {
    return "Your account is pending activation. Contact your workspace administrator.";
  }
  if (normalized.includes("does not have access to this app")) {
    return "Your account is active, but it has no app access yet. Contact your workspace administrator.";
  }
  if (normalized.includes("tenant membership") || normalized.includes("forbidden")) {
    return "Your account cannot access this workspace. Contact your workspace administrator.";
  }
  return raw;
}

function resolvePostLoginPath(): string {
  if (typeof window === "undefined") {
    return "/";
  }
  const raw = String(new URLSearchParams(window.location.search).get("return_to") || "").trim();
  if (!raw || !raw.startsWith("/") || raw.startsWith("//")) {
    return "/";
  }
  return raw;
}

export function LoginPage() {
  const auth = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isSigningIn, setIsSigningIn] = useState(false);
  const [isAwaitingValidation, setIsAwaitingValidation] = useState(false);
  const passwordInputRef = useRef<HTMLInputElement | null>(null);

  const normalizedEmail = useMemo(() => email.trim().toLowerCase(), [email]);
  const postLoginPath = useMemo(() => resolvePostLoginPath(), []);
  const tenantLabel = auth.tenantSlug || String(import.meta.env.VITE_DEFAULT_TENANT_SLUG || "").trim().toLowerCase();
  const authProtectionEnabled = shouldProtectFrontendAuth();
  const resolvedAccessError = normalizeAccessErrorMessage(auth.accessError);
  const hasNoAppAccessError = String(auth.accessError || "").toLowerCase().includes("does not have access to this app");
  const accessValidationResolved = !authProtectionEnabled
    || (!auth.accessLoading && (Boolean(auth.accessProfile) || Boolean(auth.accessError)));
  const readyForRedirect = auth.status === "authenticated"
    && Boolean(auth.user)
    && (
      !authProtectionEnabled
      || (Boolean(auth.accessProfile) && auth.accessCacheStatus?.source === "network")
      || (accessValidationResolved && hasNoAppAccessError)
    );
  const isBusy = isSigningIn || isAwaitingValidation;

  useEffect(() => {
    if (readyForRedirect) {
      window.location.replace(postLoginPath);
    }
  }, [postLoginPath, readyForRedirect]);

  useEffect(() => {
    if (!isAwaitingValidation) {
      return;
    }
    if (auth.status !== "authenticated") {
      setIsAwaitingValidation(false);
      return;
    }
    if (!accessValidationResolved) {
      return;
    }
    setIsAwaitingValidation(false);
  }, [accessValidationResolved, auth.status, isAwaitingValidation]);

  async function handlePasswordLogin() {
    if (!normalizedEmail || !password || isSigningIn) {
      return;
    }
    auth.clearAuthNotice();
    setError(null);
    setIsAwaitingValidation(false);
    setIsSigningIn(true);
    try {
      await auth.signInWithPassword(normalizedEmail, password);
      setIsAwaitingValidation(true);
    } catch (err) {
      setError(normalizeAuthError(err, "Unable to sign in right now. Please try again."));
      setIsAwaitingValidation(false);
    } finally {
      setIsSigningIn(false);
    }
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await handlePasswordLogin();
  }

  return (
    <div className="mx-auto flex w-full max-w-[920px] flex-col gap-6">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="Welcome to TheSphereWorks"
        description="Sign in with your email and password to access workspace apps."
        gradientVariant="workspace"
      />

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft sm:p-6">
        <form className="mx-auto max-w-[420px] space-y-4 rounded-2xl border border-blue-100 bg-white p-4" onSubmit={handleSubmit}>
          <div>
            <h2 className="text-base font-semibold text-slate-900">Sign in with password</h2>
            <p className="mt-1 text-sm text-slate-600">Use your existing workspace credentials.</p>
          </div>
          <Input
            placeholder="you@company.com"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            type="email"
            autoComplete="email"
            onKeyDown={(event) => {
              if (event.key !== "Enter") {
                return;
              }
              event.preventDefault();
              passwordInputRef.current?.focus();
            }}
          />
          <Input
            placeholder="Password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            type="password"
            autoComplete="current-password"
            ref={passwordInputRef}
          />
          <Button
            type="submit"
            disabled={isBusy || !normalizedEmail || !password}
            className="w-full"
          >
            {isBusy ? <Spinner className="size-4" /> : <ShieldCheck className="size-4" />}
            {isSigningIn ? "Signing in..." : isAwaitingValidation ? "Validating access..." : "Sign in"}
          </Button>
        </form>

        {error ? (
          <p className="mt-4 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</p>
        ) : null}
        {!error && resolvedAccessError ? (
          <p className="mt-4 rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">{resolvedAccessError}</p>
        ) : null}
        {!error && !resolvedAccessError && auth.authNotice ? (
          <p className="mt-4 rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">{auth.authNotice}</p>
        ) : null}

        <div className="mt-5 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
          <p>Access is invite-only. Contact your workspace administrator if you need access.</p>
          {tenantLabel ? <p className="mt-1 text-xs text-slate-500">Active workspace: {tenantLabel}</p> : null}
        </div>
      </section>
    </div>
  );
}
