import { ShieldCheck } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
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
  if (message.includes("supabase frontend env vars are not configured")) {
    return "Sign-in is not configured for this environment yet.";
  }
  return fallback;
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

  const normalizedEmail = useMemo(() => email.trim().toLowerCase(), [email]);
  const postLoginPath = useMemo(() => resolvePostLoginPath(), []);
  const tenantLabel = auth.tenantSlug || String(import.meta.env.VITE_DEFAULT_TENANT_SLUG || "").trim().toLowerCase();

  useEffect(() => {
    if (auth.status === "authenticated" && auth.user) {
      window.location.replace(postLoginPath);
    }
  }, [auth.status, auth.user, postLoginPath]);

  async function handlePasswordLogin() {
    setError(null);
    setIsSigningIn(true);
    try {
      await auth.signInWithPassword(normalizedEmail, password);
      window.location.assign(postLoginPath);
    } catch (err) {
      setError(normalizeAuthError(err, "Unable to sign in right now. Please try again."));
    } finally {
      setIsSigningIn(false);
    }
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
        <div className="mx-auto max-w-[420px] space-y-4 rounded-2xl border border-blue-100 bg-white p-4">
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
          />
          <Input
            placeholder="Password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            type="password"
            autoComplete="current-password"
          />
          <Button
            onClick={handlePasswordLogin}
            disabled={isSigningIn || !normalizedEmail || !password}
            className="w-full"
          >
            {isSigningIn ? <Spinner className="size-4" /> : <ShieldCheck className="size-4" />}
            {isSigningIn ? "Signing in..." : "Sign in"}
          </Button>
        </div>

        {error ? (
          <p className="mt-4 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</p>
        ) : null}

        <div className="mt-5 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
          <p>Access is invite-only. Contact your workspace administrator if you need access.</p>
          {tenantLabel ? <p className="mt-1 text-xs text-slate-500">Active workspace: {tenantLabel}</p> : null}
        </div>
      </section>
    </div>
  );
}
