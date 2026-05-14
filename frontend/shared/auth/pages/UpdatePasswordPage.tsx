import { CheckCircle2, KeyRound } from "lucide-react";
import { useEffect, useMemo, useState, type FormEvent } from "react";

import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { PASSWORD_RULE_MESSAGE, validatePasswordAgainstPolicy } from "@shared/auth/passwordRules";
import { useAuth } from "@shared/auth/useAuth";

type RecoveryTokens = {
  accessToken: string;
  refreshToken: string | null;
  expiresInSeconds: number | null;
  flowType: string | null;
};

function parseRecoveryTokensFromHash(): RecoveryTokens | null {
  if (typeof window === "undefined") {
    return null;
  }
  const hash = String(window.location.hash || "").replace(/^#/, "");
  if (!hash) {
    return null;
  }
  const params = new URLSearchParams(hash);
  const accessToken = String(params.get("access_token") || "").trim();
  if (!accessToken) {
    return null;
  }
  const refreshTokenRaw = String(params.get("refresh_token") || "").trim();
  const expiresRaw = Number(params.get("expires_in"));
  return {
    accessToken,
    refreshToken: refreshTokenRaw || null,
    expiresInSeconds: Number.isFinite(expiresRaw) ? expiresRaw : null,
    flowType: String(params.get("type") || "").trim().toLowerCase() || null,
  };
}

function parseRecoveryHashError(): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  const hash = String(window.location.hash || "").replace(/^#/, "");
  if (!hash) {
    return null;
  }
  const params = new URLSearchParams(hash);
  const errorCode = String(params.get("error_code") || "").trim();
  const description = String(params.get("error_description") || "").trim();
  if (!errorCode && !description) {
    return null;
  }
  return description || "Recovery link is invalid or expired.";
}

function mapRecoveryError(error: unknown): string {
  const text = String(error instanceof Error ? error.message : "").trim().toLowerCase();
  if (!text) {
    return "Unable to update password right now. Please try again.";
  }
  if (text.includes("weak") || text.includes("password should")) {
    return "Password does not meet security requirements. Use a stronger password.";
  }
  if (text.includes("expired") || text.includes("invalid") || text.includes("jwt")) {
    return "Recovery session expired. Request a new password reset email.";
  }
  if (text.includes("reauthentication") || text.includes("nonce")) {
    return "This recovery session requires reauthentication. Request a new reset email.";
  }
  return "Unable to update password right now. Please try again.";
}

function validatePassword(password: string, confirmPassword: string): string | null {
  const policyError = validatePasswordAgainstPolicy(password);
  const normalizedConfirm = String(confirmPassword || "");
  if (policyError) {
    return policyError;
  }
  if (!normalizedConfirm) {
    return "Confirm your new password.";
  }
  if (String(password || "") !== normalizedConfirm) {
    return "New password and confirmation must match.";
  }
  return null;
}

export function UpdatePasswordPage() {
  const auth = useAuth();
  const [recoveryResolved, setRecoveryResolved] = useState(false);
  const [tokenReady, setTokenReady] = useState(false);
  const [tokenError, setTokenError] = useState<string | null>(null);
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [updating, setUpdating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  useEffect(() => {
    if (recoveryResolved) {
      return;
    }
    const hashError = parseRecoveryHashError();
    if (hashError) {
      setTokenError("Recovery link is invalid or expired. Request a new password reset email.");
      setTokenReady(false);
      setRecoveryResolved(true);
      return;
    }
    const tokens = parseRecoveryTokensFromHash();
    if (tokens) {
      if (tokens.flowType && tokens.flowType !== "recovery") {
        setTokenError("This link is not a password recovery link.");
        setTokenReady(false);
        setRecoveryResolved(true);
        return;
      }
      auth.setSessionFromTokens({
        accessToken: tokens.accessToken,
        refreshToken: tokens.refreshToken,
        expiresInSeconds: tokens.expiresInSeconds,
      });
      window.history.replaceState({}, "", window.location.pathname + window.location.search);
      setTokenReady(true);
      setTokenError(null);
      setRecoveryResolved(true);
      return;
    }

    if (auth.status === "authenticated") {
      setTokenReady(true);
      setTokenError(null);
      setRecoveryResolved(true);
      return;
    }

    if (auth.status === "unauthenticated") {
      setTokenError("Open this page from your password reset email to continue.");
      setTokenReady(false);
      setRecoveryResolved(true);
      return;
    }
  }, [auth, recoveryResolved]);
  useEffect(() => {
    if (tokenReady || tokenError) {
      return;
    }
    if (auth.status === "authenticated") {
      setTokenReady(true);
      return;
    }
    if (auth.status === "unauthenticated") {
      setTokenError("Open this page from your password reset email to continue.");
      setTokenReady(false);
    }
  }, [auth.status, tokenError, tokenReady]);

  const validationError = useMemo(() => validatePassword(password, confirmPassword), [password, confirmPassword]);
  const canSubmit = tokenReady && !validationError && !updating;

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canSubmit) {
      return;
    }
    setUpdating(true);
    setError(null);
    setSuccess(null);
    try {
      await auth.updatePassword(password);
      setPassword("");
      setConfirmPassword("");
      setSuccess("Password updated successfully. You can now sign in with your new password.");
    } catch (submitError) {
      setError(mapRecoveryError(submitError));
    } finally {
      setUpdating(false);
    }
  }

  return (
    <div className="mx-auto flex w-full max-w-[920px] flex-col gap-6">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="Update Password"
        description="Create a new password for your workspace account."
        gradientVariant="workspace"
      />

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft sm:p-6">
        {tokenError ? (
          <p className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{tokenError}</p>
        ) : (
          <form className="mx-auto max-w-[440px] space-y-4 rounded-2xl border border-blue-100 bg-white p-4" onSubmit={handleSubmit}>
            <div>
              <h2 className="text-base font-semibold text-slate-900">Set a new password</h2>
              <p className="mt-1 text-sm text-slate-600">{PASSWORD_RULE_MESSAGE}</p>
            </div>
            <Input
              placeholder="New password"
              value={password}
              onChange={(event) => {
                setPassword(event.target.value);
                setError(null);
                setSuccess(null);
              }}
              type="password"
              autoComplete="new-password"
            />
            <Input
              placeholder="Confirm new password"
              value={confirmPassword}
              onChange={(event) => {
                setConfirmPassword(event.target.value);
                setError(null);
                setSuccess(null);
              }}
              type="password"
              autoComplete="new-password"
            />
            {validationError ? (
              <p className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{validationError}</p>
            ) : null}
            <Button
              type="submit"
              disabled={!canSubmit}
              className="w-full"
            >
              {updating ? <Spinner className="size-4" /> : <KeyRound className="size-4" />}
              {updating ? "Updating..." : "Update password"}
            </Button>
          </form>
        )}

        {error ? (
          <p className="mt-4 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</p>
        ) : null}
        {success ? (
          <p className="mt-4 inline-flex items-center gap-2 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
            <CheckCircle2 className="size-4" />
            {success}
          </p>
        ) : null}

        <div className="mt-5 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
          <p>Email/password login remains the only sign-in method for this workspace.</p>
        </div>
      </section>
    </div>
  );
}
