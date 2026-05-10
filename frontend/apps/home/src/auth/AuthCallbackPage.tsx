import { useEffect, useState } from "react";

import { PageBanner } from "@/components/layout/PageBanner";
import { useAuth } from "@shared/auth/useAuth";

export function AuthCallbackPage() {
  const auth = useAuth();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      try {
        await auth.completeCallbackFromUrl(window.location.href);
        if (!cancelled) {
          window.history.replaceState({}, "", "/");
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to finish auth callback");
        }
      }
    }

    void run();
    return () => {
      cancelled = true;
    };
  }, [auth]);

  return (
    <div className="mx-auto flex w-full max-w-[720px] flex-col gap-6">
      <PageBanner eyebrow="Auth" title="Completing Sign-In" description="Please wait while we finish authentication." />
      {error ? (
        <section className="rounded-2xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">{error}</section>
      ) : null}
    </div>
  );
}
