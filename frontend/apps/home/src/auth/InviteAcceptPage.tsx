import { useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import { PageBanner } from "@/components/layout/PageBanner";
import { useAuth } from "@shared/auth/useAuth";

type InviteAcceptPageProps = {
  token: string;
};

export function InviteAcceptPage({ token }: InviteAcceptPageProps) {
  const auth = useAuth();
  const [status, setStatus] = useState<string>("loading");
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      try {
        const response = await fetch(`/api/auth/v1/invitations/${encodeURIComponent(token)}`);
        const json = await response.json().catch(() => null);
        const data = json && typeof json === "object" && "data" in (json as Record<string, unknown>)
          ? (json as { data?: unknown }).data
          : json;
        if (!response.ok) {
          throw new Error("Invitation not found or unavailable.");
        }
        if (!cancelled) {
          setPayload((data && typeof data === "object") ? (data as Record<string, unknown>) : null);
          setStatus("ready");
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load invitation");
          setStatus("error");
        }
      }
    }

    void run();
    return () => {
      cancelled = true;
    };
  }, [token]);

  async function handleAccept() {
    setError(null);
    try {
      if (!auth.session?.accessToken) {
        throw new Error("Please sign in before accepting the invite.");
      }
      const response = await fetch(`/api/auth/v1/invitations/${encodeURIComponent(token)}/accept`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${auth.session.accessToken}`,
        },
      });
      const json = await response.json().catch(() => null);
      if (!response.ok) {
        const unwrapped = json && typeof json === "object" && "error" in (json as Record<string, unknown>)
          ? (json as { error?: Record<string, unknown> }).error
          : json;
        const detail = typeof (unwrapped as { detail?: unknown })?.detail === "string"
          ? (unwrapped as { detail: string }).detail
          : "Failed to accept invitation";
        throw new Error(detail);
      }
      window.history.replaceState({}, "", "/tradsphere/home");
      window.location.reload();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to accept invitation");
    }
  }

  if (status === "loading") {
    return <PageBanner eyebrow="Invite" title="Loading Invite" description="Fetching invite details..." />;
  }

  return (
    <div className="mx-auto flex w-full max-w-[720px] flex-col gap-6">
      <PageBanner eyebrow="Invite" title="Accept Invitation" description="Join the tenant app access from this invitation." />
      <section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft space-y-3">
        <p className="text-sm text-slate-700">Email: {String(payload?.email || "")}</p>
        <p className="text-sm text-slate-700">Role: {String(payload?.role || "")}</p>
        <p className="text-sm text-slate-700">Status: {String(payload?.status || "")}</p>
        <Button onClick={handleAccept}>Accept Invite</Button>
        {error ? <p className="text-sm text-rose-700">{error}</p> : null}
      </section>
    </div>
  );
}
