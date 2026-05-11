import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { useAuth } from "@shared/auth/useAuth";

export function PendingInvitePage() {
  const auth = useAuth();

  return (
    <div className="mx-auto flex w-full max-w-[760px] flex-col gap-6">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="Invitation pending"
        description="Your invitation is pending or has not been accepted yet."
        gradientVariant="workspace"
      />

      <section className="rounded-2xl border border-slate-200 bg-white/95 p-5 shadow-soft">
        <p className="text-sm text-slate-700">
          Your account exists, but workspace access is not active yet. Contact your workspace administrator if this invitation should already be active.
        </p>

        <div className="mt-4 grid gap-3 sm:grid-cols-2">
          <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Signed-in email</p>
            <p className="mt-1 text-sm font-medium text-slate-900">{auth.user?.email || "-"}</p>
          </div>
          <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Active workspace</p>
            <p className="mt-1 text-sm font-medium text-slate-900">{auth.tenantSlug || "-"}</p>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          <Button variant="secondary" onClick={() => window.location.replace("/auth/login")}>Back to login</Button>
          <Button
            variant="outline"
            onClick={() => {
              void auth.signOut();
              window.location.replace("/auth/login");
            }}
          >
            Sign out
          </Button>
        </div>
      </section>
    </div>
  );
}
