import { ArrowRight, Lock, LogIn } from "lucide-react";

import { APP_NAV_ITEMS } from "@/components/layout/navigation";
import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Section, SectionHeader } from "@shared/components";
import { shouldProtectTradsphereFrontend } from "@shared/auth/guards";
import { useAuth } from "@shared/auth/useAuth";
import { useEffect, useMemo, useState } from "react";

type WorkspacePortalPageProps = {
  onNavigate: (route: string) => void;
};

export function WorkspacePortalPage({ onNavigate }: WorkspacePortalPageProps) {
  const auth = useAuth();
  const isSignedIn = auth.status === "authenticated" && Boolean(auth.user);
  const [tenantDraft, setTenantDraft] = useState(auth.tenantSlug || "");

  useEffect(() => {
    setTenantDraft(auth.tenantSlug || "");
  }, [auth.tenantSlug]);

  const appNavItems = useMemo(() => {
    const protectionEnabled = shouldProtectTradsphereFrontend();
    const hasPermission = Boolean(auth.accessProfile?.permissions?.includes("tradsphere.viewer"));
    const canAccessTradsphere = isSignedIn && (!protectionEnabled || hasPermission);

    return APP_NAV_ITEMS.map((item) =>
      item.id === "tradsphere" ? { ...item, available: canAccessTradsphere } : { ...item, available: false },
    );
  }, [auth.accessProfile, isSignedIn]);

  const availableApps = appNavItems.filter((item) => item.available);

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-6">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="Workspace Home"
        description="Select an app to continue. Access is invite-only."
        gradientVariant="workspace"
        action={
          !isSignedIn ? (
            <Button onClick={() => onNavigate("/auth/login")}>
              <LogIn className="size-4" />
              Sign in
            </Button>
          ) : availableApps.some((app) => app.id === "tradsphere") ? (
            <Button onClick={() => onNavigate("/tradsphere/home")}>Open Tradsphere</Button>
          ) : undefined
        }
      />

      {!isSignedIn ? (
        <Section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
          <SectionHeader title="Sign-In Required" description="Sign in to view apps available to your account." />
          <p className="text-sm text-slate-600">Use your email and password on the login page.</p>
        </Section>
      ) : null}

      {isSignedIn && availableApps.length === 0 ? (
        <Section className="rounded-2xl border border-amber-200 bg-amber-50 p-5 shadow-soft">
          <SectionHeader
            title="No App Access Yet"
            description="Your account is signed in, but no workspace apps are currently available."
          />
          <p className="text-sm text-amber-900">Contact your workspace administrator to request access.</p>
        </Section>
      ) : null}

      <Section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
        <SectionHeader
          title="Workspace Context"
          description="Current account and workspace context used for access checks."
        />
        <div className="grid gap-3 sm:grid-cols-3">
          <ContextCard label="Signed-in email" value={auth.user?.email || "Not signed in"} />
          <ContextCard label="Workspace tenant" value={auth.tenantSlug || "-"} />
          <ContextCard label="Auth status" value={auth.status} />
        </div>
        <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 px-3 py-3">
          <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Tenant selector</p>
          <div className="mt-2 flex flex-col gap-2 sm:flex-row">
            <Input
              value={tenantDraft}
              onChange={(event) => setTenantDraft(event.target.value)}
              placeholder="tenant slug (e.g. taaa)"
              className="sm:max-w-[260px]"
            />
            <Button
              variant="secondary"
              onClick={() => auth.setTenantSlug(tenantDraft)}
              disabled={!tenantDraft.trim() || tenantDraft.trim().toLowerCase() === (auth.tenantSlug || "").toLowerCase()}
            >
              Switch tenant
            </Button>
          </div>
        </div>
      </Section>

      <Section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
        <SectionHeader
          title="Announcements"
          description="Important updates for all workspace users."
        />
        <p className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
          No announcements right now.
        </p>
      </Section>

      <Section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
        <SectionHeader title="Apps" description="Open an available app or preview upcoming workspaces." />

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 md:grid-cols-3">
          {appNavItems.map((app) => {
            const Icon = app.icon;
            const isLockedByAuth = !isSignedIn;
            return (
              <article
                key={app.id}
                className="group flex h-full flex-col rounded-2xl border border-blue-100 bg-white/95 p-4 shadow-sm transition hover:-translate-y-0.5 hover:border-blue-300 hover:shadow-md"
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="flex items-center gap-3">
                    <div className="flex size-10 items-center justify-center rounded-xl border border-blue-100 bg-blue-50 text-blue-700">
                      <Icon className="size-5" />
                    </div>
                    <div>
                      <h2 className="text-base font-semibold text-slate-900">{app.label}</h2>
                      <p className="text-sm text-slate-500">{app.description}</p>
                    </div>
                  </div>
                  <span
                    className={`rounded-full px-2.5 py-1 text-xs font-medium ${
                      app.available
                        ? "bg-emerald-50 text-emerald-700"
                        : isLockedByAuth
                          ? "bg-blue-50 text-blue-700"
                          : "bg-slate-100 text-slate-500"
                    }`}
                  >
                    {app.available ? "Available" : isLockedByAuth ? "Sign-in required" : "Soon"}
                  </span>
                </div>

                {app.available ? (
                  <Button
                    className="mt-4 w-full justify-between"
                    onClick={() => onNavigate(app.route)}
                    aria-label={`Open ${app.label}`}
                  >
                    Open {app.label}
                    <ArrowRight className="size-4" />
                  </Button>
                ) : isLockedByAuth ? (
                  <Button className="mt-4 w-full" variant="secondary" onClick={() => onNavigate("/auth/login")}>
                    <Lock className="size-4" />
                    Sign in to check access
                  </Button>
                ) : (
                  <Button className="mt-4 w-full" variant="secondary" disabled>
                    Coming Soon
                  </Button>
                )}
              </article>
            );
          })}
        </div>
      </Section>
    </div>
  );
}

function ContextCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
      <p className="text-xs uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <p className="mt-1 text-sm font-medium text-slate-900">{value}</p>
    </div>
  );
}
