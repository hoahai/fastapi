import { ArrowRight } from "lucide-react";

import { APP_NAV_ITEMS } from "@/components/layout/navigation";
import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { Section, SectionHeader } from "@shared/components";

type WorkspacePortalPageProps = {
  onNavigate: (route: string) => void;
};

export function WorkspacePortalPage({ onNavigate }: WorkspacePortalPageProps) {
  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-6">
      <PageBanner
        eyebrow="Workspace Portal"
        title="Workspace"
        description="Select an app to continue."
        gradientVariant="workspace"
        action={
          <Button onClick={() => onNavigate("/tradsphere/home")}>Open Tradsphere</Button>
        }
      />

      <Section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
        <SectionHeader
          title="Announcements"
          description="Important updates for all workspace users."
        />
        {/* TODO: Replace this placeholder when a backend announcements source is available. */}
        <p className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
          No announcements right now.
        </p>
      </Section>

      <Section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
        <SectionHeader title="Apps" description="Open an available app or preview upcoming workspaces." />

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 md:grid-cols-3">
          {APP_NAV_ITEMS.map((app) => {
            const Icon = app.icon;
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
                      app.available ? "bg-emerald-50 text-emerald-700" : "bg-slate-100 text-slate-500"
                    }`}
                  >
                    {app.available ? "Available" : "Soon"}
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
