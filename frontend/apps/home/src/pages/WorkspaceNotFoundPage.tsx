import { LayoutDashboard } from "lucide-react";

import { HOME_ROUTE } from "@shell/components/layout/navigation";
import { PageBanner } from "@shell/components/layout/PageBanner";
import { Button } from "@tradsphere/components/ui/button";

type WorkspaceNotFoundPageProps = {
  onNavigate: (route: string) => void;
};

export function WorkspaceNotFoundPage({ onNavigate }: WorkspaceNotFoundPageProps) {
  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-6">
      <PageBanner
        eyebrow="Workspace"
        title="Page Not Found"
        description="This frontend route does not exist. Use the navigation to continue."
        action={
          <Button onClick={() => onNavigate(HOME_ROUTE)}>
            <LayoutDashboard className="size-4" />
            Back to Workspace
          </Button>
        }
      />
      <section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft">
        <p className="text-sm text-slate-600">
          Only apps and pages that your account can access are shown in navigation. If you expected a page here, contact your workspace administrator.
        </p>
      </section>
    </div>
  );
}
