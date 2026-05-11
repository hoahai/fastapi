import { RotateCw } from "lucide-react";
import { useEffect } from "react";

import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { useAuth } from "@shared/auth/useAuth";

export function AuthCallbackPage() {
  const auth = useAuth();

  useEffect(() => {
    if (auth.status === "authenticated") {
      window.location.replace("/");
    }
  }, [auth.status]);

  return (
    <div className="mx-auto flex w-full max-w-[720px] flex-col gap-6">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="Sign-in callback"
        description="Email link sign-in is disabled for this workspace."
        gradientVariant="workspace"
      />

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft">
        <div className="flex items-center gap-3 text-sm text-slate-700">
          <RotateCw className="size-4 text-blue-700" />
          <p>Use email and password on the login page to continue.</p>
        </div>
        <Button className="mt-4" variant="secondary" onClick={() => window.location.replace("/auth/login")}>Back to login</Button>
      </section>
    </div>
  );
}
