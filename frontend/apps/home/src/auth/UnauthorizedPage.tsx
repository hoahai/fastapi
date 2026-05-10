import { PageBanner } from "@/components/layout/PageBanner";

export function UnauthorizedPage() {
  return (
    <div className="mx-auto flex w-full max-w-[720px] flex-col gap-6">
      <PageBanner
        eyebrow="Auth"
        title="Unauthorized"
        description="You are signed in but do not have access to this tenant/app."
      />
    </div>
  );
}
