import { PageBanner } from "@/components/layout/PageBanner";

export function PendingInvitePage() {
  return (
    <div className="mx-auto flex w-full max-w-[720px] flex-col gap-6">
      <PageBanner
        eyebrow="Auth"
        title="Pending Invitation"
        description="Your account is signed in but still pending invite acceptance or activation."
      />
    </div>
  );
}
