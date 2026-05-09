import { PageBanner } from "@/components/layout/PageBanner";
import type { ReactNode } from "react";

type HeroBannerProps = {
  action?: ReactNode;
};

export function HeroBanner({ action }: HeroBannerProps) {
  return (
    <PageBanner
      eyebrow="tradsphere"
      title="Accounts"
      description="Manage account-level schedules, stations, and estimate operations."
      action={action}
    />
  );
}
