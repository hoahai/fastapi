import type { ReactNode } from "react";

import { cn } from "@tradsphere/lib/utils";

type PageBannerProps = {
  eyebrow: string;
  title: string;
  description: string;
  action?: ReactNode;
  className?: string;
  gradientVariant?: "app" | "workspace" | "admin" | "shiftzy";
};

const bannerGradientByVariant: Record<NonNullable<PageBannerProps["gradientVariant"]>, string> = {
  app: "bg-[radial-gradient(circle_at_82%_30%,rgba(251,146,60,0.45),transparent_34%),radial-gradient(circle_at_58%_30%,rgba(239,68,68,0.38),transparent_42%),radial-gradient(circle_at_10%_34%,rgba(76,29,149,0.48),transparent_38%),linear-gradient(135deg,#f8fafc_6%,#eef2ff_42%,#ffffff_70%)]",
  workspace:
    "bg-[radial-gradient(circle_at_18%_24%,rgba(56,189,248,0.28),transparent_42%),radial-gradient(circle_at_82%_20%,rgba(99,102,241,0.3),transparent_42%),radial-gradient(circle_at_56%_78%,rgba(167,139,250,0.24),transparent_50%),linear-gradient(135deg,#f8fbff_4%,#eef2ff_48%,#f5f3ff_100%)]",
  admin:
    "bg-[radial-gradient(circle_at_14%_24%,rgba(37,99,235,0.26),transparent_42%),radial-gradient(circle_at_86%_22%,rgba(147,51,234,0.24),transparent_42%),radial-gradient(circle_at_52%_80%,rgba(13,148,136,0.24),transparent_50%),linear-gradient(135deg,#f8fafc_5%,#eff6ff_46%,#f5f3ff_72%,#f0fdfa_100%)]",
  shiftzy:
    "bg-[radial-gradient(circle_at_16%_24%,rgba(14,165,233,0.28),transparent_44%),radial-gradient(circle_at_82%_22%,rgba(59,130,246,0.26),transparent_40%),radial-gradient(circle_at_62%_78%,rgba(16,185,129,0.2),transparent_48%),linear-gradient(135deg,#f7fbff_4%,#e8f2ff_45%,#eefcf7_100%)]",
};

export function PageBanner({
  eyebrow,
  title,
  description,
  action,
  className,
  gradientVariant = "app",
}: PageBannerProps) {
  return (
    <section className={cn("relative overflow-hidden rounded-3xl border border-blue-100 bg-white shadow-soft", className)}>
      <div className={cn("absolute inset-0", bannerGradientByVariant[gradientVariant])} />
      <div className="relative flex min-h-[150px] flex-col justify-center gap-2 px-8 py-8 md:min-h-[200px] md:px-12 md:py-10">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-blue-600/80">{eyebrow}</p>
        <h1 className="text-4xl font-semibold tracking-tight text-slate-900 md:text-5xl">{title}</h1>
        <p className="max-w-xl text-sm text-slate-700">{description}</p>
        {action ? <div className="pt-3">{action}</div> : null}
      </div>
    </section>
  );
}
