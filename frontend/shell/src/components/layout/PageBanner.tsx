import type { ReactNode } from "react";

import { cn } from "@shared/components/utils/cn";

type PageBannerProps = {
  eyebrow: string;
  title: string;
  description: string;
  action?: ReactNode;
  className?: string;
  gradientVariant?: "app" | "workspace" | "admin" | "shiftzy" | "tradsphere";
};

const bannerGradientByVariant: Record<NonNullable<PageBannerProps["gradientVariant"]>, string> = {
  app: "bg-[radial-gradient(circle_at_82%_24%,rgba(59,130,246,0.34),transparent_40%),radial-gradient(circle_at_56%_24%,rgba(129,140,248,0.38),transparent_46%),radial-gradient(circle_at_12%_30%,rgba(79,70,229,0.3),transparent_44%),linear-gradient(140deg,#f7faff_4%,#e9f1ff_45%,#f5f9ff_100%)]",
  workspace:
    "bg-[radial-gradient(circle_at_18%_24%,rgba(56,189,248,0.3),transparent_44%),radial-gradient(circle_at_82%_20%,rgba(99,102,241,0.34),transparent_44%),radial-gradient(circle_at_56%_78%,rgba(167,139,250,0.26),transparent_54%),linear-gradient(135deg,#f7fbff_4%,#e8f0ff_50%,#f1f1ff_100%)]",
  admin:
    "bg-[radial-gradient(circle_at_14%_24%,rgba(37,99,235,0.28),transparent_42%),radial-gradient(circle_at_86%_22%,rgba(147,51,234,0.22),transparent_44%),radial-gradient(circle_at_52%_80%,rgba(13,148,136,0.24),transparent_52%),linear-gradient(135deg,#f7fafd_5%,#edf4ff_48%,#f3f2ff_72%,#eefcf9_100%)]",
  shiftzy:
    "bg-[radial-gradient(circle_at_16%_24%,rgba(14,165,233,0.3),transparent_44%),radial-gradient(circle_at_82%_22%,rgba(59,130,246,0.28),transparent_42%),radial-gradient(circle_at_62%_78%,rgba(16,185,129,0.2),transparent_48%),linear-gradient(135deg,#f6fbff_4%,#e5f1ff_45%,#eaf9f4_100%)]",
  tradsphere:
    "bg-[radial-gradient(circle_at_14%_30%,rgba(59,130,246,0.36),transparent_44%),radial-gradient(circle_at_86%_24%,rgba(99,102,241,0.34),transparent_44%),radial-gradient(circle_at_54%_86%,rgba(167,139,250,0.24),transparent_50%),linear-gradient(138deg,#f7fbff_3%,#e7f0ff_44%,#eaeeff_76%,#f5f7ff_100%)]",
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
    <section className={cn("relative overflow-hidden rounded-[1.7rem] border border-blue-100/90 bg-white/95 shadow-soft", className)}>
      <div className={cn("absolute inset-0", bannerGradientByVariant[gradientVariant])} />
      <div className="pointer-events-none absolute inset-x-6 bottom-0 h-px bg-gradient-to-r from-transparent via-blue-200/70 to-transparent" />
      <div className="pointer-events-none absolute right-[-3.5rem] top-[-3.5rem] size-40 rounded-full border border-white/40 bg-white/30 blur-2xl" />
      <div className="relative flex min-h-[176px] flex-col justify-center gap-3 px-7 py-8 md:min-h-[216px] md:px-11 md:py-10">
        <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-indigo-700/85">{eyebrow}</p>
        <h1 className="text-[2.1rem] font-semibold tracking-[-0.024em] text-slate-900 md:text-[3rem]">{title}</h1>
        <p className="max-w-3xl text-sm leading-6 text-slate-700 md:text-[15px]">{description}</p>
        {action ? <div className="pt-4">{action}</div> : null}
      </div>
    </section>
  );
}
