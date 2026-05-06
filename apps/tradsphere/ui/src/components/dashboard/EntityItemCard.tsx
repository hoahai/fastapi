import type { ReactNode } from "react";

import { AnimatedHoverCard, HoverCircle } from "@/components/ui/animated-hover-card";
import { cn } from "@/lib/utils";

interface EntityItemCardProps {
  rootAs?: "article" | "button" | "div";
  rootClassName?: string;
  circleClassName?: string;
  circleContent: ReactNode;
  badge?: ReactNode;
  title: ReactNode;
  subtitle?: ReactNode;
  subtitleClassName?: string;
  srOnlyText?: string;
}

export function EntityItemCard({
  rootAs = "article",
  rootClassName,
  circleClassName,
  circleContent,
  badge,
  title,
  subtitle,
  subtitleClassName,
  srOnlyText,
}: EntityItemCardProps) {
  const content = (
    <>
      <HoverCircle
        className={cn(
          "mx-auto mb-3 grid size-20 place-items-center rounded-full bg-blue-50 text-blue-600 transition-colors group-hover:bg-blue-100",
          circleClassName,
        )}
      >
        {badge}
        {circleContent}
      </HoverCircle>
      <p className="text-base font-medium text-slate-700">{title}</p>
      {subtitle !== undefined ? (
        <p className={cn("mt-2 min-h-5 text-xs italic text-slate-500", subtitleClassName)}>{subtitle}</p>
      ) : null}
      {srOnlyText ? <p className="sr-only">{srOnlyText}</p> : null}
    </>
  );

  if (rootAs === "button") {
    return (
      <AnimatedHoverCard as="button" type="button" className={cn("text-center", rootClassName)}>
        {content}
      </AnimatedHoverCard>
    );
  }

  if (rootAs === "div") {
    return (
      <AnimatedHoverCard as="div" className={cn("text-center", rootClassName)}>
        {content}
      </AnimatedHoverCard>
    );
  }

  return (
    <AnimatedHoverCard as="article" className={cn("text-center", rootClassName)}>
      {content}
    </AnimatedHoverCard>
  );
}
