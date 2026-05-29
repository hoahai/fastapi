import type { ReactNode } from "react";

import { AnimatedHoverCard, HoverCircle } from "@/components/ui/animated-hover-card";
import { cn } from "@/lib/utils";

interface EntityItemCardProps {
  rootAs?: "article" | "button" | "div";
  rootClassName?: string;
  onClick?: () => void;
  disabled?: boolean;
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
  onClick,
  disabled,
  circleClassName,
  circleContent,
  badge,
  title,
  subtitle,
  subtitleClassName,
  srOnlyText,
}: EntityItemCardProps) {
  const isInteractive = rootAs === "button" && !disabled && Boolean(onClick);

  const content = (
    <>
      <HoverCircle
        className={cn(
          "mx-auto mb-3 size-20 overflow-visible rounded-full border border-blue-100/85 bg-blue-50/85 text-blue-600 shadow-[0_0_0_0_rgba(59,130,246,0)] transition-[background-color,box-shadow,color] duration-200 group-hover:bg-blue-100 group-hover:shadow-[0_0_18px_4px_rgba(59,130,246,0.2)]",
          !isInteractive && "transition-none group-hover:scale-100 group-hover:bg-blue-50 group-hover:shadow-none",
          circleClassName,
        )}
      >
        {badge}
        <span className="absolute inset-0 flex items-center justify-center whitespace-nowrap text-center leading-none">
          {circleContent}
        </span>
      </HoverCircle>
      <p className="text-base font-semibold text-slate-700">{title}</p>
      {subtitle !== undefined ? (
        <p className={cn("mt-2 min-h-5 text-xs italic text-slate-500", subtitleClassName)}>{subtitle}</p>
      ) : null}
      {srOnlyText ? <p className="sr-only">{srOnlyText}</p> : null}
    </>
  );

  if (rootAs === "button") {
    return (
      <AnimatedHoverCard
        as="button"
        type="button"
        className={cn(
          "cursor-pointer text-center disabled:cursor-not-allowed",
          !isInteractive && "hover:!translate-y-0 transition-none",
          rootClassName,
        )}
        onClick={onClick}
        disabled={disabled}
      >
        {content}
      </AnimatedHoverCard>
    );
  }

  if (rootAs === "div") {
    return (
      <AnimatedHoverCard
        as="div"
        className={cn("text-center hover:!translate-y-0 transition-none", rootClassName)}
      >
        {content}
      </AnimatedHoverCard>
    );
  }

  return (
    <AnimatedHoverCard
      as="article"
      className={cn("text-center hover:!translate-y-0 transition-none", rootClassName)}
    >
      {content}
    </AnimatedHoverCard>
  );
}
