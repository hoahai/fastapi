import {
  type ButtonHTMLAttributes,
  type HTMLAttributes,
  type ReactNode,
} from "react";

import { cn } from "@/lib/utils";

type HoverCardBaseProps = {
  children: ReactNode;
  className?: string;
  contentClassName?: string;
};

type HoverCardButtonProps = HoverCardBaseProps &
  Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children" | "className"> & {
    as: "button";
  };

type HoverCardArticleProps = HoverCardBaseProps &
  Omit<HTMLAttributes<HTMLElement>, "children" | "className"> & {
    as?: "article";
  };

type HoverCardDivProps = HoverCardBaseProps &
  Omit<HTMLAttributes<HTMLDivElement>, "children" | "className"> & {
    as: "div";
  };

type AnimatedHoverCardProps =
  | HoverCardButtonProps
  | HoverCardArticleProps
  | HoverCardDivProps;

function focusAndInteractionClasses(as: AnimatedHoverCardProps["as"]): string {
  if (as === "button") {
    return "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300 focus-visible:ring-offset-2";
  }
  return "";
}

export function AnimatedHoverCard(props: AnimatedHoverCardProps) {
  const commonRootClassName = cn(
    "group relative overflow-visible rounded-xl bg-transparent",
    "transition-transform duration-200 ease-out hover:-translate-y-0.5",
    "motion-reduce:transition-none motion-reduce:hover:translate-y-0",
    focusAndInteractionClasses(props.as),
    props.className,
  );

  const content = <div className={cn("relative z-10", props.contentClassName)}>{props.children}</div>;

  if (props.as === "button") {
    const {
      as: _as,
      type = "button",
      children,
      className,
      contentClassName,
      ...rest
    } = props;

    return (
      <button type={type} className={commonRootClassName} {...rest}>
        {content}
      </button>
    );
  }

  if (props.as === "div") {
    const { as: _as, children, className, contentClassName, ...rest } = props;

    return (
      <div className={commonRootClassName} {...rest}>
        {content}
      </div>
    );
  }

  const { as: _as, children, className, contentClassName, ...rest } = props;

  return (
    <article className={commonRootClassName} {...rest}>
      {content}
    </article>
  );
}

interface HoverCircleProps {
  children: ReactNode;
  className?: string;
}

export function HoverCircle({ children, className }: HoverCircleProps) {
  return (
    <div
      className={cn(
        "relative z-10 transition-transform duration-150 ease-out group-hover:scale-[1.055]",
        "motion-reduce:transition-none motion-reduce:group-hover:scale-100",
        className,
      )}
    >
      {children}
    </div>
  );
}
