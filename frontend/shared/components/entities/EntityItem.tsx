import type { ButtonHTMLAttributes, HTMLAttributes, ReactNode } from "react";

import { cn } from "../utils/cn";

type EntityItemButtonProps = {
  as: "button";
} & Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children">;

type EntityItemContainerProps = {
  as?: "article" | "div";
} & Omit<HTMLAttributes<HTMLElement>, "children">;

export type EntityItemProps = (EntityItemButtonProps | EntityItemContainerProps) & {
  className?: string;
  children: ReactNode;
};

export function EntityItem({ as = "article", className, children, ...props }: EntityItemProps) {
  if (as === "button") {
    const buttonProps = props as Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children">;
    return (
      <button
        type={buttonProps.type ?? "button"}
        className={cn("group relative isolate overflow-visible rounded-xl bg-transparent", className)}
        {...buttonProps}
      >
        {children}
      </button>
    );
  }

  const Tag = as;
  return (
    <Tag
      className={cn("group relative isolate overflow-visible rounded-xl bg-transparent", className)}
      {...(props as Omit<HTMLAttributes<HTMLElement>, "children">)}
    >
      {children}
    </Tag>
  );
}
