import type { ReactNode } from "react";
import { AlertCircle, AlertTriangle, CheckCircle2, Info } from "lucide-react";

import { cn } from "../utils/cn";

export type MessageVariant = "info" | "success" | "warning" | "error";

export type StackMessage = {
  id?: string;
  variant: MessageVariant;
  title?: ReactNode;
  message: ReactNode;
};

type MessageItemSize = "page" | "section";

type MessageStackProps = {
  messages: StackMessage[];
  className?: string;
};

function variantStyles(variant: MessageVariant): string {
  if (variant === "success") {
    return "border-emerald-200 bg-emerald-50/80 text-emerald-800";
  }
  if (variant === "warning") {
    return "border-amber-200 bg-amber-50/80 text-amber-800";
  }
  if (variant === "error") {
    return "border-rose-200 bg-rose-50/80 text-rose-800";
  }
  return "border-blue-200 bg-blue-50/80 text-blue-800";
}

function MessageIcon({ variant }: { variant: MessageVariant }) {
  if (variant === "success") {
    return <CheckCircle2 className="mt-0.5 size-4 shrink-0" />;
  }
  if (variant === "warning") {
    return <AlertTriangle className="mt-0.5 size-4 shrink-0" />;
  }
  if (variant === "error") {
    return <AlertCircle className="mt-0.5 size-4 shrink-0" />;
  }
  return <Info className="mt-0.5 size-4 shrink-0" />;
}

function MessageItem({
  item,
  size,
}: {
  item: StackMessage;
  size: MessageItemSize;
}) {
  const textSize = size === "section" ? "text-xs sm:text-sm" : "text-sm";
  const containerSpacing = size === "section" ? "rounded-lg px-3 py-2.5" : "rounded-xl px-4 py-3";
  return (
    <div
      className={cn(
        "border shadow-sm",
        containerSpacing,
        textSize,
        variantStyles(item.variant),
      )}
    >
      <div className="flex items-start gap-2">
        <MessageIcon variant={item.variant} />
        <div className="min-w-0">
          {item.title ? <p className="font-semibold leading-5">{item.title}</p> : null}
          <div className={cn("leading-5", item.title ? "mt-0.5" : undefined)}>{item.message}</div>
        </div>
      </div>
    </div>
  );
}

function SharedMessageStack({
  messages,
  className,
  size,
}: MessageStackProps & { size: MessageItemSize }) {
  if (!messages.length) {
    return null;
  }
  return (
    <div className={cn("space-y-2", className)} role="status" aria-live="polite">
      {messages.map((item, index) => (
        <MessageItem
          key={item.id || `${item.variant}-${index}`}
          item={item}
          size={size}
        />
      ))}
    </div>
  );
}

export function PageMessageStack({ messages, className }: MessageStackProps) {
  return <SharedMessageStack messages={messages} className={className} size="page" />;
}

export function SectionMessageStack({ messages, className }: MessageStackProps) {
  return <SharedMessageStack messages={messages} className={className} size="section" />;
}
