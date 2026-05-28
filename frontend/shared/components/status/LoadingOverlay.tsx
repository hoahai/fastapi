import { Loader2 } from "lucide-react";

import { cn } from "../utils/cn";

type OverlayPanelProps = {
  message: string;
  panelClassName?: string;
};

function OverlayPanel({ message, panelClassName }: OverlayPanelProps) {
  return (
    <div
      className={cn(
        "flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft",
        panelClassName,
      )}
    >
      <Loader2 className="size-4 animate-spin text-blue-600" />
      <span>{message}</span>
    </div>
  );
}

export type PageLoadingOverlayProps = {
  message: string;
  className?: string;
  panelClassName?: string;
};

export function PageLoadingOverlay({ message, className, panelClassName }: PageLoadingOverlayProps) {
  return (
    <div className={cn("fixed inset-0 z-30 flex items-center justify-center bg-slate-950/25 backdrop-blur-[1.5px]", className)}>
      <OverlayPanel message={message} panelClassName={panelClassName} />
    </div>
  );
}

export type SectionLoadingOverlayProps = {
  message: string;
  className?: string;
  panelClassName?: string;
};

export function SectionLoadingOverlay({ message, className, panelClassName }: SectionLoadingOverlayProps) {
  return (
    <div className={cn("absolute inset-0 z-20 flex items-center justify-center rounded-2xl bg-white/70 backdrop-blur-[1px]", className)}>
      <OverlayPanel message={message} panelClassName={panelClassName} />
    </div>
  );
}

export type PageLoadingLayerProps = PageLoadingOverlayProps & {
  active: boolean;
};

export function PageLoadingLayer({ active, ...props }: PageLoadingLayerProps) {
  if (!active) {
    return null;
  }
  return <PageLoadingOverlay {...props} />;
}

export type SectionLoadingLayerProps = SectionLoadingOverlayProps & {
  active: boolean;
};

export function SectionLoadingLayer({ active, ...props }: SectionLoadingLayerProps) {
  if (!active) {
    return null;
  }
  return <SectionLoadingOverlay {...props} />;
}
