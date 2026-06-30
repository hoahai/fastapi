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
        "flex items-center gap-2.5 rounded-2xl border border-blue-100/90 bg-white/95 px-5 py-3.5 text-sm font-medium text-slate-700 shadow-soft",
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
    <div className={cn("fixed inset-0 z-[120] flex items-center justify-center bg-slate-950/30 backdrop-blur-[2px]", className)}>
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
    <div className={cn("absolute inset-0 z-[150] flex items-center justify-center rounded-2xl bg-slate-950/22 backdrop-blur-[2px]", className)}>
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
