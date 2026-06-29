import { type ReactNode } from "react";

import { cn } from "../utils/cn";

type SearchEmptyStatePanelProps = {
  icon: ReactNode;
  message: string;
  description?: string;
  action?: ReactNode;
  className?: string;
};

export function SearchEmptyStatePanel({
  icon,
  message,
  description,
  action,
  className,
}: SearchEmptyStatePanelProps) {
  return (
    <div className={cn("rounded-[1.35rem] border border-dashed border-blue-200/90 bg-gradient-to-b from-blue-50/45 to-white px-6 py-11 text-center", className)}>
      <div className="mx-auto flex max-w-xl flex-col items-center gap-3">
        <div className="inline-flex size-14 items-center justify-center rounded-full border border-blue-100 bg-blue-50 text-blue-700">
          {icon}
        </div>
        <div className="space-y-1">
          <p className="text-base font-semibold text-slate-900">{message}</p>
          {description ? <p className="text-sm leading-6 text-slate-600">{description}</p> : null}
        </div>
        {action ? action : null}
      </div>
    </div>
  );
}
