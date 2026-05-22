import type { ReactNode } from "react";

import { Input } from "@/components/ui/input";
import { SectionCard } from "@shared/components/layout/SectionCard";

interface DashboardPanelProps {
  title: string;
  icon: ReactNode;
  actions?: ReactNode;
  searchValue: string;
  onSearchChange: (value: string) => void;
  children: ReactNode;
  className?: string;
}

export function DashboardPanel({
  title,
  icon,
  actions,
  searchValue,
  onSearchChange,
  children,
  className,
}: DashboardPanelProps) {
  return (
    <SectionCard
      className={className}
      title={(
        <span className="flex items-center gap-2">
          {icon}
          <span>{title}</span>
        </span>
      )}
      actions={<div className="flex items-center gap-1">{actions}</div>}
      contentClassName="space-y-5"
    >
        <div className="flex justify-end">
          <Input
            placeholder="Search"
            className="max-w-xs"
            value={searchValue}
            onChange={(event) => onSearchChange(event.target.value)}
          />
        </div>
        {children}
    </SectionCard>
  );
}
