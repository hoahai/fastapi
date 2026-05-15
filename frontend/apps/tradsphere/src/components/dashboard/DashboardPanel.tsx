import type { ReactNode } from "react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";

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
    <Card className={`overflow-hidden ${className ?? ""}`}>
      <CardHeader className="border-b border-blue-100 bg-blue-50/70 py-3">
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="flex items-center gap-3">
            {icon}
            <span className="text-sm font-bold uppercase tracking-[0.14em] text-blue-800">{title}</span>
          </CardTitle>
          <div className="flex items-center gap-1">{actions}</div>
        </div>
      </CardHeader>
      <CardContent className="space-y-5 p-5 pt-5">
        <div className="flex justify-end">
          <Input
            placeholder="Search"
            className="max-w-xs"
            value={searchValue}
            onChange={(event) => onSearchChange(event.target.value)}
          />
        </div>
        {children}
      </CardContent>
    </Card>
  );
}
