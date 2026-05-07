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
    <Card className={className}>
      <CardHeader className="border-b border-blue-100 bg-secondary/60 py-4">
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="flex items-center gap-3">
            {icon}
            <span className="text-lg font-semibold text-blue-700">{title}</span>
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
