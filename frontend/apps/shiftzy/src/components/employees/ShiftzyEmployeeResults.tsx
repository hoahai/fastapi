import type { ReactNode } from "react";
import { AlertCircle, Loader2, Search } from "lucide-react";

import { SectionCard } from "@shared/components/layout/SectionCard";

import { ShiftzyEmployeeResultGroup } from "./ShiftzyEmployeeResultGroup";
import type { ShiftzyEmployeeItem, ShiftzyEmployeeSectionGroup } from "./types";

type ShiftzyEmployeeResultsState = "idle" | "loading" | "error" | "empty" | "ready";

type ShiftzyEmployeeResultsProps = {
  state: ShiftzyEmployeeResultsState;
  groups: ShiftzyEmployeeSectionGroup[];
  error?: string | null;
  canEdit: boolean;
  disabled?: boolean;
  onEdit: (item: ShiftzyEmployeeItem) => void;
  onToggleActive: (item: ShiftzyEmployeeItem) => void;
};

export function ShiftzyEmployeeResults({
  state,
  groups,
  error,
  canEdit,
  disabled,
  onEdit,
  onToggleActive,
}: ShiftzyEmployeeResultsProps) {
  if (state === "idle") {
    return (
      <EmptyPanel
        icon={<Search className="size-5 text-slate-400" />}
        message="Enter search criteria to find Shiftzy employees."
      />
    );
  }

  if (state === "loading" && !groups.length) {
    return (
      <EmptyPanel
        icon={<Loader2 className="size-5 animate-spin text-slate-500" />}
        message="Searching Shiftzy employees..."
      />
    );
  }

  if (state === "error") {
    return (
      <EmptyPanel
        icon={<AlertCircle className="size-5 text-rose-500" />}
        message={error || "Unable to search Shiftzy employees."}
      />
    );
  }

  if (state === "empty") {
    return <EmptyPanel icon={<Search className="size-5 text-slate-400" />} message="No Shiftzy employees matched your search." />;
  }

  return (
    <SectionCard title="Results" contentClassName="space-y-4">
        {groups.map((group) => (
          <ShiftzyEmployeeResultGroup
            key={group.key}
            group={group}
            disabled={disabled}
            canEdit={canEdit}
            onEdit={onEdit}
            onToggleActive={onToggleActive}
          />
        ))}
    </SectionCard>
  );
}

function EmptyPanel({ icon, message }: { icon: ReactNode; message: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-6 py-10 text-center">
      <div className="inline-flex items-center gap-2 text-sm text-slate-600">
        {icon}
        <span>{message}</span>
      </div>
    </div>
  );
}
