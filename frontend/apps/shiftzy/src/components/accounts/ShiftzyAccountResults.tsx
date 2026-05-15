import type { ReactNode } from "react";
import { AlertCircle, Loader2, Search } from "lucide-react";

import { ShiftzyAccountResultGroup } from "./ShiftzyAccountResultGroup";
import type { ShiftzyAccountItem, ShiftzyAccountSectionGroup } from "./types";

type ShiftzyAccountResultsState = "idle" | "loading" | "error" | "empty" | "ready";

type ShiftzyAccountResultsProps = {
  state: ShiftzyAccountResultsState;
  groups: ShiftzyAccountSectionGroup[];
  error?: string | null;
  canEdit: boolean;
  disabled?: boolean;
  onEdit: (item: ShiftzyAccountItem) => void;
  onToggleActive: (item: ShiftzyAccountItem) => void;
};

export function ShiftzyAccountResults({
  state,
  groups,
  error,
  canEdit,
  disabled,
  onEdit,
  onToggleActive,
}: ShiftzyAccountResultsProps) {
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
    <section className="rounded-2xl border border-blue-100 bg-white/95 p-4 shadow-soft">
      <header className="border-b border-slate-200 pb-2">
        <h3 className="text-sm font-semibold text-slate-800">Results</h3>
      </header>
      <div className="mt-3 space-y-4">
        {groups.map((group) => (
          <ShiftzyAccountResultGroup
            key={group.key}
            group={group}
            disabled={disabled}
            canEdit={canEdit}
            onEdit={onEdit}
            onToggleActive={onToggleActive}
          />
        ))}
      </div>
    </section>
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
