import { AlertCircle, ChevronDown, Search } from "lucide-react";

import { StationResultCard } from "./StationResultCard";
import type { StationGroup, StationRecord } from "./types";

type StationResultsState = "idle" | "loading" | "error" | "empty" | "ready";

type StationResultsProps = {
  state: StationResultsState;
  groups: StationGroup[];
  groupOpenState: Record<string, boolean>;
  error?: string | null;
  disabled?: boolean;
  onToggleGroup: (groupKey: string) => void;
  onEdit: (station: StationRecord) => void;
};

export function StationResults({
  state,
  groups,
  groupOpenState,
  error,
  disabled,
  onToggleGroup,
  onEdit,
}: StationResultsProps) {
  if (state === "idle") {
    return (
      <EmptyPanel
        icon={<Search className="size-5 text-slate-400" />}
        message="Use the search form above to find stations."
      />
    );
  }

  if (state === "error") {
    return (
      <EmptyPanel
        icon={<AlertCircle className="size-5 text-rose-500" />}
        message={error || "Could not load stations. Please try again."}
      />
    );
  }

  if (state === "empty") {
    return <EmptyPanel icon={<Search className="size-5 text-slate-400" />} message="No stations found." />;
  }

  if (state === "loading" && !groups.length) {
    return <EmptyPanel icon={<Search className="size-5 text-slate-400" />} message="Searching stations..." />;
  }

  return (
    <div className="space-y-4">
      {groups.map((group) => {
        const isOpen = groupOpenState[group.key] ?? true;
        return (
          <section key={group.key} className="rounded-2xl border border-blue-100 bg-white shadow-soft">
            <button
              type="button"
              onClick={() => onToggleGroup(group.key)}
              className="flex w-full items-center justify-between gap-3 rounded-2xl px-5 py-3 text-left"
            >
              <div>
                <h3 className="text-base font-bold text-blue-900">{group.label}</h3>
                <p className="mt-0.5 text-xs text-slate-500">{group.items.length} stations</p>
              </div>
              <span className="inline-flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">
                {isOpen ? "Collapse" : "Expand"}
                <ChevronDown className={`size-3.5 transition-transform ${isOpen ? "rotate-180" : "rotate-0"}`} />
              </span>
            </button>

            {isOpen ? (
              <div className="grid gap-3 px-3 pb-4 sm:grid-cols-2 xl:grid-cols-3">
                {group.items.map((station) => (
                  <StationResultCard
                    key={station.code}
                    station={station}
                    disabled={disabled}
                    onEdit={onEdit}
                  />
                ))}
              </div>
            ) : null}
          </section>
        );
      })}
    </div>
  );
}

function EmptyPanel({ icon, message }: { icon: JSX.Element; message: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-6 py-10 text-center">
      <div className="inline-flex items-center gap-2 text-sm text-slate-600">
        {icon}
        <span>{message}</span>
      </div>
    </div>
  );
}
