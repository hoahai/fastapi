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
    <section className="rounded-2xl border border-blue-100 bg-white/95 p-4 shadow-soft">
      <header className="border-b border-slate-200 pb-2">
        <h3 className="text-sm font-semibold text-slate-800">Results</h3>
      </header>
      <div className="mt-3 space-y-4">
        {groups.map((group) => {
          const isOpen = groupOpenState[group.key] ?? true;
          return (
            <section key={group.key} className="group overflow-hidden rounded-2xl border border-blue-100 bg-slate-50/70">
              <button
                type="button"
                onClick={() => onToggleGroup(group.key)}
                className="flex w-full items-center justify-between gap-2 border-b border-blue-100 bg-blue-50/70 px-4 py-3 text-left"
              >
                <p className="text-sm font-bold uppercase tracking-[0.14em] text-blue-800">{group.label}</p>
                <span className="inline-flex items-center text-slate-500" aria-hidden="true">
                  <span className="mr-2 text-xs">{group.items.length} stations</span>
                  <ChevronDown className={`size-4 transition-transform ${isOpen ? "rotate-180" : "rotate-0"}`} />
                </span>
              </button>

              {isOpen ? (
                <div className="p-3">
                  <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                    {group.items.map((station) => (
                      <StationResultCard
                        key={station.code}
                        station={station}
                        disabled={disabled}
                        onEdit={onEdit}
                      />
                    ))}
                  </div>
                </div>
              ) : null}
            </section>
          );
        })}
      </div>
    </section>
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
