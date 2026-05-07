import type { ReactNode } from "react";
import { AlertCircle, Loader2, Search } from "lucide-react";

import { EstimateNumberResultGroup } from "@/components/estnums/EstimateNumberResultGroup";
import { Button } from "@/components/ui/button";

import type { EstimateAccountGroup, EstimateSearchItem } from "./types";

type EstimateNumberResultsState = "idle" | "min-query" | "loading" | "error" | "empty" | "ready";

type EstimateNumberResultsProps = {
  state: EstimateNumberResultsState;
  groups: EstimateAccountGroup[];
  error?: string | null;
  minQueryLength: number;
  loadMoreVisible: boolean;
  loadMoreLoading: boolean;
  disabled?: boolean;
  onLoadMore: () => void;
  onViewSchedule: (item: EstimateSearchItem) => void;
  onEditEstimate: (item: EstimateSearchItem) => void;
  onCopyEstNum: (item: EstimateSearchItem) => void;
};

export function EstimateNumberResults({
  state,
  groups,
  error,
  minQueryLength,
  loadMoreVisible,
  loadMoreLoading,
  disabled,
  onLoadMore,
  onViewSchedule,
  onEditEstimate,
  onCopyEstNum,
}: EstimateNumberResultsProps) {
  if (state === "idle") {
    return (
      <EmptyPanel
        icon={<Search className="size-5 text-slate-400" />}
        message="Search for an estimate number to begin."
      />
    );
  }

  if (state === "min-query") {
    return (
      <EmptyPanel
        icon={<Search className="size-5 text-slate-400" />}
        message={`Type at least ${minQueryLength} characters, or enter an exact EstNum.`}
      />
    );
  }

  if (state === "loading") {
    return (
      <EmptyPanel
        icon={<Loader2 className="size-5 animate-spin text-slate-500" />}
        message="Searching estimate numbers..."
      />
    );
  }

  if (state === "error") {
    return (
      <EmptyPanel
        icon={<AlertCircle className="size-5 text-rose-500" />}
        message={error || "Unable to search estimate numbers."}
      />
    );
  }

  if (state === "empty") {
    return <EmptyPanel icon={<Search className="size-5 text-slate-400" />} message="No estimate numbers matched your search." />;
  }

  return (
    <div className="space-y-4">
      {groups.map((group) => (
        <EstimateNumberResultGroup
          key={group.key}
          group={group}
          disabled={disabled}
          onViewSchedule={onViewSchedule}
          onEditEstimate={onEditEstimate}
          onCopyEstNum={onCopyEstNum}
        />
      ))}

      {loadMoreVisible ? (
        <div className="flex justify-center">
          <Button variant="outline" onClick={onLoadMore} disabled={disabled || loadMoreLoading}>
            {loadMoreLoading ? (
              <>
                <Loader2 className="size-4 animate-spin" />
                Loading...
              </>
            ) : (
              "Load more"
            )}
          </Button>
        </div>
      ) : null}
    </div>
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
