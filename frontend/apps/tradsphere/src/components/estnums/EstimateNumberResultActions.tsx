import { Pencil, Table2 } from "lucide-react";

import { Button } from "@/components/ui/button";

import type { EstimateSearchItem } from "./types";

type EstimateNumberResultActionsProps = {
  item: EstimateSearchItem;
  disabled?: boolean;
  onViewSchedule: (item: EstimateSearchItem) => void;
  onEditEstimate: (item: EstimateSearchItem) => void;
};

export function EstimateNumberResultActions({
  item,
  disabled,
  onViewSchedule,
  onEditEstimate,
}: EstimateNumberResultActionsProps) {
  return (
    <div className="flex flex-wrap items-center justify-end gap-2">
      <Button
        variant="outline"
        size="sm"
        onClick={(event) => {
          event.stopPropagation();
          onViewSchedule(item);
        }}
        disabled={!item.hasSchedule || disabled}
        aria-label={`View schedule for EstNum ${item.estNum}`}
      >
        <Table2 className="size-3.5" />
        View Schedule
      </Button>
      <Button
        variant="outline"
        size="sm"
        onClick={(event) => {
          event.stopPropagation();
          onEditEstimate(item);
        }}
        disabled={disabled}
        aria-label={`Edit EstNum ${item.estNum}`}
      >
        <Pencil className="size-3.5" />
        Edit Estimate
      </Button>
    </div>
  );
}
