import { Copy, Pencil, Table2 } from "lucide-react";

import { Button } from "@/components/ui/button";

import type { EstimateSearchItem } from "./types";

type EstimateNumberResultActionsProps = {
  item: EstimateSearchItem;
  disabled?: boolean;
  onViewSchedule: (item: EstimateSearchItem) => void;
  onEditEstimate: (item: EstimateSearchItem) => void;
  onCopyEstNum: (item: EstimateSearchItem) => void;
};

export function EstimateNumberResultActions({
  item,
  disabled,
  onViewSchedule,
  onEditEstimate,
  onCopyEstNum,
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
      <Button
        variant="ghost"
        size="sm"
        onClick={(event) => {
          event.stopPropagation();
          onCopyEstNum(item);
        }}
        disabled={disabled}
        aria-label={`Copy EstNum ${item.estNum}`}
      >
        <Copy className="size-3.5" />
        Copy
      </Button>
    </div>
  );
}
