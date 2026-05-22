import { Pencil, UserRoundCheck, UserRoundX } from "lucide-react";

import { Button } from "@tradsphere/components/ui/button";

import type { ShiftzyEmployeeItem } from "./types";

type ShiftzyEmployeeResultActionsProps = {
  item: ShiftzyEmployeeItem;
  disabled?: boolean;
  canEdit: boolean;
  onEdit: (item: ShiftzyEmployeeItem) => void;
  onToggleActive: (item: ShiftzyEmployeeItem) => void;
};

export function ShiftzyEmployeeResultActions({
  item,
  disabled,
  canEdit,
  onEdit,
  onToggleActive,
}: ShiftzyEmployeeResultActionsProps) {
  return (
    <div className="mt-3 flex flex-wrap items-center justify-end gap-2 border-t border-slate-200 pt-3">
      <Button
        variant="outline"
        size="sm"
        onClick={(event) => {
          event.stopPropagation();
          onEdit(item);
        }}
        disabled={!canEdit || disabled}
        aria-label={`Edit ${item.name}`}
      >
        <Pencil className="size-3.5" />
        Edit
      </Button>
      <Button
        variant="outline"
        size="sm"
        onClick={(event) => {
          event.stopPropagation();
          onToggleActive(item);
        }}
        disabled={!canEdit || disabled}
        aria-label={`${item.active ? "Deactivate" : "Activate"} ${item.name}`}
      >
        {item.active ? <UserRoundX className="size-3.5" /> : <UserRoundCheck className="size-3.5" />}
        {item.active ? "Deactivate" : "Activate"}
      </Button>
    </div>
  );
}
