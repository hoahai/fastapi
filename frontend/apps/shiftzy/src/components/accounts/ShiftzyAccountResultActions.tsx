import { Pencil, UserRoundCheck, UserRoundX } from "lucide-react";

import { Button } from "@/components/ui/button";

import type { ShiftzyAccountItem } from "./types";

type ShiftzyAccountResultActionsProps = {
  item: ShiftzyAccountItem;
  disabled?: boolean;
  canEdit: boolean;
  onEdit: (item: ShiftzyAccountItem) => void;
  onToggleActive: (item: ShiftzyAccountItem) => void;
};

export function ShiftzyAccountResultActions({
  item,
  disabled,
  canEdit,
  onEdit,
  onToggleActive,
}: ShiftzyAccountResultActionsProps) {
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
