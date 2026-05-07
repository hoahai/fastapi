import { cn } from "@/lib/utils";

import type { ScheduleViewMode } from "./ScheduleTable";

interface ScheduleViewModeToggleProps {
  mode: ScheduleViewMode;
  onModeChange: (mode: ScheduleViewMode) => void;
  detailEnabled?: boolean;
}

export function ScheduleViewModeToggle({ mode, onModeChange, detailEnabled = true }: ScheduleViewModeToggleProps) {
  return (
    <fieldset className="flex items-center gap-3">
      <legend className="text-sm font-medium text-slate-700">View mode:</legend>
      <label
        className={cn(
          "inline-flex items-center gap-2 rounded-md border px-2.5 py-1 text-sm",
          mode === "compact" ? "border-blue-300 bg-blue-50 text-blue-700" : "border-slate-200 bg-white text-slate-700",
        )}
      >
        <input
          type="radio"
          name="schedule-view-mode"
          value="compact"
          checked={mode === "compact"}
          onChange={() => onModeChange("compact")}
          className="h-4 w-4 border-slate-300 text-blue-600 focus:ring-blue-500"
        />
        Compact
      </label>
      <label
        className={cn(
          "inline-flex items-center gap-2 rounded-md border px-2.5 py-1 text-sm",
          !detailEnabled
            ? "cursor-not-allowed border-slate-200 bg-slate-100 text-slate-400"
            : mode === "detail"
              ? "border-blue-300 bg-blue-50 text-blue-700"
              : "border-slate-200 bg-white text-slate-700",
        )}
      >
        <input
          type="radio"
          name="schedule-view-mode"
          value="detail"
          checked={mode === "detail"}
          onChange={() => onModeChange("detail")}
          className="h-4 w-4 border-slate-300 text-blue-600 focus:ring-blue-500"
          disabled={!detailEnabled}
        />
        Detail
      </label>
    </fieldset>
  );
}
