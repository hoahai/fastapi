import { useRef, useState } from "react";
import { ChevronDown, UserMinus, UserRoundCheck } from "lucide-react";

import { cn } from "@tradsphere/lib/utils";
import { Tooltip } from "@shared/components/actions/Tooltip";
import type { ShiftzyEmployeeItem, ShiftzyEmployeeSectionGroup } from "./types";

type ShiftzyEmployeeResultGroupProps = {
  group: ShiftzyEmployeeSectionGroup;
  disabled?: boolean;
  canEdit: boolean;
  onEdit: (item: ShiftzyEmployeeItem) => void;
  onToggleActive: (item: ShiftzyEmployeeItem) => void;
};

export function ShiftzyEmployeeResultGroup({
  group,
  disabled,
  canEdit,
  onEdit,
  onToggleActive,
}: ShiftzyEmployeeResultGroupProps) {
  const canOpen = canEdit && !disabled;

  return (
    <details open className="group overflow-hidden rounded-2xl border border-blue-100 bg-slate-50/70">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-2 border-b border-blue-100 bg-blue-50/70 px-4 py-3">
        <p className="text-sm font-bold uppercase tracking-[0.14em] text-blue-800">{group.label}</p>
        <span className="inline-flex items-center gap-2 text-slate-500" aria-hidden="true">
          <span className="text-xs">{group.items.length} employees</span>
          <ChevronDown className="size-4 transition-transform group-open:rotate-180" />
        </span>
      </summary>

      <div className="p-3">
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
          {group.items.map((item) => (
            <article
              key={item.id}
              role="button"
              tabIndex={canOpen ? 0 : -1}
              onClick={() => {
                if (canOpen) {
                  onEdit(item);
                }
              }}
              onKeyDown={(event) => {
                if (!canOpen) {
                  return;
                }
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  onEdit(item);
                }
              }}
              className={cn(
                "space-y-3.5 rounded-xl border p-4 shadow-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
                canOpen ? "cursor-pointer" : "cursor-default",
                item.active
                  ? "border-slate-200 bg-white hover:border-blue-300 hover:shadow-md"
                  : "border-slate-300 bg-slate-100/95 text-slate-600 hover:border-slate-300 hover:shadow-sm",
              )}
            >
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <p className={cn("truncate text-base font-semibold leading-tight", item.active ? "text-slate-900" : "text-slate-700")}>
                    {item.name}
                  </p>
                  <div className="mt-2 flex flex-wrap items-center gap-1.5">
                    <Chip label={item.scheduleSection || "Area: -"} kind="area" />
                    <Chip label={item.positionName || "Position: -"} kind="position" />
                  </div>
                </div>

                <div className="flex items-center gap-1.5">
                  <StatusToggleIconButton
                    active={item.active}
                    disabled={!canEdit || Boolean(disabled)}
                    onClick={() => onToggleActive(item)}
                  />
                  <StatusChip active={item.active} />
                </div>
              </div>

              <div className={cn("border-t pt-2.5", item.active ? "border-slate-200" : "border-slate-300")}>
                <p className="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-500">Note</p>
                {toNoteText(item.note) ? (
                  <p className={cn("mt-1 line-clamp-2 text-sm leading-5", item.active ? "text-slate-700" : "text-slate-600")}>
                    {toNoteText(item.note)}
                  </p>
                ) : (
                  <p className="mt-1 text-sm italic text-slate-500">No note</p>
                )}
              </div>
            </article>
          ))}
        </div>
      </div>
    </details>
  );
}

function StatusToggleIconButton({
  active,
  disabled,
  onClick,
}: {
  active: boolean;
  disabled: boolean;
  onClick: () => void;
}) {
  const anchorRef = useRef<HTMLButtonElement | null>(null);
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const tooltipText = active ? "Deactivate employee" : "Activate employee";

  return (
    <>
      <button
        ref={anchorRef}
        type="button"
        onClick={(event) => {
          if (disabled) {
            return;
          }
          event.stopPropagation();
          onClick();
        }}
        onMouseEnter={() => setTooltipOpen(true)}
        onMouseLeave={() => setTooltipOpen(false)}
        onFocus={() => setTooltipOpen(true)}
        onBlur={() => setTooltipOpen(false)}
        className={cn(
          "inline-flex h-7 w-7 items-center justify-center rounded-full border border-slate-300 bg-white text-slate-600 transition-colors focus-visible:outline-none focus-visible:ring-2",
          disabled
            ? "pointer-events-none cursor-default opacity-50"
            : active
              ? "focus-visible:ring-rose-300 hover:border-rose-300 hover:bg-rose-50 hover:text-rose-700"
              : "focus-visible:ring-emerald-300 hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-700",
        )}
        aria-label={tooltipText}
      >
        {active ? <UserMinus className="size-3.5" /> : <UserRoundCheck className="size-3.5" />}
      </button>
      <Tooltip open={tooltipOpen} anchorRef={anchorRef} text={tooltipText} />
    </>
  );
}

function Chip({ label, kind }: { label: string; kind: "area" | "position" }) {
  return (
    <span
      className={cn(
        "rounded-full border px-2.5 py-0.5 text-xs font-medium",
        chipToneClass(kind, label),
      )}
    >
      {label}
    </span>
  );
}

const AREA_CHIP_PALETTE = [
  "border-blue-200 bg-blue-100 text-blue-700",
  "border-cyan-200 bg-cyan-100 text-cyan-700",
  "border-sky-200 bg-sky-100 text-sky-700",
  "border-indigo-200 bg-indigo-100 text-indigo-700",
  "border-violet-200 bg-violet-100 text-violet-700",
  "border-fuchsia-200 bg-fuchsia-100 text-fuchsia-700",
  "border-rose-200 bg-rose-100 text-rose-700",
  "border-orange-200 bg-orange-100 text-orange-700",
];

const POSITION_CHIP_PALETTE = [
  "border-emerald-200 bg-emerald-100 text-emerald-700",
  "border-teal-200 bg-teal-100 text-teal-700",
  "border-lime-200 bg-lime-100 text-lime-700",
  "border-amber-200 bg-amber-100 text-amber-700",
  "border-green-200 bg-green-100 text-green-700",
  "border-yellow-200 bg-yellow-100 text-yellow-700",
  "border-cyan-200 bg-cyan-100 text-cyan-700",
  "border-sky-200 bg-sky-100 text-sky-700",
  "border-orange-200 bg-orange-100 text-orange-700",
  "border-blue-200 bg-blue-100 text-blue-700",
  "border-violet-200 bg-violet-100 text-violet-700",
  "border-pink-200 bg-pink-100 text-pink-700",
];

function chipToneClass(kind: "area" | "position", rawValue: string): string {
  const value = String(rawValue || "").trim();
  const normalized = value.toLowerCase();
  if (!value || normalized === "-" || normalized.endsWith(": -")) {
    return "border-slate-300 bg-slate-200 text-slate-700";
  }
  const palette = kind === "area" ? AREA_CHIP_PALETTE : POSITION_CHIP_PALETTE;
  const index = hashText(normalized) % palette.length;
  return palette[index];
}

function hashText(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) >>> 0;
  }
  return hash;
}

function StatusChip({ active }: { active: boolean }) {
  return (
    <span
      className={cn(
        "rounded-full border px-2.5 py-0.5 text-xs font-semibold",
        active
          ? "border-emerald-200 bg-emerald-50 text-emerald-700"
          : "border-slate-300 bg-slate-200 text-slate-700",
      )}
    >
      {active ? "Active" : "Inactive"}
    </span>
  );
}

function toNoteText(value: string | null | undefined): string {
  const text = String(value || "").trim();
  return text;
}
