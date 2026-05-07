import { useEffect, useMemo, useRef, useState } from "react";
import { CheckCircle2, Circle, Pencil } from "lucide-react";

import { EntityItemCard } from "@/components/dashboard/EntityItemCard";
import { FloatingActionMenu, type FloatingActionMenuItem } from "@/components/dashboard/FloatingActionMenu";
import { cn } from "@/lib/utils";

import type { EsnumItem } from "./types";

interface ScheduleCardProps {
  esnum: EsnumItem;
  onClick?: () => void;
  onEditEstimate?: () => void;
  disabled?: boolean;
}

export function ScheduleCard({ esnum, onClick, onEditEstimate, disabled }: ScheduleCardProps) {
  const note = esnum.note?.trim() || "";
  const anchorRef = useRef<HTMLDivElement | null>(null);
  const closeTimerRef = useRef<number | null>(null);
  const [isCardHovered, setIsCardHovered] = useState(false);
  const [isMenuHovered, setIsMenuHovered] = useState(false);
  const [isFocusWithin, setIsFocusWithin] = useState(false);
  const isScheduleClickable = Boolean(!disabled && esnum.hasSchedule && onClick);

  function clearCloseTimer() {
    if (closeTimerRef.current !== null) {
      window.clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
  }

  function queueMenuClose() {
    clearCloseTimer();
    closeTimerRef.current = window.setTimeout(() => {
      setIsCardHovered(false);
      setIsMenuHovered(false);
      setIsFocusWithin(false);
    }, 110);
  }

  useEffect(() => {
    return () => {
      if (closeTimerRef.current !== null) {
        window.clearTimeout(closeTimerRef.current);
      }
    };
  }, []);

  const isMenuOpen = Boolean(!disabled && onEditEstimate && (isCardHovered || isMenuHovered || isFocusWithin));
  const menuItems = useMemo<FloatingActionMenuItem[]>(() => {
    if (!onEditEstimate) {
      return [];
    }
    return [
      {
        key: "edit-estimate",
        label: "Update Estimate",
        icon: <Pencil className="size-3.5" />,
        onSelect: onEditEstimate,
      },
    ];
  }, [onEditEstimate]);

  return (
    <>
      <div
        ref={anchorRef}
        className="shrink-0"
        title={esnum.hasSchedule ? undefined : "No schedule yet"}
        onMouseEnter={() => {
          clearCloseTimer();
          setIsCardHovered(true);
        }}
        onMouseLeave={() => {
          setIsCardHovered(false);
          if (!isMenuHovered && !isFocusWithin) {
            queueMenuClose();
          }
        }}
        onFocusCapture={() => {
          clearCloseTimer();
          setIsFocusWithin(true);
        }}
        onBlurCapture={() => {
          setIsFocusWithin(false);
          if (!isCardHovered && !isMenuHovered) {
            queueMenuClose();
          }
        }}
      >
        <EntityItemCard
          rootAs={isScheduleClickable ? "button" : "div"}
          rootClassName={cn(
            "min-w-44 p-5",
            isScheduleClickable
              ? "cursor-pointer"
              : "cursor-default hover:translate-y-0 group-hover:translate-y-0",
          )}
          onClick={
            isScheduleClickable
              ? () => {
                  onClick?.();
                }
              : undefined
          }
          disabled={disabled}
          circleClassName={cn(
            "text-2xl font-semibold tracking-tight",
            isScheduleClickable
              ? "bg-blue-50 text-blue-600 group-hover:bg-blue-100"
              : "bg-blue-50 text-blue-600 group-hover:bg-blue-50 group-hover:scale-100",
          )}
          badge={
            esnum.hasSchedule ? (
              <span className="absolute -right-6 -top-1 z-20 inline-flex items-center gap-1 rounded-full bg-emerald-100/95 px-2 py-0.5 text-[11px] font-semibold leading-none text-emerald-800 shadow-sm">
                <CheckCircle2 className="size-3" />
                Scheduled
              </span>
            ) : (
              <span className="absolute -right-6 -top-1 z-20 inline-flex items-center gap-1 rounded-full bg-slate-200/95 px-2 py-0.5 text-[11px] font-semibold leading-none text-slate-700 shadow-sm">
                <Circle className="size-3" />
                No Schedule
              </span>
            )
          }
          circleContent={esnum.estnum}
          title={esnum.name}
          subtitle={note}
          srOnlyText={esnum.hasSchedule ? "Has schedule" : "No schedule yet"}
        />
      </div>
      <FloatingActionMenu
        open={isMenuOpen}
        anchorRef={anchorRef}
        belowOffsetY={-24}
        items={menuItems}
        onClose={() => {
          clearCloseTimer();
          setIsCardHovered(false);
          setIsMenuHovered(false);
          setIsFocusWithin(false);
        }}
        onPointerEnter={() => {
          clearCloseTimer();
          setIsMenuHovered(true);
        }}
        onPointerLeave={() => {
          setIsMenuHovered(false);
          if (!isCardHovered && !isFocusWithin) {
            queueMenuClose();
          }
        }}
      />
    </>
  );
}
