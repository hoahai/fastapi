import { useEffect, useRef, useState } from "react";
import { Ban, CalendarDays, CheckCircle2, Clock3, Users, XCircle, type LucideIcon } from "lucide-react";

import type { LeaveSpherePtoStatus, LeaveSphereTeamRegion } from "@leavesphere/lib/ptoTypes";
import { Tooltip, type TooltipPlacement } from "@shared/components/actions/Tooltip";

export type LeaveSpherePtoChipTone =
  | "holiday_us"
  | "holiday_mexico"
  | "holiday_philippines"
  | "approved"
  | "pending"
  | "rejected"
  | "cancelled"
  | "out";

type LeaveSpherePtoChipMeta = {
  className: string;
  label: string;
  Icon: LucideIcon;
};

const CHIP_META: Record<LeaveSpherePtoChipTone, LeaveSpherePtoChipMeta> = {
  holiday_us: {
    className: "border-blue-300 bg-blue-800 text-white",
    label: "U.S. Holiday",
    Icon: CalendarDays,
  },
  holiday_mexico: {
    className: "border-emerald-300 bg-emerald-700 text-white",
    label: "Mexico Holiday",
    Icon: CalendarDays,
  },
  holiday_philippines: {
    className: "border-fuchsia-300 bg-fuchsia-700 text-white",
    label: "Philippines Holiday",
    Icon: CalendarDays,
  },
  pending: {
    className: "border-amber-200 bg-amber-50 text-amber-700",
    label: "Pending",
    Icon: Clock3,
  },
  approved: {
    className: "border-emerald-200 bg-emerald-50 text-emerald-700",
    label: "Approved",
    Icon: CheckCircle2,
  },
  rejected: {
    className: "border-rose-200 bg-rose-50 text-rose-700",
    label: "Rejected",
    Icon: XCircle,
  },
  cancelled: {
    className: "border-slate-300 bg-slate-100 text-slate-600",
    label: "Cancelled",
    Icon: Ban,
  },
  out: {
    className: "border-blue-200 bg-blue-50 text-blue-700",
    label: "Out",
    Icon: Users,
  },
};

export function mapLeaveSphereHolidayRegionToChipTone(teamRegion: LeaveSphereTeamRegion): LeaveSpherePtoChipTone {
  if (teamRegion === "Mexico") {
    return "holiday_mexico";
  }
  if (teamRegion === "Philippines") {
    return "holiday_philippines";
  }
  return "holiday_us";
}

export function mapLeaveSpherePtoStatusToChipTone(status: LeaveSpherePtoStatus): LeaveSpherePtoChipTone {
  if (status === "approved") {
    return "approved";
  }
  if (status === "rejected") {
    return "rejected";
  }
  if (status === "cancelled") {
    return "cancelled";
  }
  return "pending";
}

export function getLeaveSpherePtoChipMeta(tone: LeaveSpherePtoChipTone): LeaveSpherePtoChipMeta {
  return CHIP_META[tone];
}

type LeaveSpherePtoChipLabelProps = {
  tone: LeaveSpherePtoChipTone;
  label?: string;
  className?: string;
  expandHitArea?: boolean;
  truncate?: boolean;
  showTooltip?: boolean;
  tooltipText?: string;
  tooltipClassName?: string;
  tooltipPlacement?: TooltipPlacement;
  tooltipOffsetY?: number;
  tooltipFollowCursor?: boolean;
  tooltipCursorOffsetX?: number;
  tooltipCursorOffsetY?: number;
  tooltipOpenDelayMs?: number;
};

export function LeaveSpherePtoChipLabel({
  tone,
  label,
  className,
  expandHitArea = false,
  truncate = false,
  showTooltip = false,
  tooltipText,
  tooltipClassName,
  tooltipPlacement,
  tooltipOffsetY,
  tooltipFollowCursor,
  tooltipCursorOffsetX,
  tooltipCursorOffsetY,
  tooltipOpenDelayMs = 0,
}: LeaveSpherePtoChipLabelProps) {
  const { Icon, label: defaultLabel } = getLeaveSpherePtoChipMeta(tone);
  const resolvedLabel = label ?? defaultLabel;
  const anchorRef = useRef<HTMLSpanElement | null>(null);
  const openDelayTimeoutRef = useRef<number | null>(null);
  const [isTooltipOpen, setIsTooltipOpen] = useState(false);
  const resolvedTooltipText = tooltipText ?? resolvedLabel;
  const clearOpenDelayTimeout = () => {
    if (openDelayTimeoutRef.current === null) {
      return;
    }
    window.clearTimeout(openDelayTimeoutRef.current);
    openDelayTimeoutRef.current = null;
  };

  useEffect(() => () => {
    if (openDelayTimeoutRef.current !== null) {
      window.clearTimeout(openDelayTimeoutRef.current);
      openDelayTimeoutRef.current = null;
    }
  }, []);

  return (
    <>
      <span
        ref={anchorRef}
        className={`${expandHitArea ? "flex h-full w-full min-w-0" : "inline-flex max-w-full"} items-center gap-1 align-middle leading-none ${className ?? ""}`.trim()}
        onMouseEnter={() => {
          if (showTooltip) {
            clearOpenDelayTimeout();
            if (tooltipOpenDelayMs <= 0) {
              setIsTooltipOpen(true);
              return;
            }
            openDelayTimeoutRef.current = window.setTimeout(() => {
              setIsTooltipOpen(true);
              openDelayTimeoutRef.current = null;
            }, tooltipOpenDelayMs);
          }
        }}
        onMouseLeave={() => {
          if (showTooltip) {
            clearOpenDelayTimeout();
            setIsTooltipOpen(false);
          }
        }}
      >
        <Icon className="size-3 shrink-0 align-middle" />
        <span className={`${truncate ? "truncate" : ""} block leading-none`.trim()}>{resolvedLabel}</span>
      </span>
      {showTooltip ? (
        <Tooltip
          open={isTooltipOpen}
          anchorRef={anchorRef}
          text={resolvedTooltipText}
          offsetY={tooltipOffsetY}
          placement={tooltipPlacement}
          followCursor={tooltipFollowCursor}
          cursorOffsetX={tooltipCursorOffsetX}
          cursorOffsetY={tooltipCursorOffsetY}
          className={tooltipClassName}
        />
      ) : null}
    </>
  );
}

type LeaveSpherePtoToneChipProps = {
  tone: LeaveSpherePtoChipTone;
  label?: string;
  className?: string;
};

export function LeaveSpherePtoToneChip({ tone, label, className }: LeaveSpherePtoToneChipProps) {
  const { className: toneClassName, label: defaultLabel } = getLeaveSpherePtoChipMeta(tone);
  const resolvedLabel = label ?? defaultLabel;
  return (
    <span className={`inline-flex items-center rounded-full border px-2 py-0.5 leading-none ${toneClassName} ${className ?? ""}`.trim()}>
      <LeaveSpherePtoChipLabel tone={tone} label={resolvedLabel} />
    </span>
  );
}

type LeaveSpherePtoStatusChipProps = {
  status: LeaveSpherePtoStatus;
  label?: string;
  className?: string;
};

export function LeaveSpherePtoStatusChip({ status, label, className }: LeaveSpherePtoStatusChipProps) {
  const tone = mapLeaveSpherePtoStatusToChipTone(status);
  const { className: toneClassName, label: defaultLabel } = getLeaveSpherePtoChipMeta(tone);
  const resolvedLabel = label ?? defaultLabel;
  return (
    <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-semibold leading-none ${toneClassName} ${className ?? ""}`.trim()}>
      <LeaveSpherePtoChipLabel tone={tone} label={resolvedLabel} />
    </span>
  );
}
