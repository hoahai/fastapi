import { shiftIsoDateByDays } from "@shared/utils/time";

import type { LeaveSpherePtoStatus } from "@leavesphere/lib/ptoTypes";

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

export type LeaveSpherePtoRequestSurfaceTone = "neutral" | "pending" | "approved" | "rejected" | "cancelled";
export type LeaveSpherePtoRequestTimelineTone = "past" | "future" | "pending" | "approved" | "rejected" | "cancelled";
export type LeaveSpherePtoRequestSurfaceVariant = "card" | "row" | "tile";

const REQUEST_SURFACE_CLASS_NAMES: Record<LeaveSpherePtoRequestSurfaceVariant, Record<LeaveSpherePtoRequestTimelineTone, string>> = {
  card: {
    future: "border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50/80 focus-visible:ring-slate-300",
    past: "border-slate-200 bg-slate-50/60 hover:border-slate-300 hover:bg-slate-100/50 focus-visible:ring-slate-300",
    pending: "border-amber-200 bg-amber-50 hover:border-amber-300 hover:bg-amber-100/70 focus-visible:ring-amber-300",
    approved: "border-emerald-300 bg-emerald-50 hover:border-emerald-400 hover:bg-emerald-100/80 focus-visible:ring-emerald-300",
    rejected: "border-rose-200 bg-rose-50 hover:border-rose-300 hover:bg-rose-100/70 focus-visible:ring-rose-300",
    cancelled: "border-slate-300 bg-slate-100 hover:border-slate-400 hover:bg-slate-200/70 focus-visible:ring-slate-300",
  },
  row: {
    future: "border-t border-blue-100/80 bg-white text-slate-700 transition-colors hover:bg-blue-50/40",
    past: "border-t border-slate-200/80 bg-slate-50/60 text-slate-700 transition-colors hover:bg-slate-100/50",
    pending: "border-t border-amber-200/80 bg-amber-50 text-slate-700 transition-colors hover:bg-amber-100/70",
    approved: "border-t border-emerald-200/80 bg-emerald-50 text-slate-700 transition-colors hover:bg-emerald-100/70",
    rejected: "border-t border-rose-200/80 bg-rose-50 text-slate-700 transition-colors hover:bg-rose-100/70",
    cancelled: "border-t border-slate-300/80 bg-slate-100 text-slate-700 transition-colors hover:bg-slate-200/70",
  },
  tile: {
    future: "border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50/80",
    past: "border-slate-200 bg-slate-50/60 hover:border-slate-300 hover:bg-slate-100/50",
    pending: "border-amber-200 bg-amber-50 hover:border-amber-300 hover:bg-amber-100/70",
    approved: "border-emerald-200 bg-emerald-50 hover:border-emerald-300 hover:bg-emerald-100/70",
    rejected: "border-rose-200 bg-rose-50 hover:border-rose-300 hover:bg-rose-100/70",
    cancelled: "border-slate-300 bg-slate-100 hover:border-slate-400 hover:bg-slate-200/70",
  },
};

export function normalizeLeaveSpherePtoStatus(value: unknown): LeaveSpherePtoStatus {
  const normalized = asString(value).toLowerCase();
  if (normalized === "approved") {
    return "approved";
  }
  if (normalized === "rejected") {
    return "rejected";
  }
  if (normalized === "cancelled" || normalized === "canceled") {
    return "cancelled";
  }
  return "pending";
}

export function formatLeaveSpherePtoStatusLabel(status: LeaveSpherePtoStatus): string {
  if (status === "approved") {
    return "Approved";
  }
  if (status === "rejected") {
    return "Rejected";
  }
  if (status === "cancelled") {
    return "Cancelled";
  }
  return "Pending";
}

export function getLeaveSpherePtoRequestSurfaceTone(status: LeaveSpherePtoStatus): LeaveSpherePtoRequestSurfaceTone {
  if (status === "approved") {
    return "approved";
  }
  if (status === "pending") {
    return "pending";
  }
  if (status === "rejected") {
    return "rejected";
  }
  if (status === "cancelled") {
    return "cancelled";
  }
  return "neutral";
}

export function getLeaveSpherePtoRequestTimelineTone(params: {
  status: LeaveSpherePtoStatus;
  startDate: string;
  endDate: string;
  todayIsoDate: string;
}): LeaveSpherePtoRequestTimelineTone {
  const { status, startDate, endDate, todayIsoDate } = params;
  if (status === "pending") {
    return "pending";
  }
  if (status === "cancelled") {
    return "cancelled";
  }
  if (endDate && todayIsoDate && endDate < todayIsoDate) {
    return "past";
  }
  if (status === "approved" && startDate && todayIsoDate) {
    const next7Days = shiftIsoDateByDays(todayIsoDate, 7);
    if (startDate <= next7Days) {
      return "approved";
    }
  }
  if (status === "rejected") {
    return "rejected";
  }
  return "future";
}

export function getLeaveSpherePtoRequestSurfaceClassName(
  tone: LeaveSpherePtoRequestTimelineTone,
  variant: LeaveSpherePtoRequestSurfaceVariant = "card",
): string {
  return REQUEST_SURFACE_CLASS_NAMES[variant][tone];
}
