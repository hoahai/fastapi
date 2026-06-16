import type { LeaveSpherePtoStatus } from "@leavesphere/lib/ptoTypes";
import { DEFAULT_TIME_ZONE, getTodayIsoDateInTimeZone } from "@shared/utils/time";

export type LeaveSpherePtoActionRole = "user" | "approver" | "admin";
export type LeaveSpherePtoActionStatus = "create" | LeaveSpherePtoStatus;

export type LeaveSpherePtoActionConfig = {
  canEditForm: boolean;
  canSubmit: boolean;
  canCancel: boolean;
  canApprove: boolean;
  canReject: boolean;
  canRevert: boolean;
};

type RuleKey = `${LeaveSpherePtoActionRole}:${"create" | "pending" | "approved" | "rejected" | "canceled"}`;

const NO_ACTIONS: LeaveSpherePtoActionConfig = {
  canEditForm: false,
  canSubmit: false,
  canCancel: false,
  canApprove: false,
  canReject: false,
  canRevert: false,
};

const BEFORE_START_RULES: Record<RuleKey, LeaveSpherePtoActionConfig> = {
  "user:create": { ...NO_ACTIONS, canEditForm: true, canSubmit: true },
  "user:pending": { ...NO_ACTIONS, canEditForm: true, canSubmit: true, canCancel: true },
  "user:approved": { ...NO_ACTIONS },
  "user:rejected": { ...NO_ACTIONS },
  "user:canceled": { ...NO_ACTIONS },

  "approver:create": { ...NO_ACTIONS },
  "approver:pending": { ...NO_ACTIONS, canApprove: true, canReject: true, canCancel: true },
  "approver:approved": { ...NO_ACTIONS, canReject: true, canCancel: true, canRevert: true },
  "approver:rejected": { ...NO_ACTIONS, canApprove: true, canCancel: true, canRevert: true },
  "approver:canceled": { ...NO_ACTIONS, canApprove: true, canReject: true, canRevert: true },

  "admin:create": { ...NO_ACTIONS, canEditForm: true, canSubmit: true },
  "admin:pending": { ...NO_ACTIONS, canEditForm: true, canSubmit: true, canApprove: true, canReject: true, canCancel: true },
  "admin:approved": { ...NO_ACTIONS, canEditForm: true, canSubmit: true, canReject: true, canCancel: true, canRevert: true },
  "admin:rejected": { ...NO_ACTIONS, canApprove: true, canCancel: true, canRevert: true },
  "admin:canceled": { ...NO_ACTIONS, canRevert: true },
};

const ON_OR_AFTER_START_RULES: Record<RuleKey, LeaveSpherePtoActionConfig> = {
  "user:create": { ...NO_ACTIONS, canEditForm: true, canSubmit: true },
  "user:pending": { ...NO_ACTIONS },
  "user:approved": { ...NO_ACTIONS },
  "user:rejected": { ...NO_ACTIONS },
  "user:canceled": { ...NO_ACTIONS },

  "approver:create": { ...NO_ACTIONS },
  "approver:pending": { ...NO_ACTIONS, canApprove: true, canReject: true },
  "approver:approved": { ...NO_ACTIONS },
  "approver:rejected": { ...NO_ACTIONS },
  "approver:canceled": { ...NO_ACTIONS },

  "admin:create": { ...NO_ACTIONS, canEditForm: true, canSubmit: true },
  "admin:pending": { ...NO_ACTIONS, canEditForm: true, canSubmit: true, canApprove: true, canReject: true, canCancel: true },
  "admin:approved": { ...NO_ACTIONS, canEditForm: true, canSubmit: true, canReject: true, canCancel: true },
  "admin:rejected": { ...NO_ACTIONS, canApprove: true, canCancel: true },
  "admin:canceled": { ...NO_ACTIONS, canRevert: true },
};

function normalizeStatus(status: LeaveSpherePtoActionStatus): "create" | "pending" | "approved" | "rejected" | "canceled" {
  if (status === "create") {
    return "create";
  }
  if (status === "cancelled") {
    return "canceled";
  }
  return status;
}

function isBeforeStartDate(startDate: string | null | undefined, todayIso: string): boolean {
  if (!startDate) {
    return true;
  }
  return todayIso < startDate;
}

export function getPtoRequestActionConfig(params: {
  role: LeaveSpherePtoActionRole;
  status: LeaveSpherePtoActionStatus;
  startDate?: string | null;
  todayIsoDate?: string;
}): LeaveSpherePtoActionConfig {
  const normalizedStatus = normalizeStatus(params.status);
  const key = `${params.role}:${normalizedStatus}` as RuleKey;
  const todayIsoDate = params.todayIsoDate ?? getTodayIsoDateInTimeZone(DEFAULT_TIME_ZONE);
  const isBeforeStart = isBeforeStartDate(params.startDate, todayIsoDate);
  const table = isBeforeStart ? BEFORE_START_RULES : ON_OR_AFTER_START_RULES;
  return table[key] ?? NO_ACTIONS;
}
