import type { ApiRequestOptions } from "@shared/hooks/useApiRequest";

import type {
  LeaveSphereHoliday,
  LeaveSpherePtoBalance,
  LeaveSpherePtoRequest,
  LeaveSpherePtoStatus,
  LeaveSpherePtoType,
  LeaveSphereTeamRegion,
} from "@leavesphere/lib/ptoTypes";
import { normalizeLeaveSphereTeamRegion } from "@leavesphere/lib/ptoTypes";
import type {
  LeaveSphereAdminPtoActionCode,
  LeaveSphereAdminPtoTransaction,
  LeaveSphereAdminPtoTransactionStatus,
} from "@leavesphere/lib/adminPtoBalanceLedger";

export type LeaveSphereAdminEmployee = {
  employeeId: string;
  employeeName: string;
  pictureUrl?: string | null;
  title: string;
  managerId: string;
  managerName: string;
  teamRegion: LeaveSphereTeamRegion;
  active: boolean;
};

export type LeaveSphereAdminPtoTypeConfig = {
  code: string;
  type?: string;
  label: string;
  active: boolean;
};

export type LeaveSphereAdminPtoActionConfig = {
  code: string;
  label: string;
  detail: string;
};

export type LeaveSphereAdminWorkspaceData = {
  currentUserId: string;
  currentUserName: string;
  employees: LeaveSphereAdminEmployee[];
  balanceTransactions: LeaveSphereAdminPtoTransaction[];
  employeeBalances: Array<{
    employeeId: string;
    employeeName: string;
    balances: LeaveSpherePtoBalance[];
  }>;
  requests: LeaveSpherePtoRequest[];
  holidays: LeaveSphereHoliday[];
  ptoTypes: LeaveSphereAdminPtoTypeConfig[];
  ptoActions: LeaveSphereAdminPtoActionConfig[];
};

export type LeaveSphereAdminLoadResult = {
  workspace: LeaveSphereAdminWorkspaceData;
  refreshMessage: string | null;
};

export type LeaveSphereAdminMutationResult = {
  workspace: LeaveSphereAdminWorkspaceData;
  createdRequestId?: string | null;
};

export type LeaveSphereAdminCreateRequestInput = {
  employeeId: string;
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: number;
  description: string;
  approveImmediately?: boolean;
};

export type LeaveSphereAdminReviewRequestInput = {
  requestId: string;
  approve?: boolean;
  action?: "approve" | "reject" | "cancel" | "revert";
  approverNote: string;
};

export type LeaveSphereAdminUpdateRequestInput = {
  transactionId: string;
  type: string;
  startDate: string;
  endDate: string;
  hours: number;
  description: string;
  year?: number | null;
  ptoTypeCode?: string | null;
  calendarId?: string | null;
};

export type LeaveSphereAdminAdjustBalanceInput = {
  employeeId: string;
  ptoTypeCode: string;
  ptoActionCode: LeaveSphereAdminPtoActionCode;
  transactionId?: string | null;
  hours: number;
  year: number;
  status: LeaveSphereAdminPtoTransactionStatus;
  approverNote: string;
};

export type LeaveSphereAdminSetupInput =
  | {
      kind: "pto_type";
      code: string;
      label: string;
      active: boolean;
    }
  | {
      kind: "pto_action";
      code: string;
      label: string;
      detail: string;
    }
  | {
      kind: "employee";
      employeeName: string;
      title: string;
      managerId: string;
      teamRegion: LeaveSphereTeamRegion;
    }
  | {
      kind: "employee_manager";
      employeeId: string;
      managerId: string;
    }
  | {
      kind: "holiday";
      name: string;
      date: string;
      teamRegion: LeaveSphereTeamRegion;
    };

type RequestJson = (url: string, options?: ApiRequestOptions) => Promise<unknown>;

type BaseArgs = {
  requestJson: RequestJson;
  workspaceKey: string;
  currentUserId: string;
  currentUserName: string;
  timeZone?: string | null;
};

type LoadArgs = BaseArgs & {
  freshData?: boolean;
  calendarMonth?: string | null;
  historyStartDate?: string | null;
  historyEndDate?: string | null;
  includePending?: boolean;
};

type CreateRequestArgs = BaseArgs & {
  payload: LeaveSphereAdminCreateRequestInput;
};

type ReviewRequestArgs = BaseArgs & {
  payload: LeaveSphereAdminReviewRequestInput;
};

type UpdateRequestArgs = BaseArgs & {
  payload: LeaveSphereAdminUpdateRequestInput;
};

type AdjustBalanceArgs = BaseArgs & {
  payload: LeaveSphereAdminAdjustBalanceInput;
};

type SetupArgs = BaseArgs & {
  payload: LeaveSphereAdminSetupInput;
};

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function asNumber(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function unwrapEnvelope(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

function cloneWorkspace(workspace: LeaveSphereAdminWorkspaceData): LeaveSphereAdminWorkspaceData {
  return {
    ...workspace,
    employees: workspace.employees.map((item) => ({ ...item })),
    balanceTransactions: workspace.balanceTransactions.map((item) => ({ ...item })),
    employeeBalances: workspace.employeeBalances.map((item) => ({
      ...item,
      balances: item.balances.map((balance) => ({ ...balance })),
    })),
    requests: workspace.requests.map((item) => ({ ...item })),
    holidays: workspace.holidays.map((item) => ({ ...item })),
    ptoTypes: workspace.ptoTypes.map((item) => ({ ...item })),
    ptoActions: workspace.ptoActions.map((item) => ({ ...item })),
  };
}

function mergeListByKey<T>(
  baseItems: T[],
  incomingItems: T[],
  getKey: (item: T) => string,
): T[] {
  const merged: T[] = [];
  const seenKeys = new Set<string>();

  for (const item of incomingItems) {
    const key = getKey(item);
    if (!key || seenKeys.has(key)) {
      continue;
    }
    seenKeys.add(key);
    merged.push(item);
  }

  for (const item of baseItems) {
    const key = getKey(item);
    if (!key || seenKeys.has(key)) {
      continue;
    }
    seenKeys.add(key);
    merged.push(item);
  }

  return merged;
}

export function mergeLeaveSphereAdminWorkspace(
  current: LeaveSphereAdminWorkspaceData | null,
  incoming: LeaveSphereAdminWorkspaceData,
): LeaveSphereAdminWorkspaceData {
  if (!current) {
    return cloneWorkspace(incoming);
  }

  return {
    ...current,
    ...incoming,
    currentUserId: incoming.currentUserId || current.currentUserId,
    currentUserName: incoming.currentUserName || current.currentUserName,
    employees: mergeListByKey(incoming.employees, current.employees, (item) => item.employeeId),
    balanceTransactions: incoming.balanceTransactions.map((item) => ({ ...item })),
    employeeBalances: incoming.employeeBalances.map((item) => ({
      ...item,
      balances: item.balances.map((balance) => ({ ...balance })),
    })),
    requests: mergeListByKey(incoming.requests, current.requests, (item) => item.id),
    holidays: mergeListByKey(incoming.holidays, current.holidays, (item) => item.id),
    ptoTypes: mergeListByKey(incoming.ptoTypes, current.ptoTypes, (item) => item.code),
    ptoActions: mergeListByKey(incoming.ptoActions, current.ptoActions, (item) => item.code),
  };
}

function normalizeWorkspaceResponse(payload: unknown): LeaveSphereAdminWorkspaceData | null {
  const raw = unwrapEnvelope(payload);
  const workspaceRaw = isRecord(raw) && isRecord(raw.workspace) ? raw.workspace : raw;
  if (!isRecord(workspaceRaw)) {
    return null;
  }

  const currentUserId = asString(workspaceRaw.currentUserId);
  const currentUserName = asString(workspaceRaw.currentUserName);
  if (!currentUserId || !currentUserName) {
    return null;
  }

  const employees = Array.isArray(workspaceRaw.employees)
    ? workspaceRaw.employees
        .filter(isRecord)
        .map((item) => ({
          employeeId: asString(item.employeeId),
          employeeName: asString(item.employeeName),
          pictureUrl: asString(item.pictureUrl) || null,
          title: asString(item.title),
          managerId: asString(item.managerId),
          managerName: asString(item.managerName),
          teamRegion: normalizeLeaveSphereTeamRegion(item.teamRegion),
          active: Boolean(item.active),
        }))
        .filter((item) => item.employeeId && item.employeeName)
    : [];

  const balanceTransactions = Array.isArray(workspaceRaw.balanceTransactions)
    ? workspaceRaw.balanceTransactions
        .filter(isRecord)
        .map((item) => ({
          id: asString(item.id),
          employeeId: asString(item.employeeId),
          ptoTypeCode: asString(item.ptoTypeCode),
          ptoActionCode: asString(item.ptoActionCode).toLowerCase() as LeaveSphereAdminPtoActionCode,
          hours: asNumber(item.hours),
          year: Math.trunc(asNumber(item.year)),
          status: (asString(item.status) as LeaveSphereAdminPtoTransactionStatus) || "Approved",
          approverNote: asString(item.approverNote) || null,
          createdAt: asString(item.createdAt),
          createdByName: asString(item.createdByName) || null,
        }))
        .filter((item) => item.id && item.employeeId && item.ptoTypeCode)
    : [];

  const employeeBalances = Array.isArray(workspaceRaw.employeeBalances)
    ? workspaceRaw.employeeBalances
        .filter(isRecord)
        .map((row) => ({
          employeeId: asString(row.employeeId),
          employeeName: asString(row.employeeName),
          balances: Array.isArray(row.balances)
            ? row.balances
                .filter(isRecord)
                .map((balance) => ({
                  type: asString(balance.type) || asString(balance.code),
                  code: asString(balance.code) || undefined,
                  label: asString(balance.label) || asString(balance.code) || asString(balance.type) || "PTO",
                  totalHours: asNumber(balance.totalHours),
                  usedHours: asNumber(balance.usedHours),
                  scheduledHours: asNumber(balance.scheduledHours),
                  remainingHours: balance.remainingHours === undefined ? undefined : asNumber(balance.remainingHours),
                }))
            : [],
        }))
        .filter((row) => row.employeeId && row.employeeName)
    : [];

  const requests = Array.isArray(workspaceRaw.requests)
    ? workspaceRaw.requests
        .filter(isRecord)
        .map((item) => ({
          id: asString(item.id),
          employeeId: asString(item.employeeId),
          managerId: asString(item.managerId),
          type: asString(item.type) || "vacation",
          ptoTypeCode: asString(item.ptoTypeCode) || undefined,
          startDate: asString(item.startDate),
          endDate: asString(item.endDate),
          hours: asNumber(item.hours),
          description: asString(item.description),
          approverNote: asString(item.approverNote) || null,
          status: (asString(item.status) as LeaveSpherePtoStatus) || "pending",
          submittedAt: asString(item.submittedAt),
          reviewedAt: asString(item.reviewedAt) || null,
          reviewerName: asString(item.reviewerName) || null,
        }))
        .filter((item) => item.id && item.type)
    : [];

  const holidays = Array.isArray(workspaceRaw.holidays)
    ? workspaceRaw.holidays
        .filter(isRecord)
        .map((item) => ({
          id: asString(item.id),
          name: asString(item.name),
          date: asString(item.date),
          teamRegion: normalizeLeaveSphereTeamRegion(item.teamRegion),
        }))
        .filter((item) => item.id && item.date)
    : [];

  const ptoTypes = Array.isArray(workspaceRaw.ptoTypes)
    ? workspaceRaw.ptoTypes
        .filter(isRecord)
        .map((item) => ({
          code: asString(item.code || item.type || item.name),
          type: asString(item.type) || undefined,
          label: asString(item.label) || asString(item.name) || asString(item.code) || "PTO",
          active: Boolean(item.active ?? true),
        }))
        .filter((item) => item.code && item.label)
    : [];

  const ptoActions = Array.isArray(workspaceRaw.ptoActions)
    ? workspaceRaw.ptoActions
        .filter(isRecord)
        .map((item) => ({
          code: asString(item.code).toUpperCase(),
          label: asString(item.label) || asString(item.name) || asString(item.code) || "PTO Action",
          detail: asString(item.detail) || asString(item.color) || "",
        }))
        .filter((item) => item.code && item.label)
    : [];

  return {
    currentUserId,
    currentUserName,
    employees,
    balanceTransactions,
    employeeBalances,
    requests,
    holidays,
    ptoTypes,
    ptoActions,
  };
}

function normalizeCreatedRequestId(payload: unknown): string | null {
  const raw = unwrapEnvelope(payload);
  if (!isRecord(raw)) {
    return null;
  }
  const createdRequestId = asString(raw.createdRequestId);
  if (createdRequestId) {
    return createdRequestId;
  }
  if (isRecord(raw.workspace)) {
    return asString(raw.workspace.createdRequestId) || null;
  }
  return null;
}

export async function loadLeaveSphereAdminPtoWorkspace(params: LoadArgs): Promise<LeaveSphereAdminLoadResult> {
  const query = new URLSearchParams();
  if (params.calendarMonth) {
    query.set("overlap_month", params.calendarMonth);
  }
  if (params.historyStartDate) {
    query.set("history_start_date", params.historyStartDate);
  }
  if (params.historyEndDate) {
    query.set("history_end_date", params.historyEndDate);
  }
  if (params.includePending !== undefined) {
    query.set("include_pending", String(params.includePending));
  }

  const endpoint = query.toString()
    ? `/api/leavesphere/v1/admin/pto/workspace?${query.toString()}`
    : "/api/leavesphere/v1/admin/pto/workspace";
  const payload = await params.requestJson(endpoint, {
    method: "GET",
    successToast: false,
    errorToast: false,
  });
  const workspace = normalizeWorkspaceResponse(payload);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
    refreshMessage: null,
  };
}

export async function createLeaveSphereAdminPtoRequest(params: CreateRequestArgs): Promise<LeaveSphereAdminMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/admin/pto/requests", {
    method: "POST",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = normalizeWorkspaceResponse(response);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
    createdRequestId: normalizeCreatedRequestId(response),
  };
}

export async function updateLeaveSphereAdminPtoRequest(params: UpdateRequestArgs): Promise<LeaveSphereAdminMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/admin/pto/requests", {
    method: "PUT",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = normalizeWorkspaceResponse(response);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
  };
}

export async function reviewLeaveSphereAdminPtoRequest(params: ReviewRequestArgs): Promise<LeaveSphereAdminMutationResult> {
  const action = params.payload.action || (params.payload.approve ? "approve" : "reject");
  const response = await params.requestJson("/api/leavesphere/v1/admin/pto/review", {
    method: "POST",
    body: {
      requestId: params.payload.requestId,
      action,
      approverNote: params.payload.approverNote,
    },
    successToast: false,
    errorToast: false,
  });
  const workspace = normalizeWorkspaceResponse(response);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
  };
}

export async function adjustLeaveSphereAdminPtoBalance(params: AdjustBalanceArgs): Promise<LeaveSphereAdminMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/admin/pto/balances/adjust", {
    method: "POST",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = normalizeWorkspaceResponse(response);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
  };
}

export async function updateLeaveSphereAdminSetupData(params: SetupArgs): Promise<LeaveSphereAdminMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/admin/pto/setup", {
    method: "POST",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = normalizeWorkspaceResponse(response);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
  };
}
