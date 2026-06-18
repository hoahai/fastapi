import type { ApiRequestOptions } from "@shared/hooks/useApiRequest";

import type {
  LeaveSphereHoliday,
  LeaveSpherePtoBalance,
  LeaveSpherePtoRequest,
  LeaveSpherePtoType,
  LeaveSphereTeamRegion,
} from "@leavesphere/lib/ptoTypes";
import { normalizeLeaveSphereTeamRegion } from "@leavesphere/lib/ptoTypes";
import { normalizeLeaveSpherePtoStatus } from "@leavesphere/lib/ptoStatus";
import type {
  LeaveManagementActionCode,
  LeaveManagementTransaction,
  LeaveManagementTransactionStatus,
} from "@leavesphere/lib/leaveManagementBalanceLedger";

export type LeaveManagementEmployee = {
  employeeId: string;
  employeeName: string;
  pictureUrl?: string | null;
  title: string;
  managerId: string;
  managerName: string;
  teamRegion: LeaveSphereTeamRegion;
  active: boolean;
};

export type LeaveManagementTypeConfig = {
  code: string;
  type?: string;
  label: string;
  active: boolean;
};

export type LeaveManagementActionConfig = {
  code: string;
  label: string;
  detail: string;
};

export type LeaveManagementWorkspaceData = {
  currentUserId: string;
  currentUserName: string;
  currentUserEmail: string;
  managerId: string | null;
  currentUserTeamRegion: LeaveSphereTeamRegion;
  isManager: boolean;
  employees: LeaveManagementEmployee[];
  balanceTransactions: LeaveManagementTransaction[];
  employeeBalances: Array<{
    employeeId: string;
    employeeName: string;
    balances: LeaveSpherePtoBalance[];
  }>;
  requests: LeaveSpherePtoRequest[];
  holidays: LeaveSphereHoliday[];
  ptoTypes: LeaveManagementTypeConfig[];
  ptoActions: LeaveManagementActionConfig[];
  defaultRequestActionCode?: string;
  defaultCancelActionCode?: string;
  lastSubmissionDate?: string | null;
};

export type LeaveManagementWorkspaceDelta = Partial<LeaveManagementWorkspaceData>;

export type LeaveManagementLoadResult = {
  workspace: LeaveManagementWorkspaceData;
  refreshMessage: string | null;
};

export type LeaveManagementMutationResult = {
  workspace: LeaveManagementWorkspaceData;
  createdRequestId?: string | null;
};

export type LeaveManagementCreateRequestInput = {
  employeeId: string;
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: number;
  description: string;
  approveImmediately?: boolean;
};

export type LeaveManagementReviewRequestInput = {
  requestId: string;
  approve?: boolean;
  action?: "approve" | "reject" | "cancel" | "revert";
  approverNote: string;
};

export type LeaveManagementUpdateRequestInput = {
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

export type LeaveManagementAdjustBalanceInput = {
  employeeId: string;
  ptoTypeCode: string;
  ptoActionCode: LeaveManagementActionCode;
  transactionId?: string | null;
  hours: number;
  year: number;
  status: LeaveManagementTransactionStatus;
  description: string;
  approverNote: string;
};

export type LeaveManagementSetupInput =
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
  currentWorkspace?: LeaveManagementWorkspaceData | null;
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
  payload: LeaveManagementCreateRequestInput;
};

type ReviewRequestArgs = BaseArgs & {
  payload: LeaveManagementReviewRequestInput;
};

type UpdateRequestArgs = BaseArgs & {
  payload: LeaveManagementUpdateRequestInput;
};

type AdjustBalanceArgs = BaseArgs & {
  payload: LeaveManagementAdjustBalanceInput;
};

type SetupArgs = BaseArgs & {
  payload: LeaveManagementSetupInput;
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

function cloneWorkspace(workspace: LeaveManagementWorkspaceData): LeaveManagementWorkspaceData {
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

function cloneBalanceRows(rows: LeaveSpherePtoBalance[]): LeaveSpherePtoBalance[] {
  return rows.map((item) => ({ ...item }));
}

function cloneEmployeeBalanceRows(
  rows: LeaveManagementWorkspaceData["employeeBalances"],
): LeaveManagementWorkspaceData["employeeBalances"] {
  return rows.map((item) => ({
    ...item,
    balances: cloneBalanceRows(item.balances),
  }));
}

function mergeListByKey<T>(
  currentItems: T[],
  incomingItems: T[] | undefined | null,
  getKey: (item: T) => string,
): T[] {
  if (!Array.isArray(incomingItems)) {
    return currentItems.map((item) => ({ ...item }));
  }

  const incomingByKey = new Map<string, T>();
  const incomingOrder: string[] = [];
  for (const item of incomingItems) {
    const key = getKey(item);
    if (!key || incomingByKey.has(key)) {
      continue;
    }
    incomingByKey.set(key, item);
    incomingOrder.push(key);
  }

  const merged: T[] = [];
  const seenKeys = new Set<string>();

  for (const item of currentItems) {
    const key = getKey(item);
    if (!key || seenKeys.has(key)) {
      continue;
    }
    seenKeys.add(key);
    const incoming = incomingByKey.get(key);
    merged.push(incoming ? incoming : { ...item });
  }

  for (const key of incomingOrder) {
    if (seenKeys.has(key)) {
      continue;
    }
    seenKeys.add(key);
    const incoming = incomingByKey.get(key);
    if (incoming) {
      merged.push(incoming);
    }
  }

  return merged;
}

function mergeBalanceRows(
  currentRows: LeaveSpherePtoBalance[],
  incomingRows: LeaveSpherePtoBalance[] | undefined | null,
): LeaveSpherePtoBalance[] {
  if (!Array.isArray(incomingRows)) {
    return cloneBalanceRows(currentRows);
  }
  return mergeListByKey(
    currentRows,
    incomingRows,
    (item) => item.code || item.type || item.label,
  );
}

function mergeEmployeeBalanceRows(
  currentRows: LeaveManagementWorkspaceData["employeeBalances"],
  incomingRows: LeaveManagementWorkspaceData["employeeBalances"] | undefined | null,
): LeaveManagementWorkspaceData["employeeBalances"] {
  if (!Array.isArray(incomingRows)) {
    return cloneEmployeeBalanceRows(currentRows);
  }

  const currentByEmployeeId = new Map(currentRows.map((item) => [item.employeeId, item] as const));
  const incomingByEmployeeId = new Map<string, LeaveManagementWorkspaceData["employeeBalances"][number]>();
  const incomingOrder: string[] = [];
  for (const row of incomingRows) {
    if (!incomingByEmployeeId.has(row.employeeId)) {
      incomingByEmployeeId.set(row.employeeId, row);
      incomingOrder.push(row.employeeId);
    }
  }
  const merged: LeaveManagementWorkspaceData["employeeBalances"] = [];

  for (const currentRow of currentRows) {
    const incomingRow = incomingByEmployeeId.get(currentRow.employeeId);
    if (!incomingRow) {
      merged.push({
        ...currentRow,
        balances: cloneBalanceRows(currentRow.balances),
      });
      continue;
    }
    merged.push({
      ...(currentRow || {}),
      ...incomingRow,
      balances: mergeBalanceRows(currentRow?.balances || [], incomingRow.balances),
    });
  }

  for (const employeeId of incomingOrder) {
    if (currentByEmployeeId.has(employeeId)) {
      continue;
    }
    const incomingRow = incomingByEmployeeId.get(employeeId);
    if (!incomingRow) {
      continue;
    }
    merged.push({
      ...incomingRow,
      balances: mergeBalanceRows([], incomingRow.balances),
    });
  }

  return merged;
}

export function mergeLeaveManagementWorkspace(
  current: LeaveManagementWorkspaceData | null,
  incoming: LeaveManagementWorkspaceDelta,
): LeaveManagementWorkspaceData {
  if (!current) {
    return cloneWorkspace(incoming as LeaveManagementWorkspaceData);
  }

  return {
    ...current,
    ...incoming,
    currentUserId: incoming.currentUserId || current.currentUserId,
    currentUserName: incoming.currentUserName || current.currentUserName,
    currentUserEmail: incoming.currentUserEmail || current.currentUserEmail,
    managerId: incoming.managerId === undefined ? current.managerId : incoming.managerId,
    currentUserTeamRegion: incoming.currentUserTeamRegion || current.currentUserTeamRegion,
    isManager: incoming.isManager ?? current.isManager,
    employees: mergeListByKey(current.employees, incoming.employees, (item) => item.employeeId),
    balanceTransactions: mergeListByKey(current.balanceTransactions, incoming.balanceTransactions, (item) => item.id),
    employeeBalances: mergeEmployeeBalanceRows(current.employeeBalances, incoming.employeeBalances),
    requests: mergeListByKey(current.requests, incoming.requests, (item) => item.id),
    holidays: mergeListByKey(current.holidays, incoming.holidays, (item) => item.id),
    ptoTypes: mergeListByKey(current.ptoTypes, incoming.ptoTypes, (item) => item.code),
    ptoActions: mergeListByKey(current.ptoActions, incoming.ptoActions, (item) => item.code),
  };
}

function normalizeWorkspaceResponse(payload: unknown): LeaveManagementWorkspaceData | null {
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
          ptoActionCode: asString(item.ptoActionCode).toLowerCase() as LeaveManagementActionCode,
          hours: asNumber(item.hours),
          year: Math.trunc(asNumber(item.year)),
          status: (asString(item.status) as LeaveManagementTransactionStatus) || "Approved",
          description: asString(item.description),
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
          status: normalizeLeaveSpherePtoStatus(item.status),
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
    currentUserEmail: asString(workspaceRaw.currentUserEmail),
    managerId: workspaceRaw.managerId === undefined ? null : asString(workspaceRaw.managerId) || null,
    currentUserTeamRegion: normalizeLeaveSphereTeamRegion(workspaceRaw.currentUserTeamRegion),
    isManager: Boolean(workspaceRaw.isManager),
    employees,
    balanceTransactions,
    employeeBalances,
    requests,
    holidays,
    ptoTypes,
    ptoActions,
    defaultRequestActionCode: asString(workspaceRaw.defaultRequestActionCode) || undefined,
    defaultCancelActionCode: asString(workspaceRaw.defaultCancelActionCode) || undefined,
    lastSubmissionDate: asString(workspaceRaw.lastSubmissionDate) || null,
  };
}

function normalizeWorkspaceDelta(payload: unknown): LeaveManagementWorkspaceDelta | null {
  const raw = unwrapEnvelope(payload);
  const workspaceRaw = isRecord(raw) && isRecord(raw.workspacePatch) ? raw.workspacePatch : null;
  if (!isRecord(workspaceRaw)) {
    return null;
  }

  const patch: LeaveManagementWorkspaceDelta = {};
  if ("currentUserId" in workspaceRaw) {
    patch.currentUserId = asString(workspaceRaw.currentUserId);
  }
  if ("currentUserName" in workspaceRaw) {
    patch.currentUserName = asString(workspaceRaw.currentUserName);
  }
  if ("currentUserEmail" in workspaceRaw) {
    patch.currentUserEmail = asString(workspaceRaw.currentUserEmail) || undefined;
  }
  if ("managerId" in workspaceRaw) {
    patch.managerId = workspaceRaw.managerId === undefined ? undefined : asString(workspaceRaw.managerId) || null;
  }
  if ("currentUserTeamRegion" in workspaceRaw) {
    patch.currentUserTeamRegion = normalizeLeaveSphereTeamRegion(workspaceRaw.currentUserTeamRegion);
  }
  if ("isManager" in workspaceRaw) {
    patch.isManager = Boolean(workspaceRaw.isManager);
  }
  if ("defaultRequestActionCode" in workspaceRaw) {
    patch.defaultRequestActionCode = asString(workspaceRaw.defaultRequestActionCode) || undefined;
  }
  if ("defaultCancelActionCode" in workspaceRaw) {
    patch.defaultCancelActionCode = asString(workspaceRaw.defaultCancelActionCode) || undefined;
  }
  if ("lastSubmissionDate" in workspaceRaw) {
    patch.lastSubmissionDate = asString(workspaceRaw.lastSubmissionDate) || null;
  }
  if (Array.isArray(workspaceRaw.employees)) {
    patch.employees = workspaceRaw.employees
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
      .filter((item) => item.employeeId && item.employeeName);
  }
  if (Array.isArray(workspaceRaw.balanceTransactions)) {
    patch.balanceTransactions = workspaceRaw.balanceTransactions
      .filter(isRecord)
      .map((item) => ({
        id: asString(item.id),
        employeeId: asString(item.employeeId),
        ptoTypeCode: asString(item.ptoTypeCode),
        ptoActionCode: asString(item.ptoActionCode).toLowerCase() as LeaveManagementActionCode,
        hours: asNumber(item.hours),
        year: Math.trunc(asNumber(item.year)),
        status: (asString(item.status) as LeaveManagementTransactionStatus) || "Approved",
        description: asString(item.description),
        approverNote: asString(item.approverNote) || null,
        createdAt: asString(item.createdAt),
        createdByName: asString(item.createdByName) || null,
      }))
      .filter((item) => item.id && item.employeeId && item.ptoTypeCode);
  }
  if (Array.isArray(workspaceRaw.employeeBalances)) {
    patch.employeeBalances = workspaceRaw.employeeBalances
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
      .filter((row) => row.employeeId && row.employeeName);
  }
  if (Array.isArray(workspaceRaw.requests)) {
    patch.requests = workspaceRaw.requests
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
        status: normalizeLeaveSpherePtoStatus(item.status),
        submittedAt: asString(item.submittedAt),
        reviewedAt: asString(item.reviewedAt) || null,
        reviewerName: asString(item.reviewerName) || null,
      }))
      .filter((item) => item.id && item.type);
  }
  if (Array.isArray(workspaceRaw.holidays)) {
    patch.holidays = workspaceRaw.holidays
      .filter(isRecord)
      .map((item) => ({
        id: asString(item.id),
        name: asString(item.name),
        date: asString(item.date),
        teamRegion: normalizeLeaveSphereTeamRegion(item.teamRegion),
      }))
      .filter((item) => item.id && item.date);
  }
  if (Array.isArray(workspaceRaw.ptoTypes)) {
    patch.ptoTypes = workspaceRaw.ptoTypes
      .filter(isRecord)
      .map((item) => ({
        code: asString(item.code || item.type || item.name),
        type: asString(item.type) || undefined,
        label: asString(item.label) || asString(item.name) || asString(item.code) || "PTO",
        active: Boolean(item.active ?? true),
      }))
      .filter((item) => item.code && item.label);
  }
  if (Array.isArray(workspaceRaw.ptoActions)) {
    patch.ptoActions = workspaceRaw.ptoActions
      .filter(isRecord)
      .map((item) => ({
        code: asString(item.code).toUpperCase(),
        label: asString(item.label) || asString(item.name) || asString(item.code) || "PTO Action",
        detail: asString(item.detail) || asString(item.color) || "",
      }))
      .filter((item) => item.code && item.label);
  }

  return Object.keys(patch).length > 0 ? patch : null;
}

function resolveWorkspaceFromMutationResponse(
  response: unknown,
  currentWorkspace: LeaveManagementWorkspaceData | null | undefined,
): LeaveManagementWorkspaceData | null {
  const workspacePatch = normalizeWorkspaceDelta(response);
  if (workspacePatch) {
    if (!currentWorkspace) {
      return null;
    }
    return mergeLeaveManagementWorkspace(currentWorkspace, workspacePatch);
  }
  return normalizeWorkspaceResponse(response);
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

export async function loadLeaveManagementWorkspace(params: LoadArgs): Promise<LeaveManagementLoadResult> {
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
  if (params.freshData) {
    query.set("fresh_data", "true");
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

export async function createLeaveManagementRequest(params: CreateRequestArgs): Promise<LeaveManagementMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/admin/pto/requests", {
    method: "POST",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = resolveWorkspaceFromMutationResponse(response, params.currentWorkspace);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
    createdRequestId: normalizeCreatedRequestId(response),
  };
}

export async function updateLeaveManagementRequest(params: UpdateRequestArgs): Promise<LeaveManagementMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/admin/pto/requests", {
    method: "PUT",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = resolveWorkspaceFromMutationResponse(response, params.currentWorkspace);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
  };
}

export async function reviewLeaveManagementRequest(params: ReviewRequestArgs): Promise<LeaveManagementMutationResult> {
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
  const workspace = resolveWorkspaceFromMutationResponse(response, params.currentWorkspace);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
  };
}

export async function adjustLeaveManagementBalance(params: AdjustBalanceArgs): Promise<LeaveManagementMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/admin/pto/balances/adjust", {
    method: "POST",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = resolveWorkspaceFromMutationResponse(response, params.currentWorkspace);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
  };
}

export async function updateLeaveManagementSetupData(params: SetupArgs): Promise<LeaveManagementMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/admin/pto/setup", {
    method: "POST",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = resolveWorkspaceFromMutationResponse(response, params.currentWorkspace);
  if (!workspace) {
    throw new Error("Unable to normalize LeaveSphere admin workspace response.");
  }
  return {
    workspace,
  };
}
