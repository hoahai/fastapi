import { DEFAULT_TIME_ZONE, getCurrentYearInTimeZone } from "@shared/utils/time";
import {
  type LeaveSphereDirectReport,
  type LeaveSphereHoliday,
  type LeaveSpherePtoActionConfig,
  type LeaveSpherePtoBalance,
  type LeaveSpherePtoEmployee,
  type LeaveSpherePtoLoadResult,
  type LeaveSpherePtoMutationResult,
  type LeaveSpherePtoRequest,
  type LeaveSpherePtoReviewInput,
  type LeaveSpherePtoStatus,
  type LeaveSpherePtoSubmitInput,
  type LeaveSpherePtoType,
  type LeaveSpherePtoTypeConfig,
  type LeaveSpherePtoUpdateInput,
  type LeaveSpherePtoWorkspaceData,
  normalizeLeaveSphereTeamRegion,
} from "@leavesphere/lib/ptoTypes";

type RequestJson = (url: string, options?: { method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE"; body?: unknown; successToast?: boolean | string | { title: string; message?: string }; errorToast?: boolean | string | { title: string; message?: string } }) => Promise<unknown>;

type WorkspaceArgs = {
  requestJson: RequestJson;
  year?: number;
  timeZone?: string | null;
};

type SubmitArgs = {
  requestJson: RequestJson;
  payload: LeaveSpherePtoSubmitInput;
};

type UpdateArgs = {
  requestJson: RequestJson;
  payload: LeaveSpherePtoUpdateInput;
};

type ReviewArgs = {
  requestJson: RequestJson;
  payload: LeaveSpherePtoReviewInput;
};

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function asNumber(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
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

function normalizePtoType(value: unknown): LeaveSpherePtoType | null {
  const text = asString(value);
  return text || null;
}

function normalizePtoStatus(value: unknown): LeaveSpherePtoStatus {
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

function normalizeDate(value: unknown): string {
  const text = asString(value);
  if (!text) {
    return "";
  }
  return text.slice(0, 10);
}

function normalizePtoTypeConfig(value: unknown): LeaveSpherePtoTypeConfig | null {
  if (!isRecord(value)) {
    return null;
  }
  const type = normalizePtoType(value.type ?? value.code ?? value.name);
  if (!type) {
    return null;
  }
  const code = asString(value.code) || type;
  const label = asString(value.label) || asString(value.name) || code;
  return {
    code,
    type,
    label,
    active: Boolean(value.active ?? true),
    listingOrder: asNumber(value.listingOrder),
    rolloverable: value.rolloverable === undefined ? undefined : Boolean(value.rolloverable),
    payoutable: value.payoutable === undefined ? undefined : Boolean(value.payoutable),
    usaDefaultHour: value.usaDefaultHour === undefined ? undefined : asNumber(value.usaDefaultHour),
    phlDefaultHour: value.phlDefaultHour === undefined ? undefined : asNumber(value.phlDefaultHour),
  };
}

function normalizePtoActionConfig(value: unknown): LeaveSpherePtoActionConfig | null {
  if (!isRecord(value)) {
    return null;
  }
  const code = asString(value.code).toUpperCase();
  const name = asString(value.name) || code;
  if (!code && !name) {
    return null;
  }
  return {
    code: code || name.toUpperCase(),
    name,
    color: value.color === undefined ? null : asString(value.color) || null,
  };
}

function normalizeBalanceRow(value: unknown): LeaveSpherePtoBalance | null {
  if (!isRecord(value)) {
    return null;
  }
  const type = asString(value.type ?? value.code ?? value.label);
  if (!type) {
    return null;
  }
  const label = asString(value.label) || asString(value.code) || type;
  return {
    type,
    code: asString(value.code) || undefined,
    label,
    totalHours: asNumber(value.totalHours),
    usedHours: asNumber(value.usedHours),
    scheduledHours: asNumber(value.scheduledHours),
    remainingHours: value.remainingHours === undefined ? undefined : asNumber(value.remainingHours),
  };
}

function normalizeDirectReport(value: unknown): LeaveSphereDirectReport | null {
  if (!isRecord(value)) {
    return null;
  }
  const employeeId = asString(value.employeeId);
  const employeeName = asString(value.employeeName) || asString(value.name);
  if (!employeeId || !employeeName) {
    return null;
  }
  return {
    employeeId,
    employeeName,
    pictureUrl: value.pictureUrl === undefined ? null : asString(value.pictureUrl) || null,
    title: asString(value.title),
  };
}

function normalizeEmployee(value: unknown): LeaveSpherePtoEmployee | null {
  if (!isRecord(value)) {
    return null;
  }
  const employeeId = asString(value.employeeId);
  const employeeName = asString(value.employeeName);
  if (!employeeId || !employeeName) {
    return null;
  }
  return {
    employeeId,
    employeeName,
    pictureUrl: value.pictureUrl === undefined ? null : asString(value.pictureUrl) || null,
    title: value.title === undefined ? undefined : asString(value.title) || null,
  };
}

function normalizeHoliday(value: unknown): LeaveSphereHoliday | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asString(value.id);
  const name = asString(value.name);
  const date = normalizeDate(value.date);
  if (!id || !name || !date) {
    return null;
  }
  return {
    id,
    name,
    date,
    teamRegion: normalizeLeaveSphereTeamRegion(value.teamRegion),
  };
}

function normalizeRequest(value: unknown): LeaveSpherePtoRequest | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asString(value.id);
  const employeeId = asString(value.employeeId);
  const type = asString(value.type ?? value.ptoTypeCode);
  if (!id || !employeeId || !type) {
    return null;
  }
  return {
    id,
    employeeId,
    managerId: value.managerId === undefined ? null : asString(value.managerId) || null,
    year: value.year === undefined ? undefined : asNumber(value.year),
    type,
    ptoTypeCode: asString(value.ptoTypeCode) || undefined,
    startDate: normalizeDate(value.startDate),
    endDate: normalizeDate(value.endDate),
    hours: asNumber(value.hours),
    description: asString(value.description),
    approverNote: value.approverNote === undefined ? null : asString(value.approverNote) || null,
    status: normalizePtoStatus(value.status),
    submittedAt: normalizeDate(value.submittedAt),
    reviewedAt: value.reviewedAt === undefined ? null : normalizeDate(value.reviewedAt) || null,
    reviewerName: value.reviewerName === undefined ? null : asString(value.reviewerName) || null,
  };
}

function normalizeWorkspace(payload: unknown): LeaveSpherePtoWorkspaceData | null {
  const raw = unwrapEnvelope(payload);
  const workspace = isRecord(raw) && isRecord(raw.workspace) ? raw.workspace : raw;
  if (!isRecord(workspace)) {
    return null;
  }

  const currentUserId = asString(workspace.currentUserId);
  const currentUserName = asString(workspace.currentUserName);
  const currentUserEmail = asString(workspace.currentUserEmail);
  if (!currentUserId || !currentUserName) {
    return null;
  }

  const ptoTypes = Array.isArray(workspace.ptoTypes)
    ? workspace.ptoTypes.map(normalizePtoTypeConfig).filter((item): item is LeaveSpherePtoTypeConfig => Boolean(item))
    : [];
  const ptoActions = Array.isArray(workspace.ptoActions)
    ? workspace.ptoActions.map(normalizePtoActionConfig).filter((item): item is LeaveSpherePtoActionConfig => Boolean(item))
    : [];
  const employees = Array.isArray(workspace.employees)
    ? workspace.employees.map(normalizeEmployee).filter((item): item is LeaveSpherePtoEmployee => Boolean(item))
    : [];
  const balances = Array.isArray(workspace.balances)
    ? workspace.balances.map(normalizeBalanceRow).filter((item): item is LeaveSpherePtoBalance => Boolean(item))
    : [];
  const requests = Array.isArray(workspace.requests)
    ? workspace.requests.map(normalizeRequest).filter((item): item is LeaveSpherePtoRequest => Boolean(item))
    : [];
  const holidays = Array.isArray(workspace.holidays)
    ? workspace.holidays.map(normalizeHoliday).filter((item): item is LeaveSphereHoliday => Boolean(item))
    : [];
  const directReports = Array.isArray(workspace.directReports)
    ? workspace.directReports.map(normalizeDirectReport).filter((item): item is LeaveSphereDirectReport => Boolean(item))
    : [];

  return {
    currentUserId,
    currentUserName,
    currentUserEmail,
    managerId: workspace.managerId === undefined ? null : asString(workspace.managerId) || null,
    currentUserTeamRegion: normalizeLeaveSphereTeamRegion(workspace.currentUserTeamRegion),
    isManager: Boolean(workspace.isManager),
    employees,
    ptoTypes,
    ptoActions,
    defaultRequestActionCode: asString(workspace.defaultRequestActionCode) || undefined,
    defaultCancelActionCode: asString(workspace.defaultCancelActionCode) || undefined,
    balances,
    requests,
    holidays,
    directReports,
  };
}

function buildWorkspacePayload(response: unknown): LeaveSpherePtoWorkspaceData | null {
  const normalized = normalizeWorkspace(response);
  if (normalized) {
    return normalized;
  }
  const raw = unwrapEnvelope(response);
  if (isRecord(raw) && isRecord(raw.workspace)) {
    return normalizeWorkspace(raw.workspace);
  }
  return null;
}

export async function loadLeaveSpherePtoWorkspace(params: WorkspaceArgs): Promise<LeaveSpherePtoLoadResult> {
  const timeZone = params.timeZone || DEFAULT_TIME_ZONE;
  const response = await params.requestJson(
    `/api/leavesphere/v1/ui/my-pto/load?year=${encodeURIComponent(String(params.year ?? getCurrentYearInTimeZone(timeZone)))}`,
    {
      method: "GET",
      successToast: false,
      errorToast: false,
    },
  );
  const workspace = buildWorkspacePayload(response);
  if (!workspace) {
    throw new Error("Unable to load My PTO workspace.");
  }
  return {
    workspace,
    source: "network",
    refreshMessage: null,
  };
}

export async function submitLeaveSpherePtoRequest(params: SubmitArgs): Promise<LeaveSpherePtoMutationResult> {
  if (!Number.isFinite(params.payload.hours) || params.payload.hours <= 0) {
    throw new Error("Hours must be greater than zero.");
  }
  const response = await params.requestJson("/api/leavesphere/v1/ui/my-pto/requests", {
    method: "POST",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = buildWorkspacePayload(response);
  if (!workspace) {
    throw new Error("Unable to submit PTO request.");
  }
  const raw = unwrapEnvelope(response);
  return {
    workspace,
    source: "network",
    createdRequestId: isRecord(raw) ? asString(raw.createdRequestId) || null : null,
  };
}

export async function updateLeaveSpherePtoRequest(params: UpdateArgs): Promise<LeaveSpherePtoMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/ui/my-pto/requests", {
    method: "PUT",
    body: params.payload,
    successToast: false,
    errorToast: false,
  });
  const workspace = buildWorkspacePayload(response);
  if (!workspace) {
    throw new Error("Unable to update PTO request.");
  }
  const raw = unwrapEnvelope(response);
  return {
    workspace,
    source: "network",
    updated: isRecord(raw) ? asNumber(raw.updated) : undefined,
  };
}

export async function cancelLeaveSpherePtoRequest(params: {
  requestJson: RequestJson;
  transactionId: string;
}): Promise<LeaveSpherePtoMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/ui/my-pto/requests", {
    method: "DELETE",
    body: { transactionId: params.transactionId },
    successToast: false,
    errorToast: false,
  });
  const workspace = buildWorkspacePayload(response);
  if (!workspace) {
    throw new Error("Unable to cancel PTO request.");
  }
  const raw = unwrapEnvelope(response);
  return {
    workspace,
    source: "network",
    updated: isRecord(raw) ? asNumber(raw.updated) : undefined,
  };
}

export async function reviewLeaveSpherePtoRequest(params: ReviewArgs): Promise<LeaveSpherePtoMutationResult> {
  const response = await params.requestJson("/api/leavesphere/v1/ui/my-pto/review", {
    method: "POST",
    body: {
      requestId: params.payload.requestId,
      transactionId: params.payload.requestId,
      action: params.payload.action,
      approverNote: params.payload.approverNote,
    },
    successToast: false,
    errorToast: false,
  });
  const workspace = buildWorkspacePayload(response);
  if (!workspace) {
    throw new Error("Unable to review PTO request.");
  }
  const raw = unwrapEnvelope(response);
  return {
    workspace,
    source: "network",
    updated: isRecord(raw) ? asNumber(raw.updated) : undefined,
  };
}
