import type { ApiRequestOptions } from "@shared/hooks/useApiRequest";

import type {
  LeaveSphereTeamRegion,
  LeaveSphereHoliday,
  LeaveSpherePtoRequest,
  LeaveSpherePtoStatus,
  LeaveSpherePtoType,
} from "@leavesphere/lib/ptoMocks";
import {
  normalizeLeaveSphereTeamRegion,
} from "@leavesphere/lib/ptoMocks";
import {
  buildLeaveSphereAdminBalanceUsageRows,
  deriveLeaveSphereAdminEmployeeBalances,
  seedLeaveSphereAdminBalanceTransactions,
  type LeaveSphereAdminEmployeeBalance,
  type LeaveSphereAdminEmployeeBalanceUsageRow,
  type LeaveSphereAdminPtoActionCode,
  type LeaveSphereAdminPtoTransaction,
  type LeaveSphereAdminPtoTransactionStatus,
} from "@leavesphere/lib/adminPtoBalanceLedger";

export type LeaveSphereAdminEmployee = {
  employeeId: string;
  employeeName: string;
  title: string;
  managerId: string;
  managerName: string;
  teamRegion: LeaveSphereTeamRegion;
  active: boolean;
};

export type LeaveSphereAdminPtoTypeConfig = {
  code: string;
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
  employeeBalanceUsage: LeaveSphereAdminEmployeeBalanceUsageRow[];
  balanceTransactions: LeaveSphereAdminPtoTransaction[];
  employeeBalances: LeaveSphereAdminEmployeeBalance[];
  requests: LeaveSpherePtoRequest[];
  holidays: LeaveSphereHoliday[];
  ptoTypes: LeaveSphereAdminPtoTypeConfig[];
  ptoActions: LeaveSphereAdminPtoActionConfig[];
};

export type LeaveSphereAdminLoadResult = {
  workspace: LeaveSphereAdminWorkspaceData;
  source: "mock" | "network";
  refreshMessage: string | null;
};

export type LeaveSphereAdminMutationResult = {
  workspace: LeaveSphereAdminWorkspaceData;
  source: "mock" | "network";
  createdRequestId?: string | null;
};

export type LeaveSphereAdminCreateRequestInput = {
  employeeId: string;
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: number;
  reason: string;
};

export type LeaveSphereAdminReviewRequestInput = {
  requestId: string;
  approve: boolean;
  note: string;
};

export type LeaveSphereAdminUpdateRequestInput = {
  transactionId: string;
  type: string;
  startDate: string;
  endDate: string;
  hours: number;
  reason: string;
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
  note: string;
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
};

type LoadArgs = BaseArgs & {
  freshData?: boolean;
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

const USE_API_FLAG = "VITE_LEAVESPHERE_USE_API";
const MOCK_DELAY_MS = 180;

const PTO_TYPE_LABELS: Record<string, string> = {
  vacation: "Vacation",
  sick: "Sick",
  personal: "Personal",
  floating: "Floating Holiday",
};

const PTO_TYPES: string[] = ["vacation", "sick", "personal", "floating"];

const STORE = new Map<string, LeaveSphereAdminWorkspaceData>();

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

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

function toIsoDate(value: Date): string {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addDays(value: Date, days: number): Date {
  const next = new Date(value);
  next.setDate(next.getDate() + days);
  return next;
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

function resolveUseApi(): boolean {
  const raw = asString(import.meta.env[USE_API_FLAG]);
  if (!raw) {
    return true;
  }
  return raw.toLowerCase() !== "false" && raw !== "0";
}

function cloneWorkspace(workspace: LeaveSphereAdminWorkspaceData): LeaveSphereAdminWorkspaceData {
  return {
    ...workspace,
    employees: workspace.employees.map((item) => ({ ...item })),
    employeeBalanceUsage: workspace.employeeBalanceUsage.map((item) => ({
      ...item,
      balances: item.balances.map((balance) => ({ ...balance })),
    })),
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

function buildBaseEmployeeBalances(params: Record<string, [number, number, number]>): LeaveSphereAdminEmployeeBalance["balances"] {
  return PTO_TYPES.map((type) => {
    const tuple = params[type];
    return {
      type,
      label: PTO_TYPE_LABELS[type],
      totalHours: tuple[0],
      usedHours: tuple[1],
      scheduledHours: tuple[2],
    };
  });
}

function deriveWorkspaceEmployeeBalances(params: {
  employeeBalanceUsage: LeaveSphereAdminEmployeeBalanceUsageRow[];
  balanceTransactions: LeaveSphereAdminPtoTransaction[];
  year: number;
}): LeaveSphereAdminEmployeeBalance[] {
  return deriveLeaveSphereAdminEmployeeBalances({
    usageRows: params.employeeBalanceUsage,
    transactions: params.balanceTransactions,
    year: params.year,
  });
}

function seedWorkspace(params: {
  currentUserId: string;
  currentUserName: string;
}): LeaveSphereAdminWorkspaceData {
  const { currentUserId, currentUserName } = params;
  const now = new Date();

  const employees: LeaveSphereAdminEmployee[] = [
    {
      employeeId: currentUserId,
      employeeName: currentUserName,
      title: "LeaveSphere Admin",
      managerId: currentUserId,
      managerName: currentUserName,
      teamRegion: "US",
      active: true,
    },
    {
      employeeId: "emp-lee-chen",
      employeeName: "Lee Chen",
      title: "Senior Designer",
      managerId: currentUserId,
      managerName: currentUserName,
      teamRegion: "Philippines",
      active: true,
    },
    {
      employeeId: "emp-sara-johnson",
      employeeName: "Sara Johnson",
      title: "Performance Analyst",
      managerId: currentUserId,
      managerName: currentUserName,
      teamRegion: "US",
      active: true,
    },
    {
      employeeId: "emp-mateo-garcia",
      employeeName: "Mateo Garcia",
      title: "Paid Media Specialist",
      managerId: currentUserId,
      managerName: currentUserName,
      teamRegion: "Mexico",
      active: true,
    },
    {
      employeeId: "emp-noah-park",
      employeeName: "Noah Park",
      title: "Lifecycle Marketing Manager",
      managerId: "emp-mateo-garcia",
      managerName: "Mateo Garcia",
      teamRegion: "Mexico",
      active: true,
    },
  ];

  const baseEmployeeBalances: LeaveSphereAdminEmployeeBalance[] = [
    {
      employeeId: currentUserId,
      employeeName: currentUserName,
      balances: buildBaseEmployeeBalances({
        vacation: [120, 48, 8],
        sick: [64, 8, 0],
        personal: [40, 8, 8],
        floating: [16, 8, 0],
      }),
    },
    {
      employeeId: "emp-lee-chen",
      employeeName: "Lee Chen",
      balances: buildBaseEmployeeBalances({
        vacation: [120, 24, 16],
        sick: [64, 16, 8],
        personal: [40, 8, 0],
        floating: [16, 0, 0],
      }),
    },
    {
      employeeId: "emp-sara-johnson",
      employeeName: "Sara Johnson",
      balances: buildBaseEmployeeBalances({
        vacation: [120, 32, 8],
        sick: [64, 16, 0],
        personal: [40, 0, 8],
        floating: [16, 8, 0],
      }),
    },
    {
      employeeId: "emp-mateo-garcia",
      employeeName: "Mateo Garcia",
      balances: buildBaseEmployeeBalances({
        vacation: [120, 56, 0],
        sick: [64, 8, 0],
        personal: [40, 8, 0],
        floating: [16, 0, 8],
      }),
    },
    {
      employeeId: "emp-noah-park",
      employeeName: "Noah Park",
      balances: buildBaseEmployeeBalances({
        vacation: [120, 40, 16],
        sick: [64, 8, 0],
        personal: [40, 8, 0],
        floating: [16, 0, 0],
      }),
    },
  ];

  const requests: LeaveSpherePtoRequest[] = [
    {
      id: "admin-pto-rq-1",
      employeeId: "emp-lee-chen",
      employeeName: "Lee Chen",
      managerId: currentUserId,
      type: "vacation",
      startDate: toIsoDate(addDays(now, 7)),
      endDate: toIsoDate(addDays(now, 9)),
      hours: 24,
      reason: "Family wedding trip",
      status: "pending",
      submittedAt: toIsoDate(addDays(now, -2)),
      reviewedAt: null,
      reviewerName: null,
      managerNote: null,
    },
    {
      id: "admin-pto-rq-2",
      employeeId: "emp-sara-johnson",
      employeeName: "Sara Johnson",
      managerId: currentUserId,
      type: "sick",
      startDate: toIsoDate(addDays(now, 1)),
      endDate: toIsoDate(addDays(now, 1)),
      hours: 8,
      reason: "Scheduled procedure",
      status: "pending",
      submittedAt: toIsoDate(addDays(now, -1)),
      reviewedAt: null,
      reviewerName: null,
      managerNote: null,
    },
    {
      id: "admin-pto-rq-3",
      employeeId: "emp-mateo-garcia",
      employeeName: "Mateo Garcia",
      managerId: currentUserId,
      type: "floating",
      startDate: toIsoDate(addDays(now, -8)),
      endDate: toIsoDate(addDays(now, -8)),
      hours: 8,
      reason: "Floating holiday",
      status: "approved",
      submittedAt: toIsoDate(addDays(now, -12)),
      reviewedAt: toIsoDate(addDays(now, -11)),
      reviewerName: currentUserName,
      managerNote: "Approved.",
    },
    {
      id: "admin-pto-rq-4",
      employeeId: "emp-noah-park",
      employeeName: "Noah Park",
      managerId: "emp-mateo-garcia",
      type: "personal",
      startDate: toIsoDate(addDays(now, 14)),
      endDate: toIsoDate(addDays(now, 14)),
      hours: 8,
      reason: "Family appointment",
      status: "approved",
      submittedAt: toIsoDate(addDays(now, -3)),
      reviewedAt: toIsoDate(addDays(now, -2)),
      reviewerName: "Mateo Garcia",
      managerNote: "Coverage confirmed.",
    },
    {
      id: "admin-pto-rq-5",
      employeeId: currentUserId,
      employeeName: currentUserName,
      managerId: currentUserId,
      type: "vacation",
      startDate: toIsoDate(addDays(now, 18)),
      endDate: toIsoDate(addDays(now, 19)),
      hours: 16,
      reason: "Weekend extension",
      status: "pending",
      submittedAt: toIsoDate(addDays(now, -1)),
      reviewedAt: null,
      reviewerName: null,
      managerNote: null,
    },
  ];

  const year = now.getFullYear();
  const holidays: LeaveSphereHoliday[] = [
    { id: `${year}-us-new-year`, name: "New Year's Day", date: `${year}-01-01`, teamRegion: "US" },
    { id: `${year}-us-memorial`, name: "Memorial Day", date: `${year}-05-25`, teamRegion: "US" },
    { id: `${year}-us-independence`, name: "Independence Day", date: `${year}-07-04`, teamRegion: "US" },
    { id: `${year}-us-labor`, name: "Labor Day", date: `${year}-09-07`, teamRegion: "US" },
    { id: `${year}-mx-new-year`, name: "New Year's Day", date: `${year}-01-01`, teamRegion: "Mexico" },
    { id: `${year}-mx-labor`, name: "Labor Day", date: `${year}-05-01`, teamRegion: "Mexico" },
    { id: `${year}-mx-independence`, name: "Independence Day", date: `${year}-09-16`, teamRegion: "Mexico" },
    { id: `${year}-ph-new-year`, name: "New Year's Day", date: `${year}-01-01`, teamRegion: "Philippines" },
    { id: `${year}-ph-day-of-valour`, name: "Day of Valour", date: `${year}-04-09`, teamRegion: "Philippines" },
    { id: `${year}-ph-independence`, name: "Independence Day", date: `${year}-06-12`, teamRegion: "Philippines" },
  ];

  const ptoTypes: LeaveSphereAdminPtoTypeConfig[] = PTO_TYPES.map((type) => ({
    code: type,
    label: PTO_TYPE_LABELS[type],
    active: true,
  }));

  const ptoActions: LeaveSphereAdminPtoActionConfig[] = [
    { code: "accrual", label: "Accrual", detail: "Monthly PTO accrual import" },
    { code: "adjustment", label: "Manual Adjustment", detail: "Admin override to fix balances" },
    { code: "carry_over", label: "Carry Over", detail: "Year rollover carry-over sync" },
  ];

  const employeeBalanceUsage = buildLeaveSphereAdminBalanceUsageRows(baseEmployeeBalances);
  const balanceTransactions = seedLeaveSphereAdminBalanceTransactions({
    employeeBalances: baseEmployeeBalances,
    years: [year - 1, year, year + 1],
    createdAt: toIsoDate(now),
    createdByName: currentUserName,
  });

  return {
    currentUserId,
    currentUserName,
    employees,
    employeeBalanceUsage,
    balanceTransactions,
    employeeBalances: deriveWorkspaceEmployeeBalances({
      employeeBalanceUsage,
      balanceTransactions,
      year,
    }),
    requests,
    holidays,
    ptoTypes,
    ptoActions,
  };
}

function ensureWorkspace(params: {
  workspaceKey: string;
  currentUserId: string;
  currentUserName: string;
  reset?: boolean;
}): LeaveSphereAdminWorkspaceData {
  if (!params.reset && STORE.has(params.workspaceKey)) {
    return cloneWorkspace(STORE.get(params.workspaceKey)!);
  }
  const seeded = seedWorkspace({
    currentUserId: params.currentUserId,
    currentUserName: params.currentUserName,
  });
  STORE.set(params.workspaceKey, seeded);
  return cloneWorkspace(seeded);
}

function writeWorkspace(workspaceKey: string, workspace: LeaveSphereAdminWorkspaceData): LeaveSphereAdminWorkspaceData {
  STORE.set(workspaceKey, cloneWorkspace(workspace));
  return cloneWorkspace(workspace);
}

function resolveWorkspaceBalanceYear(workspace: LeaveSphereAdminWorkspaceData): number {
  const currentYear = new Date().getFullYear();
  if (workspace.balanceTransactions.some((item) => item.year === currentYear)) {
    return currentYear;
  }
  const firstTransactionYear = workspace.balanceTransactions[0]?.year;
  return Number.isInteger(firstTransactionYear) ? firstTransactionYear : currentYear;
}

function normalizeNetworkWorkspace(payload: unknown): LeaveSphereAdminWorkspaceData | null {
  const raw = unwrapEnvelope(payload);
  if (!isRecord(raw)) {
    return null;
  }
  const currentUserId = asString(raw.currentUserId);
  const currentUserName = asString(raw.currentUserName);
  if (!currentUserId || !currentUserName) {
    return null;
  }

  const workspace = ensureWorkspace({
    workspaceKey: `${currentUserId}:network`,
    currentUserId,
    currentUserName,
    reset: true,
  });

  const employeesRaw = Array.isArray(raw.employees) ? raw.employees : [];
  if (employeesRaw.length > 0) {
    workspace.employees = employeesRaw
      .filter(isRecord)
      .map((item) => ({
        employeeId: asString(item.employeeId),
        employeeName: asString(item.employeeName),
        title: asString(item.title),
        managerId: asString(item.managerId),
        managerName: asString(item.managerName),
        teamRegion: normalizeLeaveSphereTeamRegion(item.teamRegion),
        active: Boolean(item.active),
      }))
      .filter((item) => item.employeeId && item.employeeName);
  }

  const holidaysRaw = Array.isArray(raw.holidays) ? raw.holidays : [];
  if (holidaysRaw.length > 0) {
    workspace.holidays = holidaysRaw
      .filter(isRecord)
      .map((item) => ({
        id: asString(item.id),
        name: asString(item.name),
        date: asString(item.date),
        teamRegion: normalizeLeaveSphereTeamRegion(item.teamRegion),
      }))
      .filter((item) => item.id && item.date);
  }

  const ptoTypesRaw = Array.isArray(raw.ptoTypes) ? raw.ptoTypes : [];
  if (ptoTypesRaw.length > 0) {
    workspace.ptoTypes = ptoTypesRaw
      .filter(isRecord)
      .map((item) => ({
        code: asString(item.code || item.type || item.name),
        label: asString(item.label) || asString(item.name) || asString(item.code) || "PTO",
        active: Boolean(item.active ?? true),
      }))
      .filter((item) => Boolean(item.code) && Boolean(item.label));
  }

  const ptoActionsRaw = Array.isArray(raw.ptoActions) ? raw.ptoActions : [];
  if (ptoActionsRaw.length > 0) {
    workspace.ptoActions = ptoActionsRaw
      .filter(isRecord)
      .map((item) => ({
        code: asString(item.code).toUpperCase(),
        label: asString(item.label) || asString(item.name) || asString(item.code) || "PTO Action",
        detail: asString(item.detail) || asString(item.color) || "",
      }))
      .filter((item) => Boolean(item.code) && Boolean(item.label));
  }

  const requestsRaw = Array.isArray(raw.requests) ? raw.requests : [];
  workspace.requests = requestsRaw
    .filter(isRecord)
    .map((item) => ({
      id: asString(item.id),
      employeeId: asString(item.employeeId),
      employeeName: asString(item.employeeName),
      managerId: asString(item.managerId),
      type: asString(item.type) || "vacation",
      startDate: asString(item.startDate),
      endDate: asString(item.endDate),
      hours: asNumber(item.hours),
      reason: asString(item.reason),
      status: (asString(item.status) as LeaveSpherePtoStatus) || "pending",
      submittedAt: asString(item.submittedAt),
      reviewedAt: asString(item.reviewedAt) || null,
      reviewerName: asString(item.reviewerName) || null,
      managerNote: asString(item.managerNote) || null,
    }))
    .filter((item) => item.id && item.type);

  const employeeBalancesRaw = Array.isArray(raw.employeeBalances) ? raw.employeeBalances : [];
  if (employeeBalancesRaw.length > 0) {
    const networkEmployeeBalances: LeaveSphereAdminEmployeeBalance[] = employeeBalancesRaw
      .filter(isRecord)
      .map((row) => ({
        employeeId: asString(row.employeeId),
        employeeName: asString(row.employeeName),
        balances: (Array.isArray(row.balances) ? row.balances : [])
          .filter(isRecord)
          .map((item) => ({
            type: asString(item.type) || asString(item.code) || "vacation",
            code: asString(item.code) || undefined,
            label: asString(item.label) || asString(item.code) || asString(item.type) || "PTO",
            totalHours: asNumber(item.totalHours),
            usedHours: asNumber(item.usedHours),
            scheduledHours: asNumber(item.scheduledHours),
            remainingHours: item.remainingHours === undefined ? undefined : asNumber(item.remainingHours),
          })),
      }))
      .filter((row) => row.employeeId && row.employeeName);

    if (networkEmployeeBalances.length > 0) {
      workspace.employeeBalanceUsage = buildLeaveSphereAdminBalanceUsageRows(networkEmployeeBalances);
      workspace.balanceTransactions = seedLeaveSphereAdminBalanceTransactions({
        employeeBalances: networkEmployeeBalances,
        years: [new Date().getFullYear()],
        createdAt: toIsoDate(new Date()),
        createdByName: currentUserName,
      });
    }
  }

  const balanceTransactionsRaw = Array.isArray(raw.balanceTransactions)
    ? raw.balanceTransactions
    : Array.isArray(raw.ptoTransactions)
      ? raw.ptoTransactions
      : [];
  if (balanceTransactionsRaw.length > 0) {
    workspace.balanceTransactions = balanceTransactionsRaw
      .filter(isRecord)
      .map((item) => ({
        id: asString(item.id) || `txn-${Math.random().toString(36).slice(2, 8)}`,
        employeeId: asString(item.employeeId),
        ptoTypeCode: asString(item.ptoTypeCode) || "vacation",
        ptoActionCode: (asString(item.ptoActionCode) as LeaveSphereAdminPtoActionCode) || "load_grant",
        hours: asNumber(item.hours),
        year: Math.trunc(asNumber(item.year)) || new Date().getFullYear(),
        status: (asString(item.status) as LeaveSphereAdminPtoTransactionStatus) || "Approved",
        note: asString(item.note) || null,
        createdAt: asString(item.createdAt) || toIsoDate(new Date()),
        createdByName: asString(item.createdByName) || null,
      }))
      .filter((item) => item.employeeId && item.ptoTypeCode);
  }

  workspace.employeeBalances = deriveWorkspaceEmployeeBalances({
    employeeBalanceUsage: workspace.employeeBalanceUsage,
    balanceTransactions: workspace.balanceTransactions,
    year: resolveWorkspaceBalanceYear(workspace),
  });

  return workspace;
}

function findEmployee(workspace: LeaveSphereAdminWorkspaceData, employeeId: string): LeaveSphereAdminEmployee | null {
  return workspace.employees.find((item) => item.employeeId === employeeId) || null;
}

function findEmployeeBalanceUsage(workspace: LeaveSphereAdminWorkspaceData, employeeId: string): LeaveSphereAdminEmployeeBalanceUsageRow | null {
  return workspace.employeeBalanceUsage.find((item) => item.employeeId === employeeId) || null;
}

function applyLeaveSphereAdminBalanceTransaction(
  workspace: LeaveSphereAdminWorkspaceData,
  payload: LeaveSphereAdminAdjustBalanceInput,
  currentUserName: string,
): void {
  const nextHours = asNumber(payload.hours);
  const transactionId = asString(payload.transactionId);
  const nextTransaction = {
    id: transactionId || `pto-txn-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
    employeeId: payload.employeeId,
    ptoTypeCode: payload.ptoTypeCode,
    ptoActionCode: payload.ptoActionCode,
    hours: nextHours,
    year: payload.year,
    status: payload.status,
    note: asString(payload.note) || null,
    createdAt: toIsoDate(new Date()),
    createdByName: currentUserName,
  };

  if (transactionId) {
    let replaced = false;
    workspace.balanceTransactions = workspace.balanceTransactions.map((item) => {
      if (
        !replaced
        && item.id === transactionId
        && item.employeeId === payload.employeeId
        && item.ptoTypeCode === payload.ptoTypeCode
        && item.year === payload.year
        && item.status === "Approved"
        && item.ptoActionCode === "load_grant"
      ) {
        replaced = true;
        return {
          ...item,
          hours: nextHours,
          note: asString(payload.note) || null,
          status: payload.status,
          createdByName: currentUserName,
        };
      }
      return item;
    });
    if (!replaced) {
      workspace.balanceTransactions = [nextTransaction, ...workspace.balanceTransactions];
    }
  } else {
    workspace.balanceTransactions = [nextTransaction, ...workspace.balanceTransactions];
  }

  workspace.employeeBalances = deriveWorkspaceEmployeeBalances({
    employeeBalanceUsage: workspace.employeeBalanceUsage,
    balanceTransactions: workspace.balanceTransactions,
    year: payload.year,
  });
}

export async function loadLeaveSphereAdminPtoWorkspace(params: LoadArgs): Promise<LeaveSphereAdminLoadResult> {
  const { requestJson, workspaceKey, currentUserId, currentUserName, freshData } = params;

  if (resolveUseApi()) {
    try {
      const payload = await requestJson("/api/leavesphere/v1/admin/pto/workspace", {
        method: "GET",
        successToast: false,
        errorToast: false,
      });
      const normalized = normalizeNetworkWorkspace(payload);
      if (normalized) {
        return {
          workspace: writeWorkspace(workspaceKey, normalized),
          source: "network",
          refreshMessage: null,
        };
      }
    } catch {
      // Fallback to local placeholder data.
    }
  }

  if (freshData) {
    await wait(MOCK_DELAY_MS);
  }

  return {
    workspace: ensureWorkspace({
      workspaceKey,
      currentUserId,
      currentUserName,
      reset: Boolean(freshData),
    }),
    source: "mock",
    refreshMessage: resolveUseApi()
      ? "Using local placeholder Leave Management data because LeaveSphere admin endpoints are not available yet."
      : "Using local placeholder Leave Management data. Set VITE_LEAVESPHERE_USE_API=true when backend endpoints are ready.",
  };
}

export async function createLeaveSphereAdminPtoRequest(params: CreateRequestArgs): Promise<LeaveSphereAdminMutationResult> {
  const { requestJson, workspaceKey, currentUserId, currentUserName, payload } = params;

  if (resolveUseApi()) {
    try {
      const response = await requestJson("/api/leavesphere/v1/admin/pto/requests", {
        method: "POST",
        body: payload,
        successToast: false,
        errorToast: false,
      });
      const normalized = normalizeNetworkWorkspace(response);
      if (normalized) {
        return {
          workspace: writeWorkspace(workspaceKey, normalized),
          source: "network",
          createdRequestId: normalized.requests[0]?.id ?? null,
        };
      }
    } catch {
      // Fallback to local placeholder data.
    }
  }

  await wait(MOCK_DELAY_MS);
  const workspace = ensureWorkspace({ workspaceKey, currentUserId, currentUserName });
  const employee = findEmployee(workspace, payload.employeeId);
  if (!employee) {
    return {
      workspace: writeWorkspace(workspaceKey, workspace),
      source: "mock",
    };
  }

  const type = PTO_TYPES.includes(payload.type) ? payload.type : "vacation";
  const startDate = asString(payload.startDate);
  const endDate = asString(payload.endDate);
  const hours = Math.max(1, asNumber(payload.hours));

  const request: LeaveSpherePtoRequest = {
    id: `admin-pto-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
    employeeId: employee.employeeId,
    employeeName: employee.employeeName,
    managerId: employee.managerId,
    type,
    startDate,
    endDate,
    hours,
    reason: asString(payload.reason) || "Admin-created PTO request",
    status: "pending",
    submittedAt: toIsoDate(new Date()),
    reviewedAt: null,
    reviewerName: null,
    managerNote: `Created by ${currentUserName}`,
  };

  workspace.requests = [request, ...workspace.requests];
  const balance = findEmployeeBalanceUsage(workspace, employee.employeeId);
  const typeBalance = balance?.balances.find((item) => item.type === type);
  if (typeBalance) {
    typeBalance.scheduledHours += hours;
  }
  workspace.employeeBalances = deriveWorkspaceEmployeeBalances({
    employeeBalanceUsage: workspace.employeeBalanceUsage,
    balanceTransactions: workspace.balanceTransactions,
    year: resolveWorkspaceBalanceYear(workspace),
  });

  return {
    workspace: writeWorkspace(workspaceKey, workspace),
    source: "mock",
    createdRequestId: request.id,
  };
}

export async function updateLeaveSphereAdminPtoRequest(params: UpdateRequestArgs): Promise<LeaveSphereAdminMutationResult> {
  const { requestJson, workspaceKey, currentUserId, currentUserName, payload } = params;

  if (resolveUseApi()) {
    try {
      const response = await requestJson("/api/leavesphere/v1/admin/pto/requests", {
        method: "PUT",
        body: payload,
        successToast: false,
        errorToast: false,
      });
      const normalized = normalizeNetworkWorkspace(response);
      if (normalized) {
        return {
          workspace: writeWorkspace(workspaceKey, normalized),
          source: "network",
        };
      }
    } catch {
      // Fallback to local placeholder data.
    }
  }

  await wait(MOCK_DELAY_MS);
  const workspace = ensureWorkspace({ workspaceKey, currentUserId, currentUserName });
  const target = workspace.requests.find((item) => item.id === payload.transactionId);
  if (!target) {
    return {
      workspace: writeWorkspace(workspaceKey, workspace),
      source: "mock",
    };
  }

  target.type = payload.type;
  target.startDate = asString(payload.startDate);
  target.endDate = asString(payload.endDate);
  target.hours = Math.max(1, asNumber(payload.hours));
  target.reason = asString(payload.reason) || target.reason;
  return {
    workspace: writeWorkspace(workspaceKey, workspace),
    source: "mock",
  };
}

export async function reviewLeaveSphereAdminPtoRequest(params: ReviewRequestArgs): Promise<LeaveSphereAdminMutationResult> {
  const { requestJson, workspaceKey, currentUserId, currentUserName, payload } = params;

  if (resolveUseApi()) {
    try {
      const response = await requestJson("/api/leavesphere/v1/admin/pto/review", {
        method: "POST",
        body: {
          requestId: payload.requestId,
          action: payload.approve ? "approve" : "reject",
          note: payload.note,
        },
        successToast: false,
        errorToast: false,
      });
      const normalized = normalizeNetworkWorkspace(response);
      if (normalized) {
        return {
          workspace: writeWorkspace(workspaceKey, normalized),
          source: "network",
        };
      }
    } catch {
      // Fallback to local placeholder data.
    }
  }

  await wait(MOCK_DELAY_MS);
  const workspace = ensureWorkspace({ workspaceKey, currentUserId, currentUserName });
  const target = workspace.requests.find((item) => item.id === payload.requestId);
  if (!target) {
    return {
      workspace: writeWorkspace(workspaceKey, workspace),
      source: "mock",
    };
  }

  const nextStatus: LeaveSpherePtoStatus = payload.approve ? "approved" : "rejected";
  target.status = nextStatus;
  target.reviewedAt = toIsoDate(new Date());
  target.reviewerName = currentUserName;
  target.managerNote = asString(payload.note) || null;

  const balance = findEmployeeBalanceUsage(workspace, target.employeeId);
  const typeBalance = balance?.balances.find((item) => item.type === target.type);
  if (typeBalance) {
    typeBalance.scheduledHours = Math.max(0, typeBalance.scheduledHours - target.hours);
    if (payload.approve) {
      typeBalance.usedHours += target.hours;
    }
  }
  workspace.employeeBalances = deriveWorkspaceEmployeeBalances({
    employeeBalanceUsage: workspace.employeeBalanceUsage,
    balanceTransactions: workspace.balanceTransactions,
    year: resolveWorkspaceBalanceYear(workspace),
  });

  return {
    workspace: writeWorkspace(workspaceKey, workspace),
    source: "mock",
  };
}

export async function adjustLeaveSphereAdminPtoBalance(params: AdjustBalanceArgs): Promise<LeaveSphereAdminMutationResult> {
  const { requestJson, workspaceKey, currentUserId, currentUserName, payload } = params;
  const workspace = ensureWorkspace({ workspaceKey, currentUserId, currentUserName });

  if (resolveUseApi()) {
    try {
      const response = await requestJson("/api/leavesphere/v1/admin/pto/balances/adjust", {
        method: "POST",
        body: payload,
        successToast: false,
        errorToast: false,
      });
      const responseRaw = unwrapEnvelope(response);
      const hasExplicitBalanceTransactions = isRecord(responseRaw) && (
        Array.isArray(responseRaw.balanceTransactions)
        || Array.isArray(responseRaw.ptoTransactions)
      );
      const normalized = normalizeNetworkWorkspace(response);
      if (normalized) {
        if (!hasExplicitBalanceTransactions) {
          const mergedWorkspace = cloneWorkspace(workspace);
          applyLeaveSphereAdminBalanceTransaction(mergedWorkspace, payload, currentUserName);
          return {
            workspace: writeWorkspace(workspaceKey, mergedWorkspace),
            source: "network",
          };
        }
        return {
          workspace: writeWorkspace(workspaceKey, normalized),
          source: "network",
        };
      }
    } catch {
      // Fallback to local placeholder data.
    }
  }

  await wait(MOCK_DELAY_MS);
  const employee = findEmployee(workspace, payload.employeeId);
  if (!employee) {
    return {
      workspace: writeWorkspace(workspaceKey, workspace),
      source: "mock",
    };
  }

  applyLeaveSphereAdminBalanceTransaction(workspace, payload, currentUserName);

  return {
    workspace: writeWorkspace(workspaceKey, workspace),
    source: "mock",
  };
}

export async function updateLeaveSphereAdminSetupData(params: SetupArgs): Promise<LeaveSphereAdminMutationResult> {
  const { requestJson, workspaceKey, currentUserId, currentUserName, payload } = params;

  if (resolveUseApi()) {
    try {
      const response = await requestJson("/api/leavesphere/v1/admin/pto/setup", {
        method: "POST",
        body: payload,
        successToast: false,
        errorToast: false,
      });
      const normalized = normalizeNetworkWorkspace(response);
      if (normalized) {
        return {
          workspace: writeWorkspace(workspaceKey, normalized),
          source: "network",
        };
      }
    } catch {
      // Fallback to local placeholder data.
    }
  }

  await wait(MOCK_DELAY_MS);
  const workspace = ensureWorkspace({ workspaceKey, currentUserId, currentUserName });

  if (payload.kind === "pto_type") {
    const existing = workspace.ptoTypes.find((item) => item.code === payload.code);
    if (existing) {
      existing.label = asString(payload.label) || existing.label;
      existing.active = Boolean(payload.active);
    } else {
      workspace.ptoTypes.push({
        code: payload.code,
        label: asString(payload.label) || PTO_TYPE_LABELS[payload.code],
        active: Boolean(payload.active),
      });
    }
  } else if (payload.kind === "pto_action") {
    const code = asString(payload.code) || `action-${Date.now().toString(36)}`;
    const existing = workspace.ptoActions.find((item) => item.code === code);
    if (existing) {
      existing.label = asString(payload.label) || existing.label;
      existing.detail = asString(payload.detail) || existing.detail;
    } else {
      workspace.ptoActions.push({
        code,
        label: asString(payload.label) || "PTO Action",
        detail: asString(payload.detail) || "",
      });
    }
  } else if (payload.kind === "employee") {
    const manager = workspace.employees.find((item) => item.employeeId === payload.managerId) || null;
    const employeeId = `emp-${Date.now().toString(36)}`;
    const employeeName = asString(payload.employeeName) || "New Employee";
    workspace.employees.push({
      employeeId,
      employeeName,
      title: asString(payload.title) || "Team Member",
      managerId: manager?.employeeId || workspace.currentUserId,
      managerName: manager?.employeeName || workspace.currentUserName,
      teamRegion: normalizeLeaveSphereTeamRegion(payload.teamRegion),
      active: true,
    });
    const defaultEmployeeBalance: LeaveSphereAdminEmployeeBalance = {
      employeeId,
      employeeName,
      balances: PTO_TYPES.map((type) => ({
        type,
        label: PTO_TYPE_LABELS[type],
        totalHours: type === "vacation" ? 120 : type === "sick" ? 64 : type === "personal" ? 40 : 16,
        usedHours: 0,
        scheduledHours: 0,
      })),
    };
    workspace.employeeBalanceUsage.push(...buildLeaveSphereAdminBalanceUsageRows([defaultEmployeeBalance]));
    workspace.balanceTransactions = [
      ...seedLeaveSphereAdminBalanceTransactions({
        employeeBalances: [defaultEmployeeBalance],
        years: [new Date().getFullYear()],
        createdAt: toIsoDate(new Date()),
        createdByName: currentUserName,
      }),
      ...workspace.balanceTransactions,
    ];
  } else if (payload.kind === "employee_manager") {
    const employee = workspace.employees.find((item) => item.employeeId === payload.employeeId);
    const manager = workspace.employees.find((item) => item.employeeId === payload.managerId);
    if (employee && manager) {
      employee.managerId = manager.employeeId;
      employee.managerName = manager.employeeName;
      workspace.requests = workspace.requests.map((item) => (
        item.employeeId === employee.employeeId
          ? { ...item, managerId: manager.employeeId }
          : item
      ));
    }
  } else if (payload.kind === "holiday") {
    const date = asString(payload.date);
    const teamRegion = normalizeLeaveSphereTeamRegion(payload.teamRegion);
    if (date) {
      const key = `holiday-${teamRegion.toLowerCase()}-${date}`;
      const existing = workspace.holidays.find((item) => item.id === key);
      if (existing) {
        existing.name = asString(payload.name) || existing.name;
      } else {
        workspace.holidays.push({
          id: key,
          name: asString(payload.name) || "Company Holiday",
          date,
          teamRegion,
        });
      }
      workspace.holidays.sort((left, right) => left.date.localeCompare(right.date));
    }
  }

  workspace.employeeBalances = deriveWorkspaceEmployeeBalances({
    employeeBalanceUsage: workspace.employeeBalanceUsage,
    balanceTransactions: workspace.balanceTransactions,
    year: resolveWorkspaceBalanceYear(workspace),
  });

  return {
    workspace: writeWorkspace(workspaceKey, workspace),
    source: "mock",
  };
}
