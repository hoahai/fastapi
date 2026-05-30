import type { ApiRequestOptions } from "@shared/hooks/useApiRequest";

export type LeaveSpherePtoStatus = "pending" | "approved" | "rejected" | "cancelled";
export type LeaveSpherePtoType = "vacation" | "sick" | "personal" | "floating";

export type LeaveSpherePtoBalance = {
  type: LeaveSpherePtoType;
  label: string;
  totalHours: number;
  usedHours: number;
  scheduledHours: number;
};

export type LeaveSpherePtoRequest = {
  id: string;
  employeeId: string;
  employeeName: string;
  managerId: string;
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: number;
  reason: string;
  status: LeaveSpherePtoStatus;
  submittedAt: string;
  reviewedAt: string | null;
  reviewerName: string | null;
  managerNote: string | null;
};

export type LeaveSphereHoliday = {
  id: string;
  name: string;
  date: string;
};

export type LeaveSphereDirectReport = {
  employeeId: string;
  employeeName: string;
  title: string;
};

export type LeaveSpherePtoWorkspaceData = {
  currentUserId: string;
  currentUserName: string;
  managerId: string;
  isManager: boolean;
  balances: LeaveSpherePtoBalance[];
  requests: LeaveSpherePtoRequest[];
  holidays: LeaveSphereHoliday[];
  directReports: LeaveSphereDirectReport[];
};

export type LeaveSpherePtoLoadResult = {
  workspace: LeaveSpherePtoWorkspaceData;
  source: "mock" | "network";
  refreshMessage: string | null;
};

export type LeaveSpherePtoSubmitInput = {
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: number;
  reason: string;
};

export type LeaveSpherePtoReviewInput = {
  requestId: string;
  approve: boolean;
  note: string;
};

export type LeaveSpherePtoMutationResult = {
  workspace: LeaveSpherePtoWorkspaceData;
  source: "mock" | "network";
};

type RequestJson = (url: string, options?: ApiRequestOptions) => Promise<unknown>;

type WorkspaceArgs = {
  requestJson: RequestJson;
  workspaceKey: string;
  currentUserId: string;
  currentUserName: string;
  isManager: boolean;
  freshData?: boolean;
};

type SubmitArgs = {
  requestJson: RequestJson;
  workspaceKey: string;
  currentUserId: string;
  currentUserName: string;
  managerId: string;
  isManager: boolean;
  payload: LeaveSpherePtoSubmitInput;
};

type ReviewArgs = {
  requestJson: RequestJson;
  workspaceKey: string;
  currentUserId: string;
  currentUserName: string;
  isManager: boolean;
  payload: LeaveSpherePtoReviewInput;
};

const USE_API_FLAG = "VITE_LEAVESPHERE_USE_API";
const MOCK_DELAY_MS = 160;
const PTO_TYPES: LeaveSpherePtoType[] = ["vacation", "sick", "personal", "floating"];
const PTO_LABELS: Record<LeaveSpherePtoType, string> = {
  vacation: "Vacation",
  sick: "Sick",
  personal: "Personal",
  floating: "Floating Holiday",
};

const WORKSPACE_STORE = new Map<string, LeaveSpherePtoWorkspaceData>();

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function toIsoDate(value: Date): string {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addDays(base: Date, days: number): Date {
  const next = new Date(base);
  next.setDate(next.getDate() + days);
  return next;
}

function hoursForSpan(startIso: string, endIso: string): number {
  const start = new Date(`${startIso}T00:00:00`);
  const end = new Date(`${endIso}T00:00:00`);
  const days = Math.max(1, Math.round((end.getTime() - start.getTime()) / (24 * 60 * 60 * 1000)) + 1);
  return days * 8;
}

function cloneWorkspace(workspace: LeaveSpherePtoWorkspaceData): LeaveSpherePtoWorkspaceData {
  return {
    ...workspace,
    balances: workspace.balances.map((item) => ({ ...item })),
    requests: workspace.requests.map((item) => ({ ...item })),
    holidays: workspace.holidays.map((item) => ({ ...item })),
    directReports: workspace.directReports.map((item) => ({ ...item })),
  };
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
  return raw.toLowerCase() === "true" || raw === "1";
}

function baseBalances(): LeaveSpherePtoBalance[] {
  return [
    { type: "vacation", label: PTO_LABELS.vacation, totalHours: 120, usedHours: 40, scheduledHours: 16 },
    { type: "sick", label: PTO_LABELS.sick, totalHours: 64, usedHours: 8, scheduledHours: 0 },
    { type: "personal", label: PTO_LABELS.personal, totalHours: 40, usedHours: 8, scheduledHours: 8 },
    { type: "floating", label: PTO_LABELS.floating, totalHours: 16, usedHours: 8, scheduledHours: 0 },
  ];
}

function baseHolidays(referenceDate: Date): LeaveSphereHoliday[] {
  const year = referenceDate.getFullYear();
  return [
    { id: `${year}-new-year`, name: "New Year's Day", date: `${year}-01-01` },
    { id: `${year}-memorial-day`, name: "Memorial Day", date: `${year}-05-25` },
    { id: `${year}-independence-day`, name: "Independence Day", date: `${year}-07-04` },
    { id: `${year}-labor-day`, name: "Labor Day", date: `${year}-09-07` },
    { id: `${year}-thanksgiving`, name: "Thanksgiving", date: `${year}-11-26` },
    { id: `${year}-christmas`, name: "Christmas Day", date: `${year}-12-25` },
  ];
}

function buildSeedRequests(params: {
  now: Date;
  currentUserId: string;
  currentUserName: string;
  managerId: string;
  isManager: boolean;
}): LeaveSpherePtoRequest[] {
  const { now, currentUserId, currentUserName, managerId, isManager } = params;
  const ownUpcomingStart = toIsoDate(addDays(now, 16));
  const ownUpcomingEnd = toIsoDate(addDays(now, 17));
  const ownPastStart = toIsoDate(addDays(now, -40));
  const ownPastEnd = toIsoDate(addDays(now, -39));

  const requests: LeaveSpherePtoRequest[] = [
    {
      id: "pto-self-approved",
      employeeId: currentUserId,
      employeeName: currentUserName,
      managerId,
      type: "vacation",
      startDate: ownPastStart,
      endDate: ownPastEnd,
      hours: hoursForSpan(ownPastStart, ownPastEnd),
      reason: "Family travel",
      status: "approved",
      submittedAt: toIsoDate(addDays(now, -55)),
      reviewedAt: toIsoDate(addDays(now, -50)),
      reviewerName: "Alex Morgan",
      managerNote: "Approved. Coverage already scheduled.",
    },
    {
      id: "pto-self-pending",
      employeeId: currentUserId,
      employeeName: currentUserName,
      managerId,
      type: "personal",
      startDate: ownUpcomingStart,
      endDate: ownUpcomingEnd,
      hours: hoursForSpan(ownUpcomingStart, ownUpcomingEnd),
      reason: "Personal appointments",
      status: "pending",
      submittedAt: toIsoDate(addDays(now, -2)),
      reviewedAt: null,
      reviewerName: null,
      managerNote: null,
    },
  ];

  if (!isManager) {
    return requests;
  }

  const reportAStart = toIsoDate(addDays(now, 8));
  const reportAEnd = toIsoDate(addDays(now, 10));
  const reportBStart = toIsoDate(addDays(now, 3));
  const reportBEnd = toIsoDate(addDays(now, 3));

  return [
    ...requests,
    {
      id: "pto-report-pending-1",
      employeeId: "emp-lee-chen",
      employeeName: "Lee Chen",
      managerId: currentUserId,
      type: "vacation",
      startDate: reportAStart,
      endDate: reportAEnd,
      hours: hoursForSpan(reportAStart, reportAEnd),
      reason: "Family wedding out of state",
      status: "pending",
      submittedAt: toIsoDate(addDays(now, -1)),
      reviewedAt: null,
      reviewerName: null,
      managerNote: null,
    },
    {
      id: "pto-report-pending-2",
      employeeId: "emp-sara-johnson",
      employeeName: "Sara Johnson",
      managerId: currentUserId,
      type: "sick",
      startDate: reportBStart,
      endDate: reportBEnd,
      hours: hoursForSpan(reportBStart, reportBEnd),
      reason: "Medical procedure recovery",
      status: "pending",
      submittedAt: toIsoDate(addDays(now, -3)),
      reviewedAt: null,
      reviewerName: null,
      managerNote: null,
    },
    {
      id: "pto-report-approved",
      employeeId: "emp-mateo-garcia",
      employeeName: "Mateo Garcia",
      managerId: currentUserId,
      type: "floating",
      startDate: toIsoDate(addDays(now, -12)),
      endDate: toIsoDate(addDays(now, -12)),
      hours: 8,
      reason: "Floating holiday",
      status: "approved",
      submittedAt: toIsoDate(addDays(now, -16)),
      reviewedAt: toIsoDate(addDays(now, -15)),
      reviewerName: currentUserName,
      managerNote: "Approved.",
    },
  ];
}

function seedWorkspace(params: {
  currentUserId: string;
  currentUserName: string;
  isManager: boolean;
}): LeaveSpherePtoWorkspaceData {
  const now = new Date();
  const managerId = params.isManager ? params.currentUserId : "mgr-alex-morgan";

  return {
    currentUserId: params.currentUserId,
    currentUserName: params.currentUserName,
    managerId,
    isManager: params.isManager,
    balances: baseBalances(),
    requests: buildSeedRequests({
      now,
      currentUserId: params.currentUserId,
      currentUserName: params.currentUserName,
      managerId,
      isManager: params.isManager,
    }),
    holidays: baseHolidays(now),
    directReports: params.isManager
      ? [
          { employeeId: "emp-lee-chen", employeeName: "Lee Chen", title: "Senior Designer" },
          { employeeId: "emp-sara-johnson", employeeName: "Sara Johnson", title: "Performance Analyst" },
          { employeeId: "emp-mateo-garcia", employeeName: "Mateo Garcia", title: "Paid Media Specialist" },
        ]
      : [],
  };
}

function ensureWorkspace(params: {
  workspaceKey: string;
  currentUserId: string;
  currentUserName: string;
  isManager: boolean;
  reset?: boolean;
}): LeaveSpherePtoWorkspaceData {
  if (!params.reset && WORKSPACE_STORE.has(params.workspaceKey)) {
    return cloneWorkspace(WORKSPACE_STORE.get(params.workspaceKey)!);
  }
  const seeded = seedWorkspace({
    currentUserId: params.currentUserId,
    currentUserName: params.currentUserName,
    isManager: params.isManager,
  });
  WORKSPACE_STORE.set(params.workspaceKey, seeded);
  return cloneWorkspace(seeded);
}

function writeWorkspace(workspaceKey: string, workspace: LeaveSpherePtoWorkspaceData): LeaveSpherePtoWorkspaceData {
  WORKSPACE_STORE.set(workspaceKey, cloneWorkspace(workspace));
  return cloneWorkspace(workspace);
}

function normalizeNetworkWorkspace(payload: unknown): LeaveSpherePtoWorkspaceData | null {
  const raw = unwrapEnvelope(payload);
  if (!isRecord(raw)) {
    return null;
  }

  const currentUserId = asString(raw.currentUserId);
  const currentUserName = asString(raw.currentUserName);
  const managerId = asString(raw.managerId);
  const isManager = Boolean(raw.isManager);
  const balancesRaw = Array.isArray(raw.balances) ? raw.balances : [];
  const requestsRaw = Array.isArray(raw.requests) ? raw.requests : [];
  const holidaysRaw = Array.isArray(raw.holidays) ? raw.holidays : [];
  const directReportsRaw = Array.isArray(raw.directReports) ? raw.directReports : [];

  const balances = balancesRaw
    .filter(isRecord)
    .map((item) => {
      const type = asString(item.type) as LeaveSpherePtoType;
      return {
        type,
        label: asString(item.label) || PTO_LABELS[type] || "PTO",
        totalHours: Number(item.totalHours || 0),
        usedHours: Number(item.usedHours || 0),
        scheduledHours: Number(item.scheduledHours || 0),
      } satisfies LeaveSpherePtoBalance;
    })
    .filter((item) => PTO_TYPES.includes(item.type));

  const requests = requestsRaw
    .filter(isRecord)
    .map((item) => ({
      id: asString(item.id),
      employeeId: asString(item.employeeId),
      employeeName: asString(item.employeeName),
      managerId: asString(item.managerId),
      type: asString(item.type) as LeaveSpherePtoType,
      startDate: asString(item.startDate),
      endDate: asString(item.endDate),
      hours: Number(item.hours || 0),
      reason: asString(item.reason),
      status: asString(item.status) as LeaveSpherePtoStatus,
      submittedAt: asString(item.submittedAt),
      reviewedAt: asString(item.reviewedAt) || null,
      reviewerName: asString(item.reviewerName) || null,
      managerNote: asString(item.managerNote) || null,
    }))
    .filter((item) => item.id && PTO_TYPES.includes(item.type));

  const holidays = holidaysRaw
    .filter(isRecord)
    .map((item) => ({
      id: asString(item.id),
      name: asString(item.name),
      date: asString(item.date),
    }))
    .filter((item) => item.id && item.date);

  const directReports = directReportsRaw
    .filter(isRecord)
    .map((item) => ({
      employeeId: asString(item.employeeId),
      employeeName: asString(item.employeeName),
      title: asString(item.title),
    }))
    .filter((item) => item.employeeId && item.employeeName);

  if (!currentUserId || !currentUserName || !managerId) {
    return null;
  }

  return {
    currentUserId,
    currentUserName,
    managerId,
    isManager,
    balances: balances.length > 0 ? balances : baseBalances(),
    requests,
    holidays,
    directReports,
  };
}

export async function loadLeaveSpherePtoWorkspace(params: WorkspaceArgs): Promise<LeaveSpherePtoLoadResult> {
  const { requestJson, workspaceKey, currentUserId, currentUserName, isManager, freshData } = params;

  if (resolveUseApi()) {
    try {
      const payload = await requestJson("/api/leavesphere/v1/pto/workspace", {
        method: "GET",
        successToast: false,
        errorToast: false,
      });
      const networkWorkspace = normalizeNetworkWorkspace(payload);
      if (networkWorkspace) {
        return {
          workspace: writeWorkspace(workspaceKey, networkWorkspace),
          source: "network",
          refreshMessage: null,
        };
      }
    } catch {
      // Fallback to local data when API is unavailable.
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
      isManager,
      reset: Boolean(freshData),
    }),
    source: "mock",
    refreshMessage: resolveUseApi()
      ? "Using local placeholder PTO data because LeaveSphere API is not available yet."
      : "Using local placeholder PTO data. Set VITE_LEAVESPHERE_USE_API=true when backend endpoints are ready.",
  };
}

export async function submitLeaveSpherePtoRequest(params: SubmitArgs): Promise<LeaveSpherePtoMutationResult> {
  const { requestJson, workspaceKey, currentUserId, currentUserName, managerId, isManager, payload } = params;

  if (resolveUseApi()) {
    try {
      const response = await requestJson("/api/leavesphere/v1/pto/requests", {
        method: "POST",
        body: payload,
        successToast: false,
        errorToast: false,
      });
      const networkWorkspace = normalizeNetworkWorkspace(response);
      if (networkWorkspace) {
        return {
          workspace: writeWorkspace(workspaceKey, networkWorkspace),
          source: "network",
        };
      }
    } catch {
      // Fallback to local store.
    }
  }

  await wait(MOCK_DELAY_MS);
  const current = ensureWorkspace({
    workspaceKey,
    currentUserId,
    currentUserName,
    isManager,
  });

  const normalizedType = PTO_TYPES.includes(payload.type) ? payload.type : "vacation";
  const normalizedReason = asString(payload.reason) || "PTO request";
  const startDate = asString(payload.startDate);
  const endDate = asString(payload.endDate);
  const requestedHours = Number.isFinite(payload.hours) && payload.hours > 0
    ? payload.hours
    : hoursForSpan(startDate, endDate);

  const nextRequest: LeaveSpherePtoRequest = {
    id: `pto-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
    employeeId: currentUserId,
    employeeName: currentUserName,
    managerId,
    type: normalizedType,
    startDate,
    endDate,
    hours: requestedHours,
    reason: normalizedReason,
    status: "pending",
    submittedAt: toIsoDate(new Date()),
    reviewedAt: null,
    reviewerName: null,
    managerNote: null,
  };

  const updated = {
    ...current,
    requests: [nextRequest, ...current.requests],
    balances: current.balances.map((item) => {
      if (item.type !== normalizedType) {
        return item;
      }
      return {
        ...item,
        scheduledHours: item.scheduledHours + requestedHours,
      };
    }),
  } satisfies LeaveSpherePtoWorkspaceData;

  return {
    workspace: writeWorkspace(workspaceKey, updated),
    source: "mock",
  };
}

export async function reviewLeaveSpherePtoRequest(params: ReviewArgs): Promise<LeaveSpherePtoMutationResult> {
  const { requestJson, workspaceKey, currentUserId, currentUserName, isManager, payload } = params;

  if (resolveUseApi()) {
    try {
      const response = await requestJson(`/api/leavesphere/v1/pto/requests/${encodeURIComponent(payload.requestId)}/decision`, {
        method: "POST",
        body: {
          action: payload.approve ? "approve" : "reject",
          note: payload.note,
        },
        successToast: false,
        errorToast: false,
      });
      const networkWorkspace = normalizeNetworkWorkspace(response);
      if (networkWorkspace) {
        return {
          workspace: writeWorkspace(workspaceKey, networkWorkspace),
          source: "network",
        };
      }
    } catch {
      // Fallback to local store.
    }
  }

  await wait(MOCK_DELAY_MS);
  const current = ensureWorkspace({
    workspaceKey,
    currentUserId,
    currentUserName,
    isManager,
  });

  const reviewedAt = toIsoDate(new Date());
  const nextStatus: LeaveSpherePtoStatus = payload.approve ? "approved" : "rejected";
  const requestToReview = current.requests.find((item) => item.id === payload.requestId);
  if (!requestToReview) {
    return {
      workspace: writeWorkspace(workspaceKey, current),
      source: "mock",
    };
  }

  const updated = {
    ...current,
    requests: current.requests.map((item) => {
      if (item.id !== payload.requestId) {
        return item;
      }
      return {
        ...item,
        status: nextStatus,
        reviewedAt,
        reviewerName: currentUserName,
        managerNote: asString(payload.note) || null,
      };
    }),
    balances: current.balances.map((item) => {
      if (item.type !== requestToReview.type || requestToReview.employeeId !== current.currentUserId) {
        return item;
      }
      const nextScheduled = Math.max(0, item.scheduledHours - requestToReview.hours);
      return payload.approve
        ? {
            ...item,
            scheduledHours: nextScheduled,
            usedHours: item.usedHours + requestToReview.hours,
          }
        : {
            ...item,
            scheduledHours: nextScheduled,
          };
    }),
  } satisfies LeaveSpherePtoWorkspaceData;

  return {
    workspace: writeWorkspace(workspaceKey, updated),
    source: "mock",
  };
}
