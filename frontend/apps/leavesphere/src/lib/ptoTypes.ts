export type LeaveSpherePtoStatus = "pending" | "approved" | "rejected" | "cancelled";
export type LeaveSpherePtoType = string;
export type LeaveSphereTeamRegion = "US" | "Mexico" | "Philippines";

export const LEAVESPHERE_TEAM_REGION_OPTIONS: Array<{ value: LeaveSphereTeamRegion; label: string }> = [
  { value: "US", label: "U.S." },
  { value: "Mexico", label: "Mexico" },
  { value: "Philippines", label: "Philippines" },
];

export type LeaveSpherePtoBalance = {
  type: string;
  code?: string;
  label: string;
  totalHours: number;
  usedHours: number;
  scheduledHours: number;
  remainingHours?: number;
};

export type LeaveSpherePtoTypeConfig = {
  code: string;
  type: LeaveSpherePtoType;
  label: string;
  active: boolean;
  listingOrder: number;
  rolloverable?: boolean;
  payoutable?: boolean;
  usaDefaultHour?: number;
  phlDefaultHour?: number;
};

export type LeaveSpherePtoActionConfig = {
  code: string;
  name: string;
  color?: string | null;
};

export type LeaveSpherePtoEmployee = {
  employeeId: string;
  employeeName: string;
  pictureUrl?: string | null;
  title?: string | null;
};

export type LeaveSpherePtoRequest = {
  id: string;
  employeeId: string;
  managerId: string | null;
  year?: number;
  type: string;
  ptoTypeCode?: string;
  startDate: string;
  endDate: string;
  hours: number;
  description: string;
  approverNote: string | null;
  status: LeaveSpherePtoStatus;
  submittedAt: string;
  reviewedAt: string | null;
  reviewerName: string | null;
};

export type LeaveSphereHoliday = {
  id: string;
  name: string;
  date: string;
  teamRegion: LeaveSphereTeamRegion;
};

export type LeaveSphereDirectReport = {
  employeeId: string;
  employeeName: string;
  pictureUrl?: string | null;
  title: string;
};

export type LeaveSpherePtoWorkspaceData = {
  currentUserId: string;
  currentUserName: string;
  currentUserEmail: string;
  managerId: string | null;
  currentUserTeamRegion: LeaveSphereTeamRegion;
  isManager: boolean;
  employees: LeaveSpherePtoEmployee[];
  ptoTypes: LeaveSpherePtoTypeConfig[];
  ptoActions: LeaveSpherePtoActionConfig[];
  defaultRequestActionCode?: string;
  defaultCancelActionCode?: string;
  balances: LeaveSpherePtoBalance[];
  requests: LeaveSpherePtoRequest[];
  holidays: LeaveSphereHoliday[];
  directReports: LeaveSphereDirectReport[];
};

export type LeaveSpherePtoLoadResult = {
  workspace: LeaveSpherePtoWorkspaceData;
  source: "network";
  refreshMessage: null;
};

export type LeaveSpherePtoSubmitInput = {
  type: LeaveSpherePtoType;
  startDate: string;
  endDate: string;
  hours: number;
  description: string;
  year: number;
};

export type LeaveSpherePtoUpdateInput = LeaveSpherePtoSubmitInput & {
  transactionId: string;
};

export type LeaveSpherePtoReviewAction = "approve" | "reject" | "cancel" | "revert";

export type LeaveSpherePtoReviewInput = {
  requestId: string;
  action: LeaveSpherePtoReviewAction;
  approverNote: string;
};

export type LeaveSpherePtoMutationResult = {
  workspace: LeaveSpherePtoWorkspaceData;
  source: "network";
  createdRequestId?: string | null;
  updated?: number;
  status?: LeaveSpherePtoStatus | "Pending" | "Approved" | "Rejected" | "Canceled";
};

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

export function normalizeLeaveSphereTeamRegion(value: unknown, fallback: LeaveSphereTeamRegion = "US"): LeaveSphereTeamRegion {
  const normalized = asString(value).toLowerCase();
  if (normalized === "mexico" || normalized === "mx") {
    return "Mexico";
  }
  if (
    normalized === "philippines"
    || normalized === "ph"
    || normalized === "phl"
    || normalized === "philippine"
    || normalized === "philipine"
  ) {
    return "Philippines";
  }
  if (normalized === "us" || normalized === "u.s." || normalized === "usa" || normalized === "united states") {
    return "US";
  }
  return fallback;
}
