import type { ApiRequestOptions } from "@shared/hooks/useApiRequest";
import { normalizeLeaveSphereTeamRegion, type LeaveSphereTeamRegion } from "@leavesphere/lib/ptoTypes";

export type LeaveSphereEmployeeManagementEmployee = {
  id: string;
  identityKey: string;
  firstName: string;
  lastName: string;
  email: string;
  phone: string | null;
  dob: string | null;
  pictureUrl: string | null;
  region: LeaveSphereTeamRegion;
  startDate: string | null;
  title: string | null;
  isAE: boolean;
  active: boolean;
};

export type LeaveSphereEmployeeManagementSummary = {
  totalEmployees: number;
  activeEmployees: number;
  inactiveEmployees: number;
};

export type LeaveSphereEmployeeManagementCapabilities = {
  canCreate: boolean;
  canUpdate: boolean;
  canActivate: boolean;
  canDeactivate: boolean;
  canArchive: boolean;
};

export type LeaveSphereEmployeeManagementWorkspace = {
  pageCode: string;
  pageTitle: string;
  summary: LeaveSphereEmployeeManagementSummary;
  capabilities: LeaveSphereEmployeeManagementCapabilities;
  employees: LeaveSphereEmployeeManagementEmployee[];
  source?: string | null;
  refreshMessage?: string | null;
};

export type LeaveSphereEmployeeManagementFormState = {
  identityKey: string;
  firstName: string;
  lastName: string;
  email: string;
  phone: string;
  dob: string;
  pictureUrl: string;
  region: LeaveSphereTeamRegion | "";
  startDate: string;
  title: string;
  isAE: boolean;
  active: boolean;
};

export type LeaveSphereEmployeeManagementSubmitPayload = {
  id: string | null;
  form: LeaveSphereEmployeeManagementFormState;
};

const DEFAULT_CAPABILITIES: LeaveSphereEmployeeManagementCapabilities = {
  canCreate: true,
  canUpdate: true,
  canActivate: true,
  canDeactivate: true,
  canArchive: false,
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return "";
}

function asBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return ["1", "true", "yes", "y", "on"].includes(normalized);
  }
  return false;
}

function unwrapEnvelope(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

function normalizeOptionalText(value: unknown): string | null {
  const text = asString(value);
  return text || null;
}

function normalizeDate(value: unknown): string | null {
  const text = asString(value);
  if (!text) {
    return null;
  }
  return text.slice(0, 10);
}

export function normalizeLeaveSphereEmployeeManagementEmployee(
  value: unknown,
): LeaveSphereEmployeeManagementEmployee | null {
  if (!isRecord(value)) {
    return null;
  }

  const id = asString(value.id);
  const identityKey = asString(value.identityKey);
  const firstName = asString(value.firstName);
  const lastName = asString(value.lastName);
  const email = asString(value.email).toLowerCase();
  if (!id || !identityKey || !firstName || !lastName || !email) {
    return null;
  }

  return {
    id,
    identityKey,
    firstName,
    lastName,
    email,
    phone: normalizeOptionalText(value.phone),
    dob: normalizeDate(value.dob),
    pictureUrl: normalizeOptionalText(value.pictureUrl),
    region: normalizeLeaveSphereTeamRegion(value.region, "US"),
    startDate: normalizeDate(value.startDate),
    title: normalizeOptionalText(value.title),
    isAE: asBoolean(value.isAE),
    active: asBoolean(value.active),
  };
}

function computeSummary(employees: LeaveSphereEmployeeManagementEmployee[]): LeaveSphereEmployeeManagementSummary {
  const activeEmployees = employees.filter((employee) => employee.active).length;
  return {
    totalEmployees: employees.length,
    activeEmployees,
    inactiveEmployees: employees.length - activeEmployees,
  };
}

function normalizeCapabilities(value: unknown): LeaveSphereEmployeeManagementCapabilities {
  if (!isRecord(value)) {
    return { ...DEFAULT_CAPABILITIES };
  }
  return {
    canCreate: value.canCreate === undefined ? DEFAULT_CAPABILITIES.canCreate : asBoolean(value.canCreate),
    canUpdate: value.canUpdate === undefined ? DEFAULT_CAPABILITIES.canUpdate : asBoolean(value.canUpdate),
    canActivate: value.canActivate === undefined ? DEFAULT_CAPABILITIES.canActivate : asBoolean(value.canActivate),
    canDeactivate: value.canDeactivate === undefined ? DEFAULT_CAPABILITIES.canDeactivate : asBoolean(value.canDeactivate),
    canArchive: value.canArchive === undefined ? DEFAULT_CAPABILITIES.canArchive : asBoolean(value.canArchive),
  };
}

export function sortLeaveSphereEmployeeManagementEmployees(
  employees: LeaveSphereEmployeeManagementEmployee[],
): LeaveSphereEmployeeManagementEmployee[] {
  return [...employees].sort((left, right) => {
    const leftName = `${left.firstName} ${left.lastName}`.trim().toLowerCase();
    const rightName = `${right.firstName} ${right.lastName}`.trim().toLowerCase();
    const byName = leftName.localeCompare(rightName);
    if (byName !== 0) {
      return byName;
    }
    return left.email.localeCompare(right.email);
  });
}

export function normalizeLeaveSphereEmployeeManagementWorkspace(
  payload: unknown,
): LeaveSphereEmployeeManagementWorkspace | null {
  const data = unwrapEnvelope(payload);
  const workspace = isRecord(data) && isRecord(data.workspace) ? data.workspace : data;
  if (!isRecord(workspace)) {
    return null;
  }

  const employees = Array.isArray(workspace.employees)
    ? workspace.employees
        .map((item) => normalizeLeaveSphereEmployeeManagementEmployee(item))
        .filter((item): item is LeaveSphereEmployeeManagementEmployee => item !== null)
    : [];
  const activeEmployeesFromEmployees = employees.filter((employee) => employee.active).length;
  const summary = isRecord(workspace.summary)
    ? {
        totalEmployees:
          typeof workspace.summary.totalEmployees === "number"
            ? workspace.summary.totalEmployees
            : employees.length,
        activeEmployees:
          typeof workspace.summary.activeEmployees === "number"
            ? workspace.summary.activeEmployees
            : activeEmployeesFromEmployees,
        inactiveEmployees:
          typeof workspace.summary.inactiveEmployees === "number"
            ? workspace.summary.inactiveEmployees
            : employees.length - activeEmployeesFromEmployees,
      }
    : computeSummary(employees);

  return {
    pageCode: asString(workspace.pageCode) || "employee-management",
    pageTitle: asString(workspace.pageTitle) || "Employee Management",
    summary,
    capabilities: normalizeCapabilities(workspace.capabilities),
    employees: sortLeaveSphereEmployeeManagementEmployees(employees),
    source: asString((isRecord(data) ? data.source : null) ?? workspace.source) || null,
    refreshMessage: asString((isRecord(data) ? data.refreshMessage : null) ?? workspace.refreshMessage) || null,
  };
}

export function normalizeLeaveSphereEmployeeManagementForm(
  form: LeaveSphereEmployeeManagementFormState,
): LeaveSphereEmployeeManagementFormState {
  return {
    identityKey: asString(form.identityKey),
    firstName: asString(form.firstName),
    lastName: asString(form.lastName),
    email: asString(form.email).toLowerCase(),
    phone: asString(form.phone),
    dob: asString(form.dob).slice(0, 10),
    pictureUrl: asString(form.pictureUrl),
    region: form.region,
    startDate: asString(form.startDate).slice(0, 10),
    title: asString(form.title),
    isAE: Boolean(form.isAE),
    active: Boolean(form.active),
  };
}

export function buildLeaveSphereEmployeeManagementMutationPayload(
  form: LeaveSphereEmployeeManagementFormState,
): Record<string, unknown> {
  const normalized = normalizeLeaveSphereEmployeeManagementForm(form);
  return {
    identityKey: normalized.identityKey,
    firstName: normalized.firstName,
    lastName: normalized.lastName,
    email: normalized.email,
    phone: normalized.phone || null,
    dob: normalized.dob || null,
    pictureUrl: normalized.pictureUrl || null,
    region: normalized.region || "US",
    startDate: normalized.startDate || null,
    title: normalized.title || null,
    isAE: normalized.isAE,
    active: normalized.active,
  };
}

type RequestJson = (url: string, options?: ApiRequestOptions) => Promise<unknown>;

function buildLoadUrl(freshData: boolean): string {
  return freshData ? "/api/leavesphere/v1/ui/employees/load?fresh_data=true" : "/api/leavesphere/v1/ui/employees/load";
}

export async function loadLeaveSphereEmployeeManagementWorkspace(
  params: {
    requestJson: RequestJson;
    freshData?: boolean;
  },
): Promise<LeaveSphereEmployeeManagementWorkspace> {
  const payload = await params.requestJson(buildLoadUrl(Boolean(params.freshData)), {
    errorToast: false,
  });
  const workspace = normalizeLeaveSphereEmployeeManagementWorkspace(payload);
  if (!workspace) {
    throw new Error("Could not load employee management workspace.");
  }
  return workspace;
}

export async function createLeaveSphereEmployeeManagementEmployee(
  params: {
    requestJson: RequestJson;
    payload: LeaveSphereEmployeeManagementFormState;
  },
): Promise<unknown> {
  return params.requestJson("/api/leavesphere/v1/employees", {
    method: "POST",
    body: buildLeaveSphereEmployeeManagementMutationPayload(params.payload),
    errorToast: false,
    successToast: "Employee created",
  });
}

export async function updateLeaveSphereEmployeeManagementEmployee(
  params: {
    requestJson: RequestJson;
    employeeId: string;
    payload: LeaveSphereEmployeeManagementFormState;
  },
): Promise<unknown> {
  return params.requestJson(`/api/leavesphere/v1/employees/${encodeURIComponent(params.employeeId)}`, {
    method: "PUT",
    body: buildLeaveSphereEmployeeManagementMutationPayload(params.payload),
    errorToast: false,
    successToast: "Employee updated",
  });
}

export async function activateLeaveSphereEmployeeManagementEmployee(
  params: {
    requestJson: RequestJson;
    employeeId: string;
  },
): Promise<unknown> {
  return params.requestJson(`/api/leavesphere/v1/employees/${encodeURIComponent(params.employeeId)}/activate`, {
    method: "POST",
    errorToast: false,
    successToast: "Employee activated",
  });
}

export async function deactivateLeaveSphereEmployeeManagementEmployee(
  params: {
    requestJson: RequestJson;
    employeeId: string;
  },
): Promise<unknown> {
  return params.requestJson(`/api/leavesphere/v1/employees/${encodeURIComponent(params.employeeId)}/deactivate`, {
    method: "POST",
    errorToast: false,
    successToast: "Employee deactivated",
  });
}

export function extractLeaveSphereEmployeeManagementCreatedEmployeeId(
  payload: unknown,
): string | null {
  const data = unwrapEnvelope(payload);
  if (!isRecord(data)) {
    return null;
  }
  return asString(data.id) || asString(data.employeeId) || null;
}

export function extractLeaveSphereEmployeeManagementEmployeeFromPayload(
  payload: unknown,
): LeaveSphereEmployeeManagementEmployee | null {
  const data = unwrapEnvelope(payload);
  if (!isRecord(data)) {
    return null;
  }
  if (isRecord(data.employee)) {
    return normalizeLeaveSphereEmployeeManagementEmployee(data.employee);
  }
  return normalizeLeaveSphereEmployeeManagementEmployee(data);
}
