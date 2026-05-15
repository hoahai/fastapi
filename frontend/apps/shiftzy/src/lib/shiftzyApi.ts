import type { ApiRequestOptions } from "@/hooks/useApiRequest";

export type ShiftzyRequestJson = (url: string, options?: ApiRequestOptions) => Promise<unknown>;

export type ShiftzyWeek = {
  weekNo: number;
  startDate: string;
  endDate: string;
  isTodayWeek: boolean;
};

export type ShiftzyPosition = {
  code: string;
  name: string;
  icon: string | null;
  active: boolean;
};

export type ShiftzyEmployee = {
  id: string;
  name: string;
  scheduleSection: string;
  note: string | null;
  refPositionCode: string | null;
  active: boolean;
};

export type ShiftzyShift = {
  id: string;
  name: string;
  startTime: string;
  endTime: string;
  duration: string | null;
  active: boolean;
};

export type ShiftzySchedule = {
  id: string;
  employeeId: string;
  positionCode: string;
  shiftId: string | null;
  date: string;
  startTime: string;
  endTime: string;
  note: string | null;
  employeeName: string;
  scheduleSection: string;
  positionName: string;
  shiftName: string;
};

export type ShiftzyBootstrapPayload = {
  weeks: ShiftzyWeek[];
  employees: ShiftzyEmployee[];
  positions: ShiftzyPosition[];
  shifts: ShiftzyShift[];
};

export type ShiftzyScheduleUpdateInput = {
  id: string;
  employeeId: string;
  positionCode: string;
  shiftId: string | null;
  date: string;
  startTime: string;
  endTime: string;
  note: string | null;
};

export type ShiftzyScheduleCreateInput = {
  id?: string | null;
  employeeId: string;
  positionCode: string;
  shiftId: string | null;
  date: string;
  startTime: string;
  endTime: string;
  note: string | null;
};

export type ShiftzyDuplicateSchedulesInput = {
  weekStart: number;
  weekEnd: number;
  overwrite?: boolean;
  returnSchedules?: boolean;
};

export type ShiftzyEmployeesFetchOptions = {
  includeAll?: boolean;
  employeeId?: string | null;
};

export type ShiftzyPositionsFetchOptions = {
  includeAll?: boolean;
  code?: string | null;
};

export type ShiftzyEmployeeCreateInput = {
  id?: string | null;
  name: string;
  scheduleSection: string;
  note?: string | null;
  refPositionCode?: string | null;
  active?: boolean;
};

export type ShiftzyEmployeeUpdateInput = {
  id: string;
  name?: string | null;
  scheduleSection?: string | null;
  note?: string | null;
  refPositionCode?: string | null;
  active?: boolean;
};

function buildShiftzyCompatHeaders(): HeadersInit {
  const headers: Record<string, string> = {};
  const authMode = String(import.meta.env.VITE_AUTH_MODE || "compat").trim().toLowerCase();
  const legacyEnabled = String(import.meta.env.VITE_AUTH_ENABLE_LEGACY_API_KEY_FALLBACK || "true")
    .trim()
    .toLowerCase();
  const legacyApiKey = String(import.meta.env.VITE_LEGACY_API_KEY || "").trim();
  const legacyUserName = String(import.meta.env.VITE_LEGACY_USER_NAME || "").trim();

  if (authMode === "compat" && legacyEnabled !== "false" && legacyApiKey) {
    headers["X-API-Key"] = legacyApiKey;
    if (legacyUserName) {
      headers["X-User-Name"] = legacyUserName;
    }
  }
  return headers;
}

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

function asNullableString(value: unknown): string | null {
  const text = asString(value);
  return text || null;
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

function asBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
  }
  return false;
}

function unwrapData(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

function normalizeWeek(value: unknown): ShiftzyWeek | null {
  if (!isRecord(value)) {
    return null;
  }
  const weekNo = Math.trunc(asNumber(value.week_no));
  const startDate = asString(value.start_date);
  const endDate = asString(value.end_date);
  if (!weekNo || !startDate || !endDate) {
    return null;
  }
  return {
    weekNo,
    startDate,
    endDate,
    isTodayWeek: asBoolean(value.is_today_week),
  };
}

function normalizePosition(value: unknown): ShiftzyPosition | null {
  if (!isRecord(value)) {
    return null;
  }
  const code = asString(value.code);
  const name = asString(value.name);
  if (!code || !name) {
    return null;
  }
  return {
    code,
    name,
    icon: asNullableString(value.icon),
    active: asBoolean(value.active),
  };
}

function normalizeEmployee(value: unknown): ShiftzyEmployee | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asString(value.id);
  const name = asString(value.name);
  const scheduleSection = asString(value.schedule_section);
  if (!id || !name || !scheduleSection) {
    return null;
  }
  return {
    id,
    name,
    scheduleSection,
    note: asNullableString(value.note),
    refPositionCode: asNullableString(value.ref_positionCode),
    active: asBoolean(value.active),
  };
}

function normalizeShift(value: unknown): ShiftzyShift | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asString(value.id);
  const name = asString(value.name);
  const startTime = asString(value.start_time);
  const endTime = asString(value.end_time);
  if (!id || !name || !startTime || !endTime) {
    return null;
  }
  return {
    id,
    name,
    startTime,
    endTime,
    duration: asNullableString(value.duration),
    active: asBoolean(value.active),
  };
}

function normalizeSchedule(value: unknown): ShiftzySchedule | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asString(value.id);
  const employeeId = asString(value.employee_id);
  const positionCode = asString(value.position_code);
  const date = asString(value.date);
  const startTime = asString(value.start_time);
  const endTime = asString(value.end_time);
  if (!id || !employeeId || !positionCode || !date || !startTime || !endTime) {
    return null;
  }
  return {
    id,
    employeeId,
    positionCode,
    shiftId: asNullableString(value.shift_id),
    date,
    startTime,
    endTime,
    note: asNullableString(value.note),
    employeeName: asString(value.employee_name) || "Unassigned",
    scheduleSection: asString(value.schedule_section) || "General",
    positionName: asString(value.position_name) || positionCode,
    shiftName: asString(value.shift_name) || "Custom",
  };
}

function normalizeList<T>(value: unknown, normalizeItem: (item: unknown) => T | null): T[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const next: T[] = [];
  for (const entry of value) {
    const normalized = normalizeItem(entry);
    if (normalized) {
      next.push(normalized);
    }
  }
  return next;
}

export async function fetchShiftzyBootstrap(requestJson: ShiftzyRequestJson): Promise<ShiftzyBootstrapPayload> {
  const payload = await requestJson(
    "/api/shiftzy/v1/bootstrap?tables=weeks,employees,positions,shifts&all=true",
    {
      headers: buildShiftzyCompatHeaders(),
      errorToast: { title: "Could not load Shiftzy reference data" },
    },
  );
  const data = unwrapData(payload);
  if (!isRecord(data)) {
    return { weeks: [], employees: [], positions: [], shifts: [] };
  }
  return {
    weeks: normalizeList(data.weeks, normalizeWeek),
    employees: normalizeList(data.employees, normalizeEmployee),
    positions: normalizeList(data.positions, normalizePosition),
    shifts: normalizeList(data.shifts, normalizeShift),
  };
}

export async function fetchShiftzyWeeks(
  requestJson: ShiftzyRequestJson,
  weekBefore = 3,
  weekAfter = 6,
): Promise<ShiftzyWeek[]> {
  const params = new URLSearchParams({
    week_before: String(Math.max(0, Math.trunc(weekBefore))),
    week_after: String(Math.max(0, Math.trunc(weekAfter))),
  });
  const payload = await requestJson(`/api/shiftzy/v1/weeks?${params.toString()}`, {
    headers: buildShiftzyCompatHeaders(),
    errorToast: { title: "Could not load Shiftzy weeks" },
  });
  return normalizeList(unwrapData(payload), normalizeWeek);
}

export async function fetchShiftzySchedules(
  requestJson: ShiftzyRequestJson,
  weekNo: number,
): Promise<ShiftzySchedule[]> {
  const params = new URLSearchParams({ week_no: String(Math.trunc(weekNo)) });
  const payload = await requestJson(`/api/shiftzy/v1/schedules?${params.toString()}`, {
    headers: buildShiftzyCompatHeaders(),
    errorToast: { title: "Could not load Shiftzy schedules" },
  });
  return normalizeList(unwrapData(payload), normalizeSchedule);
}

export async function updateShiftzySchedule(
  requestJson: ShiftzyRequestJson,
  input: ShiftzyScheduleUpdateInput,
): Promise<void> {
  await requestJson("/api/shiftzy/v1/schedules", {
    method: "PUT",
    headers: buildShiftzyCompatHeaders(),
    body: {
      id: input.id,
      employee_id: input.employeeId,
      position_code: input.positionCode,
      shift_id: input.shiftId,
      date: input.date,
      start_time: input.startTime,
      end_time: input.endTime,
      note: input.note,
    },
    successToast: "Shift updated",
    errorToast: { title: "Could not update shift" },
  });
}

export async function createShiftzySchedules(
  requestJson: ShiftzyRequestJson,
  input: ShiftzyScheduleCreateInput | ShiftzyScheduleCreateInput[],
): Promise<void> {
  const rows = Array.isArray(input) ? input : [input];
  const payload = rows.map((item) => ({
    id: item.id || undefined,
    employee_id: item.employeeId,
    position_code: item.positionCode,
    shift_id: item.shiftId,
    date: item.date,
    start_time: item.startTime,
    end_time: item.endTime,
    note: item.note,
  }));
  await requestJson("/api/shiftzy/v1/schedules", {
    method: "POST",
    headers: buildShiftzyCompatHeaders(),
    body: payload,
    successToast: "Schedule added",
    errorToast: { title: "Could not add schedule" },
  });
}

export async function deleteShiftzySchedules(
  requestJson: ShiftzyRequestJson,
  scheduleIds: string[],
): Promise<void> {
  const ids = scheduleIds
    .map((id) => String(id).trim())
    .filter(Boolean);
  if (!ids.length) {
    return;
  }
  await requestJson("/api/shiftzy/v1/schedules", {
    method: "DELETE",
    headers: buildShiftzyCompatHeaders(),
    body: ids,
    successToast: "Schedule removed",
    errorToast: { title: "Could not remove schedule" },
  });
}

export async function duplicateShiftzySchedules(
  requestJson: ShiftzyRequestJson,
  input: ShiftzyDuplicateSchedulesInput,
): Promise<void> {
  const params = new URLSearchParams({
    week_start: String(Math.trunc(input.weekStart)),
    week_end: String(Math.trunc(input.weekEnd)),
    overwrite: input.overwrite ? "true" : "false",
    return_schedules: input.returnSchedules ? "true" : "false",
  });
  await requestJson(`/api/shiftzy/v1/schedules/duplicate?${params.toString()}`, {
    method: "POST",
    headers: buildShiftzyCompatHeaders(),
    successToast: "Schedules duplicated",
    errorToast: { title: "Could not duplicate schedules" },
  });
}

export async function exportShiftzySchedulesPdf(
  requestJson: ShiftzyRequestJson,
  weekNo: number,
): Promise<ArrayBuffer> {
  const params = new URLSearchParams({
    week_no: String(Math.trunc(weekNo)),
    orientation: "portrait",
  });
  const payload = await requestJson(`/api/shiftzy/v1/schedules/pdf?${params.toString()}`, {
    headers: {
      ...buildShiftzyCompatHeaders(),
      Accept: "application/pdf",
    },
    errorToast: false,
  });
  if (!(payload instanceof ArrayBuffer)) {
    throw new Error("Could not export schedules PDF.");
  }
  return payload;
}

export async function fetchShiftzyEmployees(
  requestJson: ShiftzyRequestJson,
  options: ShiftzyEmployeesFetchOptions = {},
): Promise<ShiftzyEmployee[]> {
  const params = new URLSearchParams();
  if (options.employeeId && String(options.employeeId).trim()) {
    params.set("id", String(options.employeeId).trim());
  }
  params.set("all", options.includeAll === false ? "false" : "true");
  const payload = await requestJson(`/api/shiftzy/v1/employees?${params.toString()}`, {
    headers: buildShiftzyCompatHeaders(),
    errorToast: { title: "Could not load Shiftzy employees" },
  });
  return normalizeList(unwrapData(payload), normalizeEmployee);
}

export async function fetchShiftzyPositions(
  requestJson: ShiftzyRequestJson,
  options: ShiftzyPositionsFetchOptions = {},
): Promise<ShiftzyPosition[]> {
  const params = new URLSearchParams();
  if (options.code && String(options.code).trim()) {
    params.set("code", String(options.code).trim());
  }
  params.set("all", options.includeAll === false ? "false" : "true");
  const payload = await requestJson(`/api/shiftzy/v1/positions?${params.toString()}`, {
    headers: buildShiftzyCompatHeaders(),
    errorToast: { title: "Could not load Shiftzy positions" },
  });
  return normalizeList(unwrapData(payload), normalizePosition);
}

export async function createShiftzyEmployees(
  requestJson: ShiftzyRequestJson,
  input: ShiftzyEmployeeCreateInput | ShiftzyEmployeeCreateInput[],
): Promise<void> {
  const rows = Array.isArray(input) ? input : [input];
  const payload = rows.map((item) => ({
    id: item.id || undefined,
    name: String(item.name || "").trim(),
    schedule_section: String(item.scheduleSection || "").trim(),
    note: item.note ?? null,
    ref_positionCode: item.refPositionCode ?? null,
    active: item.active ?? true,
  }));
  await requestJson("/api/shiftzy/v1/employees", {
    method: "POST",
    headers: buildShiftzyCompatHeaders(),
    body: payload,
    successToast: "Employee added",
    errorToast: { title: "Could not add employee" },
  });
}

export async function updateShiftzyEmployees(
  requestJson: ShiftzyRequestJson,
  input: ShiftzyEmployeeUpdateInput | ShiftzyEmployeeUpdateInput[],
): Promise<void> {
  const rows = Array.isArray(input) ? input : [input];
  const payload = rows.map((item) => ({
    id: String(item.id || "").trim(),
    ...(item.name !== undefined ? { name: item.name } : {}),
    ...(item.scheduleSection !== undefined
      ? { schedule_section: item.scheduleSection }
      : {}),
    ...(item.note !== undefined ? { note: item.note } : {}),
    ...(item.refPositionCode !== undefined
      ? { ref_positionCode: item.refPositionCode }
      : {}),
    ...(item.active !== undefined ? { active: item.active } : {}),
  }));
  await requestJson("/api/shiftzy/v1/employees", {
    method: "PUT",
    headers: buildShiftzyCompatHeaders(),
    body: payload,
    successToast: "Employee updated",
    errorToast: { title: "Could not update employee" },
  });
}

export async function deleteShiftzyEmployees(
  requestJson: ShiftzyRequestJson,
  employeeIds: string[],
): Promise<void> {
  const ids = employeeIds
    .map((id) => String(id).trim())
    .filter(Boolean);
  if (!ids.length) {
    return;
  }
  await requestJson("/api/shiftzy/v1/employees", {
    method: "DELETE",
    headers: buildShiftzyCompatHeaders(),
    body: ids,
    successToast: "Employee removed",
    errorToast: { title: "Could not remove employee" },
  });
}
