import type { ApiRequestOptions } from "@shared/hooks/useApiRequest";

export type FundsphereRequestJson = (url: string, options?: ApiRequestOptions) => Promise<unknown>;

export type FundsphereService = {
  id: string;
  name: string;
  conseroId: string | null;
  departmentCode: string;
  departmentName: string;
  departmentListingOrder: number | null;
  description: string | null;
  commission: string;
  netAdjustment: string;
  active: boolean;
  dateCreated?: string | null;
  dateUpdated?: string | null;
};

export type FundsphereDepartment = {
  code: string;
  name: string;
  color: string | null;
  listingOrder: number | null;
  dateCreated?: string | null;
  dateUpdated?: string | null;
};

export type FundsphereServiceSearchCriteria = {
  name: string;
  departmentCode: string;
  statusFilter: "" | "active" | "inactive";
};

export type FundsphereServiceFormState = {
  name: string;
  departmentCode: string;
  description: string;
  commission: string;
  netAdjustment: string;
  active: boolean;
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

function asNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  const text = asString(value);
  if (!text) {
    return null;
  }
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatDecimalDisplay(value: unknown): string {
  const text = asString(value);
  if (!text) {
    return "";
  }
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed.toFixed(2) : text;
}

function normalizeDate(value: unknown): string | null {
  const text = asString(value);
  return text ? text.slice(0, 10) : null;
}

function unwrapEnvelope(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

export function normalizeFundsphereService(value: unknown): FundsphereService | null {
  if (!isRecord(value)) {
    return null;
  }

  const id = asString(value.id);
  const name = asString(value.name);
  const departmentCode = asString(value.departmentCode).toUpperCase();
  const departmentName = asString(value.departmentName);
  if (!id || !name || !departmentCode || !departmentName) {
    return null;
  }

  return {
    id,
    name,
    conseroId: asString(value.conseroId) || null,
    departmentCode,
    departmentName,
    departmentListingOrder: asNullableNumber(value.departmentListingOrder),
    description: asString(value.description) || null,
    commission: formatDecimalDisplay(value.commission) || "0.00",
    netAdjustment: formatDecimalDisplay(value.netAdjustment) || "0.00",
    active: asBoolean(value.active),
    dateCreated: normalizeDate(value.dateCreated),
    dateUpdated: normalizeDate(value.dateUpdated),
  };
}

export function normalizeFundsphereServices(payload: unknown): FundsphereService[] {
  const data = unwrapEnvelope(payload);
  const services = Array.isArray(data) ? data : [];
  return services
    .map((item) => normalizeFundsphereService(item))
    .filter((item): item is FundsphereService => item !== null);
}

export function normalizeFundsphereDepartment(value: unknown): FundsphereDepartment | null {
  if (!isRecord(value)) {
    return null;
  }

  const code = asString(value.code).toUpperCase();
  const name = asString(value.name);
  if (!code || !name) {
    return null;
  }

  return {
    code,
    name,
    color: asString(value.color) || null,
    listingOrder: asNullableNumber(value.listingOrder),
    dateCreated: normalizeDate(value.dateCreated),
    dateUpdated: normalizeDate(value.dateUpdated),
  };
}

export function normalizeFundsphereDepartments(payload: unknown): FundsphereDepartment[] {
  const data = unwrapEnvelope(payload);
  const departments = Array.isArray(data) ? data : [];
  return departments
    .map((item) => normalizeFundsphereDepartment(item))
    .filter((item): item is FundsphereDepartment => item !== null);
}

export function normalizeFundsphereServiceForm(service: FundsphereService | null): FundsphereServiceFormState {
  if (!service) {
    return {
      name: "",
      departmentCode: "",
      description: "",
      commission: "",
      netAdjustment: "",
      active: true,
    };
  }
  return {
    name: service.name,
    departmentCode: service.departmentCode,
    description: service.description ?? "",
    commission: formatDecimalDisplay(service.commission),
    netAdjustment: formatDecimalDisplay(service.netAdjustment),
    active: service.active,
  };
}

function cleanOptionalText(value: string): string | null {
  const text = asString(value);
  return text || null;
}

export async function loadFundsphereServices(params: {
  requestJson: FundsphereRequestJson;
  criteria?: FundsphereServiceSearchCriteria;
}): Promise<FundsphereService[]> {
  const searchParams = new URLSearchParams();
  const criteria = params.criteria;
  const name = asString(criteria?.name);
  const departmentCode = asString(criteria?.departmentCode).toUpperCase();
  const statusFilter = criteria?.statusFilter ?? "active";

  if (name) {
    searchParams.set("name", name);
  }
  if (departmentCode) {
    searchParams.set("departmentCode", departmentCode);
  }
  if (statusFilter === "active" || statusFilter === "inactive") {
    searchParams.set("status", statusFilter);
  } else {
    searchParams.set("status", "all");
  }
  searchParams.set("summary", "true");

  const query = searchParams.toString();
  const payload = await params.requestJson(query ? `/api/fundsphere/v1/services?${query}` : "/api/fundsphere/v1/services");
  return normalizeFundsphereServices(payload);
}

export async function loadFundsphereService(params: {
  requestJson: FundsphereRequestJson;
  id: string;
}): Promise<FundsphereService> {
  const id = asString(params.id);
  const payload = await params.requestJson(`/api/fundsphere/v1/services?id=${encodeURIComponent(id)}`, {
    errorToast: false,
  });
  const service = normalizeFundsphereService(payload);
  if (!service) {
    throw new Error("Could not load service.");
  }
  return service;
}

export async function loadFundsphereDepartments(params: {
  requestJson: FundsphereRequestJson;
}): Promise<FundsphereDepartment[]> {
  const payload = await params.requestJson("/api/fundsphere/v1/departments", { errorToast: false });
  return normalizeFundsphereDepartments(payload);
}

export async function createFundsphereService(params: {
  requestJson: FundsphereRequestJson;
  form: FundsphereServiceFormState;
}): Promise<void> {
  await params.requestJson("/api/fundsphere/v1/services", {
    method: "POST",
    body: {
      name: asString(params.form.name),
      departmentCode: asString(params.form.departmentCode).toUpperCase(),
      description: cleanOptionalText(params.form.description),
      commission: cleanOptionalText(params.form.commission),
      netAdjustment: cleanOptionalText(params.form.netAdjustment),
      active: params.form.active,
    },
    successToast: {
      title: "Service created",
      message: `Service ${asString(params.form.name)} was created successfully.`,
    },
    errorToast: {
      title: "Create failed",
    },
  });
}

export async function updateFundsphereService(params: {
  requestJson: FundsphereRequestJson;
  id: string;
  form: FundsphereServiceFormState;
}): Promise<void> {
  const id = asString(params.id);
  await params.requestJson(`/api/fundsphere/v1/services?id=${encodeURIComponent(id)}`, {
    method: "PUT",
    body: {
      name: asString(params.form.name),
      departmentCode: asString(params.form.departmentCode).toUpperCase(),
      description: cleanOptionalText(params.form.description),
      commission: cleanOptionalText(params.form.commission),
      netAdjustment: cleanOptionalText(params.form.netAdjustment),
      active: params.form.active,
    },
    successToast: {
      title: "Service updated",
      message: `Service ${asString(params.form.name)} was updated successfully.`,
    },
    errorToast: {
      title: "Save failed",
    },
  });
}
