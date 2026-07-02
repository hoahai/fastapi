import type { ApiRequestOptions } from "@shared/hooks/useApiRequest";

export type FundsphereRequestJson = (url: string, options?: ApiRequestOptions) => Promise<unknown>;

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

function asNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  const parsed = Number(asString(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function unwrapEnvelope(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

function normalizeNumberText(value: unknown): string {
  const text = asString(value);
  if (!text) {
    return "";
  }
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed.toFixed(2) : text;
}

export type FundsphereBudgetMatrixPeriod = {
  month: number;
  year: number;
  value: string;
  label: string;
};

export type FundsphereBudgetMatrixRow = {
  budgetId: string | null;
  accountCode: string;
  accountName: string;
  year: number;
  month: number;
  serviceId: string;
  serviceName: string;
  departmentCode: string;
  departmentName: string;
  departmentListingOrder: number | null;
  subService: string;
  grossAmount: string;
  commission: string;
  netAdjustment: string;
  netAmount: string;
  note: string;
};

export type FundsphereBudgetMatrixResponse = {
  accountCodes: string[];
  periods: FundsphereBudgetMatrixPeriod[];
  rows: FundsphereBudgetMatrixRow[];
  rowCount: number;
};

export type FundsphereBudgetFormState = {
  grossAmount: string;
  commission: string;
  netAdjustment: string;
  note: string;
};

export type FundsphereBudgetCellContext = {
  accountCode: string;
  accountName: string;
  month: number;
  year: number;
  serviceId: string;
  serviceName: string;
  departmentCode: string;
  departmentName: string;
  subService: string;
};

function normalizeBudgetMatrixRow(value: unknown): FundsphereBudgetMatrixRow | null {
  if (!isRecord(value)) {
    return null;
  }

  const accountCode = asString(value.accountCode).toUpperCase();
  const accountName = asString(value.accountName);
  const year = asNullableNumber(value.year);
  const month = asNullableNumber(value.month);
  const serviceId = asString(value.serviceId);
  const serviceName = asString(value.serviceName);
  const departmentCode = asString(value.departmentCode).toUpperCase();
  const departmentName = asString(value.departmentName);
  if (!accountCode || !accountName || !year || !month || !serviceId || !serviceName || !departmentCode || !departmentName) {
    return null;
  }

  return {
    budgetId: asString(value.budgetId) || null,
    accountCode,
    accountName,
    year,
    month,
    serviceId,
    serviceName,
    departmentCode,
    departmentName,
    departmentListingOrder: asNullableNumber(value.departmentListingOrder),
    subService: asString(value.subService),
    grossAmount: normalizeNumberText(value.grossAmount),
    commission: normalizeNumberText(value.commission),
    netAdjustment: normalizeNumberText(value.netAdjustment),
    netAmount: normalizeNumberText(value.netAmount),
    note: asString(value.note),
  };
}

function normalizeBudgetMatrixRows(payload: unknown): FundsphereBudgetMatrixRow[] {
  const data = unwrapEnvelope(payload);
  const rows = isRecord(data) && Array.isArray(data.rows) ? data.rows : Array.isArray(data) ? data : [];
  return rows
    .map((item) => normalizeBudgetMatrixRow(item))
    .filter((item): item is FundsphereBudgetMatrixRow => item !== null);
}

export function normalizeFundsphereBudgetMatrixResponse(payload: unknown): FundsphereBudgetMatrixResponse {
  const data = unwrapEnvelope(payload);
  if (!isRecord(data)) {
    return {
      accountCodes: [],
      periods: [],
      rows: [],
      rowCount: 0,
    };
  }

  const accountCodes = Array.isArray(data.accountCodes)
    ? Array.from(new Set(data.accountCodes.map((item) => asString(item).toUpperCase()).filter(Boolean)))
    : [];
  const periods = Array.isArray(data.periods)
    ? data.periods
        .map((item) => {
          if (!isRecord(item)) {
            return null;
          }
          const month = asNullableNumber(item.month);
          const year = asNullableNumber(item.year);
          const value = asString(item.value);
          const label = asString(item.label);
          if (!month || !year || !value || !label) {
            return null;
          }
          return { month, year, value, label };
        })
        .filter((item): item is FundsphereBudgetMatrixPeriod => item !== null)
    : [];
  const rows = normalizeBudgetMatrixRows(data);
  const rowCount = asNullableNumber(data.rowCount) ?? rows.length;

  return {
    accountCodes,
    periods,
    rows,
    rowCount,
  };
}

function normalizeBudgetFormNumber(value: unknown): string {
  const text = asString(value);
  return text ? text : "";
}

export function normalizeFundsphereBudgetForm(value: FundsphereBudgetMatrixRow | null): FundsphereBudgetFormState {
  if (!value) {
    return {
      grossAmount: "",
      commission: "",
      netAdjustment: "",
      note: "",
    };
  }
  return {
    grossAmount: normalizeBudgetFormNumber(value.grossAmount),
    commission: normalizeBudgetFormNumber(value.commission),
    netAdjustment: normalizeBudgetFormNumber(value.netAdjustment),
    note: value.note,
  };
}

export function normalizeFundsphereBudgetRow(payload: unknown): FundsphereBudgetMatrixRow | null {
  return normalizeBudgetMatrixRow(payload);
}

function cleanOptionalText(value: string): string | null {
  const text = asString(value);
  return text || null;
}

function parseBudgetIdentity(value: FundsphereBudgetCellContext) {
  return {
    accountCode: asString(value.accountCode).toUpperCase(),
    accountName: asString(value.accountName),
    month: Number(value.month),
    year: Number(value.year),
    serviceId: asString(value.serviceId),
    serviceName: asString(value.serviceName),
    departmentCode: asString(value.departmentCode).toUpperCase(),
    departmentName: asString(value.departmentName),
    subService: asString(value.subService),
  };
}

export async function loadFundsphereBudgetMatrix(params: {
  requestJson: FundsphereRequestJson;
  accountCodes: string[];
  periods: string[];
  serviceIds: string[];
}): Promise<FundsphereBudgetMatrixResponse> {
  const payload = await params.requestJson("/api/fundsphere/v1/masterBudgetControl/budgetMatrix/load", {
    method: "POST",
    body: {
      accountCodes: params.accountCodes,
      periods: params.periods,
      serviceIds: params.serviceIds,
    },
    errorToast: false,
  });
  return normalizeFundsphereBudgetMatrixResponse(payload);
}

export async function loadFundsphereBudgetDetail(params: {
  requestJson: FundsphereRequestJson;
  budgetId: string;
}): Promise<FundsphereBudgetMatrixRow | null> {
  const payload = await params.requestJson(`/api/fundsphere/v1/budgets?id=${encodeURIComponent(params.budgetId)}`, {
    method: "GET",
    errorToast: false,
  });
  const data = unwrapEnvelope(payload);
  if (Array.isArray(data)) {
    return normalizeBudgetMatrixRow(data[0] ?? null);
  }
  return normalizeBudgetMatrixRow(data);
}

export async function createFundsphereBudget(params: {
  requestJson: FundsphereRequestJson;
  identity: FundsphereBudgetCellContext;
  form: FundsphereBudgetFormState;
  changeReason?: string;
}): Promise<{ inserted: number; id: string }> {
  const identity = parseBudgetIdentity(params.identity);
  const payload = await params.requestJson("/api/fundsphere/v1/budgets", {
    method: "POST",
    body: {
      accountCode: identity.accountCode,
      month: identity.month,
      year: identity.year,
      serviceId: identity.serviceId,
      subService: identity.subService,
      grossAmount: normalizeNumberText(params.form.grossAmount),
      commission: normalizeNumberText(params.form.commission),
      netAdjustment: normalizeNumberText(params.form.netAdjustment),
      note: cleanOptionalText(params.form.note),
      changeNote: cleanOptionalText(params.changeReason ?? ""),
    },
    successToast: {
      title: "Budget created",
      message: `${identity.accountCode} ${identity.month}/${identity.year} was saved successfully.`,
    },
    errorToast: {
      title: "Create failed",
    },
  });
  const data = unwrapEnvelope(payload);
  if (!isRecord(data)) {
    throw new Error("Could not create budget.");
  }
  return {
    inserted: Number(data.inserted ?? 0) || 0,
    id: asString(data.id),
  };
}

export async function updateFundsphereBudget(params: {
  requestJson: FundsphereRequestJson;
  budgetId: string;
  identity: FundsphereBudgetCellContext;
  form: FundsphereBudgetFormState;
  changeReason?: string;
}): Promise<{ updated: number; id: string }> {
  const identity = parseBudgetIdentity(params.identity);
  const payload = await params.requestJson(`/api/fundsphere/v1/budgets?id=${encodeURIComponent(params.budgetId)}`, {
    method: "PUT",
    body: {
      grossAmount: normalizeNumberText(params.form.grossAmount),
      commission: normalizeNumberText(params.form.commission),
      netAdjustment: normalizeNumberText(params.form.netAdjustment),
      note: cleanOptionalText(params.form.note),
      changeNote: cleanOptionalText(params.changeReason ?? ""),
      subService: identity.subService,
    },
    successToast: {
      title: "Budget saved",
      message: `${identity.accountCode} ${identity.month}/${identity.year} was updated successfully.`,
    },
    errorToast: {
      title: "Save failed",
    },
  });
  const data = unwrapEnvelope(payload);
  if (!isRecord(data)) {
    throw new Error("Could not update budget.");
  }
  return {
    updated: Number(data.updated ?? 0) || 0,
    id: asString(data.id),
  };
}
