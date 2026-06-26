import type { ApiRequestOptions } from "@shared/hooks/useApiRequest";

export type FundsphereRequestJson = (url: string, options?: ApiRequestOptions) => Promise<unknown>;

export type FundsphereAccount = {
  code: string;
  name: string;
  logoUrl: string | null;
  conseroId: string | null;
  conseroName: string | null;
  strataName: string | null;
  active: boolean;
  endDate: string | null;
  dateCreated?: string | null;
  dateUpdated?: string | null;
};

export type FundsphereAccountSearchCriteria = {
  code: string;
  name: string;
  aeName: string;
  statusFilter: "" | "active" | "inactive";
};

export type FundsphereAccountFormState = {
  code: string;
  name: string;
  logoUrl: string;
  conseroId: string;
  conseroName: string;
  strataName: string;
  active: boolean;
  endDate: string;
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

function normalizeDate(value: unknown): string | null {
  const text = asString(value);
  if (!text) {
    return null;
  }
  return text.slice(0, 10);
}

function unwrapEnvelope(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

export function normalizeFundsphereAccount(value: unknown): FundsphereAccount | null {
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
    logoUrl: asString(value.logoUrl) || null,
    conseroId: asString(value.conseroId) || null,
    conseroName: asString(value.conseroName) || null,
    strataName: asString(value.strataName) || null,
    active: asBoolean(value.active),
    endDate: normalizeDate(value.endDate),
    dateCreated: normalizeDate(value.dateCreated),
    dateUpdated: normalizeDate(value.dateUpdated),
  };
}

export function normalizeFundsphereAccounts(payload: unknown): FundsphereAccount[] {
  const data = unwrapEnvelope(payload);
  const accounts = Array.isArray(data) ? data : [];
  return accounts
    .map((item) => normalizeFundsphereAccount(item))
    .filter((item): item is FundsphereAccount => item !== null);
}

export function normalizeFundsphereAccountForm(account: FundsphereAccount | null): FundsphereAccountFormState {
  if (!account) {
    return {
      code: "",
      name: "",
      logoUrl: "",
      conseroId: "",
      conseroName: "",
      strataName: "",
      active: true,
      endDate: "",
    };
  }
  return {
    code: account.code,
    name: account.name,
    logoUrl: account.logoUrl ?? "",
    conseroId: account.conseroId ?? "",
    conseroName: account.conseroName ?? "",
    strataName: account.strataName ?? "",
    active: account.active,
    endDate: account.endDate ?? "",
  };
}

function cleanOptionalText(value: string): string | null {
  const text = asString(value);
  return text || null;
}

export async function loadFundsphereAccounts(params: {
  requestJson: FundsphereRequestJson;
  criteria?: FundsphereAccountSearchCriteria;
}): Promise<FundsphereAccount[]> {
  const searchParams = new URLSearchParams();
  const criteria = params.criteria;
  const code = asString(criteria?.code);
  const name = asString(criteria?.name);
  const aeName = asString(criteria?.aeName);
  const statusFilter = criteria?.statusFilter ?? "active";

  if (code) {
    searchParams.set("code", code);
  }
  if (name) {
    searchParams.set("name", name);
  }
  if (aeName) {
    searchParams.set("aeName", aeName);
  }
  if (statusFilter === "active" || statusFilter === "inactive") {
    searchParams.set("status", statusFilter);
  } else {
    searchParams.set("status", "all");
  }

  const query = searchParams.toString();
  const payload = await params.requestJson(query ? `/api/fundsphere/v1/accounts?${query}` : "/api/fundsphere/v1/accounts");
  return normalizeFundsphereAccounts(payload);
}

export async function createFundsphereAccount(params: {
  requestJson: FundsphereRequestJson;
  form: FundsphereAccountFormState;
}): Promise<void> {
  const code = asString(params.form.code).toUpperCase();
  const name = asString(params.form.name);
  await params.requestJson("/api/fundsphere/v1/accounts", {
    method: "POST",
    body: {
      code,
      name,
      logoUrl: cleanOptionalText(params.form.logoUrl),
      conseroId: cleanOptionalText(params.form.conseroId),
      conseroName: cleanOptionalText(params.form.conseroName),
      strataName: cleanOptionalText(params.form.strataName),
      active: params.form.active,
      endDate: cleanOptionalText(params.form.endDate),
    },
    successToast: {
      title: "Account created",
      message: `Account ${code} was created successfully.`,
    },
    errorToast: {
      title: "Create failed",
    },
  });
}

export async function updateFundsphereAccount(params: {
  requestJson: FundsphereRequestJson;
  code: string;
  form: FundsphereAccountFormState;
}): Promise<void> {
  const code = asString(params.code).toUpperCase();
  await params.requestJson(`/api/fundsphere/v1/accounts?code=${encodeURIComponent(code)}`, {
    method: "PUT",
    body: {
      name: asString(params.form.name),
      logoUrl: cleanOptionalText(params.form.logoUrl),
      conseroId: cleanOptionalText(params.form.conseroId),
      conseroName: cleanOptionalText(params.form.conseroName),
      strataName: cleanOptionalText(params.form.strataName),
      active: params.form.active,
      endDate: cleanOptionalText(params.form.endDate),
    },
    successToast: {
      title: "Account updated",
      message: `Account ${code} was updated successfully.`,
    },
    errorToast: {
      title: "Save failed",
    },
  });
}

export async function uploadFundsphereAccountLogo(params: {
  requestJson: FundsphereRequestJson;
  accountCode: string;
  file: File;
}): Promise<string> {
  const formData = new FormData();
  formData.append("accountCode", asString(params.accountCode));
  formData.append("file", params.file);

  const payload = await params.requestJson("/api/fundsphere/v1/accounts/logo/upload", {
    method: "POST",
    body: formData,
    errorToast: false,
  });
  const data = unwrapEnvelope(payload);
  if (!isRecord(data)) {
    throw new Error("Could not upload logo.");
  }
  const logoUrl = asString(data.logoUrl);
  if (!logoUrl) {
    throw new Error("Could not upload logo.");
  }
  return logoUrl;
}
