export type LeaveSphereQuickApprovalViewState = "ready" | "invalid" | "expired" | "already_handled";

export type LeaveSphereQuickApprovalDecision = "approved" | "rejected";

export type LeaveSphereQuickApprovalRequestStatus = "pending" | "approved" | "rejected" | "cancelled";

export type LeaveSphereQuickApprovalPreview = {
  requestId: string | null;
  employeeName: string;
  ptoTypeCode: string | null;
  ptoTypeLabel: string;
  startDate: string;
  endDate: string;
  hoursRequested: number | null;
  daysRequested: number | null;
  reason: string | null;
  currentStatus: LeaveSphereQuickApprovalRequestStatus;
};

export type LeaveSphereQuickApprovalLoadResult = {
  state: LeaveSphereQuickApprovalViewState;
  preview: LeaveSphereQuickApprovalPreview | null;
  handledDecision: LeaveSphereQuickApprovalDecision | null;
  canAct: boolean;
  source: "api" | "mock";
  message: string | null;
};

export type LeaveSphereQuickApprovalDecisionResult = {
  state: "success" | "invalid" | "expired" | "already_handled";
  decision: LeaveSphereQuickApprovalDecision | null;
  source: "api" | "mock";
  message: string | null;
};

export type LeaveSphereQuickApprovalErrorKind = "invalid" | "expired" | "already_handled" | "generic";

class LeaveSphereQuickApprovalError extends Error {
  kind: LeaveSphereQuickApprovalErrorKind;

  constructor(kind: LeaveSphereQuickApprovalErrorKind, message: string) {
    super(message);
    this.name = "LeaveSphereQuickApprovalError";
    this.kind = kind;
  }
}

type ApiResponse = {
  status: number;
  payload: unknown;
};

const PUBLIC_APPROVAL_BASE = "/api/leavesphere/v1/public/approval";
const MOCK_DELAY_MS = 220;

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function normalizeLookupKey(value: string): string {
  return asString(value).toLowerCase().replace(/[^a-z0-9]+/g, "");
}

const QUICK_APPROVAL_PTO_TYPE_LABELS: Record<string, string> = {
  person: "Personal/Birthday",
  personal: "Personal/Birthday",
  pto: "Paid Time Off",
  vacation: "Vacation",
  sick: "Sick",
  floating: "Floating Holiday",
};

function resolveQuickApprovalPtoTypeLabel(rawCode: unknown, rawLabel: unknown): string {
  const codeKey = normalizeLookupKey(asString(rawCode));
  if (codeKey && QUICK_APPROVAL_PTO_TYPE_LABELS[codeKey]) {
    return QUICK_APPROVAL_PTO_TYPE_LABELS[codeKey];
  }

  const label = asString(rawLabel);
  const labelKey = normalizeLookupKey(label);
  if (labelKey && QUICK_APPROVAL_PTO_TYPE_LABELS[labelKey]) {
    return QUICK_APPROVAL_PTO_TYPE_LABELS[labelKey];
  }

  return label || asString(rawCode) || "PTO";
}

function unwrapEnvelope(payload: unknown): Record<string, unknown> | null {
  const root = asRecord(payload);
  if (!root) {
    return null;
  }
  if ("data" in root) {
    return asRecord(root.data);
  }
  return root;
}

function extractApiMessage(payload: unknown): string {
  const root = asRecord(payload);
  if (!root) {
    return "";
  }
  const detail = asString(root.detail);
  if (detail) {
    return detail;
  }
  const error = asRecord(root.error);
  if (!error) {
    return "";
  }
  return asString(error.message) || asString(error.detail);
}

function normalizeRequestStatus(value: unknown): LeaveSphereQuickApprovalRequestStatus {
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

function normalizeDecision(value: unknown): LeaveSphereQuickApprovalDecision | null {
  const normalized = asString(value).toLowerCase();
  if (normalized === "approved" || normalized === "approve") {
    return "approved";
  }
  if (normalized === "rejected" || normalized === "reject") {
    return "rejected";
  }
  return null;
}

function normalizeViewState(value: unknown): LeaveSphereQuickApprovalViewState {
  const normalized = asString(value).toLowerCase();
  if (normalized === "expired" || normalized === "token_expired") {
    return "expired";
  }
  if (
    normalized === "already_handled"
    || normalized === "already-approved"
    || normalized === "already_rejected"
    || normalized === "handled"
  ) {
    return "already_handled";
  }
  if (normalized === "invalid" || normalized === "invalid_token") {
    return "invalid";
  }
  return "ready";
}

function parsePreview(candidate: Record<string, unknown> | null): LeaveSphereQuickApprovalPreview | null {
  if (!candidate) {
    return null;
  }
  const request = asRecord(candidate.preview) || asRecord(candidate.request) || candidate;
  if (!request) {
    return null;
  }
  const employeeName = asString(request.employeeName) || asString(request.employee) || "Employee";
  const ptoTypeCode = asString(request.ptoTypeCode) || asString(request.ptoType) || null;
  const ptoTypeLabel = resolveQuickApprovalPtoTypeLabel(ptoTypeCode, request.ptoTypeLabel);
  const startDate = asString(request.startDate);
  const endDate = asString(request.endDate);
  if (!startDate || !endDate) {
    return null;
  }
  return {
    requestId: asString(request.requestId) || asString(request.id) || null,
    employeeName,
    ptoTypeCode,
    ptoTypeLabel,
    startDate,
    endDate,
    hoursRequested: asNumber(request.hoursRequested) ?? asNumber(request.hours),
    daysRequested: asNumber(request.daysRequested) ?? asNumber(request.days),
    reason: asString(request.reason) || asString(request.note) || null,
    currentStatus: normalizeRequestStatus(request.currentStatus ?? request.status),
  };
}

function normalizeCanAct(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  return true;
}

function mapDecisionError(status: number, payload: unknown): LeaveSphereQuickApprovalError {
  const message = extractApiMessage(payload).toLowerCase();
  if (status === 410 || message.includes("expired")) {
    return new LeaveSphereQuickApprovalError("expired", "This quick approval link has expired.");
  }
  if (
    message.includes("already")
    || message.includes("not pending")
    || message.includes("handled")
  ) {
    return new LeaveSphereQuickApprovalError("already_handled", "This request has already been handled.");
  }
  if (status === 404 || message.includes("invalid") || message.includes("not found") || message.includes("token")) {
    return new LeaveSphereQuickApprovalError("invalid", "This quick approval link is invalid.");
  }
  return new LeaveSphereQuickApprovalError("generic", "We couldn't complete this action right now.");
}

function isMissingRouteResponse(response: ApiResponse): boolean {
  if (response.status !== 404) {
    return false;
  }
  return extractApiMessage(response.payload).toLowerCase() === "not found";
}

function shouldUseMockFallback(): boolean {
  const explicit = asString(import.meta.env.VITE_LEAVESPHERE_PUBLIC_APPROVAL_MOCK).toLowerCase();
  if (explicit === "0" || explicit === "false" || explicit === "no") {
    return false;
  }
  if (explicit === "1" || explicit === "true" || explicit === "yes") {
    return true;
  }
  return true;
}

function isTokenSemanticErrorMessage(message: string): boolean {
  const normalized = message.toLowerCase();
  return (
    normalized.includes("invalid")
    || normalized.includes("expired")
    || normalized.includes("already")
    || normalized.includes("not pending")
  );
}

function isMockFallbackEligibleError(response: ApiResponse): boolean {
  const status = response.status;
  const message = extractApiMessage(response.payload).toLowerCase();
  if (status === 404) {
    return true;
  }
  if (![400, 401, 403, 405].includes(status)) {
    return false;
  }
  if (isTokenSemanticErrorMessage(message)) {
    return false;
  }
  return (
    !message
    || message.includes("x-tenant-id")
    || message.includes("tenant")
    || message.includes("api key")
    || message.includes("unauthorized")
    || message.includes("forbidden")
    || message.includes("permission")
    || message.includes("not found")
    || message.includes("method not allowed")
  );
}

async function requestPublicJson(path: string, options: RequestInit): Promise<ApiResponse> {
  let response: Response;
  try {
    response = await fetch(path, options);
  } catch {
    throw new LeaveSphereQuickApprovalError("generic", "Network issue detected. Please try again.");
  }

  const payload = await response.json().catch(() => null);
  return {
    status: response.status,
    payload,
  };
}

function mockStateFromToken(token: string): LeaveSphereQuickApprovalViewState {
  const normalized = token.toLowerCase();
  if (normalized.includes("expired")) {
    return "expired";
  }
  if (normalized.includes("handled") || normalized.includes("approved") || normalized.includes("rejected")) {
    return "already_handled";
  }
  if (normalized.includes("invalid") || normalized.includes("bad")) {
    return "invalid";
  }
  return "ready";
}

function buildMockPreview(): LeaveSphereQuickApprovalPreview {
  return {
    requestId: "demo-pto-request-1",
    employeeName: "Jordan Rivera",
    ptoTypeCode: "PTO",
    ptoTypeLabel: "Paid Time Off",
    startDate: "2026-06-18",
    endDate: "2026-06-20",
    hoursRequested: 24,
    daysRequested: 3,
    reason: "Family travel already booked.",
    currentStatus: "pending",
  };
}

async function loadMockDetail(token: string): Promise<LeaveSphereQuickApprovalLoadResult> {
  await wait(MOCK_DELAY_MS);
  const state = mockStateFromToken(token);
  if (state === "ready") {
    return {
      state,
      preview: buildMockPreview(),
      handledDecision: null,
      canAct: true,
      source: "mock",
      message: "Preview mode: backend public approval endpoints are not available in this environment.",
    };
  }
  return {
    state,
    preview: null,
    handledDecision: state === "already_handled" ? "approved" : null,
    canAct: false,
    source: "mock",
    message: "Preview mode: backend public approval endpoints are not available in this environment.",
  };
}

async function submitMockDecision(
  token: string,
  decision: LeaveSphereQuickApprovalDecision,
): Promise<LeaveSphereQuickApprovalDecisionResult> {
  await wait(MOCK_DELAY_MS);
  const state = mockStateFromToken(token);
  if (state === "invalid") {
    return { state: "invalid", decision: null, source: "mock", message: "This quick approval link is invalid." };
  }
  if (state === "expired") {
    return { state: "expired", decision: null, source: "mock", message: "This quick approval link has expired." };
  }
  if (state === "already_handled") {
    return { state: "already_handled", decision: null, source: "mock", message: "This request has already been handled." };
  }
  return {
    state: "success",
    decision,
    source: "mock",
    message: "Preview mode: action stored in mock flow only.",
  };
}

export async function loadLeaveSphereQuickApproval(token: string): Promise<LeaveSphereQuickApprovalLoadResult> {
  const path = `${PUBLIC_APPROVAL_BASE}/${encodeURIComponent(token)}`;
  const response = await requestPublicJson(path, { method: "GET" });

  if (shouldUseMockFallback() && (isMissingRouteResponse(response) || isMockFallbackEligibleError(response))) {
    return loadMockDetail(token);
  }

  if (isMissingRouteResponse(response)) {
    throw new LeaveSphereQuickApprovalError("generic", "Quick approval service is unavailable.");
  }

  if (response.status >= 400) {
    throw mapDecisionError(response.status, response.payload);
  }

  const body = unwrapEnvelope(response.payload);
  const state = normalizeViewState(body?.state ?? body?.tokenState ?? body?.status);
  const handledDecision = normalizeDecision(body?.decision ?? body?.handledDecision);
  const preview = parsePreview(body);
  const canAct = normalizeCanAct(body?.canAct);
  if (state === "ready" && !preview) {
    throw new LeaveSphereQuickApprovalError("generic", "The quick approval request data is unavailable.");
  }
  return {
    state,
    preview,
    handledDecision,
    canAct,
    source: "api",
    message: null,
  };
}

export async function submitLeaveSphereQuickApprovalDecision(args: {
  token: string;
  decision: LeaveSphereQuickApprovalDecision;
  reason?: string;
}): Promise<LeaveSphereQuickApprovalDecisionResult> {
  const { token, decision, reason } = args;
  const path = `${PUBLIC_APPROVAL_BASE}/${encodeURIComponent(token)}/${decision === "approved" ? "approve" : "reject"}`;
  const response = await requestPublicJson(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      reason: asString(reason) || undefined,
    }),
  });

  if (shouldUseMockFallback() && (isMissingRouteResponse(response) || isMockFallbackEligibleError(response))) {
    return submitMockDecision(token, decision);
  }

  if (isMissingRouteResponse(response)) {
    throw new LeaveSphereQuickApprovalError("generic", "Quick approval service is unavailable.");
  }

  if (response.status >= 400) {
    const mapped = mapDecisionError(response.status, response.payload);
    if (mapped.kind === "invalid") {
      return { state: "invalid", decision: null, source: "api", message: mapped.message };
    }
    if (mapped.kind === "expired") {
      return { state: "expired", decision: null, source: "api", message: mapped.message };
    }
    if (mapped.kind === "already_handled") {
      return { state: "already_handled", decision: null, source: "api", message: mapped.message };
    }
    throw mapped;
  }

  return {
    state: "success",
    decision,
    source: "api",
    message: null,
  };
}
