import { useCallback, useEffect, useRef } from "react";

import { useToast } from "@/components/ui/toast";
import { buildAuthHeaders } from "@shared/api/authHeaders";
import { useAuth } from "@shared/auth/useAuth";

export type ApiMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";

export type ToastConfig =
  | boolean
  | string
  | {
      title: string;
      message?: string;
    };

export type ApiRequestOptions = {
  method?: ApiMethod;
  headers?: HeadersInit;
  body?: unknown;
  successToast?: ToastConfig;
  errorToast?: ToastConfig;
};

const DISABLED_ACCOUNT_NOTICE = "Your account has been disabled. Contact your workspace administrator.";

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

function extractApiError(payload: unknown): string | null {
  if (!isRecord(payload)) {
    return null;
  }

  const error = isRecord(payload.error) ? payload.error : null;
  if (error) {
    return asString(error.message) || asString(error.detail) || null;
  }

  return asString(payload.detail) || null;
}

function extractApiErrorCode(payload: unknown): string | null {
  if (!isRecord(payload)) {
    return null;
  }

  const error = isRecord(payload.error) ? payload.error : null;
  const errorCode = asString(error?.code);
  if (errorCode) {
    return errorCode.toLowerCase();
  }

  const detail = error?.detail;
  if (isRecord(detail)) {
    const detailCode = asString(detail.code);
    if (detailCode) {
      return detailCode.toLowerCase();
    }
  }

  const payloadDetail = payload.detail;
  if (isRecord(payloadDetail)) {
    const detailCode = asString(payloadDetail.code);
    if (detailCode) {
      return detailCode.toLowerCase();
    }
  }
  return null;
}

function isDisabledAccountResponse(status: number, code: string | null, message: string): boolean {
  if (status !== 403) {
    return false;
  }
  if (code === "tenant_membership_disabled") {
    return true;
  }
  return message.toLowerCase().includes("account has been disabled");
}

function getToastContent(
  config: ToastConfig,
  defaults: { title: string; message?: string },
): { title: string; message?: string } | null {
  if (config === false) {
    return null;
  }
  if (config === true) {
    return defaults;
  }
  if (typeof config === "string") {
    return { title: config, message: defaults.message };
  }
  return {
    title: config.title || defaults.title,
    message: config.message ?? defaults.message,
  };
}

async function parseResponsePayload(response: Response): Promise<unknown> {
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    return response.json().catch(() => null);
  }
  if (contentType.includes("text/")) {
    return response.text().catch(() => null);
  }
  return response.arrayBuffer().catch(() => null);
}

export function useApiRequest() {
  const toast = useToast();
  const auth = useAuth();
  const authRef = useRef(auth);

  useEffect(() => {
    authRef.current = auth;
  }, [auth]);

  const requestJson = useCallback(
    async (url: string, options: ApiRequestOptions = {}): Promise<unknown> => {
      const method = options.method ?? "GET";
      const hasBody = options.body !== undefined;
      const isFormData = typeof FormData !== "undefined" && options.body instanceof FormData;
      // Keep workspace access fresh on each user action without blocking request execution.
      authRef.current.refreshAccessProfile();
      const ensuredSession = await authRef.current.ensureFreshSession();
      const baseHeaders = buildAuthHeaders(ensuredSession, authRef.current.tenantSlug, false);
      const requestHeaders = new Headers(baseHeaders);
      const callerHeaders = new Headers(options.headers ?? {});
      callerHeaders.forEach((value, key) => requestHeaders.set(key, value));

      let body: BodyInit | undefined;
      if (hasBody) {
        if (isFormData) {
          body = options.body as FormData;
        } else {
          if (!requestHeaders.has("Content-Type")) {
            requestHeaders.set("Content-Type", "application/json");
          }
          body = JSON.stringify(options.body);
        }
      }

      let response: Response;
      try {
        response = await fetch(url, {
          method,
          headers: requestHeaders,
          body,
        });
      } catch {
        const message =
          typeof navigator !== "undefined" && !navigator.onLine
            ? "You're offline. Reconnect to continue."
            : "Network request failed. Please try again.";
        const errorToast = getToastContent(options.errorToast ?? true, {
          title: "Request failed",
          message,
        });
        if (errorToast) {
          toast.error(errorToast.title, errorToast.message);
        }
        throw new Error(message);
      }

      const payload = await parseResponsePayload(response);
      if (!response.ok) {
        const message = extractApiError(payload) || `Request failed with status ${response.status}.`;
        const code = extractApiErrorCode(payload);

        if (isDisabledAccountResponse(response.status, code, message)) {
          await authRef.current.signOut({ notice: DISABLED_ACCOUNT_NOTICE });
          throw new Error(DISABLED_ACCOUNT_NOTICE);
        }
        if (response.status === 401) {
          await authRef.current.signOut();
          throw new Error("Your session has expired. Please sign in again.");
        }

        const errorToast = getToastContent(options.errorToast ?? true, {
          title: "Request failed",
          message,
        });
        if (errorToast) {
          toast.error(errorToast.title, errorToast.message);
        }
        throw new Error(message);
      }

      const defaultSuccessToast = method !== "GET";
      const successToast = getToastContent(options.successToast ?? defaultSuccessToast, {
        title: "Request succeeded",
        message: "Action completed successfully.",
      });
      if (successToast) {
        toast.success(successToast.title, successToast.message);
      }

      return payload;
    },
    [toast],
  );

  return { requestJson };
}
