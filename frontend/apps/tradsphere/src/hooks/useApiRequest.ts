import { useCallback } from "react";

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

  const requestJson = useCallback(
    async (url: string, options: ApiRequestOptions = {}): Promise<unknown> => {
      const method = options.method ?? "GET";
      const hasBody = options.body !== undefined;
      const isFormData = typeof FormData !== "undefined" && options.body instanceof FormData;
      const baseHeaders = buildAuthHeaders(auth.session, auth.tenantSlug, false);
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

      const response = await fetch(url, {
        method,
        headers: requestHeaders,
        body,
      });

      const payload = await parseResponsePayload(response);
      if (!response.ok) {
        const message = extractApiError(payload) || `Request failed with status ${response.status}.`;
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
    [auth.session, auth.tenantSlug, toast],
  );

  return { requestJson };
}
