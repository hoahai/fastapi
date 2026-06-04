const OFFICE_EXT_RE = /^(?<base>.+?)(?:\s*x(?<ext>\d{1,6}))?$/i;
const PHONE_ALLOWED_RE = /^[\d\s().+-]+$/;

function isValidUsPhoneBase(value: string): boolean {
  if (!value || !PHONE_ALLOWED_RE.test(value)) {
    return false;
  }
  if (value.includes("+") && !value.trim().startsWith("+")) {
    return false;
  }
  if (value.split("+").length > 2) {
    return false;
  }
  const digits = value.replace(/\D/g, "");
  if (digits.length === 10) {
    return true;
  }
  return digits.length === 11 && digits.startsWith("1");
}

function formatUsPhoneBase(value: string): string | null {
  const digits = value.replace(/\D/g, "");
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits.startsWith("1")) {
    const local = digits.slice(1);
    return `(${local.slice(0, 3)}) ${local.slice(3, 6)}-${local.slice(6)}`;
  }
  return null;
}

export type UsPhoneFieldValidationOptions = {
  field: string;
  maxLength: number;
  allowExtension: boolean;
};

export function normalizeUsPhoneDisplay(value: string, allowExtension: boolean): string {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }

  if (allowExtension) {
    const match = OFFICE_EXT_RE.exec(text);
    if (!match || typeof match.groups?.base !== "string") {
      return text;
    }
    const base = match.groups.base.trim();
    const ext = typeof match.groups.ext === "string" ? match.groups.ext : "";
    const formattedBase = formatUsPhoneBase(base);
    if (!formattedBase) {
      return text;
    }
    return ext ? `${formattedBase} x${ext}` : formattedBase;
  }

  const formatted = formatUsPhoneBase(text);
  return formatted ?? text;
}

export function normalizeUsPhoneOnInput(value: string, allowExtension: boolean): string {
  const text = String(value ?? "").replace(/\s+/g, " ").trimStart();
  if (!text) {
    return "";
  }

  if (allowExtension) {
    const match = OFFICE_EXT_RE.exec(text);
    if (!match || typeof match.groups?.base !== "string") {
      return text;
    }
    const base = match.groups.base.trim();
    const ext = typeof match.groups.ext === "string" ? match.groups.ext : "";
    const formattedBase = formatUsPhoneBase(base);
    if (!formattedBase) {
      return text;
    }
    return ext ? `${formattedBase} x${ext}` : formattedBase;
  }

  const formatted = formatUsPhoneBase(text);
  return formatted ?? text;
}

export function validateUsPhoneField(
  value: string,
  options: UsPhoneFieldValidationOptions,
): string | null {
  const text = String(value ?? "").trim();
  if (!text) {
    return null;
  }
  if (text.length > options.maxLength) {
    return `${options.field} must be <= ${options.maxLength} characters.`;
  }
  if (options.allowExtension) {
    const match = OFFICE_EXT_RE.exec(text);
    if (!match || typeof match.groups?.base !== "string") {
      return `${options.field} must be a US phone format; optional extension x####.`;
    }
    if (!isValidUsPhoneBase(match.groups.base.trim())) {
      return `${options.field} must be all digits (10/11) or valid US phone format.`;
    }
    return null;
  }
  if (/\bx\d+\s*$/i.test(text)) {
    return `${options.field} cannot include extension; use Office for x####.`;
  }
  if (!isValidUsPhoneBase(text)) {
    return `${options.field} must be all digits (10/11) or valid US phone format.`;
  }
  return null;
}
