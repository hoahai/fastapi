export const PASSWORD_RULE_MESSAGE = "Password must be at least 8 characters and include at least one letter and one number.";
export const MIN_PASSWORD_LENGTH = 8;

export function hasPasswordLetter(value: string): boolean {
  return /[A-Za-z]/.test(String(value || ""));
}

export function hasPasswordDigit(value: string): boolean {
  return /\d/.test(String(value || ""));
}

export function isPasswordComplexEnough(value: string): boolean {
  const text = String(value || "");
  return text.length >= MIN_PASSWORD_LENGTH && hasPasswordLetter(text) && hasPasswordDigit(text);
}

export function validatePasswordAgainstPolicy(value: string): string | null {
  const text = String(value || "");
  if (!text) {
    return "New password is required.";
  }
  if (!isPasswordComplexEnough(text)) {
    return PASSWORD_RULE_MESSAGE;
  }
  return null;
}
