function normalizeEmailValue(value: string): string {
  return String(value ?? "").trim().toLowerCase();
}

export function splitEmailList(raw: string): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const part of String(raw ?? "").split(/[;,\n]+/g)) {
    const email = normalizeEmailValue(part);
    if (!email || seen.has(email)) {
      continue;
    }
    seen.add(email);
    output.push(email);
  }
  return output;
}

export function mergeUniqueEmails(base: string[], additions: string[]): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const item of [...base, ...additions]) {
    const normalized = normalizeEmailValue(item);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

export function isLikelyEmailAddress(value: string): boolean {
  const normalized = normalizeEmailValue(value);
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized);
}
