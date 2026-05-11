import { SupabaseAuthProvider } from "./supabaseAuthProvider";
import type { AuthProviderAdapter } from "./types";

function resolveProviderName(): string {
  return String(import.meta.env.VITE_AUTH_PROVIDER || "supabase").trim().toLowerCase();
}

export function createAuthProvider(): AuthProviderAdapter {
  const providerName = resolveProviderName();
  if (providerName !== "supabase") {
    throw new Error(`Unsupported auth provider: ${providerName}`);
  }
  return new SupabaseAuthProvider();
}
