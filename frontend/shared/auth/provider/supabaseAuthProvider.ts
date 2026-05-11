import type { AuthProviderAdapter, AuthStateChangeEvent } from "./types";
import {
  getUser as getSupabaseUser,
  refreshSession as refreshSupabaseSession,
  signInWithPassword as supabaseSignInWithPassword,
  signUpWithPassword as supabaseSignUpWithPassword,
} from "../supabaseClient";
import type { SupabaseSession } from "../types";

export class SupabaseAuthProvider implements AuthProviderAdapter {
  readonly name = "supabase";

  async signInWithPassword(email: string, password: string) {
    return supabaseSignInWithPassword(email, password);
  }

  async signUpWithPassword(email: string, password: string) {
    return supabaseSignUpWithPassword(email, password);
  }

  async refreshSession(session: SupabaseSession): Promise<SupabaseSession | null> {
    const refreshToken = String(session.refreshToken || "").trim();
    if (!refreshToken) {
      return null;
    }
    return refreshSupabaseSession(refreshToken);
  }

  async signOut(): Promise<void> {
    // Session is local-only in Phase 1; clearing is handled by AuthProvider state.
  }

  async getSession(): Promise<SupabaseSession | null> {
    return null;
  }

  onAuthStateChange(callback: (event: AuthStateChangeEvent, session: SupabaseSession | null) => void): () => void {
    // Supabase JS client is intentionally not used in Phase 1 frontend auth.
    void callback;
    return () => {};
  }

  async getUser(accessToken: string) {
    return getSupabaseUser(accessToken);
  }

  getAccessToken(session: SupabaseSession | null): string | null {
    return session?.accessToken || null;
  }
}
