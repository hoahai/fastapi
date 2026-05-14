import type { AuthProviderAdapter, AuthStateChangeEvent } from "./types";
import {
  getUser as getSupabaseUser,
  refreshSession as refreshSupabaseSession,
  sendPasswordResetEmail as sendSupabasePasswordResetEmail,
  signInWithPassword as supabaseSignInWithPassword,
  signUpWithPassword as supabaseSignUpWithPassword,
  updatePassword as updateSupabasePassword,
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

  async updatePassword(accessToken: string, newPassword: string): Promise<void> {
    await updateSupabasePassword(accessToken, newPassword);
  }

  async sendPasswordResetEmail(email: string, options?: { redirectTo?: string }): Promise<void> {
    await sendSupabasePasswordResetEmail(email, options?.redirectTo);
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
