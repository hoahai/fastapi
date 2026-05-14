import type { AuthUser, SupabaseSession } from "../types";

export type AuthStateChangeEvent = "SIGNED_IN" | "SIGNED_OUT" | "TOKEN_REFRESHED";

export type SignUpWithPasswordResult = {
  session: SupabaseSession | null;
  user: AuthUser | null;
};

export interface AuthProviderAdapter {
  readonly name: string;
  signInWithPassword(email: string, password: string): Promise<{ session: SupabaseSession; user: AuthUser }>;
  signUpWithPassword(email: string, password: string): Promise<SignUpWithPasswordResult>;
  updatePassword(accessToken: string, newPassword: string): Promise<void>;
  sendPasswordResetEmail(email: string, options?: { redirectTo?: string }): Promise<void>;
  refreshSession(session: SupabaseSession): Promise<SupabaseSession | null>;
  signOut(): Promise<void>;
  getSession(): Promise<SupabaseSession | null>;
  onAuthStateChange(callback: (event: AuthStateChangeEvent, session: SupabaseSession | null) => void): () => void;
  getUser(accessToken: string): Promise<AuthUser>;
  getAccessToken(session: SupabaseSession | null): string | null;
}
