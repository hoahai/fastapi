export type SupabaseSession = {
  accessToken: string;
  refreshToken?: string | null;
  expiresAt?: number | null;
};

export type AuthUser = {
  id: string;
  email: string | null;
  fullName?: string | null;
  firstName?: string | null;
  lastName?: string | null;
};

export type AccessProfile = {
  user: AuthUser;
  tenant: {
    id: string;
    slug: string;
  };
  app: {
    id: string;
    code: string;
  };
  role: string;
  permissions: string[];
};

export type AuthStatus = "idle" | "loading" | "authenticated" | "unauthenticated";
