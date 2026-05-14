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
  app?: {
    id: string;
    code: string;
  } | null;
  role?: string | null;
  assignments?: Array<{
    tenant: {
      id: string;
      slug: string;
      name?: string | null;
      status?: string | null;
    };
    app: {
      id: string;
      code: string;
      name?: string | null;
    };
    role: string;
  }>;
  permissions: string[];
  appAccess?: Array<{
    app: {
      id: string;
      code: string;
      name?: string | null;
    };
    role: string;
    permissions?: string[];
  }>;
  scope?: {
    isSuperAdmin?: boolean;
    hasAnyAdminScope?: boolean;
  };
};

export type AuthStatus = "idle" | "loading" | "authenticated" | "unauthenticated";
