/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_SUPABASE_URL?: string;
  readonly VITE_SUPABASE_ANON_KEY?: string;
  readonly VITE_AUTH_REDIRECT_URL?: string;
  readonly VITE_DEFAULT_POST_LOGIN_ROUTE?: string;
  readonly VITE_DEFAULT_TENANT_SLUG?: string;
  readonly VITE_AUTH_MODE?: string;
  readonly VITE_AUTH_ENABLE_LEGACY_API_KEY_FALLBACK?: string;
  readonly VITE_AUTH_PROTECT_TRADSPHERE?: string;
  readonly VITE_LEGACY_API_KEY?: string;
  readonly VITE_LEGACY_USER_NAME?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
