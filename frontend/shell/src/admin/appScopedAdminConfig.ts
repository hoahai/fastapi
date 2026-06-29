export type AppScopedAdminSectionKey = "user_access" | "page_permissions";

const DEFAULT_ADMIN_SECTIONS: AppScopedAdminSectionKey[] = ["user_access", "page_permissions"];

export function resolveAppScopedAdminSections(appCode: string): AppScopedAdminSectionKey[] {
  void appCode;
  const configured = DEFAULT_ADMIN_SECTIONS;
  const unique = new Set(configured);
  return Array.from(unique);
}
