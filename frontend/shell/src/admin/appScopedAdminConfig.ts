export type AppScopedAdminSectionKey = "user_access" | "tradsphere_permission_details";

const DEFAULT_ADMIN_SECTIONS: AppScopedAdminSectionKey[] = ["user_access"];

const APP_SCOPED_ADMIN_SECTION_OVERRIDES: Record<string, AppScopedAdminSectionKey[]> = {
  tradsphere: ["user_access", "tradsphere_permission_details"],
};

export function resolveAppScopedAdminSections(appCode: string): AppScopedAdminSectionKey[] {
  const normalizedAppCode = String(appCode || "").trim().toLowerCase();
  const configured = APP_SCOPED_ADMIN_SECTION_OVERRIDES[normalizedAppCode] ?? DEFAULT_ADMIN_SECTIONS;
  const unique = new Set(configured);
  return Array.from(unique);
}
