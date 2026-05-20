import { useMemo } from "react";

import { SectionCard } from "@shared/components";
import { roleLabel } from "@shared/auth/accessAssignments";

type ScopedUser = {
  userId: string;
  email: string | null;
  fullName: string | null;
  role: string | null;
};

type TradspherePermissionDetailsSectionProps = {
  users: ScopedUser[];
  loadingUsers: boolean;
  appLabel: string;
};

type MatrixRow = {
  capability: string;
  viewer: boolean;
  editor: boolean;
  admin: boolean;
  note: string;
};

const PERMISSION_MATRIX: MatrixRow[] = [
  {
    capability: "View pages",
    viewer: true,
    editor: true,
    admin: true,
    note: "Covers app pages available under viewer scope.",
  },
  {
    capability: "Edit records",
    viewer: false,
    editor: true,
    admin: true,
    note: "Covers edit actions across forms and workflow pages.",
  },
  {
    capability: "Manage app user access",
    viewer: false,
    editor: false,
    admin: true,
    note: "Controls app-scoped admin membership actions.",
  },
];

function normalizeRole(role: string | null | undefined): "viewer" | "editor" | "admin" {
  const normalized = String(role || "").trim().toLowerCase();
  if (normalized === "admin") {
    return "admin";
  }
  if (normalized === "editor") {
    return "editor";
  }
  return "viewer";
}

function rolePriority(role: "viewer" | "editor" | "admin"): number {
  if (role === "admin") {
    return 0;
  }
  if (role === "editor") {
    return 1;
  }
  return 2;
}

function yesNo(value: boolean): string {
  return value ? "Yes" : "No";
}

export function TradspherePermissionDetailsSection({
  users,
  loadingUsers,
  appLabel,
}: TradspherePermissionDetailsSectionProps) {
  const sortedUsers = useMemo(() => {
    return [...users].sort((left, right) => {
      const leftRole = normalizeRole(left.role);
      const rightRole = normalizeRole(right.role);
      const roleCmp = rolePriority(leftRole) - rolePriority(rightRole);
      if (roleCmp !== 0) {
        return roleCmp;
      }
      const leftName = String(left.fullName || left.email || left.userId).trim().toLowerCase();
      const rightName = String(right.fullName || right.email || right.userId).trim().toLowerCase();
      return leftName.localeCompare(rightName);
    });
  }, [users]);

  return (
    <SectionCard
      title={`${appLabel} Permission Details`}
      description="This section is app-specific and can be enabled only for selected apps."
      className="p-5"
    >
      <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50/70 p-3">
        <p className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Role Capability Matrix</p>
        <div className="mt-2 overflow-x-auto">
          <table className="min-w-full text-left text-sm text-slate-700">
            <thead>
              <tr className="text-xs uppercase tracking-[0.08em] text-slate-500">
                <th className="px-2 py-2 font-semibold">Capability</th>
                <th className="px-2 py-2 font-semibold">Viewer</th>
                <th className="px-2 py-2 font-semibold">Editor</th>
                <th className="px-2 py-2 font-semibold">Admin</th>
                <th className="px-2 py-2 font-semibold">Notes</th>
              </tr>
            </thead>
            <tbody>
              {PERMISSION_MATRIX.map((row) => (
                <tr key={row.capability} className="border-t border-slate-200">
                  <td className="px-2 py-2 font-medium text-slate-900">{row.capability}</td>
                  <td className="px-2 py-2">{yesNo(row.viewer)}</td>
                  <td className="px-2 py-2">{yesNo(row.editor)}</td>
                  <td className="px-2 py-2">{yesNo(row.admin)}</td>
                  <td className="px-2 py-2 text-xs text-slate-600">{row.note}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="mt-3 rounded-xl border border-slate-200 bg-white p-3">
        <p className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Current User Role Coverage</p>
        {loadingUsers ? (
          <p className="mt-2 text-sm text-slate-600">Loading users...</p>
        ) : sortedUsers.length === 0 ? (
          <p className="mt-2 text-sm text-slate-600">No users found for this app scope.</p>
        ) : (
          <div className="mt-2 overflow-x-auto">
            <table className="min-w-full text-left text-sm text-slate-700">
              <thead>
                <tr className="text-xs uppercase tracking-[0.08em] text-slate-500">
                  <th className="px-2 py-2 font-semibold">User</th>
                  <th className="px-2 py-2 font-semibold">Role</th>
                  <th className="px-2 py-2 font-semibold">Page-Level Profile</th>
                </tr>
              </thead>
              <tbody>
                {sortedUsers.map((user) => {
                  const normalizedRole = normalizeRole(user.role);
                  const pageProfile = normalizedRole === "admin"
                    ? "Full access, including app-scoped admin actions."
                    : normalizedRole === "editor"
                      ? "View + edit actions on allowed pages."
                      : "Read-only page access.";
                  return (
                    <tr key={user.userId} className="border-t border-slate-200">
                      <td className="px-2 py-2 text-slate-900">{user.fullName || user.email || user.userId}</td>
                      <td className="px-2 py-2">{roleLabel(normalizedRole)}</td>
                      <td className="px-2 py-2 text-xs text-slate-600">{pageProfile}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </SectionCard>
  );
}
