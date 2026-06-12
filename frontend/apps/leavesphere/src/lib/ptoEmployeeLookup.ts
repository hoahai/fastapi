export type LeaveSpherePtoEmployeeLookupItem = {
  employeeId: string;
  employeeName: string;
  pictureUrl?: string | null;
};

export type LeaveSpherePtoEmployeeDisplay = {
  employeeName: string;
  pictureUrl: string | null;
};

function normalizeText(value: string): string {
  return value.trim();
}

export function buildLeaveSpherePtoEmployeeLookup(
  employees: Iterable<LeaveSpherePtoEmployeeLookupItem> | null | undefined,
): Map<string, LeaveSpherePtoEmployeeDisplay> {
  const lookup = new Map<string, LeaveSpherePtoEmployeeDisplay>();
  for (const employee of employees ?? []) {
    const employeeId = normalizeText(employee.employeeId);
    const employeeName = normalizeText(employee.employeeName);
    if (!employeeId || !employeeName || lookup.has(employeeId)) {
      continue;
    }
    lookup.set(employeeId, {
      employeeName,
      pictureUrl: employee.pictureUrl ?? null,
    });
  }
  return lookup;
}

export function resolveLeaveSpherePtoEmployeeDisplay(
  lookup: Map<string, LeaveSpherePtoEmployeeDisplay>,
  employeeId: string,
): LeaveSpherePtoEmployeeDisplay {
  const resolved = lookup.get(normalizeText(employeeId));
  if (resolved) {
    return resolved;
  }
  return {
    employeeName: normalizeText(employeeId),
    pictureUrl: null,
  };
}
