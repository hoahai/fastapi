import type { LeaveSpherePtoBalance, LeaveSpherePtoType } from "@leavesphere/lib/ptoMocks";

export type LeaveSphereAdminPtoActionCode = "load_grant" | "adjustment";
export type LeaveSphereAdminPtoTransactionStatus = "Pending" | "Approved" | "Rejected" | "Canceled";
export type LeaveSphereAdminAdjustmentDirection = "increase" | "decrease";

export type LeaveSphereAdminPtoTransaction = {
  id: string;
  employeeId: string;
  ptoTypeCode: LeaveSpherePtoType;
  ptoActionCode: LeaveSphereAdminPtoActionCode;
  hours: number;
  year: number;
  status: LeaveSphereAdminPtoTransactionStatus;
  note: string | null;
  createdAt: string;
  createdByName: string | null;
};

export type LeaveSphereAdminLoadRequest = LeaveSphereAdminPtoTransaction;

export type LeaveSphereAdminEmployeeBalance = {
  employeeId: string;
  employeeName: string;
  balances: LeaveSpherePtoBalance[];
};

export type LeaveSphereAdminEmployeeBalanceUsageItem = {
  type: LeaveSpherePtoType;
  label: string;
  usedHours: number;
  scheduledHours: number;
};

export type LeaveSphereAdminEmployeeBalanceUsageRow = {
  employeeId: string;
  employeeName: string;
  balances: LeaveSphereAdminEmployeeBalanceUsageItem[];
};

const PTO_TYPES: LeaveSpherePtoType[] = ["vacation", "sick", "personal", "floating"];

const PTO_TYPE_LABELS: Record<LeaveSpherePtoType, string> = {
  vacation: "Vacation",
  sick: "Sick",
  personal: "Personal",
  floating: "Floating Holiday",
};

function makeBalanceKey(employeeId: string, type: LeaveSpherePtoType): string {
  return `${employeeId}::${type}`;
}

function asFiniteNumber(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

export function buildLeaveSphereAdminBalanceUsageRows(
  employeeBalances: LeaveSphereAdminEmployeeBalance[],
): LeaveSphereAdminEmployeeBalanceUsageRow[] {
  return employeeBalances.map((row) => ({
    employeeId: row.employeeId,
    employeeName: row.employeeName,
    balances: PTO_TYPES.map((type) => {
      const balance = row.balances.find((item) => item.type === type);
      return {
        type,
        label: balance?.label || PTO_TYPE_LABELS[type],
        usedHours: asFiniteNumber(balance?.usedHours),
        scheduledHours: asFiniteNumber(balance?.scheduledHours),
      };
    }),
  }));
}

export function seedLeaveSphereAdminBalanceTransactions(params: {
  employeeBalances: LeaveSphereAdminEmployeeBalance[];
  years: number[];
  createdAt: string;
  createdByName: string | null;
}): LeaveSphereAdminPtoTransaction[] {
  const transactions: LeaveSphereAdminPtoTransaction[] = [];

  for (const year of params.years) {
    for (const row of params.employeeBalances) {
      for (const balance of row.balances) {
        const totalHours = asFiniteNumber(balance.totalHours);
        if (totalHours === 0) {
          continue;
        }
        transactions.push({
          id: `seed-load-${row.employeeId}-${balance.type}-${year}`,
          employeeId: row.employeeId,
          ptoTypeCode: balance.type,
          ptoActionCode: "load_grant",
          hours: totalHours,
          year,
          status: "Approved",
          note: "Opening PTO balance load",
          createdAt: params.createdAt,
          createdByName: params.createdByName,
        });
      }
    }
  }

  return transactions;
}

export function deriveLeaveSphereAdminEmployeeBalances(params: {
  usageRows: LeaveSphereAdminEmployeeBalanceUsageRow[];
  transactions: LeaveSphereAdminPtoTransaction[];
  year: number;
}): LeaveSphereAdminEmployeeBalance[] {
  const grantedHoursByKey = new Map<string, number>();

  for (const transaction of params.transactions) {
    if (transaction.year !== params.year || transaction.status !== "Approved") {
      continue;
    }
    const key = makeBalanceKey(transaction.employeeId, transaction.ptoTypeCode);
    grantedHoursByKey.set(key, (grantedHoursByKey.get(key) || 0) + asFiniteNumber(transaction.hours));
  }

  return params.usageRows.map((row) => ({
    employeeId: row.employeeId,
    employeeName: row.employeeName,
    balances: PTO_TYPES.map((type) => {
      const usage = row.balances.find((item) => item.type === type);
      return {
        type,
        label: usage?.label || PTO_TYPE_LABELS[type],
        totalHours: grantedHoursByKey.get(makeBalanceKey(row.employeeId, type)) || 0,
        usedHours: asFiniteNumber(usage?.usedHours),
        scheduledHours: asFiniteNumber(usage?.scheduledHours),
      };
    }),
  }));
}

export function resolveLeaveSphereAdminTransactionHours(params: {
  ptoActionCode: LeaveSphereAdminPtoActionCode;
  hours: number;
  direction?: LeaveSphereAdminAdjustmentDirection;
}): number {
  const absoluteHours = Math.abs(asFiniteNumber(params.hours));
  if (params.ptoActionCode === "adjustment" && params.direction === "decrease") {
    return absoluteHours * -1;
  }
  return absoluteHours;
}

export function getLeaveSphereAdminLoadRequests(params: {
  transactions: LeaveSphereAdminPtoTransaction[];
  employeeId: string;
  ptoTypeCode: LeaveSpherePtoType;
  year: number;
}): LeaveSphereAdminLoadRequest[] {
  return params.transactions
    .filter((transaction) => (
      transaction.employeeId === params.employeeId
      && transaction.ptoTypeCode === params.ptoTypeCode
      && transaction.year === params.year
      && transaction.status === "Approved"
      && transaction.ptoActionCode === "load_grant"
    ))
    .slice()
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt));
}
