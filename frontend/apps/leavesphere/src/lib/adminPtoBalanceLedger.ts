import type { LeaveSpherePtoBalance } from "@leavesphere/lib/ptoMocks";

export type LeaveSphereAdminPtoActionCode = "load_grant" | "adjustment";
export type LeaveSphereAdminPtoTransactionStatus = "Pending" | "Approved" | "Rejected" | "Canceled";
export type LeaveSphereAdminAdjustmentDirection = "increase" | "decrease";

export type LeaveSphereAdminPtoTransaction = {
  id: string;
  employeeId: string;
  ptoTypeCode: string;
  ptoActionCode: LeaveSphereAdminPtoActionCode;
  hours: number;
  year: number;
  status: LeaveSphereAdminPtoTransactionStatus;
  approverNote: string | null;
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
  type: string;
  label: string;
  totalHours: number;
  usedHours: number;
  scheduledHours: number;
  remainingHours?: number;
};

export type LeaveSphereAdminEmployeeBalanceUsageRow = {
  employeeId: string;
  employeeName: string;
  balances: LeaveSphereAdminEmployeeBalanceUsageItem[];
};

const PTO_TYPE_LABELS: Record<string, string> = {
  vacation: "Vacation",
  sick: "Sick",
  personal: "Personal",
  floating: "Floating Holiday",
};

function makeBalanceKey(employeeId: string, type: string): string {
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
    balances: row.balances.map((balance) => ({
      type: balance.type,
      label: balance.label || PTO_TYPE_LABELS[balance.type] || balance.type,
      totalHours: asFiniteNumber(balance.totalHours),
      usedHours: asFiniteNumber(balance.usedHours),
      scheduledHours: asFiniteNumber(balance.scheduledHours),
      remainingHours: typeof balance.remainingHours === "number" && Number.isFinite(balance.remainingHours)
        ? balance.remainingHours
        : undefined,
    })),
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
          approverNote: "Opening PTO balance load",
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

  return params.usageRows.map((row) => {
    const typeOrder = new Map<string, number>();
    row.balances.forEach((balance, index) => {
      typeOrder.set(balance.type, index);
    });
    for (const transaction of params.transactions) {
      if (transaction.employeeId !== row.employeeId || transaction.year !== params.year) {
        continue;
      }
      if (!typeOrder.has(transaction.ptoTypeCode)) {
        typeOrder.set(transaction.ptoTypeCode, typeOrder.size);
      }
    }

    const balances = [...typeOrder.keys()].map((type) => {
      const usage = row.balances.find((item) => item.type === type);
      const balanceKey = makeBalanceKey(row.employeeId, type);
      const totalHours = grantedHoursByKey.has(balanceKey)
        ? grantedHoursByKey.get(balanceKey) || 0
        : asFiniteNumber(usage?.totalHours);
      const usedHours = asFiniteNumber(usage?.usedHours);
      const scheduledHours = asFiniteNumber(usage?.scheduledHours);
      return {
        type,
        label: usage?.label || PTO_TYPE_LABELS[type] || type,
        totalHours,
        usedHours,
        scheduledHours,
      };
    });

    balances.sort((left, right) => {
      const leftIndex = typeOrder.get(left.type) ?? 0;
      const rightIndex = typeOrder.get(right.type) ?? 0;
      if (leftIndex !== rightIndex) {
        return leftIndex - rightIndex;
      }
      return left.type.localeCompare(right.type);
    });

    return {
      employeeId: row.employeeId,
      employeeName: row.employeeName,
      balances,
    };
  });
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
  ptoTypeCode: string;
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
