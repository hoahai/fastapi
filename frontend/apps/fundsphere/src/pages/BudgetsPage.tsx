import { Fragment, useEffect, useMemo, useRef, useState, type CSSProperties, type FormEvent, type KeyboardEvent } from "react";
import { ChevronDown, ChevronUp, RefreshCw, Search, X } from "lucide-react";

import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { Section } from "@shared/components/layout/Section";
import { SectionHeader } from "@shared/components/layout/SectionHeader";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { SearchEmptyStatePanel } from "@shared/components/status/SearchEmptyStatePanel";
import {
  FormRow,
  ModalCacheFooter,
  ModalCloseButton,
  ModalFooter,
  ModalHeaderRow,
  ModalShell,
  ReadOnlyField,
} from "@shared/components";
import { shouldFetchNetwork, type CachePolicy } from "@shared/cache";
import { shouldProtectFrontendAuth } from "@shared/auth/guards";
import { hasAppEditAccess } from "@shared/auth/permissions";
import { useAuth } from "@shared/auth/useAuth";
import { useApiRequest } from "@shared/hooks/useApiRequest";
import { useOnlineStatus } from "@shared/hooks/useOnlineStatus";
import { useScopedPersistentState } from "@shared/hooks/useScopedPersistentState";
import { cn } from "@shared/components/utils/cn";
import { PageBanner } from "@shell/components/layout/PageBanner";
import { Button } from "@tradsphere/components/ui/button";
import { AppDropdown, type AppDropdownOption } from "@tradsphere/components/ui/app-dropdown";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@tradsphere/components/ui/dialog";
import { Input } from "@tradsphere/components/ui/input";
import { Textarea } from "@tradsphere/components/ui/textarea";
import { UnsavedChangesDialog } from "@tradsphere/components/ui/unsaved-changes-dialog";

import { loadFundsphereAccounts, type FundsphereAccount } from "@fundsphere/lib/accountsApi";
import {
  buildFundsphereBudgetsMatrixCacheKey,
  readFundsphereBudgetDetailCacheSnapshot,
  readFundsphereBudgetAccountsCacheSnapshot,
  readFundsphereBudgetsMatrixCacheSnapshot,
  syncFundsphereBudgetAccountsCache,
  syncFundsphereBudgetDetailCache,
  syncFundsphereBudgetsMatrixCache,
  FUNDSPHERE_BUDGETS_PAGE_CODE,
  type FundsphereBudgetsCacheContext,
  type FundsphereBudgetsCacheCriteria,
} from "@fundsphere/lib/budgetsCache";
import {
  createFundsphereBudget,
  type FundsphereRequestJson,
  loadFundsphereBudgetDetail,
  loadFundsphereBudgetMatrix,
  normalizeFundsphereBudgetForm,
  updateFundsphereBudget,
  type FundsphereBudgetCellContext,
  type FundsphereBudgetFormState,
  type FundsphereBudgetMatrixResponse,
  type FundsphereBudgetMatrixRow,
} from "@fundsphere/lib/budgetsApi";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type BudgetMode = "create" | "edit";

type BudgetSearchCriteria = FundsphereBudgetsCacheCriteria;

type PersistedBudgetsPageState = {
  searchDraft: BudgetSearchCriteria;
  searchCriteria: BudgetSearchCriteria;
  hasSearched: boolean;
  departmentOpenState?: Record<string, boolean>;
  serviceOpenState?: Record<string, boolean>;
  selectedCell?: PersistedBudgetCellSelectionState;
};

type PersistedBudgetCellSelectionState = {
  accountCode: string;
  accountName: string;
  month: number;
  year: number;
  serviceId: string;
  serviceName: string;
  departmentCode: string;
  departmentName: string;
  subService: string;
  mode: BudgetMode;
};

type BudgetCellValue = {
  row: FundsphereBudgetMatrixRow | null;
  cents: number;
};

type BudgetHierarchyLevel = 0 | 1 | 2;

type BudgetColumn = {
  key: string;
  accountCode: string;
  accountName: string;
  month: number;
  year: number;
  label: string;
};

type BudgetDetailRow = {
  key: string;
  departmentCode: string;
  departmentName: string;
  departmentListingOrder: number | null;
  serviceId: string;
  serviceName: string;
  subService: string;
  cells: Record<string, BudgetCellValue>;
  totalCentsByColumn: Record<string, number>;
  totalCents: number;
};

type BudgetServiceGroup = {
  key: string;
  departmentCode: string;
  departmentName: string;
  departmentListingOrder: number | null;
  serviceId: string;
  serviceName: string;
  detailRows: BudgetDetailRow[];
  totalCentsByColumn: Record<string, number>;
  totalCents: number;
};

type BudgetDepartmentGroup = {
  key: string;
  departmentCode: string;
  departmentName: string;
  departmentListingOrder: number | null;
  serviceGroups: BudgetServiceGroup[];
  totalCentsByColumn: Record<string, number>;
  totalCents: number;
};

type BudgetCellContext = FundsphereBudgetCellContext & {
  budgetId: string | null;
  budgetRow: FundsphereBudgetMatrixRow | null;
};

type BudgetModalProps = {
  open: boolean;
  mode: BudgetMode;
  canEdit: boolean;
  busy: boolean;
  budgetCell: BudgetCellContext | null;
  requestJson: FundsphereRequestJson;
  cacheContext: FundsphereBudgetsCacheContext;
  isOnline: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (payload: {
    mode: BudgetMode;
    budgetId: string | null;
    identity: FundsphereBudgetCellContext;
    form: FundsphereBudgetFormState;
    changeReason: string;
  }) => Promise<void>;
};

const FUNDSPHERE_APP_CODE = "fundsphere";
const DEFAULT_SEARCH_CRITERIA: BudgetSearchCriteria = {
  accountCodes: [],
  periods: [],
};
const EMPTY_PAGE_STATE: PersistedBudgetsPageState = {
  searchDraft: { ...DEFAULT_SEARCH_CRITERIA },
  searchCriteria: { ...DEFAULT_SEARCH_CRITERIA },
  hasSearched: false,
};
const CURRENCY_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return "";
}

function formatRelativeTime(timestamp: number): string {
  const ageMs = Math.max(0, Date.now() - timestamp);
  if (ageMs < 60_000) {
    return "just now";
  }
  const minutes = Math.floor(ageMs / 60_000);
  if (minutes < 60) {
    return `${minutes} min ago`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours} hr ago`;
  }
  const days = Math.floor(hours / 24);
  return `${days} day${days === 1 ? "" : "s"} ago`;
}

function normalizeSelectionList(values: string[]): string[] {
  const normalized = Array.from(
    new Set(
      (values ?? [])
        .map((value) => asString(value).toUpperCase())
        .filter(Boolean),
    ),
  );
  normalized.sort((left, right) => left.localeCompare(right));
  return normalized;
}

function normalizePeriodSelectionList(values: string[], periodOrder: Map<string, number>): string[] {
  const normalized = Array.from(
    new Set(
      (values ?? [])
        .map((value) => asString(value))
        .filter(Boolean),
    ),
  );
  normalized.sort((left, right) => {
    const leftIndex = periodOrder.get(left) ?? Number.MAX_SAFE_INTEGER;
    const rightIndex = periodOrder.get(right) ?? Number.MAX_SAFE_INTEGER;
    if (leftIndex !== rightIndex) {
      return leftIndex - rightIndex;
    }
    return left.localeCompare(right);
  });
  return normalized;
}

function hasBudgetSearchCriteria(criteria: BudgetSearchCriteria): boolean {
  return Boolean(criteria.accountCodes.length || criteria.periods.length);
}

function areBudgetSearchCriteriaEqual(left: BudgetSearchCriteria, right: BudgetSearchCriteria): boolean {
  const leftAccounts = normalizeSelectionList(left.accountCodes);
  const rightAccounts = normalizeSelectionList(right.accountCodes);
  const leftPeriods = [...left.periods];
  const rightPeriods = [...right.periods];
  return (
    leftAccounts.length === rightAccounts.length &&
    leftAccounts.every((value, index) => value === rightAccounts[index]) &&
    leftPeriods.length === rightPeriods.length &&
    leftPeriods.every((value, index) => value === rightPeriods[index])
  );
}

function normalizeBooleanRecord(value: unknown): Record<string, boolean> {
  if (!isRecord(value)) {
    return {};
  }
  const normalized: Record<string, boolean> = {};
  for (const [key, nextValue] of Object.entries(value)) {
    if (typeof nextValue === "boolean") {
      normalized[key] = nextValue;
    }
  }
  return normalized;
}

function normalizeBudgetCellSelectionState(value: unknown): PersistedBudgetCellSelectionState | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const accountCode = asString(value.accountCode).toUpperCase();
  const accountName = asString(value.accountName);
  const month = Number(value.month);
  const year = Number(value.year);
  const serviceId = asString(value.serviceId);
  const serviceName = asString(value.serviceName);
  const departmentCode = asString(value.departmentCode).toUpperCase();
  const departmentName = asString(value.departmentName);
  const subService = asString(value.subService);
  const mode = value.mode === "edit" || value.mode === "create" ? value.mode : null;

  if (
    !accountCode ||
    !accountName ||
    !Number.isFinite(month) ||
    !Number.isFinite(year) ||
    !serviceId ||
    !serviceName ||
    !departmentCode ||
    !departmentName ||
    !mode
  ) {
    return undefined;
  }

  return {
    accountCode,
    accountName,
    month: Math.trunc(month),
    year: Math.trunc(year),
    serviceId,
    serviceName,
    departmentCode,
    departmentName,
    subService,
    mode,
  };
}

function getMonthLabel(month: number): string {
  return new Intl.DateTimeFormat("en-US", { month: "long" }).format(new Date(2026, month - 1, 1));
}

function formatPeriodLabel(month: number, year: number): string {
  return `${getMonthLabel(month)}'${String(year).slice(-2)}`;
}

function formatCurrency(value: unknown): string {
  const numeric = Number(asString(value));
  if (!Number.isFinite(numeric)) {
    return "—";
  }
  return CURRENCY_FORMATTER.format(numeric);
}

function toCents(value: unknown): number {
  const numeric = Number(asString(value));
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return Math.round(numeric * 100);
}

function centsToCurrency(cents: number): string {
  return CURRENCY_FORMATTER.format((cents || 0) / 100);
}

function buildBudgetPeriodOptions(): AppDropdownOption[] {
  const now = new Date();
  const options: AppDropdownOption[] = [];
  for (let offset = 3; offset >= -12; offset -= 1) {
    const date = new Date(now.getFullYear(), now.getMonth() + offset, 1);
    const month = date.getMonth() + 1;
    const year = date.getFullYear();
    options.push({
      value: `${month}/${year}`,
      label: formatPeriodLabel(month, year),
    });
  }
  return options;
}

function buildBudgetAccountOptions(accounts: FundsphereAccount[]): AppDropdownOption[] {
  return [...accounts]
    .sort((left, right) => {
      if (left.active !== right.active) {
        return left.active ? -1 : 1;
      }
      return left.code.localeCompare(right.code);
    })
    .map((account) => ({
      value: account.code.toUpperCase(),
      label: `${account.code.toUpperCase()} - ${account.name}`,
      muted: !account.active,
    }));
}

function createEmptyBudgetForm(): FundsphereBudgetFormState {
  return {
    grossAmount: "",
    commission: "",
    netAdjustment: "",
    note: "",
  };
}

function normalizeBudgetFormValue(value: string): string {
  const text = asString(value);
  if (!text) {
    return "";
  }
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed.toFixed(2) : text;
}

function budgetFormFromRow(row: FundsphereBudgetMatrixRow | null): FundsphereBudgetFormState {
  if (!row) {
    return createEmptyBudgetForm();
  }
  return normalizeFundsphereBudgetForm(row);
}

function buildColumnKey(accountCode: string, month: number, year: number): string {
  return `${accountCode.toUpperCase()}::${month}/${year}`;
}

function buildBudgetCellSelectionKey(cell: {
  accountCode: string;
  month: number;
  year: number;
  serviceId: string;
  subService: string;
}): string {
  return [cell.accountCode.toUpperCase(), cell.month, cell.year, cell.serviceId, cell.subService].join("::");
}

function buildBudgetColumns(
  accountsByCode: Map<string, FundsphereAccount>,
  selectedAccountCodes: string[],
  selectedPeriods: string[],
): BudgetColumn[] {
  const columns: BudgetColumn[] = [];
  for (const accountCode of selectedAccountCodes) {
    const account = accountsByCode.get(accountCode.toUpperCase()) ?? null;
    const accountName = account ? account.name : accountCode.toUpperCase();
    for (const period of selectedPeriods) {
      const [monthText, yearText] = period.split("/");
      const month = Number(monthText);
      const year = Number(yearText);
      if (!month || !year) {
        continue;
      }
      columns.push({
        key: buildColumnKey(accountCode, month, year),
        accountCode: accountCode.toUpperCase(),
        accountName,
        month,
        year,
        label: formatPeriodLabel(month, year),
      });
    }
  }
  return columns;
}

function buildBudgetMatrixHierarchy(
  rows: FundsphereBudgetMatrixRow[],
  columns: BudgetColumn[],
): {
  departments: BudgetDepartmentGroup[];
  grandTotalsByColumn: Record<string, number>;
  grandTotalCents: number;
} {
  const detailByKey = new Map<string, BudgetDetailRow>();

  for (const row of rows) {
    const detailKey = [row.departmentCode.toUpperCase(), row.serviceId, row.subService].join("::");
    const columnKey = buildColumnKey(row.accountCode, row.month, row.year);
    const existing = detailByKey.get(detailKey);
    if (!existing) {
      detailByKey.set(detailKey, {
        key: detailKey,
        departmentCode: row.departmentCode.toUpperCase(),
        departmentName: row.departmentName,
        departmentListingOrder: row.departmentListingOrder,
        serviceId: row.serviceId,
        serviceName: row.serviceName,
        subService: row.subService,
        cells: {
          [columnKey]: {
            row,
            cents: toCents(row.grossAmount),
          },
        },
        totalCentsByColumn: {},
        totalCents: 0,
      });
      continue;
    }
    existing.cells[columnKey] = {
      row,
      cents: toCents(row.grossAmount),
    };
  }

  const detailRows = Array.from(detailByKey.values()).map((detail) => {
    const totalCentsByColumn: Record<string, number> = {};
    let totalCents = 0;
    for (const column of columns) {
      const cell = detail.cells[column.key];
      const cents = cell?.cents ?? 0;
      totalCentsByColumn[column.key] = cents;
      totalCents += cents;
    }
    return {
      ...detail,
      totalCentsByColumn,
      totalCents,
    };
  });

  detailRows.sort((left, right) => {
    const departmentOrderLeft = left.departmentListingOrder ?? Number.MAX_SAFE_INTEGER;
    const departmentOrderRight = right.departmentListingOrder ?? Number.MAX_SAFE_INTEGER;
    if (departmentOrderLeft !== departmentOrderRight) {
      return departmentOrderLeft - departmentOrderRight;
    }
    const departmentComparison = left.departmentName.localeCompare(right.departmentName);
    if (departmentComparison !== 0) {
      return departmentComparison;
    }
    const serviceComparison = left.serviceName.localeCompare(right.serviceName);
    if (serviceComparison !== 0) {
      return serviceComparison;
    }
    return left.subService.localeCompare(right.subService);
  });

  const serviceMap = new Map<string, BudgetServiceGroup>();
  for (const detail of detailRows) {
    const serviceKey = [detail.departmentCode, detail.serviceId].join("::");
    const existing = serviceMap.get(serviceKey);
    if (!existing) {
      serviceMap.set(serviceKey, {
        key: serviceKey,
        departmentCode: detail.departmentCode,
        departmentName: detail.departmentName,
        departmentListingOrder: detail.departmentListingOrder,
        serviceId: detail.serviceId,
        serviceName: detail.serviceName,
        detailRows: [detail],
        totalCentsByColumn: {},
        totalCents: 0,
      });
      continue;
    }
    existing.detailRows.push(detail);
  }

  const serviceGroups = Array.from(serviceMap.values()).map((serviceGroup) => {
    const totalCentsByColumn: Record<string, number> = {};
    let totalCents = 0;
    for (const column of columns) {
      const cents = serviceGroup.detailRows.reduce((sum, detail) => sum + (detail.totalCentsByColumn[column.key] ?? 0), 0);
      totalCentsByColumn[column.key] = cents;
      totalCents += cents;
    }
    return {
      ...serviceGroup,
      totalCentsByColumn,
      totalCents,
    };
  });

  serviceGroups.sort((left, right) => {
    const departmentOrderLeft = left.departmentListingOrder ?? Number.MAX_SAFE_INTEGER;
    const departmentOrderRight = right.departmentListingOrder ?? Number.MAX_SAFE_INTEGER;
    if (departmentOrderLeft !== departmentOrderRight) {
      return departmentOrderLeft - departmentOrderRight;
    }
    const departmentComparison = left.departmentName.localeCompare(right.departmentName);
    if (departmentComparison !== 0) {
      return departmentComparison;
    }
    const serviceComparison = left.serviceName.localeCompare(right.serviceName);
    if (serviceComparison !== 0) {
      return serviceComparison;
    }
    return left.serviceId.localeCompare(right.serviceId);
  });

  const departmentMap = new Map<string, BudgetDepartmentGroup>();
  for (const serviceGroup of serviceGroups) {
    const departmentKey = serviceGroup.departmentCode;
    const existing = departmentMap.get(departmentKey);
    if (!existing) {
      departmentMap.set(departmentKey, {
        key: departmentKey,
        departmentCode: serviceGroup.departmentCode,
        departmentName: serviceGroup.departmentName,
        departmentListingOrder: serviceGroup.departmentListingOrder,
        serviceGroups: [serviceGroup],
        totalCentsByColumn: {},
        totalCents: 0,
      });
      continue;
    }
    existing.serviceGroups.push(serviceGroup);
  }

  const departments = Array.from(departmentMap.values()).map((departmentGroup) => {
    const totalCentsByColumn: Record<string, number> = {};
    let totalCents = 0;
    for (const column of columns) {
      const cents = departmentGroup.serviceGroups.reduce(
        (sum, serviceGroup) => sum + (serviceGroup.totalCentsByColumn[column.key] ?? 0),
        0,
      );
      totalCentsByColumn[column.key] = cents;
      totalCents += cents;
    }
    return {
      ...departmentGroup,
      totalCentsByColumn,
      totalCents,
    };
  });

  departments.sort((left, right) => {
    const departmentOrderLeft = left.departmentListingOrder ?? Number.MAX_SAFE_INTEGER;
    const departmentOrderRight = right.departmentListingOrder ?? Number.MAX_SAFE_INTEGER;
    if (departmentOrderLeft !== departmentOrderRight) {
      return departmentOrderLeft - departmentOrderRight;
    }
    const departmentComparison = left.departmentName.localeCompare(right.departmentName);
    if (departmentComparison !== 0) {
      return departmentComparison;
    }
    return left.departmentCode.localeCompare(right.departmentCode);
  });

  const grandTotalsByColumn: Record<string, number> = {};
  let grandTotalCents = 0;
  for (const column of columns) {
    const cents = departments.reduce((sum, departmentGroup) => sum + (departmentGroup.totalCentsByColumn[column.key] ?? 0), 0);
    grandTotalsByColumn[column.key] = cents;
    grandTotalCents += cents;
  }

  return {
    departments,
    grandTotalsByColumn,
    grandTotalCents,
  };
}

function upsertBudgetRowIntoMatrix(
  matrix: FundsphereBudgetMatrixResponse | null,
  row: FundsphereBudgetMatrixRow,
): FundsphereBudgetMatrixResponse | null {
  if (!matrix) {
    return matrix;
  }

  const identityMatches = (candidate: FundsphereBudgetMatrixRow) =>
    candidate.accountCode.toUpperCase() === row.accountCode.toUpperCase() &&
    candidate.year === row.year &&
    candidate.month === row.month &&
    candidate.serviceId === row.serviceId &&
    candidate.subService === row.subService;

  const nextRows = [...matrix.rows];
  const index = nextRows.findIndex(identityMatches);
  if (index >= 0) {
    nextRows[index] = row;
  } else {
    nextRows.push(row);
  }

  return {
    ...matrix,
    rows: nextRows,
    rowCount: nextRows.length,
  };
}

function isPersistedBudgetsPageState(value: unknown): value is PersistedBudgetsPageState {
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.hasSearched !== "boolean" || !isRecord(value.searchDraft) || !isRecord(value.searchCriteria)) {
    return false;
  }
  return (
    Array.isArray(value.searchDraft.accountCodes) &&
    Array.isArray(value.searchDraft.periods) &&
    Array.isArray(value.searchCriteria.accountCodes) &&
    Array.isArray(value.searchCriteria.periods) &&
    (value.selectedCell === undefined || normalizeBudgetCellSelectionState(value.selectedCell) !== undefined)
  );
}

function BudgetMatrixCellButton({
  value,
  title,
  onClick,
  align = "center",
  selected = false,
}: {
  value: BudgetCellValue | null;
  title: string;
  onClick: () => void;
  align?: "center" | "right";
  selected?: boolean;
}) {
  return (
    <button
      type="button"
      className={cn(
        "flex w-full items-center rounded-md px-2 py-2 text-sm transition",
        align === "right" ? "justify-end text-right" : "justify-center text-center",
        value?.row
          ? "bg-white/85 font-medium text-slate-800 hover:bg-blue-50 hover:text-blue-800"
          : "bg-slate-50/80 text-slate-400 hover:bg-blue-50 hover:text-blue-800",
        selected
          ? "ring-2 ring-inset ring-blue-500/70 bg-blue-50/90 text-blue-900 shadow-[0_0_0_1px_rgba(59,130,246,0.25)]"
          : null,
      )}
      title={title}
      onClick={onClick}
    >
      {value ? centsToCurrency(value.cents) : "—"}
    </button>
  );
}

function BudgetHierarchyLabel({
  level,
  title,
  subtitle,
  open,
  onToggle,
  emphasis = "default",
}: {
  level: BudgetHierarchyLevel;
  title: string;
  subtitle?: string | null;
  open?: boolean;
  onToggle?: () => void;
  emphasis?: "default" | "muted" | "subtle";
}) {
  const titleClass =
    level === 0
      ? "text-sm font-semibold text-slate-900"
      : level === 1
        ? "text-sm font-medium text-slate-800"
        : "text-sm font-medium text-slate-700";
  const subtitleClass =
    emphasis === "default"
      ? "text-[11px] uppercase tracking-[0.12em] text-slate-500"
      : emphasis === "muted"
        ? "text-[11px] uppercase tracking-[0.12em] text-slate-400"
      : "text-[10px] uppercase tracking-[0.11em] text-slate-400";

  return (
    <div className="flex min-w-0 items-start gap-2 text-left">
      {typeof onToggle === "function" ? (
        <span
          className={cn(
            "inline-flex shrink-0 items-center justify-center text-slate-500",
            level === 0 ? "h-4 w-4" : level === 1 ? "h-4 w-4" : "h-3.5 w-3.5",
          )}
          aria-hidden="true"
        >
          {open ? <ChevronDown className="size-3.5 shrink-0" /> : <ChevronUp className="size-3.5 shrink-0" />}
        </span>
      ) : null}
      <div className="min-w-0">
        <p className={cn("whitespace-normal break-words leading-tight tracking-[-0.01em]", titleClass)}>{title}</p>
        {subtitle ? <p className={cn("whitespace-normal break-words leading-tight", subtitleClass)}>{subtitle}</p> : null}
      </div>
    </div>
  );
}

function BudgetMatrixBoundaryDivider() {
  return (
    <span
      aria-hidden="true"
      className="pointer-events-none absolute right-0 top-0 z-20 h-full w-[2px] bg-slate-400"
    />
  );
}

function BudgetMatrixAccountHeaderLabel({
  accountCode,
  accountName,
  useAccountName,
}: {
  accountCode: string;
  accountName?: string | null;
  useAccountName: boolean;
}) {
  return (
    <span className="block whitespace-normal break-words leading-tight tracking-[0.02em]">
      {useAccountName && accountName ? accountName : accountCode.toUpperCase()}
    </span>
  );
}

function handleHierarchyCellKeyDown(
  event: KeyboardEvent<HTMLTableCellElement>,
  onToggle: () => void,
) {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    onToggle();
  }
}

const BUDGET_MATRIX_DEPARTMENT_COLUMN_CLASS = "sticky left-0 z-50 w-[180px] min-w-[180px] max-w-[180px]";
const BUDGET_MATRIX_SERVICE_COLUMN_CLASS = "sticky left-[180px] z-40 w-[220px] min-w-[220px] max-w-[220px]";
const BUDGET_MATRIX_SEGMENT_COLUMN_CLASS = "sticky left-[400px] z-30 w-[220px] min-w-[220px] max-w-[220px]";
const BUDGET_MATRIX_PERIOD_WIDTH = 104;
const BUDGET_MATRIX_ACCOUNT_HEADER_HEIGHT = 48;
const BUDGET_MATRIX_PERIOD_HEADER_HEIGHT = 40;
const BUDGET_MATRIX_HEADER_TOTAL_HEIGHT = BUDGET_MATRIX_ACCOUNT_HEADER_HEIGHT + BUDGET_MATRIX_PERIOD_HEADER_HEIGHT;
const BUDGET_MATRIX_PERIOD_COLUMN_CLASS = "w-[104px] min-w-[104px] max-w-[104px]";
const BUDGET_MATRIX_FROZEN_HEADER_CLASS = `bg-slate-50/95 border-b border-slate-200 px-2 py-2 text-left text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500 h-[${BUDGET_MATRIX_HEADER_TOTAL_HEIGHT}px]`;
const BUDGET_MATRIX_ACCOUNT_HEADER_CLASS = `border-b border-slate-200 px-2 py-0 text-center text-[12px] font-semibold uppercase tracking-[0.12em] text-slate-900 whitespace-normal break-words h-[${BUDGET_MATRIX_ACCOUNT_HEADER_HEIGHT}px]`;
const BUDGET_MATRIX_PERIOD_HEADER_CLASS = `border-b border-slate-200 px-2 py-0 text-center text-[11px] font-semibold text-slate-700 whitespace-nowrap h-[${BUDGET_MATRIX_PERIOD_HEADER_HEIGHT}px]`;

const BUDGET_MATRIX_DEPARTMENT_COLUMN_STYLE: CSSProperties = {
  position: "sticky",
  left: 0,
  zIndex: 80,
  width: 180,
  minWidth: 180,
  maxWidth: 180,
  backgroundColor: "#f8fafc",
  boxShadow: "inset -1px 0 0 0 #e2e8f0",
};

const BUDGET_MATRIX_SERVICE_COLUMN_STYLE: CSSProperties = {
  position: "sticky",
  left: 180,
  zIndex: 70,
  width: 220,
  minWidth: 220,
  maxWidth: 220,
  backgroundColor: "#ffffff",
  boxShadow: "inset -1px 0 0 0 #e2e8f0",
};

const BUDGET_MATRIX_SEGMENT_COLUMN_STYLE: CSSProperties = {
  position: "sticky",
  left: 400,
  zIndex: 60,
  width: 220,
  minWidth: 220,
  maxWidth: 220,
  backgroundColor: "#ffffff",
  boxShadow: "inset -1px 0 0 0 #e2e8f0",
};

const BUDGET_MATRIX_ACCOUNT_HEADER_STYLE: CSSProperties = {
  position: "sticky",
  top: 0,
  zIndex: 50,
  backgroundColor: "#f8fafc",
  color: "#0f172a",
  height: BUDGET_MATRIX_ACCOUNT_HEADER_HEIGHT,
  minHeight: BUDGET_MATRIX_ACCOUNT_HEADER_HEIGHT,
  maxHeight: BUDGET_MATRIX_ACCOUNT_HEADER_HEIGHT,
};

const BUDGET_MATRIX_PERIOD_HEADER_STYLE: CSSProperties = {
  position: "sticky",
  top: BUDGET_MATRIX_ACCOUNT_HEADER_HEIGHT,
  zIndex: 49,
  backgroundColor: "#f1f5f9",
  color: "#334155",
  height: BUDGET_MATRIX_PERIOD_HEADER_HEIGHT,
  minHeight: BUDGET_MATRIX_PERIOD_HEADER_HEIGHT,
  maxHeight: BUDGET_MATRIX_PERIOD_HEADER_HEIGHT,
};

function BudgetModal({
  open,
  mode,
  canEdit,
  busy,
  budgetCell,
  requestJson,
  cacheContext,
  isOnline,
  onOpenChange,
  onSubmit,
}: BudgetModalProps) {
  const [detailRow, setDetailRow] = useState<FundsphereBudgetMatrixRow | null>(budgetCell?.budgetRow ?? null);
  const [detailCacheStatus, setDetailCacheStatus] = useState<CacheStatus | null>(null);
  const [isDetailRefreshing, setIsDetailRefreshing] = useState(false);
  const [form, setForm] = useState<FundsphereBudgetFormState>(createEmptyBudgetForm());
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [changeReason, setChangeReason] = useState("");
  const [isReasonDialogOpen, setIsReasonDialogOpen] = useState(false);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);

  const currentRow = detailRow ?? budgetCell?.budgetRow ?? null;
  const currentBudgetId = currentRow?.budgetId ?? budgetCell?.budgetId ?? null;
  const baseline = useMemo(() => budgetFormFromRow(currentRow), [currentRow]);
  const baselineKey = useMemo(() => {
    if (!budgetCell) {
      return "none";
    }
    return [
      budgetCell.accountCode,
      budgetCell.year,
      budgetCell.month,
      budgetCell.serviceId,
      budgetCell.subService,
      currentBudgetId ?? "new",
    ].join("::");
  }, [budgetCell, currentBudgetId]);

  useEffect(() => {
    if (!open) {
      return;
    }
    setDetailRow(budgetCell?.budgetRow ?? null);
    setDetailCacheStatus(null);
    setIsDetailRefreshing(false);
    setForm(baseline);
    setSubmitError(null);
    setChangeReason("");
    setIsReasonDialogOpen(false);
    setIsDiscardDialogOpen(false);
  }, [baseline, baselineKey, open]);

  useEffect(() => {
    if (!open || mode !== "edit" || !currentBudgetId) {
      return;
    }
    void refreshBudgetDetail("cache-first");
  }, [currentBudgetId, mode, open]);

  const isDirty = useMemo(() => {
    const normalizedCurrent = {
      grossAmount: normalizeBudgetFormValue(form.grossAmount),
      commission: normalizeBudgetFormValue(form.commission),
      netAdjustment: normalizeBudgetFormValue(form.netAdjustment),
      note: asString(form.note),
    };
    const normalizedBaseline = {
      grossAmount: normalizeBudgetFormValue(baseline.grossAmount),
      commission: normalizeBudgetFormValue(baseline.commission),
      netAdjustment: normalizeBudgetFormValue(baseline.netAdjustment),
      note: asString(baseline.note),
    };
    return JSON.stringify(normalizedCurrent) !== JSON.stringify(normalizedBaseline);
  }, [baseline, form]);

  const grossAmountValid = Number.isFinite(Number(asString(form.grossAmount))) && asString(form.grossAmount).trim().length > 0;
  const canSubmit = canEdit && grossAmountValid && isDirty && !busy;
  const canRevert = canEdit && isDirty && !busy;
  const currentNetAmountText = currentRow?.budgetId
    ? formatCurrency(currentRow.netAmount)
    : "Calculated after save";

  function updateForm(field: keyof FundsphereBudgetFormState, nextValue: string) {
    setForm((current) => ({
      ...current,
      [field]: nextValue,
    }));
    setSubmitError(null);
  }

  function restoreBaseline() {
    setForm(baseline);
    setSubmitError(null);
    setChangeReason("");
    setIsReasonDialogOpen(false);
  }

  async function refreshBudgetDetail(policy: CachePolicy): Promise<void> {
    if (mode !== "edit" || !currentBudgetId) {
      setDetailCacheStatus(null);
      setIsDetailRefreshing(false);
      return;
    }

    const snapshot = readFundsphereBudgetDetailCacheSnapshot(cacheContext, currentBudgetId);
    const cachedBudget = snapshot?.data ?? null;
    const shouldFetch = shouldFetchNetwork(policy, snapshot);

    if (cachedBudget) {
      setDetailRow(cachedBudget);
      setDetailCacheStatus({
        source: "cache",
        fetchedAt: snapshot?.fetchedAt ?? Date.now(),
      });
    }

    if (!shouldFetch) {
      setIsDetailRefreshing(false);
      return;
    }

    if (!isOnline) {
      setIsDetailRefreshing(false);
      return;
    }

    setIsDetailRefreshing(true);
    try {
      const nextBudget = await loadFundsphereBudgetDetail({
        requestJson,
        budgetId: currentBudgetId,
      });
      if (!nextBudget) {
        return;
      }
      setDetailRow(nextBudget);
      const fetchedAt = Date.now();
      setDetailCacheStatus({
        source: "network",
        fetchedAt,
      });
      syncFundsphereBudgetDetailCache(cacheContext, nextBudget, { source: "network", fetchedAt });
    } finally {
      setIsDetailRefreshing(false);
    }
  }

  function handleDialogOpenChange(nextOpen: boolean) {
    if (nextOpen) {
      onOpenChange(true);
      return;
    }
    if (busy) {
      return;
    }
    if (isDirty) {
      setIsDiscardDialogOpen(true);
      return;
    }
    onOpenChange(false);
  }

  async function handleConfirmSubmit() {
    if (!budgetCell) {
      return;
    }
    setSubmitError(null);
    try {
      await onSubmit({
        mode,
        budgetId: currentBudgetId,
        identity: {
          accountCode: budgetCell.accountCode,
          accountName: budgetCell.accountName,
          month: budgetCell.month,
          year: budgetCell.year,
          serviceId: budgetCell.serviceId,
          serviceName: budgetCell.serviceName,
          departmentCode: budgetCell.departmentCode,
          departmentName: budgetCell.departmentName,
          subService: budgetCell.subService,
        },
        form,
        changeReason: asString(changeReason),
      });
      setIsReasonDialogOpen(false);
      onOpenChange(false);
    } catch (error) {
      setSubmitError(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not save budget.");
    }
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          className="max-h-[92vh] max-w-[740px] overflow-hidden rounded-xl bg-white p-6"
          onInteractOutside={(event) => {
            if (busy || isReasonDialogOpen || isDirty) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={busy} busyMessage={mode === "create" ? "Creating budget..." : "Saving budget..."} className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close budget modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>{mode === "create" ? "Add Budget" : "Edit Budget"}</DialogTitle>
                <DialogDescription>
                  {mode === "create"
                    ? "Create a budget cell for the selected account, period, and service."
                    : "Update the selected budget cell and preserve the identity of the row."}
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>

            <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1 space-y-4">
              <Section className="space-y-3">
                <SectionHeader
                  title="Budget Identity"
                  description="These values identify the row and stay fixed for the selected cell."
                />

                <FormRow label="Account">
                  <ReadOnlyField value={budgetCell ? `${budgetCell.accountCode} - ${budgetCell.accountName}` : "-"} />
                </FormRow>
                <FormRow label="Period">
                  <ReadOnlyField value={budgetCell ? `${budgetCell.month}/${budgetCell.year}` : "-"} />
                </FormRow>
                <FormRow label="Department">
                  <ReadOnlyField value={budgetCell ? budgetCell.departmentName : "-"} />
                </FormRow>
                <FormRow label="Service">
                  <ReadOnlyField value={budgetCell ? budgetCell.serviceName : "-"} />
                </FormRow>
                <FormRow label="Segment / Sub-service">
                  <ReadOnlyField value={budgetCell?.subService || "-"} />
                </FormRow>
              </Section>

              <Section className="space-y-3">
                <SectionHeader
                  title="Budget Values"
                  description="Edit the monetary values and note for the selected budget cell."
                />

                <FormRow
                  label={(
                    <>
                      Gross Amount<span className="ml-1 text-rose-600">*</span>
                    </>
                  )}
                >
                  <Input
                    value={form.grossAmount}
                    onChange={(event) => updateForm("grossAmount", event.target.value)}
                    onBlur={(event) => updateForm("grossAmount", normalizeBudgetFormValue(event.target.value))}
                    disabled={busy || !canEdit}
                    inputMode="decimal"
                    placeholder="0.00"
                  />
                </FormRow>

                <FormRow label="Commission">
                  <Input
                    value={form.commission}
                    onChange={(event) => updateForm("commission", event.target.value)}
                    onBlur={(event) => updateForm("commission", normalizeBudgetFormValue(event.target.value))}
                    disabled={busy || !canEdit}
                    inputMode="decimal"
                    placeholder="0.00"
                  />
                </FormRow>

                <FormRow label="Net Adjustment">
                  <Input
                    value={form.netAdjustment}
                    onChange={(event) => updateForm("netAdjustment", event.target.value)}
                    onBlur={(event) => updateForm("netAdjustment", normalizeBudgetFormValue(event.target.value))}
                    disabled={busy || !canEdit}
                    inputMode="decimal"
                    placeholder="0.00"
                  />
                </FormRow>

                <FormRow label="Net Amount">
                  <ReadOnlyField value={currentNetAmountText} />
                </FormRow>

                <FormRow label="Note" alignStart>
                  <Textarea
                    value={form.note}
                    onChange={(event) => updateForm("note", event.target.value)}
                    disabled={busy || !canEdit}
                    rows={4}
                    maxLength={2048}
                    placeholder="Optional budget note"
                  />
                </FormRow>
              </Section>

              {submitError ? <p className="text-sm text-rose-600">{submitError}</p> : null}
            </div>

            {mode === "edit" ? (
              <ModalCacheFooter
                text={
                  isDetailRefreshing
                    ? "Refreshing budget data..."
                    : detailCacheStatus
                      ? `Data source: ${detailCacheStatus.source}. Last updated ${formatRelativeTime(detailCacheStatus.fetchedAt)}.`
                      : "No cached budget data yet"
                }
                onRefresh={() => {
                  if (!isDetailRefreshing && !isDirty && !busy) {
                    void refreshBudgetDetail("network-only");
                  }
                }}
                disabled={isDetailRefreshing || isDirty || busy}
                refreshing={isDetailRefreshing}
                refreshLabel="Refresh budget data"
                tooltipText={
                  isDirty
                    ? "Save or discard your edits before refreshing budget data."
                    : "Click to refresh this budget data"
                }
                actions={(
                  <>
                    {canRevert ? (
                      <Button variant="outline" type="button" onClick={restoreBaseline} disabled={busy}>
                        Revert
                      </Button>
                    ) : null}
                    <Button type="button" onClick={() => setIsReasonDialogOpen(true)} disabled={!canSubmit}>
                      Save
                    </Button>
                  </>
                )}
              />
            ) : (
              <ModalFooter className="mt-4 border-t border-slate-100 pt-3">
                <div className="flex items-center justify-end gap-2">
                  {canRevert ? (
                    <Button variant="outline" type="button" onClick={restoreBaseline} disabled={busy}>
                      Revert
                    </Button>
                  ) : null}
                  <Button type="button" onClick={() => setIsReasonDialogOpen(true)} disabled={!canSubmit}>
                    Save
                  </Button>
                </div>
              </ModalFooter>
            )}
          </ModalShell>
        </DialogContent>
      </Dialog>

      <Dialog open={isReasonDialogOpen} onOpenChange={setIsReasonDialogOpen}>
        <DialogContent
          className="max-w-[560px] rounded-xl bg-white p-6"
          onInteractOutside={(event) => {
            if (busy) {
              event.preventDefault();
            }
          }}
        >
          <DialogHeader>
            <DialogTitle>Change Reason</DialogTitle>
            <DialogDescription>
              Add an optional change reason before saving this budget. You can leave it blank.
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 space-y-4">
            <FormRow label="Reason" alignStart>
              <Textarea
                value={changeReason}
                onChange={(event) => setChangeReason(event.target.value)}
                disabled={busy}
                rows={4}
                maxLength={2048}
                placeholder="Optional reason for this change"
              />
            </FormRow>
          </div>

          <ModalFooter className="mt-6">
            <div className="flex items-center justify-end gap-2">
              <Button variant="outline" type="button" onClick={() => setIsReasonDialogOpen(false)} disabled={busy}>
                Back
              </Button>
              <Button type="button" onClick={() => void handleConfirmSubmit()} disabled={busy}>
                {busy ? (
                  <>
                    <RefreshCw className="size-4 animate-spin" />
                    Saving...
                  </>
                ) : (
                  "Save"
                )}
              </Button>
            </div>
          </ModalFooter>
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isDiscardDialogOpen}
        onKeepEditing={() => setIsDiscardDialogOpen(false)}
        onDiscardChanges={() => {
          setIsDiscardDialogOpen(false);
          onOpenChange(false);
        }}
      />
    </>
  );
}

function FundsphereBudgetPageContent() {
  const { requestJson } = useApiRequest();
  const { isOnline } = useOnlineStatus();
  const auth = useAuth();
  const periodOrder = useMemo(() => {
    const order = new Map<string, number>();
    buildBudgetPeriodOptions().forEach((option, index) => {
      order.set(option.value, index);
    });
    return order;
  }, []);

  const cacheContext = useMemo<FundsphereBudgetsCacheContext>(
    () => ({
      tenantSlug: auth.tenantSlug || "",
      userKey: auth.user?.id || auth.user?.email || "",
    }),
    [auth.tenantSlug, auth.user?.email, auth.user?.id],
  );
  const canEditFundsphere = useMemo(() => {
    if (!shouldProtectFrontendAuth()) {
      return true;
    }
    return hasAppEditAccess(auth.accessProfile, FUNDSPHERE_APP_CODE);
  }, [auth.accessProfile]);

  const [pageState, setPageState, pageStateControls] = useScopedPersistentState<PersistedBudgetsPageState>(
    {
      appCode: FUNDSPHERE_APP_CODE,
      pageCode: FUNDSPHERE_BUDGETS_PAGE_CODE,
      stateKey: "filters",
    },
    EMPTY_PAGE_STATE,
    { validate: isPersistedBudgetsPageState },
  );

  const [accountOptions, setAccountOptions] = useState<FundsphereAccount[]>([]);
  const [matrix, setMatrix] = useState<FundsphereBudgetMatrixResponse | null>(null);
  const accountOptionsRef = useRef<FundsphereAccount[]>([]);
  const matrixRef = useRef<FundsphereBudgetMatrixResponse | null>(null);
  const didRestoreMatrixCacheRef = useRef<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [accountOptionsError, setAccountOptionsError] = useState<string | null>(null);
  const [matrixError, setMatrixError] = useState<string | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [isLoadingAccounts, setIsLoadingAccounts] = useState(true);
  const [isLoadingMatrix, setIsLoadingMatrix] = useState(false);
  const [isRefreshingMatrix, setIsRefreshingMatrix] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState<BudgetMode>("create");
  const [modalCell, setModalCell] = useState<BudgetCellContext | null>(null);
  const accountOptionsTokenRef = useRef(0);
  const matrixRequestTokenRef = useRef(0);

  const searchDraft = pageState.searchDraft;
  const searchCriteria = pageState.searchCriteria;
  const hasSearched = pageState.hasSearched;
  const departmentOpenState = pageState.departmentOpenState ?? {};
  const serviceOpenState = pageState.serviceOpenState ?? {};
  const selectedCell = pageState.selectedCell ?? null;
  const selectedCellKey = selectedCell ? buildBudgetCellSelectionKey(selectedCell) : null;

  function updateDepartmentOpenState(
    updater: Record<string, boolean> | ((current: Record<string, boolean>) => Record<string, boolean>),
  ) {
    setPageState((current) => {
      const currentState = normalizeBooleanRecord(current.departmentOpenState);
      const nextState = typeof updater === "function" ? updater(currentState) : updater;
      return {
        ...current,
        departmentOpenState: nextState,
      };
    });
  }

  function updateServiceOpenState(
    updater: Record<string, boolean> | ((current: Record<string, boolean>) => Record<string, boolean>),
  ) {
    setPageState((current) => {
      const currentState = normalizeBooleanRecord(current.serviceOpenState);
      const nextState = typeof updater === "function" ? updater(currentState) : updater;
      return {
        ...current,
        serviceOpenState: nextState,
      };
    });
  }

  useEffect(() => {
    accountOptionsRef.current = accountOptions;
  }, [accountOptions]);

  useEffect(() => {
    if (!pageStateControls.hydrated) {
      return;
    }
    setPageState((current) => {
      const normalizedSearchDraft = {
        accountCodes: normalizeSelectionList(current.searchDraft.accountCodes),
        periods: normalizePeriodSelectionList(current.searchDraft.periods, periodOrder),
      };
      const normalizedSearchCriteria = {
        accountCodes: normalizeSelectionList(current.searchCriteria.accountCodes),
        periods: normalizePeriodSelectionList(current.searchCriteria.periods, periodOrder),
      };
      const normalizedDepartmentOpenState = normalizeBooleanRecord(current.departmentOpenState);
      const normalizedServiceOpenState = normalizeBooleanRecord(current.serviceOpenState);
      const normalizedSelectedCell = normalizeBudgetCellSelectionState(current.selectedCell);
      const searchDraftChanged = !areBudgetSearchCriteriaEqual(current.searchDraft, normalizedSearchDraft);
      const searchCriteriaChanged = !areBudgetSearchCriteriaEqual(current.searchCriteria, normalizedSearchCriteria);
      const departmentOpenChanged = JSON.stringify(normalizedDepartmentOpenState) !== JSON.stringify(current.departmentOpenState ?? {});
      const serviceOpenChanged = JSON.stringify(normalizedServiceOpenState) !== JSON.stringify(current.serviceOpenState ?? {});
      const selectedCellChanged = JSON.stringify(normalizedSelectedCell ?? null) !== JSON.stringify(current.selectedCell ?? null);
      if (!searchDraftChanged && !searchCriteriaChanged && !departmentOpenChanged && !serviceOpenChanged && !selectedCellChanged) {
        return current;
      }
      return {
        ...current,
        searchDraft: searchDraftChanged ? normalizedSearchDraft : current.searchDraft,
        searchCriteria: searchCriteriaChanged ? normalizedSearchCriteria : current.searchCriteria,
        departmentOpenState: departmentOpenChanged ? normalizedDepartmentOpenState : current.departmentOpenState,
        serviceOpenState: serviceOpenChanged ? normalizedServiceOpenState : current.serviceOpenState,
        selectedCell: selectedCellChanged ? normalizedSelectedCell : current.selectedCell,
      };
    });
  }, [pageStateControls.hydrated, periodOrder, setPageState]);

  useEffect(() => {
    if (!pageStateControls.hydrated || !hasSearched) {
      return;
    }
    const restoreKey = buildFundsphereBudgetsMatrixCacheKey(cacheContext, searchCriteria);
    if (didRestoreMatrixCacheRef.current === restoreKey) {
      return;
    }
    didRestoreMatrixCacheRef.current = restoreKey;
    void refreshBudgetMatrix("cache-first", searchCriteria);
  }, [cacheContext, hasSearched, pageStateControls.hydrated, searchCriteria]);

  useEffect(() => {
    if (!pageStateControls.hydrated) {
      return;
    }
    void refreshBudgetAccounts("cache-first");
  }, [pageStateControls.hydrated, requestJson, cacheContext]);

  function commitBudgetAccounts(
    nextAccounts: FundsphereAccount[] | null,
    source: "cache" | "network",
  ) {
    accountOptionsRef.current = nextAccounts ?? [];
    setAccountOptions(nextAccounts ?? []);
    if (!nextAccounts) {
      return;
    }
    syncFundsphereBudgetAccountsCache(cacheContext, nextAccounts, { source, fetchedAt: Date.now() });
  }

  function commitBudgetMatrix(
    nextMatrix: FundsphereBudgetMatrixResponse | null,
    source: "cache" | "network",
    criteria: BudgetSearchCriteria,
  ) {
    matrixRef.current = nextMatrix;
    setMatrix(nextMatrix);
    if (!nextMatrix) {
      return;
    }
    const fetchedAt = Date.now();
    setCacheStatus({ source, fetchedAt });
    setPageState((current) => ({
      ...current,
      hasSearched: true,
      searchCriteria: areBudgetSearchCriteriaEqual(current.searchCriteria, criteria)
        ? current.searchCriteria
        : {
            accountCodes: normalizeSelectionList(criteria.accountCodes),
            periods: normalizePeriodSelectionList(criteria.periods, periodOrder),
          },
    }));
    syncFundsphereBudgetsMatrixCache(cacheContext, nextMatrix, criteria, { source, fetchedAt });
  }

  async function refreshBudgetAccounts(policy: CachePolicy): Promise<void> {
    const requestToken = ++accountOptionsTokenRef.current;
    const snapshot = readFundsphereBudgetAccountsCacheSnapshot(cacheContext);
    const cachedAccounts = snapshot?.data ?? null;
    const shouldFetch = shouldFetchNetwork(policy, snapshot);

    if (cachedAccounts) {
      commitBudgetAccounts(cachedAccounts, "cache");
    }

    if (!shouldFetch) {
      setIsLoadingAccounts(false);
      return;
    }

    if (!isOnline) {
      if (!cachedAccounts) {
        setAccountOptionsError("You're offline. Connect to load account options.");
      }
      setIsLoadingAccounts(false);
      return;
    }

    setIsLoadingAccounts(true);
    try {
      const nextAccounts = await loadFundsphereAccounts({
        requestJson,
        criteria: {
          code: "",
          name: "",
          aeName: "",
          statusFilter: "",
        },
      });
      if (requestToken !== accountOptionsTokenRef.current) {
        return;
      }
      commitBudgetAccounts(nextAccounts, "network");
      setAccountOptionsError(null);
    } catch (error) {
      if (requestToken !== accountOptionsTokenRef.current) {
        return;
      }
      if (!cachedAccounts) {
        setAccountOptionsError(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not load account options.");
      }
    } finally {
      if (requestToken === accountOptionsTokenRef.current) {
        setIsLoadingAccounts(false);
      }
    }
  }

  async function refreshBudgetMatrix(
    policy: CachePolicy,
    criteria: BudgetSearchCriteria = searchCriteria,
  ): Promise<void> {
    const requestToken = ++matrixRequestTokenRef.current;
    const normalizedCriteria = {
      accountCodes: normalizeSelectionList(criteria.accountCodes),
      periods: normalizePeriodSelectionList(criteria.periods, periodOrder),
    };
    const snapshot = readFundsphereBudgetsMatrixCacheSnapshot(cacheContext, normalizedCriteria);
    const cachedMatrix = snapshot?.data ?? null;
    const shouldFetch = shouldFetchNetwork(policy, snapshot);

    if (cachedMatrix) {
      commitBudgetMatrix(cachedMatrix, "cache", normalizedCriteria);
      setMatrixError(null);
      setRefreshMessage(null);
    }

    if (!shouldFetch) {
      setIsLoadingMatrix(false);
      setIsRefreshingMatrix(false);
      return;
    }

    if (!isOnline) {
      if (cachedMatrix) {
        setRefreshMessage("You're offline. Showing cached budget matrix.");
      } else {
        setMatrixError("You're offline. Connect to load the budget matrix.");
      }
      setIsLoadingMatrix(false);
      setIsRefreshingMatrix(false);
      return;
    }

    if (cachedMatrix) {
      setIsRefreshingMatrix(true);
    } else {
      setIsLoadingMatrix(true);
      setRefreshMessage(null);
      setMatrixError(null);
    }

    try {
      const nextMatrix = await loadFundsphereBudgetMatrix({
        requestJson,
        accountCodes: normalizedCriteria.accountCodes,
        periods: normalizedCriteria.periods,
      });
      if (requestToken !== matrixRequestTokenRef.current) {
        return;
      }
      commitBudgetMatrix(nextMatrix, "network", normalizedCriteria);
      setMatrixError(null);
      setRefreshMessage(null);
    } catch (error) {
      if (requestToken !== matrixRequestTokenRef.current) {
        return;
      }
      if (matrixRef.current || cachedMatrix) {
        setRefreshMessage("Showing cached budget matrix. Could not refresh.");
        setMatrixError(null);
      } else {
        setMatrixError(error instanceof Error && error.message.trim() ? error.message.trim() : "Could not load the budget matrix.");
      }
    } finally {
      if (requestToken === matrixRequestTokenRef.current) {
        setIsLoadingMatrix(false);
        setIsRefreshingMatrix(false);
      }
    }
  }

  async function refreshBudgetPageBundle(policy: CachePolicy, criteria: BudgetSearchCriteria = searchCriteria): Promise<void> {
    await Promise.all([
      refreshBudgetAccounts(policy),
      hasBudgetSearchCriteria(criteria) ? refreshBudgetMatrix(policy, criteria) : Promise.resolve(),
    ]);
  }

  const accountDropdownOptions = useMemo<AppDropdownOption[]>(
    () => buildBudgetAccountOptions(accountOptions),
    [accountOptions],
  );
  const accountsByCode = useMemo(() => {
    return new Map(accountOptions.map((account) => [account.code.toUpperCase(), account]));
  }, [accountOptions]);
  const periodDropdownOptions = useMemo<AppDropdownOption[]>(() => buildBudgetPeriodOptions(), []);
  const matrixColumns = useMemo(
    () => buildBudgetColumns(accountsByCode, searchCriteria.accountCodes, searchCriteria.periods),
    [accountsByCode, searchCriteria.accountCodes, searchCriteria.periods],
  );
  const periodCountPerAccount = searchCriteria.periods.length;
  const accountGroupWidth = Math.max(periodCountPerAccount, 1) * BUDGET_MATRIX_PERIOD_WIDTH;
  const useAccountNamesInHeader = accountGroupWidth >= 312;
  const getAccountBoundaryClass = (columnIndex: number) =>
    periodCountPerAccount > 0 && (columnIndex + 1) % periodCountPerAccount === 0
      ? "relative border-r-2 border-r-slate-400"
      : "";
  const getAccountBoundaryStyle = (columnIndex: number): CSSProperties | undefined =>
    periodCountPerAccount > 0 && (columnIndex + 1) % periodCountPerAccount === 0
      ? { boxShadow: "inset -2px 0 0 0 #cbd5e1" }
      : undefined;
  const matrixHierarchy = useMemo(() => {
    if (!matrix || matrixColumns.length === 0) {
      return {
        departments: [] as BudgetDepartmentGroup[],
        grandTotalsByColumn: {} as Record<string, number>,
        grandTotalCents: 0,
      };
    }
    return buildBudgetMatrixHierarchy(matrix.rows, matrixColumns);
  }, [matrix, matrixColumns]);

  useEffect(() => {
    if (!matrixHierarchy.departments.length) {
      return;
    }
    updateDepartmentOpenState((current) => {
      const next = { ...current };
      for (const department of matrixHierarchy.departments) {
        if (typeof next[department.key] !== "boolean") {
          next[department.key] = true;
        }
      }
      return next;
    });
    updateServiceOpenState((current) => {
      const next = { ...current };
      for (const department of matrixHierarchy.departments) {
        for (const serviceGroup of department.serviceGroups) {
          if (typeof next[serviceGroup.key] !== "boolean") {
            next[serviceGroup.key] = true;
          }
        }
      }
      return next;
    });
  }, [matrixHierarchy.departments]);

  const hasMatrix = Boolean(matrix && matrixHierarchy.departments.length > 0);
  const hasDraftFilters = hasBudgetSearchCriteria(searchDraft);
  const searchResultText = !hasSearched
    ? "Select accounts and periods, then click Search to load the budget matrix."
    : !matrix && !isLoadingMatrix && !isRefreshingMatrix
      ? "Click Search to load the budget matrix."
      : !matrix
        ? "Loading budget matrix..."
      : matrixHierarchy.departments.length === 0
        ? "No budget rows matched the current accounts and periods."
        : `${matrixHierarchy.departments.length} department${matrixHierarchy.departments.length === 1 ? "" : "s"} shown.`;

  const pageMessages: StackMessage[] = [];
  if (refreshMessage) {
    pageMessages.push({
      id: "fundsphere-budgets-refresh",
      variant: refreshMessage.toLowerCase().includes("offline") ? "info" : "warning",
      message: refreshMessage,
    });
  }
  if (accountOptionsError) {
    pageMessages.push({
      id: "fundsphere-budgets-account-options",
      variant: "error",
      message: accountOptionsError,
    });
  }
  if (matrixError) {
    pageMessages.push({
      id: "fundsphere-budgets-error",
      variant: "error",
      message: matrixError,
    });
  }

  const cacheStatusText = isRefreshingMatrix
    ? "Refreshing budget matrix..."
    : !isOnline && cacheStatus
      ? `Offline. Showing cached budget matrix from ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : cacheStatus
        ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
        : "No cached budget matrix yet";

  const isPageRefreshing = isLoadingAccounts || isLoadingMatrix || isRefreshingMatrix;
  const canRefresh = Boolean(isOnline && !isPageRefreshing && !isSaving && !modalOpen);

  function handleSearchSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const nextSearchCriteria = {
      accountCodes: normalizeSelectionList(searchDraft.accountCodes),
      periods: normalizePeriodSelectionList(searchDraft.periods, periodOrder),
    };
    didRestoreMatrixCacheRef.current = buildFundsphereBudgetsMatrixCacheKey(cacheContext, nextSearchCriteria);
    const shouldForceRefresh =
      hasSearched && areBudgetSearchCriteriaEqual(nextSearchCriteria, searchCriteria);

    setPageState((current) => ({
      ...current,
      searchCriteria: nextSearchCriteria,
      hasSearched: true,
    }));

    if (shouldForceRefresh) {
      void refreshBudgetMatrix("network-only", nextSearchCriteria);
      return;
    }

    if (!matrixRef.current) {
      void refreshBudgetMatrix("cache-first", nextSearchCriteria);
      return;
    }

    void refreshBudgetMatrix("cache-first", nextSearchCriteria);
  }

  function resetSearchCriteria() {
    matrixRef.current = null;
    didRestoreMatrixCacheRef.current = null;
    setMatrix(null);
    setCacheStatus(null);
    setRefreshMessage(null);
    setMatrixError(null);
    updateDepartmentOpenState({});
    updateServiceOpenState({});
    setPageState((current) => ({
      ...current,
      searchDraft: { ...DEFAULT_SEARCH_CRITERIA },
      searchCriteria: { ...DEFAULT_SEARCH_CRITERIA },
      hasSearched: false,
      departmentOpenState: {},
      serviceOpenState: {},
      selectedCell: undefined,
    }));
  }

  function openCellModal(cell: BudgetCellContext, mode: BudgetMode) {
    if (!canEditFundsphere) {
      return;
    }
    setPageState((current) => ({
      ...current,
      selectedCell: {
        accountCode: cell.accountCode,
        accountName: cell.accountName,
        month: cell.month,
        year: cell.year,
        serviceId: cell.serviceId,
        serviceName: cell.serviceName,
        departmentCode: cell.departmentCode,
        departmentName: cell.departmentName,
        subService: cell.subService,
        mode,
      },
    }));
    setModalMode(mode);
    setModalCell(cell);
    setModalOpen(true);
  }

  async function handleBudgetSubmit(payload: {
    mode: BudgetMode;
    budgetId: string | null;
    identity: FundsphereBudgetCellContext;
    form: FundsphereBudgetFormState;
    changeReason: string;
  }) {
    if (!canEditFundsphere || isSaving) {
      return;
    }

    setIsSaving(true);
    setMatrixError(null);
    try {
      let savedBudgetId = payload.budgetId;
      if (payload.mode === "create") {
        const created = await createFundsphereBudget({
          requestJson,
          identity: payload.identity,
          form: payload.form,
          changeReason: payload.changeReason,
        });
        savedBudgetId = created.id || savedBudgetId;
      } else {
        if (!payload.budgetId) {
          throw new Error("Budget ID is required.");
        }
        const updated = await updateFundsphereBudget({
          requestJson,
          budgetId: payload.budgetId,
          identity: payload.identity,
          form: payload.form,
          changeReason: payload.changeReason,
        });
        savedBudgetId = updated.id || savedBudgetId;
      }

      let savedBudget: FundsphereBudgetMatrixRow | null = null;
      if (savedBudgetId) {
        try {
          savedBudget = await loadFundsphereBudgetDetail({
            requestJson,
            budgetId: savedBudgetId,
          });
        } catch {
          savedBudget = null;
        }
      }

      if (savedBudget) {
        syncFundsphereBudgetDetailCache(cacheContext, savedBudget, { source: "network", fetchedAt: Date.now() });
        const nextMatrix = upsertBudgetRowIntoMatrix(matrixRef.current, savedBudget);
        if (nextMatrix) {
          commitBudgetMatrix(nextMatrix, "cache", searchCriteria);
        }
      }

      void refreshBudgetMatrix("network-only", searchCriteria);
    } catch (error) {
      throw error instanceof Error ? error : new Error("Could not save budget.");
    } finally {
      setIsSaving(false);
    }
  }

  const budgetCounts = useMemo(() => {
    const detailRowCount = matrixHierarchy.departments.reduce(
      (count, departmentGroup) =>
        count + departmentGroup.serviceGroups.reduce((serviceCount, serviceGroup) => serviceCount + serviceGroup.detailRows.length, 0),
      0,
    );
    return {
      detailRowCount,
      departmentCount: matrixHierarchy.departments.length,
    };
  }, [matrixHierarchy.departments]);

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          gradientVariant="fundsphere"
          eyebrow="FundSphere"
          title="Budgets"
          description="Explore budgets as a pivoted matrix by department, service, segment, account, and period."
        />
      )}
      footer={hasSearched && (matrix || cacheStatus) ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            void refreshBudgetPageBundle("network-only", searchCriteria);
          }}
          disabled={!canRefresh}
          refreshing={isPageRefreshing}
          refreshLabel="Refresh budgets and filters"
          tooltipText={isOnline ? "Click to hard refresh budget data and filters" : "Offline. Reconnect to refresh budgets."}
          containerClassName="w-full"
        />
      ) : null}
    >
      <Section className="rounded-[1.45rem] border border-blue-100/90 bg-white/95 p-5 shadow-soft">
        <SectionHeader
          title="Budget Search"
          description="Select accounts and periods, then click Search to load the budget matrix."
        />

        <form className="space-y-5 px-1.5" onSubmit={handleSearchSubmit}>
          <div className="grid grid-cols-1 gap-3.5 md:grid-cols-2 xl:grid-cols-4">
            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Accounts
              </span>
              <AppDropdown
                value=""
                values={searchDraft.accountCodes}
                multiple
                options={accountDropdownOptions}
                onValueChange={() => {
                  // Multi-select is controlled through onValuesChange.
                }}
                onValuesChange={(values) =>
                  setPageState((current) => ({
                    ...current,
                    searchDraft: {
                      ...current.searchDraft,
                      accountCodes: normalizeSelectionList(values),
                    },
                  }))
                }
                searchable
                loading={isLoadingAccounts}
                allowCustomValue={false}
                ariaLabel="Account filter"
                placeholder="Select accounts"
              />
            </label>

            <label className="block min-w-0">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                Periods
              </span>
              <AppDropdown
                value=""
                values={searchDraft.periods}
                multiple
                options={periodDropdownOptions}
                onValueChange={() => {
                  // Multi-select is controlled through onValuesChange.
                }}
                onValuesChange={(values) =>
                  setPageState((current) => ({
                    ...current,
                    searchDraft: {
                      ...current.searchDraft,
                      periods: normalizePeriodSelectionList(values, periodOrder),
                    },
                  }))
                }
                searchable={false}
                allowCustomValue={false}
                ariaLabel="Period filter"
                placeholder="Select periods"
              />
            </label>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-blue-100/70 bg-blue-50/40 px-3 py-2">
            <p className="text-xs font-medium text-slate-500">{searchResultText}</p>
            <div className="flex flex-wrap items-center justify-end gap-2">
              {hasDraftFilters ? (
                <Button variant="outline" type="button" onClick={resetSearchCriteria} disabled={isLoadingMatrix || isRefreshingMatrix}>
                  Clear
                </Button>
              ) : null}
              {hasDraftFilters ? (
                <Button type="submit" disabled={isLoadingMatrix || isRefreshingMatrix || isLoadingAccounts}>
                  <Search className="size-4" />
                  Search
                </Button>
              ) : null}
            </div>
          </div>
        </form>
      </Section>

      <div className="relative">
        {hasMatrix ? (
          <Section className="overflow-hidden rounded-[1.45rem] border border-blue-100/90 bg-white/95 p-0 shadow-soft">
            <div className="flex items-center justify-between gap-3 border-b border-blue-100/70 px-5 py-4">
              <div>
                <p className="text-sm font-semibold text-slate-900">Budget Matrix</p>
                <p className="text-xs text-slate-500">
                  {budgetCounts.departmentCount} departments, {budgetCounts.detailRowCount} budget rows.
                </p>
              </div>
              <div className="flex flex-wrap items-center gap-2 text-xs font-medium text-slate-600">
                <span className="rounded-full bg-blue-50 px-2.5 py-1 text-blue-700">
                  Accounts {searchCriteria.accountCodes.length}
                </span>
                <span className="rounded-full bg-slate-100 px-2.5 py-1 text-slate-700">
                  Periods {searchCriteria.periods.length}
                </span>
              </div>
            </div>

            <div className="overflow-x-auto overflow-y-hidden">
              <table className="w-max min-w-full table-fixed border-separate border-spacing-0">
                <colgroup>
                  <col style={{ width: 180, minWidth: 180 }} />
                  <col style={{ width: 220, minWidth: 220 }} />
                  <col style={{ width: 220, minWidth: 220 }} />
                  {matrixColumns.map((column) => (
                    <col key={column.key} style={{ width: 104, minWidth: 104 }} />
                  ))}
                </colgroup>
                <thead>
                  <tr className="bg-slate-50/90">
                    <th
                      className={cn(
                        BUDGET_MATRIX_DEPARTMENT_COLUMN_CLASS,
                        BUDGET_MATRIX_FROZEN_HEADER_CLASS,
                        "bg-slate-100/95 italic text-xs font-semibold normal-case tracking-[-0.01em] text-slate-900",
                      )}
                      style={BUDGET_MATRIX_DEPARTMENT_COLUMN_STYLE}
                      rowSpan={2}
                    >
                      Department
                    </th>
                    <th
                      className={cn(
                        BUDGET_MATRIX_SERVICE_COLUMN_CLASS,
                        BUDGET_MATRIX_FROZEN_HEADER_CLASS,
                        "bg-slate-100/95 italic text-xs font-semibold normal-case tracking-[-0.01em] text-slate-900",
                      )}
                      style={BUDGET_MATRIX_SERVICE_COLUMN_STYLE}
                      rowSpan={2}
                    >
                      Service
                    </th>
                    <th
                      className={cn(
                        BUDGET_MATRIX_SEGMENT_COLUMN_CLASS,
                        BUDGET_MATRIX_FROZEN_HEADER_CLASS,
                        "bg-slate-100/95 italic text-xs font-semibold normal-case tracking-[-0.01em] text-slate-900",
                      )}
                      style={BUDGET_MATRIX_SEGMENT_COLUMN_STYLE}
                      rowSpan={2}
                    >
                      Segment
                    </th>
                    {searchCriteria.accountCodes.map((accountCode, accountIndex) => {
                      const account = accountsByCode.get(accountCode.toUpperCase());
                      const isLastAccountGroup = accountIndex === searchCriteria.accountCodes.length - 1;
                      return (
                        <th
                          key={accountCode}
                          colSpan={searchCriteria.periods.length}
                          className={cn(
                            BUDGET_MATRIX_ACCOUNT_HEADER_CLASS,
                            !isLastAccountGroup ? "relative border-r-2 border-r-slate-400" : "",
                          )}
                          style={{
                            ...BUDGET_MATRIX_ACCOUNT_HEADER_STYLE,
                            width: accountGroupWidth,
                            minWidth: accountGroupWidth,
                            maxWidth: accountGroupWidth,
                          }}
                        >
                          <BudgetMatrixAccountHeaderLabel
                            accountCode={account?.code ?? accountCode}
                            accountName={account?.name ?? null}
                            useAccountName={useAccountNamesInHeader}
                          />
                          {!isLastAccountGroup ? <BudgetMatrixBoundaryDivider /> : null}
                        </th>
                      );
                    })}
                  </tr>
                  <tr className="bg-slate-50/95">
                    {searchCriteria.accountCodes.flatMap((accountCode) =>
                      searchCriteria.periods.map((period, periodIndex) => {
                        const [monthText, yearText] = period.split("/");
                        const month = Number(monthText);
                        const year = Number(yearText);
                        const label = Number.isFinite(month) && Number.isFinite(year)
                          ? formatPeriodLabel(month, year)
                          : period;
                        return (
                          <th
                            key={`${accountCode}:${period}`}
                            className={cn(
                              BUDGET_MATRIX_PERIOD_COLUMN_CLASS,
                              BUDGET_MATRIX_PERIOD_HEADER_CLASS,
                              getAccountBoundaryClass(periodIndex),
                            )}
                            style={{
                              ...BUDGET_MATRIX_PERIOD_HEADER_STYLE,
                              ...getAccountBoundaryStyle(periodIndex),
                            }}
                          >
                            {label}
                            {periodIndex === periodCountPerAccount - 1 ? <BudgetMatrixBoundaryDivider /> : null}
                          </th>
                        );
                      }),
                    )}
                  </tr>
                </thead>
                <tbody>
                  {matrixHierarchy.departments.map((departmentGroup) => {
                    const departmentOpen = departmentOpenState[departmentGroup.key] ?? true;
                    const departmentBodyRowCount = departmentOpen
                      ? departmentGroup.serviceGroups.reduce((count, serviceGroup) => {
                        const serviceOpen = serviceOpenState[serviceGroup.key] ?? true;
                        return count + 1 + (serviceOpen ? serviceGroup.detailRows.length : 0);
                      }, 0)
                      : 0;
                    let departmentCellRendered = false;

                    if (!departmentOpen) {
                      return (
                        <Fragment key={departmentGroup.key}>
                          <tr key={`${departmentGroup.key}:collapsed`} className="bg-violet-50/70">
                              <td
                                className={cn(
                                  BUDGET_MATRIX_DEPARTMENT_COLUMN_CLASS,
                                  "border-b border-violet-200/80 bg-violet-100 px-2 py-2 align-top cursor-pointer select-none",
                                )}
                              style={BUDGET_MATRIX_DEPARTMENT_COLUMN_STYLE}
                              role="button"
                              tabIndex={0}
                              aria-expanded={departmentOpen}
                              onClick={() => {
                                updateDepartmentOpenState((current) => ({
                                  ...current,
                                  [departmentGroup.key]: !departmentOpen,
                                }));
                              }}
                              onKeyDown={(event) =>
                                handleHierarchyCellKeyDown(event, () => {
                                  updateDepartmentOpenState((current) => ({
                                    ...current,
                                    [departmentGroup.key]: !departmentOpen,
                                  }));
                                })
                              }
                            >
                              <BudgetHierarchyLabel
                                level={0}
                                title={`${departmentGroup.departmentName} Total`}
                                subtitle={null}
                                open={departmentOpen}
                                onToggle={() => undefined}
                              />
                            </td>
                            <td
                              className={cn(BUDGET_MATRIX_SERVICE_COLUMN_CLASS, "border-b border-violet-200/80 bg-violet-100 px-2 py-2")}
                              style={BUDGET_MATRIX_SERVICE_COLUMN_STYLE}
                            />
                            <td
                              className={cn(BUDGET_MATRIX_SEGMENT_COLUMN_CLASS, "border-b border-violet-200/80 bg-violet-100 px-2 py-2")}
                              style={BUDGET_MATRIX_SEGMENT_COLUMN_STYLE}
                            />
                            {matrixColumns.map((column, columnIndex) => {
                              return (
                              <td
                                key={`${departmentGroup.key}:${column.key}`}
                                className={cn(
                                  BUDGET_MATRIX_PERIOD_COLUMN_CLASS,
                                  getAccountBoundaryClass(columnIndex),
                                  "border-b border-violet-200/80 px-1 py-2 text-center text-sm font-semibold text-slate-900",
                                )}
                                style={getAccountBoundaryStyle(columnIndex)}
                              >
                                {centsToCurrency(departmentGroup.totalCentsByColumn[column.key] ?? 0)}
                                {columnIndex === periodCountPerAccount - 1 ? <BudgetMatrixBoundaryDivider /> : null}
                              </td>
                              );
                            })}
                          </tr>
                        </Fragment>
                      );
                    }

                    return (
                      <Fragment key={departmentGroup.key}>
                        {departmentGroup.serviceGroups.map((serviceGroup) => {
                          const serviceOpen = serviceOpenState[serviceGroup.key] ?? true;
                          const serviceDetailRows = serviceOpen ? serviceGroup.detailRows : [];
                          const serviceRowSpan = 1 + serviceDetailRows.length;
                          const showDepartmentCell = !departmentCellRendered;
                          if (showDepartmentCell) {
                            departmentCellRendered = true;
                          }

                          return (
                            <Fragment key={serviceGroup.key}>
                              <tr key={`${serviceGroup.key}:total`} className="bg-violet-100/70">
                                {showDepartmentCell ? (
                                  <td
                                    rowSpan={departmentBodyRowCount}
                                    className={cn(
                                      BUDGET_MATRIX_DEPARTMENT_COLUMN_CLASS,
                                      "border-b border-slate-200 bg-slate-50 px-2 py-2 align-top cursor-pointer select-none",
                                    )}
                                    style={BUDGET_MATRIX_DEPARTMENT_COLUMN_STYLE}
                                    role="button"
                                    tabIndex={0}
                                    aria-expanded={departmentOpen}
                                    onClick={() => {
                                      updateDepartmentOpenState((current) => ({
                                        ...current,
                                        [departmentGroup.key]: !departmentOpen,
                                      }));
                                    }}
                                    onKeyDown={(event) =>
                                      handleHierarchyCellKeyDown(event, () => {
                                        updateDepartmentOpenState((current) => ({
                                          ...current,
                                          [departmentGroup.key]: !departmentOpen,
                                        }));
                                      })
                                    }
                                  >
                                    <BudgetHierarchyLabel
                                      level={0}
                                      title={departmentGroup.departmentName}
                                      subtitle={null}
                                      open={departmentOpen}
                                      onToggle={() => undefined}
                                    />
                                  </td>
                                ) : null}
                                  <td
                                    rowSpan={serviceRowSpan}
                                    className={cn(
                                      BUDGET_MATRIX_SERVICE_COLUMN_CLASS,
                                      "border-t border-violet-200/80 bg-violet-100 px-2 py-2 align-top cursor-pointer select-none",
                                    )}
                                    style={BUDGET_MATRIX_SERVICE_COLUMN_STYLE}
                                    role="button"
                                  tabIndex={0}
                                  aria-expanded={serviceOpen}
                                  onClick={() => {
                                    updateServiceOpenState((current) => ({
                                      ...current,
                                      [serviceGroup.key]: !serviceOpen,
                                    }));
                                  }}
                                  onKeyDown={(event) =>
                                    handleHierarchyCellKeyDown(event, () => {
                                      updateServiceOpenState((current) => ({
                                        ...current,
                                        [serviceGroup.key]: !serviceOpen,
                                      }));
                                    })
                                  }
                                >
                                  <BudgetHierarchyLabel
                                    level={1}
                                    title={`${serviceGroup.serviceName} Total`}
                                    subtitle={null}
                                    open={serviceOpen}
                                    onToggle={() => undefined}
                                    emphasis="muted"
                                  />
                                </td>
                                <td
                                  className={cn(BUDGET_MATRIX_SEGMENT_COLUMN_CLASS, "border-t border-violet-200/80 bg-violet-100 px-2 py-2")}
                                  style={BUDGET_MATRIX_SEGMENT_COLUMN_STYLE}
                                />
                                {matrixColumns.map((column, columnIndex) => {
                                  return (
                                  <td
                                    key={`${serviceGroup.key}:${column.key}`}
                                    className={cn(
                                      BUDGET_MATRIX_PERIOD_COLUMN_CLASS,
                                      getAccountBoundaryClass(columnIndex),
                                      "border-t border-violet-200/80 px-1 py-2 text-center text-sm font-semibold text-slate-800",
                                    )}
                                    style={getAccountBoundaryStyle(columnIndex)}
                                  >
                                    {centsToCurrency(serviceGroup.totalCentsByColumn[column.key] ?? 0)}
                                    {columnIndex === periodCountPerAccount - 1 ? <BudgetMatrixBoundaryDivider /> : null}
                                  </td>
                                  );
                                })}
                              </tr>

                              {serviceDetailRows.map((detailRow) => {
                                const detailLabel = detailRow.subService ? detailRow.subService : "";
                                return (
                                  <tr key={detailRow.key} className="bg-white">
                                    <td
                                      className={cn(BUDGET_MATRIX_SEGMENT_COLUMN_CLASS, "border-b border-slate-200 bg-white px-2 py-2")}
                                      style={BUDGET_MATRIX_SEGMENT_COLUMN_STYLE}
                                    >
                                      <BudgetHierarchyLabel
                                        level={2}
                                        title={detailLabel || " "}
                                        subtitle={null}
                                        emphasis="subtle"
                                      />
                                    </td>
                                    {matrixColumns.map((column, columnIndex) => {
                                      const cell = detailRow.cells[column.key] ?? null;
                                      return (
                                        <td
                                          key={`${detailRow.key}:${column.key}`}
                                          className={cn(
                                            BUDGET_MATRIX_PERIOD_COLUMN_CLASS,
                                            getAccountBoundaryClass(columnIndex),
                                            "border-b border-slate-200 px-1 py-1",
                                          )}
                                          style={{
                                            width: BUDGET_MATRIX_PERIOD_WIDTH,
                                            minWidth: BUDGET_MATRIX_PERIOD_WIDTH,
                                            maxWidth: BUDGET_MATRIX_PERIOD_WIDTH,
                                            ...getAccountBoundaryStyle(columnIndex),
                                          }}
                                        >
                                          <BudgetMatrixCellButton
                                            value={cell}
                                            title={
                                              cell?.row
                                                ? `${column.accountCode} ${column.label} ${detailRow.serviceName} ${detailLabel || "segment"}`
                                                : `${column.accountCode} ${column.label} ${detailRow.serviceName} ${detailLabel || "segment"} - create budget`
                                            }
                                            selected={
                                              selectedCellKey ===
                                              buildBudgetCellSelectionKey({
                                                accountCode: column.accountCode,
                                                month: column.month,
                                                year: column.year,
                                                serviceId: detailRow.serviceId,
                                                subService: detailRow.subService,
                                              })
                                            }
                                            onClick={() => {
                                              const cellRow = cell?.row ?? null;
                                              openCellModal(
                                                {
                                                  budgetId: cellRow?.budgetId ?? null,
                                                  budgetRow: cellRow,
                                                  accountCode: column.accountCode,
                                                  accountName: column.accountName,
                                                  month: column.month,
                                                  year: column.year,
                                                  serviceId: detailRow.serviceId,
                                                  serviceName: detailRow.serviceName,
                                                  departmentCode: detailRow.departmentCode,
                                                  departmentName: detailRow.departmentName,
                                                  subService: detailRow.subService,
                                                },
                                                cellRow ? "edit" : "create",
                                              );
                                            }}
                                            align="center"
                                          />
                                          {columnIndex === periodCountPerAccount - 1 ? <BudgetMatrixBoundaryDivider /> : null}
                                        </td>
                                      );
                                    })}
                                  </tr>
                                );
                              })}
                            </Fragment>
                          );
                        })}
                        <tr key={`${departmentGroup.key}:total`} className="bg-violet-100/70">
                          <td
                            className={cn(BUDGET_MATRIX_DEPARTMENT_COLUMN_CLASS, "border-t border-violet-200/80 bg-violet-100 px-2 py-2")}
                            style={BUDGET_MATRIX_DEPARTMENT_COLUMN_STYLE}
                          >
                            <p className="text-sm font-semibold text-slate-900">{departmentGroup.departmentName} Total</p>
                          </td>
                          <td
                            className={cn(BUDGET_MATRIX_SERVICE_COLUMN_CLASS, "border-t border-violet-200/80 bg-violet-100 px-2 py-2")}
                            style={BUDGET_MATRIX_SERVICE_COLUMN_STYLE}
                          />
                          <td
                            className={cn(BUDGET_MATRIX_SEGMENT_COLUMN_CLASS, "border-t border-violet-200/80 bg-violet-100 px-2 py-2")}
                            style={BUDGET_MATRIX_SEGMENT_COLUMN_STYLE}
                          />
                          {matrixColumns.map((column, columnIndex) => {
                            return (
                            <td
                              key={`${departmentGroup.key}:${column.key}`}
                              className={cn(
                                BUDGET_MATRIX_PERIOD_COLUMN_CLASS,
                                getAccountBoundaryClass(columnIndex),
                                "border-t border-violet-200/80 px-1 py-2 text-center text-sm font-semibold text-slate-900",
                              )}
                              style={getAccountBoundaryStyle(columnIndex)}
                            >
                              {centsToCurrency(departmentGroup.totalCentsByColumn[column.key] ?? 0)}
                              {columnIndex === periodCountPerAccount - 1 ? <BudgetMatrixBoundaryDivider /> : null}
                            </td>
                            );
                          })}
                        </tr>
                      </Fragment>
                    );
                  })}
                </tbody>
                <tfoot>
                  <tr className="bg-slate-900 text-white">
                    <td
                      className={cn(
                        BUDGET_MATRIX_DEPARTMENT_COLUMN_CLASS,
                        "border-t border-slate-800 bg-slate-900 px-2 py-2",
                      )}
                      style={BUDGET_MATRIX_DEPARTMENT_COLUMN_STYLE}
                    >
                      <p className="text-sm font-semibold">Grand Total</p>
                      <p className="text-[11px] uppercase tracking-[0.12em] text-slate-300">All departments</p>
                    </td>
                    <td
                      className={cn(BUDGET_MATRIX_SERVICE_COLUMN_CLASS, "border-t border-slate-800 bg-slate-900 px-2 py-2")}
                      style={BUDGET_MATRIX_SERVICE_COLUMN_STYLE}
                    />
                    <td
                      className={cn(BUDGET_MATRIX_SEGMENT_COLUMN_CLASS, "border-t border-slate-800 bg-slate-900 px-2 py-2")}
                      style={BUDGET_MATRIX_SEGMENT_COLUMN_STYLE}
                    />
                    {matrixColumns.map((column, columnIndex) => {
                      return (
                      <td
                        key={`grand:${column.key}`}
                        className={cn(
                          BUDGET_MATRIX_PERIOD_COLUMN_CLASS,
                          getAccountBoundaryClass(columnIndex),
                          "border-t border-slate-800 px-1 py-2 text-center text-sm font-semibold",
                        )}
                        style={getAccountBoundaryStyle(columnIndex)}
                      >
                        {centsToCurrency(matrixHierarchy.grandTotalsByColumn[column.key] ?? 0)}
                        {columnIndex === periodCountPerAccount - 1 ? <BudgetMatrixBoundaryDivider /> : null}
                      </td>
                      );
                    })}
                  </tr>
                </tfoot>
              </table>
            </div>

            <SectionLoadingLayer
              active={Boolean(isRefreshingMatrix && matrix)}
              message="Refreshing budget matrix..."
              className="rounded-[1.1rem]"
            />
          </Section>
        ) : (
          <SearchEmptyStatePanel
            icon={<Search className="size-7" />}
            message={hasSearched ? "No budget rows found" : "Search budgets"}
            description={
              hasSearched
                ? "No budget rows matched the selected accounts and periods. Adjust the filters or clear them to search again."
                : "Use the search form above to load the pivoted budget matrix."
            }
          />
        )}

        <PageLoadingLayer active={Boolean(isLoadingMatrix && !matrix)} message="Loading budget matrix..." />
      </div>

      <BudgetModal
        open={modalOpen}
        mode={modalMode}
        canEdit={canEditFundsphere}
        busy={isSaving}
        budgetCell={modalCell}
        requestJson={requestJson}
        cacheContext={cacheContext}
        isOnline={isOnline}
        onOpenChange={setModalOpen}
        onSubmit={handleBudgetSubmit}
      />
    </AppPageLayout>
  );
}

export default function FundsphereBudgetsPage() {
  return <FundsphereBudgetPageContent />;
}
