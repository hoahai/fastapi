import { useState } from "react";

import { cn } from "@/lib/utils";

export type ScheduleViewMode = "compact" | "detail";

type ScheduleWeekColumn = {
  key: string;
  label: string;
  monthLabel: string;
};

type ScheduleMonthGroup = {
  label: string;
  count: number;
};

type ScheduleDataRow = {
  kind: "data";
  values: string[];
  weekValues: number[];
  totalSpot: number;
  totalGross: string;
};

type ScheduleSubtotal = {
  weeklySpotTotals: number[];
  weeklyGrossTotals: string[];
  monthlyGrossTotals: string[];
  totalSpot: number;
  totalGross: string;
};

type ScheduleGroup = {
  groupKey: string;
  groupLabel: string;
  rows: ScheduleDataRow[];
  subtotal: ScheduleSubtotal;
};

type ScheduleTotals = {
  weeklySpotTotals: number[];
  weeklyGrossTotals: string[];
  monthlyGrossTotals: string[];
  totalSpot: number;
  totalGross: string;
};

type ScheduleTablePayload = {
  staticColumns: string[];
  weekColumns: ScheduleWeekColumn[];
  monthGroups: ScheduleMonthGroup[];
  zeroSpotWeekKeys: string[];
  rows?: ScheduleDataRow[];
  groups?: ScheduleGroup[];
  totals: ScheduleTotals;
};

export type ScheduleTableData = {
  estNum: number;
  estNumNote: string;
  viewMode: ScheduleViewMode;
  billingType: string;
  contextTitle: string;
  summary: {
    gross: string;
    spots: number;
  };
  table: ScheduleTablePayload;
};

interface ScheduleTableProps {
  data: ScheduleTableData;
}

interface HoverState {
  hoveredRowKey: string | null;
  hoveredColumnIndex: number | null;
}

function getCellHoverClass(
  hoverState: HoverState,
  rowKey: string | null,
  columnIndex: number | null,
  options?: {
    isHeader?: boolean;
    isZeroSpot?: boolean;
  },
): string {
  const isRowHovered = rowKey !== null && hoverState.hoveredRowKey === rowKey;
  const isColumnHovered = columnIndex !== null && hoverState.hoveredColumnIndex === columnIndex;
  const isActiveCell = isRowHovered && isColumnHovered;

  if (isActiveCell) {
    return options?.isHeader ? "bg-blue-200/70" : "bg-blue-100";
  }
  if (isRowHovered || isColumnHovered) {
    return options?.isHeader ? "bg-blue-100/80" : "bg-blue-50/80";
  }
  if (options?.isZeroSpot) {
    return "bg-slate-200/70";
  }
  return "";
}

function getRangeHoverClass(hoverState: HoverState, startColumnIndex: number, endColumnIndex: number): string {
  const hoveredColumnIndex = hoverState.hoveredColumnIndex;
  if (hoveredColumnIndex === null) {
    return "";
  }
  return hoveredColumnIndex >= startColumnIndex && hoveredColumnIndex <= endColumnIndex ? "bg-blue-100/80" : "";
}

function renderTotalsRows(
  table: ScheduleTablePayload,
  staticColSpan: number,
  totals: ScheduleTotals,
  options: {
    rowKeyPrefix: string;
    hoverState: HoverState;
    onHoverCell: (rowKey: string, columnIndex: number) => void;
    weekStartColumnIndex: number;
    totalSpotColumnIndex: number;
    totalGrossColumnIndex: number;
  },
): JSX.Element {
  let monthOffset = 0;

  return (
    <>
      <tr className="bg-slate-100 font-semibold text-slate-800">
        <td
          colSpan={staticColSpan}
          className={cn(
            "border border-slate-300 px-3 py-2 text-left",
            getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-spots`, 0),
          )}
          onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-spots`, 0)}
        >
          Total Spots
        </td>
        {totals.weeklySpotTotals.map((value, index) => {
          const columnIndex = options.weekStartColumnIndex + index;
          return (
            <td
              key={`total-spots-${index}`}
              className={cn(
                "border border-slate-300 px-2 py-2 text-center",
                getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-spots`, columnIndex),
              )}
              onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-spots`, columnIndex)}
            >
              {value}
            </td>
          );
        })}
        <td
          className={cn(
            "border border-slate-300 px-3 py-2 text-center",
            getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-spots`, options.totalSpotColumnIndex),
          )}
          onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-spots`, options.totalSpotColumnIndex)}
        >
          {totals.totalSpot}
        </td>
        <td
          className={cn(
            "border border-slate-300 px-3 py-2 text-right",
            getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-spots`, options.totalGrossColumnIndex),
          )}
          onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-spots`, options.totalGrossColumnIndex)}
        >
          ${totals.totalGross}
        </td>
      </tr>
      <tr className="bg-slate-100 font-semibold text-slate-800">
        <td
          colSpan={staticColSpan}
          className={cn(
            "border border-slate-300 px-3 py-2 text-left",
            getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-gross`, 0),
          )}
          onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-gross`, 0)}
        >
          Total Gross
        </td>
        {totals.weeklyGrossTotals.map((value, index) => {
          const columnIndex = options.weekStartColumnIndex + index;
          return (
            <td
              key={`total-gross-${index}`}
              className={cn(
                "border border-slate-300 px-2 py-2 text-center",
                getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-gross`, columnIndex),
              )}
              onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-gross`, columnIndex)}
            >
              ${value}
            </td>
          );
        })}
        <td
          className={cn(
            "border border-slate-300 px-3 py-2 text-center",
            getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-gross`, options.totalSpotColumnIndex),
          )}
          onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-gross`, options.totalSpotColumnIndex)}
        >
          -
        </td>
        <td
          className={cn(
            "border border-slate-300 px-3 py-2 text-right",
            getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-gross`, options.totalGrossColumnIndex),
          )}
          onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-gross`, options.totalGrossColumnIndex)}
        >
          -
        </td>
      </tr>
      <tr className="bg-slate-100 font-semibold text-slate-800">
        <td
          colSpan={staticColSpan}
          className={cn(
            "border border-slate-300 px-3 py-2 text-left",
            getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-month`, 0),
          )}
          onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-month`, 0)}
        >
          Total Gross by Month
        </td>
        {table.monthGroups.map((group, index) => {
          const monthStart = options.weekStartColumnIndex + monthOffset;
          const monthEnd = monthStart + group.count - 1;
          monthOffset += group.count;
          return (
            <td
              key={`total-month-gross-${index}`}
              colSpan={group.count}
              className={cn(
                "border border-slate-300 px-2 py-2 text-center",
                getRangeHoverClass(options.hoverState, monthStart, monthEnd),
              )}
              onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-month`, monthStart)}
            >
              ${totals.monthlyGrossTotals[index] ?? "0.00"}
            </td>
          );
        })}
        <td
          className={cn(
            "border border-slate-300 px-3 py-2 text-center",
            getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-month`, options.totalSpotColumnIndex),
          )}
          onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-month`, options.totalSpotColumnIndex)}
        >
          -
        </td>
        <td
          className={cn(
            "border border-slate-300 px-3 py-2 text-right",
            getCellHoverClass(options.hoverState, `${options.rowKeyPrefix}-total-month`, options.totalGrossColumnIndex),
          )}
          onMouseEnter={() => options.onHoverCell(`${options.rowKeyPrefix}-total-month`, options.totalGrossColumnIndex)}
        >
          -
        </td>
      </tr>
    </>
  );
}

export function ScheduleTable({ data }: ScheduleTableProps) {
  const table = data.table;
  const zeroSpotWeekKeySet = new Set(table.zeroSpotWeekKeys);
  const weekColumns = table.weekColumns;
  const staticColumnCount = table.staticColumns.length;
  const weekStartColumnIndex = staticColumnCount;
  const totalSpotColumnIndex = staticColumnCount + weekColumns.length;
  const totalGrossColumnIndex = totalSpotColumnIndex + 1;
  const [hoveredRowKey, setHoveredRowKey] = useState<string | null>(null);
  const [hoveredColumnIndex, setHoveredColumnIndex] = useState<number | null>(null);
  const hoverState: HoverState = { hoveredRowKey, hoveredColumnIndex };

  function onHoverCell(rowKey: string, columnIndex: number): void {
    setHoveredRowKey(rowKey);
    setHoveredColumnIndex(columnIndex);
  }

  function clearHover(): void {
    setHoveredRowKey(null);
    setHoveredColumnIndex(null);
  }

  let monthOffset = 0;

  return (
    <div className="inline-flex min-w-max flex-col gap-3 align-top">
      <div className="w-full rounded-md bg-blue-600 px-3 py-2 text-sm font-semibold text-white">{data.contextTitle}</div>
      <div className="w-full rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
        Gross: ${data.summary.gross} | Spots: {data.summary.spots}
      </div>

      <table className="min-w-max border-collapse text-xs text-slate-800" onMouseLeave={clearHover}>
        <thead>
          <tr className="bg-blue-100 text-slate-800">
            {table.staticColumns.map((columnLabel, index) => (
              <th
                key={`static-${columnLabel}`}
                rowSpan={2}
                className={cn(
                  "border border-slate-300 px-3 py-2 text-center font-semibold",
                  getCellHoverClass(hoverState, null, index, { isHeader: true }),
                )}
                onMouseEnter={() => {
                  setHoveredRowKey(null);
                  setHoveredColumnIndex(index);
                }}
              >
                {columnLabel}
              </th>
            ))}
            {table.monthGroups.map((group, index) => {
              const monthStart = weekStartColumnIndex + monthOffset;
              const monthEnd = monthStart + group.count - 1;
              monthOffset += group.count;
              return (
                <th
                  key={`month-${group.label}-${index}`}
                  colSpan={group.count}
                  className={cn(
                    "border border-slate-300 px-3 py-2 text-center font-semibold",
                    getRangeHoverClass(hoverState, monthStart, monthEnd),
                  )}
                >
                  {group.label}
                </th>
              );
            })}
            <th
              rowSpan={2}
              className={cn(
                "border border-slate-300 px-3 py-2 text-center font-semibold",
                getCellHoverClass(hoverState, null, totalSpotColumnIndex, { isHeader: true }),
              )}
              onMouseEnter={() => {
                setHoveredRowKey(null);
                setHoveredColumnIndex(totalSpotColumnIndex);
              }}
            >
              Total Spot
            </th>
            <th
              rowSpan={2}
              className={cn(
                "border border-slate-300 px-3 py-2 text-right font-semibold",
                getCellHoverClass(hoverState, null, totalGrossColumnIndex, { isHeader: true }),
              )}
              onMouseEnter={() => {
                setHoveredRowKey(null);
                setHoveredColumnIndex(totalGrossColumnIndex);
              }}
            >
              Total Gross
            </th>
          </tr>
          <tr className="bg-blue-100 text-slate-800">
            {weekColumns.map((column, weekIndex) => {
              const columnIndex = weekStartColumnIndex + weekIndex;
              return (
                <th
                  key={`week-${column.key}`}
                  className={cn(
                    "border border-slate-300 px-2 py-2 text-center font-semibold",
                    getCellHoverClass(hoverState, null, columnIndex, { isHeader: true }),
                  )}
                  onMouseEnter={() => {
                    setHoveredRowKey(null);
                    setHoveredColumnIndex(columnIndex);
                  }}
                >
                  {column.label}
                </th>
              );
            })}
          </tr>
        </thead>

        {data.viewMode === "compact" ? (
          <tbody>
            {(table.rows ?? []).map((row, rowIndex) => {
              const rowKey = `compact-row-${rowIndex}`;
              return (
                <tr key={rowKey}>
                  {row.values.map((cellValue, cellIndex) => (
                    <td
                      key={`${rowKey}-cell-${cellIndex}`}
                      className={cn(
                        "border border-slate-300 px-3 py-1.5",
                        cellIndex === 0 ? "text-left" : "text-center",
                        getCellHoverClass(hoverState, rowKey, cellIndex),
                      )}
                      onMouseEnter={() => onHoverCell(rowKey, cellIndex)}
                    >
                      {cellValue}
                    </td>
                  ))}
                  {row.weekValues.map((value, weekIndex) => {
                    const weekKey = weekColumns[weekIndex]?.key ?? "";
                    const columnIndex = weekStartColumnIndex + weekIndex;
                    return (
                      <td
                        key={`${rowKey}-week-${weekIndex}`}
                        className={cn(
                          "border border-slate-300 px-2 py-1.5 text-center",
                          getCellHoverClass(hoverState, rowKey, columnIndex, {
                            isZeroSpot: zeroSpotWeekKeySet.has(weekKey),
                          }),
                        )}
                        onMouseEnter={() => onHoverCell(rowKey, columnIndex)}
                      >
                        {value}
                      </td>
                    );
                  })}
                  <td
                    className={cn(
                      "border border-slate-300 px-3 py-1.5 text-center",
                      getCellHoverClass(hoverState, rowKey, totalSpotColumnIndex),
                    )}
                    onMouseEnter={() => onHoverCell(rowKey, totalSpotColumnIndex)}
                  >
                    {row.totalSpot}
                  </td>
                  <td
                    className={cn(
                      "border border-slate-300 px-3 py-1.5 text-right",
                      getCellHoverClass(hoverState, rowKey, totalGrossColumnIndex),
                    )}
                    onMouseEnter={() => onHoverCell(rowKey, totalGrossColumnIndex)}
                  >
                    ${row.totalGross}
                  </td>
                </tr>
              );
            })}

            {renderTotalsRows(table, staticColumnCount, table.totals, {
              rowKeyPrefix: "compact",
              hoverState,
              onHoverCell,
              weekStartColumnIndex,
              totalSpotColumnIndex,
              totalGrossColumnIndex,
            })}
          </tbody>
        ) : (
          <tbody>
            {(table.groups ?? []).flatMap((group, groupIndex) => {
              const renderedRows: JSX.Element[] = [];

              group.rows.forEach((row, rowIndex) => {
                const rowKey = `detail-${group.groupKey}-row-${rowIndex}`;
                renderedRows.push(
                  <tr key={rowKey}>
                    {row.values.map((cellValue, cellIndex) => (
                      <td
                        key={`${rowKey}-cell-${cellIndex}`}
                        className={cn(
                          "border border-slate-300 px-3 py-1.5",
                          cellIndex === 0 || cellIndex === 5
                            ? "text-left"
                            : cellIndex === 7
                              ? "text-right"
                              : "text-center",
                          getCellHoverClass(hoverState, rowKey, cellIndex),
                        )}
                        onMouseEnter={() => onHoverCell(rowKey, cellIndex)}
                      >
                        {cellValue}
                      </td>
                    ))}
                    {row.weekValues.map((value, weekIndex) => {
                      const weekKey = weekColumns[weekIndex]?.key ?? "";
                      const columnIndex = weekStartColumnIndex + weekIndex;
                      return (
                        <td
                          key={`${rowKey}-week-${weekIndex}`}
                          className={cn(
                            "border border-slate-300 px-2 py-1.5 text-center",
                            getCellHoverClass(hoverState, rowKey, columnIndex, {
                              isZeroSpot: zeroSpotWeekKeySet.has(weekKey),
                            }),
                          )}
                          onMouseEnter={() => onHoverCell(rowKey, columnIndex)}
                        >
                          {value}
                        </td>
                      );
                    })}
                    <td
                      className={cn(
                        "border border-slate-300 px-3 py-1.5 text-center",
                        getCellHoverClass(hoverState, rowKey, totalSpotColumnIndex),
                      )}
                      onMouseEnter={() => onHoverCell(rowKey, totalSpotColumnIndex)}
                    >
                      {row.totalSpot}
                    </td>
                    <td
                      className={cn(
                        "border border-slate-300 px-3 py-1.5 text-right",
                        getCellHoverClass(hoverState, rowKey, totalGrossColumnIndex),
                      )}
                      onMouseEnter={() => onHoverCell(rowKey, totalGrossColumnIndex)}
                    >
                      ${row.totalGross}
                    </td>
                  </tr>,
                );
              });

              renderedRows.push(
                <tr key={`group-${group.groupKey}-subtotal-spots`} className="bg-slate-100 font-semibold text-slate-800">
                  <td
                    colSpan={staticColumnCount}
                    className={cn(
                      "border border-slate-300 px-3 py-2 text-right",
                      getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-spots`, 0),
                    )}
                    onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-spots`, 0)}
                  >
                    {group.groupLabel} - Total Spots
                  </td>
                  {group.subtotal.weeklySpotTotals.map((value, index) => {
                    const columnIndex = weekStartColumnIndex + index;
                    return (
                      <td
                        key={`group-${group.groupKey}-subtotal-spots-week-${index}`}
                        className={cn(
                          "border border-slate-300 px-2 py-2 text-center",
                          getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-spots`, columnIndex),
                        )}
                        onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-spots`, columnIndex)}
                      >
                        {value}
                      </td>
                    );
                  })}
                  <td
                    className={cn(
                      "border border-slate-300 px-3 py-2 text-center",
                      getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-spots`, totalSpotColumnIndex),
                    )}
                    onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-spots`, totalSpotColumnIndex)}
                  >
                    {group.subtotal.totalSpot}
                  </td>
                  <td
                    className={cn(
                      "border border-slate-300 px-3 py-2 text-right",
                      getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-spots`, totalGrossColumnIndex),
                    )}
                    onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-spots`, totalGrossColumnIndex)}
                  >
                    ${group.subtotal.totalGross}
                  </td>
                </tr>,
              );

              renderedRows.push(
                <tr key={`group-${group.groupKey}-subtotal-gross`} className="bg-slate-100 font-semibold text-slate-800">
                  <td
                    colSpan={staticColumnCount}
                    className={cn(
                      "border border-slate-300 px-3 py-2 text-right",
                      getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-gross`, 0),
                    )}
                    onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-gross`, 0)}
                  >
                    {group.groupLabel} - Total Gross
                  </td>
                  {group.subtotal.weeklyGrossTotals.map((value, index) => {
                    const columnIndex = weekStartColumnIndex + index;
                    return (
                      <td
                        key={`group-${group.groupKey}-subtotal-gross-week-${index}`}
                        className={cn(
                          "border border-slate-300 px-2 py-2 text-center",
                          getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-gross`, columnIndex),
                        )}
                        onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-gross`, columnIndex)}
                      >
                        ${value}
                      </td>
                    );
                  })}
                  <td
                    className={cn(
                      "border border-slate-300 px-3 py-2 text-center",
                      getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-gross`, totalSpotColumnIndex),
                    )}
                    onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-gross`, totalSpotColumnIndex)}
                  >
                    -
                  </td>
                  <td
                    className={cn(
                      "border border-slate-300 px-3 py-2 text-right",
                      getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-gross`, totalGrossColumnIndex),
                    )}
                    onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-gross`, totalGrossColumnIndex)}
                  >
                    -
                  </td>
                </tr>,
              );

              let subtotalMonthOffset = 0;
              renderedRows.push(
                <tr key={`group-${group.groupKey}-subtotal-month`} className="bg-slate-100 font-semibold text-slate-800">
                  <td
                    colSpan={staticColumnCount}
                    className={cn(
                      "border border-slate-300 px-3 py-2 text-right",
                      getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-month`, 0),
                    )}
                    onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-month`, 0)}
                  >
                    {group.groupLabel} - Total Gross by Month
                  </td>
                  {table.monthGroups.map((monthGroup, monthIndex) => {
                    const monthStart = weekStartColumnIndex + subtotalMonthOffset;
                    const monthEnd = monthStart + monthGroup.count - 1;
                    subtotalMonthOffset += monthGroup.count;
                    return (
                      <td
                        key={`group-${group.groupKey}-subtotal-month-${monthIndex}`}
                        colSpan={monthGroup.count}
                        className={cn(
                          "border border-slate-300 px-2 py-2 text-center",
                          getRangeHoverClass(hoverState, monthStart, monthEnd),
                        )}
                        onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-month`, monthStart)}
                      >
                        ${group.subtotal.monthlyGrossTotals[monthIndex] ?? "0.00"}
                      </td>
                    );
                  })}
                  <td
                    className={cn(
                      "border border-slate-300 px-3 py-2 text-center",
                      getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-month`, totalSpotColumnIndex),
                    )}
                    onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-month`, totalSpotColumnIndex)}
                  >
                    -
                  </td>
                  <td
                    className={cn(
                      "border border-slate-300 px-3 py-2 text-right",
                      getCellHoverClass(hoverState, `detail-${group.groupKey}-subtotal-month`, totalGrossColumnIndex),
                    )}
                    onMouseEnter={() => onHoverCell(`detail-${group.groupKey}-subtotal-month`, totalGrossColumnIndex)}
                  >
                    -
                  </td>
                </tr>,
              );

              if (groupIndex < (table.groups?.length ?? 0) - 1) {
                renderedRows.push(
                  <tr key={`group-${group.groupKey}-spacer`}>
                    <td colSpan={staticColumnCount + weekColumns.length + 2} className="h-2 border-0 bg-transparent" />
                  </tr>,
                );
              }

              return renderedRows;
            })}

            {renderTotalsRows(table, staticColumnCount, table.totals, {
              rowKeyPrefix: "detail",
              hoverState,
              onHoverCell,
              weekStartColumnIndex,
              totalSpotColumnIndex,
              totalGrossColumnIndex,
            })}
          </tbody>
        )}
      </table>
    </div>
  );
}
