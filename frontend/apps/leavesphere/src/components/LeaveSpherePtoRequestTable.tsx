import { useEffect, useMemo, useState, type ReactNode } from "react";

import { Button } from "@tradsphere/components/ui/button";
import { LeaveSpherePtoEmployeeHeader } from "@leavesphere/components/LeaveSpherePtoEmployeeHeader";
import { LeaveSpherePtoTypeChip } from "@leavesphere/components/PtoTypeChip";
import { LeaveSpherePtoStatusChip } from "@leavesphere/components/PtoStatusChip";
import type { LeaveSpherePtoEmployeeDisplay } from "@leavesphere/lib/ptoEmployeeLookup";
import type { LeaveSpherePtoRequest } from "@leavesphere/lib/ptoTypes";
import { formatLeaveSpherePtoStatusLabel, getLeaveSpherePtoRequestSurfaceClassName, getLeaveSpherePtoRequestTimelineTone } from "@leavesphere/lib/ptoStatus";

type LeaveSpherePtoRequestTableProps = {
  requests: LeaveSpherePtoRequest[];
  emptyMessage: string;
  resolveEmployee: (request: LeaveSpherePtoRequest) => LeaveSpherePtoEmployeeDisplay;
  requestTypeLabel: (type: string) => string;
  formatSubmittedLabel: (request: LeaveSpherePtoRequest) => string;
  formatDateRangeLabel: (request: LeaveSpherePtoRequest) => string;
  formatHoursLabel: (hours: number) => string;
  onRequestClick: (request: LeaveSpherePtoRequest) => void;
  todayIsoDate: string;
  showDescription?: boolean;
  getDescriptionLabel?: (request: LeaveSpherePtoRequest) => ReactNode;
  employeeColumnClassName?: string;
  dateRangeColumnClassName?: string;
  descriptionColumnClassName?: string;
  pageSize?: number;
};

export function LeaveSpherePtoRequestTable({
  requests,
  emptyMessage,
  resolveEmployee,
  requestTypeLabel,
  formatSubmittedLabel,
  formatDateRangeLabel,
  formatHoursLabel,
  onRequestClick,
  todayIsoDate,
  showDescription = false,
  getDescriptionLabel,
  employeeColumnClassName,
  dateRangeColumnClassName,
  descriptionColumnClassName,
  pageSize = 20,
}: LeaveSpherePtoRequestTableProps) {
  const colSpan = showDescription ? 6 : 5;
  const normalizedPageSize = Math.max(1, Math.trunc(pageSize || 20));
  const sortedRequests = useMemo(() => {
    return [...requests].sort((left, right) => {
      const startDateCompare = right.startDate.localeCompare(left.startDate);
      if (startDateCompare !== 0) {
        return startDateCompare;
      }

      const leftEmployeeName = resolveEmployee(left).employeeName;
      const rightEmployeeName = resolveEmployee(right).employeeName;
      const employeeCompare = leftEmployeeName.localeCompare(rightEmployeeName);
      if (employeeCompare !== 0) {
        return employeeCompare;
      }

      const submittedAtCompare = right.submittedAt.localeCompare(left.submittedAt);
      if (submittedAtCompare !== 0) {
        return submittedAtCompare;
      }

      return left.id.localeCompare(right.id);
    });
  }, [requests, resolveEmployee]);
  const requestSignature = useMemo(() => sortedRequests.map((request) => request.id).join("::"), [sortedRequests]);
  const [currentPage, setCurrentPage] = useState(1);
  const totalPages = Math.max(1, Math.ceil(sortedRequests.length / normalizedPageSize));

  useEffect(() => {
    setCurrentPage(1);
  }, [requestSignature, normalizedPageSize]);

  useEffect(() => {
    setCurrentPage((page) => Math.min(Math.max(1, page), totalPages));
  }, [totalPages]);

  const visibleRequests = useMemo(() => {
    const startIndex = (currentPage - 1) * normalizedPageSize;
    return sortedRequests.slice(startIndex, startIndex + normalizedPageSize);
  }, [currentPage, normalizedPageSize, sortedRequests]);
  const startIndex = sortedRequests.length === 0 ? 0 : ((currentPage - 1) * normalizedPageSize) + 1;
  const endIndex = Math.min(sortedRequests.length, currentPage * normalizedPageSize);

  return (
    <div className="overflow-hidden rounded-xl border border-blue-100">
      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-sm">
          <thead className="bg-blue-50/70 text-xs uppercase tracking-[0.08em] text-slate-600">
            <tr>
              <th className={`${employeeColumnClassName ?? "px-3 py-2.5"}`}>Employee</th>
              <th className="px-3 py-2.5 text-center">Type</th>
              <th className={`${dateRangeColumnClassName ?? "px-3 py-2.5"}`}>Date range</th>
              {showDescription ? <th className={`${descriptionColumnClassName ?? "px-3 py-2.5"}`}>Description</th> : null}
              <th className="px-3 py-2.5 text-center">Status</th>
              <th className="px-3 py-2.5 text-center">Hours</th>
            </tr>
          </thead>
          <tbody>
            {sortedRequests.length === 0 ? (
              <tr>
                <td colSpan={colSpan} className="px-3 py-6 text-center text-sm text-slate-600">
                  {emptyMessage}
                </td>
              </tr>
            ) : visibleRequests.map((request) => {
              const employee = resolveEmployee(request);
              const typeLabel = requestTypeLabel(request.ptoTypeCode ?? request.type);
              const rowTone = getLeaveSpherePtoRequestTimelineTone({
                status: request.status,
                startDate: request.startDate,
                endDate: request.endDate,
                todayIsoDate,
              });
              return (
                <tr
                  key={request.id}
                  className={`cursor-pointer ${getLeaveSpherePtoRequestSurfaceClassName(rowTone, "row")}`}
                  onClick={() => onRequestClick(request)}
                >
                  <td className={`${employeeColumnClassName ?? "px-3 py-2.5"}`}>
                    <LeaveSpherePtoEmployeeHeader
                      employeeName={employee.employeeName}
                      pictureUrl={employee.pictureUrl}
                      title={employee.employeeName}
                      subtitle={formatSubmittedLabel(request)}
                      titleClassName="font-medium"
                    />
                  </td>
                  <td className="px-3 py-2.5 text-center">
                    <LeaveSpherePtoTypeChip type={typeLabel} label={typeLabel} />
                  </td>
                  <td className={`${dateRangeColumnClassName ?? "px-3 py-2.5 whitespace-nowrap"}`}>{formatDateRangeLabel(request)}</td>
                  {showDescription ? (
                    <td className={`${descriptionColumnClassName ?? "px-3 py-2.5 text-slate-600"}`}>
                      {getDescriptionLabel ? getDescriptionLabel(request) : request.description || "-"}
                    </td>
                  ) : null}
                  <td className="px-3 py-2.5 text-center">
                    <LeaveSpherePtoStatusChip status={request.status} label={formatLeaveSpherePtoStatusLabel(request.status)} />
                  </td>
                  <td className="px-3 py-2.5 text-center">{formatHoursLabel(request.hours)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {sortedRequests.length > 0 && totalPages > 1 ? (
        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-blue-100 bg-white px-3 py-2.5 text-sm text-slate-600">
          <p>
            Showing {startIndex}-{endIndex} of {sortedRequests.length} requests
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCurrentPage((page) => Math.max(1, page - 1))}
              disabled={currentPage <= 1}
            >
              Previous
            </Button>
            <span className="min-w-20 text-center text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">
              Page {currentPage} / {totalPages}
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCurrentPage((page) => Math.min(totalPages, page + 1))}
              disabled={currentPage >= totalPages}
            >
              Next
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
