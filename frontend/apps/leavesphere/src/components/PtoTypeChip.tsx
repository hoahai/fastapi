import type { ReactNode } from "react";

type LeaveSpherePtoTypeChipTone = "pto" | "personal" | "sick";

type LeaveSpherePtoTypeChipMeta = {
  className: string;
  label: string;
};

const CHIP_META: Record<LeaveSpherePtoTypeChipTone, LeaveSpherePtoTypeChipMeta> = {
  pto: {
    className: "border-blue-200 bg-blue-50 text-blue-700",
    label: "PTO",
  },
  personal: {
    className: "border-purple-200 bg-purple-50 text-purple-700",
    label: "Personal",
  },
  sick: {
    className: "border-orange-200 bg-orange-50 text-orange-700",
    label: "Sick",
  },
};

function normalizeText(value: string): string {
  return value.trim().toLowerCase();
}

export function mapLeaveSpherePtoTypeToChipTone(type: string): LeaveSpherePtoTypeChipTone {
  const normalized = normalizeText(type);
  if (normalized.includes("sick")) {
    return "sick";
  }
  if (normalized.includes("personal")) {
    return "personal";
  }
  return "pto";
}

type LeaveSpherePtoTypeChipProps = {
  type: string;
  label?: ReactNode;
  className?: string;
};

export function LeaveSpherePtoTypeChip({ type, label, className }: LeaveSpherePtoTypeChipProps) {
  const tone = mapLeaveSpherePtoTypeToChipTone(type);
  const meta = CHIP_META[tone];
  const resolvedLabel = label ?? meta.label;
  return (
    <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-semibold leading-none ${meta.className} ${className ?? ""}`.trim()}>
      {resolvedLabel}
    </span>
  );
}
