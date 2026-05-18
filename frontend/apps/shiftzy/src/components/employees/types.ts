import type { ShiftzyEmployee } from "@shiftzy/lib/shiftzyApi";

export type ShiftzyEmployeeSearchFormValues = {
  name: string;
  scheduleSection: string;
  positionCode: string;
  status: "" | "active" | "inactive";
};

export type ShiftzyEmployeeItem = ShiftzyEmployee & {
  positionName: string;
};

export type ShiftzyEmployeeSectionGroup = {
  key: string;
  label: string;
  items: ShiftzyEmployeeItem[];
};
