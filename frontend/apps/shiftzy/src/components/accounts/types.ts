import type { ShiftzyEmployee } from "@shiftzy/lib/shiftzyApi";

export type ShiftzyAccountSearchFormValues = {
  name: string;
  scheduleSection: string;
  positionCode: string;
  status: "" | "active" | "inactive";
};

export type ShiftzyAccountItem = ShiftzyEmployee & {
  positionName: string;
};

export type ShiftzyAccountSectionGroup = {
  key: string;
  label: string;
  items: ShiftzyAccountItem[];
};
