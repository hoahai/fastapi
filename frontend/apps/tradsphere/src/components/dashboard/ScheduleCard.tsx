import { CheckCircle2 } from "lucide-react";

import { EntityItemCard } from "@/components/dashboard/EntityItemCard";

import type { EsnumItem } from "./types";

interface ScheduleCardProps {
  esnum: EsnumItem;
}

export function ScheduleCard({ esnum }: ScheduleCardProps) {
  const note = esnum.note?.trim() || "";

  return (
    <EntityItemCard
      rootAs="button"
      rootClassName="min-w-44 p-5"
      circleClassName="text-2xl font-semibold tracking-tight"
      badge={
        esnum.hasSchedule ? (
          <span className="absolute -right-6 -top-1 z-20 inline-flex items-center gap-1 rounded-full bg-emerald-100/95 px-2 py-0.5 text-[11px] font-semibold leading-none text-emerald-800 shadow-sm">
            <CheckCircle2 className="size-3" />
            Scheduled
          </span>
        ) : undefined
      }
      circleContent={esnum.estnum}
      title={esnum.name}
      subtitle={note}
      srOnlyText={esnum.hasSchedule ? "Has schedule" : "No schedule yet"}
    />
  );
}
