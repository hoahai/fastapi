import { EntityItemCard } from "@/components/dashboard/EntityItemCard";
import { cn } from "@/lib/utils";

import type { StationItem } from "./types";

interface StationCardProps {
  station: StationItem;
}

export function StationCard({ station }: StationCardProps) {
  const firstContact = station.repContacts?.[0];
  const contactName = firstContact?.fullName?.trim() || "";
  const contactEmail = firstContact?.email?.trim() || "";

  return (
    <EntityItemCard
      rootAs="article"
      rootClassName="min-w-52 cursor-pointer p-5"
      circleClassName="mb-4 text-xl font-bold tracking-tight"
      circleContent={station.code}
      title={contactName}
      subtitle={contactEmail}
      subtitleClassName={cn("text-sm", contactEmail ? "text-slate-500" : "text-slate-400")}
    />
  );
}
