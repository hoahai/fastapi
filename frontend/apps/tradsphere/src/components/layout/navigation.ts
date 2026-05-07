import {
  CalendarClock,
  LayoutDashboard,
  RadioTower,
  type LucideIcon,
  WalletCards,
  Wrench,
} from "lucide-react";

export type AppNavItem = {
  id: string;
  label: string;
  description: string;
  route: string;
  icon: LucideIcon;
  available: boolean;
};

export const HOME_ROUTE = "/";

export const APP_NAV_ITEMS: AppNavItem[] = [
  {
    id: "tradsphere",
    label: "Tradsphere",
    description: "Broadcast schedule workspace",
    route: "/tradsphere/home",
    icon: RadioTower,
    available: true,
  },
  {
    id: "spendsphere",
    label: "Spendsphere",
    description: "Coming soon",
    route: "/spendsphere/home",
    icon: WalletCards,
    available: false,
  },
  {
    id: "fundsphere",
    label: "Fundsphere",
    description: "Coming soon",
    route: "/fundsphere/home",
    icon: LayoutDashboard,
    available: false,
  },
  {
    id: "opssphere",
    label: "Opssphere",
    description: "Coming soon",
    route: "/opssphere/home",
    icon: Wrench,
    available: false,
  },
  {
    id: "shiftzy",
    label: "Shiftzy",
    description: "Coming soon",
    route: "/shiftzy/home",
    icon: CalendarClock,
    available: false,
  },
];
