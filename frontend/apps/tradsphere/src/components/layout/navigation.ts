import {
  CalendarClock,
  House,
  LayoutDashboard,
  RadioTower,
  Search,
  type LucideIcon,
  WalletCards,
  Wrench,
} from "lucide-react";

export type AppNavChildItem = {
  id: string;
  label: string;
  route: string;
  available: boolean;
};

export type AppNavItem = {
  id: string;
  label: string;
  description: string;
  route: string;
  icon: LucideIcon;
  available: boolean;
  activeMatchPrefix?: string;
  children?: AppNavChildItem[];
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
    activeMatchPrefix: "/tradsphere/",
    children: [
      {
        id: "tradsphere-home",
        label: "Tradsphere Home",
        route: "/tradsphere/home",
        available: true,
      },
      {
        id: "tradsphere-estnums",
        label: "Estimate Numbers",
        route: "/tradsphere/estnums",
        available: true,
      },
    ],
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

export const HOME_CHILD_ITEM: AppNavChildItem = {
  id: "workspace-home",
  label: "Portal / Workspace Home",
  route: HOME_ROUTE,
  available: true,
};

export const TRADSPHERE_HOME_CHILD_ICON = House;
export const TRADSPHERE_ESTNUMS_CHILD_ICON = Search;
