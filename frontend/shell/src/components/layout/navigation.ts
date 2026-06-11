import {
  CalendarClock,
  ClipboardCheck,
  ContactRound,
  House,
  LayoutDashboard,
  MapPinned,
  Plane,
  RadioTower,
  Send,
  Search,
  Umbrella,
  UserCheck,
  Users,
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
    label: "TradSphere",
    description: "Broadcast schedule workspace",
    route: "/tradsphere/home",
    icon: RadioTower,
    available: true,
    activeMatchPrefix: "/tradsphere/",
    children: [
      {
        id: "tradsphere-home",
        label: "Accounts",
        route: "/tradsphere/home",
        available: true,
      },
      {
        id: "tradsphere-estnums",
        label: "Estimate Numbers",
        route: "/tradsphere/estnums",
        available: true,
      },
      {
        id: "tradsphere-contacts",
        label: "Contacts",
        route: "/tradsphere/contacts",
        available: true,
      },
      {
        id: "tradsphere-stations",
        label: "Stations",
        route: "/tradsphere/stations",
        available: true,
      },
      {
        id: "tradsphere-traffic",
        label: "Traffic",
        route: "/tradsphere/traffic",
        available: true,
      },
      {
        id: "tradsphere-invoice-checklists",
        label: "Invoice Checklists",
        route: "/tradsphere/invoice-checklists",
        available: true,
      },
    ],
  },
  {
    id: "leavesphere",
    label: "LeaveSphere",
    description: "PTO and request management",
    route: "/leavesphere/home",
    icon: Umbrella,
    available: true,
    activeMatchPrefix: "/leavesphere/",
    children: [
      {
        id: "leavesphere-home",
        label: "My PTO",
        route: "/leavesphere/home",
        available: true,
      },
      {
        id: "leavesphere-admin-pto",
        label: "Leave Management",
        route: "/leavesphere/admin-pto",
        available: true,
      },
    ],
  },
  {
    id: "spendsphere",
    label: "SpendSphere",
    description: "Coming soon",
    route: "/spendsphere/home",
    icon: WalletCards,
    available: false,
  },
  {
    id: "fundsphere",
    label: "FundSphere",
    description: "Coming soon",
    route: "/fundsphere/home",
    icon: LayoutDashboard,
    available: false,
  },
  {
    id: "opssphere",
    label: "OpsSphere",
    description: "Coming soon",
    route: "/opssphere/home",
    icon: Wrench,
    available: false,
  },
  {
    id: "shiftzy",
    label: "Shiftzy",
    description: "Workforce schedule maker",
    route: "/shiftzy/home",
    icon: CalendarClock,
    available: true,
    activeMatchPrefix: "/shiftzy/",
    children: [
      {
        id: "shiftzy-schedules",
        label: "Schedules",
        route: "/shiftzy/home",
        available: true,
      },
      {
        id: "shiftzy-employees",
        label: "Employees",
        route: "/shiftzy/employees",
        available: true,
      },
    ],
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
export const TRADSPHERE_CONTACTS_CHILD_ICON = ContactRound;
export const TRADSPHERE_STATIONS_CHILD_ICON = MapPinned;
export const TRADSPHERE_TRAFFIC_CHILD_ICON = Send;
export const TRADSPHERE_INVOICE_CHECKLISTS_CHILD_ICON = ClipboardCheck;
export const TRADSPHERE_USERS_ACCESS_CHILD_ICON = UserCheck;
export const SHIFTZY_SCHEDULES_CHILD_ICON = CalendarClock;
export const SHIFTZY_ACCOUNTS_CHILD_ICON = Users;
export const LEAVESPHERE_MY_PTO_CHILD_ICON = Plane;
export const LEAVESPHERE_ADMIN_PTO_CHILD_ICON = Users;
