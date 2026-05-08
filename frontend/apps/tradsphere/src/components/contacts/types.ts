export type ContactSearchFormValues = {
  name: string;
  email: string;
  company: string;
  phone: string;
  station: string;
};

export type ContactUsageRow = {
  linkId: number | null;
  stationCode: string;
  stationName: string;
  mediaType: string;
  market: string | null;
  contactType: string;
  primaryContact: boolean;
  active: boolean;
};

export type ContactAccountUsage = {
  accountCode: string;
  accountName: string;
};

export type ContactEstNumUsage = {
  estNum: number;
  accountCode: string;
  accountName: string;
  month: number | null;
  quarter: number | null;
  year: number | null;
  periodLabel: string;
  mediaType: string;
  broadcastMonths: number[];
  broadcastYears: number[];
  stationCodes: string[];
};

export type ContactRecord = {
  id: number;
  email: string;
  firstName: string;
  lastName: string;
  fullName: string;
  company: string;
  jobTitle: string;
  office: string;
  cell: string;
  note: string;
  active: boolean;
  stationCodes: string[];
  contactTypes: string[];
  usage: ContactUsageRow[];
  usedByAccounts: ContactAccountUsage[];
  usedByEstNums: ContactEstNumUsage[];
  usedByStationCount: number;
  usedByAccountCount: number;
  usedByEstNumCount: number;
  isPrimaryContact: boolean;
};

export type ContactGroup = {
  key: string;
  label: string;
  items: ContactRecord[];
};
