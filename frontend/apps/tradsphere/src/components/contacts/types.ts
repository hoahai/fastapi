export type ContactSearchFormValues = {
  name: string;
  email: string;
  company: string;
  contactType: string;
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
  usedByStationCount: number;
  isPrimaryContact: boolean;
};

export type ContactGroup = {
  key: string;
  label: string;
  items: ContactRecord[];
};
