export type StationSearchFormValues = {
  stationCode: string;
  stationName: string;
  mediaType: string;
  language: string;
  affiliation: string;
  contact: string;
};

export type StationContactSummary = {
  id: number | null;
  contactType: string;
  fullName: string;
  email: string;
  primaryContact: boolean;
};

export type StationDeliveryMethodSummary = {
  id: number | null;
  name: string;
  url: string;
  username: string;
  deadline: string;
  note: string;
};

export type StationRecord = {
  code: string;
  name: string;
  mediaType: string;
  syscode: string;
  language: string;
  affiliation: string;
  deliveryMethodId: number | null;
  deliveryMethod: StationDeliveryMethodSummary | null;
  contacts: StationContactSummary[];
  repContacts: StationContactSummary[];
  note: string;
};

export type StationGroup = {
  key: string;
  label: string;
  items: StationRecord[];
};
