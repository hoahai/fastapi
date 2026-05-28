export type AccountSelection = {
  accountCode: string;
  label: string;
  name?: string;
  active?: boolean;
};

export type AccountInfo = {
  code: string;
  name: string;
  logoUrl?: string | null;
  billingType?: string | null;
  market?: string | null;
  note?: string | null;
  active?: boolean;
};

export type EsnumItem = {
  estnum: number;
  name: string;
  hasSchedule: boolean;
  note?: string | null;
};

export type StationRepContact = {
  fullName?: string | null;
  email?: string | null;
};

export type StationItem = {
  code: string;
  name: string;
  deliveryMethodId?: number | null;
  mediaType?: string | null;
  repContacts?: StationRepContact[];
};

export type ApiMainLoadResponse = {
  meta?: {
    timestamp?: string;
    duration_ms?: number;
    duration_hms?: string;
    client_id?: string;
    request_id?: string;
  };
  data: {
    account: AccountInfo;
    esnums?: EsnumItem[];
    stations?: StationItem[];
  };
};

export type MainLoadResponse = {
  account: AccountInfo;
  esnums: EsnumItem[];
  stations: StationItem[];
};
