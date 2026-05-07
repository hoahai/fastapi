export type EstimateSearchItem = {
  estNum: number;
  accountCode: string;
  accountName?: string | null;
  buyer?: string | null;
  mediaType?: string | null;
  note?: string | null;
  hasSchedule: boolean;
  flightStart?: string | null;
  flightEnd?: string | null;
  year?: number | null;
  quarter?: number | null;
  month?: number | null;
  broadcastYears?: number[];
  broadcastMonths?: number[];
};

export type EstimateSearchPage = {
  items: EstimateSearchItem[];
  total: number | null;
  limit: number;
  nextCursor: string | null;
  nextOffset: number | null;
  backendMode: "search" | "legacy-exact";
};

export type EstimateYearGroup = {
  key: string;
  label: string;
  items: EstimateSearchItem[];
};

export type EstimateAccountGroup = {
  key: string;
  accountCode: string;
  accountName?: string | null;
  years: EstimateYearGroup[];
};
