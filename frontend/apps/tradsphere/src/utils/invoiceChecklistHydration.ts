export type InvoiceChecklistHydrationSummary = {
  isLocalDraft?: boolean;
  stationCount: number;
  matchedStationCount?: number;
  mismatchStationCount: number;
};

export type InvoiceChecklistHydrationDetail = {
  isLocalDraft?: boolean;
  stations: Array<{ id: number; status?: string | null }>;
};

export type InvoiceChecklistHydrationInput = {
  selectedChecklist: InvoiceChecklistHydrationDetail | null;
  selectedChecklistId: string | null;
  selectedChecklistSummary: InvoiceChecklistHydrationSummary | null;
};

export function shouldTreatChecklistDetailAsHydrating(input: InvoiceChecklistHydrationInput): boolean {
  const { selectedChecklist, selectedChecklistId, selectedChecklistSummary } = input;
  if (!selectedChecklist || !selectedChecklistId) {
    return false;
  }
  if (selectedChecklist.isLocalDraft) {
    return false;
  }
  if (selectedChecklist.stations.length > 0) {
    const summaryStationCount = Math.trunc(selectedChecklistSummary?.stationCount ?? 0);
    const summaryMatchedStationCount = Math.trunc(selectedChecklistSummary?.matchedStationCount ?? 0);
    const summaryMismatchStationCount = Math.trunc(selectedChecklistSummary?.mismatchStationCount ?? 0);
    const allStationsHaveBlankStatus = selectedChecklist.stations.every((station) => !String(station.status ?? "").trim());
    const summaryLooksFullyMatched = (
      summaryStationCount > 0
      && selectedChecklist.stations.length === summaryStationCount
      && summaryMatchedStationCount >= summaryStationCount
      && summaryMismatchStationCount === 0
    );
    return Boolean(
      selectedChecklistSummary
      && !selectedChecklistSummary.isLocalDraft
      && allStationsHaveBlankStatus
      && summaryLooksFullyMatched,
    );
  }
  return Boolean(
    selectedChecklistSummary
    && !selectedChecklistSummary.isLocalDraft
    && (selectedChecklistSummary.stationCount > 0 || selectedChecklistSummary.mismatchStationCount > 0),
  );
}
