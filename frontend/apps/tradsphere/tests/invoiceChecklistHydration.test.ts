import assert from "node:assert/strict";

import { shouldTreatChecklistDetailAsHydrating } from "../src/utils/invoiceChecklistHydration.ts";

function run(): void {
  assert.equal(
    shouldTreatChecklistDetailAsHydrating({
      selectedChecklistId: "checklist-1",
      selectedChecklist: {
        stations: [],
      },
      selectedChecklistSummary: {
        stationCount: 3,
        mismatchStationCount: 0,
      },
    }),
    true,
    "an empty selected detail with a populated summary should be treated as hydrating",
  );

  assert.equal(
    shouldTreatChecklistDetailAsHydrating({
      selectedChecklistId: "checklist-1",
      selectedChecklist: {
        stations: [{ id: 11, status: "MATCHED" }],
      },
      selectedChecklistSummary: {
        stationCount: 3,
        matchedStationCount: 3,
        mismatchStationCount: 0,
      },
    }),
    false,
    "a checklist with real stations should not be treated as hydrating",
  );

  assert.equal(
    shouldTreatChecklistDetailAsHydrating({
      selectedChecklistId: "checklist-1",
      selectedChecklist: {
        isLocalDraft: true,
        stations: [],
      },
      selectedChecklistSummary: {
        stationCount: 3,
        mismatchStationCount: 0,
      },
    }),
    false,
    "local drafts should never be treated as hydrating",
  );

  assert.equal(
    shouldTreatChecklistDetailAsHydrating({
      selectedChecklistId: "checklist-1",
      selectedChecklist: {
        stations: [],
      },
      selectedChecklistSummary: {
        stationCount: 0,
        matchedStationCount: 0,
        mismatchStationCount: 0,
      },
    }),
    false,
    "an empty summary should not be treated as hydrating",
  );

  assert.equal(
    shouldTreatChecklistDetailAsHydrating({
      selectedChecklistId: "checklist-1",
      selectedChecklist: {
        stations: [
          { id: 11, status: "" },
          { id: 12, status: null },
        ],
      },
      selectedChecklistSummary: {
        stationCount: 2,
        matchedStationCount: 2,
        mismatchStationCount: 0,
      },
    }),
    true,
    "a fully matched summary with blank station statuses should be treated as hydrating",
  );
}

run();
console.log("invoiceChecklistHydration.test.ts passed");
