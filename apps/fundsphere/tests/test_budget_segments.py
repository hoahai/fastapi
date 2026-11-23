from __future__ import annotations

from decimal import Decimal
import json
import unittest
from unittest.mock import patch

from apps.fundsphere.api.v1.helpers.dbQueries import (
    BudgetSegmentValidationError,
    _fetch_master_budget_control_budget_rows_for_buckets,
    _filter_budget_rows_by_service_names,
    _format_budget_change_history_line,
    replace_budget_segments,
    validate_budget_segment_rows,
)
from apps.fundsphere.api.v1.endpoints.masterBudgetControl.budgetSegments import (
    BudgetSegmentsReplaceRequest,
)
from apps.fundsphere.api.v1.endpoints.masterBudgetControl.budgetData import (
    _build_budget_data_values,
    _extract_service_names,
    _parse_segment_signature,
    _parse_budget_data_sheet_rows,
)


class _FakeCursor:
    def __init__(self, budget_id: str):
        self.budget_id = budget_id
        self.rowcount = 0
        self.inserted_rows: list[tuple] = []
        self.executed: list[tuple[str, tuple | None]] = []

    def execute(self, query: str, params: tuple | None = None):
        self.executed.append((query, params))
        if query.startswith("DELETE"):
            self.rowcount = 2

    def fetchone(self):
        return (self.budget_id,)

    def executemany(self, _query: str, rows: list[tuple]):
        self.inserted_rows = rows
        self.rowcount = len(rows)


class BudgetSegmentsTests(unittest.TestCase):
    def test_budget_history_summary_uses_readable_single_line_language(self):
        segment_line = _format_budget_change_history_line(
            {
                "changedBy": "hai",
                "actionType": "UPDATE",
                "changedFields": json.dumps(
                    [
                        "segment.Service.objectivePercent",
                        "segment.Service.English",
                    ]
                ),
                "oldData": json.dumps(
                    {
                        "segment.Service.objectivePercent": None,
                        "segment.Service.English": None,
                    }
                ),
                "newData": json.dumps(
                    {
                        "segment.Service.objectivePercent": "100.00",
                        "segment.Service.English": "100.00",
                    }
                ),
            }
        )
        self.assertEqual(
            segment_line,
            "hai added budget segments: Service objective none → 100.00%; "
            "Service / English none → 100.00%",
        )
        self.assertNotIn("\n", segment_line)

        create_line = _format_budget_change_history_line(
            {
                "changedBy": "sheryl",
                "actionType": "CREATE",
                "changedFields": json.dumps(
                    ["subService", "grossAmount", "commission", "netAdjustment"]
                ),
                "oldData": "{}",
                "newData": json.dumps(
                    {
                        "subService": "Service Google Ads",
                        "grossAmount": "1250.00",
                        "commission": "32.0000",
                        "netAdjustment": "0.00",
                    }
                ),
            }
        )
        self.assertEqual(
            create_line,
            "sheryl created budget: sub-service Service Google Ads; "
            "gross amount 1250.00; commission 32.0000; "
            "net adjustment 0.00",
        )

    def test_budget_service_selector_accepts_blank_and_multiple_values(self):
        self.assertEqual(_extract_service_names(""), [])
        self.assertEqual(
            _extract_service_names(" Search Engine Marketing,\nDigital Display Ads "),
            ["Search Engine Marketing", "Digital Display Ads"],
        )

    def test_budget_service_selector_filters_rows_case_insensitively(self):
        rows = [
            {"budgetId": "sem-1", "serviceName": "Search Engine Marketing"},
            {"budgetId": "display-1", "serviceName": "Digital Display Ads"},
        ]

        self.assertEqual(
            _filter_budget_rows_by_service_names(rows, ["digital display ads"]),
            [rows[1]],
        )
        self.assertEqual(_filter_budget_rows_by_service_names(rows, []), rows)

    def test_budget_db_query_restricts_selected_services(self):
        columns = {
            "account_code_col": "accountCode",
            "account_name_col": "accountName",
            "service_id_col": "serviceId",
            "service_name_col": "serviceName",
            "service_department_code_col": "departmentCode",
            "department_code_col": "code",
            "department_listing_order_col": "listingOrder",
            "budget_id_col": "id",
            "budget_account_code_col": "accountCode",
            "budget_service_id_col": "serviceId",
            "budget_year_col": "year",
            "budget_month_col": "month",
            "budget_sub_service_col": "subService",
            "budget_gross_amount_col": "grossAmount",
            "budget_commission_col": "commission",
            "budget_net_adjustment_col": "netAdjustment",
            "budget_note_col": "note",
        }

        with patch(
            "apps.fundsphere.api.v1.helpers.dbQueries._resolve_budget_data_columns",
            return_value=columns,
        ), patch(
            "apps.fundsphere.api.v1.helpers.dbQueries.fetch_all",
            return_value=[{"budgetId": ""}],
        ) as fetch_all:
            _fetch_master_budget_control_budget_rows_for_buckets(
                accounts_table="Accounts",
                budgets_table="Budgets",
                services_table="Services",
                departments_table="Departments",
                budget_segment_objectives_table="",
                budget_segment_languages_table="",
                budget_change_histories_table="BudgetChangeHistories",
                buckets=[("ACME", 3, 2026)],
                service_names=["Digital Display Ads"],
            )

        query, params = fetch_all.call_args.args
        self.assertIn("LOWER(s.`serviceName`) IN (%s)", query)
        self.assertEqual(params[0], "digital display ads")
        self.assertEqual(params[1:], ("ACME", 3, 2026))

    def test_budget_data_segment_preview_round_trips_as_a_change(self):
        row = {
            "budgetId": "budget-1",
            "accountName": "Acme",
            "year": 2026,
            "month": 3,
            "serviceName": "SEO",
            "subService": "Retail",
            "grossAmount": "1000.00",
            "commission": "0.2500",
            "netAdjustment": "0.00",
            "note": "",
            "segments": [
                {
                    "objective": "New",
                    "objectivePercent": Decimal("60.00"),
                    "language": "English",
                    "segmentPercent": "40.00",
                },
                {
                    "objective": "New",
                    "objectivePercent": Decimal("60.00"),
                    "language": "Spanish",
                    "segmentPercent": "60.00",
                },
                {
                    "objective": "Used",
                    "objectivePercent": Decimal("40.00"),
                    "language": "English",
                    "segmentPercent": "60.00",
                },
                {
                    "objective": "Used",
                    "objectivePercent": "40.00",
                    "language": "Spanish",
                    "segmentPercent": "40.00",
                },
            ],
        }
        values = _build_budget_data_values([row], include_segments=True)[1]
        self.assertEqual(
            values[0],
            "subService=Retail|grossAmount=1000.00|commission=0.2500|"
            "netAdjustment=0.00|note=|segments="
            "new-objective=60.00;new-english=40.00;new-spanish=60.00;"
            "used-objective=40.00;used-english=60.00;used-spanish=40.00",
        )
        self.assertEqual(
            values[11],
            "New60:E40/S60, Used40:E60/S40",
        )
        values[11] = "Used40:S45/E55, New60:S55/E45"
        values[12] = "Adjust segment allocation"

        changed, creates, deletes, errors = _parse_budget_data_sheet_rows(
            [values],
            start_row=6,
            column_indexes={
                "originalSig": 0,
                "budgetId": 1,
                "year": 3,
                "month": 4,
                "subService": 6,
                "grossAmount": 7,
                "commission": 8,
                "netAdjustment": 9,
                "note": 10,
                "segment": 11,
                "changeNote": 12,
                "accountCode": 15,
                "serviceId": 21,
            },
            include_segments=True,
        )

        self.assertEqual(creates, [])
        self.assertEqual(deletes, [])
        self.assertEqual(errors, [])
        self.assertTrue(changed[0]["segmentsChanged"])
        self.assertEqual(
            changed[0]["segmentDiffs"],
            [
                {
                    "field": "segment.New.English",
                    "oldValue": "40.00",
                    "newValue": "45.00",
                },
                {
                    "field": "segment.New.Spanish",
                    "oldValue": "60.00",
                    "newValue": "55.00",
                },
                {
                    "field": "segment.Used.English",
                    "oldValue": "60.00",
                    "newValue": "55.00",
                },
                {
                    "field": "segment.Used.Spanish",
                    "oldValue": "40.00",
                    "newValue": "45.00",
                },
            ],
        )

    def test_legacy_segment_signature_remains_readable(self):
        self.assertEqual(
            _parse_segment_signature(
                "Service=100.00(English=100.00)"
            ),
            [
                {
                    "objective": "Service",
                    "objectivePercent": Decimal("100.00"),
                    "language": "English",
                    "segmentPercent": Decimal("100.00"),
                }
            ],
        )

    def test_budget_data_segment_preview_rejects_invalid_values(self):
        row = {
            "budgetId": "budget-1",
            "year": 2026,
            "month": 3,
            "subService": "Retail",
            "grossAmount": "1000.00",
            "commission": "0.2500",
            "netAdjustment": "0.00",
            "note": "",
            "segments": [],
        }
        values = _build_budget_data_values([row], include_segments=True)[1]
        values[11] = "New100:E101"

        _changed, _creates, _deletes, errors = _parse_budget_data_sheet_rows(
            [values],
            start_row=6,
            column_indexes={
                "originalSig": 0,
                "budgetId": 1,
                "year": 3,
                "month": 4,
                "subService": 6,
                "grossAmount": 7,
                "commission": 8,
                "netAdjustment": 9,
                "note": 10,
                "segment": 11,
                "changeNote": 12,
                "accountCode": 15,
                "serviceId": 21,
            },
            include_segments=True,
        )

        self.assertTrue(any(error["field"] == "segment.segmentPercent" for error in errors))

    def test_existing_row_detects_segment_removal_without_is_row_changed_flag(self):
        row = {
            "budgetId": "budget-1",
            "accountName": "Acme",
            "year": 2026,
            "month": 3,
            "serviceName": "SEO",
            "subService": "Retail",
            "grossAmount": "1000.00",
            "commission": "0.2500",
            "netAdjustment": "0.00",
            "note": "",
            "segments": [
                {
                    "objective": "Service",
                    "objectivePercent": "100.00",
                    "language": "English",
                    "segmentPercent": "100.00",
                }
            ],
        }
        values = _build_budget_data_values([row], include_segments=True)[1]
        values[11] = ""

        changed, creates, deletes, errors = _parse_budget_data_sheet_rows(
            [values],
            start_row=6,
            column_indexes={
                "originalSig": 0,
                "budgetId": 1,
                "year": 3,
                "month": 4,
                "subService": 6,
                "grossAmount": 7,
                "commission": 8,
                "netAdjustment": 9,
                "note": 10,
                "segment": 11,
                "changeNote": 12,
                "accountCode": 15,
                "serviceId": 21,
            },
            is_row_changed_index=26,
            include_segments=True,
        )

        self.assertEqual(creates, [])
        self.assertEqual(deletes, [])
        self.assertEqual(errors, [])
        self.assertEqual(
            [diff["field"] for diff in changed[0]["segmentDiffs"]],
            ["segment.Service.objectivePercent", "segment.Service.English"],
        )

    def test_existing_row_detects_note_removal_without_is_row_changed_flag(self):
        row = {
            "budgetId": "budget-1",
            "accountName": "Acme",
            "year": 2026,
            "month": 3,
            "serviceName": "SEO",
            "subService": "Retail",
            "grossAmount": "1000.00",
            "commission": "0.2500",
            "netAdjustment": "0.00",
            "note": "Original note",
            "segments": [],
        }
        values = _build_budget_data_values([row], include_segments=True)[1]
        values[10] = ""

        changed, creates, deletes, errors = _parse_budget_data_sheet_rows(
            [values],
            start_row=6,
            column_indexes={
                "originalSig": 0,
                "budgetId": 1,
                "year": 3,
                "month": 4,
                "subService": 6,
                "grossAmount": 7,
                "commission": 8,
                "netAdjustment": 9,
                "note": 10,
                "segment": 11,
                "changeNote": 12,
                "accountCode": 15,
                "serviceId": 21,
            },
            is_row_changed_index=26,
            include_segments=True,
        )

        self.assertEqual(creates, [])
        self.assertEqual(deletes, [])
        self.assertEqual(errors, [])
        self.assertEqual(changed[0]["diffs"][0]["field"], "note")

    def test_segment_rows_are_normalized_and_sorted(self):
        rows = validate_budget_segment_rows(
            [
                {
                    "objective": "Used",
                    "objectivePercent": "40.00",
                    "language": "Spanish",
                    "segmentPercent": "60.00",
                },
                {
                    "objective": "New",
                    "objectivePercent": Decimal("60"),
                    "language": "English",
                    "segmentPercent": Decimal("40"),
                },
                {
                    "objective": "New",
                    "objectivePercent": Decimal("60"),
                    "language": "Spanish",
                    "segmentPercent": Decimal("60"),
                },
                {
                    "objective": "Used",
                    "objectivePercent": Decimal("40"),
                    "language": "English",
                    "segmentPercent": Decimal("40"),
                },
            ]
        )

        self.assertEqual(
            rows,
            [
                {
                    "objective": "New",
                    "objectivePercent": Decimal("60.00"),
                    "language": "English",
                    "segmentPercent": Decimal("40.00"),
                },
                {
                    "objective": "New",
                    "objectivePercent": Decimal("60.00"),
                    "language": "Spanish",
                    "segmentPercent": Decimal("60.00"),
                },
                {
                    "objective": "Used",
                    "objectivePercent": Decimal("40.00"),
                    "language": "English",
                    "segmentPercent": Decimal("40.00"),
                },
                {
                    "objective": "Used",
                    "objectivePercent": Decimal("40.00"),
                    "language": "Spanish",
                    "segmentPercent": Decimal("60.00"),
                },
            ],
        )

    def test_segment_objective_totals_must_equal_100(self):
        with self.assertRaises(BudgetSegmentValidationError) as context:
            validate_budget_segment_rows(
                [
                    {
                        "objective": "New",
                        "objectivePercent": "60.00",
                        "language": "English",
                        "segmentPercent": "70.00",
                    },
                    {
                        "objective": "New",
                        "objectivePercent": "60.00",
                        "language": "Spanish",
                        "segmentPercent": "30.00",
                    },
                    {
                        "objective": "Used",
                        "objectivePercent": "30.00",
                        "language": "English",
                        "segmentPercent": "100.00",
                    },
                ]
            )

        self.assertEqual(
            [error["field"] for error in context.exception.errors],
            ["objectiveTotal"],
        )

    def test_segment_ratio_is_valid_within_each_objective(self):
        rows = validate_budget_segment_rows(
            [
                {
                    "objective": "New",
                    "objectivePercent": "60.00",
                    "language": "English",
                    "segmentPercent": "70.00",
                },
                {
                    "objective": "New",
                    "objectivePercent": "60.00",
                    "language": "Spanish",
                    "segmentPercent": "30.00",
                },
                {
                    "objective": "Used",
                    "objectivePercent": "40.00",
                    "language": "English",
                    "segmentPercent": "100.00",
                },
            ]
        )

        self.assertEqual(len(rows), 3)

    def test_segment_rows_reject_duplicate_keys_and_out_of_range_percent(self):
        with self.assertRaises(BudgetSegmentValidationError) as context:
            validate_budget_segment_rows(
                [
                    {
                        "objective": "New",
                        "objectivePercent": 60,
                        "language": "English",
                        "segmentPercent": 40,
                    },
                    {
                        "objective": "New",
                        "objectivePercent": 60,
                        "language": "English",
                        "segmentPercent": 61,
                    },
                    {
                        "objective": "Used",
                        "objectivePercent": 40,
                        "language": "Spanish",
                        "segmentPercent": 101,
                    },
                ]
            )

        messages = [str(error["message"]) for error in context.exception.errors]
        self.assertIn("Duplicate objective/language combination", messages)
        self.assertIn("segmentPercent must be between 0 and 100", messages)

    def test_replace_budget_segments_is_transactional_and_replaces_collection(self):
        cursor = _FakeCursor("budget-1")

        def run_transaction(work):
            return work(cursor)

        with (
            patch(
                "apps.fundsphere.api.v1.helpers.dbQueries.get_db_tables",
                return_value={
                    "BUDGETS": "Budgets",
                    "BUDGETSEGMENTOBJECTIVES": "BudgetSegmentObjective",
                    "BUDGETSEGMENTLANGUAGES": "BudgetSegmentLanguage",
                },
            ),
            patch(
                "apps.fundsphere.api.v1.helpers.dbQueries._resolve_budget_segment_columns",
                return_value={
                    "objective_id_col": "id",
                    "objective_budget_id_col": "budgetId",
                    "objective_col": "objective",
                    "objective_percent_col": "objectivePercent",
                    "language_id_col": "id",
                    "language_objective_id_col": "budgetSegmentObjectiveId",
                    "language_col": "language",
                    "segment_percent_col": "segmentPercent",
                    "budget_id_col": "id",
                },
            ),
            patch(
                "apps.fundsphere.api.v1.helpers.dbQueries.run_transaction",
                side_effect=run_transaction,
            ),
            patch(
                "apps.fundsphere.api.v1.helpers.dbQueries.get_budget_segments",
                return_value=[],
            ),
        ):
            result = replace_budget_segments(
                budget_id="budget-1",
                segments=[
                    {
                        "objective": "New",
                        "objectivePercent": Decimal("60.00"),
                        "language": "English",
                        "segmentPercent": Decimal("40.00"),
                    },
                    {
                        "objective": "New",
                        "objectivePercent": Decimal("60.00"),
                        "language": "Spanish",
                        "segmentPercent": Decimal("60.00"),
                    },
                    {
                        "objective": "Used",
                        "objectivePercent": Decimal("40.00"),
                        "language": "English",
                        "segmentPercent": Decimal("60.00"),
                    },
                    {
                        "objective": "Used",
                        "objectivePercent": Decimal("40.00"),
                        "language": "Spanish",
                        "segmentPercent": Decimal("40.00"),
                    },
                ],
            )

        self.assertEqual(result["budgetId"], "budget-1")
        self.assertEqual(result["deletedCount"], 2)
        self.assertEqual(result["writtenRowCount"], 4)
        self.assertEqual(len(cursor.inserted_rows), 4)
        self.assertEqual(
            cursor.inserted_rows[0][2:],
            ("English", Decimal("40.00")),
        )

    def test_replace_request_rejects_unknown_fields(self):
        with self.assertRaises(ValueError):
            BudgetSegmentsReplaceRequest.model_validate(
                {
                    "segments": [
                        {
                            "objective": "New",
                            "objectivePercent": 100,
                            "language": "English",
                            "segmentPercent": 40,
                            "unexpected": True,
                        }
                    ]
                }
            )


if __name__ == "__main__":
    unittest.main()
