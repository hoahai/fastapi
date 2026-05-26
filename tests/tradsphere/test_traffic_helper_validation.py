import unittest
import uuid
from decimal import Decimal
from unittest.mock import patch

from apps.tradsphere.api.v1.helpers import traffic as traffic_helper


class TrafficHelperValidationTests(unittest.TestCase):
    def test_uuid_validation_for_traffic_id(self):
        with self.assertRaises(ValueError) as ctx_missing:
            traffic_helper.get_traffic_detail_data(traffic_id="")
        self.assertEqual(str(ctx_missing.exception), "trafficId is required")

        with self.assertRaises(ValueError) as ctx_invalid:
            traffic_helper.get_traffic_detail_data(traffic_id="not-a-uuid")
        self.assertEqual(str(ctx_invalid.exception), "trafficId must be a valid UUID")

    def test_rotation_summary_warning_when_not_100(self):
        summary = traffic_helper._build_rotation_summary(total_rotation=Decimal("92.50"))
        self.assertEqual(summary["totalRotation"], 92.5)
        self.assertTrue(summary["rotationWarning"])
        self.assertIn("expected 100.00%", str(summary["rotationWarningMessage"]))
        self.assertEqual(summary["warnings"][0]["code"], "ROTATION_TOTAL_NOT_100")

    def test_ready_transition_does_not_block_on_rotation_warning(self):
        traffic_id = str(uuid.uuid4())

        with patch.object(
            traffic_helper,
            "_ensure_traffic_exists",
            return_value={"id": traffic_id, "status": "draft"},
        ), patch.object(
            traffic_helper,
            "_safe_db_call",
            return_value=1,
        ), patch.object(
            traffic_helper,
            "_build_detail_payload",
            return_value={
                "summary": {
                    "totalRotation": 50.0,
                    "rotationWarning": True,
                    "rotationWarningMessage": "Total rotation is 50.00%; expected 100.00%",
                }
            },
        ):
            result = traffic_helper.mark_traffic_ready_data(traffic_id=traffic_id)

        self.assertEqual(result["id"], traffic_id)
        self.assertEqual(result["status"], "ready")
        self.assertTrue(result["summary"]["rotationWarning"])

    def test_email_ready_requires_to_emails_subject_and_body(self):
        traffic_id = str(uuid.uuid4())

        with patch.object(
            traffic_helper,
            "_ensure_traffic_exists",
            return_value={"id": traffic_id, "status": "draft"},
        ):
            with self.assertRaises(ValueError) as ctx_to:
                traffic_helper.upsert_traffic_email_data(
                    traffic_id=traffic_id,
                    payload={"sentStatus": "ready", "subject": "Sub", "body": "Body"},
                )
            self.assertEqual(str(ctx_to.exception), "toEmails is required")

            with self.assertRaises(ValueError) as ctx_subject:
                traffic_helper.upsert_traffic_email_data(
                    traffic_id=traffic_id,
                    payload={"sentStatus": "ready", "toEmails": ["a@example.com"], "body": "Body"},
                )
            self.assertEqual(str(ctx_subject.exception), "subject is required when sentStatus is ready")

            with self.assertRaises(ValueError) as ctx_body:
                traffic_helper.upsert_traffic_email_data(
                    traffic_id=traffic_id,
                    payload={"sentStatus": "ready", "toEmails": ["a@example.com"], "subject": "Sub"},
                )
            self.assertEqual(str(ctx_body.exception), "body is required when sentStatus is ready")

    def test_flight_allows_missing_file_url_and_validates_rotation_range(self):
        traffic_id = str(uuid.uuid4())

        base_payload = {
            "flightStart": "2026-06-01",
            "flightEnd": "2026-06-15",
        }

        with patch.object(
            traffic_helper,
            "_ensure_traffic_exists",
            return_value={"id": traffic_id, "status": "draft"},
        ), patch.object(
            traffic_helper,
            "_safe_db_call",
            side_effect=[101, {"id": 101, "trafficId": traffic_id, "rotation": 100}],
        ):
            result = traffic_helper.create_traffic_flight_data(
                traffic_id=traffic_id,
                payload=dict(base_payload),
            )
            self.assertEqual(result["id"], 101)
            self.assertEqual(result["trafficId"], traffic_id)

        with patch.object(
            traffic_helper,
            "_ensure_traffic_exists",
            return_value={"id": traffic_id, "status": "draft"},
        ):
            with self.assertRaises(ValueError) as ctx_rotation:
                traffic_helper.create_traffic_flight_data(
                    traffic_id=traffic_id,
                    payload={
                        **base_payload,
                        "fileUrl": "https://cdn.example.com/file.pdf",
                        "rotation": 101,
                    },
                )
            self.assertEqual(str(ctx_rotation.exception), "rotation must be between 0 and 100")

    def test_station_candidates_require_valid_date_range(self):
        with patch.object(
            traffic_helper,
            "ensure_tradsphere_account_codes_exist",
            return_value=None,
        ):
            with self.assertRaises(ValueError) as ctx:
                traffic_helper.list_station_candidates_for_flight_range_data(
                    account_code="TAAA",
                    flight_start="2026-06-30",
                    flight_end="2026-06-01",
                )
        self.assertEqual(str(ctx.exception), "flightStart must be on or before flightEnd")

    def test_station_candidates_deduplicate_and_normalize_codes(self):
        with patch.object(
            traffic_helper,
            "ensure_tradsphere_account_codes_exist",
            return_value=None,
        ), patch.object(
            traffic_helper,
            "_safe_db_call",
            return_value=[
                {"estNum": 26001, "estNumNote": "June test", "estNumMedium": "tv", "stationCode": "kabc", "stationName": "ABC"},
                {"estNum": 26001, "estNumNote": "June test", "estNumMedium": "tv", "stationCode": "KABC", "stationName": "ABC Duplicate"},
                {"estNum": 26002, "estNumNote": "", "estNumMedium": "ra", "stationCode": "Kxyz", "stationName": "XYZ"},
            ],
        ) as safe_db_call_mock, patch.object(
            traffic_helper,
            "list_stations_data",
            return_value=[
                {
                    "code": "KABC",
                    "deliveryMethod": {"id": 12, "name": "Station Portal"},
                    "contacts": {"TRAFFIC": ["traffic@kabc.com"]},
                }
            ],
        ):
            result = traffic_helper.list_station_candidates_for_flight_range_data(
                account_code="taaa",
                flight_start="2026-06-01",
                flight_end="2026-06-30",
            )

        self.assertEqual(result["accountCode"], "TAAA")
        self.assertEqual(result["flightStart"], "2026-06-01")
        self.assertEqual(result["flightEnd"], "2026-06-30")
        self.assertEqual(
            result["estNums"],
            [
                {"estNum": 26001, "note": "June test", "medium": "TV", "stationCount": 1},
                {"estNum": 26002, "note": None, "medium": "RA", "stationCount": 1},
            ],
        )
        self.assertEqual(
            result["stations"],
            [
                {
                    "stationCode": "KABC",
                    "stationName": "ABC",
                    "deliveryMethod": "Station Portal",
                    "contactsSnapshot": {"TRAFFIC": ["traffic@kabc.com"]},
                },
                {
                    "stationCode": "KXYZ",
                    "stationName": "XYZ",
                    "deliveryMethod": None,
                    "contactsSnapshot": None,
                },
            ],
        )
        self.assertEqual(result["summary"]["candidateCount"], 2)
        self.assertEqual(result["summary"]["estNumCount"], 2)
        safe_db_call_mock.assert_called_once_with(
            traffic_helper.list_schedule_station_candidates_for_account_range,
            account_code="TAAA",
            flight_start="2026-06-01",
            flight_end="2026-06-30",
            est_nums=[],
            languages=[],
        )

    def test_station_candidates_forward_est_num_filter(self):
        with patch.object(
            traffic_helper,
            "ensure_tradsphere_account_codes_exist",
            return_value=None,
        ), patch.object(
            traffic_helper,
            "_safe_db_call",
            return_value=[],
        ) as safe_db_call_mock, patch.object(
            traffic_helper,
            "list_stations_data",
            return_value=[],
        ):
            traffic_helper.list_station_candidates_for_flight_range_data(
                account_code="TAAA",
                flight_start="2026-06-01",
                flight_end="2026-06-30",
                est_nums=[26002, 26001, 26001],
            )

        safe_db_call_mock.assert_called_once_with(
            traffic_helper.list_schedule_station_candidates_for_account_range,
            account_code="TAAA",
            flight_start="2026-06-01",
            flight_end="2026-06-30",
            est_nums=[26001, 26002],
            languages=[],
        )

    def test_station_candidates_forward_language_filter(self):
        with patch.object(
            traffic_helper,
            "ensure_tradsphere_account_codes_exist",
            return_value=None,
        ), patch.object(
            traffic_helper,
            "_safe_db_call",
            return_value=[],
        ) as safe_db_call_mock, patch.object(
            traffic_helper,
            "list_stations_data",
            return_value=[],
        ):
            traffic_helper.list_station_candidates_for_flight_range_data(
                account_code="TAAA",
                flight_start="2026-06-01",
                flight_end="2026-06-30",
                languages=["spanish", "english", "SPANISH"],
            )

        safe_db_call_mock.assert_called_once_with(
            traffic_helper.list_schedule_station_candidates_for_account_range,
            account_code="TAAA",
            flight_start="2026-06-01",
            flight_end="2026-06-30",
            est_nums=[],
            languages=["English", "Spanish"],
        )


if __name__ == "__main__":
    unittest.main()
