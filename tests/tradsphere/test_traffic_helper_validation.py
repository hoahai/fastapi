import unittest
import uuid
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from apps.tradsphere.api.v1.helpers import dbQueries
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

    def test_bulk_save_returns_traffic_list_item_payload(self):
        traffic_id = str(uuid.uuid4())
        traffic_row = {
            "id": traffic_id,
            "accountCode": "TAAA",
            "campaign": "Spring Retail Push",
            "status": "draft",
            "note": "Save all updates together",
            "dateCreated": "2026-05-23T10:00:00+07:00",
            "dateUpdated": "2026-05-23T10:00:00+07:00",
        }
        detail = {
            "traffic": traffic_row,
            "flights": [
                {
                    "isci": "TAAA260611EH",
                }
            ],
            "stations": [
                {
                    "stationCode": "KABC",
                }
            ],
            "email": {
                "toEmails": ["traffic@kabc.com"],
                "ccEmails": [],
                "bccEmails": [],
                "sentStatus": "draft",
                "sentAt": None,
            },
            "summary": {
                "totalRotation": 55.0,
                "rotationWarning": True,
                "rotationWarningMessage": "Total rotation is 55.00%; expected 100.00%",
                "warnings": [],
                "flightCount": 1,
                "stationCount": 1,
            },
        }

        def _load_flights_stub(_: str):
            return []

        def _load_stations_stub(_: str):
            return []

        with patch.object(
            traffic_helper,
            "_ensure_traffic_exists",
            return_value=traffic_row,
        ), patch.object(
            traffic_helper,
            "_load_traffic_flights_safe",
            new=_load_flights_stub,
        ), patch.object(
            traffic_helper,
            "_load_traffic_stations_safe",
            new=_load_stations_stub,
        ), patch.object(
            traffic_helper,
            "_safe_db_call",
            return_value=1,
        ), patch.object(
            traffic_helper,
            "get_traffic_detail_data",
            return_value=detail,
        ), patch.object(
            traffic_helper,
            "map_station_names_by_codes",
            return_value={"KABC": "ABC LOS ANGELES"},
        ), patch.object(
            traffic_helper,
            "ensure_tradsphere_account_codes_exist",
            return_value=None,
        ), patch.object(
            traffic_helper,
            "ensure_station_codes_exist",
            return_value=None,
        ):
            result = traffic_helper.bulk_save_traffic_data(
                payload={
                    "trafficId": traffic_id,
                    "traffic": {
                        "campaign": "Spring Retail Push",
                        "status": "draft",
                        "note": "Save all updates together",
                    },
                    "updateTraffic": True,
                    "flightCreates": [],
                    "flightUpdates": [],
                    "flightDeletes": [],
                    "stationCreates": [],
                    "stationUpdates": [],
                    "stationDeletes": [],
                    "upsertEmail": False,
                }
            )

        self.assertEqual(result["trafficId"], traffic_id)
        self.assertIn("trafficListItem", result)
        self.assertEqual(result["trafficListItem"]["id"], traffic_id)
        self.assertEqual(result["trafficListItem"]["searchStations"], ["KABC", "ABC LOS ANGELES"])
        self.assertEqual(result["trafficListItem"]["searchIscis"], ["TAAA260611EH"])
        self.assertEqual(result["trafficListItem"]["searchEmails"], ["traffic@kabc.com"])
        self.assertEqual(result["trafficListItem"]["summary"]["totalRotation"], 55.0)

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

    def test_test_email_send_injects_notice_without_persisting(self):
        traffic_id = str(uuid.uuid4())
        smtp_result = {"message_id": "abc123"}

        with patch.object(
            traffic_helper,
            "_ensure_traffic_exists",
            return_value={"id": traffic_id, "status": "draft"},
        ), patch.object(
            traffic_helper,
            "get_smtp_settings",
            return_value={
                "host": "smtp.example.com",
                "port": 587,
                "from_email": "noreply@example.com",
                "username": None,
                "password": None,
                "from_name": "Tradsphere",
                "reply_to": None,
                "use_tls": True,
                "use_ssl": False,
                "timeout_seconds": 20,
            },
        ), patch.object(
            traffic_helper,
            "send_smtp_email",
            return_value=smtp_result,
        ) as send_mock:
            result = traffic_helper.send_traffic_email_test_data(
                traffic_id=traffic_id,
                payload={
                    "toEmail": "test@example.com",
                    "subject": "May 2026 Traffic",
                    "body": "<html><body><p>Hello team.</p></body></html>",
                },
            )

        self.assertEqual(result["trafficId"], traffic_id)
        self.assertEqual(result["testEmail"]["toEmail"], "test@example.com")
        self.assertEqual(result["testEmail"]["smtpMessageId"], "abc123")
        send_mock.assert_called_once()
        sent_kwargs = send_mock.call_args.kwargs
        self.assertEqual(sent_kwargs["subject"], "[Test] May 2026 Traffic")
        self.assertIn("TEST EMAIL - This is a test copy", sent_kwargs["text_body"])
        self.assertIn("TEST EMAIL - This is a test copy", sent_kwargs["html_body"])
        self.assertIn("Hello team.", sent_kwargs["html_body"])

    def test_send_email_marks_sent_and_returns_detail(self):
        traffic_id = str(uuid.uuid4())
        email_row = {
            "id": 42,
            "trafficId": traffic_id,
            "toEmails": ["traffic@example.com"],
            "ccEmails": [],
            "bccEmails": [],
            "subject": "May 2026 Traffic",
            "body": "<p>Hello team.</p>",
            "sentStatus": "sent",
            "sentAt": "2026-05-26T10:00:00+07:00",
            "sentByUserId": "user-123",
            "smtpMessageId": "msg-123",
            "lastSendAttemptAt": "2026-05-26T10:00:00+07:00",
            "lastSendError": None,
        }
        detail = {
            "traffic": {
                "id": traffic_id,
                "accountCode": "TAAA",
                "campaign": "Spring Retail Push",
                "status": "sent",
            },
            "flights": [],
            "stations": [],
            "email": {
                "sentStatus": "sent",
                "sentAt": "2026-05-26T10:00:00+07:00",
            },
            "summary": {
                "totalRotation": 0.0,
                "rotationWarning": True,
                "rotationWarningMessage": "Total rotation is 0.00%; expected 100.00%",
                "warnings": [],
                "flightCount": 0,
                "stationCount": 0,
            },
        }

        def _safe_db_side_effect(func, *args, **kwargs):
            if func is traffic_helper.upsert_traffic_email:
                return 1
            if func is traffic_helper.get_traffic_email_row:
                return email_row
            if func is traffic_helper.update_traffic:
                return 1
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        with patch.object(
            traffic_helper,
            "_ensure_traffic_exists",
            return_value={"id": traffic_id, "status": "draft"},
        ), patch.object(
            traffic_helper,
            "get_smtp_settings",
            return_value={
                "host": "smtp.example.com",
                "port": 587,
                "from_email": "noreply@example.com",
                "username": None,
                "password": None,
                "from_name": "Tradsphere",
                "reply_to": None,
                "use_tls": True,
                "use_ssl": False,
                "timeout_seconds": 20,
            },
        ), patch.object(
            traffic_helper,
            "send_smtp_email",
            return_value={"message_id": "msg-123"},
        ), patch.object(
            traffic_helper,
            "_safe_db_call",
            side_effect=_safe_db_side_effect,
        ), patch.object(
            traffic_helper,
            "get_traffic_detail_data",
            return_value=detail,
        ):
            result = traffic_helper.send_traffic_email_data(
                traffic_id=traffic_id,
                payload={
                    "toEmails": ["traffic@example.com"],
                    "ccEmails": [],
                    "bccEmails": [],
                    "subject": "May 2026 Traffic",
                    "body": "<p>Hello team.</p>",
                    "markSentAfterSend": True,
                },
                sent_by_user_id="user-123",
            )

        self.assertEqual(result["trafficId"], traffic_id)
        self.assertEqual(result["email"]["sentStatus"], "sent")
        self.assertEqual(result["detail"]["traffic"]["status"], "sent")

    def test_traffic_flights_query_orders_by_created_time(self):
        traffic_id = str(uuid.uuid4())

        with patch.object(
            dbQueries,
            "get_db_tables",
            return_value={"TRAFFICFLIGHTS": "TrafficFlights"},
        ), patch.object(
            dbQueries,
            "fetch_all",
            return_value=[],
        ) as fetch_mock:
            dbQueries.list_traffic_flights(traffic_id=traffic_id)

        query = fetch_mock.call_args.args[0]
        self.assertIn("ORDER BY dateCreated ASC, id ASC", query)

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
            media_types=[],
        )

    def test_station_candidates_builds_month_summary(self):
        safe_rows = [
            {
                "estNum": 26001,
                "estNumNote": "June test",
                "estNumMedium": "TV",
                "stationCode": "KABC",
                "stationName": "ABC",
                "broadcastMonth": 4,
                "broadcastYear": 2026,
                "startDate": "2026-04-01",
                "totalSpot": 5,
                "totalGross": "100.00",
            },
            {
                "estNum": 26001,
                "estNumNote": "June test",
                "estNumMedium": "TV",
                "stationCode": "KABC",
                "stationName": "ABC",
                "broadcastMonth": 5,
                "broadcastYear": 2026,
                "startDate": "2026-04-06",
                "totalSpot": 1,
                "totalGross": "20.00",
            },
            {
                "estNum": 26001,
                "estNumNote": "June test",
                "estNumMedium": "TV",
                "stationCode": "KXYZ",
                "stationName": "XYZ",
                "broadcastMonth": 5,
                "broadcastYear": 2026,
                "startDate": "2026-05-01",
                "totalSpot": 3,
                "totalGross": "30.50",
            },
            {
                "estNum": 26002,
                "estNumNote": None,
                "estNumMedium": "RA",
                "stationCode": "KXYZ",
                "stationName": "XYZ",
                "broadcastMonth": 6,
                "broadcastYear": 2026,
                "startDate": "2026-06-01",
                "totalSpot": 2,
                "totalGross": "40.25",
            },
        ]
        with patch.object(
            traffic_helper,
            "ensure_tradsphere_account_codes_exist",
            return_value=None,
        ), patch.object(
            traffic_helper,
            "_safe_db_call",
            return_value=safe_rows,
        ) as safe_db_call_mock, patch.object(
            traffic_helper,
            "list_stations_data",
            return_value=[
                {"code": "KABC", "name": "ABC", "deliveryMethod": "Station Portal", "contacts": None},
                {"code": "KXYZ", "name": "XYZ", "deliveryMethod": None, "contacts": None},
            ],
        ):
            result = traffic_helper.list_station_candidates_for_flight_range_data(
                account_code="TAAA",
                flight_start="2026-06-01",
                flight_end="2026-06-30",
            )

        self.assertEqual(
            result["summary"]["months"],
            [
                {"monthKey": "2026-03", "year": 2026, "month": 3, "label": "MAR'26"},
                {"monthKey": "2026-04", "year": 2026, "month": 4, "label": "APR'26"},
                {"monthKey": "2026-06", "year": 2026, "month": 6, "label": "JUN'26"},
            ],
        )
        self.assertEqual(len(result["summary"]["rows"]), 3)
        self.assertEqual(
            result["summary"]["rows"][0],
            {
                "estNum": 26001,
                "stationCode": "KABC",
                "stationName": "ABC",
                "monthCells": [
                    {
                        "monthKey": "2026-03",
                        "year": 2026,
                        "month": 3,
                        "label": "MAR'26",
                        "hasSchedule": True,
                        "hasSpot": True,
                        "scheduleCount": 1,
                        "totalSpot": 5,
                        "totalGrossText": "$100.00",
                    },
                    {
                        "monthKey": "2026-04",
                        "year": 2026,
                        "month": 4,
                        "label": "APR'26",
                        "hasSchedule": True,
                        "hasSpot": True,
                        "scheduleCount": 1,
                        "totalSpot": 1,
                        "totalGrossText": "$20.00",
                    },
                    {
                        "monthKey": "2026-06",
                        "year": 2026,
                        "month": 6,
                        "label": "JUN'26",
                        "hasSchedule": False,
                        "hasSpot": False,
                        "scheduleCount": 0,
                        "totalSpot": 0,
                        "totalGrossText": "$0.00",
                    },
                ],
            },
        )
        self.assertEqual(
            result["summary"]["rows"][1]["monthCells"][0],
            {
                "monthKey": "2026-03",
                "year": 2026,
                "month": 3,
                "label": "MAR'26",
                "hasSchedule": False,
                "hasSpot": False,
                "scheduleCount": 0,
                "totalSpot": 0,
                "totalGrossText": "$0.00",
            },
        )
        self.assertEqual(
            result["summary"]["rows"][1]["monthCells"][1],
            {
                "monthKey": "2026-04",
                "year": 2026,
                "month": 4,
                "label": "APR'26",
                "hasSchedule": True,
                "hasSpot": True,
                "scheduleCount": 1,
                "totalSpot": 3,
                "totalGrossText": "$30.50",
            },
        )
        self.assertEqual(
            result["summary"]["rows"][1]["monthCells"][2],
            {
                "monthKey": "2026-06",
                "year": 2026,
                "month": 6,
                "label": "JUN'26",
                "hasSchedule": False,
                "hasSpot": False,
                "scheduleCount": 0,
                "totalSpot": 0,
                "totalGrossText": "$0.00",
            },
        )
        self.assertEqual(
            result["summary"]["rows"][2]["monthCells"][2],
            {
                "monthKey": "2026-06",
                "year": 2026,
                "month": 6,
                "label": "JUN'26",
                "hasSchedule": True,
                "hasSpot": True,
                "scheduleCount": 1,
                "totalSpot": 2,
                "totalGrossText": "$40.25",
            },
        )
        safe_db_call_mock.assert_called_once_with(
            traffic_helper.list_schedule_station_candidates_for_account_range,
            account_code="TAAA",
            flight_start="2026-06-01",
            flight_end="2026-06-30",
            est_nums=[],
            languages=[],
            media_types=[],
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
            media_types=[],
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
            media_types=[],
        )

    def test_station_candidates_use_shared_calendar_month_bucket(self):
        safe_rows = [
            {
                "estNum": 26001,
                "stationCode": "KABC",
                "stationName": "ABC",
                "startDate": "2026-04-29",
                "totalSpot": 2,
                "totalGross": "40.25",
            }
        ]
        with patch.object(
            traffic_helper,
            "ensure_tradsphere_account_codes_exist",
            return_value=None,
        ), patch.object(
            traffic_helper,
            "_safe_db_call",
            return_value=safe_rows,
        ), patch.object(
            traffic_helper,
            "list_stations_data",
            return_value=[],
        ), patch.object(
            traffic_helper,
            "get_calendar_month_bucket",
            return_value=(2026, 4),
        ) as calendar_bucket_mock:
            result = traffic_helper.list_station_candidates_for_flight_range_data(
                account_code="TAAA",
                flight_start="2026-04-01",
                flight_end="2026-04-30",
            )

        calendar_bucket_mock.assert_called_once_with(date(2026, 4, 29))
        self.assertEqual(
            result["summary"]["months"],
            [{"monthKey": "2026-04", "year": 2026, "month": 4, "label": "APR'26"}],
        )


if __name__ == "__main__":
    unittest.main()
