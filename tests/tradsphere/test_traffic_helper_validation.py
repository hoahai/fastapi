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

    def test_flight_requires_file_url_and_rotation_range(self):
        traffic_id = str(uuid.uuid4())

        base_payload = {
            "flightStart": "2026-06-01",
            "flightEnd": "2026-06-15",
        }

        with patch.object(
            traffic_helper,
            "_ensure_traffic_exists",
            return_value={"id": traffic_id, "status": "draft"},
        ):
            with self.assertRaises(ValueError) as ctx_file:
                traffic_helper.create_traffic_flight_data(
                    traffic_id=traffic_id,
                    payload=dict(base_payload),
                )
            self.assertEqual(str(ctx_file.exception), "fileUrl is required")

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


if __name__ == "__main__":
    unittest.main()
