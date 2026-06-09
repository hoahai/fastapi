import unittest
from datetime import date

from apps.tradsphere.api.v1.helpers import broadcastCalendar as bc


class BroadcastCalendarHelperTests(unittest.TestCase):
    def test_get_broadcast_calendar_info_exposes_broadcast_and_calendar_buckets(self):
        info = bc.get_broadcast_calendar_info(date(2026, 4, 22))

        self.assertEqual(info["broadcastMonth"], 4)
        self.assertEqual(info["broadcastYear"], 2026)
        self.assertEqual(info["beginBroadcastMonth"], date(2026, 3, 30))
        self.assertEqual(info["endBroadcastMonth"], date(2026, 4, 26))
        self.assertEqual(info["numberOfBroadcastWeek"], 4)

        self.assertEqual(info["calendarMonth"], 4)
        self.assertEqual(info["calendarYear"], 2026)
        self.assertEqual(info["beginCalendarMonth"], date(2026, 4, 6))
        self.assertEqual(info["endCalendarMonth"], date(2026, 5, 3))
        self.assertEqual(info["numberOfCalendarWeek"], 4)
        self.assertEqual(info["calendarWeekNumofMonth"], 3)

    def test_bucket_helpers_distinguish_sunday_end_and_monday_start(self):
        target_date = date(2026, 5, 1)

        self.assertEqual(bc.get_broadcast_month_bucket(target_date), (2026, 5))
        self.assertEqual(bc.get_calendar_month_bucket(target_date), (2026, 4))

    def test_month_range_helpers_use_the_expected_week_anchor(self):
        self.assertEqual(
            bc.get_broadcast_month_range(4, 2026),
            (date(2026, 3, 30), date(2026, 4, 26)),
        )
        self.assertEqual(
            bc.get_calendar_month_range(4, 2026),
            (date(2026, 4, 6), date(2026, 5, 3)),
        )

    def test_result_type_supports_calendar_fields(self):
        self.assertEqual(bc.get_broadcast_calendar_value(date(2026, 5, 1), "month"), 5)
        self.assertEqual(bc.get_broadcast_calendar_value(date(2026, 5, 1), "calendar_month"), 4)
        self.assertEqual(
            bc.get_broadcast_calendar_value(date(2026, 5, 1), "calendar_start_date"),
            date(2026, 4, 6),
        )


if __name__ == "__main__":
    unittest.main()
