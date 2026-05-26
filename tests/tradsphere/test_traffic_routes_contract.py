import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from apps.tradsphere.api.v1.endpoints.core import (
    traffic,
    trafficEmail,
    trafficFlights,
    trafficStations,
)
from apps.tradsphere.api.v1.router import router as tradsphere_router


class TrafficRouteContractTests(unittest.TestCase):
    def test_query_param_traffic_routes_are_registered(self):
        actual_routes: set[tuple[str, str]] = set()
        for route in tradsphere_router.routes:
            path = str(getattr(route, "path", "") or "")
            methods = set(getattr(route, "methods", set()) or set())
            for method in methods:
                if method in {"GET", "POST", "PUT", "DELETE"}:
                    actual_routes.add((method, path))

        expected_routes = {
            ("GET", "/v1/traffic/account"),
            ("GET", "/v1/traffic"),
            ("POST", "/v1/traffic"),
            ("POST", "/v1/traffic/bulk-save"),
            ("PUT", "/v1/traffic"),
            ("POST", "/v1/traffic/ready"),
            ("POST", "/v1/traffic/archive"),
            ("POST", "/v1/traffic/flight"),
            ("PUT", "/v1/traffic/flight"),
            ("DELETE", "/v1/traffic/flight"),
            ("GET", "/v1/traffic/station-candidates"),
            ("POST", "/v1/traffic/station"),
            ("PUT", "/v1/traffic/station"),
            ("DELETE", "/v1/traffic/station"),
            ("PUT", "/v1/traffic/email"),
        }

        for route_key in expected_routes:
            self.assertIn(route_key, actual_routes)

    def test_old_path_param_traffic_routes_are_not_registered(self):
        traffic_paths = [
            str(getattr(route, "path", "") or "")
            for route in tradsphere_router.routes
            if "/v1/traffic" in str(getattr(route, "path", "") or "")
        ]

        self.assertNotIn("/v1/traffic/{trafficId}", traffic_paths)
        self.assertNotIn("/v1/traffic/{traffic_id}", traffic_paths)
        self.assertNotIn("/v1/traffic/{trafficId}/ready", traffic_paths)
        self.assertNotIn("/v1/traffic/{traffic_id}/ready", traffic_paths)
        self.assertNotIn("/v1/traffic/{trafficId}/archive", traffic_paths)
        self.assertNotIn("/v1/traffic/{traffic_id}/archive", traffic_paths)
        self.assertNotIn("/v1/traffic/{trafficId}/flights/{flightId}", traffic_paths)
        self.assertNotIn("/v1/traffic/{traffic_id}/flights/{flight_id}", traffic_paths)
        self.assertNotIn("/v1/traffic/{trafficId}/stations/{stationId}", traffic_paths)
        self.assertNotIn("/v1/traffic/{traffic_id}/stations/{station_id}", traffic_paths)
        self.assertNotIn("/v1/traffic/{trafficId}/email", traffic_paths)
        self.assertNotIn("/v1/traffic/{traffic_id}/email", traffic_paths)

        self.assertFalse(any("{" in path and "/v1/traffic" in path for path in traffic_paths))


class TrafficMissingQueryParamTests(unittest.TestCase):
    def assert_missing_param(self, func, *args, expected_detail: str, **kwargs):
        with self.assertRaises(HTTPException) as ctx:
            func(*args, **kwargs)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(str(ctx.exception.detail), expected_detail)

    def test_missing_code_for_account_load(self):
        self.assert_missing_param(
            traffic.get_account_traffic_route,
            code=None,
            expected_detail="code is required",
        )

    def test_missing_id_for_traffic_detail_update_and_status_actions(self):
        self.assert_missing_param(
            traffic.get_traffic_detail_route,
            traffic_id=None,
            expected_detail="id is required",
        )
        self.assert_missing_param(
            traffic.update_traffic_route,
            traffic_id=None,
            payload={},
            expected_detail="id is required",
        )
        self.assert_missing_param(
            traffic.mark_traffic_ready_route,
            traffic_id=None,
            expected_detail="id is required",
        )
        self.assert_missing_param(
            traffic.archive_traffic_route,
            traffic_id=None,
            expected_detail="id is required",
        )

    def test_missing_traffic_id_for_child_create_routes(self):
        self.assert_missing_param(
            trafficFlights.create_traffic_flight_route,
            traffic_id=None,
            payload={},
            expected_detail="trafficId is required",
        )
        self.assert_missing_param(
            trafficStations.create_traffic_station_route,
            traffic_id=None,
            payload={},
            expected_detail="trafficId is required",
        )
        self.assert_missing_param(
            trafficStations.get_traffic_station_candidates_route,
            account_code=None,
            flight_start="2026-06-01",
            flight_end="2026-06-30",
            expected_detail="accountCode is required",
        )
        self.assert_missing_param(
            trafficStations.get_traffic_station_candidates_route,
            account_code="TAAA",
            flight_start=None,
            flight_end="2026-06-30",
            expected_detail="flightStart is required",
        )
        self.assert_missing_param(
            trafficStations.get_traffic_station_candidates_route,
            account_code="TAAA",
            flight_start="2026-06-01",
            flight_end=None,
            expected_detail="flightEnd is required",
        )

    def test_missing_flight_id_or_traffic_id_for_flight_update_delete(self):
        self.assert_missing_param(
            trafficFlights.update_traffic_flight_route,
            traffic_id=None,
            flight_id="1",
            payload={},
            expected_detail="trafficId is required",
        )
        self.assert_missing_param(
            trafficFlights.update_traffic_flight_route,
            traffic_id="abc",
            flight_id=None,
            payload={},
            expected_detail="flightId is required",
        )
        self.assert_missing_param(
            trafficFlights.delete_traffic_flight_route,
            traffic_id=None,
            flight_id="1",
            expected_detail="trafficId is required",
        )
        self.assert_missing_param(
            trafficFlights.delete_traffic_flight_route,
            traffic_id="abc",
            flight_id=None,
            expected_detail="flightId is required",
        )

    def test_missing_station_id_or_traffic_id_for_station_update_delete(self):
        self.assert_missing_param(
            trafficStations.update_traffic_station_route,
            traffic_id=None,
            station_id="1",
            payload={},
            expected_detail="trafficId is required",
        )
        self.assert_missing_param(
            trafficStations.update_traffic_station_route,
            traffic_id="abc",
            station_id=None,
            payload={},
            expected_detail="stationId is required",
        )
        self.assert_missing_param(
            trafficStations.delete_traffic_station_route,
            traffic_id=None,
            station_id="1",
            expected_detail="trafficId is required",
        )
        self.assert_missing_param(
            trafficStations.delete_traffic_station_route,
            traffic_id="abc",
            station_id=None,
            expected_detail="stationId is required",
        )

    def test_missing_traffic_id_for_email_upsert(self):
        request = SimpleNamespace(state=SimpleNamespace(auth_principal=None))
        self.assert_missing_param(
            trafficEmail.upsert_traffic_email_route,
            request,
            traffic_id=None,
            payload={},
            expected_detail="trafficId is required",
        )

    def test_station_candidates_accept_optional_est_num_filters(self):
        with patch.object(
            trafficStations,
            "list_station_candidates_for_flight_range_data",
            return_value={"ok": True},
        ) as list_mock:
            result = trafficStations.get_traffic_station_candidates_route(
                account_code="TAAA",
                flight_start="2026-06-01",
                flight_end="2026-06-30",
                est_nums=["26001,26002"],
                est_num=None,
                languages=None,
                language=None,
            )

        self.assertEqual(result, {"ok": True})
        list_mock.assert_called_once_with(
            account_code="TAAA",
            flight_start="2026-06-01",
            flight_end="2026-06-30",
            est_nums=[26001, 26002],
            languages=[],
        )

    def test_station_candidates_reject_invalid_est_num_filter(self):
        with self.assertRaises(HTTPException) as ctx:
            trafficStations.get_traffic_station_candidates_route(
                account_code="TAAA",
                flight_start="2026-06-01",
                flight_end="2026-06-30",
                est_nums=["BAD"],
                est_num=None,
                languages=None,
                language=None,
            )
        self.assertEqual(ctx.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
