import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.main import app


class ApiEndpointContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_subway_endpoint_returns_expected_shape(self):
        fake_rows = [
            {
                "stop_id": "A12",
                "stop_name": "Times Sq-42 St",
                "lat": 40.755,
                "lon": -73.987,
                "feed_type": "subway",
                "metadata": {"source_path": r"C:\secret\path.json"},
                "unexpected_field": "drop-me",
            }
        ]
        with (
            patch("api.routers.stops.enforce_rate_limit", return_value=None),
            patch("api.routers.stops.get_or_create_session_id", return_value="sid-1"),
            patch("api.routers.stops.log_session_query", return_value=None),
            patch("api.routers.stops._query_stops_from_combined", return_value=fake_rows),
        ):
            resp = self.client.get("/api/stops/subway")

        self.assertEqual(resp.status_code, 200, resp.text)
        payload = resp.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(
            set(payload[0].keys()), {"stop_id", "stop_name", "lat", "lon", "feed_type"}
        )
        self.assertEqual(payload[0]["stop_id"], "A12")

    def test_citibike_endpoint_returns_expected_shape(self):
        fake_rows = [
            {
                "station_id": "123",
                "station_name": "Broadway & W 58 St",
                "lat": 40.764,
                "lon": -73.983,
                "capacity": 39,
                "internal_debug": "drop-me",
            }
        ]
        with (
            patch("api.routers.citibike.enforce_rate_limit", return_value=None),
            patch("api.routers.citibike.get_or_create_session_id", return_value="sid-2"),
            patch("api.routers.citibike.log_session_query", return_value=None),
            patch("api.routers.citibike._query_citibike_from_combined", return_value=fake_rows),
        ):
            resp = self.client.get("/api/citibike/stations")

        self.assertEqual(resp.status_code, 200, resp.text)
        payload = resp.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(
            set(payload[0].keys()), {"station_id", "station_name", "lat", "lon", "capacity"}
        )
        self.assertEqual(payload[0]["capacity"], 39)

    def test_alerts_active_endpoint_returns_expected_shape(self):
        fake_rows = [
            {
                "alert_id": "alert-1",
                "feed_source": "subway",
                "header_text_plain": "Service change",
                "mercury_alert_type": "SUSPENSION",
                "is_planned": True,
                "is_active_now": True,
                "first_seen_at": "2026-01-01T00:00:00Z",
                "last_seen_at": "2026-01-01T01:00:00Z",
                "retrieval_score": 0.99,
            }
        ]
        with (
            patch("api.routers.alerts.enforce_rate_limit", return_value=None),
            patch("api.routers.alerts.get_or_create_session_id", return_value="sid-3"),
            patch("api.routers.alerts.log_session_query", return_value=None),
            patch("api.routers.alerts.query", return_value=fake_rows),
        ):
            resp = self.client.get("/api/alerts/active")

        self.assertEqual(resp.status_code, 200, resp.text)
        payload = resp.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(
            set(payload[0].keys()),
            {
                "alert_id",
                "feed_source",
                "header_text_plain",
                "mercury_alert_type",
                "is_planned",
                "is_active_now",
                "first_seen_at",
                "last_seen_at",
            },
        )
        self.assertEqual(payload[0]["alert_id"], "alert-1")


if __name__ == "__main__":
    unittest.main()
