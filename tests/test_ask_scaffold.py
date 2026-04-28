import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.main import app


class AskScaffoldTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_ask_returns_l3_scaffold_payload(self):
        with (
            patch("api.routers.ask.enforce_rate_limit", return_value=None),
            patch("api.routers.ask.get_or_create_session_id", return_value="sid-ask-1"),
            patch("api.routers.ask.log_session_query", return_value=None),
            patch("api.routers.ask.classify_intent", return_value=("sql", "ollama")),
        ):
            resp = self.client.get("/ask", params={"q": "How many subway stops are there?"})

        self.assertEqual(resp.status_code, 200, resp.text)
        payload = resp.json()
        self.assertEqual(payload["intent"], "sql")
        self.assertEqual(payload["intent_source"], "ollama")
        self.assertEqual(payload["stage"], "L3_scaffold")
        self.assertIn("answer_id", payload)

    def test_ask_blocks_raw_sql_passthrough(self):
        with patch("api.routers.ask.enforce_rate_limit", return_value=None):
            resp = self.client.get("/ask", params={"q": "SELECT * FROM transit.service_alerts"})

        self.assertEqual(resp.status_code, 400, resp.text)
        self.assertIn("Raw SQL is not allowed", resp.text)

    def test_ask_returns_none_on_router_fallback(self):
        with (
            patch("api.routers.ask.enforce_rate_limit", return_value=None),
            patch("api.routers.ask.get_or_create_session_id", return_value="sid-ask-2"),
            patch("api.routers.ask.log_session_query", return_value=None),
            patch("api.routers.ask.classify_intent", return_value=("none", "heuristic_fallback")),
        ):
            resp = self.client.get("/ask", params={"q": "hi there"})

        self.assertEqual(resp.status_code, 200, resp.text)
        payload = resp.json()
        self.assertEqual(payload["intent"], "none")
        self.assertEqual(payload["intent_source"], "heuristic_fallback")


if __name__ == "__main__":
    unittest.main()
