import unittest
from unittest.mock import Mock, patch

import requests

from api.intent_router import classify_intent


class IntentRouterTests(unittest.TestCase):
    def test_classify_intent_uses_ollama_response(self):
        mock_resp = Mock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"response": "sql"}

        with patch("api.intent_router.requests.post", return_value=mock_resp):
            intent, source = classify_intent("How many bus stops are active?")

        self.assertEqual(intent, "sql")
        self.assertEqual(source, "ollama")

    def test_classify_intent_falls_back_when_ollama_fails(self):
        with patch(
            "api.intent_router.requests.post",
            side_effect=requests.RequestException("boom"),
        ):
            intent, source = classify_intent("hello")

        self.assertEqual(intent, "none")
        self.assertEqual(source, "heuristic_fallback")


if __name__ == "__main__":
    unittest.main()
