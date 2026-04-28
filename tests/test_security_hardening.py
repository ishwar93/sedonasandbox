import unittest

from api.security import SecurityValidationError, sanitize_public_payload_with_allowlist


class SecurityHardeningTests(unittest.TestCase):
    def test_allowlist_drops_unknown_fields(self):
        rows = [
            {
                'stop_id': 'A12',
                'stop_name': 'Example Stop',
                'lat': 40.7,
                'lon': -73.9,
                'feed_type': 'subway',
                'unexpected_field': 'should_not_leak',
            }
        ]
        allowed = {'stop_id', 'stop_name', 'lat', 'lon', 'feed_type'}
        cleaned = sanitize_public_payload_with_allowlist(rows, allowed)

        self.assertEqual(len(cleaned), 1)
        self.assertEqual(set(cleaned[0].keys()), allowed)
        self.assertNotIn('unexpected_field', cleaned[0])

    def test_metadata_keys_are_removed_before_allowlist(self):
        payload = {
            'alert_id': 'x',
            'metadata': {'source_path': r'C:\secret\path.txt'},
            'internal_debug': 'debug-only',
        }
        allowed = {'alert_id', 'metadata', 'internal_debug'}
        cleaned = sanitize_public_payload_with_allowlist(payload, allowed)

        self.assertEqual(cleaned, {'alert_id': 'x'})

    def test_sensitive_output_token_is_blocked(self):
        payload = {'alert_id': 'x', 'header_text_plain': r'Traceback at C:\secret\stack.txt'}
        allowed = {'alert_id', 'header_text_plain'}

        with self.assertRaises(SecurityValidationError):
            sanitize_public_payload_with_allowlist(payload, allowed)


if __name__ == '__main__':
    unittest.main()
