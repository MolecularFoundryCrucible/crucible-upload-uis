"""Unit tests for mqtt_print.send_print_job's status/error branching against
crucible-api's POST /print/barcode.

A 200 response is not itself success -- the body's "status" field must be checked.
These tests exist specifically to catch a regression where any 200 gets treated as a
successful print (silently swallowing "error"/"timeout" outcomes).
"""
import unittest
from unittest.mock import MagicMock, patch

import mqtt_print


def _response(status_code, json_body=None, text=""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.json.return_value = json_body or {}
    if status_code < 400:
        resp.raise_for_status.return_value = None
    else:
        import requests
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            f"{status_code} error", response=resp
        )
    return resp


class TestSendPrintJob(unittest.TestCase):
    def test_requires_printer_id(self):
        with self.assertRaises(RuntimeError):
            mqtt_print.send_print_job("", "mfid", "name")

    @patch("mqtt_print.requests.post")
    def test_status_ok_returns_job_id(self, mock_post):
        mock_post.return_value = _response(200, {
            "job_id": "abc123", "printer_id": "b30-113", "mfid": "mfid",
            "name": "name", "ts": 1.0, "status": "ok", "detail": None,
        })
        job_id = mqtt_print.send_print_job("b30-113", "mfid", "name")
        self.assertEqual(job_id, "abc123")

    @patch("mqtt_print.requests.post")
    def test_status_error_raises_with_detail(self, mock_post):
        mock_post.return_value = _response(200, {
            "job_id": "abc123", "status": "error",
            "detail": "Requested printer not currently online.",
        })
        with self.assertRaisesRegex(RuntimeError, "not currently online"):
            mqtt_print.send_print_job("b30-113", "mfid", "name")

    @patch("mqtt_print.requests.post")
    def test_status_timeout_raises(self, mock_post):
        mock_post.return_value = _response(200, {
            "job_id": "abc123", "status": "timeout", "detail": None,
        })
        with self.assertRaisesRegex(RuntimeError, "timeout"):
            mqtt_print.send_print_job("b30-113", "mfid", "name")

    @patch("mqtt_print.requests.post")
    def test_422_raises_value_error_not_runtime_error(self, mock_post):
        mock_post.return_value = _response(422, {"detail": "mfid is not a valid MFID"})
        with self.assertRaisesRegex(ValueError, "mfid is not a valid MFID"):
            mqtt_print.send_print_job("b30-113", "bad-mfid", "name")

    @patch("mqtt_print.requests.post")
    def test_server_error_propagates(self, mock_post):
        import requests
        mock_post.return_value = _response(500, text="internal error")
        with self.assertRaises(requests.exceptions.HTTPError):
            mqtt_print.send_print_job("b30-113", "mfid", "name")

    @patch("mqtt_print.requests.post")
    def test_does_not_use_retrying_session(self, mock_post):
        """Must use a plain requests.post, not CrucibleClient's retry-wrapped session --
        retrying this call risks double-printing a label (see module docstring)."""
        mock_post.return_value = _response(200, {"job_id": "x", "status": "ok"})
        mqtt_print.send_print_job("b30-113", "mfid", "name")
        mock_post.assert_called_once()


if __name__ == "__main__":
    unittest.main()
