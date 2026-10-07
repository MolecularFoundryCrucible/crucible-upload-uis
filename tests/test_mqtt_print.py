"""Unit tests for mqtt_print.send_print_job's status/error branching against
nano-crucible's client.print.barcode().

client.print.barcode() itself never raises on a print failure -- it returns a dict
with status "ok"/"error"/"timeout" regardless, logging a warning on the latter two.
send_print_job() is the translation layer that turns that into exceptions, since every
existing caller in this repo (print_sample_barcode/print_tray_barcodes and their Flask
routes) is exception-based. These tests exist specifically to catch a regression where
a non-"ok" status gets silently treated as success.
"""
import unittest
from unittest.mock import MagicMock, patch

import requests

import mqtt_print


def _http_error(status_code, detail=None):
    resp = MagicMock()
    resp.status_code = status_code
    return requests.exceptions.HTTPError(
        f"{status_code} error" + (f": {detail}" if detail else ""), response=resp
    )


class TestSendPrintJob(unittest.TestCase):
    def test_requires_printer_id(self):
        with self.assertRaises(RuntimeError):
            mqtt_print.send_print_job("", "mfid", "name")

    @patch("mqtt_print._backend.client")
    def test_status_ok_returns_job_id(self, mock_client):
        mock_client.print.barcode.return_value = {
            "job_id": "abc123", "printer_id": "b30-113", "mfid": "mfid",
            "name": "name", "ts": 1.0, "status": "ok", "detail": None,
        }
        job_id = mqtt_print.send_print_job("b30-113", "mfid", "name")
        self.assertEqual(job_id, "abc123")
        mock_client.print.barcode.assert_called_once_with("b30-113", "mfid", "name")

    @patch("mqtt_print._backend.client")
    def test_status_error_raises_with_detail(self, mock_client):
        mock_client.print.barcode.return_value = {
            "job_id": "abc123", "status": "error",
            "detail": "Requested printer not currently online.",
        }
        with self.assertRaisesRegex(RuntimeError, "not currently online"):
            mqtt_print.send_print_job("b30-113", "mfid", "name")

    @patch("mqtt_print._backend.client")
    def test_status_timeout_raises(self, mock_client):
        mock_client.print.barcode.return_value = {
            "job_id": "abc123", "status": "timeout", "detail": None,
        }
        with self.assertRaisesRegex(RuntimeError, "timeout"):
            mqtt_print.send_print_job("b30-113", "mfid", "name")

    @patch("mqtt_print._backend.client")
    def test_422_raises_value_error_not_runtime_error(self, mock_client):
        mock_client.print.barcode.side_effect = _http_error(422, "mfid is not a valid MFID")
        with self.assertRaisesRegex(ValueError, "mfid is not a valid MFID"):
            mqtt_print.send_print_job("b30-113", "bad-mfid", "name")

    @patch("mqtt_print._backend.client")
    def test_server_error_propagates(self, mock_client):
        mock_client.print.barcode.side_effect = _http_error(500)
        with self.assertRaises(requests.exceptions.HTTPError):
            mqtt_print.send_print_job("b30-113", "mfid", "name")

    @patch("mqtt_print._backend.client")
    def test_uses_shared_client_not_a_new_session(self, mock_client):
        """Must go through the shared CrucibleClient (same api_url/api_key as the rest
        of this repo), not a standalone request, so there's one URL/client to manage."""
        mock_client.print.barcode.return_value = {"job_id": "x", "status": "ok"}
        mqtt_print.send_print_job("b30-113", "mfid", "name")
        mock_client.print.barcode.assert_called_once()


if __name__ == "__main__":
    unittest.main()
