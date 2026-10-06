"""Submits sample barcode print jobs to crucible-api, which publishes them to a
crucible-label-printer over MQTT and waits for the result.

Deliberately does not go through CrucibleClient/client._request(): that session
auto-retries POSTs on 502/503/504, but /print/barcode is not safe to retry -- the MQTT
publish (and physical print) may already have happened before a 5xx reaches the caller,
so a retried request risks printing the label twice. A plain request, with the same
bearer-token auth and no retry wrapping, is used instead.
"""
import requests
from crucible.config import config as _crucible_config

API_URL = "https://crucible.lbl.gov/api/v3"

# Server-side budget is 8s (publish + wait for the printer's result); pad generously
# for network/queueing overhead rather than racing the server's own timeout.
REQUEST_TIMEOUT = 15


def send_print_job(printer_id: str, mfid: str, name: str) -> str:
    """POST /print/barcode and wait for the printer to confirm. Returns the job_id.

    Raises ValueError for a malformed request (bad printer_id/mfid, HTTP 422) and
    RuntimeError if the request was valid but the label didn't actually print
    (status "error" or "timeout").
    """
    if not printer_id:
        raise RuntimeError("printer_id is required")

    resp = requests.post(
        f"{API_URL}/print/barcode",
        json={"printer_id": printer_id, "mfid": mfid, "name": name},
        headers={"Authorization": f"Bearer {_crucible_config.api_key}"},
        timeout=REQUEST_TIMEOUT,
    )

    if resp.status_code == 422:
        detail = _error_detail(resp)
        raise ValueError(f"Invalid print request: {detail}")
    resp.raise_for_status()

    data = resp.json()
    if data["status"] != "ok":
        detail = data.get("detail") or f"print job {data['status']}"
        raise RuntimeError(detail)
    return data["job_id"]


def _error_detail(resp) -> str:
    try:
        body = resp.json()
        return body.get("detail") or body.get("message") or resp.text
    except (ValueError, AttributeError):
        return resp.text
