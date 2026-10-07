"""Submits sample barcode print jobs to crucible-api, which publishes them to a
crucible-label-printer over MQTT and waits for the result.

Goes through the same CrucibleClient (and so the same api_url/api_key) used
everywhere else in this repo, via nano-crucible's client.print.barcode(). That
method already calls the server with retry=False internally -- the server may
have already published the MQTT job (and the printer may have already printed
it) by the time a transient 5xx reaches the caller, so retrying could trigger a
second, genuinely new print job and double-print the label.
"""
import requests

import prefect_backend as _backend


def send_print_job(printer_id: str, mfid: str, name: str) -> str:
    """Submit a print job and wait for the printer to confirm. Returns the job_id.

    Raises ValueError for a malformed request (bad printer_id/mfid) and
    RuntimeError if the request was valid but the label didn't actually print
    (status "error" or "timeout").
    """
    if not printer_id:
        raise RuntimeError("printer_id is required")

    try:
        result = _backend.client.print.barcode(printer_id, mfid, name)
    except requests.exceptions.HTTPError as e:
        resp = e.response
        if resp is not None and resp.status_code == 422:
            raise ValueError(f"Invalid print request: {e}") from e
        raise

    if result["status"] != "ok":
        detail = result.get("detail") or f"print job {result['status']}"
        raise RuntimeError(detail)
    return result["job_id"]
