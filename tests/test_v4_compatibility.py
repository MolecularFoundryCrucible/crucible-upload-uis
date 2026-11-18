"""Hermetic unit tests for nano-crucible 4.0.0 / crucible-api v4 compatibility
and Crucible Hub visualization links in crucible-upload-uis.

Guarantees:
- Zero real network I/O (requests.Session.send and urllib.request.urlopen are hard-blocked).
- Zero reads or writes to machine-local .env or instrument_conf.py.
- Exercises real CrucibleClient resource methods at the _request boundary against
  nano-crucible 4.0.0 so parameter/model validation, RemovedError checks, URL routing,
  and Pydantic response parsing are genuinely verified offline.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import requests

# Clean-checkout fallback for gitignored instrument_conf.py (never touches existing file)
if "instrument_conf" not in sys.modules and importlib.util.find_spec("instrument_conf") is None:
    _default_conf_path = Path(__file__).resolve().parent.parent / "instrument_conf.default.py"
    _spec = importlib.util.spec_from_file_location("instrument_conf", _default_conf_path)
    _mod = importlib.util.module_from_spec(_spec)
    assert _spec is not None and _spec.loader is not None
    _spec.loader.exec_module(_mod)
    sys.modules["instrument_conf"] = _mod

from crucible import CrucibleClient
from crucible.errors import CrucibleAPIError, NotSignedInError, RemovedError
from crucible.utils.identifiers import IdentifierNotFoundError

with patch("tkinter.Tk", return_value=MagicMock()), \
     patch.object(requests.Session, "send", side_effect=AssertionError("Network blocked during import")):
    import main
    import mqtt_print
    import prefect_backend as pb

# Canonical 26-char lowercase Crockford Base32 MFIDs (valid for is_mfid)
SAMPLE_MFID = "0t6k7ms7r5zak000bwynwcfacr"
PARENT_SAMPLE_MFID = "0t6k7ms7r5zak000bwynwcfacs"
CHILD_SAMPLE_MFID = "0t6k7ms7r5zak000bwynwcfact"
CARRIER_MFID = "0t6k7ms7r5zak000bwynwcfacv"
TRAY1_MFID = "0t6k7ms7r5zak000bwynwcfacw"
TRAY2_MFID = "0t6k7ms7r5zak000bwynwcfacx"
DATASET_MFID = "0t6k7ms7r5zak000bwynwcfacy"
SESSION_MFID = "0t6k7ms7r5zak000bwynwcfacz"
CHILD_DATASET_MFID = "0t6k7ms7r5zak000bwynwcfad0"
INSTRUMENT_MFID = "0t6k7ms7r5zak000bwynwcfad1"
USER_MFID = "0t6k7ms7r5zak000bwynwcfad2"
USER_ORCID = "0000-0002-1825-0097"
USER_ORCID_X = "0000-0002-1694-233X"


class HermeticTestCase(unittest.TestCase):
    """Base test case that hard-blocks network I/O and provides a real v4 CrucibleClient."""

    def setUp(self):
        self._send_patcher = patch.object(
            requests.Session,
            "send",
            side_effect=AssertionError("Hermetic test attempted real requests.Session.send"),
        )
        self._send_patcher.start()
        self.addCleanup(self._send_patcher.stop)

        self._urlopen_patcher = patch(
            "urllib.request.urlopen",
            side_effect=AssertionError("Hermetic test attempted real urllib.request.urlopen"),
        )
        self._urlopen_patcher.start()
        self.addCleanup(self._urlopen_patcher.stop)

        self._logger_patcher = patch.object(
            pb, "get_run_logger", return_value=logging.getLogger("hermetic_test")
        )
        self._logger_patcher.start()
        self.addCleanup(self._logger_patcher.stop)

        self.requests_log: list[tuple[str, str, dict]] = []

    @staticmethod
    def make_api_error(status: int, detail: str = "error") -> CrucibleAPIError:
        resp = requests.Response()
        resp.status_code = status
        resp.reason = "Error"
        resp._content = json.dumps({"detail": detail}).encode("utf-8")
        return CrucibleAPIError(
            f"{status} Error: {detail}", response=resp, status=status, detail=detail
        )

    def bind_v4_client(self, router) -> CrucibleClient:
        def _fake_request(_client_self, method: str, endpoint: str, **kwargs):
            self.requests_log.append((method.lower(), endpoint, kwargs))
            return router(method.lower(), endpoint, kwargs)

        req_patcher = patch.object(CrucibleClient, "_request", new=_fake_request)
        req_patcher.start()
        self.addCleanup(req_patcher.stop)

        client = CrucibleClient(
            api_url="https://crucible.lbl.gov/api/v4",
            api_key="hermetic-test-api-key",
        )

        pb_patcher = patch.object(pb, "client", client)
        pb_patcher.start()
        self.addCleanup(pb_patcher.stop)
        return client


# =============================================================================
# Group 1: Client Initialization & Auth Modes
# =============================================================================
class TestClientInitialization(HermeticTestCase):
    def setUp(self):
        super().setUp()
        self._orig_pb_client = pb.client
        import crucible_ingestion.client as ci_client

        self._orig_ci_client = ci_client._client

        def _restore():
            pb.client = self._orig_pb_client
            ci_client._client = self._orig_ci_client

        self.addCleanup(_restore)

    def test_init_with_api_key_sets_client_and_ingestion_client(self):
        fake_client = MagicMock(api_key="secret-key", api_url="https://crucible.lbl.gov/api/v4")
        with patch.object(pb, "CrucibleClient", return_value=fake_client) as mock_cls, \
             patch("crucible_ingestion.set_client") as mock_set_client:
            os.environ.pop("CRUCIBLE_API_URL", None)
            result = pb._init_crucible_client()

        self.assertIs(result, fake_client)
        self.assertIs(pb.client, fake_client)
        mock_cls.assert_called_once_with(api_url="https://crucible.lbl.gov/api/v4")
        mock_set_client.assert_called_once_with(fake_client)

    def test_init_with_oauth_session_api_key_none_succeeds_and_sets_ingestion_client(self):
        from crucible.auth.session import SessionCredential

        fake_tokens = MagicMock(access_token="tok", refresh_token="ref", expires_at=9999999999.0)
        fake_store = MagicMock()
        with patch.dict(os.environ, {}, clear=False), \
             patch("crucible.client._resolve_credential", return_value=SessionCredential(fake_tokens, fake_store)), \
             patch("crucible_ingestion.set_client") as mock_set_client:
            os.environ.pop("CRUCIBLE_API_KEY", None)
            result = pb._init_crucible_client()

        self.assertIsNotNone(result)
        self.assertIs(pb.client, result)
        self.assertIsNone(pb.client.api_key)
        mock_set_client.assert_called_once_with(result)

    def test_init_without_credentials_sets_client_none_and_guards_print(self):
        with patch.object(pb, "CrucibleClient", side_effect=NotSignedInError("Not signed in")), \
             patch("crucible_ingestion.set_client") as mock_set_client:
            result = pb._init_crucible_client()

        self.assertIsNone(result)
        self.assertTrue(hasattr(pb, "client"))
        self.assertIsNone(pb.client)
        mock_set_client.assert_not_called()

        with self.assertRaisesRegex(RuntimeError, "Crucible client is not configured"):
            pb._require_client()

        with self.assertRaisesRegex(RuntimeError, "Crucible client is not configured"):
            mqtt_print.send_print_job("b30-113", SAMPLE_MFID, "Sample-1")

        with patch("mqtt_print._backend.client") as patched_client:
            patched_client.print.barcode.return_value = {"job_id": "job-1", "status": "ok"}
            self.assertEqual(mqtt_print.send_print_job("b30-113", SAMPLE_MFID, "Sample-1"), "job-1")


# =============================================================================
# Group 2: lookup_user & /api/user/lookup
# =============================================================================
class TestLookupUserAndRoutes(HermeticTestCase):
    def test_lookup_user_bare_orcid_and_list_projects(self):
        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("get", f"/users/{USER_ORCID}"):
                return {
                    "unique_id": USER_ORCID,
                    "first_name": "Alice",
                    "last_name": "Smith",
                }
            if (method, endpoint) == ("get", f"/users/{USER_ORCID}/projects"):
                return {
                    "items": [
                        {"project_id": "MFP00002", "title": "Second"},
                        {"project_id": "MFP00001", "title": None},
                    ],
                    "total": 2,
                }
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        self.bind_v4_client(router)
        res = pb.lookup_user(USER_ORCID)
        self.assertEqual(res, {
            "name": "Alice Smith",
            "orcid": USER_ORCID,
            "projects": [
                {"project_id": "MFP00001", "title": ""},
                {"project_id": "MFP00002", "title": "Second"},
            ],
        })
        endpoints = [(m, ep) for m, ep, _ in self.requests_log]
        self.assertEqual(endpoints, [
            ("get", f"/users/{USER_ORCID}"),
            ("get", f"/users/{USER_ORCID}/projects"),
        ])

    def test_lookup_user_https_orcid_url_with_trailing_slash_and_lowercase_x(self):
        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("get", f"/users/{USER_ORCID_X}"):
                return {
                    "unique_id": USER_MFID,
                    "orcid": USER_ORCID_X,
                    "first_name": "Bob",
                    "last_name": "Jones",
                }
            if (method, endpoint) == ("get", f"/users/{USER_MFID}/projects"):
                return {"items": [{"project_id": "MFP00010", "title": "X Project"}]}
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        self.bind_v4_client(router)
        res = pb.lookup_user("  https://orcid.org/0000-0002-1694-233x/  ")
        self.assertEqual(res["orcid"], USER_MFID)
        self.assertEqual(res["name"], "Bob Jones")
        self.assertEqual(res["projects"], [{"project_id": "MFP00010", "title": "X Project"}])
        self.assertEqual(self.requests_log[0][:2], ("get", f"/users/{USER_ORCID_X}"))

    def test_lookup_user_http_orcid_url(self):
        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("get", f"/users/{USER_ORCID}"):
                return {"unique_id": USER_ORCID, "first_name": "Alice", "last_name": "Smith"}
            if (method, endpoint) == ("get", f"/users/{USER_ORCID}/projects"):
                return {"items": []}
            raise AssertionError(f"Unexpected request: {method} {endpoint}")

        self.bind_v4_client(router)
        res = pb.lookup_user(f"http://orcid.org/{USER_ORCID}")
        self.assertEqual(res["orcid"], USER_ORCID)
        self.assertEqual(self.requests_log[0][:2], ("get", f"/users/{USER_ORCID}"))

    def test_lookup_user_email_and_username(self):
        def router(method, endpoint, kwargs):
            params = kwargs.get("params") or {}
            if (method, endpoint) == ("get", "/users") and params.get("email") == "alice@lbl.gov":
                return {
                    "items": [{"unique_id": USER_MFID, "first_name": "Alice", "last_name": "LBL"}],
                    "total": 1,
                }
            if (method, endpoint) == ("get", "/users") and params.get("username") == "alice_lbl":
                return {
                    "items": [{"unique_id": USER_MFID, "first_name": "", "last_name": "", "username": "alice_lbl"}],
                    "total": 1,
                }
            if (method, endpoint) == ("get", f"/users/{USER_MFID}/projects"):
                return {"items": [{"project_id": "MFP00001", "title": "Beamline"}]}
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        self.bind_v4_client(router)
        by_email = pb.lookup_user("Alice@LBL.gov")
        self.assertEqual(by_email["orcid"], USER_MFID)
        self.assertEqual(by_email["name"], "Alice LBL")

        by_username = pb.lookup_user("alice_lbl")
        self.assertEqual(by_username["orcid"], USER_MFID)
        self.assertEqual(by_username["name"], "alice_lbl")

    def test_lookup_user_blank_returns_empty_dict_without_api_call(self):
        self.bind_v4_client(lambda *a: (_ for _ in ()).throw(AssertionError("Must not call API")))
        self.assertEqual(pb.lookup_user(""), {})
        self.assertEqual(pb.lookup_user("   "), {})
        self.assertEqual(len(self.requests_log), 0)

    def test_lookup_user_not_found_and_flask_route_404_vs_500(self):
        mode = {"state": "missing_email"}

        def router(method, endpoint, kwargs):
            if mode["state"] == "missing_email":
                return {"items": [], "total": 0}
            if mode["state"] == "missing_orcid":
                raise self.make_api_error(404, "User not found")
            if mode["state"] == "server_500":
                raise self.make_api_error(500, "Database unavailable")
            raise AssertionError("unreachable")

        self.bind_v4_client(router)
        flask_client = main.app.test_client()

        # IdentifierNotFoundError on email miss -> {} -> HTTP 404
        self.assertEqual(pb.lookup_user("nobody@lbl.gov"), {})
        r404_email = flask_client.post("/api/user/lookup", json={"identifier": "nobody@lbl.gov"})
        self.assertEqual(r404_email.status_code, 404)

        # CrucibleAPIError(status=404) on ORCID miss -> {} -> HTTP 404
        mode["state"] = "missing_orcid"
        self.assertEqual(pb.lookup_user(USER_ORCID), {})
        r404_orcid = flask_client.post("/api/user/lookup", json={"identifier": USER_ORCID})
        self.assertEqual(r404_orcid.status_code, 404)

        # CrucibleAPIError(status=500) -> re-raised -> HTTP 500
        mode["state"] = "server_500"
        with self.assertRaises(CrucibleAPIError):
            pb.lookup_user(USER_ORCID)
        r500 = flask_client.post("/api/user/lookup", json={"identifier": USER_ORCID})
        self.assertEqual(r500.status_code, 500)

    def test_lookup_user_when_client_is_none_raises_runtime_error(self):
        with patch.object(pb, "client", None):
            with self.assertRaisesRegex(RuntimeError, "Crucible client is not configured"):
                pb.lookup_user("alice@lbl.gov")
            resp = main.app.test_client().post("/api/user/lookup", json={"identifier": "alice@lbl.gov"})
            self.assertEqual(resp.status_code, 500)
            self.assertIn("not configured", resp.get_json()["error"])


# =============================================================================
# Group 3: _format_sample & find_samples
# =============================================================================
class TestFormatSampleAndFindSamples(HermeticTestCase):
    def test_format_sample_timestamp_precedence_and_none_coalescing(self):
        s_v4 = pb._format_sample({
            "unique_id": SAMPLE_MFID,
            "sample_name": "S1",
            "sample_type": None,
            "creation_time": "2026-11-01T12:00:00Z",
            "timestamp": "2026-10-01T00:00:00Z",
            "description": None,
        })
        self.assertEqual(s_v4["sample_type"], "")
        self.assertEqual(s_v4["date_created"], "2026-11-01T12:00:00Z")
        self.assertNotIn("None", s_v4["description"])

        s_ts = pb._format_sample({
            "unique_id": SAMPLE_MFID,
            "sample_name": "S2",
            "creation_time": None,
            "timestamp": "2026-10-15T08:30:00Z",
        })
        self.assertEqual(s_ts["date_created"], "2026-10-15T08:30:00Z")

        s_legacy = pb._format_sample({
            "unique_id": SAMPLE_MFID,
            "sample_name": "S3",
            "date_created": "2025-05-20T00:00:00Z",
        })
        self.assertEqual(s_legacy["date_created"], "2025-05-20T00:00:00Z")

        s_empty = pb._format_sample({"unique_id": SAMPLE_MFID, "sample_name": "S4"})
        self.assertEqual(s_empty["date_created"], "")

    def test_find_samples_by_mfid_and_project_scope(self):
        def router(method, endpoint, kwargs):
            self.assertEqual((method, endpoint), ("get", "/samples"))
            params = kwargs.get("params") or {}
            self.assertEqual(params.get("mfid"), [SAMPLE_MFID])
            self.assertNotIn("unique_id", params)
            return {
                "items": [{
                    "unique_id": SAMPLE_MFID,
                    "sample_name": "Wafer-A",
                    "sample_type": "wafer",
                    "creation_time": "2026-11-10T00:00:00Z",
                    "description": "Test wafer",
                }],
                "next_cursor": None,
            }

        self.bind_v4_client(router)
        res = pb.find_samples(sample_unique_id=f"  {SAMPLE_MFID}  ", project_id="MFP00001")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["unique_id"], SAMPLE_MFID)
        self.assertEqual(self.requests_log[0][2]["params"]["project_id"], "MFP00001")

        res_no_proj = pb.find_samples(sample_unique_id=SAMPLE_MFID, project_id=None)
        self.assertEqual(len(res_no_proj), 1)

    def test_find_samples_non_mfid_and_404_return_empty_list(self):
        def router(method, endpoint, kwargs):
            raise self.make_api_error(404, "Not found")

        self.bind_v4_client(router)
        # Non-MFID returns [] immediately with zero HTTP calls and no ValueError
        self.assertEqual(pb.find_samples(sample_unique_id="wrong-project", project_id="MFP00001"), [])
        self.assertEqual(len(self.requests_log), 0)

        # 404 CrucibleAPIError returns [] defensively
        self.assertEqual(pb.find_samples(sample_unique_id=SAMPLE_MFID, project_id="MFP00001"), [])
        self.assertEqual(len(self.requests_log), 1)

    def test_find_samples_by_name_and_project(self):
        def router(method, endpoint, kwargs):
            params = kwargs.get("params") or {}
            self.assertEqual((method, endpoint), ("get", "/samples"))
            self.assertEqual(params.get("sample_name"), "LiF")
            self.assertEqual(params.get("project_id"), "MFP00001")
            self.assertEqual(params.get("limit"), 10)
            return {
                "items": [{"unique_id": SAMPLE_MFID, "sample_name": "LiF"}],
                "next_cursor": None,
            }

        self.bind_v4_client(router)
        res = pb.find_samples(sample_name="LiF", project_id="MFP00001")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["sample_name"], "LiF")


# =============================================================================
# Group 4: create_sample & Flask Sample Creation Routes
# =============================================================================
class TestCreateSample(HermeticTestCase):
    def test_create_sample_passes_sample_model_with_owner_ref(self):
        def router(method, endpoint, kwargs):
            self.assertEqual((method, endpoint), ("post", "/samples"))
            body = kwargs["json"]
            self.assertEqual(body["owner_ref"], USER_ORCID)
            self.assertNotIn("owner_orcid", body)
            self.assertNotIn("owner", body)
            self.assertEqual(body["sample_name"], "SpinFilm-1")
            self.assertEqual(body["project_id"], "MFP00001")
            self.assertEqual(body["sample_type"], "thin film")
            self.assertEqual(body["timestamp"], "2026-11-17T10:00:00Z")
            return {"unique_id": SAMPLE_MFID, "sample_name": "SpinFilm-1"}

        self.bind_v4_client(router)
        created = pb.create_sample(
            sample_name="SpinFilm-1",
            owner_orcid=USER_ORCID,
            project_id="MFP00001",
            description="desc",
            timestamp="2026-11-17T10:00:00Z",
            sample_type="thin film",
        )
        self.assertEqual(created, {"unique_id": SAMPLE_MFID, "sample_name": "SpinFilm-1"})

    def test_flask_sample_create_and_photobox_create_carrier_routes(self):
        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("get", "/samples"):
                return {"items": [], "next_cursor": None}
            if (method, endpoint) == ("post", "/samples"):
                body = kwargs["json"]
                self.assertEqual(body["owner_ref"], USER_ORCID)
                self.assertNotIn("owner_orcid", body)
                return {"unique_id": CARRIER_MFID, "sample_name": body["sample_name"]}
            if method == "post" and endpoint.startswith(f"/samples/{CARRIER_MFID}/children/"):
                return {"ok": True}
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        self.bind_v4_client(router)
        flask_client = main.app.test_client()

        r1 = flask_client.post("/api/sample/create", json={
            "sample_name": "NewSamp",
            "owner_orcid": USER_ORCID,
            "project_id": "MFP00001",
            "sample_type": "crystal",
        })
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r1.get_json()["unique_id"], CARRIER_MFID)

        r2 = flask_client.post("/api/photobox/create_carrier", json={
            "orcid": USER_ORCID,
            "project_id": "MFP00001",
        })
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.get_json()["sample_name"], "CAR000001")

        # Also verify flow_photobox_upload links carrier -> trays via samples.link
        with patch.object(pb, "task_create_dataset", return_value=DATASET_MFID), \
             patch.object(pb, "task_link_dataset_and_sample", return_value=3), \
             patch.object(pb, "_run_post_processing"):
            out_dsid = pb.flow_photobox_upload.fn(
                file="/tmp/carrier.jpg",
                carrier_uuid=CARRIER_MFID,
                tray1_uuid=TRAY1_MFID,
                tray2_uuid=TRAY2_MFID,
                sample_uuids=[SAMPLE_MFID],
                project_id="MFP00001",
                orcid=USER_ORCID,
            )
        self.assertEqual(out_dsid, DATASET_MFID)
        link_eps = [ep for m, ep, _ in self.requests_log if m == "post" and "/children/" in ep]
        self.assertEqual(link_eps, [
            f"/samples/{CARRIER_MFID}/children/{TRAY1_MFID}",
            f"/samples/{CARRIER_MFID}/children/{TRAY2_MFID}",
        ])


# =============================================================================
# Group 5: Dataset, Linking, Update & Postprocessing Flows
# =============================================================================
class TestDatasetAndPostprocessingFlows(HermeticTestCase):
    def test_check_existing_sessions_uses_owner_ref(self):
        def router(method, endpoint, kwargs):
            self.assertEqual((method, endpoint), ("get", "/datasets"))
            params = kwargs["params"]
            self.assertEqual(params["owner_ref"], USER_ORCID)
            self.assertNotIn("owner_orcid", params)
            self.assertEqual(params["project_id"], "MFP00001")
            self.assertEqual(params["dataset_name"], "themis session: sessA")
            return {
                "items": [{"unique_id": SESSION_MFID, "dataset_name": "themis session: sessA"}],
                "next_cursor": None,
            }

        self.bind_v4_client(router)
        sessions = pb.check_existing_sessions(
            session_folder_path="/data/2026/sessA",
            orcid=USER_ORCID,
            project_id="Internal Research (MFP00001)",
            instrument_name="themis",
        )
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]["unique_id"], SESSION_MFID)

    def test_create_session_uses_owner_and_links_sample_via_link_sample(self):
        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("post", "/datasets"):
                body = kwargs["json"]
                self.assertEqual(body["owner_ref"], USER_ORCID)
                self.assertNotIn("owner_orcid", body)
                self.assertEqual(body["instrument_id"], "themis")
                self.assertEqual(body["instrument_mfid"], "0t6k7ms7r5zak000bwynwcfacr")
                return {"unique_id": SESSION_MFID, "dataset_name": body["dataset_name"]}
            if (method, endpoint) == ("post", f"/resources/{SESSION_MFID}/metadata"):
                return {"comments": "session notes"}
            if (method, endpoint) == ("post", f"/datasets/{SESSION_MFID}/keywords"):
                return {"keyword": kwargs["params"]["keyword"]}
            if (method, endpoint) == ("post", f"/datasets/{SESSION_MFID}/samples/{SAMPLE_MFID}"):
                return {"dataset_id": SESSION_MFID, "sample_id": SAMPLE_MFID}
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        self.bind_v4_client(router)
        sess_name, dsid = pb.create_session(
            session_folder_path="/data/2026/sessA",
            kw_list=["tem"],
            comments="session notes",
            orcid=USER_ORCID,
            project_id="MFP00001",
            instrument_name="themis",
            sample_unique_id=SAMPLE_MFID,
        )
        self.assertEqual((sess_name, dsid), ("sessA", SESSION_MFID))
        self.assertIn(("post", f"/datasets/{SESSION_MFID}/samples/{SAMPLE_MFID}"),
                      [(m, ep) for m, ep, _ in self.requests_log])

    def test_existing_dsids_and_child_dsids_and_resolve_dsid(self):
        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("get", "/datasets"):
                self.assertEqual(kwargs["params"]["owner_ref"], USER_ORCID)
                self.assertNotIn("owner_orcid", kwargs["params"])
                return {"items": [{"unique_id": DATASET_MFID}], "next_cursor": None}
            if (method, endpoint) == ("get", f"/datasets/{SESSION_MFID}/children"):
                return {"items": [{"unique_id": CHILD_DATASET_MFID}], "next_cursor": None}
            if (method, endpoint) == ("get", "/files"):
                self.assertIn("sha256_hash", kwargs["params"])
                return {"items": [{"dataset_mfid": DATASET_MFID}], "next_cursor": None}
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        self.bind_v4_client(router)
        self.assertEqual(pb.existing_dsids(USER_ORCID, "MFP00001"), {DATASET_MFID})
        self.assertEqual(pb.child_dsids(SESSION_MFID), {CHILD_DATASET_MFID})

        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(b"hermetic test bytes")
            tmp_path = tf.name
        self.addCleanup(lambda: os.path.exists(tmp_path) and os.unlink(tmp_path))

        matched_dsid, reused = pb.resolve_dsid_for_file(tmp_path, {DATASET_MFID})
        self.assertEqual((matched_dsid, reused), (DATASET_MFID, True))

    def test_task_create_dataset_uses_owner_and_files_and_cleans_up_empty_dataset_on_failure(self):
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tf:
            tf.write(b"spectrum data")
            tmp_path = tf.name
        self.addCleanup(lambda: os.path.exists(tmp_path) and os.unlink(tmp_path))

        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("post", "/datasets"):
                body = kwargs["json"]
                self.assertEqual(body["owner_ref"], USER_ORCID)
                self.assertNotIn("owner_orcid", body)
                self.assertNotIn("instrument_name", body)
                return {"unique_id": body.get("unique_id") or DATASET_MFID}
            if (method, endpoint) == ("post", f"/resources/{DATASET_MFID}/metadata"):
                return kwargs["json"]
            if (method, endpoint) == ("get", f"/datasets/{DATASET_MFID}/files"):
                return []
            if (method, endpoint) == ("post", f"/resources/{DATASET_MFID}/delete"):
                return {"id": 1, "resource_id": DATASET_MFID, "status": "pending"}
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        v4_client = self.bind_v4_client(router)

        # Success path
        with patch.object(v4_client.datasets, "add_file", return_value={"id": "f1"}) as mock_add_file:
            created_dsid = pb.task_create_dataset.fn(
                files=[tmp_path],
                instrument_name="themis",
                project_id="MFP00001",
                orcid=USER_ORCID,
                dsid=DATASET_MFID,
                comments="ok",
                ingestor="ApiUploadIngestor",
                wait_for_ingestion=False,
            )
        self.assertEqual(created_dsid, DATASET_MFID)
        mock_add_file.assert_called_once_with(
            DATASET_MFID,
            tmp_path,
            ingestion_class="ApiUploadIngestor",
            wait_for_ingestion_response=False,
            skip_ingestion=False,
        )

        # Failure path -> cleans up empty dataset via list_files + deletions.request
        with patch.object(v4_client.datasets, "add_file", side_effect=RuntimeError("GCS upload failed")):
            with self.assertRaisesRegex(RuntimeError, "GCS upload failed"):
                pb.task_create_dataset.fn(
                    files=[tmp_path],
                    instrument_name="themis",
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    dsid=DATASET_MFID,
                    wait_for_ingestion=False,
                )
        delete_calls = [(m, ep, kw) for m, ep, kw in self.requests_log if ep == f"/resources/{DATASET_MFID}/delete"]
        self.assertEqual(len(delete_calls), 1)
        self.assertIn("empty dataset", delete_calls[0][2]["params"]["reason"])

    def test_task_update_dataset_handles_v4_nested_dicts_flat_strings_and_str_owner(self):
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tf:
            tf.write(b"updated data")
            tmp_path = tf.name
        self.addCleanup(lambda: os.path.exists(tmp_path) and os.unlink(tmp_path))

        existing_shapes = [
            # Case A: v4 nested dicts with mismatched owner/project/instrument
            {
                "unique_id": DATASET_MFID,
                "owner": {
                    "unique_id": "0000-0001-9999-8888",
                    "first_name": "Other",
                    "last_name": "Owner",
                },
                "owner_orcid": None,
                "project": {
                    "unique_id": "0t6k7ms7r5zak000bwynwcfad3",
                    "project_id": "OTHER_PROJ",
                },
                "instrument": {
                    "unique_id": INSTRUMENT_MFID,
                    "instrument_id": "other-inst",
                    "instrument_name": "Other Instrument",
                },
            },
            # Case B: flat v3 fields matching
            {
                "unique_id": DATASET_MFID,
                "owner_orcid": USER_ORCID,
                "project_id": "MFP00001",
                "instrument_id": "themis",
                "instrument_mfid": "0t6k7ms7r5zak000bwynwcfacr",
            },
            # Case C: str owner resilience (Dataset.owner is Union[str, PublicUser])
            {
                "unique_id": DATASET_MFID,
                "owner": "0000-0009-0000-0001",
                "project_id": "OTHER_PROJ_STR",
                "instrument_id": "other-inst-str",
            },
        ]

        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("get", f"/datasets/{DATASET_MFID}"):
                return existing_shapes.pop(0)
            if (method, endpoint) == ("patch", f"/datasets/{DATASET_MFID}"):
                body = kwargs["json"]
                for forbidden in ("public", "owner", "owner_orcid", "owner_ref", "instrument_name", "instrument_id"):
                    self.assertNotIn(forbidden, body)
                return {"unique_id": DATASET_MFID, **body}
            if (method, endpoint) == ("patch", f"/resources/{DATASET_MFID}/metadata"):
                return kwargs["json"]
            if (method, endpoint) == ("post", f"/datasets/{DATASET_MFID}/keywords"):
                return {"keyword": kwargs["params"]["keyword"]}
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        v4_client = self.bind_v4_client(router)
        with patch.object(v4_client.datasets, "add_file", return_value={"id": "f1"}) as mock_add_file:
            for _ in range(3):
                out = pb.task_update_dataset.fn(
                    files=[tmp_path],
                    dsid=DATASET_MFID,
                    instrument_name="themis",
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    session_name="sess1",
                    dataset_name="ds1",
                    measurement="STEM",
                    data_type="image",
                    kw_list=["haadf"],
                    comments="updated comment",
                    scientific_metadata={"kv": 300},
                    ingestor="ApiUploadIngestor",
                    wait_for_ingestion=False,
                )
                self.assertEqual(out, DATASET_MFID)
            self.assertEqual(mock_add_file.call_count, 3)

        self.assertFalse(pb.dataset_exists("not-an-mfid"))
        self.assertFalse(pb.dataset_exists(""))

    def test_task_link_dataset_to_session_and_sample_and_postprocessing(self):
        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("post", f"/datasets/{SESSION_MFID}/children/{DATASET_MFID}"):
                return {"parent_dataset_id": SESSION_MFID, "child_dataset_id": DATASET_MFID}
            if method == "post" and endpoint.startswith(f"/datasets/{DATASET_MFID}/samples/"):
                return {"ok": True}
            if (method, endpoint) == ("post", f"/datasets/{DATASET_MFID}/postprocess"):
                return {
                    "run_id": 42,
                    "dataset_mfid": DATASET_MFID,
                    "pipeline": kwargs["json"]["pipeline"],
                    "status": "requested",
                }
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        self.bind_v4_client(router)

        # Parent-child dataset link: session_dsid is parent, DATASET_MFID is child
        pb.task_link_dataset_to_session.fn(DATASET_MFID, SESSION_MFID)
        self.assertIsNone(pb.task_link_dataset_to_session.fn(DATASET_MFID, None))

        # Dataset-sample links
        linked_count = pb.task_link_dataset_and_sample.fn(DATASET_MFID, [SAMPLE_MFID, CHILD_SAMPLE_MFID])
        self.assertEqual(linked_count, 2)

        # Postprocessing pipelines
        for slug in ("insitu_aggregation", "carrier_segmentation"):
            res = pb.task_request_post_processing.fn(slug, DATASET_MFID)
            self.assertEqual(res["pipeline"], slug)
            self.assertEqual(res["status"], "requested")

        post_eps = [(m, ep, kw.get("json")) for m, ep, kw in self.requests_log]
        self.assertIn(("post", f"/datasets/{SESSION_MFID}/children/{DATASET_MFID}", None), post_eps)
        self.assertIn(("post", f"/datasets/{DATASET_MFID}/samples/{SAMPLE_MFID}", None), post_eps)
        self.assertIn(("post", f"/datasets/{DATASET_MFID}/samples/{CHILD_SAMPLE_MFID}", None), post_eps)
        self.assertIn(
            ("post", f"/datasets/{DATASET_MFID}/postprocess", {"pipeline": "insitu_aggregation", "parameters": {}}),
            post_eps,
        )
        self.assertIn(
            ("post", f"/datasets/{DATASET_MFID}/postprocess", {"pipeline": "carrier_segmentation", "parameters": {}}),
            post_eps,
        )

    def test_flow_preview_upload_calls_set_public_when_packet_public_is_true(self):
        from crucible_ingestion.packet import IngestionPacket

        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("get", f"/datasets/{DATASET_MFID}"):
                return {"unique_id": DATASET_MFID}
            if (method, endpoint) == ("put", f"/resources/{DATASET_MFID}/access/public"):
                return {
                    "principal_id": "public",
                    "principal_type": "public",
                    "permission": "viewer",
                }
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        self.bind_v4_client(router)

        pkt = IngestionPacket(
            unique_id=DATASET_MFID,
            ingestion_class="GcControlfileIngestor",
            dataset_fields={"dataset_name": "PreviewDS", "public": True},
            scientific_metadata={"parse_provenance": {"files": [{"file": "a.txt", "ingestion_class": "GcControlfileIngestor"}]}},
        )
        pkt.to_json(str(pb.preview_packet_path(DATASET_MFID)))
        self.addCleanup(pb.delete_preview_packet, DATASET_MFID)

        with patch.object(pb, "task_create_dataset", side_effect=AssertionError("dataset_exists should be True")), \
             patch.object(pb, "task_push_local_packet", side_effect=lambda p: None), \
             patch.object(pb, "task_push_local_file", side_effect=lambda *a: None), \
             patch.object(pb, "task_link_dataset_and_sample", side_effect=lambda *a: None), \
             patch.object(pb, "task_link_dataset_to_session", side_effect=lambda *a: None), \
             patch.object(pb, "_run_post_processing"):
            out = pb.flow_preview_upload.fn(
                files=["/tmp/a.txt"],
                dsid=DATASET_MFID,
                instrument_name="gc-b30-001",
                project_id="MFP00001",
                orcid=USER_ORCID,
            )

        self.assertEqual(out, DATASET_MFID)
        self.assertIn(
            ("put", f"/resources/{DATASET_MFID}/access/public"),
            [(m, ep) for m, ep, _ in self.requests_log],
        )

    def test_task_create_dataset_reraises_409_conflict_without_deletion_cleanup(self):
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tf:
            tf.write(b"data")
            tmp_path = tf.name
        self.addCleanup(lambda: os.path.exists(tmp_path) and os.unlink(tmp_path))

        v4_client = self.bind_v4_client(lambda *a: None)
        conflict_error = self.make_api_error(status=409, detail="Dataset with unique_id already exists")

        with patch.object(v4_client.datasets, "create", side_effect=conflict_error), \
             patch.object(v4_client.datasets, "list_files") as mock_list_files, \
             patch.object(v4_client.deletions, "request") as mock_del_request:
            with self.assertRaises(CrucibleAPIError) as ctx:
                pb.task_create_dataset.fn(
                    files=[tmp_path],
                    instrument_name="themis",
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    dsid=DATASET_MFID,
                    wait_for_ingestion=False,
                )
            self.assertEqual(ctx.exception.status, 409)
            mock_list_files.assert_not_called()
            mock_del_request.assert_not_called()

    def test_flow_upload_dataset_routes_to_update_when_dsid_exists_and_create_otherwise(self):
        with patch.object(pb, "task_link_dataset_to_session") as mock_link_session, \
             patch.object(pb, "task_link_dataset_and_sample") as mock_link_sample, \
             patch.object(pb, "_run_post_processing") as mock_post_proc, \
             patch.object(pb, "task_update_dataset") as mock_update_ds, \
             patch.object(pb, "task_create_dataset") as mock_create_ds:

            mock_update_ds.return_value = DATASET_MFID
            mock_create_ds.return_value = DATASET_MFID

            # Case 1: dsid exists -> routes to task_update_dataset
            with patch.object(pb, "dataset_exists", return_value=True) as mock_exists:
                res = pb.flow_upload_dataset.fn(
                    files=["/tmp/test.txt"],
                    instrument_name="themis",
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    session_name="sess1",
                    session_dsid=SESSION_MFID,
                    dsid=DATASET_MFID,
                    sample_unique_id=SAMPLE_MFID,
                    kw_list=["kw1"],
                    comments="some comment",
                    ingestor="ApiUploadIngestor",
                )
                self.assertEqual(res, DATASET_MFID)
                mock_exists.assert_called_with(DATASET_MFID)
                mock_update_ds.assert_called_once_with(
                    files=["/tmp/test.txt"],
                    dsid=DATASET_MFID,
                    instrument_name="themis",
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    session_name="sess1",
                    kw_list=["kw1"],
                    comments="some comment",
                    ingestor="ApiUploadIngestor",
                )
                mock_create_ds.assert_not_called()
                mock_link_session.assert_called_once_with(DATASET_MFID, SESSION_MFID)
                mock_link_sample.assert_called_once_with(DATASET_MFID, SAMPLE_MFID)
                mock_post_proc.assert_called_once_with("themis", DATASET_MFID)

            mock_update_ds.reset_mock()
            mock_create_ds.reset_mock()
            mock_link_session.reset_mock()
            mock_link_sample.reset_mock()
            mock_post_proc.reset_mock()

            # Case 2: dsid does not exist -> routes to task_create_dataset
            with patch.object(pb, "dataset_exists", return_value=False) as mock_exists:
                res = pb.flow_upload_dataset.fn(
                    files=["/tmp/test.txt"],
                    instrument_name="themis",
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    session_name="sess1",
                    session_dsid=SESSION_MFID,
                    dsid=DATASET_MFID,
                    sample_unique_id=SAMPLE_MFID,
                    kw_list=["kw1"],
                    comments="some comment",
                    ingestor="ApiUploadIngestor",
                )
                self.assertEqual(res, DATASET_MFID)
                mock_create_ds.assert_called_once_with(
                    files=["/tmp/test.txt"],
                    instrument_name="themis",
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    session_name="sess1",
                    dsid=DATASET_MFID,
                    kw_list=["kw1"],
                    comments="some comment",
                    ingestor="ApiUploadIngestor",
                )
                mock_update_ds.assert_not_called()

    def test_flow_multi_assignment_upload_routes_to_update_and_preserves_excluded_uuids(self):
        with patch.object(pb, "task_link_dataset_and_sample") as mock_link_sample, \
             patch.object(pb, "_run_post_processing") as mock_post_proc, \
             patch.object(pb, "task_update_dataset") as mock_update_ds, \
             patch.object(pb, "task_create_dataset") as mock_create_ds:

            mock_update_ds.return_value = DATASET_MFID
            mock_create_ds.return_value = DATASET_MFID

            # Case 1: dsid exists -> routes to task_update_dataset with excluded_uuids
            with patch.object(pb, "dataset_exists", return_value=True):
                res = pb.flow_multi_assignment_upload.fn(
                    file="/tmp/test.txt",
                    sample_uuids=[SAMPLE_MFID],
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    instrument_name="themis",
                    dsid=DATASET_MFID,
                    kw_list=["kw1"],
                    comments="multi assignment comment",
                    ingestor="ApiUploadIngestor",
                    excluded_uuids=["ex-uuid-1", "ex-uuid-2"],
                    link_samples=True,
                )
                self.assertEqual(res, DATASET_MFID)
                mock_update_ds.assert_called_once_with(
                    files=["/tmp/test.txt"],
                    dsid=DATASET_MFID,
                    instrument_name="themis",
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    kw_list=["kw1"],
                    comments="multi assignment comment",
                    ingestor="ApiUploadIngestor",
                    excluded_uuids=["ex-uuid-1", "ex-uuid-2"],
                )
                mock_create_ds.assert_not_called()
                mock_link_sample.assert_called_once_with(DATASET_MFID, [SAMPLE_MFID])
                mock_post_proc.assert_called_once_with("themis", DATASET_MFID)

            mock_update_ds.reset_mock()
            mock_create_ds.reset_mock()
            mock_link_sample.reset_mock()
            mock_post_proc.reset_mock()

            # Case 2: dsid does not exist -> routes to task_create_dataset
            with patch.object(pb, "dataset_exists", return_value=False):
                res = pb.flow_multi_assignment_upload.fn(
                    file="/tmp/test.txt",
                    sample_uuids=[SAMPLE_MFID],
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    instrument_name="themis",
                    dsid=DATASET_MFID,
                    kw_list=["kw1"],
                    comments="multi assignment comment",
                    ingestor="ApiUploadIngestor",
                    excluded_uuids=["ex-uuid-1", "ex-uuid-2"],
                    link_samples=False,
                )
                self.assertEqual(res, DATASET_MFID)
                mock_create_ds.assert_called_once_with(
                    files=["/tmp/test.txt"],
                    instrument_name="themis",
                    project_id="MFP00001",
                    orcid=USER_ORCID,
                    dsid=DATASET_MFID,
                    kw_list=["kw1"],
                    comments="multi assignment comment",
                    ingestor="ApiUploadIngestor",
                    excluded_uuids=["ex-uuid-1", "ex-uuid-2"],
                )
                mock_update_ds.assert_not_called()
                mock_link_sample.assert_not_called()

        # Case 3: test_task_update_dataset preserves excluded_uuids -> 'skipped thin films'
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tf:
            tf.write(b"data")
            tmp_path = tf.name
        self.addCleanup(lambda: os.path.exists(tmp_path) and os.unlink(tmp_path))

        metadata_patches = []

        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("get", f"/datasets/{DATASET_MFID}"):
                return {"unique_id": DATASET_MFID, "owner_orcid": USER_ORCID, "project_id": "MFP00001"}
            if (method, endpoint) == ("patch", f"/resources/{DATASET_MFID}/metadata"):
                metadata_patches.append(kwargs["json"])
                return kwargs["json"]
            if (method, endpoint) == ("post", f"/datasets/{DATASET_MFID}/keywords"):
                return {"keyword": kwargs["params"]["keyword"]}
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        v4_client = self.bind_v4_client(router)
        with patch.object(v4_client.datasets, "add_file", return_value={"id": "f1"}):
            out = pb.task_update_dataset.fn(
                files=[tmp_path],
                dsid=DATASET_MFID,
                instrument_name="themis",
                project_id="MFP00001",
                orcid=USER_ORCID,
                comments="assignment update",
                excluded_uuids=["film-uuid-1", "film-uuid-2"],
                wait_for_ingestion=False,
            )
            self.assertEqual(out, DATASET_MFID)

        self.assertEqual(len(metadata_patches), 1)
        self.assertEqual(metadata_patches[0].get("skipped thin films"), ["film-uuid-1", "film-uuid-2"])
        self.assertEqual(metadata_patches[0].get("comments"), "assignment update")

    def test_task_push_local_packet_handles_children_without_dsid_in_response(self):
        from crucible_ingestion.packet import IngestionPacket

        mock_client = MagicMock()
        mock_datasets = MagicMock()
        mock_client.datasets = mock_datasets

        mock_datasets.update.return_value = {"unique_id": DATASET_MFID}
        mock_datasets.list_children.return_value = []

        # Return exact nano-crucible 4.0.0 shape (no 'dsid' key)
        original_create = MagicMock(return_value={
            "dataset_mfid": CHILD_DATASET_MFID,
            "created_record": {"unique_id": CHILD_DATASET_MFID},
            "scientific_metadata_record": None,
            "files": [],
        })
        mock_datasets.create = original_create
        mock_datasets.link = MagicMock(return_value={"parent_dataset_id": DATASET_MFID, "child_dataset_id": CHILD_DATASET_MFID})
        mock_datasets.update_scientific_metadata.return_value = {}

        child_entry = {
            "dataset": {
                "dataset_name": "child-test-1",
                "owner_orcid": USER_ORCID,
                "project_id": "MFP00001",
            },
            "scientific_metadata": {"child_meta": "val"},
            "parent_id": DATASET_MFID,
            "sample_links": [],
            "files_to_upload": [],
            "thumbnails": [],
        }

        packet = IngestionPacket(
            unique_id=DATASET_MFID,
            ingestion_class="TestIngestor",
            dataset_fields={"dataset_name": "parent-ds"},
            scientific_metadata={},
            keywords=[],
            samples=[],
            children=[child_entry],
            thumbnails=[],
        )

        with patch.object(pb, "_require_client", return_value=mock_client):
            pb.task_push_local_packet.fn(packet)

        # Verified: child was created and linked to parent
        original_create.assert_called_once()
        mock_datasets.link.assert_called_once_with(DATASET_MFID, CHILD_DATASET_MFID)
        # Verified: mock_datasets.create is untouched
        self.assertIs(mock_datasets.create, original_create)

        # Also verify against a real v4 CrucibleClient under concurrent threads:
        # c.datasets.__dict__ is never polluted with an instance-level "create" attribute.
        import concurrent.futures

        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("patch", f"/datasets/{DATASET_MFID}"):
                return {"unique_id": DATASET_MFID}
            if (method, endpoint) == ("get", f"/datasets/{DATASET_MFID}/children"):
                return {"items": [], "next_cursor": None}
            if (method, endpoint) == ("post", "/datasets"):
                return {"unique_id": CHILD_DATASET_MFID}
            if method in ("post", "patch") and endpoint in (
                f"/resources/{DATASET_MFID}/metadata",
                f"/resources/{CHILD_DATASET_MFID}/metadata",
            ):
                return kwargs.get("json") or {}
            if (method, endpoint) == ("post", f"/datasets/{DATASET_MFID}/children/{CHILD_DATASET_MFID}"):
                return {"parent_dataset_id": DATASET_MFID, "child_dataset_id": CHILD_DATASET_MFID}
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        v4_client = self.bind_v4_client(router)
        self.assertNotIn("create", vars(v4_client.datasets))
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futs = [pool.submit(pb.task_push_local_packet.fn, packet) for _ in range(4)]
            for fut in futs:
                fut.result()
        self.assertNotIn("create", vars(v4_client.datasets))


# =============================================================================
# Group 6: main.py Flask Routes, Guards, Preview Owner Key & Hub Links
# =============================================================================
class TestMainFlaskRoutesAndHubLinks(HermeticTestCase):
    def test_routes_return_clean_error_when_backend_client_is_none(self):
        flask_client = main.app.test_client()
        with patch.object(pb, "client", None):
            r_children = flask_client.post("/api/resolve_children", json={"sample_uuid": SAMPLE_MFID})
            self.assertEqual(r_children.status_code, 500)
            self.assertIn("not configured", r_children.get_json()["error"])

            r_carrier = flask_client.get("/api/photobox/next_carrier_name?project_id=MFP00001")
            self.assertEqual(r_carrier.status_code, 500)
            self.assertIn("not configured", r_carrier.get_json()["error"])

            r_inst = flask_client.get("/api/instruments")
            self.assertEqual(r_inst.status_code, 200)
            self.assertEqual(r_inst.get_json()["instruments"], [])
            self.assertIn("not configured", r_inst.get_json()["error"])

            r_search = flask_client.get("/api/instruments/search?q=themis")
            self.assertEqual(r_search.status_code, 200)
            self.assertEqual(r_search.get_json()["instruments"], [])
            self.assertIn("not configured", r_search.get_json()["error"])

    def test_instruments_search_min_length_guard(self):
        def router(method, endpoint, kwargs):
            if (method, endpoint) == ("get", "/instruments/search"):
                self.assertEqual(kwargs["params"]["q"], "tem")
                return {
                    "items": [{
                        "unique_id": "0t6k7ms7r5zak000bwynwcfacr",
                        "instrument_id": "themis",
                        "instrument_name": "Themis Z",
                    }]
                }
            raise AssertionError(f"Unexpected request: {method} {endpoint} {kwargs}")

        self.bind_v4_client(router)
        flask_client = main.app.test_client()

        for short_q in ("", "a", "ab", "  a  "):
            self.assertEqual(pb.search_instruments(short_q), [])
            resp = flask_client.get("/api/instruments/search", query_string={"q": short_q})
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json(), {"instruments": [], "labels": {}})
        self.assertEqual(len(self.requests_log), 0)

        resp_ok = flask_client.get("/api/instruments/search?q=tem")
        self.assertEqual(resp_ok.status_code, 200)
        self.assertIn("themis", resp_ok.get_json()["instruments"])
        self.assertEqual(len(self.requests_log), 1)

    def test_preview_packet_uses_single_clean_owner_orcid_key(self):
        from crucible_ingestion.packet import IngestionPacket

        self.bind_v4_client(lambda *a: {"items": [], "next_cursor": None})
        flask_client = main.app.test_client()

        fake_pkt = IngestionPacket(
            unique_id=DATASET_MFID,
            ingestion_class="InorganicXrdIngestor",
            dataset_fields={"dataset_name": "XRD-1", "owner_orcid": None, "project_id": None},
            scientific_metadata={"angle": 25.0},
        )
        self.addCleanup(pb.delete_preview_packet, DATASET_MFID)

        with patch.object(pb, "existing_dsids", return_value=set()), \
             patch.object(pb, "read_h5_dsid", return_value=None), \
             patch.object(pb, "resolve_dsid_for_file", return_value=(DATASET_MFID, False)), \
             patch.object(pb, "parse_for_preview", return_value=(fake_pkt, [], [{"file": "scan.txt", "ingestion_class": "InorganicXrdIngestor"}], [])):
            resp = flask_client.post("/api/preview", json={
                "orcid": USER_ORCID,
                "project_id": "MFP00001",
                "instrument_name": "inorganic_xrd",
                "files": ["/tmp/scan.txt"],
            })

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        other_fields = payload["other_dataset_fields"]
        self.assertEqual(other_fields.get("owner_orcid"), USER_ORCID)
        self.assertNotIn("owner", other_fields)

    def test_index_renders_hub_links_and_zero_explore_links(self):
        flask_client = main.app.test_client()

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CRUCIBLE_HUB_URL", None)
            resp = flask_client.get("/")
        self.assertEqual(resp.status_code, 200)
        html = resp.get_data(as_text=True)
        self.assertIn("https://crucible.lbl.gov/hub", html)
        self.assertIn("/datasets/${encodeURIComponent(dsid)}", html)
        self.assertIn("/projects/${encodeURIComponent(project_id)}", html)
        self.assertNotIn("crucible.lbl.gov/explore", html)
        self.assertNotIn("CRUCIBLE_EXPLORE_URL", html)

        with patch.dict(os.environ, {"CRUCIBLE_HUB_URL": "https://staging.crucible.lbl.gov/hub/"}):
            resp_staging = flask_client.get("/")
        self.assertEqual(resp_staging.status_code, 200)
        html_staging = resp_staging.get_data(as_text=True)
        self.assertIn('"https://staging.crucible.lbl.gov/hub"', html_staging)
        self.assertNotIn("crucible.lbl.gov/explore", html_staging)


if __name__ == "__main__":
    unittest.main()
