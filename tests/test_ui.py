"""Unit tests for crucible-tem-upload-ui."""
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch, MagicMock
import requests

# On a clean checkout without gitignored instrument_conf.py, load instrument_conf.default.py
if "instrument_conf" not in sys.modules and importlib.util.find_spec("instrument_conf") is None:
    _default_conf_path = Path(__file__).resolve().parent.parent / "instrument_conf.default.py"
    _spec = importlib.util.spec_from_file_location("instrument_conf", _default_conf_path)
    _mod = importlib.util.module_from_spec(_spec)
    assert _spec is not None and _spec.loader is not None
    _spec.loader.exec_module(_mod)
    sys.modules["instrument_conf"] = _mod

# Patch module-level side effects before first import
_mock_tk = MagicMock()
_mock_crucible_client = MagicMock(api_key='test')

with patch('tkinter.Tk', return_value=_mock_tk), \
     patch('crucible.CrucibleClient', return_value=_mock_crucible_client), \
     patch.object(requests.Session, 'send', side_effect=AssertionError("Real HTTP blocked during import")):
    import main
    import prefect_backend as pb

import ai_services


# ── prefect_backend.get_emi_file_name ────────────────────────────────────────

class TestGetEmiFileName(unittest.TestCase):
    def test_plain_ser_returns_emi(self):
        assert pb.get_emi_file_name("scan.ser") == "scan.emi"

    def test_numbered_suffix_stripped(self):
        # _001 suffix should be removed before swapping extension
        assert pb.get_emi_file_name("scan_001.ser") == "scan.emi"

    def test_large_numbered_suffix_stripped(self):
        assert pb.get_emi_file_name("sample_042.ser") == "sample.emi"


# ── GC-EC electrode sample resolution ───────────────────────────────────────

class TestApplySampleMetadata(unittest.TestCase):
    def setUp(self):
        self.packet = MagicMock()
        self.packet.scientific_metadata = {
            "anode_material": "value parsed from file",
        }
        self.packet.samples = [{"unique_id": "existing", "sample_name": "Existing"}]
        self.fields = {
            "anode_material": "anode_sample_mfid",
            "cathode_material": "cathode_sample_mfid",
            "electrolyte": "electrolyte_sample_mfid",
        }

    @patch.object(pb, "find_samples")
    def test_records_names_and_stages_samples_for_linking(self, find_samples):
        find_samples.side_effect = [
            [{"unique_id": "anode-id", "sample_name": "Lithium"}],
            [{"unique_id": "cathode-id", "sample_name": "LFP"}],
            [{"unique_id": "electrolyte-id", "sample_name": "LP30"}],
        ]

        resolved = pb.apply_sample_metadata(
            self.packet,
            self.fields,
            {
                "anode_sample_mfid": "anode-id",
                "cathode_sample_mfid": "cathode-id",
                "electrolyte_sample_mfid": "electrolyte-id",
            },
            "MFP00001",
        )

        self.assertEqual(self.packet.scientific_metadata["anode_material"], {
            "sample_name": "Lithium",
            "sample_mfid": "anode-id",
        })
        self.assertEqual(self.packet.scientific_metadata["cathode_material"], {
            "sample_name": "LFP",
            "sample_mfid": "cathode-id",
        })
        self.assertEqual(self.packet.scientific_metadata["electrolyte"], {
            "sample_name": "LP30",
            "sample_mfid": "electrolyte-id",
        })
        self.assertEqual([s["unique_id"] for s in self.packet.samples],
                         ["existing", "anode-id", "cathode-id", "electrolyte-id"])
        self.assertEqual(resolved[-1]["sample_name"], "LP30")
        find_samples.assert_any_call(sample_unique_id="anode-id", project_id="MFP00001")

    def test_requires_every_configured_sample_mfid(self):
        with self.assertRaisesRegex(ValueError, "cathode material sample MFID is required"):
            pb.apply_sample_metadata(
                self.packet,
                self.fields,
                {"anode_sample_mfid": "anode-id"},
                "MFP00001",
            )

    @patch.object(pb, "find_samples", return_value=[])
    def test_rejects_sample_outside_selected_project(self, _find_samples):
        with self.assertRaisesRegex(ValueError, "No anode material sample found"):
            pb.apply_sample_metadata(
                self.packet,
                self.fields,
                {
                    "anode_sample_mfid": "wrong-project",
                    "cathode_sample_mfid": "cathode-id",
                    "electrolyte_sample_mfid": "electrolyte-id",
                },
                "MFP00001",
            )


# ── prefect_backend.check_session_depth ──────────────────────────────────────

class TestCheckSessionDepth(unittest.TestCase):
    def test_raises_for_root_path_at_default_depth(self):
        with self.assertRaises(ValueError):
            pb.check_session_depth("/")

    def test_raises_for_shallow_path_when_min_depth_is_2(self):
        with self.assertRaises(ValueError):
            pb.check_session_depth("/data", min_depth=2)

    def test_passes_for_deep_path(self):
        pb.check_session_depth("/data/sessions/2024/mysession")  # should not raise


# ── ai_services._is_non_english ──────────────────────────────────────────────

class TestIsNonEnglish(unittest.TestCase):
    def test_ascii_text_is_english(self):
        assert not ai_services._is_non_english("STEM imaging with HAADF detector at 300 kV")

    def test_mostly_non_ascii_detected(self):
        assert ai_services._is_non_english("こんにちは世界テスト実験データ")

    def test_empty_string_is_english(self):
        assert not ai_services._is_non_english("")


# ── ai_services._azure_available ─────────────────────────────────────────────

class TestAzureAvailable(unittest.TestCase):
    def test_unavailable_when_vars_empty(self):
        with patch.object(ai_services, 'AZURE_ENDPOINT', ''), \
             patch.object(ai_services, 'AZURE_API_KEY', ''):
            assert not ai_services._azure_available()

    def test_available_when_both_set(self):
        with patch.object(ai_services, 'AZURE_ENDPOINT', 'https://example.openai.azure.com'), \
             patch.object(ai_services, 'AZURE_API_KEY', 'secret-key'):
            assert ai_services._azure_available()


# ── Flask route validation ────────────────────────────────────────────────────

class TestFlaskRoutes(unittest.TestCase):
    def setUp(self):
        self._http_patcher = patch.object(
            requests.Session,
            "send",
            side_effect=AssertionError("Hermetic unit test attempted real HTTP request"),
        )
        self._http_patcher.start()
        self.addCleanup(self._http_patcher.stop)

        self._client_patcher = patch.object(pb, "client", MagicMock(api_key="test"))
        self.mock_pb_client = self._client_patcher.start()
        self.addCleanup(self._client_patcher.stop)

        self.client = main.app.test_client()

    def test_user_lookup_missing_email_returns_400(self):
        resp = self.client.post('/api/user/lookup', json={})
        self.assertEqual(resp.status_code, 400)

    def test_sample_lookup_missing_both_fields_returns_400(self):
        resp = self.client.post('/api/sample/lookup', json={"project_id": "MFP00001"})
        self.assertEqual(resp.status_code, 400)

    def test_upload_unconfigured_instrument_returns_400(self):
        with patch.dict(main.registry.INSTRUMENT_FLOWS, {"themis": None}), \
             patch.object(pb, "create_session", side_effect=AssertionError("create_session must not run")):
            resp = self.client.post('/api/upload', json={
                "orcid": "0000-0001-2345-6789",
                "project_id": "MFP00001",
                "instrument_name": "themis",
                "session_folder_path": "/data/sessions/test",
            })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("No upload flow configured", resp.get_json()["error"])


if __name__ == '__main__':
    unittest.main()
