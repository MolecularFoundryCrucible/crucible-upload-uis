"""Offline tests for mixed Attolight/SUR SPECTRE session uploads."""
from __future__ import annotations

import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch
from types import SimpleNamespace

import requests

with patch.object(requests.Session, "send", side_effect=AssertionError("Network blocked during import")):
    import prefect_backend as pb


SAMPLE_MFID = "0t6k7ms7r5zak000bwynwcfacr"
DATASET_MFID = "0t6k7ms7r5zak000bwynwcfacy"
SESSION_MFID = "0t6k7ms7r5zak000bwynwcfacz"


def write_manifest(directory: Path, stem: str, sur_names: list[str], txt_name: str | None = None):
    directory.mkdir(parents=True, exist_ok=True)
    txt_name = txt_name or f"{stem}.txt"
    xml = directory / f"{stem}.xml"
    txt = directory / txt_name
    surs = [directory / name for name in sur_names]
    entries = [xml.name, txt_name, *sur_names]
    xml.write_text(
        "<AttolightWorkspace><AcquisitionType>CL</AcquisitionType><Files>"
        + "".join(f"<File>{name}</File>" for name in entries)
        + "</Files></AttolightWorkspace>",
        encoding="utf-8",
    )
    txt.write_text("_General\noperating_mode: SEM\n", encoding="cp1252")
    for sur in surs:
        sur.write_bytes(b"test SUR placeholder")
    return [xml, txt, *surs]


class SpectreSessionTests(unittest.TestCase):
    def setUp(self):
        self._send_patcher = patch.object(
            requests.Session, "send",
            side_effect=AssertionError("Hermetic test attempted network access"),
        )
        self._send_patcher.start()
        self.addCleanup(self._send_patcher.stop)
        self._logger_patcher = patch.object(
            pb, "get_run_logger", return_value=logging.getLogger("spectre_test")
        )
        self._logger_patcher.start()
        self.addCleanup(self._logger_patcher.stop)
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name) / "run"
        self.root.mkdir()

    def test_mixed_session_routes_groups_standalone_sur_and_ordinary_files(self):
        group_a = write_manifest(self.root / "a", "workspace_a", ["a1.sur", "a2.sur", "a3.sur"])
        group_b = write_manifest(self.root / "b", "workspace_b", ["b1.sur"])
        standalone = self.root / "standalone.SUR"
        standalone.write_bytes(b"standalone")
        orphan_txt = self.root / "notes.txt"
        orphan_txt.write_text("not referenced by a manifest", encoding="utf-8")
        ordinary = self.root / "image.dm3"
        ordinary.write_bytes(b"ordinary")
        other_xml = self.root / "other.xml"
        other_xml.write_text("<OtherWorkspace />", encoding="utf-8")

        files = [str(p) for p in [*group_a, *group_b, standalone, orphan_txt, ordinary, other_xml]]
        plan = pb.plan_spectre_session_files(files)

        self.assertEqual([g["xml"] for g in plan["groups"]], [str(group_a[0]), str(group_b[0])])
        self.assertEqual(plan["groups"][0]["sur_files"], [str(p) for p in group_a[2:]])
        self.assertEqual(plan["groups"][1]["sur_files"], [str(group_b[2])])
        self.assertEqual(plan["standalone_sur"], [str(standalone)])
        self.assertEqual(
            set(plan["ordinary_files"]), {str(orphan_txt), str(ordinary), str(other_xml)}
        )
        self.assertNotIn(str(group_a[1]), plan["ordinary_files"])
        self.assertNotIn(str(group_a[2]), plan["standalone_sur"])

    def test_discovery_includes_spectre_xml_and_sur(self):
        grouped = write_manifest(self.root, "workspace", ["one.sur", "two.sur"])
        standalone = self.root / "single.sur"
        standalone.write_bytes(b"standalone")
        discovered = pb.task_identify_session_files.fn(str(self.root), "spectre")
        self.assertEqual(set(discovered), {str(p) for p in [*grouped, standalone]})

    def test_non_attolight_and_malformed_xml_remain_ordinary(self):
        other = self.root / "other.xml"
        other.write_text("<Other />", encoding="utf-8")
        malformed = self.root / "malformed.xml"
        malformed.write_text("<AttolightWorkspace>", encoding="utf-8")
        plan = pb.plan_spectre_session_files([str(other), str(malformed)])
        self.assertEqual(plan["groups"], [])
        self.assertEqual(plan["ordinary_files"], [str(other), str(malformed)])

    def test_missing_manifest_sibling_fails_preflight(self):
        xml = self.root / "broken.xml"
        xml.write_text(
            "<AttolightWorkspace><Files><File>broken.txt</File>"
            "<File>missing.sur</File></Files></AttolightWorkspace>",
            encoding="utf-8",
        )
        (self.root / "broken.txt").write_text("metadata", encoding="utf-8")
        with self.assertRaisesRegex(FileNotFoundError, "missing.sur"):
            pb.plan_spectre_session_files([str(xml), str(self.root / "broken.txt")])

    def test_two_manifests_cannot_claim_the_same_file(self):
        write_manifest(self.root, "first", ["shared.sur"])
        second = self.root / "second.xml"
        second.write_text(
            "<AttolightWorkspace><Files><File>second.txt</File>"
            "<File>shared.sur</File></Files></AttolightWorkspace>",
            encoding="utf-8",
        )
        (self.root / "second.txt").write_text("metadata", encoding="utf-8")
        files = [str(self.root / name) for name in
                 ["first.xml", "first.txt", "second.xml", "second.txt", "shared.sur"]]
        with self.assertRaisesRegex(ValueError, "claimed by both"):
            pb.plan_spectre_session_files(files)

    def test_duplicate_group_names_across_directories_are_rejected(self):
        first = write_manifest(self.root / "one", "workspace", ["one.sur"])
        second = write_manifest(self.root / "two", "workspace", ["two.sur"])
        with self.assertRaisesRegex(ValueError, "Duplicate Attolight group name"):
            pb.plan_spectre_session_files([str(p) for p in [*first, *second]])

    def test_manifest_requires_exactly_one_txt(self):
        xml = self.root / "workspace.xml"
        xml.write_text(
            "<AttolightWorkspace><Files><File>one.sur</File></Files></AttolightWorkspace>",
            encoding="utf-8",
        )
        sur = self.root / "one.sur"
        sur.write_bytes(b"sur")
        with self.assertRaisesRegex(ValueError, "exactly one TXT"):
            pb.plan_spectre_session_files([str(xml), str(sur)])

    def test_local_parse_supplies_sur_paths_and_sample_links(self):
        sur_paths = [str(self.root / "one.sur"), str(self.root / "two.sur")]
        packet = SimpleNamespace(
            ingestion_class="AttolightCLIngestor",
            children=[
                {"dataset": {"dataset_name": "one.sur"}, "sample_links": []},
                {"dataset": {"dataset_name": "two.sur"}, "sample_links": []},
            ],
        )
        with patch.object(pb, "_require_client", return_value=MagicMock()), \
             patch("crucible_ingestion.data_ingestion.parse", return_value=packet) as mock_parse, \
             patch("crucible_ingestion.set_client"):
            result = pb.task_parse_attolight_group.fn(
                str(self.root / "workspace.xml"), DATASET_MFID, sur_paths, SAMPLE_MFID
            )

        self.assertIs(result, packet)
        mock_parse.assert_called_once_with(
            str(self.root / "workspace.xml"), DATASET_MFID, "AttolightCLIngestor"
        )
        for child, path in zip(packet.children, sur_paths):
            self.assertEqual(child["files_to_upload"], [path])
            self.assertEqual(child["sample_links"], [SAMPLE_MFID])

    def test_local_parse_rejects_missing_sur_children(self):
        packet = SimpleNamespace(ingestion_class="AttolightCLIngestor", children=[])
        with patch.object(pb, "_require_client", return_value=MagicMock()), \
             patch("crucible_ingestion.data_ingestion.parse", return_value=packet), \
             patch("crucible_ingestion.set_client"):
            with self.assertRaisesRegex(RuntimeError, "expected 1"):
                pb.task_parse_attolight_group.fn(
                    str(self.root / "workspace.xml"), DATASET_MFID,
                    [str(self.root / "one.sur")], SAMPLE_MFID,
                )

    def test_group_flow_uploads_sources_packet_and_links_parent_sample(self):
        packet = SimpleNamespace(ingestion_class="AttolightCLIngestor")
        xml = str(self.root / "workspace.xml")
        txt = str(self.root / "workspace.txt")
        surs = [str(self.root / "one.sur"), str(self.root / "two.sur")]
        with patch.object(pb, "dataset_exists", return_value=False), \
             patch.object(pb, "task_create_dataset", return_value=DATASET_MFID) as mock_create, \
             patch.object(pb, "task_parse_attolight_group", return_value=packet) as mock_parse, \
             patch.object(pb, "task_push_local_packet") as mock_push_packet, \
             patch.object(pb, "task_push_local_file") as mock_push_file, \
             patch.object(pb, "task_link_dataset_to_session") as mock_link_session, \
             patch.object(pb, "task_link_dataset_and_sample") as mock_link_sample, \
             patch.object(pb, "_run_post_processing") as mock_post:
            result = pb.flow_attolight_group_upload.fn(
                files=[xml, txt], sur_files=surs, instrument_name="spectre",
                project_id="MFP00001", orcid="owner", session_name="run",
                session_dsid=SESSION_MFID, dsid=DATASET_MFID,
                sample_unique_id=SAMPLE_MFID, kw_list=["kw"], comments="comment",
                ingestor="AttolightCLIngestor",
            )

        self.assertEqual(result, DATASET_MFID)
        self.assertEqual(mock_create.call_args.kwargs["files"], [])
        self.assertEqual(mock_create.call_args.kwargs["dataset_name"], "workspace.xml")
        self.assertFalse(mock_create.call_args.kwargs["wait_for_ingestion"])
        mock_parse.assert_called_once_with(xml, DATASET_MFID, surs, SAMPLE_MFID)
        mock_push_packet.assert_called_once_with(packet)
        self.assertEqual(mock_push_file.call_args_list, [
            call(DATASET_MFID, xml, "AttolightCLIngestor"),
            call(DATASET_MFID, txt, "AttolightCLIngestor"),
        ])
        mock_link_session.assert_called_once_with(DATASET_MFID, SESSION_MFID)
        mock_link_sample.assert_called_once_with(DATASET_MFID, SAMPLE_MFID)
        mock_post.assert_called_once_with("spectre", DATASET_MFID)

    def test_spectre_flow_forces_file_specific_ingestors_only_for_xml_and_sur(self):
        grouped = write_manifest(self.root, "workspace", ["one.sur"])
        standalone = self.root / "standalone.sur"
        standalone.write_bytes(b"standalone")
        ordinary = self.root / "image.dm3"
        ordinary.write_bytes(b"ordinary")
        all_files = [str(p) for p in [*grouped, standalone, ordinary]]

        with patch.object(pb, "check_session_depth"), \
             patch.object(pb, "task_identify_session_files", return_value=all_files), \
             patch.object(pb, "create_session", return_value=("run", SESSION_MFID)), \
             patch.object(pb, "_run_session_children") as mock_run:
            result = pb.flow_spectre_session_upload.fn(
                file=str(self.root), instrument_name="spectre", project_id="MFP00001",
                orcid="owner", session_dsid=SESSION_MFID,
                sample_unique_id=SAMPLE_MFID, ingestor="UserSelectedIngestor",
            )

        self.assertEqual(result, SESSION_MFID)
        specs = mock_run.call_args.args[0]
        self.assertEqual(len(specs), 3)
        self.assertEqual(specs[0]["deployment"], "attolight-group-upload/attolight-group-upload")
        self.assertEqual(specs[0]["files"], [str(grouped[0]), str(grouped[1])])
        self.assertEqual(specs[0]["sur_files"], [str(grouped[2])])
        self.assertEqual(specs[0]["ingestor"], "AttolightCLIngestor")
        self.assertEqual(specs[1]["primary_file"], str(standalone))
        self.assertEqual(specs[1]["ingestor"], "SurIngestor")
        self.assertNotIn("deployment", specs[1])
        self.assertEqual(specs[2]["primary_file"], str(ordinary))
        self.assertEqual(specs[2]["ingestor"], "UserSelectedIngestor")

    def test_spectre_instrument_uses_dedicated_flow_and_accepts_new_extensions(self):
        from instruments.registry import INSTRUMENT_FLOWS, INSTRUMENT_FILE_TYPES
        self.assertEqual(
            INSTRUMENT_FLOWS["spectre"], "spectre-session-upload/spectre-session-upload"
        )
        self.assertLessEqual({".xml", ".sur"}, INSTRUMENT_FILE_TYPES["spectre"])


if __name__ == "__main__":
    unittest.main()
