from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.importers import canonical_json  # noqa: E402
from himr_corpus.torrent_aria2_selector import (  # noqa: E402
    ARIA2_EXECUTABLE_SHA256,
    ARIA2_RPM_SHA256,
    TorrentAria2SelectorError,
    build_aria2_selector_receipt,
    publish_private_aria2_selector_receipt,
)
from himr_corpus.torrent_bracket_reconciler import (  # noqa: E402
    _parse_torrent,
    _raw_path_sha256,
)
from himr_corpus.torrent_selective_planner import PLAN_KIND, PLANNER_VERSION  # noqa: E402


def bencode(value) -> bytes:
    if isinstance(value, bytes):
        return str(len(value)).encode("ascii") + b":" + value
    if isinstance(value, bool):
        raise TypeError("booleans are not bencode integers")
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii") + b"e"
    if isinstance(value, list):
        return b"l" + b"".join(bencode(item) for item in value) + b"e"
    if isinstance(value, dict):
        return b"d" + b"".join(
            bencode(key) + bencode(value[key]) for key in sorted(value)
        ) + b"e"
    raise TypeError(type(value))


class TorrentAria2SelectorTests(unittest.TestCase):
    def setUp(self) -> None:
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="aria2-selector-test-", dir=work)
        self.root = Path(self.temporary.name).resolve()
        self.paths = [[b"A", b"zero.mp4"], [b"B", b"selected.mp4"], [b"C", b"two.mp4"]]
        self.lengths = [5, 10, 7]
        piece_length = 8
        total = sum(self.lengths)
        info = {
            b"files": [
                {b"length": length, b"path": path}
                for path, length in zip(self.paths, self.lengths, strict=True)
            ],
            b"name": b"fixture",
            b"piece length": piece_length,
            b"pieces": b"p" * (math.ceil(total / piece_length) * 20),
        }
        self.torrent_path = self.root / "fixture.torrent"
        self.torrent_path.write_bytes(bencode({b"info": info}))
        self.torrent = _parse_torrent(self.torrent_path.read_bytes())
        self.plan_path = self.root / "plan.json"
        self._write_plan(self._plan())

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _plan(self) -> dict:
        index = 1
        manifest = self.torrent["files"][index]
        selected = {
            "youtube_video_id": "MissingID01",
            "torrent_file_index": index,
            "byte_count": manifest["byte_count"],
            "manifest_path": manifest["manifest_path"],
            "manifest_path_sha256": _raw_path_sha256(manifest["raw_components"]),
            "availability_evidence_code": "removed",
            "selection_reason": (
                "smallest_rendition_after_exact_archive_catalog_exclusion_and_no_download_"
                "youtube_unavailability_probe"
            ),
        }
        core = {
            "schema_version": 1,
            "plan_kind": PLAN_KIND,
            "planner_version": PLANNER_VERSION,
            "inputs": {
                "torrent_sha256": self.torrent["torrent_sha256"],
                "torrent_byte_count": self.torrent_path.stat().st_size,
                "torrent_filename": self.torrent_path.name,
                "info_hash_sha1": self.torrent["info_hash_sha1"],
                "discovery_sha256": "1" * 64,
                "discovery_byte_count": 1,
                "discovery_filename": "discovery.json",
                "combined_import_input_sha256": "2" * 64,
                "torrent_video_inventory_sha256": "3" * 64,
            },
            "catalog_binding": {},
            "archive_binding": {},
            "evidence_binding_sha256": "4" * 64,
            "coverage": {},
            "availability_probe_request": {},
            "availability_probe": {"probe_sha256": "5" * 64},
            "probe_candidates": [
                {
                    "smallest_rendition_file_index": index,
                    "availability_state": "unavailable",
                    "availability_evidence_code": "removed",
                }
            ],
            "malformed_manual_review": [],
            "selected_files": [selected],
            "selected_torrent_file_indices": [index],
            "statistics": {
                "selected_file_count": 1,
                "selected_payload_bytes": manifest["byte_count"],
            },
            "policy": {
                "read_only_plan": True,
                "torrent_client_invoked": False,
                "torrent_swarm_joined": False,
                "payload_downloaded_or_read": False,
                "operator_review_required_before_client_use": True,
                "selective_file_index_client_required": True,
                "publication_authority": False,
            },
        }
        digest = hashlib.sha256(canonical_json(core).encode("utf-8")).hexdigest()
        return {**core, "plan_id": f"tslp_{digest[:32]}", "plan_sha256": digest}

    def _write_plan(self, plan: dict) -> None:
        self.plan_path.write_text(
            json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )

    def test_builds_exact_one_based_selector_and_piece_capacity(self) -> None:
        receipt = build_aria2_selector_receipt(self.plan_path, self.torrent_path)
        selection = receipt["selection"]
        self.assertEqual(selection["zero_based_file_indices"], [1])
        self.assertEqual(selection["aria2_one_based_file_indices"], [2])
        self.assertEqual(selection["aria2_select_file_value"], "2")
        self.assertEqual(selection["selected_payload_bytes"], 10)
        self.assertEqual(selection["selected_piece_span_bytes"], 16)
        self.assertEqual(selection["boundary_piece_bytes_upper_bound"], 6)
        self.assertEqual(receipt["client_pin"]["rpm_sha256"], ARIA2_RPM_SHA256)
        self.assertEqual(
            receipt["client_pin"]["executable_sha256"], ARIA2_EXECUTABLE_SHA256
        )
        self.assertFalse(receipt["policy"]["torrent_client_invoked"])
        self.assertFalse(receipt["policy"]["network_actions_performed"])

    def test_private_receipt_writer_is_atomic_read_only_and_no_overwrite(self) -> None:
        receipt = build_aria2_selector_receipt(self.plan_path, self.torrent_path)
        output = self.root / "selector-receipt.json"
        summary = publish_private_aria2_selector_receipt(receipt, output)
        self.assertTrue(summary["selector_receipt_written"])
        self.assertFalse(summary["path_disclosed"])
        self.assertEqual(summary["receipt_sha256"], receipt["receipt_sha256"])
        self.assertEqual(os.stat(output).st_mode & 0o777, 0o400)
        self.assertEqual(json.loads(output.read_text(encoding="utf-8")), receipt)
        with self.assertRaisesRegex(TorrentAria2SelectorError, "already exists"):
            publish_private_aria2_selector_receipt(receipt, output)
        with self.assertRaisesRegex(TorrentAria2SelectorError, "absolute file path"):
            publish_private_aria2_selector_receipt(
                receipt, Path("relative-selector-receipt.json")
            )

    def test_refuses_empty_or_nonfinal_plan(self) -> None:
        plan = self._plan()
        plan["availability_probe"] = None
        self._reseal(plan)
        self._write_plan(plan)
        with self.assertRaisesRegex(TorrentAria2SelectorError, "complete bound"):
            build_aria2_selector_receipt(self.plan_path, self.torrent_path)

        plan = self._plan()
        plan["selected_files"] = []
        plan["selected_torrent_file_indices"] = []
        plan["statistics"]["selected_file_count"] = 0
        plan["statistics"]["selected_payload_bytes"] = 0
        self._reseal(plan)
        self._write_plan(plan)
        with self.assertRaisesRegex(TorrentAria2SelectorError, "empty"):
            build_aria2_selector_receipt(self.plan_path, self.torrent_path)

    def test_refuses_tampered_digest_torrent_or_selected_row(self) -> None:
        plan = self._plan()
        plan["statistics"]["selected_payload_bytes"] = 11
        self._write_plan(plan)
        with self.assertRaisesRegex(TorrentAria2SelectorError, "canonical SHA-256"):
            build_aria2_selector_receipt(self.plan_path, self.torrent_path)

        self._write_plan(self._plan())
        altered = bytearray(self.torrent_path.read_bytes())
        altered[-1] = ord("f")
        other_torrent = self.root / "other.torrent"
        other_torrent.write_bytes(altered)
        with self.assertRaises(TorrentAria2SelectorError):
            build_aria2_selector_receipt(self.plan_path, other_torrent)

        plan = self._plan()
        plan["selected_files"][0]["manifest_path"] = "B/different.mp4"
        self._reseal(plan)
        self._write_plan(plan)
        with self.assertRaisesRegex(TorrentAria2SelectorError, "path differs"):
            build_aria2_selector_receipt(self.plan_path, self.torrent_path)

    @staticmethod
    def _reseal(plan: dict) -> None:
        core = {
            key: value for key, value in plan.items() if key not in {"plan_id", "plan_sha256"}
        }
        digest = hashlib.sha256(canonical_json(core).encode("utf-8")).hexdigest()
        plan["plan_id"] = f"tslp_{digest[:32]}"
        plan["plan_sha256"] = digest


if __name__ == "__main__":
    unittest.main()
