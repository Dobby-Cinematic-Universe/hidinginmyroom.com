from __future__ import annotations

import hashlib
import json
import math
import os
import struct
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.importers import canonical_json  # noqa: E402
from himr_corpus.torrent_acquisition_audit import (  # noqa: E402
    ARIA2_BLOCK_BYTES,
    TorrentAcquisitionAuditError,
    build_torrent_acquisition_audit,
    publish_private_torrent_acquisition_audit,
)
from himr_corpus.torrent_aria2_selector import (  # noqa: E402
    build_aria2_selector_receipt,
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


class TorrentAcquisitionAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="torrent-acquisition-audit-test-", dir=work
        )
        self.root = Path(self.temporary.name).resolve()
        self.paths = [
            [b"A", b"launch.bat"],
            [b"B", b"selected.mp4"],
            [b"C", b"tail.mp4"],
        ]
        self.lengths = [5, 10, 7]
        self.piece_length = 8
        total = sum(self.lengths)
        info = {
            b"files": [
                {b"length": length, b"path": path}
                for path, length in zip(self.paths, self.lengths, strict=True)
            ],
            b"name": b"fixture",
            b"piece length": self.piece_length,
            b"pieces": b"p" * (math.ceil(total / self.piece_length) * 20),
        }
        self.torrent_path = self.root / "fixture.torrent"
        self.torrent_path.write_bytes(bencode({b"info": info}))
        self.torrent = _parse_torrent(self.torrent_path.read_bytes())
        self.plan_path = self.root / "plan.json"
        self.plan_path.write_text(
            json.dumps(self._plan(), sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        self.selector = build_aria2_selector_receipt(self.plan_path, self.torrent_path)
        self.selector_path = self.root / "selector.json"
        self.selector_path.write_text(
            json.dumps(self.selector, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        self.payload_root = self.root / "payload"
        self.payload_root.mkdir()
        self.torrent_root = self.payload_root / "fixture"
        for directory in ("A", "B", "C"):
            (self.torrent_root / directory).mkdir(parents=True, exist_ok=True)
        (self.torrent_root / "A" / "launch.bat").write_bytes(b"12345")
        (self.torrent_root / "B" / "selected.mp4").write_bytes(b"0123456789")
        (self.torrent_root / "C" / "tail.mp4").write_bytes(b"x")
        self.control_path = self.payload_root / "fixture.aria2"
        self.control_path.write_bytes(self._control(completed=[0, 1]))
        self.session_path = self.root / "aria2.session"
        self._write_session()

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

    def _write_session(self, selector: str | None = None) -> None:
        selector = selector or self.selector["selection"]["aria2_select_file_value"]
        options = {
            "gid": "0123456789abcdef",
            "dir": str(self.payload_root),
            "file-allocation": "none",
            "allow-overwrite": "false",
            "check-integrity": "true",
            "continue": "true",
            "auto-file-renaming": "false",
            "select-file": selector,
            "seed-time": "0",
            "bt-enable-lpd": "false",
            "bt-remove-unselected-file": "false",
        }
        body = str(self.torrent_path) + "\n" + "".join(
            f" {key}={value}\n" for key, value in options.items()
        )
        self.session_path.write_text(body, encoding="utf-8")

    def _control(
        self,
        *,
        completed: list[int],
        in_flight: list[tuple[int, list[int]]] | None = None,
        uploaded: int = 0,
    ) -> bytes:
        piece_count = math.ceil(self.torrent["total_bytes"] / self.piece_length)
        bitmap = bytearray(math.ceil(piece_count / 8))
        for piece in completed:
            bitmap[piece // 8] |= 0x80 >> (piece % 8)
        body = bytearray()
        body += struct.pack(">HI", 1, 1)
        info_hash = bytes.fromhex(self.torrent["info_hash_sha1"])
        body += struct.pack(">I", len(info_hash)) + info_hash
        body += struct.pack(
            ">IQQI",
            self.piece_length,
            self.torrent["total_bytes"],
            uploaded,
            len(bitmap),
        )
        body += bitmap
        in_flight = in_flight or []
        body += struct.pack(">I", len(in_flight))
        for piece, present_blocks in in_flight:
            piece_bytes = min(
                self.piece_length,
                self.torrent["total_bytes"] - piece * self.piece_length,
            )
            block_count = math.ceil(piece_bytes / ARIA2_BLOCK_BYTES)
            blocks = bytearray(math.ceil(block_count / 8))
            for block in present_blocks:
                blocks[block // 8] |= 0x80 >> (block % 8)
            body += struct.pack(">III", piece, piece_bytes, len(blocks)) + blocks
        return bytes(body)

    def _audit(self):
        return build_torrent_acquisition_audit(
            plan_path=self.plan_path,
            selector_receipt_path=self.selector_path,
            torrent_path=self.torrent_path,
            session_path=self.session_path,
            control_path=self.control_path,
            payload_root=self.payload_root,
            observed_at="2026-08-27T20:00:00Z",
        )

    def test_builds_path_free_bound_receipt_and_flags_complete_boundary_script(self) -> None:
        receipt = self._audit()
        self.assertTrue(receipt["status"]["audit_passed"])
        self.assertEqual(receipt["selector"]["selected_file_count"], 1)
        self.assertEqual(receipt["selector"]["selected_piece_count"], 2)
        self.assertEqual(receipt["selector"]["selected_piece_span_bytes"], 16)
        self.assertEqual(receipt["control_state"]["completed"]["authorized_piece_count"], 2)
        self.assertEqual(receipt["control_state"]["completed"]["unauthorized_piece_count"], 0)
        self.assertEqual(receipt["inventory"]["created_manifest_file_count"], 3)
        self.assertEqual(receipt["inventory"]["boundary"]["eligible_file_count"], 2)
        self.assertEqual(
            receipt["inventory"]["fully_materialized_risky_unselected_artifact_count"],
            1,
        )
        artifact = receipt["risky_unselected_artifacts"][0]
        self.assertEqual(artifact["suffix"], ".bat")
        self.assertEqual(artifact["risk_class"], "script")
        self.assertTrue(artifact["fully_piece_covered"])
        self.assertTrue(artifact["fully_materialized"])
        self.assertEqual(receipt["policy"]["payload_bytes_read"], 0)
        self.assertNotIn(str(self.root), json.dumps(receipt, sort_keys=True))

    def test_records_unauthorized_completed_and_inflight_state_without_hiding_it(self) -> None:
        self.control_path.write_bytes(
            self._control(completed=[0, 2], in_flight=[(1, [0])], uploaded=9)
        )
        receipt = self._audit()
        state = receipt["control_state"]
        self.assertFalse(state["authorized_only"])
        self.assertEqual(state["completed"]["authorized_piece_count"], 1)
        self.assertEqual(state["completed"]["unauthorized_piece_count"], 1)
        self.assertEqual(state["completed"]["unauthorized_bytes"], 6)
        self.assertEqual(state["in_flight"]["authorized_piece_count"], 1)
        self.assertEqual(state["recorded_uploaded_bytes"], 9)
        self.assertFalse(receipt["status"]["audit_passed"])
        self.assertEqual(
            receipt["status"]["selector_scope_status"],
            "unauthorized_piece_state_observed",
        )

    def test_refuses_selector_mismatch_or_tampered_selector_receipt(self) -> None:
        self._write_session(selector="1")
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "session selector"):
            self._audit()

        self._write_session()
        tampered = json.loads(self.selector_path.read_text(encoding="utf-8"))
        tampered["selection"]["selected_file_count"] = 2
        self.selector_path.write_text(json.dumps(tampered), encoding="utf-8")
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "selector receipt differs"):
            self._audit()

    def test_refuses_unknown_files_symlinks_and_hardlinks(self) -> None:
        unknown_directory = self.torrent_root / "unknown-directory"
        unknown_directory.mkdir()
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "unknown directory"):
            self._audit()
        unknown_directory.rmdir()

        unknown = self.torrent_root / "unknown.bin"
        unknown.write_bytes(b"x")
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "unknown file"):
            self._audit()
        unknown.unlink()

        tail = self.torrent_root / "C" / "tail.mp4"
        tail.unlink()
        tail.symlink_to(self.torrent_root / "B" / "selected.mp4")
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "symlink"):
            self._audit()
        tail.unlink()

        os.link(self.torrent_root / "B" / "selected.mp4", tail)
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "hard-linked"):
            self._audit()

    def test_refuses_symlinked_roots_and_hardlinked_sealed_inputs(self) -> None:
        original_payload_root = self.payload_root
        alias = self.root / "payload-alias"
        alias.symlink_to(original_payload_root, target_is_directory=True)
        self.payload_root = alias
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "path contains a symlink"):
            self._audit()
        self.payload_root = original_payload_root

        hardlink = self.root / "selector-hardlink.json"
        os.link(self.selector_path, hardlink)
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "must not be hard-linked"):
            self._audit()

    def test_refuses_malformed_or_wrong_control_state(self) -> None:
        self.control_path.write_bytes(self._control(completed=[0, 1]) + b"x")
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "trailing bytes"):
            self._audit()

        wrong = bytearray(self._control(completed=[0, 1]))
        wrong[10] ^= 1
        self.control_path.write_bytes(wrong)
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "different torrent info hash"):
            self._audit()

    def test_payload_permissions_do_not_trigger_content_reads(self) -> None:
        for path in self.torrent_root.rglob("*"):
            if path.is_file():
                path.chmod(0)
        receipt = self._audit()
        self.assertTrue(receipt["status"]["metadata_snapshot_stable"])
        self.assertEqual(receipt["policy"]["payload_files_opened"], 0)

    def test_private_writer_is_atomic_read_only_and_outside_payload(self) -> None:
        receipt = self._audit()
        output = self.root / "audit.json"
        summary = publish_private_torrent_acquisition_audit(
            receipt, output, protected_payload_root=self.payload_root
        )
        self.assertTrue(summary["audit_receipt_written"])
        self.assertFalse(summary["path_disclosed"])
        self.assertEqual(os.stat(output).st_mode & 0o777, 0o400)
        self.assertEqual(json.loads(output.read_text(encoding="utf-8")), receipt)
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "already exists"):
            publish_private_torrent_acquisition_audit(
                receipt, output, protected_payload_root=self.payload_root
            )
        with self.assertRaisesRegex(TorrentAcquisitionAuditError, "outside the payload"):
            publish_private_torrent_acquisition_audit(
                receipt,
                self.payload_root / "audit.json",
                protected_payload_root=self.payload_root,
            )


if __name__ == "__main__":
    unittest.main()
