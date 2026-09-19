from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
PROGRAM = ACQUISITION_ROOT / "torrent_completed_handoff.py"
SCHEMA = ACQUISITION_ROOT / "schemas" / "torrent-completed-handoff.schema.json"
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
TEST_ROOT = ACQUISITION_ROOT / ".test-work"

sys.path.insert(0, str(REPOSITORY_ROOT / "corpus" / "src"))

from himr_corpus.importers import canonical_json  # noqa: E402
from himr_corpus.torrent_acquisition_audit import ARIA2_BLOCK_BYTES  # noqa: E402
from himr_corpus.torrent_aria2_selector import (  # noqa: E402
    build_aria2_selector_receipt,
)
from himr_corpus.torrent_bracket_reconciler import (  # noqa: E402
    _parse_torrent,
    _raw_path_sha256,
)
from himr_corpus.torrent_selective_planner import (  # noqa: E402
    PLAN_KIND,
    PLANNER_VERSION,
)

from acquisition import torrent_completed_handoff as handoff  # noqa: E402


def bencode(value: object) -> bytes:
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


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


class TorrentCompletedHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="torrent-completed-handoff-", dir=TEST_ROOT
        )
        self.root = Path(self.temporary.name).resolve()
        self.generated_media = self.root / "generated.mp4"
        generated = run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=s=160x120:r=15:d=0.35",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=16000:duration=0.35",
                "-map",
                "0:v:0",
                "-map",
                "1:a:0",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-shortest",
                str(self.generated_media),
            ]
        )
        if generated.returncode != 0:
            self.fail(generated.stderr)

        self.paths = [
            [b"A", b"download.bat"],
            [b"B", b"selected.mp4"],
            [b"C", b"tail.mp4"],
        ]
        self.lengths = [5, self.generated_media.stat().st_size, 7]
        self.selected_index = 1
        self.piece_length = 16 * 1024
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
        self.plan_path.write_bytes(handoff.canonical_bytes(self._plan()))
        self.selector = build_aria2_selector_receipt(
            self.plan_path, self.torrent_path
        )
        self.selector_path = self.root / "selector.json"
        self.selector_path.write_bytes(handoff.canonical_bytes(self.selector))

        self.payload_root = self.root / "payload"
        self.torrent_root = self.payload_root / "fixture"
        for directory in ("A", "B", "C"):
            (self.torrent_root / directory).mkdir(parents=True, exist_ok=True)
        (self.torrent_root / "A" / "download.bat").write_bytes(b"12345")
        shutil.copyfile(self.generated_media, self.torrent_root / "B" / "selected.mp4")
        (self.torrent_root / "C" / "tail.mp4").write_bytes(b"1234567")

        self.control_path = self.payload_root / "fixture.aria2"
        self.target_pieces = self._target_pieces(self.selected_index)
        self.control_path.write_bytes(self._control(completed=self.target_pieces))
        self.session_path = self.root / "aria2.session"
        self._write_session()
        self.ffprobe = str(Path(shutil.which("ffprobe") or "").resolve())

    def tearDown(self) -> None:
        for child in sorted(self.root.rglob("*"), reverse=True):
            try:
                child.chmod(0o700 if child.is_dir() else 0o600)
            except OSError:
                pass
        self.temporary.cleanup()

    def _plan(self) -> dict:
        manifest = self.torrent["files"][self.selected_index]
        selected = {
            "youtube_video_id": "FixtureID01",
            "torrent_file_index": self.selected_index,
            "byte_count": manifest["byte_count"],
            "manifest_path": manifest["manifest_path"],
            "manifest_path_sha256": _raw_path_sha256(manifest["raw_components"]),
            "availability_evidence_code": "removed",
            "selection_reason": (
                "smallest_rendition_after_exact_archive_catalog_exclusion_and_no_"
                "download_youtube_unavailability_probe"
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
                    "smallest_rendition_file_index": self.selected_index,
                    "availability_state": "unavailable",
                    "availability_evidence_code": "removed",
                }
            ],
            "malformed_manual_review": [],
            "selected_files": [selected],
            "selected_torrent_file_indices": [self.selected_index],
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
        options = {
            "gid": "0123456789abcdef",
            "dir": str(self.payload_root),
            "file-allocation": "none",
            "allow-overwrite": "false",
            "check-integrity": "true",
            "continue": "true",
            "auto-file-renaming": "false",
            "select-file": selector
            or self.selector["selection"]["aria2_select_file_value"],
            "seed-time": "0",
            "bt-enable-lpd": "false",
            "bt-remove-unselected-file": "false",
        }
        body = str(self.torrent_path) + "\n" + "".join(
            f" {key}={value}\n" for key, value in options.items()
        )
        self.session_path.write_text(body, encoding="utf-8")

    def _target_pieces(self, index: int) -> list[int]:
        start = sum(self.lengths[:index])
        end = start + self.lengths[index]
        return list(range(start // self.piece_length, (end - 1) // self.piece_length + 1))

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
            ">IQQI", self.piece_length, self.torrent["total_bytes"], uploaded, len(bitmap)
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

    def build(self, **overrides: object) -> dict:
        manifest = self.torrent["files"][self.selected_index]
        arguments = {
            "plan_path": self.plan_path,
            "selector_receipt_path": self.selector_path,
            "torrent_path": self.torrent_path,
            "session_path": self.session_path,
            "control_path": self.control_path,
            "payload_root": self.payload_root,
            "observed_at": "2026-08-28T00:45:00Z",
            "expected_file_index": self.selected_index,
            "expected_manifest_path": manifest["manifest_path"],
            "expected_declared_byte_count": manifest["byte_count"],
            "ffprobe_path": self.ffprobe,
        }
        arguments.update(overrides)
        return handoff.build_torrent_completed_handoff(**arguments)

    def test_emits_bound_schema_valid_non_admitting_work_order(self) -> None:
        work_order = self.build()
        selected_path = self.torrent_root / "B" / "selected.mp4"
        expected_sha256 = hashlib.sha256(selected_path.read_bytes()).hexdigest()
        self.assertEqual(work_order["media"]["sha256"], expected_sha256)
        self.assertEqual(work_order["media"]["media_kind"], "video")
        self.assertTrue(work_order["verification"]["control_stable_double_read"])
        self.assertFalse(work_order["verification"]["target_is_boundary_artifact"])
        self.assertEqual(
            work_order["selected_file"]["source_identity"],
            {
                "platform": "bittorrent",
                "source_kind": "torrent_file_candidate",
                "native_id": (
                    f'{self.torrent["info_hash_sha1"]}/B/selected.mp4'
                ),
            },
        )
        self.assertFalse(work_order["catalogue_handoff"]["media_admitted"])
        self.assertEqual(work_order["policy"]["unselected_payload_files_opened"], 0)
        core = {
            key: value
            for key, value in work_order.items()
            if key not in {"work_order_id", "work_order_sha256"}
        }
        digest = hashlib.sha256(handoff.canonical_bytes(core)).hexdigest()
        self.assertEqual(work_order["work_order_sha256"], digest)
        self.assertEqual(work_order["work_order_id"], f"tch_{digest[:32]}")

        instance = self.root / "work-order.json"
        instance.write_bytes(handoff.canonical_bytes(work_order) + b"\n")
        checked = run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(SCHEMA),
                str(instance),
            ]
        )
        self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)

    def test_refuses_boundary_target_before_hash_or_probe(self) -> None:
        boundary = self.torrent["files"][0]
        with mock.patch.object(
            handoff, "_hash_pinned", side_effect=AssertionError("must not hash boundary")
        ), mock.patch.object(
            handoff, "_probe_pinned", side_effect=AssertionError("must not probe boundary")
        ), self.assertRaisesRegex(
            handoff.TorrentCompletedHandoffError, "unselected or only a boundary"
        ):
            self.build(
                expected_file_index=0,
                expected_manifest_path=boundary["manifest_path"],
                expected_declared_byte_count=boundary["byte_count"],
            )

    def test_refuses_incomplete_covering_piece_before_hash_or_probe(self) -> None:
        self.control_path.write_bytes(self._control(completed=self.target_pieces[:-1]))
        with mock.patch.object(
            handoff, "_hash_pinned", side_effect=AssertionError("must not hash incomplete")
        ), mock.patch.object(
            handoff, "_probe_pinned", side_effect=AssertionError("must not probe incomplete")
        ), self.assertRaisesRegex(
            handoff.TorrentCompletedHandoffError, "not every torrent piece"
        ):
            self.build()

    def test_refuses_exact_path_size_and_selector_mismatches(self) -> None:
        with self.assertRaisesRegex(
            handoff.TorrentCompletedHandoffError, "manifest path differs"
        ):
            self.build(expected_manifest_path="B/not-selected.mp4")
        with self.assertRaisesRegex(
            handoff.TorrentCompletedHandoffError, "declared byte count differs"
        ):
            self.build(expected_declared_byte_count=self.lengths[1] - 1)

        self._write_session(selector="1")
        with self.assertRaisesRegex(
            handoff.TorrentCompletedHandoffError, "session selector differs"
        ):
            self.build()

    def test_refuses_control_change_across_payload_read_boundary(self) -> None:
        original_probe = handoff._probe_pinned

        def mutate_after_probe(*args: object, **kwargs: object):
            result = original_probe(*args, **kwargs)
            self.control_path.write_bytes(
                self._control(completed=self.target_pieces, uploaded=1)
            )
            return result

        with mock.patch.object(
            handoff, "_probe_pinned", side_effect=mutate_after_probe
        ), self.assertRaisesRegex(
            handoff.TorrentCompletedHandoffError,
            "snapshot changed|control state changed",
        ):
            self.build()

    def test_refuses_hardlinked_or_replaced_selected_file(self) -> None:
        selected = self.torrent_root / "B" / "selected.mp4"
        hardlink = self.root / "selected-hardlink.mp4"
        os.link(selected, hardlink)
        with self.assertRaisesRegex(
            handoff.TorrentCompletedHandoffError, "hard-linked"
        ):
            self.build()
        hardlink.unlink()

        selected.unlink()
        selected.symlink_to(self.generated_media)
        with self.assertRaisesRegex(handoff.TorrentCompletedHandoffError, "symlink"):
            self.build()

    def test_private_writer_is_canonical_immutable_and_outside_payload(self) -> None:
        work_order = self.build()
        output = self.root / "handoff.json"
        summary = handoff.write_private_work_order(
            work_order, output, protected_payload_root=self.payload_root
        )
        self.assertTrue(summary["work_order_written"])
        self.assertEqual(os.stat(output).st_mode & 0o777, 0o400)
        self.assertEqual(output.read_bytes(), handoff.canonical_bytes(work_order) + b"\n")
        with self.assertRaisesRegex(
            handoff.TorrentCompletedHandoffError, "already exists"
        ):
            handoff.write_private_work_order(
                work_order, output, protected_payload_root=self.payload_root
            )
        with self.assertRaisesRegex(
            handoff.TorrentCompletedHandoffError, "outside the torrent payload"
        ):
            handoff.write_private_work_order(
                work_order,
                self.payload_root / "handoff.json",
                protected_payload_root=self.payload_root,
            )

    def test_cli_refusal_is_machine_readable_and_does_not_write(self) -> None:
        output = self.root / "must-not-exist.json"
        completed = run(
            [
                "python3",
                str(PROGRAM),
                "--plan",
                str(self.plan_path),
                "--selector-receipt",
                str(self.selector_path),
                "--torrent",
                str(self.torrent_path),
                "--session",
                str(self.session_path),
                "--control",
                str(self.control_path),
                "--payload-root",
                str(self.payload_root),
                "--observed-at",
                "2026-08-28T00:45:00Z",
                "--file-index",
                "0",
                "--manifest-path",
                "A/download.bat",
                "--declared-byte-count",
                "5",
                "--output",
                str(output),
            ]
        )
        self.assertEqual(completed.returncode, 2)
        error = json.loads(completed.stderr)
        self.assertEqual(
            error["error"]["code"], "torrent_completed_handoff_refused"
        )
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
