from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = (
    PROJECT_ROOT
    / "corpus"
    / "fixtures"
    / "archive-org-wiki-linked-files-2026-08-27.json"
)
EXPECTED_FIXTURE_SHA256 = (
    "70dfe4168daedcef5428e5c5532f59630f039655179ec4a00b44d105957fa42f"
)
EXPECTED_SOURCES = [
    {
        "identifier": "699994",
        "response_url": "https://archive.org/metadata/699994",
        "observed_at": "2026-08-27T09:07:07Z",
        "payload_sha256": (
            "f904e427ff130950ce301da9d3834a31810b64a21c206b265d4d76e6cf10c534"
        ),
    },
    {
        "identifier": "hidinginmyroom",
        "response_url": "https://archive.org/metadata/hidinginmyroom",
        "observed_at": "2026-08-27T09:07:10Z",
        "payload_sha256": (
            "1e50befbdae653064ab3971de9e94583fbd99fb25cd7c8189b51f7e26a712dac"
        ),
    },
    {
        "identifier": "hidinginmyroom3",
        "response_url": "https://archive.org/metadata/hidinginmyroom3",
        "observed_at": "2026-08-27T09:07:12Z",
        "payload_sha256": (
            "92324b53ce6d0904fb20d14de48f96941830cfa2319cdab7f24a5d13439f32b3"
        ),
    },
]


def _exact_keys(value: object, expected: set[str], context: str) -> dict:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(f"{context} has an unexpected shape")
    return value


def _validate_inventory(inventory: object) -> int:
    root = _exact_keys(
        inventory,
        {"schema_version", "inventory_id", "review_basis", "items"},
        "inventory",
    )
    if root["schema_version"] != 1:
        raise ValueError("schema_version mismatch")
    if root["inventory_id"] != "archive-org-wiki-linked-files-2026-08-27":
        raise ValueError("inventory_id mismatch")

    review = _exact_keys(
        root["review_basis"],
        {"snapshot_id", "observed_at", "snapshot_json_sha256", "sources"},
        "review_basis",
    )
    if review["snapshot_id"] != "iams_e12428c928e888d699b81a7670051131":
        raise ValueError("snapshot_id mismatch")
    if review["observed_at"] != "2026-08-27T09:07:12Z":
        raise ValueError("snapshot observed_at mismatch")
    if review["snapshot_json_sha256"] != (
        "1ea6f5d49c9a0aa5dc3dddc00f3d35bb1b6ab1ee0dcaba1060012b026536f01f"
    ):
        raise ValueError("snapshot.json digest mismatch")
    if review["sources"] != EXPECTED_SOURCES:
        raise ValueError("source-response binding mismatch")

    items = root["items"]
    if not isinstance(items, list) or len(items) != len(EXPECTED_SOURCES):
        raise ValueError("item count mismatch")

    file_count = 0
    identifiers: set[str] = set()
    for item_index, (item_value, source) in enumerate(zip(items, EXPECTED_SOURCES)):
        item = _exact_keys(item_value, {"identifier", "files"}, f"items[{item_index}]")
        identifier = item["identifier"]
        if identifier != source["identifier"]:
            raise ValueError("items are not in reviewed identifier order")
        if identifier in identifiers:
            raise ValueError("duplicate item identifier")
        identifiers.add(identifier)

        files = item["files"]
        if not isinstance(files, list) or not files:
            raise ValueError("item files must be a non-empty list")
        names: set[str] = set()
        previous_name: str | None = None
        for file_index, file_value in enumerate(files):
            file = _exact_keys(
                file_value,
                {"name", "size", "md5", "sha1"},
                f"items[{item_index}].files[{file_index}]",
            )
            name = file["name"]
            if not isinstance(name, str) or not name:
                raise ValueError("file name must be non-empty")
            if previous_name is not None and previous_name >= name:
                raise ValueError("files are not strictly name-sorted")
            previous_name = name
            if name in names:
                raise ValueError("duplicate file name")
            names.add(name)
            if not isinstance(file["size"], int) or file["size"] <= 0:
                raise ValueError("invalid provider size")
            if len(file["md5"]) != 32 or any(c not in "0123456789abcdef" for c in file["md5"]):
                raise ValueError("invalid provider MD5")
            if len(file["sha1"]) != 40 or any(c not in "0123456789abcdef" for c in file["sha1"]):
                raise ValueError("invalid provider SHA-1")
            file_count += 1

    if file_count != 44:
        raise ValueError("reviewed file count mismatch")
    return file_count


class WikiArchiveInventoryFixtureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = FIXTURE_PATH.read_bytes()
        self.inventory = json.loads(self.raw)

    def test_reviewed_inventory_is_integral_and_strict(self) -> None:
        self.assertEqual(hashlib.sha256(self.raw).hexdigest(), EXPECTED_FIXTURE_SHA256)
        self.assertEqual(_validate_inventory(self.inventory), 44)

    def test_duplicate_filename_tamper_is_rejected(self) -> None:
        tampered = copy.deepcopy(self.inventory)
        tampered["items"][0]["files"][1]["name"] = tampered["items"][0]["files"][0]["name"]
        with self.assertRaisesRegex(ValueError, "strictly name-sorted|duplicate file name"):
            _validate_inventory(tampered)

    def test_source_digest_tamper_is_rejected(self) -> None:
        tampered = copy.deepcopy(self.inventory)
        tampered["review_basis"]["sources"][0]["payload_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "source-response binding mismatch"):
            _validate_inventory(tampered)


if __name__ == "__main__":
    unittest.main()
