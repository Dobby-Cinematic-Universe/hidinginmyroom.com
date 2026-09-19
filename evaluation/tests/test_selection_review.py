from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator, FormatChecker

from evaluation import cli
from evaluation.selection_review import (
    compile_interval_freeze,
    emit_selection_review_template,
    validate_compiled_interval_freeze,
    validate_interval_selection_review,
)
from evaluation.validation import (
    ContractError,
    _expected_rendition_id,
    _stable_id,
    audit_tracked_evaluation_data,
    canonical_manifest_sha256,
    load_json,
    validate_adjudication,
    validate_annotation,
    validate_interval_freeze,
)
from evaluation.tests.test_validation import make_adjudication, make_annotation


ROOT = Path(__file__).resolve().parents[2]
COHORT_PATH = ROOT / "evaluation/cohorts/himr-asr-candidate-cohort-v1.json"


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def seal(value: dict) -> dict:
    value["manifest_sha256"] = canonical_manifest_sha256(value)
    return value


class SelectionFixture:
    def __init__(self, root: Path):
        self.root = root
        self.cohort = load_json(COHORT_PATH)
        self.catalog = root / "catalog.sqlite3"
        self.catalog.write_bytes(b"catalog-must-remain-unchanged")
        self.catalog_before = self.catalog.read_bytes()
        self.requests = [
            {
                "schema_version": 1,
                "request_id": "proposal_request_v1_fixture",
                "manifest_sha256": digest("request-v1"),
            },
            {
                "schema_version": 2,
                "request_id": "proposal_request_v2_fixture",
                "manifest_sha256": digest("request-v2"),
            },
        ]
        # The v1 proposal deliberately spans disjoint cohort ranges. This catches
        # accidental bundle-order output (0-4, 7-11, 5-6).
        self.proposals = [
            self._proposal(1, [*range(0, 5), *range(7, 12)], 99),
            self._proposal(2, [5, 6], 24),
        ]

    def _proposal(self, version: int, positions: list[int], target_count: int) -> dict:
        proposal_id = f"proposal_v{version}_fixture"
        base_count, extra = divmod(target_count, len(positions))
        recordings = []
        for local_index, cohort_index in enumerate(positions):
            candidate = self.cohort["candidates"][cohort_index]
            count = base_count + (1 if local_index < extra else 0)
            media_sha = digest(f"parent-media-{cohort_index}")
            media_id = f"media_sha256_{media_sha}"
            rendition_kind = "acquired_source_media"
            rendition_id = _expected_rendition_id(
                candidate["recording_id"], media_id, rendition_kind
            )
            intervals = []
            for interval_index in range(count):
                start = 10_000 + interval_index * 30_000
                end = start + 30_000
                interval = {
                    "interval_id": _stable_id(
                        "proposal_interval", version, cohort_index, interval_index
                    ),
                    "start_ms": start,
                    "end_ms": end,
                }
                if version == 2:
                    interval.update(parent_start_ms=start, parent_end_ms=end)
                intervals.append(interval)
            recording = {
                "candidate_id": candidate["candidate_id"],
                "recording_id": candidate["recording_id"],
                "source_id": candidate["source_id"],
                "source_native_id": candidate["native_id"],
                "parent_media_id": media_id,
                "parent_media_sha256": media_sha,
                "parent_media_byte_count": 100_000 + cohort_index,
                "parent_media_duration_ms": 500_000,
                "intervals": intervals,
            }
            if version == 1:
                recording.update(
                    rendition_id=rendition_id,
                    rendition_kind=rendition_kind,
                )
            else:
                recording.update(
                    parent_rendition_id=rendition_id,
                    parent_rendition_kind=rendition_kind,
                    analysis_media_id=f"media_sha256_{digest(f'local-{cohort_index}')}",
                    analysis_media_sha256=digest(f"local-{cohort_index}"),
                    analysis_media_byte_count=50_000 + cohort_index,
                    analysis_media_duration_ms=400_000,
                    analysis_rendition_id=_expected_rendition_id(
                        candidate["recording_id"],
                        f"media_sha256_{digest(f'local-{cohort_index}')}",
                        "admitted_local_window",
                    ),
                    analysis_rendition_kind="admitted_local_window",
                )
            recordings.append(recording)
        return {
            "schema_version": version,
            "manifest_kind": "interval_proposal",
            "manifest_sha256": digest(f"proposal-v{version}"),
            "proposal_id": proposal_id,
            "created_at": "2026-08-26T20:00:00Z",
            "recordings": recordings,
        }

    def template(self) -> dict:
        return emit_selection_review_template(
            self.cohort,
            self.requests,
            self.proposals,
            self.catalog,
            "2026-08-26T21:00:00Z",
        )

    def completed(self, *, rejected: int = 3) -> dict:
        review = self.template()
        review["review_state"] = "completed_private"
        review["reviewer"] = {
            "reviewer_id": "reviewer_fixture",
            "review_tool": {"name": "private_media_reviewer", "version": "1.0.0"},
            "reviewed_at": "2026-08-26T22:00:00Z",
            "attested_at": "2026-08-26T22:05:00Z",
            "direct_parent_media_reviewed": True,
            "asr_outputs_inspected": False,
            "reference_text_inspected": False,
            "selection_basis": "source_metadata_and_direct_parent_media_only",
        }
        rejected_so_far = 0
        for recording_index, recording in enumerate(review["recordings"]):
            recording["split"] = "calibration" if recording_index < 3 else "scoring"
            for decision in recording["intervals"]:
                if rejected_so_far < rejected:
                    decision["decision"] = "exclude"
                    decision["rejection_reason"] = "technical_quality"
                    rejected_so_far += 1
                else:
                    decision["decision"] = "include"
                    decision["accepted_start_ms"] = decision["proposal_start_ms"]
                    decision["accepted_end_ms"] = decision["proposal_end_ms"]
                    decision["flags"] = {
                        "language_tags": ["en"],
                        "code_switch": False,
                        "speaker_overlap": False,
                        "playback_speech": False,
                        "noise": "clean",
                    }
        return seal(review)

    def assert_catalog_unchanged(self, case: unittest.TestCase) -> None:
        case.assertEqual(self.catalog.read_bytes(), self.catalog_before)


class SelectionReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.fixture = SelectionFixture(self.root)
        self.validator_patch = patch(
            "evaluation.selection_review.validate_interval_proposal",
            side_effect=lambda proposal, request, cohort, catalog: proposal,
        )
        self.validator_patch.start()

    def tearDown(self) -> None:
        self.validator_patch.stop()
        self.temporary.cleanup()

    def test_v1_v2_template_is_global_cohort_order_and_query_only(self) -> None:
        template = self.fixture.template()
        self.assertEqual(template["review_state"], "incomplete_template")
        self.assertEqual(template["proposal_accounting"]["interval_count"], 123)
        self.assertEqual(template["proposal_accounting"]["total_duration_ms"], 3_690_000)
        self.assertEqual(
            [row["candidate_id"] for row in template["recordings"]],
            [row["candidate_id"] for row in self.fixture.cohort["candidates"]],
        )
        self.assertEqual([row["ordinal"] for row in template["proposal_inputs"]], [1, 2])
        validate_interval_selection_review(
            template,
            self.fixture.cohort,
            self.fixture.requests,
            self.fixture.proposals,
            self.fixture.catalog,
        )
        self.fixture.assert_catalog_unchanged(self)
        tampered = copy.deepcopy(template)
        tampered["unexpected"] = True
        seal(tampered)
        with self.assertRaisesRegex(ContractError, "unexpected"):
            validate_interval_selection_review(
                tampered,
                self.fixture.cohort,
                self.fixture.requests,
                self.fixture.proposals,
                self.fixture.catalog,
            )

    def test_three_rejects_compile_exact_hour_with_parent_lineage(self) -> None:
        review = self.fixture.completed()
        freeze = compile_interval_freeze(
            review,
            self.fixture.cohort,
            self.fixture.requests,
            self.fixture.proposals,
            self.fixture.catalog,
            "2026-08-26T22:10:00Z",
        )
        replay = compile_interval_freeze(
            review,
            self.fixture.cohort,
            self.fixture.requests,
            self.fixture.proposals,
            self.fixture.catalog,
            "2026-08-26T22:10:00Z",
        )
        self.assertEqual(freeze, replay)
        self.assertEqual(freeze["created_at"], freeze["frozen_at"])
        self.assertEqual(freeze["accounting"]["interval_count"], 120)
        self.assertEqual(freeze["accounting"]["total_duration_ms"], 3_600_000)
        self.assertEqual(len(freeze["selection_provenance"]["proposal_inputs"]), 2)
        review_schema = load_json(
            ROOT / "evaluation/schemas/interval-selection-review.schema.json"
        )
        freeze_schema = load_json(ROOT / "evaluation/schemas/interval-freeze-v2.schema.json")
        Draft202012Validator(
            review_schema, format_checker=FormatChecker()
        ).validate(review)
        Draft202012Validator(
            freeze_schema, format_checker=FormatChecker()
        ).validate(freeze)
        for index in (5, 6):
            frozen = freeze["recordings"][index]
            proposed = self.fixture.proposals[1]["recordings"][index - 5]
            self.assertEqual(frozen["media_id"], proposed["parent_media_id"])
            self.assertEqual(frozen["rendition_id"], proposed["parent_rendition_id"])
            self.assertNotEqual(frozen["media_id"], proposed["analysis_media_id"])
        validate_compiled_interval_freeze(
            freeze,
            review,
            self.fixture.cohort,
            self.fixture.requests,
            self.fixture.proposals,
            self.fixture.catalog,
        )
        self.fixture.assert_catalog_unchanged(self)

    def test_duration_floor_is_identical_in_runtime_and_both_schemas(self) -> None:
        from evaluation.selection_review import MINIMUM_ACCEPTED_DURATION_MS
        from evaluation.validation import MINIMUM_SELECTION_DURATION_MS

        review_schema = load_json(
            ROOT / "evaluation/schemas/interval-selection-review.schema.json"
        )
        freeze_schema = load_json(ROOT / "evaluation/schemas/interval-freeze-v2.schema.json")
        self.assertEqual(MINIMUM_ACCEPTED_DURATION_MS, MINIMUM_SELECTION_DURATION_MS)
        self.assertEqual(
            review_schema["$defs"]["protocol"]["properties"][
                "minimum_accepted_duration_ms"
            ]["const"],
            MINIMUM_SELECTION_DURATION_MS,
        )
        self.assertEqual(
            freeze_schema["$defs"]["accounting"]["properties"][
                "total_duration_ms"
            ]["minimum"],
            MINIMUM_SELECTION_DURATION_MS,
        )

    def test_trim_only_requires_reason_and_never_expands(self) -> None:
        review = self.fixture.completed(rejected=2)
        decision = next(
            decision
            for recording in review["recordings"]
            for decision in recording["intervals"]
            if decision["decision"] == "include"
        )
        decision["accepted_start_ms"] += 100
        decision["adjustment_reason"] = "speech_boundary_refinement"
        seal(review)
        validate_interval_selection_review(
            review,
            self.fixture.cohort,
            self.fixture.requests,
            self.fixture.proposals,
            self.fixture.catalog,
            require_completed=True,
        )
        for mutation, message in (
            ({"adjustment_reason": None}, "adjustment_reason"),
            (
                {
                    "accepted_start_ms": decision["proposal_start_ms"] - 1,
                    "adjustment_reason": "speech_boundary_refinement",
                },
                "never shift or expand",
            ),
        ):
            bad = copy.deepcopy(review)
            target = next(
                item
                for recording in bad["recordings"]
                for item in recording["intervals"]
                if item["selection_decision_id"] == decision["selection_decision_id"]
            )
            target.update(mutation)
            seal(bad)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ContractError, message):
                validate_interval_selection_review(
                    bad,
                    self.fixture.cohort,
                    self.fixture.requests,
                    self.fixture.proposals,
                    self.fixture.catalog,
                    require_completed=True,
                )

    def test_review_rejects_attestation_coverage_split_and_shortfall_tampering(self) -> None:
        base = self.fixture.completed()

        def asr(review: dict) -> None:
            review["reviewer"]["asr_outputs_inspected"] = True

        def reference(review: dict) -> None:
            review["reviewer"]["reference_text_inspected"] = True

        def direct(review: dict) -> None:
            review["reviewer"]["direct_parent_media_reviewed"] = False

        def coverage(review: dict) -> None:
            review["recordings"][0]["intervals"].pop()

        def split(review: dict) -> None:
            for recording in review["recordings"]:
                recording["split"] = "scoring"

        def short(review: dict) -> None:
            decision = next(
                item
                for recording in review["recordings"]
                for item in recording["intervals"]
                if item["decision"] == "include"
            )
            decision.update(
                decision="exclude",
                accepted_start_ms=None,
                accepted_end_ms=None,
                adjustment_reason=None,
                rejection_reason="technical_quality",
                flags={key: None for key in decision["flags"]},
            )

        for name, mutation, message in (
            ("asr", asr, "asr_outputs_inspected"),
            ("reference", reference, "reference_text_inspected"),
            ("direct", direct, "direct_parent_media_reviewed"),
            ("coverage", coverage, "decide every proposed interval"),
            ("split", split, "calibration"),
            ("short", short, "shortfall 30000 ms"),
        ):
            bad = copy.deepcopy(base)
            mutation(bad)
            seal(bad)
            with self.subTest(name=name), self.assertRaisesRegex(ContractError, message):
                validate_interval_selection_review(
                    bad,
                    self.fixture.cohort,
                    self.fixture.requests,
                    self.fixture.proposals,
                    self.fixture.catalog,
                    require_completed=True,
                )

    def test_runtime_floor_and_cross_input_validator_reject_structural_forgeries(self) -> None:
        review = self.fixture.completed()
        freeze = compile_interval_freeze(
            review,
            self.fixture.cohort,
            self.fixture.requests,
            self.fixture.proposals,
            self.fixture.catalog,
            "2026-08-26T22:10:00Z",
        )
        malformed_review_id = copy.deepcopy(freeze)
        malformed_review_id["selection_provenance"]["review_id"] = "review_plan_fixture"
        seal(malformed_review_id)
        with self.assertRaisesRegex(ContractError, "selection_provenance.review_id.*invalid format"):
            validate_interval_freeze(malformed_review_id, self.fixture.cohort)
        freeze_schema = load_json(ROOT / "evaluation/schemas/interval-freeze-v2.schema.json")
        self.assertTrue(
            list(Draft202012Validator(freeze_schema).iter_errors(malformed_review_id)),
            "the Draft 2020-12 schema must reject the same malformed review ID",
        )

        mismatched_compile_time = copy.deepcopy(freeze)
        mismatched_compile_time["created_at"] = "2026-08-26T22:09:59Z"
        seal(mismatched_compile_time)
        with self.assertRaisesRegex(ContractError, "must equal frozen_at"):
            validate_interval_freeze(mismatched_compile_time, self.fixture.cohort)

        forged_lineage = copy.deepcopy(freeze)
        forged_lineage["selection_provenance"]["proposal_inputs"][0][
            "proposal_manifest_sha256"
        ] = digest("unrelated-proposal")
        seal(forged_lineage)
        validate_interval_freeze(forged_lineage, self.fixture.cohort)
        with self.assertRaisesRegex(ContractError, "deterministic compilation"):
            validate_compiled_interval_freeze(
                forged_lineage,
                review,
                self.fixture.cohort,
                self.fixture.requests,
                self.fixture.proposals,
                self.fixture.catalog,
            )

        forged_media = copy.deepcopy(freeze)
        recording = forged_media["recordings"][0]
        media_sha = digest("unrelated-parent-media")
        recording["media_sha256"] = media_sha
        recording["media_id"] = f"media_sha256_{media_sha}"
        recording["rendition_id"] = _expected_rendition_id(
            recording["recording_id"], recording["media_id"], recording["rendition_kind"]
        )
        seal(forged_media)
        validate_interval_freeze(forged_media, self.fixture.cohort)
        with self.assertRaisesRegex(ContractError, "deterministic compilation"):
            validate_compiled_interval_freeze(
                forged_media,
                review,
                self.fixture.cohort,
                self.fixture.requests,
                self.fixture.proposals,
                self.fixture.catalog,
            )

        short = copy.deepcopy(freeze)
        interval = short["recordings"][0]["intervals"][0]
        interval["end_ms"] -= 1
        interval["interval_id"] = _stable_id(
            "interval",
            short["selection_provenance"]["review_manifest_sha256"],
            interval["proposal_id"],
            interval["proposal_interval_id"],
            interval["start_ms"],
            interval["end_ms"],
        )
        short["accounting"]["total_duration_ms"] -= 1
        short["accounting"]["splits"][0]["duration_ms"] -= 1
        short["accounting"]["strata"][0]["duration_ms"] -= 1
        seal(short)
        with self.assertRaisesRegex(ContractError, "shortfall 1 ms"):
            validate_interval_freeze(short, self.fixture.cohort)

    def test_compiled_freeze_v2_validates_downstream_annotation_and_adjudication(self) -> None:
        review = self.fixture.completed()
        freeze = compile_interval_freeze(
            review,
            self.fixture.cohort,
            self.fixture.requests,
            self.fixture.proposals,
            self.fixture.catalog,
            "2026-08-26T22:10:00Z",
        )
        pass_a = make_annotation(freeze, "pass_a", "reviewer_reference_01")
        pass_b = make_annotation(freeze, "pass_b", "reviewer_reference_02")
        for annotation in (pass_a, pass_b):
            annotation["created_at"] = "2026-08-26T23:00:00Z"
            annotation["independence_attestation"]["attested_at"] = "2026-08-26T23:00:00Z"
            seal(annotation)
            self.assertIs(
                validate_annotation(annotation, freeze, self.fixture.cohort),
                annotation,
            )
        adjudication = make_adjudication(freeze, pass_a, pass_b)
        adjudication["created_at"] = "2026-08-27T00:00:00Z"
        adjudication["adjudicator"]["adjudicated_at"] = "2026-08-27T00:00:00Z"
        seal(adjudication)
        self.assertIs(
            validate_adjudication(
                adjudication, freeze, self.fixture.cohort, pass_a, pass_b
            ),
            adjudication,
        )

    def test_cli_completed_only_cross_validates_and_has_no_output_path(self) -> None:
        template = self.fixture.template()
        review = self.fixture.completed()
        freeze = compile_interval_freeze(
            review,
            self.fixture.cohort,
            self.fixture.requests,
            self.fixture.proposals,
            self.fixture.catalog,
            "2026-08-26T22:10:00Z",
        )
        paths = {}
        for name, value in (
            ("cohort", self.fixture.cohort),
            ("request1", self.fixture.requests[0]),
            ("request2", self.fixture.requests[1]),
            ("proposal1", self.fixture.proposals[0]),
            ("proposal2", self.fixture.proposals[1]),
            ("template", template),
            ("review", review),
            ("freeze", freeze),
        ):
            path = self.root / f"{name}.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            paths[name] = path
        common = [
            "--cohort", str(paths["cohort"]), "--catalog", str(self.fixture.catalog),
            "--request", str(paths["request1"]), "--request", str(paths["request2"]),
            "--proposal", str(paths["proposal1"]), "--proposal", str(paths["proposal2"]),
        ]
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = cli.main(["validate-selection-review", *common, str(paths["template"])])
        self.assertEqual(status, 1)
        self.assertIn("completed_private", err.getvalue())

        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = cli.main(["validate-selection-review", *common, str(paths["review"])])
        self.assertEqual(status, 0)
        self.assertTrue(json.loads(out.getvalue())["freeze_ready"])

        forged = copy.deepcopy(freeze)
        forged["selection_provenance"]["proposal_inputs"][0]["request_manifest_sha256"] = digest("forged")
        seal(forged)
        paths["freeze"].write_text(json.dumps(forged), encoding="utf-8")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = cli.main([
                "validate-freeze", "--cohort", str(paths["cohort"]),
                "--catalog", str(self.fixture.catalog), "--review", str(paths["review"]),
                "--request", str(paths["request1"]), "--request", str(paths["request2"]),
                "--proposal", str(paths["proposal1"]), "--proposal", str(paths["proposal2"]),
                str(paths["freeze"]),
            ])
        self.assertEqual(status, 1)
        self.assertIn("deterministic compilation", err.getvalue())
        for command in (
            "emit-selection-review-template",
            "validate-selection-review",
            "compile-freeze",
        ):
            self.assertNotIn(
                "output",
                {action.dest for action in cli.build_parser()._subparsers._group_actions[0].choices[command]._actions},
            )

    def test_audit_rejects_tracked_review_even_when_privacy_is_missing(self) -> None:
        repository = self.root / "repository"
        repository.mkdir()
        subprocess.run(["git", "init", "-q", str(repository)], check=True)
        review_path = repository / "review.json"
        review_path.write_text(
            json.dumps({"manifest_kind": "interval_selection_review"}),
            encoding="utf-8",
        )
        subprocess.run(["git", "-C", str(repository), "add", "review.json"], check=True)
        with self.assertRaisesRegex(ContractError, "Tracked private selection review"):
            audit_tracked_evaluation_data(repository)


class LiveSelectionReviewSmokeTests(unittest.TestCase):
    """Read-only smoke over the bound private proposal snapshot when it is present."""

    def test_real_validators_merge_bound_v1_v2_proposals_without_catalog_writes(self) -> None:
        catalog = (
            ROOT
            / "research/evaluation/selection_review_a8aed2107b9f599696ec228599e6c880"
            / "workspace/catalog-snapshot.sqlite3"
        )
        directories = [
            ROOT / "research/corpus/evaluation/interval-proposals/interval_proposal_38b6d7225a1753a29d603d4bf12bde5f",
            ROOT / "research/corpus/evaluation/interval-proposals/interval_proposal_e8e12a2c1a1c584cbd7182aa06b2924a",
        ]
        paths = [
            catalog,
            *[
                directory / name
                for directory in directories
                for name in ("request.json", "proposal.json")
            ],
        ]
        if not all(path.is_file() for path in paths):
            self.skipTest("private current proposal inputs are not present")

        def file_sha256(path: Path) -> str:
            checksum = hashlib.sha256()
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    checksum.update(block)
            return checksum.hexdigest()

        before = file_sha256(catalog)
        self.assertEqual(
            before,
            "92adef66ff674a4f42c8a4a952dd467deaab296aeb84d8acdd730d7c0dccf71e",
        )
        requests = [load_json(directory / "request.json") for directory in directories]
        proposals = [load_json(directory / "proposal.json") for directory in directories]
        template = emit_selection_review_template(
            load_json(COHORT_PATH),
            requests,
            proposals,
            catalog,
            "2026-08-27T04:00:00Z",
        )
        self.assertEqual(template["review_state"], "incomplete_template")
        self.assertEqual(template["proposal_accounting"]["recording_count"], 12)
        self.assertEqual(template["proposal_accounting"]["interval_count"], 123)
        self.assertEqual(template["proposal_accounting"]["total_duration_ms"], 3_690_000)
        self.assertEqual(file_sha256(catalog), before)


if __name__ == "__main__":
    unittest.main()
