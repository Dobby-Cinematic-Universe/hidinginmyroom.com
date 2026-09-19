"""Compact request aliases preserve exact text and recover canonical evidence."""
from copy import deepcopy
import json
import unittest

from pipeline import transcript_summary_core as core
from pipeline.tests.test_transcript_summary_core import complete, output, source
from pipeline.tests.test_transcript_summary_synthesis import config, through_transcripts, topic


def raw_evidence(*, recording="one", texts=None, date=None, speaker=None, timed=True):
    selected = source(texts=texts, recording=recording, date=date, speaker=speaker, timed=timed)
    return selected, core.initial_jobs([selected])[0]["evidence"]


def scope(evidence, *, extra=(), period="2025-01"):
    return {"source_ids": sorted({citation["source_id"] for item in evidence
                                 for citation in item["citations"]} | set(extra)),
            "period": period, "level": 1, "index": 0, "final": True}


def hydrated(item, *, ordinal):
    result = deepcopy(item)
    result["evidence_id"] = "summaryitem_" + f"{ordinal:032x}"
    result["excerpts"] = [{"citation": deepcopy(citation), "text": item["text"],
                            "speaker": item["speaker"]} for citation in item["citations"]]
    result["text"] = "A supported prior summary."
    result["classification"] = "reported_statement"
    result["speaker"] = None
    return result


class CompactSummaryTests(unittest.TestCase):
    def test_all_provider_requests_use_compact_metadata_and_keep_full_local_citations(self):
        selected = source(["The park and library are discussed."], date="2025-01-12")
        for profile in core.PROFILES:
            with self.subTest(profile=profile):
                job = core.initial_jobs([selected], {**core.DEFAULT_CONFIG, "transcript_profile": profile})[0]
                data = job["prompt"]["input"]
                self.assertEqual(set(data), {"stage", "period", "sources", "evidence"})
                self.assertEqual(data["sources"], [{"source_id": "s1", "date": {
                    "value": "2025-01-12", "kind": "published"}}])
                self.assertEqual(data["evidence"], [{"evidence_id": "e1",
                    "text": "The park and library are discussed.", "source_ids": ["s1"]}])
                encoded = core.canonical(data)
                for value in (selected["source_id"], selected["transcript"]["sha256"],
                              job["evidence"][0]["evidence_id"], "/private/", "start_ms", "end_ms",
                              "char_start", "char_end", "ordinal", "source_ref", "scope"):
                    self.assertNotIn(value.encode(), encoded)
                citation = job["evidence"][0]["citations"][0]
                self.assertEqual(citation["source_id"], selected["source_id"])
                self.assertEqual(citation["transcript_sha256"], selected["transcript"]["sha256"])
                self.assertEqual((citation["start_ms"], citation["end_ms"]), (0, 1000))
                self.assertEqual((citation["char_start"], citation["char_end"]), (0, len(data["evidence"][0]["text"])))
                self.assertIn("source_ref", citation)
                body = job["request"]["body"]
                wire_input = (body["messages"][0]["content"] if profile == "anthropic_sonnet_batch" else
                              body["contents"][0]["parts"][0]["text"] if profile == "gemini_flash_batch" else body["input"])
                self.assertEqual(json.loads(wire_input), data)

    def test_raw_text_is_copied_exactly_even_when_it_looks_like_timestamps_or_ids(self):
        text = 'At 00:01:02 I read "start_ms": 1200; source s1 and e1.\n/private/note [4] 日本語🙂'
        selected, evidence = raw_evidence(texts=[text, "  Another line\nwith trailing space.  "])
        before = deepcopy(evidence)
        data = core.compact_input("chunk", {"period": None}, evidence)
        self.assertEqual([item["text"] for item in data["evidence"]],
                         [item["text"] for item in selected["segments"]])
        self.assertEqual(evidence, before)

    def test_source_aliases_include_only_used_sources_in_first_encounter_order(self):
        first, one = raw_evidence(recording="one", texts=["First passage.", "Another passage."], date="2025-01-02")
        second, two = raw_evidence(recording="two", date="2025-01-03")
        unused = source(recording="not-used", date="2025-01-01")
        evidence = [two[0], *one]
        data = core.compact_input("timeline", scope(evidence, extra=[unused["source_id"]]), evidence)
        self.assertEqual(data["sources"], [
            {"source_id": "s1", "date": {"value": "2025-01-03", "kind": "published"}},
            {"source_id": "s2", "date": {"value": "2025-01-02", "kind": "published"}}])
        self.assertEqual([item["evidence_id"] for item in data["evidence"]], ["e1", "e2", "e3"])
        self.assertEqual([item["source_ids"] for item in data["evidence"]], [["s1"], ["s2"], ["s2"]])
        self.assertNotIn(unused["source_id"], core.canonical(data).decode())
        self.assertEqual(data, core.compact_input("timeline", scope(evidence, extra=[unused["source_id"]]), deepcopy(evidence)))

    def test_known_speakers_are_aliased_per_source_and_chunk_scope(self):
        _, one = raw_evidence(recording="one", texts=["First turn.", "Second turn."], speaker="SPEAKER_0001")
        _, two = raw_evidence(recording="two", speaker="SPEAKER_0001")
        other_chunk = deepcopy(one[0])
        other_chunk["citations"][0]["source_ref"]["speaker_scope"] = "another-chunk"
        _, unknown = raw_evidence(recording="unknown")
        evidence = [*one, two[0], other_chunk, unknown[0]]
        data = core.compact_input("timeline", scope(evidence), evidence)
        self.assertEqual([item.get("speaker") for item in data["evidence"]], ["p1", "p1", "p2", "p3", None])
        self.assertNotIn("speaker", data["evidence"][-1])
        self.assertNotIn("SPEAKER_0001", core.canonical(data).decode())
        mixed = deepcopy(one[0])
        mixed["citations"] += two[0]["citations"]
        with self.assertRaisesRegex(core.SummaryError, "multiple source scopes"):
            core.compact_input("timeline", scope([mixed]), [mixed])

    def test_excerpts_deduplicate_by_original_citation_identity_not_equal_text(self):
        _, raw = raw_evidence(texts=["Identical words.", "Identical words."], speaker="SPEAKER_0002")
        evidence = [hydrated(raw[0], ordinal=1), hydrated(raw[0], ordinal=2), hydrated(raw[1], ordinal=3)]
        before = deepcopy(evidence)
        data = core.compact_input("timeline", scope(evidence), evidence)
        self.assertEqual([item["excerpt_ids"] for item in data["evidence"]], [["x1"], ["x1"], ["x2"]])
        self.assertEqual(data["source_excerpts"], [
            {"excerpt_id": "x1", "source_id": "s1", "text": "Identical words.", "speaker": "p1"},
            {"excerpt_id": "x2", "source_id": "s1", "text": "Identical words.", "speaker": "p1"}])
        self.assertEqual(evidence, before)
        conflicting = deepcopy(evidence)
        conflicting[1]["excerpts"][0]["text"] = "Different words"
        with self.assertRaisesRegex(core.SummaryError, "conflicting text or speaker"):
            core.compact_input("timeline", scope(conflicting), conflicting)

    def test_conflicting_dates_for_the_same_source_are_not_silently_merged(self):
        _, raw = raw_evidence(texts=["First.", "Second."], date="2025-01-01")
        raw[1]["citations"][0]["date"]["value"] = "2025-02-01"
        with self.assertRaisesRegex(core.SummaryError, "conflicting citation dates"):
            core.compact_input("timeline", scope(raw), raw)

    def test_excerpt_pool_follows_original_transcript_order_without_exposing_ordinals(self):
        texts = [f"Item {index:02d}." for index in range(12)]
        _, raw = raw_evidence(texts=texts, timed=False)
        citations = core._unique_citations([item["citations"][0] for item in raw])
        self.assertNotEqual([citation["ordinal"] for citation in citations], list(range(12)))
        evidence = [{"evidence_id": "summaryitem_" + "1" * 32,
                     "text": "A summary linking several original passages.",
                     "classification": "reported_statement", "speaker": None,
                     "citations": citations,
                     "excerpts": [{"citation": deepcopy(citation), "text": texts[citation["ordinal"]],
                                   "speaker": None} for citation in citations]}]
        core._validate_evidence(evidence[0])
        data = core.compact_input("timeline", scope(evidence), evidence)
        self.assertEqual([excerpt["text"] for excerpt in data["source_excerpts"]], texts)
        self.assertEqual([excerpt["excerpt_id"] for excerpt in data["source_excerpts"]],
                         [f"x{index}" for index in range(1, 13)])
        self.assertEqual(data["evidence"][0]["excerpt_ids"],
                         [f"x{citation['ordinal'] + 1}" for citation in citations])
        for excerpt in data["source_excerpts"]:
            self.assertEqual(set(excerpt), {"excerpt_id", "source_id", "text"})

    def test_excerpt_fragments_sort_by_numeric_character_position(self):
        selected = source(["0123456789ABCDEFGHIJ"])
        segment = selected["segments"][0]
        early = core._raw_evidence(selected, segment, 2, 3)
        late = core._raw_evidence(selected, segment, 10, 11)
        evidence = [hydrated(late, ordinal=1), hydrated(early, ordinal=2)]
        before = deepcopy(evidence)
        data = core.compact_input("timeline", scope(evidence), evidence)
        self.assertEqual([item["text"] for item in data["source_excerpts"]], ["2", "A"])
        self.assertEqual([item["excerpt_ids"] for item in data["evidence"]], [["x2"], ["x1"]])
        self.assertEqual(evidence, before)

    def test_excerpt_pool_groups_sources_without_reordering_evidence_or_source_aliases(self):
        _, first = raw_evidence(recording="one", texts=["A first.", "A later."], date="2025-01-01")
        _, second = raw_evidence(recording="two", texts=["B first.", "B later."], date="2025-01-02")
        evidence = [hydrated(item, ordinal=index) for index, item in
                    enumerate([second[1], first[1], second[0], first[0]], 1)]
        before = deepcopy(evidence)
        data = core.compact_input("timeline", scope(evidence), evidence)
        self.assertEqual([item["text"] for item in data["source_excerpts"]],
                         ["B first.", "B later.", "A first.", "A later."])
        self.assertEqual([item["source_id"] for item in data["source_excerpts"]], ["s1", "s1", "s2", "s2"])
        self.assertEqual([item["source_ids"] for item in data["evidence"]], [["s1"], ["s2"], ["s1"], ["s2"]])
        self.assertEqual([item["evidence_id"] for item in data["evidence"]], ["e1", "e2", "e3", "e4"])
        self.assertEqual([item["excerpt_ids"] for item in data["evidence"]], [["x2"], ["x4"], ["x1"], ["x3"]])
        self.assertEqual(evidence, before)

    def test_topic_chronology_remains_mapped_without_full_recording_ids_on_wire(self):
        selected = [source(recording="undated"), source(recording="late", date="2025-02-03"),
                    source(recording="early", date="2024-01-02"), source(recording="unused", date="2023-01-01")]
        choice = topic(["undated", "late", "early"], title="Selected walks")
        settings = config(yearly=False, archive=False, topics=[choice])
        jobs, results = through_transcripts(selected, settings)
        job = next(job for job in core.next_jobs(selected, jobs, results, settings) if job["stage"] == "topic")
        data = job["prompt"]["input"]
        self.assertEqual(data["topic"], {"id": "walks", "title": "Selected walks"})
        self.assertEqual([row["date"]["value"] for row in data["sources"]], ["2024-01-02", "2025-02-03", None])
        dates = {row["source_id"]: row["date"] for row in data["sources"]}
        self.assertEqual([dates[item["source_ids"][0]]["value"] for item in data["evidence"]],
                         ["2024-01-02", "2025-02-03", None])
        self.assertEqual(data["sources"][-1]["date"]["kind"], "unknown")
        self.assertNotIn("recording_ids", core.canonical(data).decode())
        self.assertEqual(job["config"]["broader_synthesis"]["topics"][0], choice)

    def test_api_decoder_restores_full_ids_and_produces_the_same_canonical_result(self):
        for profile in core.PROFILES:
            with self.subTest(profile=profile):
                job = core.initial_jobs([source(["First passage.", "Last passage."])],
                                        {**core.DEFAULT_CONFIG, "transcript_profile": profile})[0]
                api = output(job, text="The e1 notation is quoted as prose [00:01].")
                api["summary"][0]["evidence_ids"] = ["e2", "e1"]
                before = deepcopy(api)
                canonical = deepcopy(api)
                canonical["summary"][0]["evidence_ids"] = [job["evidence"][1]["evidence_id"], job["evidence"][0]["evidence_id"]]
                decoded = core.normalize_api_result(job, api)
                self.assertEqual(decoded, core.normalize_result(job, canonical))
                self.assertEqual(api, before)
                self.assertEqual(core.validate_result(job, decoded), decoded)
                self.assertTrue(decoded["sections"]["summary"][0]["citations"])

    def test_api_decoder_rejects_noncanonical_foreign_or_wrong_kind_aliases(self):
        job = core.initial_jobs([source(["First.", "Second."])])[0]
        invalid = ["e0", "e00", "e01", "e3", "E1", "e1 ", " e1", "s1", "x1", "p1",
                   job["evidence"][0]["evidence_id"], "summaryevidence_" + "f" * 32, 1, None, [], {}]
        for reference in invalid:
            with self.subTest(reference=reference):
                payload = output(job)
                payload["summary"][0]["evidence_ids"] = [reference]
                with self.assertRaises(core.SummaryError):
                    core.normalize_api_result(job, payload)
        for references in (["e1", "e1"], ["e1", job["evidence"][0]["evidence_id"]], [], "e1"):
            payload = output(job)
            payload["summary"][0]["evidence_ids"] = references
            with self.assertRaises(core.SummaryError):
                core.normalize_api_result(job, payload)
        with self.assertRaises(core.SummaryError):
            core.normalize_api_result(job, output(job))
        canonical = output(job)
        core.normalize_result(job, canonical)
        canonical["summary"][0]["evidence_ids"] = ["e1"]
        with self.assertRaises(core.SummaryError):
            core.normalize_result(job, canonical)

    def test_api_decoder_preserves_classification_and_schema_validation(self):
        for classification in ("reported_allegation", "uncertainty"):
            with self.subTest(classification=classification):
                selected = [source()]
                jobs = core.initial_jobs(selected)
                results = {job["job_id"]: complete(job, classification=classification) for job in jobs}
                transcript = core.next_jobs(selected, jobs, results)[0]
                payload = output(transcript, classification=classification)
                payload["summary"][0]["evidence_ids"] = ["e1"]
                decoded = core.normalize_api_result(transcript, payload)
                self.assertEqual(decoded["sections"]["summary"][0]["classification"], classification)
                payload["summary"][0]["classification"] = "reported_statement"
                with self.assertRaises(core.SummaryError):
                    core.normalize_api_result(transcript, payload)
                payload["summary"][0]["classification"] = classification
                payload["summary"][0]["text"] = " "
                with self.assertRaises(core.SummaryError):
                    core.normalize_api_result(transcript, payload)
        job = core.initial_jobs([source()])[0]
        payload = output(job)
        payload["summary"][0]["evidence_ids"] = ["e1"]
        payload["summary"][0]["speaker_name"] = "Unsupported name"
        with self.assertRaises(core.SummaryError):
            core.normalize_api_result(job, payload)

    def test_observed_plain_string_sections_are_rejected_without_invented_citations(self):
        job = core.initial_jobs([source()])[0]
        before_job = deepcopy(job)
        # Captured Batch failure shape: valid JSON, but plain strings instead of
        # the sealed schema's item objects with classification and evidence IDs.
        malformed = {"summary": ["A walk through the park."], "topics": ["Walking"],
                     "events": [], "uncertainties": []}
        for decoder in (core.normalize_api_result, core.normalize_result):
            before_payload = deepcopy(malformed)
            with self.subTest(decoder=decoder.__name__), self.assertRaisesRegex(
                    core.SummaryError, "summary item fields differ"):
                decoder(job, malformed)
            self.assertEqual(malformed, before_payload)
            for section in core.SECTIONS:
                payload = output(job)
                if decoder == core.normalize_api_result:
                    payload["summary"][0]["evidence_ids"] = ["e1"]
                payload[section] = ["A string is never a valid summary item."]
                with self.subTest(decoder=decoder.__name__, section=section), self.assertRaisesRegex(
                        core.SummaryError, "summary item fields differ"):
                    decoder(job, payload)
            for missing in ("classification", "evidence_ids"):
                payload = output(job)
                del payload["summary"][0][missing]
                with self.subTest(decoder=decoder.__name__, missing=missing), self.assertRaisesRegex(
                        core.SummaryError, "summary item fields differ"):
                    decoder(job, payload)
        self.assertEqual(job, before_job)

    def test_observed_null_arrays_and_flattened_items_remain_invalid(self):
        job = core.initial_jobs([source()])[0]
        observed_shapes = [
            {section: [None] for section in core.SECTIONS},
            {"summary": ["A supported point.", "reported_statement", "e1"]},
            {"summary": ["A supported point.", "reported_statement", "e1"],
             "topics": [], "events": [], "uncertainties": []},
        ]
        before_job = deepcopy(job)
        for payload in observed_shapes:
            before_payload = deepcopy(payload)
            for decoder in (core.normalize_api_result, core.normalize_result):
                with self.subTest(payload=payload, decoder=decoder.__name__), self.assertRaises(core.SummaryError):
                    decoder(job, payload)
                self.assertEqual(payload, before_payload)
        self.assertEqual(job, before_job)

    def test_api_decoder_replays_the_bound_alias_projection_before_accepting_output(self):
        job = core.initial_jobs([source(["First.", "Second."])])[0]
        payload = output(job)
        payload["summary"][0]["evidence_ids"] = ["e1"]
        forged = deepcopy(job)
        forged["prompt"]["input"]["evidence"][0]["source_ids"] = ["s2"]
        with self.assertRaisesRegex(core.SummaryError, "job replay"):
            core.normalize_api_result(forged, payload)


if __name__ == "__main__":
    unittest.main()
