"""Optional, byte-identical linear-time fitting for initial summary chunks.

This does not change the summary contract or accept retained jobs. It only avoids
reconstructing every pending evidence item each time one more segment is tried.
The ordinary core still validates sources and builds every final immutable job.
Unknown core versions use the original implementation, captured before callers
install any scoped wrapper around ``core.initial_jobs``.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from pipeline import transcript_summary_core as core


SUPPORTED_CORE_SHA256 = "b0c78e0ff9b6ffc167146383986c94c153edde22223e70a2cef26bea2e1adf5f"
_ORIGINAL_INITIAL_JOBS = core.initial_jobs
try:
    _CORE_SUPPORTED = hashlib.sha256(Path(core.__file__).read_bytes()).hexdigest() == SUPPORTED_CORE_SHA256
except OSError:
    _CORE_SUPPORTED = False


def _wire_size(value):
    """Bytes contributed by compact JSON inside the request's JSON string.

    Escaping is character-local, so concatenated JSON fragments contribute the
    sum of these sizes. The outer string's two quote bytes are excluded.
    """
    return len(core.canonical(core.canonical(value).decode("utf-8"))) - 2


class _ChunkSizer:
    def __init__(self, source, probe, config):
        self.source = source
        self.projected_only = config.get("transcript_input_policy") == "text_and_speaker_evidence_v1"
        # Derive all provider/schema/instruction framing from an actual core job;
        # do not duplicate its prompts, rates, schemas or request-body builders.
        framing = probe["budget"]["input_utf8_bytes"] - _wire_size(probe["prompt"]["input"])
        empty_input = {"stage": "chunk", "evidence": []}
        if not self.projected_only:
            empty_input.update(period=None, sources=[{
                "source_id": "s1",
                "date": {"value": source["date"].get("value"), "kind": source["date"].get("kind")},
            }])
        self.empty_bytes = framing + _wire_size(empty_input)
        self.reset()

    def reset(self):
        self.input_bytes = self.empty_bytes
        self.count = 0
        self.speakers = {}

    def speaker_key(self, segment):
        if segment["speaker"] is None:
            return None
        return (self.source["source_id"], core.canonical(segment["source_ref"].get("speaker_scope")),
                segment["speaker"])

    def append_size(self, segment, text):
        item = {"evidence_id": "e" + str(self.count + 1), "text": text}
        if not self.projected_only:
            item["source_ids"] = ["s1"]
        key = self.speaker_key(segment)
        if key is not None:
            item["speaker"] = self.speakers.get(key, "p" + str(len(self.speakers) + 1))
        return self.input_bytes + int(self.count > 0) + _wire_size(item)

    def accept(self, segment, input_bytes):
        key = self.speaker_key(segment)
        if key is not None and key not in self.speakers:
            self.speakers[key] = "p" + str(len(self.speakers) + 1)
        self.input_bytes = input_bytes
        self.count += 1


def initial_jobs(sources, config=None):
    """Build precisely the original chunks, using incremental fit accounting.

    The API matches ``core.initial_jobs``. There is no disk scan, persistent
    cache, model call, changed evidence policy, or relaxed final validation here.
    """
    if not _CORE_SUPPORTED:
        return _ORIGINAL_INITIAL_JOBS(sources, config)
    config = core.normalize_config(config)
    sources = core._source_list(sources)
    core._topic_selections(sources, config)
    bound = core._input_bound("chunk", config)
    jobs = []
    for source in sources:
        if not any(segment["text"].strip() for segment in source["segments"]):
            continue
        index = 0
        pending = []

        def scope():
            return core._scope([source["source_id"]], None, 0, index, False)

        first_segment = next(segment for segment in source["segments"] if segment["text"])
        try:
            probe = core.make_job("chunk", scope(), [core._raw_evidence(source, first_segment, 0, 1)], [], config)
        except core.SummaryError as error:
            if str(error) == "summary input exceeds configured byte bound":
                raise core.SummaryError("one source character cannot fit configured input bound") from error
            raise
        sizer = _ChunkSizer(source, probe, config)

        def flush():
            job = core.make_job("chunk", scope(), pending, [], config)
            # Fail closed if a future unsupported projection escaped the file
            # contract: never reserve or submit an incorrectly sized request.
            if job["budget"]["input_utf8_bytes"] != sizer.input_bytes:
                raise core.SummaryError("incremental summary input byte accounting differs")
            jobs.append(job)

        for segment in source["segments"]:
            text = segment["text"]
            start = 0
            while start < len(text):
                # Same character bounds, greedy full attempt and binary-search
                # decisions as core.initial_jobs, without rebuilding pending.
                lo, hi = start + 1, min(len(text), start + bound)
                best = None
                size = sizer.append_size(segment, text[start:hi])
                if size <= bound:
                    best = (hi, size)
                    lo = hi + 1
                while lo <= hi:
                    end = (lo + hi) // 2
                    size = sizer.append_size(segment, text[start:end])
                    if size <= bound:
                        best = (end, size)
                        lo = end + 1
                    else:
                        hi = end - 1
                if best is None:
                    if not pending:
                        raise core.SummaryError("one source character cannot fit configured input bound")
                    flush()
                    pending = []
                    sizer.reset()
                    index += 1
                    continue
                end, size = best
                pending.append(core._raw_evidence(source, segment, start, end))
                sizer.accept(segment, size)
                start = end
                if start < len(text):
                    flush()
                    pending = []
                    sizer.reset()
                    index += 1
        if pending:
            flush()
    return jobs
