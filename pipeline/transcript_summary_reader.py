"""Private reader exports: plain transcript summaries and whole-transcript links.

This presentation-only command never submits model requests or alters canonical
results. It is deliberately outside the runner's pinned implementation files so
adding this view does not invalidate existing plans.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import transcript_summary as runner
from pipeline import transcript_summary_core as core


def _transcript_href(source_id):
    return "transcripts/" + source_id + ".txt"


def _sections(result, sources, *, links, required_sources):
    sections = {}
    for section in core.SECTIONS:
        sections[section] = []
        for item in result["sections"][section]:
            # Preserve wording exactly: stripping citation-looking text could
            # remove substantive prose. Only structured evidence is projected.
            rendered = {"text": item["text"], "classification": item["classification"]}
            if links:
                ids = sorted({citation["source_id"] for citation in item["citations"]})
                if not ids or any(source_id not in sources for source_id in ids):
                    raise runner.Error("reader summary references a missing transcript")
                required_sources.update(ids)
                rendered["sources"] = [{"source_id": source_id,
                    "recording_id": sources[source_id]["recording_id"],
                    "title": sources[source_id]["title"],
                    "href": _transcript_href(source_id)} for source_id in ids]
            sections[section].append(rendered)
    return sections


def export_reader(path, expected_sha256, *, phase="transcripts"):
    if phase == "transcripts":
        from pipeline import summary_recovery_integration as recovery
        preferred = recovery.preferred_reader(dict(path=str(Path(path).resolve()), sha256=expected_sha256))
        if preferred is not None:
            return preferred
    stages = runner.phase_stages(phase)
    plan, selected = runner.load_plan(path, expected_sha256)
    root = Path(plan["request_value"]["state_root"])
    with runner.locked(root):
        state = runner.load_state(plan, selected)
        progress = runner.phase_progress(selected, state, plan["request_value"]["config"])
        complete = progress["transcript_phase_complete"] and progress["synthesis_phase_complete"]
        phase_complete = (complete if phase == "all" else progress[
            "transcript_phase_complete" if phase == "transcripts" else "synthesis_phase_complete"])
        sources = {source["source_id"]: source for source in selected}
        finals = [result for result in state["results"]
                  if result["scope"]["final"] and result["stage"] in stages]
        transcripts = {}
        for result in finals:
            if result["stage"] == "transcript":
                ids = result["scope"]["source_ids"]
                if len(ids) != 1 or ids[0] not in sources or ids[0] in transcripts:
                    raise runner.Error("reader transcript result selection differs")
                transcripts[ids[0]] = result
        required_sources = set(transcripts)
        records = [{"source_id": source["source_id"], "recording_id": source["recording_id"],
                    "title": source["title"],
                    "date": {key: source["date"][key] for key in ("value", "kind")},
                    "sections": _sections(transcripts[source["source_id"]], sources,
                                           links=False, required_sources=required_sources)}
                   for source in selected if source["source_id"] in transcripts]
        broader = sorted((result for result in finals if result["stage"] != "transcript"),
                         key=lambda result: (result["stage"], result["scope"]["period"] or ""))
        synthesis = [{"stage": result["stage"], "period": result["scope"]["period"],
                      "sections": _sections(result, sources, links=True,
                                             required_sources=required_sources)} for result in broader]
        texts = {source_id: "\n".join(segment["text"] for segment in sources[source_id]["segments"]).encode("utf-8")
                 for source_id in sorted(required_sources)}
        if any(not 0 < len(raw) <= runner.MAX_JSON for raw in texts.values()):
            raise runner.Error("reader transcript exceeds the private artifact byte bound")
        body = {"kind": "himr_private_summary_reader_export", "schema_version": 1,
                "plan_id": plan["plan_id"], "phase": phase, "phase_complete": phase_complete,
                "complete": complete, "selected_source_ids": list(plan["source_ids"]),
                "records": records, "synthesis": synthesis,
                "transcript_files": [{"source_id": source_id, "href": _transcript_href(source_id),
                                      "sha256": hashlib.sha256(raw).hexdigest()}
                                     for source_id, raw in texts.items()],
                "semantics": deepcopy(runner.SEMANTICS)}
        data = runner.canonical(body)
        if len(data) > runner.MAX_JSON:
            raise runner.Error("reader export exceeds the private artifact byte bound")
        # Publish index last: all relative links resolve when it becomes visible.
        # Immutable, content-addressed snapshots leave earlier views untouched.
        runner.mkdir(root / "reader-exports")
        folder = root / "reader-exports" / ("reader-" + runner.digest(body)[:32])
        runner.mkdir(folder)
        runner.mkdir(folder / "transcripts")
        for source_id, raw in texts.items():
            runner.put_bytes(folder / _transcript_href(source_id), raw)
        artifact = runner.put_bytes(folder / "index.json", data)
        return {"state": "exported_private_reader", "artifact": artifact,
                "transcript_summaries": len(records), "synthesis_summaries": len(synthesis),
                "phase": phase, "phase_complete": phase_complete, "complete": complete}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--phase", choices=("all", "transcripts", "synthesis"), default="transcripts")
    args = parser.parse_args(argv)
    try:
        print(json.dumps(export_reader(args.manifest, args.expected_sha256, phase=args.phase),
                         indent=2, sort_keys=True, allow_nan=False))
        return 0
    except KeyboardInterrupt:
        print("Reader export interrupted; canonical summary results are unchanged.", file=sys.stderr)
        return 130
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        if isinstance(error, (runner.Error, core.SummaryError, runner.sources_module.SourceError)):
            detail = str(error).replace("\n", " ")[:800]
        elif isinstance(error, OSError):
            detail = "local I/O failure (errno " + str(error.errno) + "); inspect private storage"
        else:
            detail = type(error).__name__ + "; inspect private plan and results"
        print("TranscriptSummaryReaderError: " + detail, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
