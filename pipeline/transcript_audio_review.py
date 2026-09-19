"""Offline, separate diarization review pilot; never a production transcript writer.

Heuristics nominate evidence, not identities, intelligibility or audio sources.
No provider calls, model downloads, archive scans, or summary submissions.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import html
import json
import math
import os
from pathlib import Path
import re
import struct
import subprocess
import wave


def binding(path):
    path = Path(path).resolve()
    data = path.read_bytes()
    return {"path": str(path), "sha256": hashlib.sha256(data).hexdigest()}


def read_bound(ref):
    data = Path(ref["path"]).read_bytes()
    if hashlib.sha256(data).hexdigest() != ref["sha256"]:
        raise ValueError("review input digest mismatch")
    return json.loads(data)


def analyze(doc, raw):
    segments = doc["segments"]
    utterances = raw.get("utterances")
    if not isinstance(utterances, list) or len(utterances) != len(segments):
        raise ValueError("pilot requires aligned AssemblyAI utterances")
    labels = Counter(s["speaker"] for s in segments)
    forward, reverse, seen, rows = {}, {}, {}, []
    for i, (s, u) in enumerate(zip(segments, utterances)):
        if (s["start_ms"], s["end_ms"], s["text"]) != (u["start"], u["end"], u["text"]):
            raise ValueError("utterance alignment mismatch")
        if s["start_ms"] < 0 or s["end_ms"] <= s["start_ms"]:
            raise ValueError("invalid segment times")
        a, b = s["speaker"], u["speaker"]
        if forward.setdefault(a, b) != b or reverse.setdefault(b, a) != a:
            raise ValueError("provider label mapping mismatch")
        confidence = u.get("confidence")
        if confidence is not None and (type(confidence) not in (int, float)
                or not math.isfinite(confidence) or not 0 <= confidence <= 1):
            raise ValueError("invalid confidence")
        flags = []
        if confidence is not None and confidence < .65:
            flags.append("low_asr_confidence_review")
        if s["end_ms"] - s["start_ms"] < 500:
            flags.append("very_short_turn_review")
        if labels[a] <= max(2, len(segments) * .02):
            flags.append("rare_label_review")
        if re.search(r"\b(donat\w*|text.to.speech|tts)\b", s["text"], re.I):
            flags.append("donation_or_tts_context_not_source_proof")
        key = re.sub(r"\W+", " ", s["text"].lower()).strip()
        prior = seen.get(key)
        if len(key.split()) >= 8 and prior is not None and 0 <= s["start_ms"] - segments[prior]["end_ms"] <= 120000:
            flags.append("nearby_repetition_review")
        seen[key] = i
        rows.append({"segment_index": i, **s, "asr_confidence": confidence,
                     "review_flags": flags, "audio_source": "unknown",
                     "identity": None, "intelligibility": "unreviewed"})
    return {"kind": "himr_audio_review_pilot", "schema_version": 1,
            "production_eligible": False, "verified_human_participant_count": None,
            "provider_label_segment_counts": dict(labels), "segments": rows}


def select_clips(rows, limit):
    """Cover labels, then source-context, repetition and confidence leads, bounded."""
    chosen = []
    for label in dict.fromkeys(r["speaker"] for r in rows):
        candidates = [r for r in rows if r["speaker"] == label]
        best = max(candidates, key=lambda r: (min(r["end_ms"]-r["start_ms"], 8000), r["asr_confidence"] or 0))
        chosen.append(best["segment_index"])
    priority = lambda r: ("nearby_repetition_review" in r["review_flags"],
                          "donation_or_tts_context_not_source_proof" in r["review_flags"],
                          len(r["review_flags"]), -(r["asr_confidence"] or 0))
    for row in sorted(rows, key=priority, reverse=True):
        if row["segment_index"] not in chosen and row["review_flags"]:
            chosen.append(row["segment_index"])
    return chosen[:limit]


def reviewed_projection(report, decisions):
    """Explicit segment-level decisions only; unknown spans cannot be published.

    Evidence is bound local reviewer material. It is provenance, not an automatic
    guarantee of a correct human judgment. No label-wide source inference.
    """
    lookup = {}
    for d in decisions:
        i = d["segment_index"]
        if type(i) is not int or not 0 <= i < len(report["segments"]) or i in lookup:
            raise ValueError("invalid/duplicate decision index")
        if d["audio_source"] not in {"participant", "playback", "tts", "unknown"}:
            raise ValueError("invalid audio source")
        if d["intelligibility"] not in {"clear", "uncertain", "unintelligible"}:
            raise ValueError("invalid intelligibility")
        if not isinstance(d.get("reviewer"), str) or not d["reviewer"].strip() or not d.get("evidence"):
            raise ValueError("reviewer and evidence required")
        for ref in d["evidence"]:
            if binding(ref["path"]) != ref:
                raise ValueError("review evidence mismatch")
        if d.get("identity") is not None and (d["audio_source"] != "participant" or not isinstance(d["identity"], str) or not d["identity"].strip()):
            raise ValueError("identity requires reviewed participant")
        lookup[i] = d
    result = []
    for row in report["segments"]:
        d = lookup.get(row["segment_index"])
        include = bool(d and d["audio_source"] == "participant" and d["intelligibility"] == "clear" and d.get("identity"))
        result.append({**row, "review_decision": d, "include_in_summary_preview": include,
                       "resolved_identity": d.get("identity") if d else None})
    return {"kind": "himr_reviewed_transcript_preview", "production_eligible": False,
            "segments": result, "summary_preview": "\n".join(
                f'{r["resolved_identity"]}: {r["text"]}' for r in result if r["include_in_summary_preview"])}


def write_json(path, value):
    with Path(path).open("x") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.write("\n")


def run(transcript, output, max_clips=12):
    if not 1 <= max_clips <= 24:
        raise ValueError("max clips must be 1..24")
    ref = binding(transcript)
    doc = read_bound(ref)
    report = analyze(doc, read_bound(doc["raw_result"]))
    output = Path(output).absolute()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    report.update(transcript=ref, raw_result=doc["raw_result"], source_media=doc["source_media"],
                  implementation=binding(__file__), sampling="biased review leads, not accuracy or archive coverage",
                  source_validation="bounded reads and stable stat witness; full media hash deliberately not reread")
    source = Path(doc["source_media"]["path"])
    before = source.stat()
    if before.st_size != doc["source_media"]["byte_count"]:
        raise ValueError("source media size mismatch")
    entries = ["<meta charset='utf-8'><title>Audio source review pilot</title>",
               "<h1>Review pilot — not production input</h1><p>Labels are not identities. Confidence is not accuracy. RMS is not SNR. Context cues do not prove TTS or playback. Clips may cover only part of a turn.</p>"]
    for i in select_clips(report["segments"], max_clips):
        row = report["segments"][i]
        start = max(0, row["start_ms"] - 1000)
        duration = min(12000, row["end_ms"] + 1000 - start)
        target = output / f"segment-{i:05d}.wav"
        subprocess.run(["ffmpeg", "-nostdin", "-v", "error", "-ss", str(start/1000),
                        "-i", str(source), "-t", str(duration/1000), "-vn", "-ac", "1",
                        "-ar", "16000", "-c:a", "pcm_s16le", "-n", str(target)],
                       check=True, timeout=60, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        with wave.open(str(target)) as wav:
            frames = wav.readframes(wav.getnframes())
        values = [v[0]/32768 for v in struct.iter_unpack("<h", frames)]
        if not values:
            raise ValueError("empty review clip")
        rms = math.sqrt(sum(v*v for v in values)/len(values))
        row["clip"] = {**binding(target), "start_ms": start, "duration_ms": len(values)/16,
                       "rms_dbfs": 20*math.log10(rms) if rms else None,
                       "covers_entire_segment": start+len(values)/16 >= row["end_ms"]}
        entries.append(f'<h2>Segment {i} · {html.escape(str(row["speaker"]))}</h2><p>{html.escape(row["text"])}</p><p>{html.escape(str(row["review_flags"]))}</p><audio controls preload="none" src="{target.name}"></audio>')
    after = source.stat()
    witness = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if witness(before) != witness(after):
        raise ValueError("source changed during pilot")
    read_bound(ref)
    read_bound(doc["raw_result"])
    write_json(output/"report.json", report)
    write_json(output/"preview.json", reviewed_projection(report, []))
    with (output/"review.html").open("x") as f:
        f.write("\n".join(entries))
    return {"output": str(output), "segments": len(report["segments"]),
            "provider_labels": len(report["provider_label_segment_counts"]),
            "clips": sum("clip" in r for r in report["segments"]),
            "flagged_segments": sum(bool(r["review_flags"]) for r in report["segments"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--transcript")
    mode.add_argument("--review-report")
    parser.add_argument("--decisions")
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-clips", type=int, default=12)
    args = parser.parse_args()
    os.umask(0o077)
    if args.review_report:
        if not args.decisions:
            parser.error("--review-report requires --decisions")
        report_ref = binding(args.review_report)
        decision_ref = binding(args.decisions)
        decisions = read_bound(decision_ref)
        if decisions.get("report") != report_ref:
            raise ValueError("decisions must bind this exact report")
        report = read_bound(report_ref)
        read_bound(report["transcript"])
        read_bound(report["raw_result"])
        result = reviewed_projection(report, decisions["decisions"])
        result.update(report=report_ref, decisions=decision_ref, implementation=binding(__file__))
        write_json(args.output, result)
        print(json.dumps({"output": args.output, "production_eligible": False}))
    else:
        if args.decisions:
            parser.error("--decisions is only for --review-report")
        print(json.dumps(run(args.transcript, args.output, args.max_clips)))


if __name__ == "__main__":
    main()
