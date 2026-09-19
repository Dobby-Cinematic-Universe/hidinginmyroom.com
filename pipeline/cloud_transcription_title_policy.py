"""Bounded conversational-title hints for future cloud diarization requests.

A title is routing metadata, never positive acoustic evidence, a speaker count,
speaker identity, or permission to buy a replacement for a usable transcript.
The sampled screen remains intact beneath this separately hash-bound overlay.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path
import re
import unicodedata

from pipeline import cloud_transcription_archive as archive
from pipeline import cloud_transcription_screen as screen

KIND = "himr_cloud_conversational_title_policy"
VERSION = "explicit_interaction_title_override_v1"
MAX_TITLE_CHARS = 8192
MAX_ALIASES = 1024
MAX_TOTAL_TITLE_CHARS = 262144
POLICY = {
    "version": VERSION,
    "positive_and_uncertain_screen_require_diarization": True,
    "explicit_interaction_title_overrides_sample_negative": True,
    "title_alone_authorizes_retranscription": False,
    "title_alone_establishes_positive_screen": False,
    "existing_provider_intents_remain_frozen": True,
    "max_title_characters": MAX_TITLE_CHARS,
    "max_aliases": MAX_ALIASES,
    "max_total_title_characters": MAX_TOTAL_TITLE_CHARS,
}
SEMANTICS = {
    "titles_are_metadata_not_acoustic_evidence": True,
    "speaker_identity_inferred": False,
    "speaker_count_inferred": False,
    "calibrated_false_negative_rate_claimed": False,
    "usable_transcript_requires_positive_screen_for_retranscription": True,
}


class TitlePolicyError(RuntimeError):
    pass


def prepare_policy():
    """Return the standalone policy to save using the usual immutable writer."""
    return {"kind": KIND, "schema_version": 1,
            "implementation": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "policy": deepcopy(POLICY), "semantics": deepcopy(SEMANTICS)}


def load_policy(reference):
    value = archive.read_bound(reference, maximum=64 * 1024)
    if value != prepare_policy():
        raise TitlePolicyError("conversational title policy implementation or configuration differs")
    return value


def _normalize(title):
    value = unicodedata.normalize("NFKC", title).casefold()
    # Filename suffix IDs are provenance, not words in the title. Retain other
    # bracketed text (e.g. [Interview with Mila]) because it can be meaningful.
    value = re.sub(r"\[(?:[a-z0-9_-]{11}|[a-f0-9]{64})\](?=\s*(?:\.\w+)?\s*$)", " ", value)
    value = value.replace("_", " ").replace("’", "'").replace("'", "")
    value = re.sub(r"\bq\s*(?:&|\+|and)\s*a\b", " q and a ", value)
    return " ".join("".join(c if c.isalnum() else " " for c in value).split())


_CONTEXT = re.compile(
    r"\b(?:how to|tips (?:for|on)|guide to|tutorial|watching|watched|"
    r"react(?:ing|ion)? to|thoughts (?:on|about)|talking about|discussing|"
    r"story (?:about|of)|remembering|planning|preparing for)\b")
_NON_PARTICIPANT = {
    "myself", "yourself", "himself", "herself", "ourselves", "themselves",
    "self", "me", "you", "camera", "cameras", "mirror", "wall", "walls", "god",
    "chatgpt", "ai", "bot", "bots", "cat", "dog", "pet", "phone", "phones",
    "chat", "nobody", "nothing", "out", "about",
    "no", "not", "without", "would", "could", "should",
}
_LEADING = {"a", "an", "the", "my", "our", "his", "her", "their", "your"}
_WITH_RULES = (
    ("explicit_interview", re.compile(r"\binterview(?:s|ed|ing)?\s+with\s+")),
    ("explicit_conversation", re.compile(
        r"\b(?:conversation|conversations|discussion|debate|chat|chats|talk)\s+with\s+")),
    ("explicit_conversation", re.compile(
        r"\b(?:talking|speaking|chatting|conversing)\s+(?:with|to)\s+")),
    ("joint_question_and_answer", re.compile(
        r"\b(?:q and a|q a|qa|questions? and answers?|answering questions)(?:\s+stream)?\s+with\s+")),
    ("explicit_call", re.compile(r"\b(?:phone |video |skype |discord |zoom )?calls?\s+(?:with|to|from)\s+")),
    ("explicit_call", re.compile(r"\b(?:calling|phoning|skyping|facetiming)\s+")),
    ("explicit_guest", re.compile(r"\b(?:livestream|live stream|streaming|stream|live)\s+with\s+")),
    ("explicit_guest", re.compile(r"\bjoined\s+by\s+")),
    ("explicit_interview", re.compile(r"\binterviewing\s+")),
)
_FORMAT_CALL = re.compile(r"\b(?:phone|video|skype|discord|zoom|facetime)\s+calls?\b")
_CALL_EXCLUSIONS = re.compile(
    r"\b(?:missed|missing|unanswered|failed|cancelled|canceled|fake|pretend|"
    r"no|not|without|waiting for|expecting|didnt|doesnt|wont|never|isnt|wasnt|"
    r"settings|setup|tutorial|guide|tips|testing|test|app|apps|feature|features)\b")
_CALLING_NAMES = re.compile(r"\bcalling\s+(?:(?:my|his|her|our|their)\s+)?\w+(?:\s+\w+)?\s+(?:a|an|names|out)\b")


def _person_after(text, end):
    tokens = text[end:].split()
    while tokens and tokens[0] in _LEADING:
        tokens.pop(0)
    return bool(tokens and tokens[0] not in _NON_PARTICIPANT and not tokens[0].isdecimal())


def _blocked(text, start):
    before = text[:start]
    if _CONTEXT.search(before):
        return True
    return bool(re.search(r"\b(?:no|not|never|fake|pretend|imaginary|mock)\s+(?:a |an |the )?$", before))


def _reasons(text):
    reasons = set()
    for reason, pattern in _WITH_RULES:
        for match in pattern.finditer(text):
            if _blocked(text, match.start()) or not _person_after(text, match.end()):
                continue
            if reason == "explicit_call" and (_CALL_EXCLUSIONS.search(text) or _CALLING_NAMES.search(text)):
                continue
            reasons.add(reason)
    for match in _FORMAT_CALL.finditer(text):
        counterpart = re.match(r"\s+(?:with|to|from)\s+", text[match.end():])
        excluded_person = counterpart and not _person_after(text, match.end() + counterpart.end())
        if not excluded_person and not _blocked(text, match.start()) and not _CALL_EXCLUSIONS.search(text):
            reasons.add("explicit_call")
    # Names before "interview" are common archive titles, but job interviews,
    # advice, and retrospectives are not evidence of a recorded conversation.
    match = re.search(r"\b([^\W\d_]+)\s+interview\b", text)
    if (match and match.group(1) not in {
            "job", "work", "my", "an", "the", "a", "his", "her", "our", "this",
            "that", "last", "next", "first", "second", "failed", "fake", "mock",
            "best", "worst", "new", "old", "no", "not", "about", "preparing"}
            and not _blocked(text, match.start())
            and not re.search(r"\b(?:tips|advice|preparation|reaction|review|analysis|job|how to)\b", text)):
        reasons.add("named_interview_format")
    return sorted(reasons)


def match_titles(recording):
    """Pure bounded matcher over exactly the caller's bound title and aliases."""
    if not isinstance(recording, dict):
        raise TitlePolicyError("title matching requires a recording object")
    aliases = recording.get("aliases", [])
    if not isinstance(aliases, list) or len(aliases) > MAX_ALIASES:
        raise TitlePolicyError("recording title aliases exceed their bound")
    titles = [("title", recording.get("title"))]
    for index, alias in enumerate(aliases):
        if not isinstance(alias, dict):
            raise TitlePolicyError("recording title alias must be an object")
        titles.append(("aliases[" + str(index) + "].title", alias.get("title")))
    matched, total = [], 0
    for source, title in titles:
        if title is None:
            continue
        if not isinstance(title, str) or len(title) > MAX_TITLE_CHARS:
            raise TitlePolicyError("recording title exceeds its bound")
        total += len(title)
        if total > MAX_TOTAL_TITLE_CHARS:
            raise TitlePolicyError("recording title collection exceeds its bound")
        normalized = _normalize(title)
        reasons = _reasons(normalized)
        if reasons:
            matched.append({"source": source, "title": title, "normalized": normalized, "reasons": reasons})
    return {"matched": bool(matched), "reasons": sorted({reason for row in matched for reason in row["reasons"]}),
            "matched_titles": matched}


def effective_decision(base_decision, base_ref, recording, policy_ref):
    """Pure overlay builder; callers must replay the base before trusting it."""
    archive._ref(base_ref)
    archive._ref(policy_ref)
    if (not isinstance(recording, dict) or not isinstance(base_decision, dict)
            or base_decision.get("kind") != screen.KIND + "_decision"
            or base_decision.get("schema_version") != 1
            or base_decision.get("recording_id") != recording.get("recording_id")
            or base_decision.get("media") != recording.get("media")
            or base_decision.get("state") not in {"screen_positive", "screen_uncertain", "screen_negative"}
            or type(base_decision.get("diarization")) is not bool
            or base_decision["diarization"] != (base_decision["state"] != "screen_negative")
            or not isinstance(base_decision.get("method"), dict)
            or "title_override" in base_decision["method"]):
        raise TitlePolicyError("title overlay requires an unmodified base screen decision")
    matching = match_titles(recording)
    result = deepcopy(base_decision)
    result["diarization"] = base_decision["diarization"] or matching["matched"]
    result["method"]["title_override"] = {
        "version": VERSION, "policy": deepcopy(policy_ref), "base_decision": deepcopy(base_ref),
        "recording_sha256": hashlib.sha256(archive.canonical(recording)).hexdigest(),
        "matching": matching, "applied": matching["matched"] and not base_decision["diarization"],
        "semantics": deepcopy(SEMANTICS),
    }
    return result


def validate_effective(decision, recording, config_ref, policy_ref):
    """Replay the original acoustic proof, then exactly recompute the overlay."""
    load_policy(policy_ref)
    method = decision.get("method") if isinstance(decision, dict) else None
    override = method.get("title_override") if isinstance(method, dict) else None
    if not isinstance(override, dict) or override.get("policy") != policy_ref:
        raise TitlePolicyError("effective screen decision lacks the admitted title policy")
    base_ref = override.get("base_decision")
    base = archive.read_bound(base_ref)
    screen.validate_decision(base, recording, config_ref)
    if decision != effective_decision(base, base_ref, recording, policy_ref):
        raise TitlePolicyError("effective screen title decision does not replay")
    return decision
