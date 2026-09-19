"""Lazy private dotenv credentials for the separate cloud transcription lane."""
from __future__ import annotations

import os
from pathlib import Path

from pipeline import transcript_summary_env as dotenv

NAMES = frozenset({'ASSEMBLYAI_API_KEY', 'REVAI_ACCESS_TOKEN', 'REVAI_API_KEY'})
DEFAULT_PATH = Path(__file__).resolve().parents[1] / '.env'
EnvError = dotenv.EnvFileError


def parse_keys(raw):
    if not isinstance(raw, bytes) or len(raw) > dotenv.MAX_ENV_BYTES:
        raise EnvError('cloud API key env file exceeds its size limit')
    try:
        text = raw.decode('utf-8')
    except UnicodeError:
        raise EnvError('cloud API key env file must be UTF-8') from None
    values = {}
    for number, line in enumerate(text.split('\n'), 1):
        line = line.removesuffix('\r').strip(' \t')
        match = dotenv._ASSIGNMENT.fullmatch(line)
        if match is None or match[1] not in NAMES:
            continue
        name, rest = match[1], match[2].lstrip(' \t')
        if name in values or not rest.startswith('=') or '\r' in line or '\x00' in line:
            raise dotenv._line_error(number)
        values[name] = dotenv._value(rest[1:], number)
    return values


def api_key(provider, *, env_file=None):
    names = {'assemblyai': ('ASSEMBLYAI_API_KEY',),
             'revai': ('REVAI_ACCESS_TOKEN', 'REVAI_API_KEY')}.get(provider)
    if names is None:
        raise EnvError('unsupported cloud transcription provider')
    # An explicit environment value, including empty, overrides dotenv values.
    values = {name: os.environ[name] for name in names if name in os.environ}
    if not values:
        raw = dotenv._read_env(DEFAULT_PATH if env_file is None else env_file,
                               missing_ok=env_file is None)
        loaded = {} if raw is None else parse_keys(raw)
        values = {name: loaded[name] for name in names if name in loaded}
    if len(set(values.values())) > 1:
        raise EnvError('conflicting Rev AI credential aliases')
    key = next(iter(values.values()), None)
    if key is not None and (not key or len(key) > 8192 or any(ord(c) < 33 or ord(c) > 126 for c in key)):
        raise EnvError('cloud API key is empty or contains invalid header characters')
    return key
