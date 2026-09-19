"""Lazy, bounded API-key dotenv loading; imports never inspect credentials.

This is a small data parser, not a shell or a general environment loader. Only
the three summary-provider API keys are returned; os.environ is never mutated.
The caller supplies the default path and invokes this only when HTTP needs a key.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import stat


MAX_ENV_BYTES = 64 * 1024
API_KEY_NAMES = frozenset({"GEMINI_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY"})
_ASSIGNMENT = re.compile(r"(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)(.*)\Z")
_DOUBLE_ESCAPES = {"n": "\n", "r": "\r", "t": "\t", "b": "\b", "f": "\f",
                   "v": "\v", "a": "\a", '"': '"', "\\": "\\"}


class EnvFileError(RuntimeError):
    """Controlled diagnostic containing neither credential values nor paths."""


def _line_error(number):
    return EnvFileError("invalid or duplicate API key assignment on env file line " + str(number))


def _value(text, number):
    text = text.strip(" \t")
    if not text or text.startswith("#"):
        return ""
    if text[0] not in {"'", '"'}:
        # A hash within a token is literal; whitespace introduces a comment.
        return re.split(r"[ \t]+#", text, maxsplit=1)[0].rstrip(" \t")
    quote, result, index = text[0], [], 1
    while index < len(text):
        char = text[index]
        if char == quote:
            rest = text[index + 1:]
            if rest and (rest[0] not in " \t" or not rest.strip(" \t").startswith("#")):
                if rest.strip(" \t"):
                    raise _line_error(number)
            return "".join(result)
        if char == "\\" and index + 1 < len(text):
            following = text[index + 1]
            if quote == '"' and following in _DOUBLE_ESCAPES:
                result.append(_DOUBLE_ESCAPES[following])
                index += 2
                continue
            if quote == "'" and following in {"'", "\\"}:
                result.append(following)
                index += 2
                continue
        result.append(char)
        index += 1
    raise _line_error(number)


def parse_api_keys(raw: bytes) -> dict[str, str]:
    """Parse supported single-line dotenv assignments without interpolation.

    Accept KEY=value or export KEY=value, surrounding horizontal whitespace,
    single/double quotes, blank/comment lines, and LF/CRLF. Duplicate supported
    names are rejected. Unrelated lines are ignored, even if their values use
    syntax unsupported by this restricted parser. Dollar signs and backticks
    remain literal. Quoted supported values may not span physical lines.
    """
    if not isinstance(raw, bytes) or len(raw) > MAX_ENV_BYTES:
        raise EnvFileError("API key env file exceeds the 64 KiB size limit")
    try:
        text = raw.decode("utf-8")
    except UnicodeError:
        raise EnvFileError("API key env file must contain valid UTF-8") from None
    values = {}
    for number, line in enumerate(text.split("\n"), 1):
        line = line.removesuffix("\r").strip(" \t")
        if not line or line.startswith("#"):
            continue
        match = _ASSIGNMENT.fullmatch(line)
        if match is None or match[1] not in API_KEY_NAMES:
            continue
        name, rest = match[1], match[2].lstrip(" \t")
        if name in values or not rest.startswith("=") or "\r" in line or "\x00" in line:
            raise _line_error(number)
        values[name] = _value(rest[1:], number)
    return values


def _witness(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _read_env(path, *, missing_ok):
    """Pin every path component, reject non-regular files before reading bytes."""
    directory = descriptor = None
    try:
        # abspath normalizes a relative filename without resolving symlinks or
        # expanding '~', '$VAR', command substitutions, or other shell syntax.
        path = Path(os.path.abspath(os.fspath(path)))
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
        directory = os.open(path.anchor, flags | os.O_DIRECTORY)
        for part in path.parts[1:-1]:
            child = os.open(part, flags | os.O_DIRECTORY, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(path.name, flags, dir_fd=directory)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise EnvFileError("API key env file must be a regular file with one link")
        if before.st_uid != os.getuid() or stat.S_IMODE(before.st_mode) not in {0o400, 0o600}:
            raise EnvFileError("API key env file must be owned by the current user with mode 0600 or 0400; use chmod 600")
        if not 0 <= before.st_size <= MAX_ENV_BYTES:
            raise EnvFileError("API key env file exceeds the 64 KiB size limit")
        raw = os.pread(descriptor, MAX_ENV_BYTES + 1, 0)
        if len(raw) != before.st_size or _witness(os.fstat(descriptor)) != _witness(before):
            raise EnvFileError("API key env file changed while being read")
        return raw
    except FileNotFoundError:
        if missing_ok:
            return None
        raise EnvFileError("explicit API key env file does not exist") from None
    except EnvFileError:
        raise
    except (OSError, ValueError, TypeError):
        raise EnvFileError("API key env file could not be opened safely; symlinks are not allowed") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory is not None:
            os.close(directory)


def api_key(name: str, *, default_path, env_file=None) -> str | None:
    """Return one explicit environment key, or lazily consult a private dotenv.

    Presence in os.environ takes precedence, including an empty value. Missing
    default files are tolerated; an explicitly requested missing file is an
    error only when this fallback is actually needed. Nothing is persisted.
    """
    if name not in API_KEY_NAMES:
        raise EnvFileError("unsupported summary API key name")
    if name in os.environ:
        return os.environ[name]
    raw = _read_env(default_path if env_file is None else env_file, missing_ok=env_file is None)
    return None if raw is None else parse_api_keys(raw).get(name)
