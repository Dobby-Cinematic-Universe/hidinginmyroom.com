"""Retained private output directories for the standalone CPU screen only."""
from contextlib import contextmanager
from contextvars import ContextVar
import os
from pathlib import Path

_ANCHORS = ContextVar("speaker_screen_output_directories", default={})


def anchor(path):
    return _ANCHORS.get().get(str(path))


@contextmanager
def retained_directory(path):
    """Pin an owned 0700 leaf; ancestor rename cannot redirect later writes."""
    path = Path(path)
    existing = anchor(path)
    parent = anchor(path.parent)
    if existing is not None:
        fd = os.dup(existing)
    elif parent is not None:
        fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    else:
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in path.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
        except BaseException:
            os.close(fd)
            raise
    try:
        info = os.fstat(fd)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise RuntimeError("screen output directory must be owned and private (0700)")
        token = _ANCHORS.set({**_ANCHORS.get(), str(path): fd})
        try:
            yield fd
        finally:
            _ANCHORS.reset(token)
    finally:
        os.close(fd)
