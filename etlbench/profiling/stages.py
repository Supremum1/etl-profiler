"""Wall-clock spans for ETL operations. Parallel workers return their own spans."""
import os
from contextlib import contextmanager, nullcontext
from time import perf_counter

_spans = None
_disabled = nullcontext()


def start():
    global _spans
    _spans = []


def active():
    return _spans is not None


def finish():
    global _spans
    spans, _spans = _spans, None
    return spans or []


def add(spans):
    if _spans is not None:
        _spans.extend(spans)


def span(stage, lane=None):
    return _record(stage, lane) if active() else _disabled


@contextmanager
def _record(stage, lane):
    started = perf_counter()
    try:
        yield
    finally:
        _spans.append((stage, os.getpid() if lane is None else lane, started, perf_counter()))
