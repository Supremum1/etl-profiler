from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context

from etlbench.common import key_ranges
from etlbench.pipelines.sync_pipeline import run as sync_run
from etlbench.profiling import stages


def worker(job):
    columns, target, batch_size, fetch_size, first, last, reader, trace = job
    if trace:
        stages.start()
    try:
        result = sync_run(columns, target, batch_size, fetch_size, first=first, last=last, reader=reader)
    finally:
        spans = stages.finish() if trace else []
    return result, spans


def run(columns, target, batch_size, fetch_size=None, workers=4, first=None, last=None, reader="stream"):
    fetch_size = fetch_size or batch_size
    jobs = [
        (columns, target, batch_size, fetch_size, start, end, reader, stages.active())
        for start, end in key_ranges(first, last, workers)
    ]
    if not jobs:
        return 0, 0
    # Spawn avoids inheriting the parent's open database sockets.
    with ProcessPoolExecutor(len(jobs), mp_context=get_context("spawn")) as pool:
        results = []
        for result, spans in pool.map(worker, jobs):
            results.append(result)
            stages.add(spans)
    rows = None if any(rows is None for rows, _ in results) else sum(rows for rows, _ in results)
    return rows, sum(batches for _, batches in results)
