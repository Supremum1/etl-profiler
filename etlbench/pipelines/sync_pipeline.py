from itertools import islice

import psycopg

from etlbench.common import PG_DSN, clickhouse, insert_sql, select_sql
from etlbench.profiling.stages import span


def run(columns, target, batch_size, fetch_size, workers=1, first=None, last=None, reader="stream"):
    if reader == "cursor":
        return _run_cursor(columns, target, batch_size, fetch_size, first, last)
    if reader == "keyset":
        return _run_keyset(columns, target, batch_size, fetch_size, first, last)
    if reader == "copy":
        return _run_copy(columns, target, batch_size, fetch_size, first, last)
    if reader == "copy-direct":
        from etlbench.pipelines.copy_direct import run as copy_direct_run
        return copy_direct_run(columns, target, first, last)
    return _run_stream(columns, target, batch_size, fetch_size, first, last)


def _write_batches(client, target, columns, fetched, batch_size):
    batches = 0
    for start in range(0, len(fetched), batch_size):
        batch = fetched[start : start + batch_size]
        with span("ch_insert"):
            client.execute(insert_sql(target, columns), batch)
        batches += 1
    return batches


def _run_cursor(columns, target, batch_size, fetch_size, first, last):
    rows = batches = 0
    where = "WHERE etl_row_num BETWEEN %s AND %s" if first is not None else ""
    parameters = (first, last) if first is not None else None
    with clickhouse() as client, psycopg.connect(PG_DSN) as connection:
        with connection.cursor(name="source") as cursor:
            with span("pg_fetch"):
                cursor.execute(select_sql(columns, where), parameters)
            while True:
                with span("pg_fetch"):
                    fetched = cursor.fetchmany(fetch_size)
                if not fetched:
                    break
                batches += _write_batches(client, target, columns, fetched, batch_size)
                rows += len(fetched)
    return rows, batches


def _run_keyset(columns, target, batch_size, fetch_size, first, last):
    rows = batches = 0
    last_seen = first - 1 if first is not None else -1
    where = "WHERE etl_row_num > %s"
    if last is not None:
        where += " AND etl_row_num <= %s"
    query = select_sql(columns, where) + " LIMIT %s"
    with clickhouse() as client, psycopg.connect(PG_DSN) as connection:
        with connection.cursor() as cursor:
            while True:
                parameters = (last_seen, last, fetch_size) if last is not None else (last_seen, fetch_size)
                with span("pg_fetch"):
                    cursor.execute(query, parameters)
                    fetched = cursor.fetchmany(fetch_size)
                if not fetched:
                    break
                batches += _write_batches(client, target, columns, fetched, batch_size)
                rows += len(fetched)
                last_seen = fetched[-1][0]
    return rows, batches


def _run_stream(columns, target, batch_size, fetch_size, first, last):
    rows = batches = 0
    where = "WHERE etl_row_num BETWEEN %s AND %s" if first is not None else ""
    parameters = (first, last) if first is not None else None
    query = select_sql(columns, where)
    with clickhouse() as client, psycopg.connect(PG_DSN) as connection:
        with connection.cursor() as cursor:
            for fetched in _fetch_batches(cursor.stream(query, parameters, size=fetch_size), fetch_size):
                batches += _write_batches(client, target, columns, fetched, batch_size)
                rows += len(fetched)
    return rows, batches


def _run_copy(columns, target, batch_size, fetch_size, first, last):
    rows = batches = 0
    where = "WHERE etl_row_num BETWEEN %s AND %s" if first is not None else ""
    parameters = (first, last) if first is not None else None
    query = f"COPY ({select_sql(columns, where)}) TO STDOUT WITH (FORMAT BINARY)"
    types = ["int8" if column == "etl_row_num" else "text" for column in columns]

    with clickhouse() as client, psycopg.connect(PG_DSN) as connection:
        with connection.cursor() as cursor, cursor.copy(query, parameters) as copy:
            copy.set_types(types)
            for fetched in _fetch_batches(copy.rows(), fetch_size):
                batches += _write_batches(client, target, columns, fetched, batch_size)
                rows += len(fetched)
    return rows, batches


def _fetch_batches(rows, fetch_size):
    while True:
        with span("pg_fetch"):
            fetched = list(islice(rows, fetch_size))
        if not fetched:
            return
        yield fetched
