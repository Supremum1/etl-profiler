"""Direct PostgreSQL COPY CSV to ClickHouse HTTP streaming."""
import http.client
import os
from urllib.parse import urlencode

import psycopg

from etlbench.common import CH, PG_DSN, quote, select_sql

from etlbench.profiling.stages import span

HTTP_CHUNK_SIZE = 1024 * 1024 * 2


def run(columns, target, first, last):
    where = "WHERE etl_row_num BETWEEN %s AND %s" if first is not None else ""
    parameters = (first, last) if first is not None else None
    copy_sql = (
        f"COPY ({select_sql(columns, where)}) TO STDOUT "
        "WITH (FORMAT CSV, NULL '\\N', HEADER false, ENCODING 'UTF8')"
    )
    names = ", ".join(map(quote, columns))
    insert_sql = f"INSERT INTO {target} ({names}) FORMAT CSV"
    query = urlencode(
        {
            "database": CH["database"],
            "query": insert_sql,
            "format_csv_null_representation": "\\N",
        }
    )
    headers = {
        "Content-Type": "application/octet-stream",
        "X-ClickHouse-User": CH["user"],
        "X-ClickHouse-Key": CH["password"],
    }
    http_connection = http.client.HTTPConnection(
        CH["host"], int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    )

    try:
        with psycopg.connect(PG_DSN) as connection, connection.cursor() as cursor:
            with cursor.copy(copy_sql, parameters) as copy:
                with span("copy_relay"):
                    http_connection.request(
                        "POST", "/?" + query, body=_chunks(copy),
                        headers=headers, encode_chunked=True,
                    )
        with span("insert_ack"):
            response = http_connection.getresponse()
            message = response.read()
        if response.status != 200:
            raise RuntimeError(
                f"ClickHouse HTTP {response.status}: {message.decode('utf-8', errors='replace')}"
            )
    finally:
        http_connection.close()

    # Counting CSV records would require parsing quoted newlines. bench.py verifies
    # the final ClickHouse row count, so this path deliberately remains byte-only.
    return None, 1


def _chunks(copy, chunk_size=HTTP_CHUNK_SIZE):
    buffer = bytearray()
    while chunk := copy.read():
        buffer.extend(chunk)
        if len(buffer) >= chunk_size:
            yield bytes(buffer)
            buffer.clear()
    if buffer:
        yield bytes(buffer)
