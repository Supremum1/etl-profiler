import csv
import json
import re
from pathlib import Path

import psycopg

from etlbench.common import PG_DSN, PG_SCHEMA, SOURCE, SOURCE_INFO_FILE, event, quote, source_key


def save_source_info():
    # Only preparation scans the source. Benchmark runs read the JSON file.
    with psycopg.connect(PG_DSN) as connection:
        cursor = connection.execute(f"SELECT * FROM {SOURCE} LIMIT 0")
        columns = [column.name for column in cursor.description]
        rows, first, last = connection.execute(
            f"SELECT count(*), min(etl_row_num), max(etl_row_num) FROM {SOURCE}"
        ).fetchone()
    info = dict(source_key=source_key(), columns=columns, rows=rows, first=first, last=last)
    temporary = SOURCE_INFO_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(SOURCE_INFO_FILE)
    event("source_info_saved", file=str(SOURCE_INFO_FILE), rows=rows)


def run(filename):
    path = Path(filename)
    with psycopg.connect(PG_DSN) as connection:
        if connection.execute("SELECT to_regclass(%s)", (SOURCE,)).fetchone()[0]:
            event("source_reused", table=SOURCE)
            save_source_info()
            return
        with path.open(encoding="utf-8-sig", newline="") as file:
            headers = next(csv.reader(file))
        columns, used = [], {"etl_row_num"}
        for number, header in enumerate(headers, 1):
            base = re.sub(r"[^a-z0-9]+", "_", header.lower()).strip("_") or f"column_{number}"
            name = base
            suffix = 2
            while name in used:
                name = f"{base}_{suffix}"
                suffix += 1
            used.add(name)
            columns.append(name)
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(PG_SCHEMA)}")
        definitions = ", ".join(f"{quote(name)} text" for name in columns)
        connection.execute(
            f"CREATE TABLE {SOURCE} "
            f"(etl_row_num bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, {definitions})"
        )
        sql = f"COPY {SOURCE} ({', '.join(map(quote, columns))}) FROM STDIN WITH (FORMAT csv, HEADER true)"
        removed_nul_bytes = 0
        with path.open("rb") as file, connection.cursor() as cursor, cursor.copy(sql) as copy:
            while chunk := file.read(4 * 1024 * 1024):
                removed_nul_bytes += chunk.count(b"\x00")
                copy.write(chunk.replace(b"\x00", b""))
        connection.execute(f"ANALYZE {SOURCE}")
    event("source_prepared", table=SOURCE, removed_nul_bytes=removed_nul_bytes)
    save_source_info()
