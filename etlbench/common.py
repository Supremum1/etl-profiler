import json
import hashlib
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import psycopg
from clickhouse_driver import Client

PG_DSN = os.getenv("POSTGRES_DSN", "postgresql://etl:etl@localhost/etl_source")
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA", "etl_public")
CH = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "9000")),
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
    "database": os.getenv("CLICKHOUSE_DATABASE", "etl_benchmark"),
}


def event(name, **fields):
    print(json.dumps({"event": name, **fields}, ensure_ascii=False), file=sys.stderr, flush=True)


def quote(name):
    return '"' + name.replace('"', '""') + '"'


PG_TABLE = os.getenv("POSTGRES_TABLE", "fact_animals")
# Keep the historical source identifier for existing default-table metadata.
TABLE_SQL = "fact_animals" if PG_TABLE == "fact_animals" else quote(PG_TABLE)
SOURCE = f"{quote(PG_SCHEMA)}.{TABLE_SQL}"
SOURCE_INFO_FILE = Path(
    os.getenv("SOURCE_INFO_FILE", str(Path(__file__).resolve().parent.parent / "source_info.json"))
)


def source_key():
    # Identify the configured source without saving its password in JSON.
    return hashlib.sha256(f"{PG_DSN}|{SOURCE}".encode()).hexdigest()


def key_ranges(first, last, workers):
    if first is None:
        return
    workers = min(workers, last - first + 1)
    width, remainder = divmod(last - first + 1, workers)
    for number in range(workers):
        end = first + width - 1 + (number < remainder)
        yield first, end
        first = end + 1


def select_sql(columns, where=""):
    names = ", ".join(map(quote, columns))
    return f"SELECT {names} FROM {SOURCE} {where} ORDER BY etl_row_num"


def insert_sql(target, columns):
    return f"INSERT INTO {target} ({', '.join(map(quote, columns))}) VALUES"


@contextmanager
def clickhouse():
    client = Client(**CH, compression=False)
    try:
        yield client
    finally:
        client.disconnect()


def source_info():
    if not SOURCE_INFO_FILE.exists():
        raise RuntimeError("Run python3 bench.py source-info before resetting caches.")
    info = json.loads(SOURCE_INFO_FILE.read_text(encoding="utf-8"))
    if info["source_key"] != source_key():
        raise RuntimeError("Source settings changed. Run python3 bench.py source-info again.")
    return info["columns"], info["rows"], info["first"], info["last"]


def create_target(client, target, columns):
    definitions = [
        f"{quote(name)} UInt64" if name == "etl_row_num"
        else f"{quote(name)} Nullable(String)"
        for name in columns
    ]
    client.execute(
        f"CREATE TABLE {target} ({', '.join(definitions)}) ENGINE=MergeTree ORDER BY etl_row_num"
    )
