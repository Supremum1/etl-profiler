from concurrent.futures import ThreadPoolExecutor
from threading import get_native_id

from psycopg.conninfo import conninfo_to_dict

from etlbench.common import PG_DSN, PG_SCHEMA, PG_TABLE, clickhouse, key_ranges, quote
from etlbench.profiling.stages import span


def literal(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def run(columns, target, batch_size, fetch_size=None, workers=1, first=None, last=None, reader=None):
    pg = conninfo_to_dict(PG_DSN)
    schema, table = PG_SCHEMA, PG_TABLE
    parameters = (
        pg.get("host", "localhost") + ":" + pg.get("port", "5432"),
        pg["dbname"], table, pg["user"], pg.get("password", ""), schema,
    )
    names = ", ".join(map(quote, columns))
    values = ", ".join(
        "toUInt64(etl_row_num)" if name == "etl_row_num" else quote(name)
        for name in columns
    )
    def insert(source_args, lane=None):
        source = "postgresql(" + ", ".join(source_args) + ")"
        with clickhouse() as client, span("server_insert", lane=lane):
            client.execute(
                f"INSERT INTO {target} ({names}) SELECT {values} FROM {source} ORDER BY etl_row_num"
            )

    if workers == 1:
        insert(list(map(literal, parameters)))
        return None, 1

    ranges = list(key_ranges(first, last, workers))
    if not ranges:
        return 0, 0

    def insert_range(bounds):
        start, end = bounds
        query = (
            f"SELECT {names} FROM {quote(schema)}.{quote(table)} "
            f"WHERE etl_row_num BETWEEN {start} AND {end}"
        )
        host, database, _, user, password, _ = parameters
        # Put the range inside the PostgreSQL query. Outer filters on query(...) stay in ClickHouse.
        insert([literal(host), literal(database), f"query({literal(query)})",
                literal(user), literal(password)], lane=get_native_id())

    with ThreadPoolExecutor(max_workers=len(ranges)) as pool:
        for _ in pool.map(insert_range, ranges):
            pass
    return None, len(ranges)  # The runner counts the result outside the timed region.
