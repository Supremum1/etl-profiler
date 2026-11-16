import asyncio

import psycopg
from asynch import Connection

from etlbench.common import CH, PG_DSN, insert_sql, select_sql
from etlbench.profiling.stages import span


async def run_tasks(*coroutines):
    # On Python 3.10 gather does not cancel the sibling after a failure.
    tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def run(columns, target, batch_size, fetch_size, workers=1, first=None, last=None, reader="stream"):
    queue = asyncio.Queue(maxsize=2)
    rows = batches = 0

    async def producer():
        await produce(queue, columns, reader, first, last, fetch_size, batch_size)

    async def writer():
        nonlocal rows, batches
        async with Connection(**CH, compression=False) as client:
            while (batch := await queue.get()) is not None:
                with span("python_columns"):
                    columnar = [list(column) for column in zip(*batch)]
                with span("ch_insert"):
                    await client._connection.execute(
                        insert_sql(target, columns), columnar, columnar=True,
                        settings={"insert_block_size": len(batch)},
                    )
                rows += len(batch)
                batches += 1

    await run_tasks(producer(), writer())
    return rows, batches


async def produce(queue, columns, reader, first, last, fetch_size, batch_size):
    async with await psycopg.AsyncConnection.connect(PG_DSN) as connection:
        async for fetched in read_batches(connection, columns, reader, first, last, fetch_size):
            for start in range(0, len(fetched), batch_size):
                with span("queue_put"):
                    await queue.put(fetched[start : start + batch_size])
    await queue.put(None)


async def read_batches(connection, columns, reader, first, last, fetch_size):
    if reader == "cursor":
        async with connection.cursor(name="source") as cursor:
            with span("pg_fetch"):
                await cursor.execute(select_sql(columns))
            while True:
                with span("pg_fetch"):
                    fetched = await cursor.fetchmany(fetch_size)
                if not fetched:
                    return
                yield fetched
    elif reader == "keyset":
        last_seen = first - 1 if first is not None else -1
        where = "WHERE etl_row_num > %s"
        if last is not None:
            where += " AND etl_row_num <= %s"
        query = select_sql(columns, where) + " LIMIT %s"
        async with connection.cursor() as cursor:
            while True:
                parameters = ((last_seen, last, fetch_size) if last is not None
                              else (last_seen, fetch_size))
                with span("pg_fetch"):
                    await cursor.execute(query, parameters)
                    fetched = await cursor.fetchmany(fetch_size)
                if not fetched:
                    return
                last_seen = fetched[-1][0]
                yield fetched
    else:
        where = "WHERE etl_row_num BETWEEN %s AND %s" if first is not None else ""
        parameters = (first, last) if first is not None else None
        async with connection.cursor() as cursor:
            stream = cursor.stream(select_sql(columns, where), parameters, size=fetch_size)
            finished = False
            while not finished:
                fetched = []
                with span("pg_fetch"):
                    for _ in range(fetch_size):
                        try:
                            fetched.append(await anext(stream))
                        except StopAsyncIteration:
                            finished = True
                            break
                if fetched:
                    yield fetched
