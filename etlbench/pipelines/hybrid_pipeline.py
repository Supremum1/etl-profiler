import asyncio
from concurrent.futures import ThreadPoolExecutor

from clickhouse_driver import Client

from etlbench.common import CH, insert_sql
from etlbench.pipelines.async_pipeline import produce, run_tasks
from etlbench.profiling.stages import span


async def run(columns, target, batch_size, fetch_size, workers=1, first=None, last=None, reader="stream"):
    queue = asyncio.Queue(maxsize=2)
    rows = batches = 0
    client = None

    def insert(batch):
        nonlocal client
        if client is None:
            client = Client(**CH, compression=False)
        with span("python_columns"):
            columnar = [list(column) for column in zip(*batch)]
        with span("ch_insert"):
            client.execute(insert_sql(target, columns), columnar, columnar=True)

    def close():
        if client:
            client.disconnect()

    async def producer():
        await produce(queue, columns, reader, first, last, fetch_size, batch_size)

    async def writer(pool):
        nonlocal rows, batches
        loop = asyncio.get_running_loop()
        while (batch := await queue.get()) is not None:
            await loop.run_in_executor(pool, insert, batch)
            rows += len(batch)
            batches += 1

    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            await run_tasks(producer(), writer(pool))
        finally:
            await asyncio.get_running_loop().run_in_executor(pool, close)
    return rows, batches
