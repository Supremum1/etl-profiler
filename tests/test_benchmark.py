"""Run integration checks against local databases with RUN_DB_TESTS=1."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid
from decimal import Decimal
from unittest.mock import patch, MagicMock

from etlbench import common, prepare
from etlbench.pipelines.async_pipeline import run_tasks
from etlbench.pipelines.copy_direct import _chunks
from etlbench.profiling.postgres_profiler import PostgresProfiler


class SourceInfoTests(unittest.TestCase):
    def test_save_and_read_without_database_access(self):
        for rows, first, last in ((4, 1, 5), (0, None, None)):
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "source_info.json"
                connection = MagicMock()
                cursor = MagicMock()
                cursor.description = [MagicMock(), MagicMock()]
                cursor.description[0].name = "etl_row_num"
                cursor.description[1].name = "name"
                counts = MagicMock()
                counts.fetchone.return_value = (rows, first, last)
                connection.execute.side_effect = [cursor, counts]
                with patch.object(prepare, "SOURCE_INFO_FILE", path), patch.object(common, "SOURCE_INFO_FILE", path):
                    with patch.object(prepare.psycopg, "connect") as connect:
                        connect.return_value.__enter__.return_value = connection
                        prepare.save_source_info()
                    with patch.object(common.psycopg, "connect", side_effect=AssertionError("Unexpected database access")):
                        self.assertEqual(common.source_info(), (["etl_row_num", "name"], rows, first, last))
                    with patch.object(common, "PG_DSN", "different-source"):
                        with self.assertRaisesRegex(RuntimeError, "settings changed"):
                            common.source_info()

    def test_missing_file_does_not_scan_source(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(common, "SOURCE_INFO_FILE", Path(directory) / "missing.json"):
                with patch.object(common.psycopg, "connect", side_effect=AssertionError("Unexpected database access")):
                    with self.assertRaisesRegex(RuntimeError, "source-info"):
                        common.source_info()


class TaskTests(unittest.TestCase):
    def test_copy_chunks_are_coalesced(self):
        copy = MagicMock()
        copy.read.side_effect = [b"abc", b"def", b"ghi", b""]
        self.assertEqual(list(_chunks(copy, chunk_size=5)), [b"abcdef", b"ghi"])

    def test_failed_writer_cancels_waiting_producer(self):
        async def scenario():
            cancelled = asyncio.Event()

            async def producer():
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

            async def writer():
                raise ValueError("failed insert")

            with self.assertRaises(ValueError):
                await run_tasks(producer(), writer())
            self.assertTrue(cancelled.is_set())

        asyncio.run(scenario())


class ProfilerTests(unittest.TestCase):
    def test_save_serializes_decimal_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profile.json"
            profiler = PostgresProfiler("unused", "etl_public")
            profiler.samples = [{"postgres": {"wal_bytes": Decimal("12.5")}}]
            self.assertIsNone(profiler.save(output, "run", "sync"))
            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["samples"][0]["postgres"]["wal_bytes"], 12.5)
            self.assertIn("Профиль PostgreSQL ETL", profiler.summary_path(output).read_text(encoding="utf-8"))

    def test_summary_distinguishes_counters_from_gauges(self):
        profiler = PostgresProfiler("unused", "etl_public")
        profiler.samples = [
            {
                "elapsed_seconds": 0.0,
                "postgres": {"temp_files": 10, "temp_bytes": 100, "blks_read": 20, "idx_scan": 1},
                "docker": {
                    "cpu_total_usage": 1_000_000_000, "cpu_system_usage": 10,
                    "cpu_throttled_time": 0, "memory_usage": 200, "memory_limit": 1000,
                    "pids_current": 5, "network_rx_bytes": 10, "network_tx_bytes": 20,
                    "blkio": [{"op": "write", "value": 100}],
                },
                "activity": [{"state": "active", "wait_event_type": "Client", "wait_event": "ClientWrite", "backends": 3}],
            },
            {
                "elapsed_seconds": 2.0,
                "postgres": {"temp_files": 12, "temp_bytes": 300, "blks_read": 30, "idx_scan": 2},
                "docker": {
                    "cpu_total_usage": 2_000_000_000, "cpu_system_usage": 20,
                    "cpu_throttled_time": 0, "memory_usage": 150, "memory_limit": 1000,
                    "pids_current": 4, "network_rx_bytes": 15, "network_tx_bytes": 30,
                    "blkio": [{"op": "write", "value": 140}],
                },
                "activity": [{"state": "active", "wait_event_type": None, "wait_event": None, "backends": 1}],
            },
        ]
        summary = profiler._summary()
        self.assertNotIn("memory_usage", summary["docker_delta"])
        self.assertEqual(summary["docker_gauges"]["memory_usage"]["change"], -50)
        self.assertEqual(summary["docker_gauges"]["memory_limit"]["end"], 1000)
        self.assertEqual(summary["docker_block_io_bytes"]["write"], 40)
        self.assertIn("2 files", summary["human_readable"]["postgres"]["temporary_files"])
        waits = summary["human_readable"]["wait_distribution"]
        self.assertEqual(waits["ожидание чтения со стороны ETL (ClientWrite)"]["share"], "75.0%")


@unittest.skipUnless(os.getenv("RUN_DB_TESTS") == "1", "requires local databases and native build")
class DatabaseTests(unittest.TestCase):
    def test_all_pipelines(self):
        import psycopg
        from etlbench.common import PG_DSN, CH, quote
        from clickhouse_driver import Client
        from _writer_core import WriterCore

        core = WriterCore(1)
        core.write_strings(["кот", "", b"dog"])
        self.assertEqual(core.take(), b"\x06" + "кот".encode() + b"\x00\x03dog")
        from asynch.proto.columns.stringcolumn import String
        from asynch.proto.streams.buffered import BufferedWriter
        from etlbench.native.native_writer import NativeWriter

        class Sink:
            def __init__(self):
                self.data = bytearray()

            def write(self, data):
                self.data.extend(data)

            async def drain(self):
                pass

        async def encode(writer_type):
            sink = Sink()
            writer = writer_type(sink, max_buffer_size=8)
            column = String(None, writer)
            column.nullable = True
            await column.write_data(["кот", None, "", "dog"])
            await writer.flush()
            return sink.data

        self.assertEqual(asyncio.run(encode(BufferedWriter)), asyncio.run(encode(NativeWriter)))
        schema = "etl_test_" + uuid.uuid4().hex
        environment = dict(os.environ, POSTGRES_SCHEMA=schema)
        directory = Path(__file__).resolve().parents[1]
        targets = []

        def command(*arguments):
            result = subprocess.run(
                [sys.executable, str(directory / "bench.py"), *arguments],
                env=environment, capture_output=True, text=True, timeout=90,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            for line in result.stderr.splitlines():
                event = json.loads(line)
                if event["event"] == "started":
                    targets.append("etl_target_" + event["run_id"])
            return result

        try:
            with tempfile.TemporaryDirectory() as temporary:
                environment["SOURCE_INFO_FILE"] = str(Path(temporary) / "source_info.json")
                csv = Path(temporary) / "source.csv"
                csv.write_text('name,region\ncat,north\n,south\n"",""\nкот,east\ndog,west\n', encoding="utf-8")
                command("prepare", str(csv))
                command("prepare", str(csv))  # Reuse must not duplicate rows.
                # Test holes in IDs: partition by key ranges, not count(rows).
                with psycopg.connect(PG_DSN) as connection:
                    connection.execute(f"DELETE FROM {quote(schema)}.fact_animals WHERE etl_row_num = 2")
                command("source-info")
                for mode in ("sync", "async", "hybrid", "parallel", "server-side"):
                    result = command(mode, "--batch-size", "2")
                    self.assertEqual(json.loads(result.stdout)["rows"], 4)
                for mode in ("sync", "parallel"):
                    result = command(mode, "--reader", "copy", "--batch-size", "2")
                    self.assertEqual(json.loads(result.stdout)["rows"], 4)
                    result = command(mode, "--reader", "copy-direct", "--batch-size", "2")
                    self.assertEqual(json.loads(result.stdout)["rows"], 4)
                result = command("async", "--driver", "patched", "--batch-size", "2")
                self.assertEqual(json.loads(result.stdout)["rows"], 4)
                with psycopg.connect(PG_DSN) as connection:
                    connection.execute(f"DELETE FROM {quote(schema)}.fact_animals")
                command("source-info")
                self.assertEqual(json.loads(command("parallel").stdout)["rows"], 0)
            client = Client(**CH)
            try:
                for target in targets:
                    self.assertEqual(client.execute(f"EXISTS TABLE {target}")[0][0], 0)
            finally:
                client.disconnect()
        finally:
            with psycopg.connect(PG_DSN) as connection:
                connection.execute(f"DROP SCHEMA IF EXISTS {quote(schema)} CASCADE")


if __name__ == "__main__":
    unittest.main()
