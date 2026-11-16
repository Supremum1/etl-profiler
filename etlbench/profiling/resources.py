"""Aligned container counters and database activity for a single ETL run."""
import json
import os
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import perf_counter
from urllib.parse import quote

import psycopg
from clickhouse_driver import Client

from etlbench.common import CH, PG_DSN
from etlbench.profiling.postgres_profiler import UnixHTTPConnection


class ResourceProfile:
    def __init__(self, interval, target):
        self.interval = interval
        self.target = target
        self.samples = []
        self.errors = {}
        self._stop = threading.Event()
        self.backend = os.getenv("RESOURCE_BACKEND", "docker")
        if self.backend == "systemd":
            from etlbench.profiling.linux_resources import LinuxResources

            linux = LinuxResources()
            self.services = dict.fromkeys(linux.units)
            self._sample_resource = linux.sample
        else:
            self.services = {
                "python": socket.gethostname(),
                "postgres": os.environ["RESOURCE_POSTGRES_CONTAINER"],
                "clickhouse": os.environ["RESOURCE_CLICKHOUSE_CONTAINER"],
            }
            self._sample_resource = lambda name: self._container(self.services[name])
        self.pg = None
        self.ch = Client(**CH, connect_timeout=2, send_receive_timeout=2)
        self.pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="etl-metrics")
        self.thread = threading.Thread(target=self._collect, name="etl-sampler", daemon=True)

    def start(self):
        self._sample()
        self.thread.start()

    def stop(self):
        self._stop.set()
        self.thread.join()
        self._sample()
        self.pool.shutdown()
        if self.pg is not None:
            self.pg.close()
        self.ch.disconnect()

    def _collect(self):
        while not self._stop.wait(self.interval):
            self._sample()

    def _sample(self):
        sources = {
            name: self.pool.submit(self._sample_resource, name)
            for name in self.services
        }
        sources["pg_activity"] = self.pool.submit(self._postgres)
        sources["ch_activity"] = self.pool.submit(self._clickhouse)
        for name, future in sources.items():
            try:
                timestamp, values = future.result()
                self.samples.append([name, timestamp, values])
            except Exception as error:
                # Keep failures visible without recording connection strings or SQL.
                self.errors[name] = type(error).__name__

    def _container(self, identifier):
        connection = UnixHTTPConnection("/var/run/docker.sock", timeout=3)
        try:
            connection.request("GET", f"/containers/{quote(identifier, safe='')}/stats?stream=false&one-shot=true")
            response = connection.getresponse()
            if response.status != 200:
                raise RuntimeError(f"Docker HTTP {response.status}")
            stats = json.loads(response.read())
        finally:
            connection.close()
        cpu = stats["cpu_stats"]
        memory = stats["memory_stats"]
        io = stats.get("blkio_stats", {}).get("io_service_bytes_recursive") or []
        network = stats.get("networks", {})
        inactive_file = memory.get("stats", {}).get(
            "inactive_file", memory.get("stats", {}).get("total_inactive_file", 0)
        )
        return perf_counter(), {
            "cpu_s": cpu["cpu_usage"]["total_usage"] / 1e9,
            "throttled_s": cpu.get("throttling_data", {}).get("throttled_time", 0) / 1e9,
            "memory": max(0, memory["usage"] - inactive_file),
            "pids": stats["pids_stats"]["current"],
            "read": sum(item["value"] for item in io if item["op"].lower() == "read") if io else None,
            "write": sum(item["value"] for item in io if item["op"].lower() == "write") if io else None,
            "rx": sum(item["rx_bytes"] for item in network.values()),
            "tx": sum(item["tx_bytes"] for item in network.values()),
        }

    def _postgres(self):
        if self.pg is None:
            self.pg = psycopg.connect(PG_DSN, autocommit=True, connect_timeout=2,
                                      options="-c statement_timeout=2000")
        waits = self.pg.execute(
            """SELECT coalesce(wait_event, 'running'), count(*)
               FROM pg_stat_activity
               WHERE datname = current_database() AND pid <> pg_backend_pid()
                     AND state = 'active'
               GROUP BY 1"""
        ).fetchall()
        read, hit, temp, size = self.pg.execute(
            """SELECT blks_read, blks_hit, temp_bytes, current_setting('block_size')::int
               FROM pg_stat_database WHERE datname = current_database()"""
        ).fetchone()
        return perf_counter(), {
            "waits": dict(waits), "buffer_read": read * size,
            "buffer_hit": hit * size, "temp": temp,
        }

    def _clickhouse(self):
        active, memory, read, written = self.ch.execute(
            """SELECT count(), sum(memory_usage), sum(read_rows), sum(written_rows)
               FROM system.processes WHERE position(query, %(target)s) > 0
                 AND query_id != currentQueryID()""",
            {"target": self.target},
        )[0]
        merges, merge_memory = self.ch.execute(
            """SELECT count(), sum(memory_usage) FROM system.merges
               WHERE database = currentDatabase() AND table = %(target)s""",
            {"target": self.target},
        )[0]
        return perf_counter(), {
            "queries": active, "query_memory": memory, "read_rows": read, "written_rows": written,
            "merges": merges, "merge_memory": merge_memory,
        }

    def save(self, path, result, started, ended, spans):
        origin = started
        payload = {
            "result": result,
            "interval": self.interval,
            "errors": self.errors,
            "resource_backend": self.backend,
            "samples": {
                name: [[round(t - origin, 6), values] for source, t, values in self.samples if source == name]
                for name in (*self.services, "pg_activity", "ch_activity")
            },
            "stages": [[stage, pid, round(a - origin, 6), round(b - origin, 6)]
                       for stage, pid, a, b in spans],
            "duration": ended - started,
        }
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        temporary.replace(path)
