"""Low-overhead PostgreSQL and optional Docker sampling for benchmark runs."""
from __future__ import annotations

import http.client
import json
import os
import socket
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote

import psycopg
from psycopg.rows import dict_row


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str, timeout: float | None = None):
        super().__init__("localhost", timeout=timeout)
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.socket_path)


class PostgresProfiler:
    """Samples cumulative counters; the caller calculates run deltas from them."""

    def __init__(
        self,
        dsn: str,
        schema: str,
        interval_seconds: float = 1.0,
        table: str = "fact_animals",
    ) -> None:
        self.dsn = dsn
        self.schema = schema
        self.table = table
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, Any]] = []
        self.settings: dict[str, Any] = {}
        self.docker_container: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = time.perf_counter()

    def start(self) -> None:
        try:
            self._load_capabilities()
        except Exception as error:
            self.samples.append(
                {
                    "elapsed_seconds": round(time.perf_counter() - self._started, 6),
                    "sample_error": f"{type(error).__name__}: {error}",
                }
            )
            return
        self._thread = threading.Thread(target=self._run, name="postgres-profiler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval_seconds + 2)
            self._final_sample()

    def save(
        self,
        output: Path,
        run_id: str,
        pipeline: str,
        run_settings: dict[str, Any] | None = None,
    ) -> str | None:
        """Save a profile without allowing an observability failure to fail ETL."""
        try:
            output.parent.mkdir(parents=True, exist_ok=True)
            summary = self._summary()
            payload = {
                "schema_version": 2,
                "run_id": run_id,
                "pipeline": pipeline,
                "run_settings": run_settings or {},
                "interval_seconds": self.interval_seconds,
                "settings": self.settings,
                "docker_container": self.docker_container,
                "summary": summary,
                "samples": self.samples,
            }
            output.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n",
                encoding="utf-8",
            )
            self.summary_path(output).write_text(
                _text_report(run_id, pipeline, run_settings or {}, summary["human_readable"]),
                encoding="utf-8",
            )
        except Exception as error:
            return f"{type(error).__name__}: {error}"
        return None

    def human_summary(self) -> dict[str, Any]:
        """Return the compact report also stored in the profile JSON."""
        return self._summary().get("human_readable", {})

    @staticmethod
    def summary_path(output: Path) -> Path:
        return output.with_name(output.stem + ".summary.txt")

    def _connection(self):
        return psycopg.connect(
            self.dsn,
            autocommit=True,
            row_factory=dict_row,
            application_name="etlbench-profiler",
        )

    def _load_capabilities(self) -> None:
        with self._connection() as connection:
            row = connection.execute(
                """SELECT current_setting('server_version') AS server_version,
                          current_setting('track_io_timing') AS track_io_timing,
                          current_setting('shared_buffers') AS shared_buffers,
                          current_setting('work_mem') AS work_mem,
                          EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements')
                              AS pg_stat_statements"""
            ).fetchone()
        self.settings = dict(row)
        self.docker_container = self._find_docker_container()

    def _run(self) -> None:
        try:
            with self._connection() as connection:
                while not self._stop.is_set():
                    self._append_sample(connection)
                    self._stop.wait(self.interval_seconds)
        except Exception as error:  # Profiling must never fail an ETL run.
            self.samples.append(
                {
                    "elapsed_seconds": round(time.perf_counter() - self._started, 6),
                    "sample_error": f"{type(error).__name__}: {error}",
                }
            )

    def _final_sample(self) -> None:
        """Capture counters after workers close and publish their final stats."""
        try:
            with self._connection() as connection:
                self._append_sample(connection)
        except Exception as error:
            self.samples.append(
                {
                    "elapsed_seconds": round(time.perf_counter() - self._started, 6),
                    "sample_error": f"{type(error).__name__}: {error}",
                }
            )

    def _append_sample(self, connection) -> None:
        sample = self._postgres_sample(connection)
        docker = self._docker_sample()
        if docker is not None:
            sample["docker"] = docker
        self.samples.append(sample)

    def _summary(self) -> dict[str, Any]:
        postgres_samples = [sample["postgres"] for sample in self.samples if "postgres" in sample]
        docker_samples = [sample["docker"] for sample in self.samples if "docker" in sample]
        summary: dict[str, Any] = {"samples": len(postgres_samples)}
        if postgres_samples:
            timed_samples = [
                sample for sample in self.samples if isinstance(sample.get("elapsed_seconds"), (int, float))
            ]
            if timed_samples:
                summary["elapsed_seconds"] = round(
                    timed_samples[-1]["elapsed_seconds"] - timed_samples[0]["elapsed_seconds"], 6
                )
            summary["postgres_delta"] = _counter_delta(postgres_samples[0], postgres_samples[-1])
        if docker_samples:
            summary["docker_delta"] = _counter_delta(
                docker_samples[0], docker_samples[-1],
                {
                    "cpu_total_usage", "cpu_system_usage", "cpu_throttled_time",
                    "network_rx_bytes", "network_tx_bytes",
                },
            )
            summary["docker_gauges"] = {
                name: _gauge_summary(docker_samples, name)
                for name in ("memory_usage", "memory_limit", "memory_cache", "pids_current")
                if _gauge_summary(docker_samples, name) is not None
            }
            summary["docker_block_io_bytes"] = _block_io_delta(
                docker_samples[0].get("blkio", []), docker_samples[-1].get("blkio", [])
            )
        waits: dict[str, int] = {}
        for sample in self.samples:
            for activity in sample.get("activity", []):
                key = "|".join(
                    str(activity.get(name) or "")
                    for name in ("state", "wait_event_type", "wait_event")
                )
                waits[key] = waits.get(key, 0) + int(activity["backends"])
        summary["activity_backend_observations"] = waits
        summary["human_readable"] = _human_summary(summary)
        return summary

    def _postgres_sample(self, connection) -> dict[str, Any]:
        counters = connection.execute(
            """SELECT d.blks_read, d.blks_hit, d.tup_returned, d.tup_fetched,
                      d.temp_files, d.temp_bytes,
                      t.seq_scan, t.idx_scan, t.seq_tup_read, t.idx_tup_fetch,
                      io.heap_blks_read, io.heap_blks_hit,
                      COALESCE(ix.idx_blks_read, 0) AS index_blks_read,
                      COALESCE(ix.idx_blks_hit, 0) AS index_blks_hit,
                      bg.checkpoints_timed, bg.checkpoints_req,
                      bg.buffers_checkpoint, bg.buffers_clean, bg.buffers_backend,
                      bg.buffers_backend_fsync, bg.buffers_alloc,
                      wal.wal_records, wal.wal_fpi, wal.wal_bytes, wal.wal_buffers_full,
                      wal.wal_write, wal.wal_sync, wal.wal_write_time, wal.wal_sync_time
               FROM pg_stat_database d
               LEFT JOIN pg_stat_user_tables t
                 ON t.schemaname=%s AND t.relname=%s
               LEFT JOIN pg_statio_user_tables io
                 ON io.schemaname=t.schemaname AND io.relname=t.relname
               LEFT JOIN LATERAL (
                   SELECT sum(idx_blks_read) AS idx_blks_read, sum(idx_blks_hit) AS idx_blks_hit
                   FROM pg_statio_user_indexes
                   WHERE schemaname=%s AND relname=%s
               ) ix ON true
               CROSS JOIN pg_stat_bgwriter bg
               CROSS JOIN pg_stat_wal wal
               WHERE d.datname=current_database()""",
            (self.schema, self.table, self.schema, self.table),
        ).fetchone()
        activity = connection.execute(
            """SELECT state, wait_event_type, wait_event, count(*) AS backends
               FROM pg_stat_activity
               WHERE backend_type='client backend'
                 AND application_name <> 'etlbench-profiler'
               GROUP BY state, wait_event_type, wait_event
               ORDER BY backends DESC"""
        ).fetchall()
        sample = {
            "elapsed_seconds": round(time.perf_counter() - self._started, 6),
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "postgres": dict(counters),
            "activity": [dict(row) for row in activity],
        }
        if self.settings.get("pg_stat_statements"):
            sample["statement"] = self._statement_sample(connection)
        return sample

    def _statement_sample(self, connection) -> dict[str, Any] | None:
        """Aggregate source SELECT statements without including profiler queries."""
        try:
            row = connection.execute(
                """SELECT COALESCE(sum(calls), 0) AS calls,
                          COALESCE(sum(total_exec_time), 0) AS total_exec_time,
                          COALESCE(sum(rows), 0) AS rows,
                          COALESCE(sum(shared_blks_hit), 0) AS shared_blks_hit,
                          COALESCE(sum(shared_blks_read), 0) AS shared_blks_read,
                          COALESCE(sum(temp_blks_read), 0) AS temp_blks_read,
                          COALESCE(sum(temp_blks_written), 0) AS temp_blks_written,
                          COALESCE(sum(blk_read_time), 0) AS blk_read_time,
                          COALESCE(sum(blk_write_time), 0) AS blk_write_time
                   FROM pg_stat_statements
                   WHERE dbid = (SELECT oid FROM pg_database WHERE datname=current_database())
                     AND query LIKE 'SELECT "etl_row_num"%'
                     AND query LIKE '%fact_animals%'"""
            ).fetchone()
            return dict(row)
        except Exception as error:
            return {"error": f"{type(error).__name__}: {error}"}

    def _docker_request(self, path: str) -> Any:
        connection = UnixHTTPConnection("/var/run/docker.sock")
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            if response.status != 200:
                return None
            return json.loads(response.read())
        finally:
            connection.close()

    def _find_docker_container(self) -> str | None:
        if not Path("/var/run/docker.sock").exists():
            return None
        explicit = os.getenv("PROFILER_POSTGRES_CONTAINER")
        if explicit:
            return explicit
        containers = self._docker_request("/containers/json?all=1") or []
        postgres = [
            item for item in containers
            if item.get("Labels", {}).get("com.docker.compose.service") == "postgres"
        ]
        return postgres[0]["Id"] if len(postgres) == 1 else None

    def _docker_sample(self) -> dict[str, Any] | None:
        if not self.docker_container:
            return None
        try:
            stats = self._docker_request(
                "/containers/" + quote(self.docker_container, safe="") + "/stats?stream=false"
            )
            if not stats:
                return None
            networks = stats.get("networks", {})
            return {
                "cpu_total_usage": stats.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage"),
                "cpu_system_usage": stats.get("cpu_stats", {}).get("system_cpu_usage"),
                "cpu_throttled_time": stats.get("cpu_stats", {}).get("throttling_data", {}).get("throttled_time"),
                "memory_usage": stats.get("memory_stats", {}).get("usage"),
                "memory_limit": stats.get("memory_stats", {}).get("limit"),
                "memory_cache": stats.get("memory_stats", {}).get("stats", {}).get("cache"),
                "pids_current": stats.get("pids_stats", {}).get("current"),
                "blkio": stats.get("blkio_stats", {}).get("io_service_bytes_recursive", []),
                "network_rx_bytes": sum(item.get("rx_bytes", 0) for item in networks.values()),
                "network_tx_bytes": sum(item.get("tx_bytes", 0) for item in networks.values()),
            }
        except Exception:
            return None


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return str(value)


def _counter_delta(
    first: dict[str, Any],
    last: dict[str, Any],
    keys: set[str] | None = None,
) -> dict[str, Any]:
    return {
        key: last[key] - value
        for key, value in first.items()
        if (keys is None or key in keys)
        and key in last
        and isinstance(value, (int, float, Decimal))
        and isinstance(last[key], (int, float, Decimal))
    }


def _gauge_summary(samples: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    values = [sample.get(name) for sample in samples]
    values = [value for value in values if isinstance(value, (int, float, Decimal))]
    if not values:
        return None
    return {
        "start": values[0],
        "peak": max(values),
        "end": values[-1],
        "change": values[-1] - values[0],
    }


def _block_io_delta(first: list[dict[str, Any]], last: list[dict[str, Any]]) -> dict[str, Any]:
    def totals(items):
        result: dict[str, Any] = {}
        for item in items:
            operation = str(item.get("op", "unknown")).lower()
            value = item.get("value")
            if isinstance(value, (int, float, Decimal)):
                result[operation] = result.get(operation, 0) + value
        return result

    before, after = totals(first), totals(last)
    return {operation: value - before.get(operation, 0) for operation, value in after.items()}


def _format_bytes(value: Any) -> str:
    if not isinstance(value, (int, float, Decimal)):
        return "недоступно"
    size = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    unit = units[0]
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            break
        size /= 1024
    return f"{size:.2f} {unit}"


def _human_summary(summary: dict[str, Any]) -> dict[str, Any]:
    elapsed = float(summary.get("elapsed_seconds") or 0)
    postgres = summary.get("postgres_delta", {})
    docker = summary.get("docker_delta", {})
    gauges = summary.get("docker_gauges", {})
    block_io = summary.get("docker_block_io_bytes", {})
    memory = gauges.get("memory_usage", {})
    memory_limit = gauges.get("memory_limit", {})
    pids = gauges.get("pids_current", {})
    cpu_seconds = float(docker.get("cpu_total_usage", 0)) / 1_000_000_000
    temp_files = int(postgres.get("temp_files", 0))
    temp_bytes = postgres.get("temp_bytes", 0)
    read_blocks = postgres.get("blks_read", 0)
    waits = summary.get("activity_backend_observations", {})
    wait_total = sum(waits.values())
    dominant_wait = max(waits, key=waits.get) if waits else None
    wait_distribution = {
        _wait_label(key): {
            "backend_observations": count,
            "share": f"{100 * count / wait_total:.1f}%" if wait_total else "0.0%",
        }
        for key, count in sorted(waits.items(), key=lambda item: item[1], reverse=True)
    }

    interpretations = []
    if temp_files:
        interpretations.append(
            f"PostgreSQL создал {temp_files} временных файлов ({_format_bytes(temp_bytes)})."
        )
    else:
        interpretations.append("PostgreSQL не создавал временных файлов.")
    if dominant_wait and "ClientWrite" in dominant_wait:
        share = 100 * waits[dominant_wait] / wait_total if wait_total else 0
        interpretations.append(
            f"В {share:.1f}% наблюдений backend PostgreSQL ждал чтения со стороны ETL; "
            "это downstream backpressure, а не само по себе узкое место PostgreSQL."
        )
    elif dominant_wait and "ClientRead" in dominant_wait:
        share = 100 * waits[dominant_wait] / wait_total if wait_total else 0
        interpretations.append(
            f"В {share:.1f}% наблюдений backend PostgreSQL ждал следующую команду ETL-клиента."
        )
    if docker.get("cpu_throttled_time", 0):
        interpretations.append("Контейнер ограничивался CPU throttling.")
    else:
        interpretations.append("CPU throttling не наблюдался.")

    return {
        "duration": f"{elapsed:.2f} s",
        "postgres": {
            "temporary_files": f"{temp_files} files / {_format_bytes(temp_bytes)}",
            "database_blocks_read": f"{read_blocks} blocks / {_format_bytes(read_blocks * 8192)}",
            "index_scans": int(postgres.get("idx_scan", 0)),
        },
        "docker": {
            "postgres_cpu": f"{cpu_seconds:.2f} CPU-s / {cpu_seconds / elapsed if elapsed else 0:.2f} cores avg",
            "memory": (
                f"start {_format_bytes(memory.get('start'))}, "
                f"peak {_format_bytes(memory.get('peak'))}, "
                f"end {_format_bytes(memory.get('end'))}, "
                f"limit {_format_bytes(memory_limit.get('end'))}"
            ),
            "pids": (
                f"start {pids.get('start', 'недоступно')}, "
                f"peak {pids.get('peak', 'недоступно')}, "
                f"end {pids.get('end', 'недоступно')}"
            ),
            "block_io": (
                f"read {_format_bytes(block_io.get('read', 0))}, "
                f"write {_format_bytes(block_io.get('write', 0))}"
            ),
            "network": (
                f"rx {_format_bytes(docker.get('network_rx_bytes', 0))}, "
                f"tx {_format_bytes(docker.get('network_tx_bytes', 0))}"
            ),
        },
        "dominant_wait": dominant_wait or "нет наблюдений",
        "wait_distribution": wait_distribution,
        "interpretation": interpretations,
    }


def _wait_label(key: str) -> str:
    labels = {
        "active|Client|ClientWrite": "ожидание чтения со стороны ETL (ClientWrite)",
        "idle in transaction|Client|ClientRead": "ожидание команды ETL (ClientRead)",
        "active|IO|DataFileRead": "чтение файла данных (DataFileRead)",
        "active|IO|BufFileWrite": "запись временного файла (BufFileWrite)",
        "active||": "активная работа CPU",
    }
    return labels.get(key, key)


def _text_report(
    run_id: str,
    pipeline: str,
    run_settings: dict[str, Any],
    report: dict[str, Any],
) -> str:
    postgres = report["postgres"]
    docker = report.get("docker", {})
    parameters = ", ".join(f"{name}={value}" for name, value in run_settings.items()) or "нет"
    interpretations = "\n".join(f"- {line}" for line in report.get("interpretation", []))
    return (
        "Профиль PostgreSQL ETL\n"
        f"Run ID: {run_id}\n"
        f"Pipeline: {pipeline}\n"
        f"Параметры: {parameters}\n"
        f"Продолжительность профиля: {report['duration']}\n\n"
        "PostgreSQL\n"
        f"- Временные файлы: {postgres['temporary_files']}\n"
        f"- Прочитано блоков БД: {postgres['database_blocks_read']}\n"
        f"- Индексных сканирований: {postgres['index_scans']}\n"
        f"- Основное ожидание: {report['dominant_wait']}\n\n"
        "Распределение наблюдений backend\n"
        + "".join(
            f"- {name}: {value['share']} ({value['backend_observations']})\n"
            for name, value in report.get("wait_distribution", {}).items()
        )
        + "\n"
        "Docker-контейнер PostgreSQL\n"
        f"- CPU: {docker.get('postgres_cpu', 'недоступно')}\n"
        f"- Память: {docker.get('memory', 'недоступно')}\n"
        f"- Процессы: {docker.get('pids', 'недоступно')}\n"
        f"- Block I/O: {docker.get('block_io', 'недоступно')}\n"
        f"- Сеть: {docker.get('network', 'недоступно')}\n\n"
        "Интерпретация\n"
        f"{interpretations or '- Недостаточно данных для интерпретации.'}\n"
    )
