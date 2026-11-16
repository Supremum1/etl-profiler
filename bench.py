import argparse
import asyncio
import importlib
import json
import os
import time
import uuid
from pathlib import Path

from etlbench.common import PG_DSN, PG_SCHEMA, PG_TABLE, clickhouse, create_target, event, source_info
from etlbench.profiling import stages
from etlbench.profiling.postgres_profiler import PostgresProfiler


def main():
    parser = argparse.ArgumentParser(description="Local PostgreSQL → ClickHouse benchmark")
    commands = parser.add_subparsers(dest="mode", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("csv")
    commands.add_parser("source-info", help="Save source metadata before cold-cache tests")
    for mode in ("sync", "async", "hybrid", "parallel", "server-side"):
        command = commands.add_parser(mode)
        command.add_argument("--batch-size", type=int, default=50_000)
        if mode != "server-side":
            reader_choices = ["stream", "keyset", "cursor"]
            if mode in ("sync", "parallel"):
                reader_choices.extend(("copy", "copy-direct"))
            command.add_argument(
                "--fetch-size",
                type=int,
                help="Rows fetched from PostgreSQL at a time; defaults to --batch-size",
            )
            command.add_argument(
                "--reader",
                choices=reader_choices,
                default="stream",
                help="PostgreSQL reader strategy; copy is available for sync and parallel",
            )
        command.add_argument("--resource-output", type=Path, help="Save container metrics and ETL stages")
        command.add_argument("--profile", action="store_true", help="Save PostgreSQL resource samples")
        command.add_argument("--profile-output", type=Path, help="JSON profile path")
        command.add_argument("--profile-interval", type=float, default=1.0)
        if mode == "parallel":
            command.add_argument("--workers", type=int, default=4)
        if mode == "server-side":
            command.add_argument("--workers", type=int, default=1,
                                 help="Concurrent ClickHouse queries reading distinct PostgreSQL ranges")
        if mode == "async":
            command.add_argument("--driver", choices=("official", "patched"), default="official")
    args = parser.parse_args()
    if args.mode == "source-info":
        from etlbench import prepare
        prepare.save_source_info()
        return
    if args.mode == "prepare":
        from etlbench import prepare
        prepare.run(args.csv)
        return
    workers = getattr(args, "workers", 1)
    fetch_size = getattr(args, "fetch_size", None)
    if fetch_size is None:
        fetch_size = args.batch_size
    reader = getattr(args, "reader", None)
    if args.batch_size < 1 or fetch_size < 1 or workers < 1:
        parser.error("batch-size, fetch-size and workers must be positive")
    if args.profile_interval <= 0:
        parser.error("profile-interval must be positive")
    driver = getattr(args, "driver", None)
    # Ignore experimental writer settings exported by the research benchmark.
    os.environ["ASYNCH_WRITER_MODE"] = "python"
    if driver == "patched":
        from etlbench.native.native_writer import enable
        enable()
    columns, expected, first, last = source_info()
    run_id = uuid.uuid4().hex
    target = f"etl_target_{run_id}"
    mode = args.mode.replace("-", "_")
    pipeline = importlib.import_module(f"etlbench.pipelines.{mode}_pipeline")
    settings = {} if args.mode == "server-side" else {"batch_size": args.batch_size}
    if args.mode != "server-side":
        settings["fetch_size"] = fetch_size
        settings["reader"] = reader
    if args.mode == "parallel" or (args.mode == "server-side" and workers > 1):
        settings["workers"] = workers
    if driver:
        settings["driver"] = driver
    event("started", run_id=run_id, pipeline=args.mode, **settings)
    profiler = None
    if args.profile:
        profiler = PostgresProfiler(PG_DSN, PG_SCHEMA, args.profile_interval, table=PG_TABLE)
    profile_output = args.profile_output or Path("/state/profiles") / f"{run_id}.json"
    resources = None
    with clickhouse() as client:
        created = False
        try:
            create_target(client, target, columns)
            created = True
            if profiler:
                profiler.start()
            if args.resource_output:
                from etlbench.profiling.resources import ResourceProfile

                resources = ResourceProfile(args.profile_interval, target)
                resources.start()
                stages.start()
            started = time.perf_counter()
            arguments = (columns, target, args.batch_size, fetch_size, workers, first, last)
            if args.mode in ("async", "hybrid"):
                rows, batches = asyncio.run(pipeline.run(*arguments, reader=reader))
            else:
                rows, batches = pipeline.run(*arguments, reader=reader)
            ended = time.perf_counter()
            seconds = ended - started
            if resources:
                spans = stages.finish()
                resources.stop()
            if profiler:
                profiler.stop()
                profile_error = profiler.save(profile_output, run_id, args.mode, settings)
                if profile_error:
                    event("profile_failed", run_id=run_id, error=profile_error)
                else:
                    event(
                        "profile_saved", run_id=run_id, file=str(profile_output),
                        summary_file=str(profiler.summary_path(profile_output)),
                    )
                    event("profile_summary", run_id=run_id, **profiler.human_summary())
                profiler = None
            actual = client.execute(f"SELECT count() FROM {target}")[0][0]
            if actual != expected or (rows is not None and rows != expected):
                raise RuntimeError(f"Row count mismatch: source={expected}, target={actual}, sent={rows}")
            result = {
                "run_id": run_id, "pipeline": args.mode, **settings,
                "rows": actual, "seconds": round(seconds, 6),
                "rows_per_second": round(actual / seconds, 3),
            }
            counter_name = (
                "statements" if args.mode == "server-side" or reader == "copy-direct"
                else "batches"
            )
            result[counter_name] = batches
        except BaseException as error:
            event("failed", run_id=run_id, error_type=type(error).__name__)
            raise
        finally:
            if resources and stages.active():
                stages.finish()
                resources.stop()
            if created:
                client.execute(f"DROP TABLE IF EXISTS {target} SYNC")
            if profiler:
                profiler.stop()
                profile_error = profiler.save(profile_output, run_id, args.mode, settings)
                if profile_error:
                    event("profile_failed", run_id=run_id, error=profile_error)
                else:
                    event(
                        "profile_saved", run_id=run_id, file=str(profile_output),
                        summary_file=str(profiler.summary_path(profile_output)),
                    )
                    event("profile_summary", run_id=run_id, **profiler.human_summary())
    if resources:
        resources.save(args.resource_output, result, started, ended, spans)
    print(json.dumps(result), flush=True)
    event("finished", run_id=run_id)


if __name__ == "__main__":
    main()
