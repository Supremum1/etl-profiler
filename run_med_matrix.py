"""Run 21 PostgreSQL to ClickHouse configurations with Docker Compose."""
import argparse
import hashlib
import json
import random
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COMPOSE = ["docker", "compose"]
BATCH_FETCH = [(10_000, 10_000), (10_000, 50_000), (50_000, 50_000)]


def compose(*arguments, **kwargs):
    return subprocess.run([*COMPOSE, *arguments], cwd=ROOT, check=True, text=True, **kwargs)


def configurations(sizes=BATCH_FETCH):
    for mode, variant, options in (
        ("sync", "sync", []),
        ("async", "async-official", ["--driver", "official"]),
        ("async", "async-patched", ["--driver", "patched"]),
        ("hybrid", "hybrid", []),
        ("parallel", "parallel-w4", ["--workers", "4"]),
    ):
        readers = ["stream", "cursor", "keyset"]
        if mode in ("sync", "parallel"):
            readers.append("copy")
            yield f"{variant}-copy-direct", [mode, *options, "--reader", "copy-direct"]
        for reader in readers:
            for batch, fetch in sizes:
                yield f"{variant}-{reader}-b{batch}-f{fetch}", [
                    mode, *options, "--reader", reader, "--batch-size", str(batch),
                    "--fetch-size", str(fetch),
                ]
    yield "server-side", ["server-side"]
    yield "server-side-w4", ["server-side", "--workers", "4"]


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prepare(csv_name):
    compose("up", "-d", "--wait", "postgres", "clickhouse")
    compose("build", "bench")
    compose("run", "--rm", "-T", "bench", "prepare", "/data/" + csv_name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--series", default="benchmark", help="Output subdirectory under results")
    parser.add_argument("--csv", default="example.csv", help="CSV filename inside data/")
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--fetch-size", type=int, default=50000)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--skip-prepare", action="store_true")
    parser.add_argument("--skip-warmup", action="store_true")
    args = parser.parse_args()
    if args.repeats < 1 or args.interval <= 0 or not args.series.replace("-", "").replace("_", "").isalnum():
        parser.error("Require positive repeats/interval and a single alphanumeric series name")
    if Path(args.csv).name != args.csv or not (ROOT / "data" / args.csv).is_file():
        parser.error("--csv must name an existing file inside data/")
    if args.batch_size < 1 or args.fetch_size < 1:
        parser.error("batch-size and fetch-size must be positive")
    if not args.skip_prepare:
        prepare(args.csv)
    output = ROOT / "results" / args.series
    output.mkdir(parents=True, exist_ok=True)
    engine = json.loads(subprocess.check_output(
        ["docker", "info", "--format", "{{json .}}"], text=True, encoding="utf-8"
    ))
    ids = {
        service: compose("ps", "-q", service, capture_output=True).stdout.strip()
        for service in ("postgres", "clickhouse")
    }
    if not all(ids.values()):
        raise RuntimeError("Start the PostgreSQL and ClickHouse services before running the matrix")
    images = {
        service: subprocess.check_output(
            ["docker", "inspect", "--format", "{{.Config.Image}}", identifier], text=True
        ).strip()
        for service, identifier in ids.items()
    }
    metadata = output / "environment.json"
    environment = {
        "dataset": args.csv, "planned_configurations": 21, "fixed_sizes": True,
        "dataset_sha256": file_hash(ROOT / "data" / args.csv), "engine_cpus": engine["NCPU"],
        "engine_memory": engine["MemTotal"], "images": images,
        "sampling_seconds": args.interval, "warmup_requested": not args.skip_warmup,
        "batch_size": args.batch_size, "fetch_size": args.fetch_size,
    }
    if metadata.exists() and any(output.glob("*.profile.json")):
        if json.loads(metadata.read_text(encoding="utf-8")) != environment:
            raise SystemExit("Series settings changed. Choose a new --series.")
    metadata.write_text(json.dumps(environment, indent=2), encoding="utf-8")
    if not args.skip_warmup:
        compose("run", "--rm", "-T", "bench-profile", "sync", "--reader", "copy-direct")
    env = [flag for service, identifier in ids.items()
           for flag in ("-e", f"RESOURCE_{service.upper()}_CONTAINER={identifier}")]
    configs = list(configurations([(args.batch_size, args.fetch_size)]))
    failures = []
    for repeat in range(1, args.repeats + 1):
        order = configs.copy()
        random.Random(20260713 + repeat).shuffle(order)
        for index, (name, command) in enumerate(order, 1):
            stem = f"{name}-r{repeat}"
            result_path = output / f"{stem}.profile.json"
            if result_path.exists():
                continue
            print(f"[{repeat}/{args.repeats} · {index}/{len(order)}] {name}", flush=True)
            with (output / f"{stem}.log").open("w", encoding="utf-8") as log:
                try:
                    compose(
                        "run", "--rm", "-T", *env, "bench-profile", *command,
                        "--profile-interval", str(args.interval),
                        "--resource-output", f"/results/{args.series}/{result_path.name}",
                        stdout=log, stderr=subprocess.STDOUT,
                    )
                    result = json.loads(result_path.read_text(encoding="utf-8"))
                    if result["errors"] or any(len(result["samples"][service]) < 2 for service in ("python", *ids)):
                        result_path.rename(output / f"{stem}.incomplete.json")
                        raise ValueError("missing container samples")
                    print(json.dumps(result["result"], ensure_ascii=False), flush=True)
                except (subprocess.CalledProcessError, OSError, ValueError):
                    failures.append(stem)
                    print(f"FAILED: {stem} (see {log.name})", flush=True)
    compose("run", "--rm", "--no-deps", "-T", "--entrypoint", "python",
            "bench", "make_dashboard.py", f"/results/{args.series}")
    if failures:
        raise SystemExit(f"{len(failures)} failed: {', '.join(failures)}")


if __name__ == "__main__":
    main()
